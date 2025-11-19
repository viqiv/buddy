use std::{fs::File, io::Read, mem::size_of, ptr::NonNull};

const MB: usize = 1024 * 1024;

#[repr(align(16))]
struct Buf {
    buf: [u8; MB * 8],
}

impl Buf {
    fn len(&self) -> usize {
        self.buf.len()
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.buf.as_mut_ptr()
    }
}

static mut BUF: Buf = Buf { buf: [0u8; MB * 8] };

fn align_b(n: usize, t: usize) -> usize {
    n & !t.wrapping_sub(1)
}

fn align_f(n: usize, t: usize) -> usize {
    align_b(n.wrapping_sub(1), t).wrapping_add(t)
}

#[derive(Debug)]
struct Page {
    idx: usize,
    ref_cnt: usize,
    ord: usize,
    next: Option<NonNull<Page>>,
    prev: Option<NonNull<Page>>,
}

impl Page {
    fn new(idx: usize, ord: usize) -> Page {
        Page {
            idx,
            ref_cnt: 0,
            ord,
            next: None,
            prev: None,
        }
    }

    fn append(&mut self, p: *mut Page) {
        assert!(self as *mut Page != p);
        let r = unsafe { p.as_mut() }.unwrap();
        r.prev = NonNull::new(self as *mut Page);
        self.next = NonNull::new(p);
    }

    fn print_list(&self) {
        let mut ptr = Some(self);
        while let Some(p) = ptr {
            println!("{:?}", p);
            ptr = match p.next {
                Some(n) => Some(unsafe { n.as_ref() }),
                _ => None,
            };
        }
    }

    fn remove(&mut self) {
        if let Some(mut nxt) = self.next {
            let nxt = unsafe { nxt.as_mut() };
            nxt.prev = self.prev
        }

        if let Some(mut prev) = self.prev {
            let prev = unsafe { prev.as_mut() };
            prev.next = self.next
        }
    }

    fn rm_links(&mut self) {
        self.next = None;
        self.prev = None;
    }

    fn split(&mut self, order: usize, idx: usize, a: &mut Allocator) {
        assert!(order < 8);
        let len = (4096 << (Allocator::ORDER - order)) / 2;
        let p_idx_1 = self.idx;
        let p_idx_2 = self.idx + (len / 4096);
        if idx == 0 {
            a.free_lists[order] = self.next;
        }
        self.remove();
        self.rm_links();
        let ptr1 = unsafe { a.ptr.add(p_idx_1) };
        let ptr2 = unsafe { a.ptr.add(p_idx_2) };
        let ptr1_r = unsafe { ptr1.as_mut() }.unwrap();
        let ptr2_r = unsafe { ptr2.as_mut() }.unwrap();
        assert!(ptr1_r.idx == p_idx_1);
        assert!(ptr2_r.idx == p_idx_2);
        ptr1_r.ord = order + 1;
        ptr2_r.ord = order + 1;
        unsafe { ptr1.as_mut() }.unwrap().append(ptr2);
        match a.free_lists[order + 1] {
            Some(f) => {
                unsafe { ptr2.as_mut() }.unwrap().append(f.as_ptr());
                a.free_lists[order + 1] = NonNull::new(ptr1);
            }
            None => {
                a.free_lists[order + 1] = NonNull::new(ptr1);
            }
        }
    }

    fn is_idle(&self) -> bool {
        self.ref_cnt == 0
    }

    fn add_to_fl(&mut self, alloc: &mut Allocator) {
        match alloc.free_lists[self.ord] {
            Some(s) => {
                self.append(s.as_ptr());
            }
            _ => {}
        }
        alloc.free_lists[self.ord] = NonNull::new(self as *mut Page);
    }

    fn join(&mut self, alloc: &mut Allocator) {
        let mut page = self;

        loop {
            if page.ord == 0 {
                return page.add_to_fl(alloc);
            }
            let addr = page.idx * 4096;
            let b_idx = Allocator::get_buddy(addr, page.ord) / 4096;
            let buddy_ptr = unsafe { alloc.ptr.add(b_idx) };
            let buddy = unsafe { buddy_ptr.as_mut() }.unwrap();
            if buddy.ord != page.ord || !buddy.is_idle() {
                return page.add_to_fl(alloc);
            }

            buddy.remove();

            if alloc.free_lists[page.ord].is_some_and(|x| x.as_ptr() == buddy_ptr) {
                alloc.free_lists[page.ord] = buddy.next;
            }
            buddy.rm_links();
            let merg = if page.idx > buddy.idx { buddy } else { page };
            merg.ord -= 1;
            page = merg;
        }
    }
}

impl Default for Page {
    fn default() -> Self {
        Self {
            idx: 0,
            ref_cnt: 0,
            ord: 0,
            next: None,
            prev: None,
        }
    }
}

struct Allocator {
    free_lists: [Option<NonNull<Page>>; 9],
    ptr: *mut Page,
    size: usize,
    meta_size: usize,
}

impl Allocator {
    const ORDER: usize = 8;

    fn new(ptr: *mut u8, len: usize) -> Allocator {
        assert!(len.count_ones() == 1);
        let npages = len / 4096;
        let page_ptr = ptr as *mut Page;
        for i in 0..npages {
            unsafe {
                *page_ptr.add(i) = Page::new(i, 0);
            }
        }
        let mut alloc = Allocator {
            free_lists: Default::default(),
            ptr: page_ptr,
            size: len,
            meta_size: npages * size_of::<Page>(),
        };
        alloc.init_free_list();
        alloc
    }

    fn init_free_list(&mut self) {
        let big_ord = 4096 << Self::ORDER;
        let meta_pages = align_f(self.meta_size, big_ord) / big_ord;
        let total_pages = self.size / big_ord;
        let mut prev: Option<NonNull<Page>> = None;
        for i in meta_pages..total_pages {
            let p_idx = (big_ord / 4096) * i;
            match self.free_lists[0] {
                Some(_) => {
                    let prev_ref = unsafe { prev.unwrap().as_mut() };
                    let nxt = unsafe { self.ptr.add(p_idx) };
                    prev_ref.append(nxt);
                    prev = NonNull::new(nxt);
                }
                None => {
                    assert!(i == meta_pages);
                    let p = unsafe { self.ptr.add(p_idx) };
                    self.free_lists[0] = NonNull::new(p);
                    prev = self.free_lists[0];
                }
            }
        }
    }

    fn get_ord(n: usize) -> usize {
        8 - (align_f(n, 4096).next_power_of_two().ilog2() - 12) as usize
    }

    fn remove_first(&mut self, fidx: usize) -> Option<*mut Page> {
        match self.free_lists[fidx] {
            Some(mut f) => {
                let fr = unsafe { f.as_mut() };
                self.free_lists[fidx] = fr.next;
                fr.remove();
                Some(f.as_ptr())
            }
            _ => None,
        }
    }

    fn split_to(&mut self, page: &mut Page, mut cur_ord: usize, t_ord: usize) {
        assert!(t_ord > cur_ord);
        let mut page_tmp = page;
        while t_ord > cur_ord {
            page_tmp.split(cur_ord, 0, self);
            page_tmp = unsafe { self.free_lists[cur_ord + 1].unwrap().as_mut() };
            cur_ord += 1;
        }
    }

    fn _alloc(&mut self, n: usize) -> Option<*mut u8> {
        if n > MB || n == 0 {
            return None;
        }

        let ord = Self::get_ord(n) as usize;

        if let Some(p) = self.remove_first(ord) {
            let p = unsafe { p.as_mut() }.unwrap();
            p.ref_cnt += 1;
            p.rm_links();
            return Some((p.idx * 4096) as *mut u8);
        }

        if ord == 0 {
            return None;
        }

        let mut i = ord - 1;
        loop {
            if let Some(p) = self.remove_first(i) {
                self.split_to(unsafe { p.as_mut().unwrap() }, i, ord);
                break;
            }
            if i == 0 {
                break;
            }
            i -= 1;
        }

        if let Some(p) = self.remove_first(ord) {
            let p = unsafe { p.as_mut() }.unwrap();
            p.ref_cnt += 1;
            p.rm_links();
            return Some((p.idx * 4096) as *mut u8);
        } else {
            None
        }
    }

    fn alloc(&mut self, n: usize) -> Option<*mut u8> {
        match self._alloc(n) {
            Some(n) => Some((n as usize + self.ptr as usize) as *mut u8),
            None => None,
        }
    }

    fn get_buddy(addr: usize, ord: usize) -> usize {
        addr ^ (4096 << (Allocator::ORDER - ord))
    }

    fn free(&mut self, addr: *mut u8) {
        let addr = (addr as usize) - self.ptr as usize;
        assert!(addr < self.size);
        let idx = addr / 4096;
        let page = unsafe { self.ptr.add(idx).as_mut() }.unwrap();
        assert!(page.next.is_none() && page.prev.is_none());
        page.ref_cnt -= 1;
        if page.ref_cnt > 0 {
            return;
        }
        page.join(self);
    }
}

#[repr(align(4))]
struct U32buff {
    buf: [u8; 4],
}
fn rand_u32() -> usize {
    let mut buf = U32buff { buf: [0, 0, 0, 0] };
    let mut r = File::open("/dev/random").unwrap();
    r.read_exact(&mut buf.buf).unwrap();
    unsafe { *(buf.buf.as_ptr() as *const u32) as usize }
}

fn main() {
    let mut a = Allocator::new(unsafe { BUF.as_mut_ptr() }, unsafe { BUF.len() });
    let mut h = vec![];

    for _ in 0..10 {
        loop {
            let size = 4096 << (rand_u32() % Allocator::ORDER);
            if let Some(a) = a.alloc(rand_u32() % size) {
                unsafe { *a = 69 };
                h.push(a);
            } else {
                break;
            };
        }

        while !h.is_empty() {
            let addr = h.remove(rand_u32() % h.len());
            a.free(addr);
        }
    }

    for i in 0..9 {
        println!("+++++++++FREELIST [{}]+++++++++++", i);
        match a.free_lists[i] {
            Some(f) => unsafe { f.as_ref() }.print_list(),
            None => {}
        }
    }
}
