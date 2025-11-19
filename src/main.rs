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
}

impl Page {
    fn new(idx: usize, ord: usize, rc: usize) -> Page {
        Page {
            idx,
            ref_cnt: rc,
            ord,
            next: None,
        }
    }

    fn rm_links(&mut self) {
        self.next = None;
    }

    fn split(&mut self, order: usize, idx: usize, a: &mut Allocator) {
        assert!(order < 8);
        let len = (4096 << (Allocator::ORDER - order)) / 2;
        let p_idx_1 = self.idx;
        let p_idx_2 = self.idx + (len / 4096);
        a.free_lists[order].remove(self);
        let ptr1 = unsafe { a.ptr.add(p_idx_1) };
        let ptr2 = unsafe { a.ptr.add(p_idx_2) };
        let ptr1_r = unsafe { ptr1.as_mut() }.unwrap();
        let ptr2_r = unsafe { ptr2.as_mut() }.unwrap();
        assert!(ptr1_r.idx == p_idx_1);
        assert!(ptr2_r.idx == p_idx_2);
        ptr1_r.ord = order + 1;
        ptr2_r.ord = order + 1;
        a.free_lists[order + 1].add(ptr2);
        a.free_lists[order + 1].add(ptr1);
    }

    fn is_idle(&self) -> bool {
        self.ref_cnt == 0
    }

    fn join(&mut self, alloc: &mut Allocator) {
        let mut page = self;

        loop {
            if page.ord == 0 {
                return alloc.free_lists[page.ord].add(page);
            }
            let addr = page.idx * 4096;
            let b_idx = Allocator::get_buddy(addr, page.ord) / 4096;
            let buddy_ptr = unsafe { alloc.ptr.add(b_idx) };
            let buddy = unsafe { buddy_ptr.as_mut() }.unwrap();
            if buddy.ord != page.ord || !buddy.is_idle() {
                return alloc.free_lists[page.ord].add(page);
            }

            alloc.free_lists[page.ord].remove(buddy);
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
        }
    }
}

#[derive(Default)]
struct FL {
    head: Option<NonNull<Page>>,
}

impl FL {
    fn remove(&mut self, node: &mut Page) {
        assert!(self.head.is_some());
        let mut h = unsafe { self.head.unwrap().as_mut() };
        if h.idx == node.idx {
            self.head = h.next;
            h.next = None;
            return;
        }

        while let Some(mut nxt) = h.next {
            let nxt = unsafe { nxt.as_mut() };
            if nxt.idx == node.idx {
                h.next = node.next;
                node.next = None;
                return;
            }
            h = nxt;
        }

        unreachable!()
    }

    fn add(&mut self, node: *mut Page) {
        let node = unsafe { node.as_mut().unwrap() };
        node.next = self.head;
        self.head = NonNull::new(node as *mut Page);
    }

    fn rm_first(&mut self) -> Option<*mut Page> {
        match self.head {
            Some(mut h) => {
                let h = unsafe { h.as_mut() };
                self.head = h.next;
                Some(h)
            }
            _ => None,
        }
    }

    fn get_head(&self) -> Option<*mut Page> {
        match self.head {
            Some(mut h) => Some(unsafe { h.as_mut() }),
            _ => None,
        }
    }

    fn print_list(&self) {
        if self.head.is_none() {
            return;
        }
        let mut ptr = Some(unsafe { self.head.unwrap().as_ref() });
        while let Some(p) = ptr {
            println!("{:?}", p);
            ptr = match p.next {
                Some(n) => Some(unsafe { n.as_ref() }),
                _ => None,
            };
        }
    }
}

struct Allocator {
    free_lists: [FL; 9],
    ptr: *mut Page,
    size: usize,
    meta_size: usize,
}

impl Allocator {
    const ORDER: usize = 8;

    fn new(ptr: *mut u8, len: usize) -> Allocator {
        assert!(len % 4096 == 0);
        let npages = len / 4096;
        let page_ptr = ptr as *mut Page;
        for i in 0..npages {
            unsafe {
                *page_ptr.add(i) = Page::new(i, 8, 1);
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
        let mut i = align_f(self.meta_size, 4096);
        loop {
            self.free((i + self.ptr as usize) as *mut u8);
            i += 4096;
            if i == self.size {
                break;
            }
        }
    }

    fn get_ord(n: usize) -> usize {
        8 - (align_f(n, 4096).next_power_of_two().ilog2() - 12) as usize
    }

    fn split_to(&mut self, page: &mut Page, mut cur_ord: usize, t_ord: usize) {
        assert!(t_ord > cur_ord);
        let mut page_tmp = page;
        let oc = cur_ord;
        while t_ord > cur_ord {
            page_tmp.split(cur_ord, 0, self);
            page_tmp = unsafe {
                self.free_lists[cur_ord + 1]
                    .get_head()
                    .unwrap()
                    .as_mut()
                    .unwrap()
            };
            cur_ord += 1;
        }
        assert!(oc != cur_ord);
    }

    fn _alloc(&mut self, n: usize) -> Option<*mut u8> {
        if n > MB || n == 0 {
            return None;
        }

        let ord = Self::get_ord(n) as usize;

        if let Some(p) = self.free_lists[ord].rm_first() {
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
            if let Some(p) = self.free_lists[i].get_head() {
                self.split_to(unsafe { p.as_mut().unwrap() }, i, ord);
                break;
            }
            if i == 0 {
                break;
            }
            i -= 1;
        }

        if let Some(p) = self.free_lists[ord].rm_first() {
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
        assert!(page.next.is_none());
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
    let mut r = File::open("/dev/urandom").unwrap();
    r.read_exact(&mut buf.buf).unwrap();
    unsafe { *(buf.buf.as_ptr() as *const u32) as usize }
}

fn main() {
    let mut a = Allocator::new(unsafe { BUF.as_mut_ptr() }, unsafe { BUF.len() });
    let mut h = vec![];

    for _ in 0..100 {
        loop {
            let size = 4096 << (rand_u32() % Allocator::ORDER);
            if let Some(a) = a.alloc(rand_u32() % size) {
                unsafe { *a = 69 };
                h.push(a);
            } else {
                // println!("ptr = {:?}", a);
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
        a.free_lists[i].print_list();
    }
}
