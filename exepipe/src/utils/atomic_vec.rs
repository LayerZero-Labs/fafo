use parking_lot::RwLock;
use std::iter::repeat_with;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const SEGLEN: usize = 2048;
const SC: Ordering = Ordering::SeqCst;

fn split_idx(idx: usize) -> (usize, usize) {
    (idx / SEGLEN, idx % SEGLEN)
}

#[derive(Clone)]
pub struct AtomicVec(Arc<RwLock<Vec<Vec<AtomicU64>>>>);

impl AtomicVec {
    pub fn new() -> Self {
        Self {
            0: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn take_free_list(&self) -> Vec<Vec<AtomicU64>> {
        let mut seg_list = self.0.write_arc();
        let mut tmp = Vec::<Vec<AtomicU64>>::with_capacity(0);
        std::mem::swap(&mut *seg_list, &mut tmp);
        tmp
    }

    pub fn set(&self, idx: usize, value: u64) {
        let (i, j) = split_idx(idx);
        let mut seg_list = self.0.write_arc();
        let seg = seg_list.get_mut(i).unwrap();
        seg[j].store(value, SC);
    }

    pub fn get(&self, idx: usize) -> u64 {
        let (i, j) = split_idx(idx);
        let seg_list = self.0.read_arc();
        let seg = seg_list.get(i).unwrap();
        seg[j].load(SC)
    }
}

pub struct AtomicVecBuilder {
    // seg_list is shared by the builder and the consumers
    seg_list: Arc<RwLock<Vec<Vec<AtomicU64>>>>,
    // 'pending' segments are finished but not flushed into seg_list yet
    pending: Vec<Vec<AtomicU64>>,
    // new values are appended into 'curr_seg'
    curr_seg: Vec<AtomicU64>,
    // size of the vector
    size: usize,
    // a list of segments allocated before
    free_list: Vec<Vec<AtomicU64>>,
}

impl AtomicVecBuilder {
    pub fn new(v: &AtomicVec, free_list: Vec<Vec<AtomicU64>>) -> Self {
        let mut res = Self {
            seg_list: v.0.clone(),
            pending: Vec::new(),
            curr_seg: Vec::with_capacity(0),
            size: 0,
            free_list,
        };
        res.allocate_new_seg();
        res.push(0); // #0 must be a dummy member
        res
    }

    fn allocate_new_seg(&mut self) {
        //round up
        self.size = ((self.size + SEGLEN - 1) / SEGLEN) * SEGLEN;
        if self.free_list.len() > 0 {
            self.curr_seg = self.free_list.pop().unwrap();
            return;
        }
        self.curr_seg = repeat_with(|| AtomicU64::new(0)).take(SEGLEN).collect();
    }

    // append value to the end of vec and return the value's position
    pub fn push(&mut self, value: u64) -> usize {
        if self.size % SEGLEN == 0 {
            let mut curr_seg = Vec::with_capacity(0);
            std::mem::swap(&mut curr_seg, &mut self.curr_seg);
            self.pending.push(curr_seg);
            self.allocate_new_seg();
        }
        let pos = self.size;
        self.size += 1;
        let (_, j) = split_idx(pos);
        self.curr_seg[j].store(value, SC);
        pos
    }

    pub fn flush(&mut self) {
        let mut pending = Vec::with_capacity(0);
        std::mem::swap(&mut pending, &mut self.pending);
        let mut curr_seg = Vec::with_capacity(0);
        std::mem::swap(&mut curr_seg, &mut self.curr_seg);
        self.allocate_new_seg();
        let mut seg_list = self.seg_list.write_arc();
        seg_list.reserve(1 + pending.len());
        seg_list.extend(pending);
        seg_list.push(curr_seg);
    }
}
