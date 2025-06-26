use super::def::{EARLY_EXE_WINDOW_SIZE, SC};
use log::debug;
use parking_lot::Mutex;
use qmdb::tasks::taskid::{join_task_id, split_task_id};
use std::{
    iter::repeat_with,
    sync::{
        atomic::{AtomicI32, AtomicU32, AtomicU64},
        Arc,
    },
};

pub const FIRST_FRAME: i32 = -1;
const DISPATCH_MASK: i32 = i32::MIN;

pub struct LinkedListItem {
    //when all_done is me, who will be the first item to ignite?
    pub to_ignite: AtomicI32,
    //when I'm ignited, who will be the next item to ignite?
    pub next: AtomicI32,
}

#[derive(Default)]
pub struct Dashboard {
    pub all_done_index: AtomicI32,
    pub valid_count: AtomicI32,
    //its high 40 bits is block height
    pub hi: i64,
    //it contains the warmed counts of batches
    pub warmed_counts: Vec<AtomicI32>,
    all_warmed_idx: Mutex<i32>,
    pub executed_bitvec: Vec<AtomicU64>,
    pub ignite_ll: Vec<LinkedListItem>,
    pub warmup_status_vec: Arc<Vec<AtomicU32>>,
    pub batch_size: i32,
}

impl Dashboard {
    pub fn new(size: usize, height: i64, batch_size: i32) -> Self {
        let b = batch_size as usize;
        debug!("DASHB size={} warmed_counts.len={}", size, size.div_ceil(b));
        Dashboard {
            all_done_index: AtomicI32::new(-1),
            valid_count: AtomicI32::new(size as i32),
            hi: join_task_id(height, 0, false),

            warmed_counts: repeat_with(|| AtomicI32::new(0))
                .take(size.div_ceil(b))
                .collect(),
            all_warmed_idx: Mutex::new(-1),

            executed_bitvec: repeat_with(|| AtomicU64::new(0))
                .take((size + 63) / 64)
                .collect(),

            //dbg_ignited_bitvec: Arc::new(repeat_with(|| {
            //    AtomicU64::new(0)
            //}).take((size + 63) / 64).collect()),
            ignite_ll: repeat_with(|| LinkedListItem {
                to_ignite: AtomicI32::new(0),
                next: AtomicI32::new(0),
            })
            .take(size)
            .collect(),

            warmup_status_vec: Arc::new(repeat_with(|| AtomicU32::new(0)).take(size).collect()),
            batch_size,
        }
    }

    pub fn height(&self) -> i64 {
        split_task_id(self.hi).0
    }

    fn parse_batch(&self, batch_idx: i32, valid_count: i32) -> (i32, i32, bool) {
        let batch_start = batch_idx * self.batch_size;
        let mut is_last = true;
        let mut num_in_batch = valid_count - batch_start;
        if num_in_batch > self.batch_size {
            is_last = false;
            num_in_batch = self.batch_size;
        }
        (batch_start, num_in_batch, is_last)
    }

    pub fn send_batch_end(&self, idx: i32) -> Option<(i32, bool)> {
        let valid_count = self.valid_count.load(SC);
        let batch_idx = idx / self.batch_size;
        let (_, mut num_in_batch, _) = self.parse_batch(batch_idx, valid_count);
        // debug!!("II start={} num_in_batch={} is_last={} batch_idx={}", start, num_in_batch, is_last, batch_idx);
        let old = self.warmed_counts[batch_idx as usize].fetch_add(1, SC);
        // if idx > 80570 {
        //     debug!!("HH idx={} batch_idx={} warmed_counts={}", idx, batch_idx, old + 1);
        // }
        if old + 1 != num_in_batch {
            return None;
        }
        let mut guard = self.all_warmed_idx.lock();
        let origin = *guard;
        while *guard < self.warmed_counts.len() as i32 - 1 {
            let next = *guard + 1;
            (_, num_in_batch, _) = self.parse_batch(next, valid_count);
            if num_in_batch < 0 {
                break;
            }
            //println!("JJ start={} num_in_batch={} is_last={} all-warmed={} valid_count={}", start, num_in_batch, is_last, next, valid_count);
            let can_incr = if 1 + *guard == batch_idx {
                true
            } else {
                let count = self.warmed_counts[next as usize].load(SC);
                //println!("HH count={} num_in_batch={} batch_size={}", count, num_in_batch, self.batch_size);
                count == num_in_batch
            };
            if can_incr {
                *guard = next;
            } else {
                break;
            }
        }
        if origin == *guard {
            return None;
        }
        let (start, num_in_batch, is_last) = self.parse_batch(*guard, valid_count);
        let mut batch_end = start + num_in_batch - 1;
        if !is_last {
            // to make sure ignite_ll is ready
            batch_end -= EARLY_EXE_WINDOW_SIZE as i32;
        }
        //println!("HERE batch_idx={} origin={} all_warmed_idx={} batch_end={}", batch_idx, origin, *guard, batch_end);
        Some((batch_end, is_last))
    }

    // set a bit to indicate a task is executed
    pub fn set_executed(&self, idx: i32) {
        set_bit_at(&self.executed_bitvec, idx);
    }

    pub fn is_executed(&self, idx: i32) -> bool {
        let (i, j) = (idx / 64, idx % 64);
        let bits = self.executed_bitvec[i as usize].load(SC);
        (bits & (1u64 << j)) != 0
    }

    pub fn set_dispatched(&self, idx: i32) -> bool {
        let old = self.ignite_ll[idx as usize]
            .next
            .fetch_or(DISPATCH_MASK, SC);
        (old & DISPATCH_MASK) == 0
    }

    pub fn not_dispatched(&self, idx: i32) -> bool {
        let old = self.ignite_ll[idx as usize].next.load(SC);
        (old & DISPATCH_MASK) == 0
    }

    pub fn set_eei(&self, my_idx: i32, eei: i32) {
        assert!(my_idx > eei, "my_idx={} eei={}", my_idx, eei);
        if eei == FIRST_FRAME {
            // set 'next' to i32::MIN to mark the first frame
            self.ignite_ll[my_idx as usize].next.store(eei, SC);
            return;
        }
        let next: i32 = self.ignite_ll[eei as usize].to_ignite.swap(my_idx, SC);
        if next != 0 {
            self.ignite_ll[my_idx as usize].next.store(next, SC);
        }
    }

    pub fn get_ignited_list(&self, idx: i32, out_vec: &mut Vec<i32>) {
        let mut next = self.ignite_ll[idx as usize].to_ignite.load(SC);
        while next != 0 {
            let old = self.ignite_ll[next as usize]
                .next
                .fetch_or(DISPATCH_MASK, SC);
            if (old & DISPATCH_MASK) == 0 {
                out_vec.push(next);
            }
            next = old & !DISPATCH_MASK; //clear dispatch_marker
        }
    }

    pub fn print_ignited_lists(&self) {
        for i in 0..self.ignite_ll.len() {
            let mut next = self.ignite_ll[i].to_ignite.load(SC);
            let is_executed = self.is_executed(i as i32);
            if next == FIRST_FRAME {
                println!("#{} {} FF", is_executed, i);
            } else {
                next &= !DISPATCH_MASK; //clear dispatch_marker
                if next == 0 {
                    println!("#{} {} nil", is_executed, i);
                } else {
                    print!("#{} {} =>", is_executed, i);
                    let mut count = 0;
                    while next != 0 && count < 200 {
                        print!(" {}", next);
                        next = self.ignite_ll[next as usize].next.load(SC);
                        next &= !DISPATCH_MASK;
                        count += 1;
                    }
                    println!();
                }
            }
        }
    }

    pub fn set_valid_count(&self, valid_count: usize) {
        self.valid_count.store(valid_count as i32 - 1, SC); //the end_block task is not counted
    }
}

fn set_bit_at(bitvec: &[AtomicU64], idx: i32) {
    let (i, j) = (idx / 64, idx % 64);
    let mask = 1u64 << j;
    bitvec[i as usize].fetch_or(mask, SC);
}

// #[inline]
// fn get_bit_at(bitvec: &[AtomicU64], idx: i32) -> bool {
//     let (i, j) = (idx / 64, idx % 64);
//     let mask = 1u64 << j;
//     bitvec[i as usize].load(SC) & mask != 0
// }

#[cfg(test)]
mod tests {
    use crate::dispatcher::def::EARLY_EXE_WINDOW_SIZE;

    use super::Dashboard;

    #[test]
    fn test_dashboard0() {
        let size = 1 << 10;
        let dashboard = Dashboard::new(size, 0, 10);
        for i in 0..size {
            let eei = if i > EARLY_EXE_WINDOW_SIZE {
                (i - EARLY_EXE_WINDOW_SIZE) as i32
            } else {
                -1
            };
            dashboard.set_eei(i as i32, eei);
        }
        dashboard.print_ignited_lists();
    }

    #[test]
    fn test_dashboard1() {
        let size = 1 << 10;
        let dashboard = Dashboard::new(size, 0, 10);
        for i in 0..size {
            let eei = if i >= 2 { 1 } else { -1 };
            dashboard.set_eei(i as i32, eei);
        }
        dashboard.print_ignited_lists();
    }
}
