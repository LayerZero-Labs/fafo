const IN_BLOCK_IDX_BITS: usize = 24;
const IN_BLOCK_IDX_MASK: i64 = (1 << IN_BLOCK_IDX_BITS) - 1;
const END_BLOCK_FLAG: i64 = 1 << 63;
const TASK_ID_MASK: i64 = !(END_BLOCK_FLAG);

#[inline]
pub fn split_task_id(mut task_id: i64) -> (i64, usize) {
    task_id &= TASK_ID_MASK;
    let height = task_id >> IN_BLOCK_IDX_BITS;
    let index = (task_id & IN_BLOCK_IDX_MASK) as usize;
    (height, index)
}

/// Entry version uses the same encoding rule as task id.
#[inline]
pub fn join_task_id(height: i64, idx: usize, end_block: bool) -> i64 {
    assert!((idx >> IN_BLOCK_IDX_BITS) == 0, "index out of range");
    let mut id = (height << IN_BLOCK_IDX_BITS) | (idx as i64);
    if end_block {
        id |= 1 << 63;
    }
    id
}

#[inline]
pub fn exclude_end_block(task_id: i64) -> i64 {
    task_id & TASK_ID_MASK
}

#[inline]
pub fn is_first_task_in_block(task_id: i64) -> bool {
    (task_id & IN_BLOCK_IDX_MASK) == 0
}

#[inline]
pub fn is_end_task_in_block(task_id: i64) -> bool {
    (task_id & END_BLOCK_FLAG) != 0
}
