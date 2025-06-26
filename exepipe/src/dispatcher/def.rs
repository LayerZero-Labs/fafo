use std::sync::atomic::Ordering;

pub const EARLY_EXE_WINDOW_SIZE: usize = 128;

pub const TX_IN_TASK: usize = 4;

pub const SC: Ordering = Ordering::SeqCst;
pub const BATCH_SIZE: i32 = 4096; //1<<21
pub const MAX_STEPS: i32 = 8;
pub const BATCH_CHAN_SIZE: usize = 1000_000;

pub const SCAN_COUNT: usize = 4;
