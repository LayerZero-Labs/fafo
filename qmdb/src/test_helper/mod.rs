mod randsrc;
pub mod task_builder;

use crate::entryfile;
use crate::tasks;
use byteorder::{BigEndian, ByteOrder};

pub use entryfile::helpers::EntryBuilder;
pub use randsrc::RandSrc;
pub use tasks::helpers::SimpleTask;

pub fn to_k80(k64: u64) -> [u8; 10] {
    let mut k80 = [0u8; 10];
    BigEndian::write_u64(&mut k80, k64);
    k80
}
