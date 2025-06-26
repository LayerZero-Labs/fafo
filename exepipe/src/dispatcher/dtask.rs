use std::sync::Arc;

use qmdb::ADS;

use crate::context::block_context::BlockContext;

pub struct DTask<T: ADS> {
    pub blk_ctx: Arc<BlockContext<T>>,
    pub idx: i32,
}

impl<T: ADS> DTask<T> {
    pub fn new(blk_ctx: Arc<BlockContext<T>>, idx: usize) -> Self {
        Self {
            blk_ctx,
            idx: idx as i32,
        }
    }
}

impl<T: ADS> Clone for DTask<T> {
    fn clone(&self) -> Self {
        Self {
            blk_ctx: self.blk_ctx.clone(),
            idx: self.idx,
        }
    }
}
