use exepipe_common::executor::exector_database::ExectorDatabase;
use exepipe_common::utils::join_address_index;
use qmdb::entryfile::readbuf::ReadBuf;
use qmdb::ADS;
use revm::primitives::{Address, FixedBytes, U256};
use revm::state::AccountInfo;

use super::block_context::BlockContext;

// to support the execution of one transaction
pub struct TxContext<'a, T: ADS> {
    blk_ctx: &'a BlockContext<T>,
    temp_buf: ReadBuf,
}

impl<'a, T: ADS> TxContext<'a, T> {
    pub fn new(blk_ctx: &'a BlockContext<T>) -> Self {
        TxContext {
            blk_ctx,
            temp_buf: ReadBuf::new(),
        }
    }
}

// Evm use TxContext as a Database
// It uses blk_ctx as datasource, and uses access_set as constrain
impl<T: ADS> ExectorDatabase for TxContext<'_, T> {
    fn basic(&mut self, address: &Address) -> Option<AccountInfo> {
        self.blk_ctx.basic(&address)
    }

    fn storage_value(&mut self, address: Address, index: &U256) -> Option<&[u8]> {
        self.temp_buf.clear();
        let addr_idx = join_address_index(&address, &index);
        self.blk_ctx
            .storage_value(&addr_idx, true, &mut self.temp_buf);
        if self.temp_buf.is_empty() {
            return None;
        }
        Some(self.temp_buf.as_slice())
    }

    fn block_hash(&mut self, number: u64) -> Option<FixedBytes<32>> {
        self.blk_ctx.get_block_hash(number)
    }
}
