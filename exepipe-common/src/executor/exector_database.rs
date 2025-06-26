use revm::{
    primitives::{Address, B256, U256},
    state::AccountInfo,
};

pub trait ExectorDatabase {
    /// Gets basic account information.
    fn basic(&mut self, address: &Address) -> Option<AccountInfo>;

    /// Gets storage value of address at index.
    fn storage_value(&mut self, address: Address, index: &U256) -> Option<&[u8]>;

    /// Gets block hash by block number.
    fn block_hash(&mut self, number: u64) -> Option<B256>;
}
