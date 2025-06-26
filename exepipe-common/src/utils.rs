use revm::primitives::{Address, U256};

pub fn join_address_index(address: &Address, index: &U256) -> [u8; 20 + 32] {
    let mut addr_idx = [0u8; 20 + 32];
    addr_idx[..20].copy_from_slice(&address[..]);
    addr_idx[20..].copy_from_slice(index.as_le_slice());
    addr_idx
}
