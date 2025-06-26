#[cfg(any(test, feature = "test-utils"))]
use crate::{
    acc_data::encode_account_info, slot_entry_value::SlotEntryValue, utils::join_address_index,
};
use qmdb::test_helper::task_builder::{SingleCsTask, TaskBuilder};
use revm::{
    primitives::{Address, KECCAK_EMPTY, U256},
    state::{AccountInfo, Bytecode},
};

/// A builder for creating changesets in tests.
/// This builder provides high-level operations for modifying state,
/// abstracting over the lower-level QMDB operations.
pub struct ChangesetBuilder {
    task_builder: TaskBuilder,
}

impl Default for ChangesetBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ChangesetBuilder {
    /// Creates a new ChangesetBuilder instance
    pub fn new() -> Self {
        Self {
            task_builder: TaskBuilder::new(),
        }
    }

    /// Creates a new EOA (Externally Owned Account)
    pub fn create_eoa(&mut self, address: Address, balance: U256, nonce: u64) -> &mut Self {
        let account_info = AccountInfo {
            balance,
            nonce,
            code_hash: KECCAK_EMPTY,
            code: Some(Bytecode::new()),
        };
        let encoded = encode_account_info(&account_info);
        self.task_builder.create(&address[..], encoded.as_slice());
        self
    }

    /// Creates a new Contract Account
    pub fn create_contract(
        &mut self,
        address: Address,
        balance: U256,
        nonce: u64,
        bytecode: Bytecode,
    ) -> &mut Self {
        let account_info = AccountInfo {
            balance,
            nonce,
            code_hash: bytecode.hash_slow(),
            code: Some(bytecode),
        };
        let encoded = encode_account_info(&account_info);
        self.task_builder.create(&address[..], encoded.as_slice());
        self
    }

    /// Updates an existing account's state
    pub fn update_account(
        &mut self,
        address: Address,
        balance: U256,
        nonce: u64,
        code_hash: revm::primitives::B256,
        code: Option<Bytecode>,
    ) -> &mut Self {
        let account_info = AccountInfo {
            balance,
            nonce,
            code_hash,
            code,
        };
        let encoded = encode_account_info(&account_info);
        self.task_builder.write(&address[..], encoded.as_slice());
        self
    }

    /// Deletes an account
    pub fn delete_account(&mut self, address: Address) -> &mut Self {
        self.task_builder.delete(&address[..], &[]);
        self
    }

    /// Creates a new storage slot
    pub fn create_storage(
        &mut self,
        address: Address,
        slot: U256,
        value: U256,
        dust: u128,
    ) -> &mut Self {
        let addr_idx = join_address_index(&address, &slot);
        let mut encoded_value = value.to_be_bytes_vec();
        SlotEntryValue::encode(&mut encoded_value, dust);
        self.task_builder.create(&addr_idx[..], &encoded_value);
        self
    }

    /// Updates an existing storage slot
    pub fn update_storage(
        &mut self,
        address: Address,
        slot: U256,
        value: U256,
        dust: u128,
    ) -> &mut Self {
        let addr_idx = join_address_index(&address, &slot);
        let mut encoded_value = value.to_be_bytes_vec();
        SlotEntryValue::encode(&mut encoded_value, dust);
        self.task_builder.write(&addr_idx[..], &encoded_value);
        self
    }

    /// Clears a storage slot
    pub fn clear_storage(&mut self, address: Address, slot: U256) -> &mut Self {
        let addr_idx = join_address_index(&address, &slot);
        self.task_builder.delete(&addr_idx[..], &[]);
        self
    }

    /// Builds and returns the final task with all changes
    pub fn build(&mut self) -> SingleCsTask {
        self.task_builder.build()
    }

    /// Access the underlying TaskBuilder if needed
    /// This allows for low-level operations when necessary
    pub fn task_builder(&mut self) -> &mut TaskBuilder {
        &mut self.task_builder
    }
}

#[cfg(test)]
mod tests {
    use qmdb::def::{OP_CREATE, OP_DELETE, OP_WRITE};

    use super::*;

    #[test]
    fn test_create_eoa() {
        let mut builder = ChangesetBuilder::new();
        let address = Address::from([1u8; 20]);
        let balance = U256::from(1000000);
        let nonce = 0;

        builder.create_eoa(address, balance, nonce);
        let task = builder.build();
        let changeset = &task.change_sets[0];
        assert_eq!(changeset.op_list.len(), 1);
        assert_eq!(changeset.op_list[0].op_type, OP_CREATE); // OP_CREATE
    }

    #[test]
    fn test_create_contract() {
        // TODO: AdsDatabaseAdapter does not support code_by_hash now
        // let mut builder = ChangesetBuilder::new();
        // let address = Address::from([2u8; 20]);
        // let bytecode = Bytecode::new_raw(Bytes::from([0x60, 0x01, 0x60, 0x02, 0x01]));

        // builder.create_contract(address, U256::ZERO, 0, bytecode);
        // let task = builder.build();
        // let changeset = &task.change_sets[0];
        // assert_eq!(changeset.op_list.len(), 1);
        // assert_eq!(changeset.op_list[0].op_type, OP_CREATE); // OP_CREATE
    }

    #[test]
    fn test_storage_operations() {
        let mut builder = ChangesetBuilder::new();
        let address = Address::from([3u8; 20]);
        let slot = U256::from(123);
        let value = U256::from(456);
        let dust = 1u128 << 48; // Minimum dust value

        // Create storage
        builder.create_storage(address, slot, value, dust);
        let task = builder.build();
        let changeset = &task.change_sets[0];
        assert_eq!(changeset.op_list.len(), 1);
        assert_eq!(changeset.op_list[0].op_type, OP_CREATE); // OP_CREATE

        // Update storage
        let mut builder = ChangesetBuilder::new();
        builder.update_storage(address, slot, value * U256::from(2), dust);
        let task = builder.build();
        let changeset = &task.change_sets[0];
        assert_eq!(changeset.op_list.len(), 1);
        assert_eq!(changeset.op_list[0].op_type, OP_WRITE); // OP_WRITE

        // Clear storage
        let mut builder = ChangesetBuilder::new();
        builder.clear_storage(address, slot);
        let task = builder.build();
        let changeset = &task.change_sets[0];
        assert_eq!(changeset.op_list.len(), 1);
        assert_eq!(changeset.op_list[0].op_type, OP_DELETE); // OP_DELETE
    }

    #[test]
    fn test_chaining() {
        let mut builder = ChangesetBuilder::new();
        let address = Address::from([4u8; 20]);
        let slot = U256::from(123);
        let value = U256::from(456);
        let dust = 1u128 << 48;

        builder
            .create_eoa(address, U256::from(1000000), 0)
            .create_storage(address, slot, value, dust)
            .update_storage(address, slot, value * U256::from(2), dust)
            .clear_storage(address, slot);

        let task = builder.build();
        let changeset = &task.change_sets[0];
        assert_eq!(changeset.op_list.len(), 4);
    }
}
