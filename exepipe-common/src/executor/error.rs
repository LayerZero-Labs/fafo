use revm::{
    context::{result::EVMError, DBErrorMarker},
    primitives::{Address, U256},
};

#[derive(thiserror::Error, Debug)]
pub enum DBError {
    #[error("DBError: Account {0} is not in access set")]
    AccountNotIncluded(Address), // tx will be dropped

    #[error("DBError: Slot {addr}/{index} is not in access set")]
    SlotNotIncluded { addr: Address, index: U256 }, // tx will be dropped

    #[error("DBError: Other error: {0}")]
    Other(#[from] anyhow::Error),
}
impl DBErrorMarker for DBError {}

#[derive(thiserror::Error, Debug)]
pub enum ExecutionError {
    #[error("Execution: Caller {0} not found")]
    CallerNotFound(Address), // tx will be dropped

    #[error("Execution: Read account {0} is not in access set")]
    ReadAccountNotIncluded(Address), // tx will be dropped

    #[error("Execution: Read slot {addr}/{index} is not in access set")]
    ReadSlotNotIncluded { addr: Address, index: U256 }, // tx will be dropped

    #[error("Execution: Writed account {0} is not in access set")]
    WritedAccountNotIncluded(Address), // tx will be dropped

    #[error("Execution: Writed slot {addr}/{index} is not in access set")]
    WritedSlotNotIncluded { addr: Address, index: U256 }, // tx will be dropped

    #[error("Execution: Nonce does not match {0} {1}")]
    NonceMismatch(u64, u64), // tx will be dropped

    #[error("InsufficientBalanceForCall: balance/required = {balance}/{required}")]
    InsufficientBalanceForCall { balance: U256, required: U256 }, // tx will be dropped

    #[error("DustFeeExceedsRemainingGas: remaining/required = {remaining}/{required}")]
    DustFeeExceedsRemainingGas { remaining: u128, required: u128 }, // tx will be dropped

    #[error("Execution: EVM Error {0}")]
    EVMError(#[from] EVMError<DBError>),

    #[error("Execution: Other error: {0}")]
    Other(#[from] anyhow::Error),
}
