use revm::{
    context::{
        result::{EVMError, HaltReason, InvalidTransaction, ResultAndState},
        Block, CfgEnv, ContextTr, DBErrorMarker, Evm, JournalOutput, JournalTr, Transaction,
    },
    handler::{
        instructions::{EthInstructions, InstructionProvider},
        EthFrame, EthPrecompiles, EvmTr, FrameResult, Handler, PrecompileProvider,
    },
    interpreter::{interpreter::EthInterpreter, InterpreterResult},
    Context, Database, MainBuilder, MainContext,
};

pub struct MpexHandler<EVM> {
    pub _phantom: core::marker::PhantomData<EVM>,
}

impl<EVM> Default for MpexHandler<EVM> {
    fn default() -> Self {
        Self {
            _phantom: core::marker::PhantomData,
        }
    }
}

impl<EVM> Handler for MpexHandler<EVM>
where
    EVM: EvmTr<
        Context: ContextTr<Journal: JournalTr<FinalOutput = JournalOutput>>,
        Precompiles: PrecompileProvider<EVM::Context, Output = InterpreterResult>,
        Instructions: InstructionProvider<
            Context = EVM::Context,
            InterpreterTypes = EthInterpreter,
        >,
    >,
{
    type Evm = EVM;
    type Error = EVMError<<<EVM::Context as ContextTr>::Db as Database>::Error, InvalidTransaction>;
    type Frame = EthFrame<
        EVM,
        EVMError<<<EVM::Context as ContextTr>::Db as Database>::Error, InvalidTransaction>,
        <EVM::Instructions as InstructionProvider>::InterpreterTypes,
    >;
    type HaltReason = HaltReason;

    // TODO: Implement the following methods
    // fn validate_initial_tx_gas(
    //     &self,
    //     evm: &Self::Evm,
    // ) -> Result<revm::interpreter::InitialAndFloorGas, Self::Error> {
    // }

    fn reward_beneficiary(
        &self,
        _evm: &mut Self::Evm,
        _exec_result: &mut FrameResult,
    ) -> Result<(), Self::Error> {
        // Skip beneficiary reward
        Ok(())
    }
}

fn _run_tx<'a, 'b, OB: Block, TX: Transaction, DB: Database>(
    block: &'a OB,
    tx: &'b TX,
    db: DB,
    disable_nonce_check: bool,
) -> (
    Evm<
        Context<&'a OB, &'b TX, CfgEnv, DB>,
        (),
        EthInstructions<EthInterpreter, Context<&'a OB, &'b TX, CfgEnv, DB>>,
        EthPrecompiles,
    >,
    Result<ResultAndState, EVMError<DB::Error>>,
) {
    let mut cfg = CfgEnv::default();
    cfg.disable_nonce_check = disable_nonce_check;
    let mut evm = Context::mainnet()
        .with_block(block)
        .with_db(db)
        .with_tx(tx)
        .with_cfg(cfg)
        .build_mainnet();
    let r = MpexHandler::default().run(&mut evm);
    (evm, r)
}

pub fn run_tx<'a, 'b, OB: Block, TX: Transaction, DB: Database>(
    block: &'a OB,
    tx: &'b TX,
    db: DB,
) -> (
    Evm<
        Context<&'a OB, &'b TX, CfgEnv, DB>,
        (),
        EthInstructions<EthInterpreter, Context<&'a OB, &'b TX, CfgEnv, DB>>,
        EthPrecompiles,
    >,
    Result<ResultAndState, EVMError<DB::Error>>,
) {
    _run_tx(block, tx, db, true)
}

pub fn run_tx_with_disable_nonce_check<'a, 'b, OB: Block, TX: Transaction, DB: Database>(
    block: &'a OB,
    tx: &'b TX,
    db: DB,
) -> (
    Evm<
        Context<&'a OB, &'b TX, CfgEnv, DB>,
        (),
        EthInstructions<EthInterpreter, Context<&'a OB, &'b TX, CfgEnv, DB>>,
        EthPrecompiles,
    >,
    Result<ResultAndState, EVMError<DB::Error>>,
) {
    _run_tx(block, tx, db, true)
}

#[cfg(test)]
mod tests {
    use revm::{
        context::{BlockEnv, TxEnv},
        handler::Handler,
        Context, MainBuilder, MainContext,
    };

    use super::MpexHandler;

    #[test]
    fn test_mpex_handler() {
        // let mut evm = Context::mainnet();
        // MpexHandler::default().run(&mut evm);

        let mut evm = Context::mainnet()
            .with_block(BlockEnv::default())
            .with_tx(TxEnv::default())
            .build_mainnet();
        // let out = evm.replay();
        let _res = MpexHandler::default().run(&mut evm);
    }
}

#[derive(Debug)]
pub struct SimpleDBError {
    error: anyhow::Error,
}

impl SimpleDBError {
    pub fn new(error: anyhow::Error) -> Self {
        Self { error }
    }
}

impl std::fmt::Display for SimpleDBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DBError: {}", self.error)
    }
}
impl std::error::Error for SimpleDBError {}
impl DBErrorMarker for SimpleDBError {}
