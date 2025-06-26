use revm::context::TxEnv;
use std::error::Error;

/// Trait for implementing execution functionality.
///
/// This trait defines the core interface for executing operations with
/// specified input and output types. The transaction is passed separately
/// to allow for better separation of concerns and reuse of witness data.
///
/// # Type Parameters
/// - `InputType`: The type of input the executor accepts
/// - `OutputType`: The type of output the executor produces
/// - `Error`: The error type that can occur during execution
///
/// # Examples
///
/// ```rust
/// use exepipe_common::executor::Executor;
/// use revm::context::TxEnv;
///
/// struct MyExecutor;
///
/// impl Executor for MyExecutor {
///     type InputType = StateData;
///     type OutputType = ExecutionResult;
///     type Error = std::io::Error;
///
///     fn execute(&self, input: Self::InputType, tx: &TxEnv) -> Result<Self::OutputType, Self::Error> {
///         // Implementation here
///         Ok(ExecutionResult::default())
///     }
/// }
/// ```
pub trait Executor {
    /// The type of input this executor accepts
    type InputType;

    /// The type of output this executor produces
    type OutputType;

    /// The type of error that can occur during execution
    type Error: Error;

    /// Execute the operation with the given input and transaction.
    ///
    /// # Arguments
    /// * `input` - The input data to process
    /// * `tx` - The transaction environment
    ///
    /// # Returns
    /// * `Ok(OutputType)` - The successful execution result
    /// * `Err(Error)` - The error that occurred during execution
    ///
    /// # Errors
    /// Returns an error if the execution fails for any reason.
    fn execute(&self, input: Self::InputType, tx: &TxEnv) -> Result<Self::OutputType, Self::Error>;
}
