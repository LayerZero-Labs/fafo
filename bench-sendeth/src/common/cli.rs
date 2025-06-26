use clap::{Parser, Subcommand};
use serde::Serialize;

#[derive(Subcommand, Debug, Serialize, Clone)]
pub enum Command {
    InitDb,
    PrepareWorkload {
        #[arg(long, default_value = "none")]
        generator: String,

        #[arg(long, default_value = "0.1")]
        contention_degree: f64,

        #[arg(long, default_value = "0.0000005")]
        skew: f64,

        /// Path to the transaction dump in ND-JSON format (used by replay generator)
        #[arg(long, default_value = "")]
        txns_json: String,

        /// Which transfers to extract from the dump: "eth" or "erc20" (default)
        #[arg(long = "txn-filter", default_value = "erc20")]
        txn_filter: String,

        /// For replay generator, whether to cycle through the dump
        #[arg(long, default_value = "true")]
        replay_cycle: bool,
    },
    ReplayWorkload {
        #[arg(long, default_value = "blocks.dat")]
        blocks_workload_filename: String,
    },
}

#[derive(Parser, Debug, Serialize, Clone)]
pub struct BenchmarkCli {
    #[command(subcommand)]
    pub command: Option<Command>,

    /// Directory to store the database's persistent files
    #[arg(long, default_value = "/tmp/QMDB_bench_sendeth")]
    pub db_dir: String,

    /// Output filename for metrics
    #[arg(long, default_value = "")]
    pub output: String,

    /// Workload. Valid choices are "token" and "eth".
    #[arg(short, long, required = true)]
    pub workload: String,

    // Total bits = count_in_block_bits + create_block_count_bits = 2^bits accounts
    // For a faster run, setting this to 26 would work.
    // TODO: Use entry_count instead.
    #[arg(long, default_value_t = 31)]
    pub entry_count_bits: usize,

    /// If not set, defaults to 18 for token (ERC-20) and 21 for eth (native ETH)
    #[arg(long, default_value_t = 0)]
    pub create_txn_per_block_bits: usize,

    // /// Number of accounts to use for the benchmark
    // #[arg(long, default_value_t = 0)]
    // pub _entry_count: u64,
    /// Number of transactions to send in total during replay-workload
    #[arg(long, default_value_t = 0)]
    pub transfer_txn_count: u64,

    /// Number of transactions per block
    /// Used in prepare-workload and replay-workload
    #[arg(long, default_value_t = 0)]
    pub txn_per_transfer_block: usize,
}
