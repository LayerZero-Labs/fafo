// Main file for benchmark sendeth.
// 4 sub-commands
// - init-db (slow) (token: native/erc20)
// - prepare-workload (fast) (contention parameters)
//      - ratio
//      - skew
//      - model (simple, barabasi-albert)
// - replay-workload (fast) (token: native/erc20)
use bench_sendeth::common::generators;
use bench_sendeth::common::{cli, def, init, utils};
use chrono::Utc;
use clap::Parser;
use log::info;
use qmdb::{config::Config, AdsWrap};
use serde_json::json;
use std::{path::Path, thread, time::Instant};

#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/*
    Each block creates 1 million (2^20) accounts, and 16 billion accounts can be created in 16,384 blocks. After running for 1024 more blocks, 1/16 of all accounts are selected to participate in transfer transactions.

    The selection method is as follows:
    The accounts are grouped, 16 accounts per group.
    One LFSR (Linear Feedback Shift Register) sequence randomly selects a group.
    Another LFSR sequence randomly selects one account from the chosen group.

    During transfers:
    Each block contains 2^19 transactions.
    Each transaction involves a sender and a receiver, modifying a total of 2^20 accounts.
    A transfer transaction requires four 6-byte integers (sender/receiver IDs and offsets), totaling 24 bytes per transaction.
    Each block needs to read/write 12MB of such integers.
*/

fn init_db(args: &cli::BenchmarkCli, token_setup: &def::TokenSetup) {
    let mut config: Config = Config::from_dir(&token_setup.get_data_dir(&args.db_dir));
    config.bypass_prefetcher = false;
    if args.workload == "eth" {
        let _ = init::init_accounts(&config, token_setup, false);
    } else if args.workload == "token" {
        let _ = init::init_accounts(&config, token_setup, true);
    } else {
        panic!("Invalid workload: {}", args.workload);
    }
}

fn prepare_workload(
    db_dir: &str,
    token_setup: &def::TokenSetup,
    generator_name: &str,
    contention_degree: f64,
    skew: f64,
    txns_json: &str,
    txn_filter: &str,
    replay_cycle: bool,
) {
    let mut config: Config = Config::from_dir(&token_setup.get_data_dir(db_dir));
    config.bypass_prefetcher = false;
    let mut generator: Box<dyn generators::Generator> = match generator_name {
        "none" => Box::new(generators::NoContentionGenerator::new(token_setup)),
        "simple" => Box::new(generators::SimpleContentionGenerator::new(
            contention_degree,
            skew,
            token_setup,
        )),
        "static" => Box::new(generators::StaticSimpleContentionGenerator::new(
            contention_degree,
            token_setup,
        )),
        "hotn" => Box::new(generators::HotNContentionGenerator::new(
            contention_degree,
            skew as u64,
            token_setup,
        )),
        "replay" => {
            let kind = if txn_filter.eq_ignore_ascii_case("eth") {
                generators::TxnFilterKind::Eth
            } else {
                generators::TxnFilterKind::Erc20
            };
            Box::new(generators::ReplayTransferGenerator::new(
                token_setup,
                txns_json,
                kind,
                replay_cycle,
            ))
        }
        _ => panic!("Invalid generator: {}", generator_name),
    };
    let gen_name = generator.get_name();

    init::init_blocks_workload(
        token_setup,
        &mut *generator,
        &format!(
            "{}/blocks_{}.dat",
            token_setup.get_data_dir(db_dir),
            gen_name
        ),
    );
}

fn replay_workload(
    args: &cli::BenchmarkCli,
    blocks_workload_filename: String,
    token_setup: &def::TokenSetup,
    metrics_recorder: &utils::MetricsRecorder,
) {
    info!("Starting benchmark with workload: {}", args.workload);
    let ref_dir = token_setup.get_data_dir(&args.db_dir);
    let run_dir = token_setup.get_data_dir(&args.db_dir) + "_run";
    info!("Resetting directory: {}", run_dir);
    utils::reset_dir(&ref_dir, &run_dir);

    info!("Initializing ADS...");
    let mut config: Config = Config::from_dir(&run_dir);
    config.bypass_prefetcher = true;
    let ads_init_start = Instant::now();
    let ads = AdsWrap::new(&config);
    info!("ADS initialized in {:.1?}", ads_init_start.elapsed());
    thread::sleep(std::time::Duration::from_millis(100));

    info!("Starting benchmark...");
    let start = Instant::now();
    bench_sendeth::common::parallel_transfer::parallel_transfer(
        &run_dir,
        ads,
        token_setup,
        &blocks_workload_filename,
        args,
    );
    let duration = start.elapsed();
    info!("Time elapsed in parallel_transfer() is: {:.1?}", duration);
    metrics::gauge!("bench-sendeth.replay.duration_s").set(duration.as_secs_f64());

    // Calculate additional metrics based on values
    let values = metrics_recorder.snapshot_values();
    let task_level_parallelism =
        values["framer.tasks_processed"] / values["framer.frames_generated"];
    metrics::gauge!("framer.task_level_parallelism").set(task_level_parallelism);
    let txn_level_parallelism = values["framer.txns_processed"] / values["framer.frames_generated"];
    metrics::gauge!("framer.txn_level_parallelism").set(txn_level_parallelism);
    info!(
        "Framer: TaskLevelParallelism={:.1} = {} tasks / {} frames",
        task_level_parallelism, values["framer.tasks_processed"], values["framer.frames_generated"]
    );
    info!(
        "Framer: TxnLevelParallelism= {:.1} = {} txns  / {} frames",
        txn_level_parallelism, values["framer.txns_processed"], values["framer.frames_generated"]
    );
    assert!(values["framer.txns_processed"] == token_setup.transfer_txn_count as f64);
}

fn main() {
    utils::init_logging("info");
    let run_start_ts = Utc::now();
    let metrics_recorder = utils::MetricsRecorder::new();
    let args = cli::BenchmarkCli::parse();
    if args.workload != "token" && args.workload != "eth" {
        panic!("Invalid workload: {}", args.workload);
    }
    let token_setup = def::get_benchmark_constants(&args);

    let cmd: String = match &args.command {
        Some(cli::Command::InitDb) => {
            init_db(&args, &token_setup);
            "init-db".to_string()
        }
        Some(cli::Command::ReplayWorkload {
            blocks_workload_filename,
        }) => {
            if !Path::new(blocks_workload_filename).exists() {
                panic!(
                    "Blocks workload file does not exist: {}",
                    blocks_workload_filename
                );
            }
            replay_workload(
                &args,
                blocks_workload_filename.clone(),
                &token_setup,
                &metrics_recorder,
            );
            format!(
                "replay_{}",
                Path::new(blocks_workload_filename)
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .trim_end_matches(".dat")
            )
        }
        Some(cli::Command::PrepareWorkload {
            generator,
            contention_degree,
            skew,
            txns_json,
            txn_filter: tx_filter,
            replay_cycle,
        }) => {
            prepare_workload(
                &args.db_dir,
                &token_setup,
                generator,
                *contention_degree,
                *skew,
                txns_json,
                tx_filter,
                *replay_cycle,
            );
            "prepare-workload".to_string()
        }
        None => {
            panic!("No command provided: valid commands are init-db, prepare-workload, replay-workload. Use --help to see all options.");
        }
    };
    let metrics_filename = if !args.output.is_empty() {
        args.output.clone()
    } else {
        let run_dir = token_setup.get_data_dir(&args.db_dir);
        format!("{}/metrics/{}.json", run_dir, cmd)
    };
    let run_end_ts = Utc::now();
    let duration_secs = (run_end_ts - run_start_ts).num_milliseconds() as f64 / 1000.0;

    let metrics_data = metrics_recorder.snapshot_metrics();

    let output_json = json!({
        "cli": args.clone(),
        "token_setup": &token_setup,
        "command": std::env::args().collect::<Vec<_>>().join(" "),
        "timestamps": {
            "start": run_start_ts.to_rfc3339(),
            "end": run_end_ts.to_rfc3339(),
            "duration_seconds": duration_secs
        },
        "entry_count": token_setup.entry_count,
        "transfer_txn_count": token_setup.transfer_txn_count,
        "metrics": metrics_data
    });

    info!("Dumping metrics to {}", metrics_filename);
    std::fs::create_dir_all(Path::new(&metrics_filename).parent().unwrap()).unwrap();
    std::fs::write(
        &metrics_filename,
        serde_json::to_string_pretty(&output_json).unwrap(),
    )
    .unwrap();
}
