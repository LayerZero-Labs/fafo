// This file is used by prepare.rs.
use std::fs;
use std::{
    fs::{remove_file, File},
    io::{ErrorKind, Write},
    path::Path,
    sync::Arc,
    time::Instant,
};

use codedb::CodeDB;
use exepipe::exetask::ExeTask;
use exepipe_common::{
    acc_data::encode_account_info, slot_entry_value::SlotEntryValue, utils::join_address_index,
};
use log::{debug, info};
use parking_lot::RwLock;
use qmdb::{
    config::Config,
    tasks::{taskid::join_task_id, TasksManager},
    test_helper::task_builder::TaskBuilder,
    AdsCore, AdsWrap, ADS,
};
use revm::{
    primitives::{alloy_primitives::U160, hex::FromHex, Address, Bytes, U256},
    state::{AccountInfo, Bytecode},
};

use super::def::{erc_address_storage, TokenSetup, CA_ADDR, ERC20_HEX};
use super::generators;

pub fn recreate_file(file_path: &str) -> File {
    if let Err(e) = remove_file(file_path) {
        if e.kind() != ErrorKind::NotFound {
            eprintln!("delete file failed: {}", e);
            return File::create(file_path).unwrap();
        }
    }
    File::create(file_path).unwrap()
}

pub fn init_accounts(
    config: &Config,
    token_setup: &TokenSetup,
    need_slot: bool,
) -> AdsWrap<ExeTask> {
    // initialize accounts
    // Create directory if it doesn't exist
    if !Path::new(&config.dir).exists() {
        fs::create_dir_all(&config.dir).unwrap();
    }
    AdsCore::init_dir(config);
    let mut ads: AdsWrap<ExeTask> = AdsWrap::new(config);

    let mut height = 0;
    let mut acc = 0u64;

    let start_time = Instant::now();
    let progress_style = indicatif::ProgressStyle::with_template(
        "[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}",
    )
    .unwrap()
    .progress_chars("##-");
    let pb = indicatif::ProgressBar::new(
        (token_setup.create_block_count * token_setup.count_in_block) as u64,
    );
    pb.set_style(progress_style.clone());
    for i in 0..token_setup.create_block_count {
        pb.inc(token_setup.count_in_block as u64);
        height += 1;

        if token_setup.count_in_block % 64 != 0 {
            panic!("COUNT_IN_BLOCK must be a multiple of 64");
        }
        let task_count = token_setup.count_in_block / 64;
        let mut tasks = Vec::with_capacity(task_count);

        for task_index in 0..task_count {
            let mut tb = TaskBuilder::new();
            for tx_index in 0..64 {
                acc += 1;
                let addr = Address::from(U160::from(acc));
                let info = AccountInfo::from_balance(U256::from(1000_000u64));
                let v = encode_account_info(&info);
                tb.create(&addr[..], v.as_slice());

                // add account storage
                if need_slot {
                    let storage_index = erc_address_storage(addr);
                    let addr_idx = join_address_index(&CA_ADDR, &storage_index);
                    let amount = U256::from(1000_000u64);
                    let mut value = amount.to_be_bytes_vec();
                    SlotEntryValue::encode(&mut value, 0);
                    tb.create(&addr_idx[..], &value);
                }

                // ca account
                if i == 0 && tx_index == 0 && task_index == 0 {
                    let bc_bytes = Bytes::from_hex(ERC20_HEX).unwrap();
                    let bytecode = Bytecode::new_raw(bc_bytes);
                    let code_hash = bytecode.hash_slow();
                    let info = AccountInfo {
                        code_hash,
                        code: Some(bytecode.clone()),
                        ..AccountInfo::default()
                    };
                    let v = encode_account_info(&info);
                    tb.create(&CA_ADDR[..], v.as_slice());

                    // add db
                    {
                        let wrbuf_size = 8 * 1024 * 1024; //8MB // TODO:refactor
                        let file_segment_size = 256 * 1024 * 1024; // 256MB
                        let code_db = CodeDB::new(&config.dir, -1, wrbuf_size, file_segment_size);
                        code_db.append(&CA_ADDR, bytecode, 0).unwrap();
                        code_db.flush().unwrap();
                    }
                }
            }
            let cs = tb.build();
            let mut exe_task = ExeTask::new(Vec::with_capacity(0));
            exe_task.set_change_sets(cs.change_sets.clone());
            tasks.push(RwLock::new(Some(exe_task)));
        }

        let tm = Arc::new(TasksManager::new(tasks, task_count));

        ads.start_block(height, tm);
        let shared_ads = ads.get_shared();
        shared_ads.insert_extra_data(height, "4224:0".to_owned());
        for idx in 0..task_count {
            let task_id = join_task_id(height, idx, idx == task_count - 1);
            shared_ads.add_task(task_id);
        }
    }
    pb.finish();
    ads.flush();
    info!(
        ">> Initialized {} accounts in {:?}, {:.1} accounts/s",
        acc,
        start_time.elapsed(),
        acc as f64 / start_time.elapsed().as_secs_f64()
    );
    ads
}

/// Creates a block file for benchmarking transfer operations.
///
/// By default, the file contains 512 blocks, each with 500K transfers.
/// Each transfer record contains a sender account address, receiver account address, and offsets.
pub fn init_blocks_workload(
    token_setup: &TokenSetup,
    generator: &mut dyn generators::Generator,
    file_path: &str,
) {
    let start_time = Instant::now();

    // Initialize output file
    recreate_file(file_path);
    let mut options = File::options();
    let mut blk_file = options.read(true).write(true).open(file_path).unwrap();
    info!("Initialized blocks workload file: {}", file_path);

    let mut bz = Vec::with_capacity(token_setup.bytes_in_vec);
    // let buf = [0u8; 500];

    let sty = indicatif::ProgressStyle::with_template(
        "[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}",
    )
    .unwrap()
    .progress_chars("##-");
    let pb = indicatif::ProgressBar::new(
        (token_setup.transfer_block_count * token_setup.txn_per_transfer_block) as u64,
    );
    pb.set_style(sty.clone());
    let mut debug_max_acc_seen = 0;
    let mut debug_min_acc_seen = u64::MAX;
    let mut txns_generated = 0;
    log::info!(
        "token_setup.transfer_block_count={}, token_setup.txn_per_transfer_block={}",
        token_setup.transfer_block_count,
        token_setup.txn_per_transfer_block
    );
    for _ in 0..token_setup.transfer_block_count {
        bz.clear();
        let accounts = generator.generate(token_setup.txn_per_transfer_block);

        if accounts.len() < token_setup.txn_per_transfer_block {
            log::warn!(
                "Less transactions than requested: if replay, we likely ran out of data. Generated {} / {} txns, {} lateest accounts_len",
                txns_generated, token_setup.transfer_txn_count, accounts.len(),
            );
        }
        if accounts.len() == 0 {
            break;
        }

        // Convert numbers into 24-byte transfer records
        for i in 0..std::cmp::min(token_setup.txn_per_transfer_block, accounts.len()) {
            let (src, dst) = accounts[i];
            for j in 0..2 {
                let bz6 = if j == 0 { src } else { dst };
                if bz6 > debug_max_acc_seen {
                    debug_max_acc_seen = bz6;
                }
                if bz6 < debug_min_acc_seen {
                    debug_min_acc_seen = bz6;
                }
                bz.extend_from_slice(&bz6.to_le_bytes()[..6]);
                // Currently this is hardcoded. TODO: Figure out why.
                let pos = 0i64; //m.get(&acc).unwrap();
                let bz6 = pos.to_le_bytes();
                bz.extend_from_slice(&bz6[..6]);
            }
        }

        txns_generated += accounts.len();
        blk_file.write_all(&bz[..]).unwrap();
        pb.inc(token_setup.txn_per_transfer_block as u64);
        // debug!("blk_file.len={}", blk_file.metadata().unwrap().len() / 12);
    }
    pb.finish();
    debug!(
        "debug_range_acc_seen={}..{}",
        debug_min_acc_seen, debug_max_acc_seen
    );
    let txns_per_sec = token_setup.transfer_block_count as f64
        * token_setup.txn_per_transfer_block as f64
        / start_time.elapsed().as_secs_f64();
    let blocks_per_sec =
        token_setup.transfer_block_count as f64 / start_time.elapsed().as_secs_f64();
    info!(
        ">> Initialized {} blocks ({} txns) in {:?}, {:.2} blocks/s, {:.2} txns/s",
        token_setup.transfer_block_count,
        token_setup.transfer_txn_count,
        start_time.elapsed(),
        blocks_per_sec,
        txns_per_sec,
    );
    metrics::gauge!("bench-sendeth.init.txns_per_sec").set(txns_per_sec);
    metrics::gauge!("bench-sendeth.init.blocks_per_sec").set(blocks_per_sec);
}

pub fn init_blocks_legacy(config: &Config, token_setup: &TokenSetup) {
    let file_path = format!("{}/blocks.dat", config.dir);

    // Create the generator as a mutable value so it can be passed to the workload initializer.
    let mut generator = generators::NoContentionGenerator::new(token_setup);

    init_blocks_workload(token_setup, &mut generator, &file_path);
}

pub fn init_accounts_and_blocks(
    config: &Config,
    token_setup: &TokenSetup,
    need_slot: bool,
) -> AdsWrap<ExeTask> {
    info!("Using QMDB directory: {}", config.dir);

    let ads = init_accounts(config, token_setup, need_slot);
    init_blocks_legacy(config, token_setup);
    ads
}
