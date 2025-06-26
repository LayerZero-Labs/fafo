// This file is used by send.rs.
use super::cli;
use super::def::erc_address_storage;
use super::def::{CA_ADDR, ERC20_HEX};

use super::def::TokenSetup;
use exepipe::test_helper::encode_ec20_transfer_data;
use exepipe::{
    exetask::{ExeTask, Tx},
    Block, ExePipe,
};
use exepipe_common::access_set::{extract_short_hash, CrwSets};
use exepipe_common::statecache::StateCache;
use exepipe_common::utils::join_address_index;
use log::{debug, info};
use qmdb::entryfile::EntryCache;
use qmdb::utils::hasher;
use qmdb::AdsWrap;
use revm::context::{BlockEnv, TxEnv};
use revm::primitives::hex::FromHex;
use revm::primitives::Bytes;
use revm::primitives::{alloy_primitives::U160, Address, TxKind, U256};
use revm::state::Bytecode;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::time::Instant;
use std::{fs::File, os::unix::fs::FileExt, sync::Arc};

use indicatif::{ProgressBar, ProgressStyle};

const TX_IN_TASK: usize = 4;

fn extract_tx_info(bz: &[u8], pos: usize) -> [u64; 4] {
    let mut res = [0u64; 4];
    for i in 0..4 {
        let j = pos + i * 6;
        let mut buf = [0u8; 8];
        buf[..6].copy_from_slice(&bz[j..j + 6]);
        res[i] = u64::from_le_bytes(buf);
    }
    res
}

const BYTES_PER_TXN: usize = 24;

#[inline(always)]
fn executor_loop(
    task_receiver: Receiver<(Vec<Vec<Box<Tx>>>, Block)>,
    done_sender: SyncSender<bool>,
    pipe: &mut ExePipe,
    start_height: i64,
    pb: &ProgressBar,
) {
    let mut _height = start_height;
    // let start = Instant::now();
    let mut pre_state_cache = Arc::new(StateCache::new());
    loop {
        match task_receiver.recv() {
            Err(_) => {
                break;
            }
            Ok((task_in, blk)) => {
                _height += 1;
                pb.inc(1);
                // debug!("Got Height={} Elapse={:?}", height, start.elapsed());
                pre_state_cache = pipe.run_block_with_sim(task_in, blk, pre_state_cache);
                //pipe.run_block(task_in, blk, pre_state_cache);
                //info!("Done Height={} Elapse={:?}", height, start.elapsed());
            }
        };
    }
    pb.finish_with_message("Finished executor_loop");
    pipe.flush();
    done_sender.send(true).unwrap();
}

#[inline(always)]
fn prepare_txn(
    task_in: &mut Vec<Vec<Box<Tx>>>,
    token_setup: &TokenSetup,
    blk_file: &File,
    file_pos: &mut u64,
    height: &mut i64,
    is_eth: bool,
) -> bool {
    *height += 1;
    // Read data out of blocks.dat
    let mut v = Vec::with_capacity(token_setup.bytes_in_vec);
    v.resize(token_setup.bytes_in_vec, 0u8);
    match blk_file.read_at(&mut v[..], *file_pos) {
        Ok(_) => {
            *file_pos += token_setup.bytes_in_vec as u64;
        }
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return false;
        }
        Err(e) => {
            log::error!("Failed to read block data: {}", e);
            return false;
        }
    }
    let v = Arc::new(v);

    // let shared_index = Arc::new(AtomicUsize::new(0));

    //for preloader in &mut preloaders {
    //	let shared_index = shared_index.clone();
    //	let v = v.clone();
    //	let cache = cache.clone();
    //    s.spawn(move || {
    //        preloader.start_new_block(cache);
    //        preloader.uring_loop(&v, &shared_index);
    //    });
    //}

    // NEW TO SEND_TOKEN
    let no_rwsets_pct = if is_eth { 100 } else { 1 }; // rwsets_pct/100 non rwsets
                                                      // END NEW TO SEND_TOKEN
    let mut tx_vec = Vec::with_capacity(TX_IN_TASK);
    for i in 0..token_setup.txn_per_transfer_block {
        let tx_info = extract_tx_info(&v[..], i * BYTES_PER_TXN);
        let mut tx_env = TxEnv::default();
        tx_env.nonce = 0;
        // Sender
        tx_env.caller = Address::from(U160::from(tx_info[0]));
        let recipient = Address::from(U160::from(tx_info[2]));
        let mut crw_sets = None;
        if is_eth {
            // Native ETH transfer
            tx_env.value = U256::from(1);
            tx_env.kind = TxKind::Call(recipient);
        } else {
            // ERC20 transfer
            tx_env.value = U256::from(0);
            tx_env.kind = TxKind::Call(CA_ADDR);
            tx_env.data = Bytes::from_hex(encode_ec20_transfer_data(&recipient, 1)).unwrap();
            if no_rwsets_pct == 0 || i % (100 / no_rwsets_pct) != 0 {
                crw_sets = Some(CrwSets::new(
                    vec![],
                    vec![],
                    vec![],
                    vec![
                        extract_short_hash(&hasher::hash(join_address_index(
                            &CA_ADDR,
                            &erc_address_storage(tx_env.caller),
                        ))),
                        extract_short_hash(&hasher::hash(join_address_index(
                            &CA_ADDR,
                            &erc_address_storage(recipient),
                        ))),
                    ],
                ));
            }
        }
        let tx = Tx {
            tx_hash: Default::default(),
            crw_sets,
            env: tx_env,
        };
        tx_vec.push(Box::new(tx));
        if tx_vec.len() == TX_IN_TASK {
            let mut v = Vec::with_capacity(TX_IN_TASK);
            std::mem::swap(&mut tx_vec, &mut v);
            // let task = Box::new(ExeTask::new(v));
            task_in.push(v);
        }
    }
    true
}

pub fn parallel_transfer(
    dir: &str,
    ads: AdsWrap<ExeTask>,
    token_setup: &TokenSetup,
    blocks_workload_filename: &str,
    args: &cli::BenchmarkCli,
) {
    // 1. Initaliation of benchmark

    let start_height = ads.get_metadb().read().get_curr_height();
    debug!("start_height={}", start_height);

    let mut pipe = ExePipe::new(ads, dir);

    let blk_file = File::options()
        .read(true)
        .write(true)
        .open(blocks_workload_filename)
        .unwrap();
    // let lfsr_pair = token_setup.create_lfsr_pair(8);

    let mut file_pos = 0;
    let mut height = start_height;

    // let entry_files: Vec<Arc<EntryFile>> = ads.get_entry_files();
    // let mut preloaders = Vec::with_capacity(URING_COUNT);
    // for i in 0..URING_COUNT {
    //     preloaders.push(Preloader::new(entry_files.clone(), i));
    // }

    // NEW TO SEND_TOKEN
    let is_eth = args.workload == "eth";
    if !is_eth {
        let bc_bytes = Bytes::from_hex(ERC20_HEX).unwrap();
        let bytecode = Bytecode::new_raw(bc_bytes);
        let _code_hash = bytecode.hash_slow();
        pipe.insert_code(&CA_ADDR, bytecode);
    }
    // END NEW TO SEND_TOKEN

    let mpb = indicatif::MultiProgress::new();
    let sty = ProgressStyle::with_template(
        "[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}",
    )
    .unwrap()
    .progress_chars("##-");
    let pb_generate = mpb.add(indicatif::ProgressBar::new(
        token_setup.transfer_block_count as u64,
    ));
    pb_generate.set_style(sty.clone());
    let pb_exec = mpb.add(indicatif::ProgressBar::new(
        token_setup.transfer_block_count as u64,
    ));
    pb_exec.set_style(sty.clone());

    // 2. Spawn a thread for pipeline (pre_state_cache?)
    let (task_sender, task_receiver) = sync_channel(4);
    let (done_sender, done_receiver) = sync_channel(1);
    std::thread::spawn(move || {
        executor_loop(
            task_receiver,
            done_sender,
            &mut pipe,
            start_height,
            &pb_exec,
        )
    });

    // 3. Main loop of benchmark
    let start = Instant::now();
    let mut should_break = false;
    for _ in 0..token_setup.transfer_block_count {
        let _cache = Arc::new(EntryCache::new());
        let mut task_in = Vec::with_capacity(token_setup.txn_per_transfer_block);
        std::thread::scope(|_s| {
            if !prepare_txn(
                &mut task_in,
                token_setup,
                &blk_file,
                &mut file_pos,
                &mut height,
                is_eth,
            ) {
                should_break = true;
            }
        });

        if should_break || task_in.is_empty() {
            break;
        }

        let mut block_env = BlockEnv::default();
        block_env.number = height as u64;
        let blk = Block::new(
            height,
            Default::default(),
            Default::default(),
            U256::ZERO,
            0,
            Default::default(),
            Default::default(),
            block_env,
        );
        // debug!(
        //     "Now send height={} {:?} task_in.len={}",
        //     height,
        //     start.elapsed(),
        //     task_in.len()
        // );
        pb_generate.inc(1);
        task_sender.send((task_in, blk)).unwrap();
    }
    pb_generate.finish_with_message("Finished generating blocks");
    drop(task_sender);
    done_receiver.recv().unwrap();
    mpb.clear().unwrap();
    let txns_per_sec = token_setup.transfer_txn_count as f64 / start.elapsed().as_secs_f64();
    info!(
        "Replayed {} transfers in {:?}, {:.1} txns/s",
        token_setup.transfer_txn_count,
        start.elapsed(),
        txns_per_sec,
    );
    metrics::gauge!("bench-sendeth.replay.txns_per_sec").set(txns_per_sec);
}
