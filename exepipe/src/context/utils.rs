use crate::{
    dispatcher::def::TX_IN_TASK,
    exetask::{ExeTask, Tx},
    simulator::simulator::SIM_SHARD_COUNT,
};
use byteorder::{BigEndian, ByteOrder};
use codedb::CodeDB;
use crossbeam::channel::Sender;
use qmdb::ADS;
use revm::{
    context::{transaction::AuthorizationTr, Transaction},
    primitives::Address,
};
use smallvec::SmallVec;
use std::{collections::HashSet, sync::Arc};

use super::block_context::UnlockTask;

pub fn warmup_task<T: ADS>(height: i64, task: &ExeTask, ads: &T, code_db: &Arc<CodeDB>) {
    for tx in &task.tx_list {
        let crw_sets = tx.crw_sets.as_ref().unwrap();
        crw_sets.ca_read_hashes.iter().for_each(|short_hash| {
            code_db.warmup(short_hash);
        });
        crw_sets.ca_write_hashes.iter().for_each(|short_hash| {
            code_db.warmup(short_hash);
        });
    }

    task.access_set.rdo_set.iter().for_each(|short_hash| {
        ads.warmup(height, short_hash);
    });
    task.access_set.rnw_set.iter().for_each(|short_hash| {
        ads.warmup(height, short_hash);
    });
}

pub fn address_suffix_u64(addr: &Address) -> u64 {
    BigEndian::read_u64(&addr[12..20])
}

pub fn extract_authorities(tx: &Tx) -> Vec<u64> {
    tx.env
        .authorization_list()
        .filter_map(|authorization| {
            authorization
                .authority()
                .map(|addr| address_suffix_u64(&addr))
        })
        .collect()
}

pub fn unlock_task(unlock_senders: &Arc<Vec<Sender<UnlockTask>>>, task: &ExeTask) {
    let mut senders = HashSet::with_capacity(TX_IN_TASK);
    let mut authorities: HashSet<u64> = HashSet::with_capacity(TX_IN_TASK);
    for tx in &task.tx_list {
        let authority_list = extract_authorities(tx);
        authorities.extend(authority_list.iter());
        let last64 = address_suffix_u64(&tx.env.caller);
        senders.insert(last64);
    }
    let senders: SmallVec<[u64; TX_IN_TASK]> = senders.into_iter().collect();
    let authorities: SmallVec<[u64; TX_IN_TASK]> = authorities.into_iter().collect();
    let last64 = senders[0];
    let idx = last64 as usize % SIM_SHARD_COUNT;
    let _ = unlock_senders[idx].send((senders, authorities));
}
