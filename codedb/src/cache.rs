use dashmap::DashMap;
use exepipe_common::{access_set::addr_to_short_hash, def::SHORT_HASH_LEN_FOR_TASK_ACCESS_SET};
use revm::{
    primitives::Address,
    state::Bytecode,
};
use smallvec::SmallVec;
use std::{
    sync::{atomic::AtomicUsize, Arc},
    thread,
};

type CodeSmallVec = SmallVec<[(Address, Bytecode); 1]>;

pub struct CodeCache {
    map: Arc<DashMap<[u8; SHORT_HASH_LEN_FOR_TASK_ACCESS_SET], (u8, CodeSmallVec)>>,
    logical_height: AtomicUsize,
    retention: u8,
}

impl CodeCache {
    pub fn new(retention: u8) -> Self {
        assert!(retention >= 2, "Retention must be greater than 2");
        Self {
            map: Arc::new(DashMap::new()),
            logical_height: AtomicUsize::new(0),
            retention,
        }
    }

    pub fn insert(&self, addr: &Address, byte_code: Bytecode) {
        let lg_height = self.get_logical_height();
        let short_hash = addr_to_short_hash(addr);
        self.map
            .entry(short_hash)
            .and_modify(|(height, vec)| {
                *height = lg_height;
                if vec.iter().any(|(a, _)| a == addr) {
                    return;
                }
                vec.push((*addr, byte_code.clone()));
            })
            .or_insert_with(|| {
                let mut vec = SmallVec::new();
                vec.push((*addr, byte_code.clone()));
                (lg_height, vec)
            });
    }

    pub fn get(&self, addr: &Address) -> Option<Bytecode> {
        let short_hash = addr_to_short_hash(addr);
        self.map.get_mut(&short_hash).and_then(|mut entry| {
            entry.0 = self.get_logical_height();
            entry
                .1
                .iter()
                .find(|(a, _)| a == addr)
                .map(|(_, b)| b.clone())
        })
    }

    pub fn contains(&self, short_hash: &[u8; SHORT_HASH_LEN_FOR_TASK_ACCESS_SET]) -> bool {
        self.map.contains_key(short_hash)
    }

    pub fn tick(&self) {
        self.logical_height
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.spawn_cleanup_task();
    }

    fn spawn_cleanup_task(&self) {
        let logical_height = self.get_logical_height();
        let retention = self.retention;
        let map = Arc::clone(&self.map);
        thread::spawn(move || {
            let per_remove_count = 256;
            let mut keys_to_remove = Vec::with_capacity(per_remove_count);
            loop {
                map.iter()
                    .filter(|entry| logical_height.wrapping_sub(entry.value().0) >= retention)
                    .take(per_remove_count)
                    .for_each(|entry| keys_to_remove.push(*entry.key()));

                for key in &keys_to_remove {
                    map.remove(key);
                }
                if keys_to_remove.len() < per_remove_count {
                    break;
                }
                keys_to_remove.clear();
            }
        });
    }

    fn get_logical_height(&self) -> u8 {
        (self
            .logical_height
            .load(std::sync::atomic::Ordering::SeqCst)
            % u8::MAX as usize) as u8
    }
}
