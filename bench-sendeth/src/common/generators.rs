// Workload generators for token transfer benchmark.
// Workloads with different degrees of contention.

// SimpleContentionWorkloadGenerator
//     new(hot_txn_ratio, hot_key_ratio/skew - 0.001 [1000 keys in 1M TPS])
//     generate(batch_size)
//         [[acc1, acc2], ..] -- [batch_size; 1]

use log::info;

use super::def::TokenSetup;
use qmdb::utils::hasher;
use qmdb::utils::lfsr::GaloisLfsr;
use serde::Deserialize;
use std::fs::File;
use std::io::{BufRead, BufReader};

const SEED: u64 = 8;

pub trait Generator {
    fn generate(&mut self, batch_size: usize) -> Vec<(u64, u64)>;
    fn get_name(&self) -> String;
}

pub struct SimpleContentionGenerator<'a> {
    /// Probability that a transaction writes to a HOT key  (0.0 – 1.0).
    hot_txn_ratio: f64,
    /// Fraction of the key-space that is considered HOT (0.0 – 1.0).
    hot_key_ratio: f64,
    /// Benchmark parameters (account/key layout).
    token_setup: &'a TokenSetup,
    /// The underlying pseudo-random sequence generator.
    lfsr_pair: (GaloisLfsr, GaloisLfsr),
    /// Size of the key space (inclusive upper bound + 1).
    key_space_size: u64,
    /// Upper bound (exclusive) of the HOT region.
    hot_key_upper_bound: u64,
}

impl<'a> SimpleContentionGenerator<'a> {
    /// Creates a new generator.
    pub fn new(hot_txn_ratio: f64, hot_key_ratio: f64, token_setup: &'a TokenSetup) -> Self {
        assert!(
            (0.0..=1.0).contains(&hot_txn_ratio),
            "hot_txn_ratio must be in [0,1]"
        );
        assert!(
            (0.0..=1.0).contains(&hot_key_ratio),
            "hot_key_ratio must be in [0,1]"
        );

        let key_space_size =
            (token_setup.count_in_block as u64) * (token_setup.create_block_count as u64);
        // Size of the HOT region – round down so that hot_size <= key_space_size.
        let hot_key_upper_bound = ((hot_key_ratio * key_space_size as f64).floor()) as u64;

        let lfsr_pair = token_setup.create_lfsr_pair(8);

        info!(
            "SimpleContentionGenerator(hot_txn_ratio={}, hot_key_ratio={})",
            hot_txn_ratio, hot_key_ratio
        );

        info!(
            "key_space_size={}, hot_key_upper_bound={}",
            key_space_size, hot_key_upper_bound
        );

        Self {
            hot_txn_ratio,
            hot_key_ratio,
            token_setup,
            lfsr_pair,
            key_space_size,
            hot_key_upper_bound,
        }
    }

    /// Draws an unbiased random number in `[0, key_space_size)` using the LFSR.
    #[inline]
    fn next_rand(&mut self) -> u64 {
        // `get_rand` already spans (almost) the entire account range; mod to
        // eliminate the tiny bias introduced by the arithmetic construction.
        self.token_setup.get_rand(&mut self.lfsr_pair) % self.key_space_size
    }

    /// Returns a key from the HOT region `(0, hot_key_upper_bound]`.
    ///
    /// Account index `0` is never created – `init_accounts` starts from `1`.
    /// We therefore shift the generated value by `+1` to guarantee the result
    /// lies in the valid 1-based range.
    #[inline]
    fn get_hot_key(&mut self) -> u64 {
        let min_key = 1;
        if self.hot_key_upper_bound == 0 {
            return min_key; // fallback to the first valid account
        }
        // Range before shift: [0, hot_key_upper_bound - 1]
        // After shift: [min_key, min_key + hot_key_upper_bound - 1]
        (self.next_rand() % self.hot_key_upper_bound) + min_key
    }

    /// Returns a key from the COLD region `(hot_key_upper_bound, key_space_size]`.
    #[inline]
    fn get_cold_key(&mut self) -> u64 {
        let min_key = 1;
        if self.hot_key_upper_bound >= self.key_space_size {
            // Entire key-space is considered HOT: pick any valid account.
            return (self.next_rand() % self.key_space_size) + min_key;
        }
        let cold_range = self.key_space_size - self.hot_key_upper_bound;
        // Produces (hot_key_upper_bound + min_key)..=key_space_size
        (self.next_rand() % cold_range) + self.hot_key_upper_bound + min_key
    }
}

#[inline]
fn check_key(key: u64, key_space_size: u64) {
    assert!(key > 0, "key must be greater than 0");
    assert!(
        key <= key_space_size,
        "src={} > key_space_size={}",
        key,
        key_space_size
    );
}

impl Generator for SimpleContentionGenerator<'_> {
    fn generate(&mut self, batch_size: usize) -> Vec<(u64, u64)> {
        let mut txns = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            let prob = (self.next_rand() as f64) / (self.key_space_size as f64);
            let src = if prob < self.hot_txn_ratio {
                self.get_hot_key()
            } else {
                self.get_cold_key()
            };
            let dst = self.get_cold_key();
            check_key(src, self.key_space_size);
            check_key(dst, self.key_space_size);
            txns.push((src, dst));
        }
        txns
    }

    fn get_name(&self) -> String {
        format!("simple_{}_{}", self.hot_txn_ratio, self.hot_key_ratio)
    }
}

pub struct NoContentionGenerator<'a> {
    token_setup: &'a TokenSetup,
    lfsr_pair: (GaloisLfsr, GaloisLfsr),
}

impl<'a> NoContentionGenerator<'a> {
    pub fn new(token_setup: &'a TokenSetup) -> Self {
        info!("NoContentionGenerator()");
        let lfsr_pair = token_setup.create_lfsr_pair(SEED);
        Self {
            token_setup,
            lfsr_pair,
        }
    }

    #[inline]
    fn next_rand(&mut self) -> u64 {
        self.token_setup.get_rand(&mut self.lfsr_pair)
    }
}

impl Generator for NoContentionGenerator<'_> {
    fn generate(&mut self, batch_size: usize) -> Vec<(u64, u64)> {
        (0..batch_size)
            .map(|_| (self.next_rand(), self.next_rand()))
            .collect()
    }

    fn get_name(&self) -> String {
        "none".to_string()
    }
}

pub struct StaticSimpleContentionGenerator<'a> {
    /// Probability that a generated transaction will be the fixed HOT pair.
    hot_txn_ratio: f64,
    /// Benchmark parameters (account/key layout).
    token_setup: &'a TokenSetup,
    /// The underlying pseudo-random sequence generator.
    lfsr_pair: (GaloisLfsr, GaloisLfsr),
    /// LFSR dedicated to HOT/COLD decision so main sequence remains untouched.
    hot_lfsr: GaloisLfsr,
}

impl<'a> StaticSimpleContentionGenerator<'a> {
    /// Creates a new static-skew generator.
    ///
    /// * `hot_txn_ratio` – fraction of transactions that should be the fixed HOT pair `(256, 257)`.
    pub fn new(hot_txn_ratio: f64, token_setup: &'a TokenSetup) -> Self {
        assert!(
            (0.0..=1.0).contains(&hot_txn_ratio),
            "hot_txn_ratio must be in [0,1]"
        );

        let lfsr_pair = token_setup.create_lfsr_pair(SEED);
        // Use a different seed for the decision LFSR to avoid correlation.
        let hot_lfsr = GaloisLfsr::new(SEED.wrapping_add(1), 63);

        info!(
            "StaticSimpleContentionGenerator(hot_txn_ratio={})",
            hot_txn_ratio
        );

        Self {
            hot_txn_ratio,
            token_setup,
            lfsr_pair,
            hot_lfsr,
        }
    }

    #[inline]
    fn next_rand(&mut self) -> u64 {
        self.token_setup.get_rand(&mut self.lfsr_pair)
    }
}

impl Generator for StaticSimpleContentionGenerator<'_> {
    fn generate(&mut self, batch_size: usize) -> Vec<(u64, u64)> {
        const HOT_SRC: u64 = 256;
        const HOT_DST: u64 = 257;

        let mut txns = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            let rand_pair = (self.next_rand(), self.next_rand());
            let prob = (self.hot_lfsr.next() as f64) / ((1u64 << 63) as f64);
            if prob < self.hot_txn_ratio {
                txns.push((HOT_SRC, HOT_DST));
            } else {
                txns.push(rand_pair);
            }
        }
        txns
    }

    fn get_name(&self) -> String {
        format!("static_{}", self.hot_txn_ratio)
    }
}

/// Selects which type of transfers to extract from the txn dump.
#[derive(Copy, Clone)]
pub enum TxnFilterKind {
    Eth,
    Erc20,
}

/// Columns we expect in each ND-JSON line from the txn dump.
#[derive(Deserialize)]
struct TxnRow {
    #[serde(default)]
    from: String,
    #[serde(default)]
    to: Option<String>,
    #[serde(default)]
    value: Option<String>,
    #[serde(default)]
    input: Option<String>,
}

/// Replay generator that streams pairs from an ND-JSON dump.
pub struct ReplayTransferGenerator<'a> {
    /// We only use the `count_in_block` to decide padding.
    token_setup: &'a TokenSetup,
    reader: BufReader<File>,
    txn_filter: TxnFilterKind,
    txn_count: u64,
    // Whether to cycle through the dump
    cycle: bool,
}

impl<'a> ReplayTransferGenerator<'a> {
    pub fn new(
        token_setup: &'a TokenSetup,
        json_path: &str,
        txn_filter: TxnFilterKind,
        cycle: bool,
    ) -> Self {
        let file = File::open(json_path).expect("Failed to open dump json file");
        info!("ReplayTransferGenerator: reading from {}", json_path);
        info!(
            "Filter: {:?}, Account range: 1..{}",
            match txn_filter {
                TxnFilterKind::Eth => "eth",
                TxnFilterKind::Erc20 => "token",
            },
            (token_setup.count_in_block as u64) * (token_setup.create_block_count as u64)
        );
        Self {
            token_setup,
            reader: BufReader::new(file),
            txn_filter,
            txn_count: 0,
            cycle,
        }
    }

    /// Returns a compact 48-bit id for an address, mapped into the allowed account range.
    /// Uses SHA-256 (hasher::hash) and takes the first 6 bytes as the id.
    #[inline]
    fn id_for(&self, addr_hex: &str) -> u64 {
        let hash = hasher::hash(addr_hex);
        // Use the first 6 bytes for a 48-bit ID, pad to 8 bytes for u64
        let mut id_bytes = [0u8; 8];
        id_bytes[..6].copy_from_slice(&hash[..6]);
        let id = u64::from_le_bytes(id_bytes);
        let max_id =
            (self.token_setup.count_in_block as u64) * (self.token_setup.create_block_count as u64);
        let final_id = (id % (max_id - 1)) + 1;
        assert!(final_id < max_id, "id={} max_id={}", id, max_id);
        final_id
    }

    /// Attempts to extract one (src,dst) pair from a TxnRow according to the filter.
    fn row_to_pair(&mut self, row: TxnRow) -> Option<(u64, u64)> {
        match self.txn_filter {
            TxnFilterKind::Eth => {
                let val_hex = row.value.unwrap_or_else(|| "0x0".to_string());
                // Any non-zero "value" makes it a value-transfer; we accept empty/0x input only.
                if val_hex == "0x0" || val_hex == "0" {
                    return None;
                }
                let input_empty = row
                    .input
                    .as_deref()
                    .unwrap_or("")
                    .trim_start_matches("0x")
                    .is_empty();
                if !input_empty {
                    return None;
                }
                let dst = row.to?;
                let src_id = self.id_for(&row.from);
                let dst_id = self.id_for(&dst);
                Some((src_id, dst_id))
            }
            TxnFilterKind::Erc20 => {
                let input = row.input?; // must exist
                let input_clean = input.trim_start_matches("0x");
                if input_clean.len() < 8 {
                    return None;
                }
                let selector = &input_clean[..8].to_lowercase();
                match selector.as_str() {
                    // transfer(address,uint256)
                    "a9059cbb" => {
                        if input_clean.len() < 8 + 64 {
                            return None;
                        }
                        let param1 = &input_clean[8..8 + 64];
                        let dst_addr_hex = &param1[24..]; // last 40 chars
                        let src_id = self.id_for(&row.from);
                        let dst_id = self.id_for(dst_addr_hex);
                        Some((src_id, dst_id))
                    }
                    // transferFrom(address,address,uint256)
                    "23b872dd" => {
                        if input_clean.len() < 8 + 64 * 2 {
                            return None;
                        }
                        let from_addr_hex = &input_clean[8 + 24..8 + 64];
                        let dst_addr_hex = &input_clean[8 + 64 + 24..8 + 64 * 2];
                        let src_id = self.id_for(from_addr_hex);
                        let dst_id = self.id_for(dst_addr_hex);
                        Some((src_id, dst_id))
                    }
                    // approve(address,uint256) – treat owner→spender as a pseudo-transfer
                    "095ea7b3" => {
                        if input_clean.len() < 8 + 64 {
                            return None;
                        }
                        let dst_addr_hex = &input_clean[8 + 24..8 + 64];
                        let src_id = self.id_for(&row.from);
                        let dst_id = self.id_for(dst_addr_hex);
                        Some((src_id, dst_id))
                    }
                    _ => None,
                }
            }
        }
    }

    /// Reads lines until a pair is found or EOF.
    fn next_pair(&mut self) -> Option<(u64, u64)> {
        let mut line = String::new();
        loop {
            line.clear();
            let bytes = self.reader.read_line(&mut line).ok()?;
            if bytes == 0 {
                if self.cycle {
                    // EOF – rewind to beginning so workload can wrap around.
                    info!("Rewinding to beginning of blocks dump");
                    use std::io::{Seek, SeekFrom};
                    self.reader.get_mut().seek(SeekFrom::Start(0)).ok()?;
                    self.reader.consume(0); // reset internal buffer
                } else {
                    return None;
                }
                continue;
            }
            match serde_json::from_str::<TxnRow>(line.trim_end()) {
                Ok(row) => {
                    if let Some(p) = self.row_to_pair(row) {
                        self.txn_count += 1;
                        return Some(p);
                    }
                }
                Err(_) => continue, // skip malformed lines
            }
        }
    }
}

impl<'a> Generator for ReplayTransferGenerator<'a> {
    fn generate(&mut self, batch_size: usize) -> Vec<(u64, u64)> {
        let mut txns = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            match self.next_pair() {
                Some(pair) => txns.push(pair),
                None => break, // no more data; will return fewer than requested
            }
        }
        // if txns.len() < batch_size {
        //     // pad with duplicates of the last element to satisfy caller expectations
        //     if let Some(&last) = txns.last() {
        //         txns.resize(batch_size, last);
        //     }
        // }
        txns
    }

    fn get_name(&self) -> String {
        match self.txn_filter {
            TxnFilterKind::Eth => "replay_eth".to_string(),
            // TODO: rename token to erc20
            TxnFilterKind::Erc20 => "replay_token".to_string(),
        }
    }
}

// HotNContentionGenerator – similar to SimpleContentionGenerator but selects
// a fixed absolute number of HOT keys instead of a fraction of the key-space.
pub struct HotNContentionGenerator<'a> {
    /// Probability that a transaction writes to a HOT key  (0.0 – 1.0).
    hot_txn_ratio: f64,
    /// Number of keys that are considered HOT (must be > 0).
    hot_key_count: u64,
    /// Benchmark parameters (account/key layout).
    token_setup: &'a TokenSetup,
    /// The underlying pseudo-random sequence generator.
    lfsr_pair: (GaloisLfsr, GaloisLfsr),
    /// Size of the key space (inclusive upper bound + 1).
    key_space_size: u64,
}

impl<'a> HotNContentionGenerator<'a> {
    /// Creates a new generator where the HOT region contains exactly
    /// `hot_key_count` distinct keys (1-based).
    pub fn new(hot_txn_ratio: f64, hot_key_count: u64, token_setup: &'a TokenSetup) -> Self {
        assert!(
            (0.0..=1.0).contains(&hot_txn_ratio),
            "hot_txn_ratio must be in [0,1]"
        );
        assert!(hot_key_count > 0, "hot_key_count must be > 0");

        let key_space_size =
            (token_setup.count_in_block as u64) * (token_setup.create_block_count as u64);
        assert!(
            hot_key_count <= key_space_size,
            "hot_key_count={} exceeds key_space_size={}",
            hot_key_count,
            key_space_size
        );

        let lfsr_pair = token_setup.create_lfsr_pair(SEED);

        info!(
            "HotNContentionGenerator(hot_txn_ratio={}, hot_key_count={})",
            hot_txn_ratio, hot_key_count
        );

        info!(
            "key_space_size={}, hot_key_count={}",
            key_space_size, hot_key_count
        );

        Self {
            hot_txn_ratio,
            hot_key_count,
            token_setup,
            lfsr_pair,
            key_space_size,
        }
    }

    /// Draws an unbiased random number in `[0, key_space_size)` using the LFSR.
    #[inline]
    fn next_rand(&mut self) -> u64 {
        self.token_setup.get_rand(&mut self.lfsr_pair) % self.key_space_size
    }

    /// Returns a key from the HOT region `1..=hot_key_count`.
    #[inline]
    fn get_hot_key(&mut self) -> u64 {
        let min_key = 1;
        if self.hot_key_count == 0 {
            // Should never happen due to constructor check, but keep safe.
            return min_key;
        }
        (self.next_rand() % self.hot_key_count) + min_key
    }

    /// Returns a key from the COLD region `(hot_key_count, key_space_size]`.
    #[inline]
    fn get_cold_key(&mut self) -> u64 {
        let min_key = 1;
        if self.hot_key_count >= self.key_space_size {
            // Entire key-space is considered HOT: pick any valid account.
            return (self.next_rand() % self.key_space_size) + min_key;
        }
        let cold_range = self.key_space_size - self.hot_key_count;
        // Produces (hot_key_count + min_key)..=key_space_size
        (self.next_rand() % cold_range) + self.hot_key_count + min_key
    }
}

impl Generator for HotNContentionGenerator<'_> {
    fn generate(&mut self, batch_size: usize) -> Vec<(u64, u64)> {
        let mut txns = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            let prob = (self.next_rand() as f64) / (self.key_space_size as f64);
            let src = if prob < self.hot_txn_ratio {
                self.get_hot_key()
            } else {
                self.get_cold_key()
            };
            let dst = self.get_cold_key();
            check_key(src, self.key_space_size);
            check_key(dst, self.key_space_size);
            txns.push((src, dst));
        }
        txns
    }

    fn get_name(&self) -> String {
        format!("hotn_{}_{}", self.hot_txn_ratio, self.hot_key_count)
    }
}
