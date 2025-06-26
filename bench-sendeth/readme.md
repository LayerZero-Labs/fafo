# bench-sendeth

`bench-sendeth` is a synthetic workload generator and replay harness used to
benchmark **FAFO**.  It can:

1. **Initialise** a large key-value database with billions of accounts.
2. **Generate** transfer workloads with tunable contention or derived from a real
   transaction trace.
3. **Replay** those workloads in parallel, collecting rich metrics along the
   way.

The tool is written in Rust and includes a helper script [`run.sh`](./run.sh) that
handles the full `cargo run …` invocation for you.

## Quick-start (recommended)

The one-liner below runs a *small* benchmark suitable for laptops, taking only a couple of minutes:

```bash
./run.sh --size fast
```

The script will:

1. Create or reuse a database under `/tmp/QMDB`.
2. Create a low-contention ETH workload if it does not already exist.
3. Replay the workload and print timing information.

Use `--help` to see all options exposed by `run.sh`:

```bash
./run.sh --help
```

Common flags:

* `--db-dir DIR` – where to store QMDB files (default `/tmp/QMDB`).
* `--size fast|medium|full|BITS` – workload size shortcut (18, 26, 30 bits) or
  an explicit power-of-two.
* `--token eth|token` – transfer asset type: native ETH or ERC-20.
* `--generator simple|static|hotn|none|replay` – contention model.

See the *Generators* section below for details.


## Reproduce paper results

```bash
# No contention
./bench-sendeth/run.sh --size full --generator none
# Contention
./bench-sendeth/run.sh --size full --generator hotn --contention-degree 0.1 --skew 100
# Replay
./workloads/get_workloads.sh
./bench-sendeth/run.sh --size full --generator replay
```


## Anatomy of a benchmark run

Internally a run consists of three distinct phases.  You can execute them
individually via the `bench-sendeth` binary, but `run.sh` already orchestrates
this flow for you. **We strongly recommend using `run.sh` to run benchmarks.**

### 1. `init-db`
Populates the database with \(2^{entry\_count\_bits}\) accounts.  This phase is
slow – expect tens of minutes to multiple hours depending on the size and
hardware.  You normally do this once per `(db-dir, size, token)` triple.

Example (30-bit ETH workload on a fast machine):

```bash
cargo run --package bench-sendeth --bin bench-sendeth --release -- \
  --db-dir /tmp/QMDB --workload eth --entry-count-bits 30 \
  init-db
```

### 2. `prepare-workload`
Creates `blocks_*.dat`, a compact description of the transfers that will be
replayed later.  Generation is fast (seconds–minutes).  The command supports
several workload *generators*:

| Generator | Description |
|-----------|-------------|
| `none`    | Zero contention, purely random senders & receivers. |
| `hotn`    | A set of N hot accounts; higher `--skew` ($\alpha$ in paper) means more churn. |
| `replay`  | Extracts transfers from a real transaction dump (`--txns-json`). |

Example (10 % hotn contention, 100 hot keys, eth workload):

```bash
cargo run --package bench-sendeth --bin bench-sendeth --release -- \
  --db-dir /tmp/QMDB --workload eth --entry-count-bits 30 \
  prepare-workload --generator hotn --contention-degree 0.1 --skew 100
```

### 3. `replay-workload`
Restores the database into a fresh directory, replays the specified workload in
parallel and writes a JSON metrics file next to the database.

```bash
cargo run --package bench-sendeth --bin bench-sendeth --release -- \
  --db-dir /tmp/QMDB --workload eth \
  replay-workload --blocks-workload-filename /tmp/QMDB/exepipe_bencheth/eth_21_10/blocks_hotn_0.1_100.dat
```

---

## Full CLI reference

Below is a consolidated list of flags across all subcommands.  Flags marked with
*️⃣ are normally set for you by `run.sh`.

### Global flags

* `--db-dir DIR` – database root directory *️⃣
* `--workload eth|token` – asset type *️⃣
* `--entry-count-bits N` – \(log_2\) of the number of accounts (default 31)
* `--create-txn-per-block-bits N` – overrides default TXs per create block
* `--transfer-txn-count N` – total TXs to send during replay (0 = all)
* `--txn-per-transfer-block N` – TXs per replay block
* `--output FILE` – custom metrics JSON path

### Subcommands

#### `init-db`
No additional flags.

#### `prepare-workload`

* `--generator ...` – see table above
* `--contention-degree FLOAT` – fraction of hot accounts (default 0.1)
* `--skew FLOAT` – Zipf skew (`simple`, `hotn`) (default 5e-7)
* `--txns-json PATH` – ND-JSON.gz dump for `replay` mode
* `--txn-filter eth|erc20` – which transfers to extract (default `erc20`)
* `--replay-cycle BOOL` – loop over dump when EOF reached (default true)

#### `replay-workload`

* `--blocks-workload-filename FILE` – path to the *.dat workload (default
  `blocks.dat`)

---

## Metrics output

Metrics are recorded with [`metrics`](https://docs.rs/metrics) and written as a
pretty-printed JSON.  When using `run.sh` the file ends up in
`$DB_DIR/exepipe_bencheth/<…>/metrics/*.json` and includes:

* Timing for each phase of the run.
* Internal contention statistics.
* Parallelism observed by the framer.

You can load the file into Python, R, or your favourite Jupyter notebook for
visualisation.

---

## Fetching traces based on historical real-world transactions

The `replay` generator needs a compressed ND-JSON dump of Ethereum transactions.
Convenience scripts under [`workloads/`](../workloads/) can fetch prepared
samples:

```bash
workloads/get_workloads.sh
```

By default `run.sh` will look for `eth.ndjson.gz` (ETH transfers) or
`token_erc20.ndjson.gz` (ERC-20 transfers) under that directory.

---
