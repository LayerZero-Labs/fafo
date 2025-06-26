#!/usr/bin/env bash
###############################################################################
# run.sh
# --------
# Lightweight helper for bench-sendeth with sensible defaults.
#
# Key knobs:
#   • --db-dir DIR       Where QMDB lives (default: /tmp/QMDB)
#   • --size fast|medium|full|BITS   Workload size (fast=18, medium=26, full=30; or specify bits directly)
#   • --token eth|token  Workload token type (default: eth)
#   • --generator simple|static|hotn|none|replay  (default: simple)
#
# The script performs the minimum needed steps:
#   1. Initialise the database if missing.
#   2. Prepare workload blocks if missing.
#   3. Replay the workload.
#
# Replay workloads are generated inside the chosen DB directory. Pre-fetched
# source data (e.g. *.ndjson.gz) is expected to live under repository/workloads/.
###############################################################################

set -euo pipefail

show_help() {
  cat <<EOF
Usage: $0 [--db-dir DIR] [--size fast|medium|full|BITS] [--token eth|token]
          [--generator simple|static|hotn|none|replay] [--contention-degree X]
          [--skew Y] [--txns-json PATH] [--txn-filter STR]

Examples:
  # Quick sanity check on a laptop
  $0 --size fast

  # Larger run on an NVMe SSD with static contention
  $0 --db-dir /mnt/nvme/QMDB --size full --generator hotn --contention-degree 0.1 --skew 100

  # Replay a real workload trace
  $0 --generator replay --txns-json workloads/eth.ndjson.gz --txn-filter eth
EOF
  exit 1
}

# ------------------------------ Defaults ------------------------------------
DB_DIR="/tmp/QMDB"
SIZE="medium"           # fast | medium | full | <bits>
TOKEN="eth"             # eth | token
GENERATOR="none"      # simple | static | hotn | none | replay
CONTENTION_DEGREE="0.1"
SKEW="0.00001"
HOTN="100"
TXNS_JSON=""
TXN_FILTER=""

# ----------------------------- CLI parsing ----------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --db-dir) DB_DIR="$2"; shift 2 ;;
    --size)   SIZE="$2"; shift 2 ;;
    --token)  TOKEN="$2"; shift 2 ;;
    --generator) GENERATOR="$2"; shift 2 ;;
    --contention-degree) CONTENTION_DEGREE="$2"; shift 2 ;;
    --skew)   SKEW="$2"; HOTN="$2"; shift 2 ;;
    --txns-json) TXNS_JSON="$2"; shift 2 ;;
    --txn-filter) TXN_FILTER="$2"; shift 2 ;;
    -h|--help) show_help ;;
    *) echo "Unknown flag: $1" >&2; show_help ;;
  esac
done

# Map SIZE -> ENTRY_COUNT_BITS
if [[ "$SIZE" =~ ^[0-9]+$ ]]; then
  ENTRY_COUNT_BITS="$SIZE"
else
  case "$SIZE" in
    fast)    ENTRY_COUNT_BITS=18 ;;
    medium)  ENTRY_COUNT_BITS=26 ;;
    full)    ENTRY_COUNT_BITS=30 ;;
    *) echo "Invalid --size '$SIZE'" >&2; exit 1 ;;
  esac
fi

# Derive CREATE_TXN_PER_BLOCK_BITS from token type
if [[ "$TOKEN" == "eth" ]]; then
  CREATE_TXN_PER_BLOCK_BITS=21
else
  CREATE_TXN_PER_BLOCK_BITS=18
fi

# Safety guard: ensure there is enough headroom so that CREATE_BLOCK_BITS
# (entry_count_bits - create_txn_per_block_bits) is at least 5. GROUP_SIZE_BITS
# in Rust is 4, so we enforce a minimum gap of 5 here to match the invariant in
# TokenSetup::new(). This also rescales the "fast" preset automatically.
MIN_GAP=5
if (( ENTRY_COUNT_BITS - CREATE_TXN_PER_BLOCK_BITS < MIN_GAP )); then
  CREATE_TXN_PER_BLOCK_BITS=$(( ENTRY_COUNT_BITS - MIN_GAP ))
fi

CREATE_BLOCK_BITS=$((ENTRY_COUNT_BITS - CREATE_TXN_PER_BLOCK_BITS))
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RUN_DIR="$DB_DIR/exepipe_bencheth/${TOKEN}_${CREATE_TXN_PER_BLOCK_BITS}_${CREATE_BLOCK_BITS}"
BLOCKS_PREFIX="$RUN_DIR/blocks"

export RUSTFLAGS="-Awarnings"

# ---------------------------- Pretty print ----------------------------------
cat <<EOM
Parameters
----------
DB_DIR              : $DB_DIR
TOKEN               : $TOKEN
ENTRY_COUNT_BITS    : $ENTRY_COUNT_BITS
GENERATOR           : $GENERATOR
CONTENTION_DEGREE   : $CONTENTION_DEGREE
SKEW                : $SKEW
RUN_DIR             : $RUN_DIR
EOM
# Print git hash if available
if command -v git >/dev/null 2>&1 && git rev-parse --git-dir >/dev/null 2>&1; then
  GIT_HASH=$(git rev-parse HEAD 2>/dev/null || echo "unknown")
  echo "GIT_HASH             : $GIT_HASH"
else
  echo "GIT_HASH             : not available"
fi

# Print key system information
echo "=== System Information ==="
echo "CPU Model           : $(lscpu | grep 'Model name' | cut -d: -f2 | xargs 2>/dev/null || sysctl -n machdep.cpu.brand_string 2>/dev/null || echo "unknown")"
echo "CPU Cores           : $(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo "unknown")"
echo "Architecture        : $(uname -m)"
echo "OS                  : $(uname -s) $(uname -r)"
echo "Memory              : $(free -h | grep '^Mem:' | awk '{print $2}' 2>/dev/null || vm_stat | grep 'Pages free:' | awk '{print $3}' | xargs -I {} echo "scale=2; {} * 4096 / 1024 / 1024 / 1024" | bc 2>/dev/null || sysctl -n hw.memsize 2>/dev/null | awk '{print $1/1024/1024/1024 " GB"}' 2>/dev/null || echo "unknown")"
echo "Available Disk      : $(df -h . | tail -1 | awk '{print $4}' 2>/dev/null || echo "unknown")"
echo ""

# Print machine domain but degrade gracefully
if command -v hostname >/dev/null 2>&1; then
  MACHINE_DOMAIN=$(hostname -f 2>/dev/null || hostname 2>/dev/null || echo "unknown")
  echo "MACHINE_DOMAIN         : $MACHINE_DOMAIN"
else
  echo "MACHINE_DOMAIN         : not available"
fi


mkdir -p "$DB_DIR" "$RUN_DIR"
ulimit -n 65535
cd "$SCRIPT_DIR"

# Common bench-sendeth prefix (cargo will build only once)
CMD_PREFIX=(cargo run --quiet --package bench-sendeth --bin bench-sendeth --release -- \
            --db-dir "$DB_DIR" \
            --entry-count-bits "$ENTRY_COUNT_BITS" \
            --create-txn-per-block-bits "$CREATE_TXN_PER_BLOCK_BITS" \
            --workload "$TOKEN")

# 1. init-db (lazy)
if [[ ! -d "$RUN_DIR/data" ]]; then
  echo "=== init-db ==="
  "${CMD_PREFIX[@]}" init-db
fi

# 2. Determine blocks file + prepare flags
case "$GENERATOR" in
  simple)
    BLOCKS_FILE="${BLOCKS_PREFIX}_${GENERATOR}_${CONTENTION_DEGREE}_${SKEW}.dat"
    PREP_FLAGS=(--generator "$GENERATOR" --contention-degree "$CONTENTION_DEGREE" --skew "$SKEW")
    ;;
  hotn)
    BLOCKS_FILE="${BLOCKS_PREFIX}_${GENERATOR}_${CONTENTION_DEGREE}_${HOTN}.dat"
    PREP_FLAGS=(--generator "$GENERATOR" --contention-degree "$CONTENTION_DEGREE" --skew "$HOTN")
    ;;
  static)
    BLOCKS_FILE="${BLOCKS_PREFIX}_${GENERATOR}_${CONTENTION_DEGREE}.dat"
    PREP_FLAGS=(--generator static --contention-degree "$CONTENTION_DEGREE")
    ;;
  none)
    BLOCKS_FILE="${BLOCKS_PREFIX}_${GENERATOR}.dat"
    PREP_FLAGS=(--generator none)
    ;;
  replay)
    BLOCKS_FILE="${BLOCKS_PREFIX}_${GENERATOR}_${TOKEN}.dat"
    PREP_FLAGS=(--generator replay)
    if [[ -n "$TXNS_JSON" ]]; then
      PREP_FLAGS+=(--txns-json "$TXNS_JSON")
    else
      if [[ "$TOKEN" == "eth" ]]; then
        TXNS_JSON="$REPO_ROOT/workloads/eth.ndjson"
      else
        TXNS_JSON="$REPO_ROOT/workloads/token_erc20.ndjson"
      fi
      PREP_FLAGS+=(--txns-json "$TXNS_JSON")
    fi
    if [[ -n "$TXN_FILTER" ]]; then
      PREP_FLAGS+=(--txn-filter "$TXN_FILTER")
    else
      PREP_FLAGS+=(--txn-filter "$TOKEN")
    fi
    ;;
  *)
    echo "Unsupported generator '$GENERATOR'" >&2; exit 1 ;;
esac

# Ensure required replay workload file is present **before** preparing workload
if [[ "$GENERATOR" == "replay" ]]; then
  if [[ ! -f "$TXNS_JSON" ]]; then
    echo "Error: Transactions JSON '$TXNS_JSON' not found." >&2
    echo "Hint: fetch workloads by running: ./workloads/get_workloads.sh" >&2
    exit 1
  fi
fi

# 3. prepare-workload (lazy)
if [[ ! -f "$BLOCKS_FILE" ]]; then
  echo "=== prepare-workload (${BLOCKS_FILE##*/}) ==="
  "${CMD_PREFIX[@]}" prepare-workload "${PREP_FLAGS[@]}"
fi

# 4. replay-workload
echo "=== replay-workload (${BLOCKS_FILE##*/}) ==="
"${CMD_PREFIX[@]}" replay-workload --blocks-workload-filename "$BLOCKS_FILE"

# If replay generator enabled, ensure txns-json exists
# (moved to earlier check before preparation)