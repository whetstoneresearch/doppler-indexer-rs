#!/usr/bin/env bash
set -euo pipefail

# ── Configuration ────────────────────────────────────────────────────────────
BLOCK_COUNT=${BENCH_BLOCKS:-3000}
RUNS=${BENCH_RUNS:-3}
BASE_BRANCH=${BENCH_BASE:-main}
PR_BRANCH=${BENCH_PR:-fix/blocking-io-in-historical}
CLEANUP=${BENCH_CLEANUP:-1}

REPO_ROOT=$(git rev-parse --show-toplevel)
BENCH_DIR="${REPO_ROOT}/bench"
WORKTREE_BASE="${BENCH_DIR}/wt-${BASE_BRANCH//\//-}"
WORKTREE_PR="${BENCH_DIR}/wt-${PR_BRANCH//\//-}"

# ── Load .env from repo root ────────────────────────────────────────────────
if [[ -f "${REPO_ROOT}/.env" ]]; then
    set -a
    source "${REPO_ROOT}/.env"
    set +a
fi

# ── Validation ───────────────────────────────────────────────────────────────
if [[ -z "${BASE_RPC_URL:-}" ]]; then
    echo "ERROR: BASE_RPC_URL is not set (and not found in .env)" >&2
    exit 1
fi

# ── Query chain head ─────────────────────────────────────────────────────────
echo "Querying chain head..."
CHAIN_HEAD_HEX=$(curl -s -X POST "$BASE_RPC_URL" \
    -H "Content-Type: application/json" \
    -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
    | python3 -c "import json,sys; print(json.load(sys.stdin)['result'])")
CHAIN_HEAD=$((CHAIN_HEAD_HEX))
START_BLOCK=$((CHAIN_HEAD - BLOCK_COUNT))

# Align start_block to parquet_block_range boundary (1000)
START_BLOCK=$(( (START_BLOCK / 1000) * 1000 ))

echo "============================================================"
echo "  Historical Collection Benchmark"
echo "  Baseline: ${BASE_BRANCH}  |  Candidate: ${PR_BRANCH}"
echo "  Chain head: ${CHAIN_HEAD}  |  Start block: ${START_BLOCK}"
echo "  Blocks: ~${BLOCK_COUNT}  |  Runs: ${RUNS}"
echo "============================================================"
echo ""

# ── Cleanup handler ──────────────────────────────────────────────────────────
cleanup() {
    echo ""
    echo "Cleaning up worktrees..."
    for wt in "$WORKTREE_BASE" "$WORKTREE_PR"; do
        git worktree remove --force "$wt" 2>/dev/null || true
        [[ -d "$wt" ]] && rm -rf "$wt"
    done
    git worktree prune 2>/dev/null || true
}

if [[ "$CLEANUP" == "1" ]]; then
    trap cleanup EXIT
fi

# ── Phase 1: Create worktrees ───────────────────────────────────────────────
echo "Creating worktrees..."
for wt in "$WORKTREE_BASE" "$WORKTREE_PR"; do
    git worktree remove --force "$wt" 2>/dev/null || true
    if [[ -d "$wt" ]]; then
        rm -rf "$wt"
        git worktree prune
    fi
done
git worktree add --detach "$WORKTREE_BASE" "$BASE_BRANCH"
git worktree add --detach "$WORKTREE_PR" "$PR_BRANCH"
echo "  Worktrees created."

# ── Phase 2: Write benchmark config ─────────────────────────────────────────
write_bench_config() {
    local worktree_dir="$1"
    mkdir -p "${worktree_dir}/config"
    cat > "${worktree_dir}/config/config.json" << CONFIGEOF
{
    "chains": [
        {
            "name": "base",
            "chain_id": 8453,
            "rpc_url_env_var": "BASE_RPC_URL",
            "contracts": "contracts/base",
            "tokens": "tokens/base.json",
            "block_receipts_method": "eth_getBlockReceipts",
            "start_block": ${START_BLOCK}
        }
    ],
    "raw_data_collection": {
        "parquet_block_range": 1000,
        "rpc_batch_size": 1000,
        "block_receipt_concurrency": 100,
        "channel_capacity": 5000,
        "factory_channel_capacity": 5000,
        "fields": {
            "block_fields": ["number", "timestamp", "transactions", "uncles"],
            "receipt_fields": ["block_number", "timestamp", "transaction_hash", "from", "to"],
            "log_fields": ["block_number", "timestamp", "transaction_hash", "log_index", "address", "topics", "data"]
        },
        "contract_logs_only": false
    }
}
CONFIGEOF
}

write_bench_config "$WORKTREE_BASE"
write_bench_config "$WORKTREE_PR"
echo "  Benchmark configs written (start_block=${START_BLOCK})."

# ── Phase 3: Build both in release mode ──────────────────────────────────────
echo ""
echo "Building baseline (${BASE_BRANCH})..."
(cd "$WORKTREE_BASE" && cargo build --release 2>&1 | tail -3) &
PID_BUILD_BASE=$!

echo "Building candidate (${PR_BRANCH})..."
(cd "$WORKTREE_PR" && cargo build --release 2>&1 | tail -3) &
PID_BUILD_PR=$!

wait $PID_BUILD_BASE || { echo "ERROR: baseline build failed" >&2; exit 1; }
echo "  Baseline built."
wait $PID_BUILD_PR || { echo "ERROR: candidate build failed" >&2; exit 1; }
echo "  Candidate built."

# ── Phase 4: Run benchmarks ─────────────────────────────────────────────────
run_single() {
    local label="$1"
    local worktree_dir="$2"
    local run_num="$3"
    local log_file="${BENCH_DIR}/${label}_run${run_num}.log"

    # Clean data directory for fresh run
    rm -rf "${worktree_dir}/data"

    local start_ts=$(date +%s%N)

    # Seed the data directory with a dummy blocks parquet file so --catch-up-only
    # has a frontier to work towards (otherwise it exits immediately on empty data).
    local frontier_start=$(( START_BLOCK + BLOCK_COUNT ))
    local frontier_end=$(( frontier_start + 999 ))
    local blocks_dir="${worktree_dir}/data/base/historical/raw/blocks"
    mkdir -p "$blocks_dir"
    touch "${blocks_dir}/blocks_${frontier_start}-${frontier_end}.parquet"

    # Run with --catch-up-only: collects from start_block to frontier, then exits
    (
        cd "$worktree_dir"
        NO_COLOR=1 RUST_LOG=info ./target/release/doppler-indexer-rs --catch-up-only 2>&1 | cat
    ) > "$log_file" 2>&1
    local exit_code=$?

    local end_ts=$(date +%s%N)
    local elapsed_ms=$(( (end_ts - start_ts) / 1000000 ))

    if [[ $exit_code -ne 0 ]]; then
        echo "    run ${run_num}: FAILED (exit code ${exit_code})" >&2
        tail -5 "$log_file" >&2
        echo "0"
        return
    fi

    echo "    run ${run_num}: ${elapsed_ms}ms" >&2
    echo "$elapsed_ms"
}

run_benchmark() {
    local label="$1"
    local worktree_dir="$2"

    echo ""
    echo "Running ${label} benchmark (${RUNS} runs of ~${BLOCK_COUNT} blocks)..."

    local times=()
    for i in $(seq 1 "$RUNS"); do
        local ms
        ms=$(run_single "$label" "$worktree_dir" "$i")
        times+=("$ms")

        # Cooldown between runs (not after last)
        if [[ $i -lt $RUNS ]]; then
            echo "    cooldown (5s)..." >&2
            sleep 5
        fi
    done

    # Write times to file for parsing
    printf "%s\n" "${times[@]}" > "${BENCH_DIR}/${label}_times.txt"
}

run_benchmark "baseline" "$WORKTREE_BASE"

echo ""
echo "Cooldown between branches (10s)..."
sleep 10

run_benchmark "candidate" "$WORKTREE_PR"

# ── Phase 5: Parse results ───────────────────────────────────────────────────
echo ""
echo "Parsing results..."

parse_results() {
    local label="$1"
    local times_file="${BENCH_DIR}/${label}_times.txt"

    python3 - "$times_file" "$BENCH_DIR" "$label" "$RUNS" << 'PYEOF'
import sys
import re
import statistics
from datetime import datetime

times_file = sys.argv[1]
bench_dir = sys.argv[2]
label = sys.argv[3]
num_runs = int(sys.argv[4])

# Read wall clock times
with open(times_file) as f:
    wall_times_ms = [int(line.strip()) for line in f if line.strip() and line.strip() != "0"]

if not wall_times_ms:
    print("RUNS=0")
    print("ERROR=all_failed")
    sys.exit(0)

# Parse log files for sub-phase timing
phase_times = {
    "collection": [],  # "Collecting N block ranges" -> "Completed historical processing"
}
write_durations = []  # per-range: "Fetching blocks X-Y" -> "Wrote N blocks to ..."

def parse_ts(s):
    return datetime.fromisoformat(s.replace('Z', '+00:00'))

for i in range(1, num_runs + 1):
    log_file = f"{bench_dir}/{label}_run{i}.log"
    try:
        with open(log_file) as f:
            lines = f.readlines()
    except FileNotFoundError:
        continue

    collection_start = None
    collection_end = None
    cur_fetch_start = None

    for line in lines:
        line = line.strip()
        line = re.sub(r'\x1b\[[0-9;]*m', '', line)

        ts_match = re.match(r'^(\S+)\s+INFO\s+', line)
        if not ts_match:
            continue
        ts_str = ts_match.group(1)

        if "Collecting" in line and "block ranges" in line:
            try:
                collection_start = parse_ts(ts_str)
            except Exception:
                pass

        if "Completed historical processing" in line:
            try:
                collection_end = parse_ts(ts_str)
            except Exception:
                pass

        if "Fetching blocks" in line:
            try:
                cur_fetch_start = parse_ts(ts_str)
            except Exception:
                pass

        if "Wrote" in line and "blocks to" in line and cur_fetch_start:
            try:
                wrote_ts = parse_ts(ts_str)
                dur = (wrote_ts - cur_fetch_start).total_seconds() * 1000
                write_durations.append(dur)
                cur_fetch_start = None
            except Exception:
                pass

    if collection_start and collection_end:
        phase_times["collection"].append(
            (collection_end - collection_start).total_seconds() * 1000
        )

# Output stats
n = len(wall_times_ms)
print(f"RUNS={n}")
print(f"WALL_MEAN={statistics.mean(wall_times_ms):.0f}")
print(f"WALL_MEDIAN={sorted(wall_times_ms)[n//2]:.0f}")
print(f"WALL_MIN={min(wall_times_ms):.0f}")
print(f"WALL_MAX={max(wall_times_ms):.0f}")
if n > 1:
    print(f"WALL_STDEV={statistics.stdev(wall_times_ms):.0f}")
else:
    print("WALL_STDEV=0")

# Blocks per second (using mean wall time)
block_count = 0
# Try to extract from log
for i in range(1, num_runs + 1):
    log_file = f"{bench_dir}/{label}_run{i}.log"
    try:
        with open(log_file) as f:
            for line in f:
                m = re.search(r'Collecting (\d+) block ranges.*blocks (\d+)-(\d+)', line)
                if m:
                    start, end = int(m.group(2)), int(m.group(3))
                    block_count = end - start + 1
                    break
        if block_count > 0:
            break
    except FileNotFoundError:
        continue

if block_count > 0 and statistics.mean(wall_times_ms) > 0:
    bps = block_count / (statistics.mean(wall_times_ms) / 1000)
    print(f"BLOCKS={block_count}")
    print(f"BLOCKS_PER_SEC={bps:.1f}")
else:
    print("BLOCKS=0")
    print("BLOCKS_PER_SEC=0")

# Collection phase timing
ct = phase_times["collection"]
if ct:
    print(f"COLLECTION_MEAN={statistics.mean(ct):.0f}")
else:
    print("COLLECTION_MEAN=N/A")

# Per-range fetch+write timing (across all runs)
if write_durations:
    write_durations.sort()
    n_wr = len(write_durations)
    print(f"RANGE_WRITES={n_wr}")
    print(f"RANGE_MEAN={statistics.mean(write_durations):.0f}")
    print(f"RANGE_MEDIAN={write_durations[n_wr//2]:.0f}")
    print(f"RANGE_P95={write_durations[int(n_wr*0.95)]:.0f}")
    print(f"RANGE_MAX={max(write_durations):.0f}")
else:
    print("RANGE_WRITES=0")
    print("RANGE_MEAN=N/A")
    print("RANGE_MEDIAN=N/A")
    print("RANGE_P95=N/A")
    print("RANGE_MAX=N/A")
PYEOF
}

BASELINE_STATS=$(parse_results "baseline")
CANDIDATE_STATS=$(parse_results "candidate")

get_stat() {
    echo "$1" | grep "^${2}=" | cut -d= -f2
}

# ── Phase 6: Print comparison ────────────────────────────────────────────────
print_row() {
    local label="$1" bval="$2" cval="$3" bnum="${4:-}" cnum="${5:-}"
    local delta=""
    if [[ -n "$bnum" && -n "$cnum" && "$bnum" != "N/A" && "$cnum" != "N/A" && "$bnum" != "0" ]]; then
        delta=$(python3 -c "
b, c = float('$bnum'), float('$cnum')
if b > 0:
    pct = ((c - b) / b) * 100
    sign = '+' if pct >= 0 else ''
    print(f'{sign}{pct:.1f}%')
else:
    print('N/A')
" 2>/dev/null || echo "N/A")
    fi
    printf "  %-28s %15s %15s %10s\n" "$label" "$bval" "$cval" "$delta"
}

fmt_ms() {
    local ms="$1"
    if [[ "$ms" == "N/A" || "$ms" == "0" ]]; then
        echo "N/A"
        return
    fi
    local secs=$(python3 -c "print(f'{int($ms)/1000:.1f}')" 2>/dev/null || echo "N/A")
    echo "${secs}s"
}

B_RUNS=$(get_stat "$BASELINE_STATS" RUNS)
C_RUNS=$(get_stat "$CANDIDATE_STATS" RUNS)
B_MEAN=$(get_stat "$BASELINE_STATS" WALL_MEAN)
C_MEAN=$(get_stat "$CANDIDATE_STATS" WALL_MEAN)
B_MEDIAN=$(get_stat "$BASELINE_STATS" WALL_MEDIAN)
C_MEDIAN=$(get_stat "$CANDIDATE_STATS" WALL_MEDIAN)
B_MIN=$(get_stat "$BASELINE_STATS" WALL_MIN)
C_MIN=$(get_stat "$CANDIDATE_STATS" WALL_MIN)
B_MAX=$(get_stat "$BASELINE_STATS" WALL_MAX)
C_MAX=$(get_stat "$CANDIDATE_STATS" WALL_MAX)
B_STDEV=$(get_stat "$BASELINE_STATS" WALL_STDEV)
C_STDEV=$(get_stat "$CANDIDATE_STATS" WALL_STDEV)
B_BLOCKS=$(get_stat "$BASELINE_STATS" BLOCKS)
C_BLOCKS=$(get_stat "$CANDIDATE_STATS" BLOCKS)
B_BPS=$(get_stat "$BASELINE_STATS" BLOCKS_PER_SEC)
C_BPS=$(get_stat "$CANDIDATE_STATS" BLOCKS_PER_SEC)
B_COLL=$(get_stat "$BASELINE_STATS" COLLECTION_MEAN)
C_COLL=$(get_stat "$CANDIDATE_STATS" COLLECTION_MEAN)
B_RMEAN=$(get_stat "$BASELINE_STATS" RANGE_MEAN)
C_RMEAN=$(get_stat "$CANDIDATE_STATS" RANGE_MEAN)
B_RMED=$(get_stat "$BASELINE_STATS" RANGE_MEDIAN)
C_RMED=$(get_stat "$CANDIDATE_STATS" RANGE_MEDIAN)
B_RP95=$(get_stat "$BASELINE_STATS" RANGE_P95)
C_RP95=$(get_stat "$CANDIDATE_STATS" RANGE_P95)
B_RMAX=$(get_stat "$BASELINE_STATS" RANGE_MAX)
C_RMAX=$(get_stat "$CANDIDATE_STATS" RANGE_MAX)

echo ""
echo "======================================================================"
echo "  HISTORICAL COLLECTION BENCHMARK RESULTS"
echo "  Baseline: ${BASE_BRANCH}  |  Candidate: ${PR_BRANCH}"
echo "  ~${BLOCK_COUNT} blocks, ${RUNS} runs each"
echo "======================================================================"
echo ""
printf "  %-28s %15s %15s %10s\n" "Metric" "Baseline" "Candidate" "Delta"
printf "  %-28s %15s %15s %10s\n" "----------------------------" "---------------" "---------------" "----------"

print_row "Successful runs" "$B_RUNS / $RUNS" "$C_RUNS / $RUNS"
print_row "Blocks collected" "$B_BLOCKS" "$C_BLOCKS"
echo ""
printf "  %-28s %15s %15s %10s\n" "  Wall clock" "" "" ""
print_row "  mean" "$(fmt_ms "$B_MEAN")" "$(fmt_ms "$C_MEAN")" "$B_MEAN" "$C_MEAN"
print_row "  median" "$(fmt_ms "$B_MEDIAN")" "$(fmt_ms "$C_MEDIAN")" "$B_MEDIAN" "$C_MEDIAN"
print_row "  min" "$(fmt_ms "$B_MIN")" "$(fmt_ms "$C_MIN")" "$B_MIN" "$C_MIN"
print_row "  max" "$(fmt_ms "$B_MAX")" "$(fmt_ms "$C_MAX")" "$B_MAX" "$C_MAX"
print_row "  stdev" "$(fmt_ms "$B_STDEV")" "$(fmt_ms "$C_STDEV")"
echo ""
print_row "Throughput (blocks/sec)" "$B_BPS" "$C_BPS" "$B_BPS" "$C_BPS"
print_row "Collection phase (mean)" "$(fmt_ms "$B_COLL")" "$(fmt_ms "$C_COLL")" "$B_COLL" "$C_COLL"
echo ""
printf "  %-28s %15s %15s %10s\n" "  Per-range fetch+write" "" "" ""
print_row "  mean" "$(fmt_ms "$B_RMEAN")" "$(fmt_ms "$C_RMEAN")" "$B_RMEAN" "$C_RMEAN"
print_row "  median" "$(fmt_ms "$B_RMED")" "$(fmt_ms "$C_RMED")" "$B_RMED" "$C_RMED"
print_row "  p95" "$(fmt_ms "$B_RP95")" "$(fmt_ms "$C_RP95")" "$B_RP95" "$C_RP95"
print_row "  max" "$(fmt_ms "$B_RMAX")" "$(fmt_ms "$C_RMAX")" "$B_RMAX" "$C_RMAX"

echo ""
echo "  Logs: bench/{baseline,candidate}_run{1..${RUNS}}.log"
echo ""
