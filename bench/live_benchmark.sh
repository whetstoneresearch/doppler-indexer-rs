#!/usr/bin/env bash
set -euo pipefail

# ── Configuration ────────────────────────────────────────────────────────────
BLOCK_COUNT=${BENCH_BLOCKS:-250}
WARMUP_BLOCKS=${BENCH_WARMUP:-5}
BASE_BRANCH=${BENCH_BASE:-main}
PR_BRANCH=${BENCH_PR:-fix/blocking-io-in-async}
CLEANUP=${BENCH_CLEANUP:-1}

REPO_ROOT=$(git rev-parse --show-toplevel)
BENCH_DIR="${REPO_ROOT}/bench"
WORKTREE_BASE="${BENCH_DIR}/wt-${BASE_BRANCH//\//-}"
WORKTREE_PR="${BENCH_DIR}/wt-${PR_BRANCH//\//-}"
TARGET_BLOCKS=$((BLOCK_COUNT + WARMUP_BLOCKS))
TIMEOUT_PER_BLOCK=5  # seconds

# ── Load .env from repo root ─────────────────────────────────────────────────
if [[ -f "${REPO_ROOT}/.env" ]]; then
    set -a
    source "${REPO_ROOT}/.env"
    set +a
fi

# ── Validation ───────────────────────────────────────────────────────────────
for var in BASE_RPC_URL BASE_WS_URL; do
    if [[ -z "${!var:-}" ]]; then
        echo "ERROR: $var is not set (and not found in .env)" >&2
        exit 1
    fi
done

echo "============================================================"
echo "  Live Mode Benchmark"
echo "  Baseline: ${BASE_BRANCH}  |  Candidate: ${PR_BRANCH}"
echo "  Blocks: ${BLOCK_COUNT} + ${WARMUP_BLOCKS} warmup"
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
    # If directory still exists (stale worktree), remove it and prune
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
    cat > "${worktree_dir}/config/config.json" << 'CONFIGEOF'
{
    "chains": [
        {
            "name": "base",
            "chain_id": 8453,
            "rpc_url_env_var": "BASE_RPC_URL",
            "ws_url_env_var": "BASE_WS_URL",
            "contracts": "contracts/base",
            "tokens": "tokens/base.json",
            "block_receipts_method": "eth_getBlockReceipts"
        }
    ],
    "raw_data_collection": {
        "parquet_block_range": 50,
        "reorg_depth": 10,
        "compaction_interval_secs": 5,
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
echo "  Benchmark configs written."

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
run_benchmark() {
    local label="$1"
    local worktree_dir="$2"
    local log_file="${BENCH_DIR}/${label}.log"
    local timeout=$((TARGET_BLOCKS * TIMEOUT_PER_BLOCK))

    echo ""
    echo "Running ${label} benchmark (collecting ${TARGET_BLOCKS} blocks)..."

    # Clear data directory for a fresh run
    rm -rf "${worktree_dir}/data"

    # Start the indexer, force no ANSI via pipe to cat
    (
        cd "$worktree_dir"
        NO_COLOR=1 RUST_LOG=info ./target/release/doppler-indexer-rs --live-only 2>&1 | cat
    ) > "$log_file" &
    local PID=$!
    # Get the actual indexer PID (child of subshell)
    sleep 1
    local INDEXER_PID
    INDEXER_PID=$(pgrep -P "$PID" -f doppler-indexer 2>/dev/null || echo "$PID")

    local collected=0
    local start_time=$(date +%s)
    local last_report=0

    while [[ $collected -lt $TARGET_BLOCKS ]]; do
        sleep 2

        if ! kill -0 "$PID" 2>/dev/null; then
            echo "  ERROR: ${label} process exited prematurely" >&2
            echo "  Last log lines:" >&2
            tail -10 "$log_file" >&2
            return 1
        fi

        collected=$(grep -c "Block [0-9]* collected: [0-9]* txs" "$log_file" 2>/dev/null || true)
        collected=${collected:-0}
        collected=$(( collected + 0 )) 2>/dev/null || collected=0

        # Print progress every 25 blocks
        if (( collected >= last_report + 25 )); then
            echo "  ${label}: ${collected} / ${TARGET_BLOCKS} blocks"
            last_report=$collected
        fi

        local elapsed=$(( $(date +%s) - start_time ))
        if [[ $elapsed -gt $timeout ]]; then
            echo "  ERROR: ${label} timed out after ${elapsed}s (collected ${collected} blocks)" >&2
            kill -KILL "$INDEXER_PID" 2>/dev/null || true
            kill -KILL "$PID" 2>/dev/null || true
            return 1
        fi
    done

    # Forceful shutdown — SIGKILL to ensure immediate stop
    kill -KILL "$INDEXER_PID" 2>/dev/null || true
    kill -KILL "$PID" 2>/dev/null || true
    wait "$PID" 2>/dev/null || true

    local actual=$(grep -c "Block [0-9]* collected: [0-9]* txs" "$log_file" 2>/dev/null || echo 0)
    echo "  ${label} complete (${actual} blocks in log)"
}

run_benchmark "baseline" "$WORKTREE_BASE"

echo ""
echo "Cooldown (10s between runs)..."
sleep 10

run_benchmark "candidate" "$WORKTREE_PR"

# ── Phase 5: Parse results ───────────────────────────────────────────────────
echo ""
echo "Parsing results..."

parse_results() {
    local label="$1"
    local log_file="${BENCH_DIR}/${label}.log"
    local csv_file="${BENCH_DIR}/${label}_blocks.csv"

    python3 - "$log_file" "$csv_file" "$WARMUP_BLOCKS" << 'PYEOF'
import sys
import re
from datetime import datetime

log_file = sys.argv[1]
csv_file = sys.argv[2]
warmup = int(sys.argv[3])

block_collected = {}
receipt_collected = {}
block_order = []

block_re = re.compile(r'^(\S+)\s+INFO\s+\S+:\s+Block\s+(\d+)\s+collected:\s+(\d+)\s+txs')
receipt_re = re.compile(r'^(\S+)\s+INFO\s+\S+:\s+Block\s+(\d+)\s+receipts\s+collected:\s+(\d+)\s+logs')
compact_start_re = re.compile(r'^(\S+)\s+INFO\s+\S+:\s+Compacting\s+range\s+(\d+)-(\d+)\s+\((\d+)\s+blocks\)')
compact_done_re = re.compile(r'^(\S+)\s+INFO\s+\S+:\s+Successfully\s+compacted\s+range\s+(\d+)-(\d+)')
ethcall_re = re.compile(r'^(\S+)\s+INFO\s+\S+:\s+Block\s+(\d+)\s+eth_calls\s+collected:\s+(\d+)')

compaction_starts = {}
compaction_durations = []
ethcall_counts = {}

with open(log_file) as f:
    for line in f:
        line = line.strip()
        # Strip any ANSI codes just in case
        line = re.sub(r'\x1b\[[0-9;]*m', '', line)

        m = block_re.match(line)
        if m:
            ts, bn, txc = m.group(1), int(m.group(2)), int(m.group(3))
            block_collected[bn] = (ts, txc)
            block_order.append(bn)
            continue

        m = receipt_re.match(line)
        if m:
            ts, bn, lc = m.group(1), int(m.group(2)), int(m.group(3))
            receipt_collected[bn] = (ts, lc)
            continue

        m = ethcall_re.match(line)
        if m:
            _, bn, cc = m.group(1), int(m.group(2)), int(m.group(3))
            ethcall_counts[bn] = cc
            continue

        m = compact_start_re.match(line)
        if m:
            ts, start, end = m.group(1), m.group(2), m.group(3)
            compaction_starts[f"{start}-{end}"] = ts
            continue

        m = compact_done_re.match(line)
        if m:
            ts, start, end = m.group(1), m.group(2), m.group(3)
            key = f"{start}-{end}"
            if key in compaction_starts:
                t0 = datetime.fromisoformat(compaction_starts[key].replace('Z', '+00:00'))
                t1 = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                dur_ms = (t1 - t0).total_seconds() * 1000
                compaction_durations.append((key, dur_ms))
            continue

def parse_ts(s):
    return datetime.fromisoformat(s.replace('Z', '+00:00'))

# Write per-block CSV
with open(csv_file, 'w') as out:
    out.write("block_number,tx_count,log_count,eth_calls,block_ts,receipt_ts,latency_ms\n")
    latencies = []
    for i, bn in enumerate(block_order):
        if i < warmup:
            continue
        if bn in block_collected and bn in receipt_collected:
            bt_str, txc = block_collected[bn]
            rt_str, lc = receipt_collected[bn]
            ec = ethcall_counts.get(bn, 0)
            lat = (parse_ts(rt_str) - parse_ts(bt_str)).total_seconds() * 1000
            latencies.append(lat)
            out.write(f"{bn},{txc},{lc},{ec},{bt_str},{rt_str},{lat:.3f}\n")

# Output summary stats
if latencies:
    import statistics
    latencies.sort()
    n = len(latencies)
    p50 = latencies[n // 2]
    p95 = latencies[int(n * 0.95)]
    p99 = latencies[int(n * 0.99)]

    first_ts = parse_ts(block_collected[block_order[warmup]][0])
    last_ts = parse_ts(block_collected[block_order[-1]][0])
    wall_s = (last_ts - first_ts).total_seconds()
    bpm = n / wall_s * 60 if wall_s > 0 else 0

    print(f"BLOCKS={n}")
    print(f"MEAN={statistics.mean(latencies):.3f}")
    print(f"MEDIAN={p50:.3f}")
    print(f"P95={p95:.3f}")
    print(f"P99={p99:.3f}")
    print(f"MIN={min(latencies):.3f}")
    print(f"MAX={max(latencies):.3f}")
    print(f"STDEV={statistics.stdev(latencies):.3f}" if n > 1 else "STDEV=0")
    print(f"WALL_CLOCK_S={wall_s:.1f}")
    print(f"THROUGHPUT_BPM={bpm:.1f}")
    print(f"COMPACTIONS={len(compaction_durations)}")
    if compaction_durations:
        ct = [d for _, d in compaction_durations]
        print(f"COMPACT_MEAN={statistics.mean(ct):.3f}")
        print(f"COMPACT_MAX={max(ct):.3f}")
    else:
        print("COMPACT_MEAN=N/A")
        print("COMPACT_MAX=N/A")
else:
    print("BLOCKS=0")
    print("ERROR=no_data")
PYEOF
}

# Capture stats into variables
BASELINE_STATS=$(parse_results "baseline")
CANDIDATE_STATS=$(parse_results "candidate")

get_stat() {
    echo "$1" | grep "^${2}=" | cut -d= -f2
}

# ── Phase 6: Print comparison ────────────────────────────────────────────────
print_row() {
    local label="$1" bval="$2" cval="$3" bnum="${4:-}" cnum="${5:-}"
    local delta=""
    if [[ -n "$bnum" && -n "$cnum" && "$bnum" != "N/A" && "$cnum" != "N/A" ]]; then
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

echo ""
echo "======================================================================"
echo "  LIVE MODE BENCHMARK RESULTS"
echo "  Baseline: ${BASE_BRANCH}  |  Candidate: ${PR_BRANCH}"
echo "  Blocks: ${BLOCK_COUNT} (after ${WARMUP_BLOCKS} warmup)"
echo "======================================================================"
echo ""
printf "  %-28s %15s %15s %10s\n" "Metric" "Baseline" "Candidate" "Delta"
printf "  %-28s %15s %15s %10s\n" "----------------------------" "---------------" "---------------" "----------"

B_MEDIAN=$(get_stat "$BASELINE_STATS" MEDIAN)
C_MEDIAN=$(get_stat "$CANDIDATE_STATS" MEDIAN)
B_P95=$(get_stat "$BASELINE_STATS" P95)
C_P95=$(get_stat "$CANDIDATE_STATS" P95)
B_P99=$(get_stat "$BASELINE_STATS" P99)
C_P99=$(get_stat "$CANDIDATE_STATS" P99)
B_MEAN=$(get_stat "$BASELINE_STATS" MEAN)
C_MEAN=$(get_stat "$CANDIDATE_STATS" MEAN)
B_MAX=$(get_stat "$BASELINE_STATS" MAX)
C_MAX=$(get_stat "$CANDIDATE_STATS" MAX)
B_STDEV=$(get_stat "$BASELINE_STATS" STDEV)
C_STDEV=$(get_stat "$CANDIDATE_STATS" STDEV)
B_WALL=$(get_stat "$BASELINE_STATS" WALL_CLOCK_S)
C_WALL=$(get_stat "$CANDIDATE_STATS" WALL_CLOCK_S)
B_BPM=$(get_stat "$BASELINE_STATS" THROUGHPUT_BPM)
C_BPM=$(get_stat "$CANDIDATE_STATS" THROUGHPUT_BPM)
B_COMP=$(get_stat "$BASELINE_STATS" COMPACTIONS)
C_COMP=$(get_stat "$CANDIDATE_STATS" COMPACTIONS)
B_CMEAN=$(get_stat "$BASELINE_STATS" COMPACT_MEAN)
C_CMEAN=$(get_stat "$CANDIDATE_STATS" COMPACT_MEAN)
B_CMAX=$(get_stat "$BASELINE_STATS" COMPACT_MAX)
C_CMAX=$(get_stat "$CANDIDATE_STATS" COMPACT_MAX)

B_BLOCKS=$(get_stat "$BASELINE_STATS" BLOCKS)
C_BLOCKS=$(get_stat "$CANDIDATE_STATS" BLOCKS)

print_row "Blocks measured" "$B_BLOCKS" "$C_BLOCKS"
print_row "Wall clock" "${B_WALL}s" "${C_WALL}s" "$B_WALL" "$C_WALL"
print_row "Throughput" "${B_BPM} b/m" "${C_BPM} b/m" "$B_BPM" "$C_BPM"
echo ""
printf "  %-28s %15s %15s %10s\n" "  RPC latency (collect→recv)" "" "" ""
print_row "  median" "${B_MEDIAN}ms" "${C_MEDIAN}ms" "$B_MEDIAN" "$C_MEDIAN"
print_row "  p95" "${B_P95}ms" "${C_P95}ms" "$B_P95" "$C_P95"
print_row "  p99" "${B_P99}ms" "${C_P99}ms" "$B_P99" "$C_P99"
print_row "  mean" "${B_MEAN}ms" "${C_MEAN}ms" "$B_MEAN" "$C_MEAN"
print_row "  max" "${B_MAX}ms" "${C_MAX}ms" "$B_MAX" "$C_MAX"
print_row "  stdev" "${B_STDEV}ms" "${C_STDEV}ms"
echo ""
printf "  %-28s %15s %15s %10s\n" "  Compaction" "" "" ""
print_row "  cycles" "$B_COMP" "$C_COMP"
print_row "  mean" "${B_CMEAN}ms" "${C_CMEAN}ms" "$B_CMEAN" "$C_CMEAN"
print_row "  max" "${B_CMAX}ms" "${C_CMAX}ms" "$B_CMAX" "$C_CMAX"

echo ""
echo "  Per-block CSVs: bench/baseline_blocks.csv, bench/candidate_blocks.csv"
echo "  Raw logs:       bench/baseline.log, bench/candidate.log"
echo ""
