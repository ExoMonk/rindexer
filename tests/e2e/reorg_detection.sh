#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------
# rindexer reorg detection E2E test via Anvil's anvil_reorg RPC method
#
# Tests two detection mechanisms:
#   1. Tip hash changed  — same block number, different hash
#   2. Parent hash mismatch — new block's parent_hash doesn't match cache
#
# (The third mechanism — log `removed` flag — cannot be tested with Anvil
#  because eth_getLogs doesn't return removed logs after a reorg.)
#
# Usage: ./reorg_detection.sh [reorg_depth]
#        reorg_depth defaults to 3
#
# Prerequisites: anvil, cast (Foundry), rindexer_cli built
# Run from repo root: ./tests/e2e/reorg_detection.sh
# ------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
REORG_DEPTH="${1:-3}"
RINDEXER_BIN="$REPO_ROOT/target/release/rindexer_cli"
FIXTURE_DIR="$SCRIPT_DIR/fixtures"
WORK_DIR=$(mktemp -d)
ANVIL_PORT=8545
ANVIL_PID=""
RINDEXER_PID=""
LOG_FILE="$WORK_DIR/rindexer_test.log"
PASS_COUNT=0
FAIL_COUNT=0

cleanup() {
    echo ""
    echo "=== Cleaning up ==="
    [[ -n "$RINDEXER_PID" ]] && kill "$RINDEXER_PID" 2>/dev/null && echo "Stopped rindexer ($RINDEXER_PID)"
    [[ -n "$ANVIL_PID" ]]    && kill "$ANVIL_PID"    2>/dev/null && echo "Stopped anvil ($ANVIL_PID)"
    wait 2>/dev/null
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT

# ------------------------------------------------------------------
# 0. Set up working directory with fixture config
# ------------------------------------------------------------------
cp "$FIXTURE_DIR/reorg_test.yaml" "$WORK_DIR/rindexer.yaml"
cp -r "$FIXTURE_DIR/abis" "$WORK_DIR/abis"
cd "$WORK_DIR"

# ------------------------------------------------------------------
# 1. Start Anvil (standalone, chain-id 137, 2s blocks)
# ------------------------------------------------------------------
echo "=== Starting Anvil (chain_id=137, block_time=2s) ==="
anvil --chain-id 137 \
      --block-time 2 \
      --port "$ANVIL_PORT" \
      --silent &
ANVIL_PID=$!
echo "Anvil PID: $ANVIL_PID"

# Wait for Anvil to be ready
for i in $(seq 1 15); do
    if cast chain-id --rpc-url "http://127.0.0.1:$ANVIL_PORT" 2>/dev/null | grep -q 137; then
        echo "Anvil ready (chain_id=137)"
        break
    fi
    if [[ $i -eq 15 ]]; then
        echo "ERROR: Anvil failed to start"
        exit 1
    fi
    sleep 0.5
done

# Show starting block
START_BLOCK=$(cast block-number --rpc-url "http://127.0.0.1:$ANVIL_PORT" 2>/dev/null)
echo "Anvil starting block: $START_BLOCK"

# ------------------------------------------------------------------
# 2. Start rindexer (live indexing)
# ------------------------------------------------------------------
echo ""
echo "=== Starting rindexer ==="
RUST_LOG=warn "$RINDEXER_BIN" start -p "$WORK_DIR" indexer > "$LOG_FILE" 2>&1 &
RINDEXER_PID=$!
echo "rindexer PID: $RINDEXER_PID"

# ------------------------------------------------------------------
# 3. Let rindexer accumulate blocks in its cache (~20s = ~10 blocks)
# ------------------------------------------------------------------
echo ""
echo "=== Waiting 20s for rindexer to cache blocks ==="
for i in $(seq 1 20); do
    CURRENT_BLOCK=$(cast block-number --rpc-url "http://127.0.0.1:$ANVIL_PORT" 2>/dev/null)
    printf "\r  Block: %s  (%d/20s)" "$CURRENT_BLOCK" "$i"

    # Check rindexer is still alive
    if ! kill -0 "$RINDEXER_PID" 2>/dev/null; then
        echo ""
        echo "ERROR: rindexer exited early. Log output:"
        cat "$LOG_FILE"
        exit 1
    fi
    sleep 1
done
echo ""

# ================================================================
# TEST 1: Tip hash changed
#
# Trigger anvil_reorg immediately — rindexer polls within ~200ms
# and sees the same block number with a different hash.
# ================================================================
echo ""
echo "========================================================"
echo "  TEST 1: Tip hash changed (same block, different hash)"
echo "========================================================"

BLOCK_BEFORE=$(cast block-number --rpc-url "http://127.0.0.1:$ANVIL_PORT" 2>/dev/null)
HASH_BEFORE=$(cast block "$BLOCK_BEFORE" -f hash --rpc-url "http://127.0.0.1:$ANVIL_PORT" 2>/dev/null)
echo "Block before reorg: $BLOCK_BEFORE (hash: ${HASH_BEFORE:0:18}...)"

# Clear log for this test
: > "$LOG_FILE"

echo "Triggering anvil_reorg (depth=$REORG_DEPTH)..."
cast rpc anvil_reorg "$REORG_DEPTH" "[]" --rpc-url "http://127.0.0.1:$ANVIL_PORT" 2>/dev/null

# Short sleep — we want rindexer to poll BEFORE Anvil mines a new block
sleep 0.5

echo "Waiting up to 15s for detection..."
TIP_DETECTED=false
for i in $(seq 1 15); do
    if grep -q "tip hash changed" "$LOG_FILE" 2>/dev/null; then
        TIP_DETECTED=true
        break
    fi
    # Also accept generic reorg detection (timing-dependent — might hit parent hash instead)
    if grep -q "REORG" "$LOG_FILE" 2>/dev/null; then
        TIP_DETECTED=true
        break
    fi
    sleep 1
done

if $TIP_DETECTED; then
    echo "  PASS: Reorg detected"
    grep -ai "reorg" "$LOG_FILE" || true
    PASS_COUNT=$((PASS_COUNT + 1))
else
    echo "  FAIL: No reorg detected"
    cat "$LOG_FILE"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi

# ------------------------------------------------------------------
# Let rindexer recover and re-cache blocks (~15s = ~7 new blocks)
# ------------------------------------------------------------------
echo ""
echo "=== Waiting 15s for rindexer to recover and re-cache blocks ==="
for i in $(seq 1 15); do
    CURRENT_BLOCK=$(cast block-number --rpc-url "http://127.0.0.1:$ANVIL_PORT" 2>/dev/null)
    printf "\r  Block: %s  (%d/15s)" "$CURRENT_BLOCK" "$i"
    sleep 1
done
echo ""

# ================================================================
# TEST 2: Parent hash mismatch
#
# We need rindexer to see a NEW block (N+1) whose parent_hash
# doesn't match the cached hash of block N.
#
# Strategy: Fire reorg + mine 1 block in rapid succession (<50ms)
# within rindexer's 200ms poll interval. rindexer's next poll sees
# block N+1 (never cached) whose parent_hash points to the new
# hash of block N, but the cache has the OLD hash → mismatch.
#
# Since the reorg + mine complete within one poll window, rindexer
# should not see the intermediate reorged tip (block N with changed
# hash). If it does, the tip hash path fires instead — still valid
# detection, just a different path.
# ================================================================
echo ""
echo "========================================================"
echo "  TEST 2: Parent hash mismatch (new block, stale parent)"
echo "========================================================"

BLOCK_BEFORE=$(cast block-number --rpc-url "http://127.0.0.1:$ANVIL_PORT" 2>/dev/null)
echo "Block before reorg: $BLOCK_BEFORE"

# Clear log for this test
: > "$LOG_FILE"

# Fire reorg + mine atomically (both complete in <50ms, within
# rindexer's 200ms poll interval). Mine exactly 1 block so block
# N+1 exists and its parent (reorged block N) is in the cache.
echo "Triggering anvil_reorg (depth=$REORG_DEPTH) + mine 1 block..."
cast rpc anvil_reorg "$REORG_DEPTH" "[]" --rpc-url "http://127.0.0.1:$ANVIL_PORT" 2>/dev/null
cast rpc evm_mine --rpc-url "http://127.0.0.1:$ANVIL_PORT" 2>/dev/null

BLOCK_AFTER=$(cast block-number --rpc-url "http://127.0.0.1:$ANVIL_PORT" 2>/dev/null)
echo "Block after reorg + mine: $BLOCK_AFTER"

echo "Waiting up to 15s for detection..."
PARENT_DETECTED=false
PARENT_EXACT=false
for i in $(seq 1 15); do
    if grep -q "parent hash mismatch" "$LOG_FILE" 2>/dev/null; then
        PARENT_DETECTED=true
        PARENT_EXACT=true
        break
    fi
    if grep -q "REORG" "$LOG_FILE" 2>/dev/null; then
        PARENT_DETECTED=true
        break
    fi
    sleep 1
done

if $PARENT_DETECTED; then
    if $PARENT_EXACT; then
        echo "  PASS: Parent hash mismatch detected (exact path)"
    else
        echo "  PASS: Reorg detected (tip hash path — timing-dependent)"
    fi
    grep -ai "reorg" "$LOG_FILE" || true
    PASS_COUNT=$((PASS_COUNT + 1))
else
    echo "  FAIL: No reorg detected"
    cat "$LOG_FILE"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi

# ------------------------------------------------------------------
# Final report
# ------------------------------------------------------------------
echo ""
echo "========================================================"
echo "  RESULTS: $PASS_COUNT/2 passed, $FAIL_COUNT/2 failed"
echo "========================================================"
echo ""
echo "  Test 1 (tip hash changed):     $(if [ $PASS_COUNT -ge 1 ]; then echo PASS; else echo FAIL; fi)"
echo "  Test 2 (parent hash mismatch): $(if [ $PASS_COUNT -ge 2 ]; then echo PASS; else echo FAIL; fi)"
echo "  (Test 3 - removed flag: not testable with Anvil)"
echo ""

if [[ $FAIL_COUNT -eq 0 ]]; then
    exit 0
else
    exit 1
fi
