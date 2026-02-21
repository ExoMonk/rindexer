#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------
# rindexer reorg detection test via Anvil's anvil_reorg RPC method
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

BLOCK_BEFORE_REORG=$(cast block-number --rpc-url "http://127.0.0.1:$ANVIL_PORT" 2>/dev/null)
echo "Block before reorg: $BLOCK_BEFORE_REORG"

# Get hash of current tip (before reorg)
HASH_BEFORE=$(cast block "$BLOCK_BEFORE_REORG" -f hash --rpc-url "http://127.0.0.1:$ANVIL_PORT" 2>/dev/null)
echo "Tip hash before reorg: $HASH_BEFORE"

# ------------------------------------------------------------------
# 4. Trigger anvil_reorg
# ------------------------------------------------------------------
echo ""
echo "=== Triggering anvil_reorg (depth=$REORG_DEPTH) ==="
cast rpc anvil_reorg "$REORG_DEPTH" "[]" --rpc-url "http://127.0.0.1:$ANVIL_PORT" 2>&1
echo "Reorg sent!"

sleep 1

# Verify block hash changed
BLOCK_AFTER_REORG=$(cast block-number --rpc-url "http://127.0.0.1:$ANVIL_PORT" 2>/dev/null)
echo "Block after reorg: $BLOCK_AFTER_REORG"

# The reorged block should have a different hash
REORGED_BLOCK=$((BLOCK_BEFORE_REORG - REORG_DEPTH + 1))
HASH_AFTER=$(cast block "$REORGED_BLOCK" -f hash --rpc-url "http://127.0.0.1:$ANVIL_PORT" 2>/dev/null)
echo "Block $REORGED_BLOCK hash after reorg: $HASH_AFTER"

# ------------------------------------------------------------------
# 5. Wait for rindexer to detect the reorg
# ------------------------------------------------------------------
echo ""
echo "=== Waiting up to 15s for rindexer to detect reorg ==="
DETECTED=false
for i in $(seq 1 15); do
    if grep -qi "reorg" "$LOG_FILE" 2>/dev/null; then
        DETECTED=true
        break
    fi
    printf "\r  Checking... (%d/15s)" "$i"
    sleep 1
done
echo ""

# ------------------------------------------------------------------
# 6. Report results
# ------------------------------------------------------------------
echo ""
echo "============================================"
if $DETECTED; then
    echo "  REORG DETECTED SUCCESSFULLY"
    echo "============================================"
    echo ""
    echo "Reorg-related log lines:"
    grep -i "reorg" "$LOG_FILE" || true
else
    echo "  REORG NOT DETECTED"
    echo "============================================"
    echo ""
    echo "Full rindexer log:"
    cat "$LOG_FILE"
fi
echo ""
echo "Full log available at: $LOG_FILE"

if $DETECTED; then
    exit 0
else
    exit 1
fi
