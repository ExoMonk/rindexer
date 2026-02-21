use alloy::primitives::U64;
use lru::LruCache;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use crate::database::clickhouse::client::ClickhouseClient;
use crate::database::generate::generate_indexer_contract_schema_name;
use crate::database::postgres::generate::{
    generate_internal_event_table_name, generate_internal_event_table_name_no_shorten,
};
use crate::event::config::EventProcessingConfig;
use crate::helpers::camel_to_snake;
use crate::indexer::fetch_logs::{BlockMeta, ReorgInfo};
use crate::metrics::indexing as metrics;
use crate::notifications::ChainStateNotification;
use crate::provider::JsonRpcCachedProvider;
use crate::PostgresClient;

/// Handles chain state notifications (reorgs, reverts, commits).
/// Used by reth feature-gated providers that emit chain state events.
/// Returns `Some(ReorgInfo)` when a reorg/revert is detected, so the caller
/// can forward it to the main indexing loop for recovery.
pub fn handle_chain_notification(
    notification: ChainStateNotification,
    info_log_name: &str,
    network: &str,
) -> Option<ReorgInfo> {
    match notification {
        ChainStateNotification::Reorged {
            revert_from_block,
            revert_to_block,
            new_from_block,
            new_to_block,
            new_tip_hash,
        } => {
            let depth = revert_from_block.saturating_sub(revert_to_block);
            metrics::record_reorg(network, depth);

            warn!(
                "{} - REORG (reth): revert blocks {} to {}, re-index {} to {} (new tip: {})",
                info_log_name,
                revert_from_block,
                revert_to_block,
                new_from_block,
                new_to_block,
                new_tip_hash
            );

            Some(ReorgInfo { fork_block: U64::from(revert_to_block), depth })
        }
        ChainStateNotification::Reverted { from_block, to_block } => {
            let depth = from_block.saturating_sub(to_block);
            metrics::record_reorg(network, depth);

            warn!(
                "{} - CHAIN REVERTED (reth): blocks {} to {} have been reverted",
                info_log_name, from_block, to_block
            );

            Some(ReorgInfo { fork_block: U64::from(to_block), depth })
        }
        ChainStateNotification::Committed { from_block, to_block, tip_hash } => {
            debug!(
                "{} - Chain committed: blocks {} to {} (tip: {})",
                info_log_name, from_block, to_block, tip_hash
            );
            None
        }
    }
}

pub fn reorg_safe_distance_for_chain(chain_id: u64) -> U64 {
    if chain_id == 1 {
        U64::from(12)
    } else {
        U64::from(64)
    }
}

/// Walk backwards from the reorged block to find the fork point.
///
/// Compares cached block hashes with current canonical chain hashes from the RPC.
/// Returns the first block number that diverged (i.e., the fork point).
pub async fn find_fork_point(
    block_cache: &LruCache<u64, BlockMeta>,
    provider: &Arc<JsonRpcCachedProvider>,
    reorged_block: u64,
) -> u64 {
    // Collect cached block numbers walking backwards from just before the reorg.
    // Cap scan at cache size to avoid iterating millions of empty slots.
    let mut blocks_to_check: Vec<U64> = Vec::new();
    let max_scan = block_cache.len() + 64; // allow gaps between cached blocks
    let scan_start = reorged_block.saturating_sub(1);
    let scan_end = scan_start.saturating_sub(max_scan as u64);
    for block_num in (scan_end..=scan_start).rev() {
        if block_cache.peek(&block_num).is_some() {
            blocks_to_check.push(U64::from(block_num));
        }
        if blocks_to_check.len() >= 64 {
            break;
        }
    }

    if blocks_to_check.is_empty() {
        warn!("No cached blocks to compare for fork point discovery, using reorged_block");
        return reorged_block;
    }

    match provider.get_block_by_number_batch(&blocks_to_check, false).await {
        Ok(canonical_blocks) => {
            // Check each canonical block against our cache (newest first)
            for block in canonical_blocks {
                let block_num = block.header.number;
                let canonical_hash = block.header.hash;

                if let Some(cached) = block_cache.peek(&block_num) {
                    if cached.hash == canonical_hash {
                        info!(
                            "Fork point found: block {} matches canonical chain, fork at {}",
                            block_num,
                            block_num + 1
                        );
                        return block_num + 1;
                    }
                }
            }

            let oldest = blocks_to_check.last().map(|b| b.to::<u64>()).unwrap_or(reorged_block);
            warn!(
                "Could not find matching block in cache (checked {} blocks), using oldest: {}",
                blocks_to_check.len(),
                oldest
            );
            oldest
        }
        Err(e) => {
            error!("Failed to fetch blocks for fork point discovery: {:?}", e);
            reorged_block.saturating_sub(1)
        }
    }
}

/// Handles reorg recovery: deletes orphaned events from storage and rewinds the checkpoint.
pub async fn handle_reorg_recovery(config: &Arc<EventProcessingConfig>, reorg: &ReorgInfo) {
    let fork_block = reorg.fork_block.to::<u64>();
    let network = &config.network_contract().network;
    let indexer_name = config.indexer_name();
    let contract_name = config.contract_name();
    let event_name = config.event_name();
    let schema = generate_indexer_contract_schema_name(&indexer_name, &contract_name);
    let event_table_name = camel_to_snake(&event_name);
    let rewind_block = fork_block.saturating_sub(1);

    info!(
        "Reorg recovery: deleting events from block >= {} for {}.{} on {} (depth={})",
        fork_block, schema, event_table_name, network, reorg.depth
    );

    if let Some(postgres) = &config.postgres() {
        delete_events_postgres(postgres, &schema, &event_table_name, fork_block, network).await;
        rewind_checkpoint_postgres(postgres, &schema, &event_name, rewind_block, network).await;
    }

    if let Some(clickhouse) = &config.clickhouse() {
        delete_events_clickhouse(clickhouse, &schema, &event_table_name, fork_block).await;
        rewind_checkpoint_clickhouse(clickhouse, &schema, &event_name, rewind_block, network).await;
    }

    info!(
        "Reorg recovery complete: checkpoint rewound to block {} for {}.{}",
        rewind_block, schema, event_table_name
    );
}

async fn delete_events_postgres(
    postgres: &Arc<PostgresClient>,
    schema: &str,
    event_table: &str,
    fork_block: u64,
    network: &str,
) {
    let full_table = format!("{}.{}", schema, event_table);
    let query = format!(
        "DELETE FROM {} WHERE block_number >= {} AND network = '{}'",
        full_table, fork_block, network
    );

    match postgres.batch_execute(&query).await {
        Ok(_) => info!("PostgreSQL: deleted events from block >= {} in {}", fork_block, full_table),
        Err(e) => error!("PostgreSQL: failed to delete reorged events: {:?}", e),
    }
}

async fn delete_events_clickhouse(
    clickhouse: &Arc<ClickhouseClient>,
    schema: &str,
    event_table: &str,
    fork_block: u64,
) {
    let full_table = format!("{}.{}", schema, event_table);
    // mutations_sync = 1 makes the DELETE synchronous — waits for completion before returning.
    // Without this, rindexer can re-index and insert new events before the old ones are deleted.
    let query = format!(
        "ALTER TABLE {} DELETE WHERE block_number >= {} SETTINGS mutations_sync = 1",
        full_table, fork_block
    );

    match clickhouse.execute(&query).await {
        Ok(_) => {
            info!("ClickHouse: deleted events from block >= {} in {}", fork_block, full_table)
        }
        Err(e) => error!("ClickHouse: failed to delete reorged events: {:?}", e),
    }
}

async fn rewind_checkpoint_postgres(
    postgres: &Arc<PostgresClient>,
    schema: &str,
    event_name: &str,
    rewind_block: u64,
    network: &str,
) {
    let internal_table = generate_internal_event_table_name(schema, event_name);
    let query = format!(
        "UPDATE rindexer_internal.{} SET last_synced_block = {} WHERE network = '{}'",
        internal_table, rewind_block, network
    );

    match postgres.batch_execute(&query).await {
        Ok(_) => info!(
            "PostgreSQL: checkpoint rewound to block {} in rindexer_internal.{}",
            rewind_block, internal_table
        ),
        Err(e) => error!("PostgreSQL: failed to rewind checkpoint: {:?}", e),
    }
}

async fn rewind_checkpoint_clickhouse(
    clickhouse: &Arc<ClickhouseClient>,
    schema: &str,
    event_name: &str,
    rewind_block: u64,
    network: &str,
) {
    let internal_table = generate_internal_event_table_name_no_shorten(schema, event_name);
    let query = format!(
        "INSERT INTO rindexer_internal.{} (network, last_synced_block) VALUES ('{}', {})",
        internal_table, network, rewind_block
    );

    match clickhouse.execute(&query).await {
        Ok(_) => info!(
            "ClickHouse: checkpoint rewound to block {} in rindexer_internal.{}",
            rewind_block, internal_table
        ),
        Err(e) => error!("ClickHouse: failed to rewind checkpoint: {:?}", e),
    }
}

/// Handles reorg recovery for native transfer indexing (PostgreSQL only, no ClickHouse for traces).
pub async fn handle_native_transfer_reorg_recovery(
    postgres: &Option<Arc<PostgresClient>>,
    indexer_name: &str,
    network: &str,
    fork_block: u64,
) {
    let schema = generate_indexer_contract_schema_name(indexer_name, "EvmTraces");
    let event_table_name = "native_transfer";
    let rewind_block = fork_block.saturating_sub(1);

    info!(
        "Native transfer reorg recovery: deleting from block >= {} for {}.{} on {}",
        fork_block, schema, event_table_name, network
    );

    if let Some(pg) = postgres {
        delete_events_postgres(pg, &schema, event_table_name, fork_block, network).await;
        // Checkpoint uses "native_transfer" as event name (hardcoded in last_synced.rs)
        rewind_checkpoint_postgres(pg, &schema, "native_transfer", rewind_block, network).await;
        info!(
            "Native transfer reorg recovery complete: checkpoint rewound to block {} for {}.{}",
            rewind_block, schema, event_table_name
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reorg_safe_distance_for_chain() {
        let mainnet_chain_id = 1;
        assert_eq!(reorg_safe_distance_for_chain(mainnet_chain_id), U64::from(12));

        let testnet_chain_id = 3;
        assert_eq!(reorg_safe_distance_for_chain(testnet_chain_id), U64::from(64));

        let other_chain_id = 42;
        assert_eq!(reorg_safe_distance_for_chain(other_chain_id), U64::from(64));
    }
}
