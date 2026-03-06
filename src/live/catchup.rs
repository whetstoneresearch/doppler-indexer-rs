//! Catchup service for incomplete blocks in live mode.
//!
//! On restart, live mode may have blocks that were partially processed:
//! - Blocks collected but not decoded (decoder crashed)
//! - Blocks decoded but not transformed (engine crashed)
//! - Blocks partially transformed (some handlers ran, others didn't)
//!
//! This service scans storage for incomplete blocks and replays them
//! through the appropriate pipeline stages.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use alloy::primitives::{Address, B256};
use tokio::sync::mpsc;
use tokio_postgres::types::ToSql;

use super::storage::{LiveStorage, StorageError};
use super::types::LiveBlockStatus;
use crate::db::DbPool;
use crate::decoding::{DecoderMessage, EthCallResult};
use crate::raw_data::historical::receipts::LogData;

/// Result of scanning for incomplete blocks.
#[derive(Debug, Default)]
pub struct CatchupScanResult {
    /// Blocks that need log decoding (have raw logs but logs_decoded=false).
    pub blocks_needing_log_decode: Vec<u64>,
    /// Blocks that need eth_call decoding (have raw eth_calls but eth_calls_decoded=false).
    pub blocks_needing_call_decode: Vec<u64>,
    /// Blocks that need transformation, with the set of handlers that still need to run.
    /// (block_number, missing_handler_keys)
    pub blocks_needing_transform: Vec<(u64, HashSet<String>)>,
}

impl CatchupScanResult {
    /// Check if any catchup work is needed.
    pub fn is_empty(&self) -> bool {
        self.blocks_needing_log_decode.is_empty()
            && self.blocks_needing_call_decode.is_empty()
            && self.blocks_needing_transform.is_empty()
    }

    /// Total number of blocks needing some form of catchup.
    pub fn total_blocks(&self) -> usize {
        let mut blocks: HashSet<u64> = HashSet::new();
        blocks.extend(&self.blocks_needing_log_decode);
        blocks.extend(&self.blocks_needing_call_decode);
        blocks.extend(self.blocks_needing_transform.iter().map(|(b, _)| *b));
        blocks.len()
    }
}

/// Service for catching up incomplete blocks on restart.
pub struct LiveCatchupService {
    storage: LiveStorage,
    registered_handlers: HashSet<String>,
    chain_id: i64,
    db_pool: Option<Arc<DbPool>>,
}

impl LiveCatchupService {
    /// Create a new catchup service.
    pub fn new(chain_name: &str, registered_handlers: HashSet<String>) -> Self {
        Self {
            storage: LiveStorage::new(chain_name),
            registered_handlers,
            chain_id: 0,
            db_pool: None,
        }
    }

    /// Create a new catchup service with database access for status reconstruction.
    pub fn with_db(
        chain_name: &str,
        chain_id: i64,
        registered_handlers: HashSet<String>,
        db_pool: Arc<DbPool>,
    ) -> Self {
        Self {
            storage: LiveStorage::new(chain_name),
            registered_handlers,
            chain_id,
            db_pool: Some(db_pool),
        }
    }

    /// Reconstruct missing status files for blocks that have data but no status.
    ///
    /// Checks local storage for existing data files and queries the database
    /// for completed handler progress. This enables recovery after status files
    /// are lost or deleted.
    pub async fn reconstruct_missing_status_files(&self) -> Result<usize, StorageError> {
        let blocks = self.storage.list_blocks()?;
        let mut reconstructed = 0;

        for block_number in blocks {
            // Check if status file exists
            match self.storage.read_status(block_number) {
                Ok(_) => continue, // Status exists, skip
                Err(StorageError::NotFound(_)) => {
                    // Status missing, reconstruct it
                }
                Err(e) => return Err(e),
            }

            // Build status from what data exists
            let status = self.reconstruct_status(block_number).await?;
            self.storage.write_status(block_number, &status)?;

            tracing::info!(
                "Reconstructed status file for block {} (collected={}, logs_decoded={}, eth_calls_decoded={}, transformed={})",
                block_number,
                status.collected,
                status.logs_decoded,
                status.eth_calls_decoded,
                status.transformed
            );
            reconstructed += 1;
        }

        if reconstructed > 0 {
            tracing::info!(
                "Reconstructed {} missing status files from local storage and database",
                reconstructed
            );
        }

        Ok(reconstructed)
    }

    /// Reconstruct a status struct by checking what data exists for a block.
    async fn reconstruct_status(&self, block_number: u64) -> Result<LiveBlockStatus, StorageError> {
        let mut status = LiveBlockStatus::default();

        // Check raw data presence
        let block_exists = self.storage.read_block(block_number).is_ok();
        let receipts_exist = self.storage.read_receipts(block_number).is_ok();
        let logs_exist = self.storage.read_logs(block_number).is_ok();
        let eth_calls_exist = self.storage.read_eth_calls(block_number).is_ok();
        let factories_exist = self.storage.read_factories(block_number).is_ok();

        // If we have the block, mark collection phases as complete
        if block_exists {
            status.collected = true;
            status.block_fetched = true;
        }
        if receipts_exist {
            status.receipts_collected = true;
        }
        if logs_exist {
            status.logs_collected = true;
        }
        // Factories are optional - mark as extracted if we have logs (extraction happens during collection)
        if logs_exist {
            status.factories_extracted = true;
        }

        // Check for decoded data
        let decoded_logs_exist = !self.storage.list_decoded_log_types(block_number)?.is_empty();
        let decoded_calls_exist = !self.storage.list_decoded_call_types(block_number)?.is_empty();

        // Check if raw logs are empty (no events to decode)
        let logs_empty = if logs_exist {
            self.storage.read_logs(block_number).map(|l| l.is_empty()).unwrap_or(false)
        } else {
            true
        };

        // If decoded data exists, mark as decoded
        // If no logs or logs are empty, no decoding needed
        if decoded_logs_exist || !logs_exist || logs_empty {
            status.logs_decoded = true;
        }

        // Check if raw eth_calls are empty (no calls to decode)
        let eth_calls_empty = if eth_calls_exist {
            self.storage.read_eth_calls(block_number).map(|c| c.is_empty()).unwrap_or(false)
        } else {
            true
        };

        // For eth_calls: mark as collected/decoded appropriately
        // If decoded data exists, mark as decoded
        // If no eth_calls or eth_calls are empty, no decoding needed
        status.eth_calls_collected = eth_calls_exist;
        if decoded_calls_exist || !eth_calls_exist || eth_calls_empty {
            status.eth_calls_decoded = true;
        }

        // Query database for completed handlers
        if let Some(ref db_pool) = self.db_pool {
            let block_num_i64 = block_number as i64;
            match db_pool
                .query(
                    "SELECT handler_key FROM _live_progress WHERE chain_id = $1 AND block_number = $2",
                    &[
                        &self.chain_id as &(dyn ToSql + Sync),
                        &block_num_i64 as &(dyn ToSql + Sync),
                    ],
                )
                .await
            {
                Ok(rows) => {
                    for row in rows {
                        let handler_key: String = row.get(0);
                        status.completed_handlers.insert(handler_key);
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to query _live_progress for block {}: {}",
                        block_number,
                        e
                    );
                }
            }
        }

        // Check if all handlers are complete
        if status.logs_decoded && status.eth_calls_decoded {
            if self.registered_handlers.is_empty() {
                // No handlers registered, transformation is complete
                status.transformed = true;
            } else if status.completed_handlers.len() == self.registered_handlers.len()
                && self
                    .registered_handlers
                    .iter()
                    .all(|h| status.completed_handlers.contains(h))
            {
                status.transformed = true;
            }
        }

        Ok(status)
    }

    /// Scan storage to identify blocks that need catchup.
    ///
    /// Checks each block's status to determine what pipeline stages are incomplete.
    pub fn scan_incomplete_blocks(&self) -> Result<CatchupScanResult, StorageError> {
        let mut result = CatchupScanResult::default();

        let blocks = self.storage.list_blocks()?;
        if blocks.is_empty() {
            tracing::debug!("Catchup scan: no blocks in storage");
            return Ok(result);
        }

        tracing::debug!(
            "Catchup scan: checking {} blocks ({} to {})",
            blocks.len(),
            blocks.first().unwrap_or(&0),
            blocks.last().unwrap_or(&0)
        );

        for block_number in blocks {
            let status = match self.storage.read_status(block_number) {
                Ok(s) => s,
                Err(StorageError::NotFound(_)) => {
                    // Block exists but no status - needs full processing
                    // This shouldn't happen normally, but handle gracefully
                    tracing::warn!(
                        "Block {} has data but no status file, skipping catchup",
                        block_number
                    );
                    continue;
                }
                Err(e) => return Err(e),
            };

            // Skip blocks that haven't completed collection phase
            if !status.logs_collected {
                tracing::debug!(
                    "Block {} has incomplete collection, skipping catchup (will be re-collected)",
                    block_number
                );
                continue;
            }

            // Check for incomplete log decoding
            if !status.logs_decoded {
                result.blocks_needing_log_decode.push(block_number);
            }

            // Check for incomplete eth_call decoding
            if status.eth_calls_collected && !status.eth_calls_decoded {
                result.blocks_needing_call_decode.push(block_number);
            }

            // Check for incomplete transformation
            if status.logs_decoded && status.eth_calls_decoded && !status.transformed {
                let missing_handlers = self.get_missing_handlers(&status);
                if !missing_handlers.is_empty() {
                    result
                        .blocks_needing_transform
                        .push((block_number, missing_handlers));
                }
            }
        }

        // Sort blocks in ascending order for sequential processing
        result.blocks_needing_log_decode.sort_unstable();
        result.blocks_needing_call_decode.sort_unstable();
        result
            .blocks_needing_transform
            .sort_unstable_by_key(|(b, _)| *b);

        Ok(result)
    }

    /// Get the set of handlers that haven't completed for a block.
    fn get_missing_handlers(&self, status: &LiveBlockStatus) -> HashSet<String> {
        self.registered_handlers
            .difference(&status.completed_handlers)
            .cloned()
            .collect()
    }

    /// Replay raw logs for blocks that need decoding.
    ///
    /// Sends LogsReady messages to the decoder channel for each block.
    pub async fn replay_logs_for_decode(
        &self,
        blocks: &[u64],
        decoder_tx: &mpsc::Sender<DecoderMessage>,
    ) -> Result<usize, StorageError> {
        if blocks.is_empty() {
            return Ok(0);
        }

        tracing::info!(
            "Catchup: replaying logs for {} blocks ({} to {})",
            blocks.len(),
            blocks.first().unwrap_or(&0),
            blocks.last().unwrap_or(&0)
        );

        let mut replayed = 0;

        for &block_number in blocks {
            // Read raw logs
            let logs = match self.storage.read_logs(block_number) {
                Ok(l) => l,
                Err(StorageError::NotFound(_)) => {
                    tracing::warn!(
                        "Block {} marked for log decode but logs not found, skipping",
                        block_number
                    );
                    continue;
                }
                Err(e) => return Err(e),
            };

            // Read block for timestamp
            let block = self.storage.read_block(block_number)?;

            // Check for factory addresses
            let factory_addresses = self
                .storage
                .read_factories(block_number)
                .unwrap_or_default();

            // Send factory addresses first if present
            if !factory_addresses.addresses_by_collection.is_empty() {
                let factory_addrs: HashMap<String, Vec<Address>> = factory_addresses
                    .addresses_by_collection
                    .iter()
                    .map(|(name, addrs)| {
                        (
                            name.clone(),
                            addrs.iter().map(|(_, addr)| Address::from(*addr)).collect(),
                        )
                    })
                    .collect();

                if let Err(e) = decoder_tx
                    .send(DecoderMessage::FactoryAddresses {
                        range_start: block_number,
                        range_end: block_number + 1,
                        addresses: factory_addrs,
                    })
                    .await
                {
                    tracing::warn!(
                        "Failed to send factory addresses for block {} during catchup: {}",
                        block_number,
                        e
                    );
                }
            }

            // Convert to LogData format
            let log_data: Vec<LogData> = logs
                .iter()
                .map(|log| LogData {
                    block_number,
                    block_timestamp: block.timestamp,
                    transaction_hash: B256::from(log.transaction_hash),
                    log_index: log.log_index,
                    address: log.address,
                    topics: log.topics.clone(),
                    data: log.data.clone(),
                })
                .collect();

            // Send to decoder
            if let Err(e) = decoder_tx
                .send(DecoderMessage::LogsReady {
                    range_start: block_number,
                    range_end: block_number + 1,
                    logs: log_data,
                    live_mode: true,
                    has_factory_matchers: !factory_addresses.addresses_by_collection.is_empty(),
                })
                .await
            {
                tracing::warn!(
                    "Failed to send logs for block {} during catchup: {}",
                    block_number,
                    e
                );
                continue;
            }

            replayed += 1;
            tracing::debug!(
                "Replayed {} logs for block {} to decoder",
                logs.len(),
                block_number
            );
        }

        Ok(replayed)
    }

    /// Replay raw eth_calls for blocks that need decoding.
    ///
    /// Sends eth_call messages to the decoder channel for each block.
    pub async fn replay_calls_for_decode(
        &self,
        blocks: &[u64],
        decoder_tx: &mpsc::Sender<DecoderMessage>,
    ) -> Result<usize, StorageError> {
        if blocks.is_empty() {
            return Ok(0);
        }

        tracing::info!(
            "Catchup: replaying eth_calls for {} blocks ({} to {})",
            blocks.len(),
            blocks.first().unwrap_or(&0),
            blocks.last().unwrap_or(&0)
        );

        let mut replayed = 0;

        for &block_number in blocks {
            // Read raw eth_calls
            let calls = match self.storage.read_eth_calls(block_number) {
                Ok(c) => c,
                Err(StorageError::NotFound(_)) => {
                    tracing::debug!(
                        "Block {} marked for call decode but no eth_calls found, skipping",
                        block_number
                    );
                    continue;
                }
                Err(e) => return Err(e),
            };

            if calls.is_empty() {
                continue;
            }

            // Group calls by (contract_name, function_name)
            let mut grouped: HashMap<(String, String), Vec<EthCallResult>> = HashMap::new();
            for call in calls {
                grouped
                    .entry((call.contract_name.clone(), call.function_name.clone()))
                    .or_default()
                    .push(EthCallResult {
                        block_number: call.block_number,
                        block_timestamp: call.block_timestamp,
                        contract_address: call.contract_address,
                        value: call.result,
                    });
            }

            // Send each group to decoder
            for ((contract_name, function_name), results) in grouped {
                if let Err(e) = decoder_tx
                    .send(DecoderMessage::EthCallsReady {
                        range_start: block_number,
                        range_end: block_number + 1,
                        contract_name: contract_name.clone(),
                        function_name: function_name.clone(),
                        results,
                        live_mode: true,
                    })
                    .await
                {
                    tracing::warn!(
                        "Failed to send eth_calls for block {} ({}/{}) during catchup: {}",
                        block_number,
                        contract_name,
                        function_name,
                        e
                    );
                }
            }

            replayed += 1;
            tracing::debug!("Replayed eth_calls for block {} to decoder", block_number);
        }

        Ok(replayed)
    }

    /// Replay logs for blocks that need transformation retry.
    ///
    /// This clears the logs_decoded flag and replays through the decoder,
    /// which will re-send to the transform engine.
    pub async fn replay_for_transform(
        &self,
        blocks: &[(u64, HashSet<String>)],
        decoder_tx: &mpsc::Sender<DecoderMessage>,
    ) -> Result<usize, StorageError> {
        if blocks.is_empty() {
            return Ok(0);
        }

        let block_numbers: Vec<u64> = blocks.iter().map(|(b, _)| *b).collect();

        tracing::info!(
            "Catchup: retrying transformation for {} blocks ({} to {})",
            block_numbers.len(),
            block_numbers.first().unwrap_or(&0),
            block_numbers.last().unwrap_or(&0)
        );

        // Clear logs_decoded flag so decoder will re-process and send to transform engine
        for &block_number in &block_numbers {
            if let Ok(mut status) = self.storage.read_status(block_number) {
                status.logs_decoded = false;
                status.transformed = false;
                if let Err(e) = self.storage.write_status(block_number, &status) {
                    tracing::warn!(
                        "Failed to clear logs_decoded for block {} during transform retry: {}",
                        block_number,
                        e
                    );
                }
            }
        }

        // Replay through decoder - it will re-decode and send to transform engine
        self.replay_logs_for_decode(&block_numbers, decoder_tx).await
    }

    /// Get the storage reference for external use.
    pub fn storage(&self) -> &LiveStorage {
        &self.storage
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_storage() -> (LiveStorage, TempDir) {
        let tmp = TempDir::new().unwrap();
        let storage = LiveStorage::with_base_dir(tmp.path().to_path_buf());
        storage.ensure_dirs().unwrap();
        (storage, tmp)
    }

    #[test]
    fn test_scan_empty_storage() {
        let (storage, _tmp) = test_storage();
        // Override storage with test storage
        let service = LiveCatchupService {
            storage,
            registered_handlers: HashSet::from(["handler_a".to_string()]),
            chain_id: 1,
            db_pool: None,
        };

        let result = service.scan_incomplete_blocks().unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_scan_complete_blocks() {
        let (storage, _tmp) = test_storage();

        // Write a complete block
        let block = super::super::types::LiveBlock {
            number: 100,
            hash: [1u8; 32],
            parent_hash: [0u8; 32],
            timestamp: 1000,
            tx_hashes: vec![],
        };
        storage.write_block(&block).unwrap();

        let mut status = LiveBlockStatus::default();
        status.collected = true;
        status.block_fetched = true;
        status.receipts_collected = true;
        status.logs_collected = true;
        status.factories_extracted = true;
        status.eth_calls_collected = true;
        status.logs_decoded = true;
        status.eth_calls_decoded = true;
        status.transformed = true;
        status.completed_handlers.insert("handler_a".to_string());
        storage.write_status(100, &status).unwrap();

        let service = LiveCatchupService {
            storage,
            registered_handlers: HashSet::from(["handler_a".to_string()]),
            chain_id: 1,
            db_pool: None,
        };

        let result = service.scan_incomplete_blocks().unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_scan_needs_log_decode() {
        let (storage, _tmp) = test_storage();

        // Write a block needing log decode
        let block = super::super::types::LiveBlock {
            number: 100,
            hash: [1u8; 32],
            parent_hash: [0u8; 32],
            timestamp: 1000,
            tx_hashes: vec![],
        };
        storage.write_block(&block).unwrap();

        let mut status = LiveBlockStatus::default();
        status.collected = true;
        status.block_fetched = true;
        status.receipts_collected = true;
        status.logs_collected = true;
        status.factories_extracted = true;
        status.eth_calls_collected = true;
        status.logs_decoded = false; // Not decoded
        status.eth_calls_decoded = true;
        status.transformed = false;
        storage.write_status(100, &status).unwrap();

        let service = LiveCatchupService {
            storage,
            registered_handlers: HashSet::from(["handler_a".to_string()]),
            chain_id: 1,
            db_pool: None,
        };

        let result = service.scan_incomplete_blocks().unwrap();
        assert_eq!(result.blocks_needing_log_decode, vec![100]);
        assert!(result.blocks_needing_call_decode.is_empty());
        assert!(result.blocks_needing_transform.is_empty());
    }

    #[test]
    fn test_scan_needs_transform() {
        let (storage, _tmp) = test_storage();

        // Write a block needing transformation
        let block = super::super::types::LiveBlock {
            number: 100,
            hash: [1u8; 32],
            parent_hash: [0u8; 32],
            timestamp: 1000,
            tx_hashes: vec![],
        };
        storage.write_block(&block).unwrap();

        let mut status = LiveBlockStatus::default();
        status.collected = true;
        status.block_fetched = true;
        status.receipts_collected = true;
        status.logs_collected = true;
        status.factories_extracted = true;
        status.eth_calls_collected = true;
        status.logs_decoded = true;
        status.eth_calls_decoded = true;
        status.transformed = false;
        status.completed_handlers.insert("handler_a".to_string());
        storage.write_status(100, &status).unwrap();

        let service = LiveCatchupService {
            storage,
            registered_handlers: HashSet::from([
                "handler_a".to_string(),
                "handler_b".to_string(),
            ]),
            chain_id: 1,
            db_pool: None,
        };

        let result = service.scan_incomplete_blocks().unwrap();
        assert!(result.blocks_needing_log_decode.is_empty());
        assert!(result.blocks_needing_call_decode.is_empty());
        assert_eq!(result.blocks_needing_transform.len(), 1);

        let (block_num, missing) = &result.blocks_needing_transform[0];
        assert_eq!(*block_num, 100);
        assert_eq!(missing.len(), 1);
        assert!(missing.contains("handler_b"));
    }

    #[tokio::test]
    async fn test_reconstruct_missing_status() {
        let (storage, _tmp) = test_storage();

        // Create a block with data but no status file
        let block = super::super::types::LiveBlock {
            number: 100,
            hash: [1u8; 32],
            parent_hash: [0u8; 32],
            timestamp: 1000,
            tx_hashes: vec![],
        };
        storage.write_block(&block).unwrap();
        storage.write_receipts(100, &[]).unwrap();
        storage.write_logs(100, &[]).unwrap();

        // Verify no status file exists
        assert!(storage.read_status(100).is_err());

        let service = LiveCatchupService {
            storage: storage.clone(),
            registered_handlers: HashSet::new(),
            chain_id: 1,
            db_pool: None,
        };

        // Reconstruct missing status files
        let count = service.reconstruct_missing_status_files().await.unwrap();
        assert_eq!(count, 1);

        // Verify status file was created
        let status = storage.read_status(100).unwrap();
        assert!(status.collected);
        assert!(status.block_fetched);
        assert!(status.receipts_collected);
        assert!(status.logs_collected);
        // With no handlers registered, transformed should be true
        assert!(status.transformed);
    }
}
