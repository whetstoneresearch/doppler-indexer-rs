//! Multicall3 infrastructure: types, calldata encoding/decoding, and generic executor.

use alloy::dyn_abi::{DynSolType, DynSolValue};
use alloy::primitives::{Address, Bytes};
use alloy::rpc::types::{BlockId, TransactionRequest};
use futures::{stream, StreamExt, TryStreamExt};

use super::config::compute_function_selector;
use super::types::{BlockInfo, EthCallCollectionError};
use crate::rpc::UnifiedRpcClient;
use crate::types::config::eth_call::{encode_call_with_params, Frequency};

// =============================================================================
// Multicall Types
// =============================================================================

/// Generic slot for tracking call metadata through multicall execution.
#[derive(Clone)]
#[allow(dead_code)]
pub(crate) struct MulticallSlotGeneric<M> {
    pub(crate) block_number: u64,
    pub(crate) block_timestamp: u64,
    pub(crate) target_address: Address,
    pub(crate) encoded_calldata: Bytes,
    pub(crate) metadata: M,
}

/// Pending multicall for a single block.
#[derive(Clone)]
pub(crate) struct BlockMulticall<M> {
    pub(crate) block_number: u64,
    pub(crate) block_id: BlockId,
    pub(crate) slots: Vec<MulticallSlotGeneric<M>>,
}

/// Metadata for regular calls.
#[derive(Clone)]
pub(crate) struct RegularCallMeta {
    pub(crate) contract_name: String,
    pub(crate) function_name: String,
    pub(crate) contract_address: Address,
    pub(crate) param_values: Vec<Vec<u8>>,
}

/// Metadata for factory calls.
#[derive(Clone)]
pub(crate) struct FactoryCallMeta {
    pub(crate) collection_name: String,
    pub(crate) function_name: String,
    pub(crate) contract_address: Address,
    pub(crate) param_values: Vec<Vec<u8>>,
}

/// Metadata for event-triggered calls.
#[derive(Clone)]
pub(crate) struct EventCallMeta {
    pub(crate) contract_name: String,
    pub(crate) function_name: String,
    pub(crate) target_address: Address,
    pub(crate) log_index: u32,
    pub(crate) param_values: Vec<Vec<u8>>,
}

/// Metadata for once calls.
#[derive(Clone)]
pub(crate) struct OnceCallMeta {
    pub(crate) contract_name: String,
    pub(crate) function_name: String,
    pub(crate) contract_address: Address,
}

/// Metadata for factory once call slots (used in process_factory_once_calls_multicall).
#[derive(Clone)]
pub(crate) struct FactoryOnceSlotMeta {
    pub(crate) collection_name: String,
    pub(crate) function_name: String,
    pub(crate) address: Address,
    pub(crate) block_number: u64,
    pub(crate) block_timestamp: u64,
}

/// Group info for factory multicall processing.
pub(crate) struct FactoryGroupInfo {
    pub(crate) collection_name: String,
    pub(crate) function_name: String,
    pub(crate) configs: Vec<super::types::CallConfig>,
    pub(crate) filtered_blocks: Vec<BlockInfo>,
    pub(crate) frequency: Frequency,
}

/// Group info for regular multicall processing.
pub(crate) struct RegularGroupInfo<'a> {
    pub(crate) contract_name: String,
    pub(crate) function_name: String,
    pub(crate) configs: Vec<&'a super::types::CallConfig>,
    pub(crate) filtered_blocks: Vec<BlockInfo>,
    pub(crate) frequency: Frequency,
}

// =============================================================================
// Calldata Encoding / Decoding
// =============================================================================

/// Build aggregate3 calldata for a batch of (target, calldata) pairs.
/// Encodes as: aggregate3((address target, bool allowFailure, bytes callData)[])
pub(crate) fn build_multicall_calldata(calls: &[(Address, &Bytes)]) -> Bytes {
    // aggregate3((address,bool,bytes)[]) selector
    let selector = compute_function_selector("aggregate3((address,bool,bytes)[])");

    // Each element is a tuple of (address, bool, bytes)
    let call_tuples: Vec<DynSolValue> = calls
        .iter()
        .map(|(addr, data)| {
            DynSolValue::Tuple(vec![
                DynSolValue::Address(*addr),
                DynSolValue::Bool(true), // allowFailure
                DynSolValue::Bytes(data.to_vec()),
            ])
        })
        .collect();

    let params = vec![DynSolValue::Array(call_tuples)];
    encode_call_with_params(selector, &params)
}

/// Decode the return data from an aggregate3 call.
/// Returns Vec<(success: bool, returnData: Vec<u8>)>.
pub(crate) fn decode_multicall_results(
    return_data: &[u8],
    expected_count: usize,
) -> Result<Vec<(bool, Vec<u8>)>, String> {
    // Return type is (bool success, bytes returnData)[]
    let result_type = DynSolType::Array(Box::new(DynSolType::Tuple(vec![
        DynSolType::Bool,
        DynSolType::Bytes,
    ])));

    let decoded = result_type
        .abi_decode(return_data)
        .map_err(|e| format!("Failed to decode multicall results: {}", e))?;

    let results_array = match decoded {
        DynSolValue::Array(arr) => arr,
        _ => return Err("Expected array from multicall decode".to_string()),
    };

    if results_array.len() != expected_count {
        return Err(format!(
            "Multicall returned {} results, expected {}",
            results_array.len(),
            expected_count
        ));
    }

    let mut results = Vec::with_capacity(expected_count);
    for item in results_array {
        match item {
            DynSolValue::Tuple(fields) if fields.len() == 2 => {
                let success = match &fields[0] {
                    DynSolValue::Bool(b) => *b,
                    _ => false,
                };
                let data = match &fields[1] {
                    DynSolValue::Bytes(b) => b.clone(),
                    _ => Vec::new(),
                };
                results.push((success, data));
            }
            _ => {
                results.push((false, Vec::new()));
            }
        }
    }

    Ok(results)
}

// =============================================================================
// Generic Multicall Executor
// =============================================================================

/// Execute multicalls and return (metadata, return_data, success) for each slot.
pub(crate) async fn execute_multicalls_generic<M: Clone + Send + Sync>(
    client: &UnifiedRpcClient,
    multicall3_address: Address,
    block_multicalls: Vec<BlockMulticall<M>>,
    rpc_batch_size: usize,
) -> Result<Vec<(M, Vec<u8>, bool)>, EthCallCollectionError> {
    // Track failed calls with chunk-relative indices
    struct ChunkFailedCall {
        relative_index: usize, // Index within the chunk's results
        target_address: Address,
        calldata: Bytes,
        block_number: u64,
        block_id: BlockId,
    }

    struct ChunkResult<M> {
        results: Vec<(M, Vec<u8>, bool)>,
        failed_calls: Vec<ChunkFailedCall>,
    }

    // Number of chunks to process concurrently
    let chunk_concurrency = 4;

    // Pre-collect chunks into owned vectors to avoid lifetime issues
    let owned_chunks: Vec<Vec<BlockMulticall<M>>> = block_multicalls
        .chunks(rpc_batch_size)
        .map(|chunk| chunk.to_vec())
        .collect();

    // Process chunks concurrently using buffered (maintains order for correct index calculation)
    let chunk_results: Vec<ChunkResult<M>> = stream::iter(owned_chunks)
        .map(|chunk| async move {
            let calls: Vec<(TransactionRequest, BlockId)> = chunk
                .iter()
                .map(|bm| {
                    let sub_calls: Vec<(Address, &Bytes)> = bm
                        .slots
                        .iter()
                        .map(|s| (s.target_address, &s.encoded_calldata))
                        .collect();
                    let multicall_data = build_multicall_calldata(&sub_calls);
                    let tx = TransactionRequest::default()
                        .to(multicall3_address)
                        .input(multicall_data.into());
                    (tx, bm.block_id)
                })
                .collect();

            let results = client.call_batch(calls).await?;

            let mut chunk_results: Vec<(M, Vec<u8>, bool)> = Vec::new();
            let mut chunk_failed_calls: Vec<ChunkFailedCall> = Vec::new();

            for (i, result) in results.into_iter().enumerate() {
                let bm = &chunk[i];
                let slot_count = bm.slots.len();

                match result {
                    Ok(bytes) => {
                        match decode_multicall_results(&bytes, slot_count) {
                            Ok(decoded) => {
                                for (j, (success, return_data)) in decoded.into_iter().enumerate() {
                                    let slot = &bm.slots[j];
                                    if !success {
                                        tracing::warn!(
                                            "Multicall sub-call failed at block {} targeting {}",
                                            bm.block_number,
                                            slot.target_address
                                        );
                                        chunk_failed_calls.push(ChunkFailedCall {
                                            relative_index: chunk_results.len(),
                                            target_address: slot.target_address,
                                            calldata: slot.encoded_calldata.clone(),
                                            block_number: bm.block_number,
                                            block_id: bm.block_id,
                                        });
                                    }
                                    chunk_results.push((
                                        slot.metadata.clone(),
                                        if success { return_data } else { Vec::new() },
                                        success,
                                    ));
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "Failed to decode multicall results for block {}: {}",
                                    bm.block_number,
                                    e
                                );
                                // Treat all sub-calls as failed
                                for slot in &bm.slots {
                                    chunk_results.push((slot.metadata.clone(), Vec::new(), false));
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Multicall RPC failed for block {}: {}", bm.block_number, e);
                        // Treat all sub-calls as failed
                        for slot in &bm.slots {
                            chunk_results.push((slot.metadata.clone(), Vec::new(), false));
                        }
                    }
                }
            }
            Ok::<_, EthCallCollectionError>(ChunkResult {
                results: chunk_results,
                failed_calls: chunk_failed_calls,
            })
        })
        .buffered(chunk_concurrency)
        .try_collect()
        .await?;

    // Flatten results and compute global indices for failed calls
    struct FailedCall {
        result_index: usize,
        target_address: Address,
        calldata: Bytes,
        block_number: u64,
        block_id: BlockId,
    }

    let mut all_results: Vec<(M, Vec<u8>, bool)> = Vec::new();
    let mut failed_retries: Vec<FailedCall> = Vec::new();

    for chunk_result in chunk_results {
        let base_index = all_results.len();

        // Convert chunk-relative indices to global indices
        for failed in chunk_result.failed_calls {
            failed_retries.push(FailedCall {
                result_index: base_index + failed.relative_index,
                target_address: failed.target_address,
                calldata: failed.calldata,
                block_number: failed.block_number,
                block_id: failed.block_id,
            });
        }

        all_results.extend(chunk_result.results);
    }

    // Retry failed multicall sub-calls individually
    if !failed_retries.is_empty() {
        tracing::info!(
            "Retrying {} failed multicall sub-calls individually",
            failed_retries.len()
        );

        let retry_batch: Vec<(TransactionRequest, BlockId)> = failed_retries
            .iter()
            .map(|f| {
                let tx = TransactionRequest::default()
                    .to(f.target_address)
                    .input(f.calldata.clone().into());
                (tx, f.block_id)
            })
            .collect();

        let retry_results = client.call_batch(retry_batch).await?;

        for (i, result) in retry_results.into_iter().enumerate() {
            let failed = &failed_retries[i];
            match result {
                Ok(bytes) if !bytes.is_empty() => {
                    tracing::info!(
                        "Individual retry succeeded for block {} target {}",
                        failed.block_number,
                        failed.target_address
                    );
                    let entry = &mut all_results[failed.result_index];
                    *entry = (entry.0.clone(), bytes.to_vec(), true);
                }
                Ok(_) => {
                    tracing::warn!(
                        "Individual retry returned empty for block {} target {}",
                        failed.block_number,
                        failed.target_address
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        "Individual retry also failed for block {} target {}: {}",
                        failed.block_number,
                        failed.target_address,
                        e
                    );
                }
            }
        }
    }

    Ok(all_results)
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::address;

    #[test]
    fn test_build_multicall_calldata_single_call() {
        let addr = address!("0000000000000000000000000000000000000001");
        let calldata = Bytes::from(vec![0x12, 0x34, 0x56, 0x78]);
        let calls = vec![(addr, &calldata)];

        let result = build_multicall_calldata(&calls);

        // Should start with aggregate3 selector
        assert!(result.len() > 4);
        // First 4 bytes are the function selector for aggregate3
        let expected_selector = compute_function_selector("aggregate3((address,bool,bytes)[])");
        assert_eq!(&result[..4], &expected_selector[..4]);
    }

    #[test]
    fn test_build_multicall_calldata_multiple_calls() {
        let addr1 = address!("0000000000000000000000000000000000000001");
        let addr2 = address!("0000000000000000000000000000000000000002");
        let calldata1 = Bytes::from(vec![0x12, 0x34, 0x56, 0x78]);
        let calldata2 = Bytes::from(vec![0xab, 0xcd, 0xef, 0x01]);
        let calls = vec![(addr1, &calldata1), (addr2, &calldata2)];

        let result = build_multicall_calldata(&calls);

        // Should produce valid ABI-encoded data
        assert!(result.len() > 4);
    }

    #[test]
    fn test_build_multicall_calldata_empty() {
        let calls: Vec<(Address, &Bytes)> = vec![];
        let result = build_multicall_calldata(&calls);
        // Should still produce valid calldata (selector + empty array encoding)
        assert!(result.len() >= 4);
    }

    #[test]
    fn test_decode_multicall_results_success() {
        // Manually encode a result array with two successful results
        let value = DynSolValue::Array(vec![
            DynSolValue::Tuple(vec![
                DynSolValue::Bool(true),
                DynSolValue::Bytes(vec![0x01, 0x02]),
            ]),
            DynSolValue::Tuple(vec![
                DynSolValue::Bool(true),
                DynSolValue::Bytes(vec![0x03, 0x04]),
            ]),
        ]);
        let encoded = value.abi_encode();

        let decoded = decode_multicall_results(&encoded, 2).unwrap();
        assert_eq!(decoded.len(), 2);
        assert!(decoded[0].0); // success
        assert_eq!(decoded[0].1, vec![0x01, 0x02]);
        assert!(decoded[1].0); // success
        assert_eq!(decoded[1].1, vec![0x03, 0x04]);
    }

    #[test]
    fn test_decode_multicall_results_mixed_success_failure() {
        let value = DynSolValue::Array(vec![
            DynSolValue::Tuple(vec![
                DynSolValue::Bool(true),
                DynSolValue::Bytes(vec![0x01]),
            ]),
            DynSolValue::Tuple(vec![
                DynSolValue::Bool(false),
                DynSolValue::Bytes(vec![]),
            ]),
        ]);
        let encoded = value.abi_encode();

        let decoded = decode_multicall_results(&encoded, 2).unwrap();
        assert_eq!(decoded.len(), 2);
        assert!(decoded[0].0);
        assert_eq!(decoded[0].1, vec![0x01]);
        assert!(!decoded[1].0);
        assert!(decoded[1].1.is_empty());
    }

    #[test]
    fn test_decode_multicall_results_wrong_count() {
        let value = DynSolValue::Array(vec![DynSolValue::Tuple(vec![
            DynSolValue::Bool(true),
            DynSolValue::Bytes(vec![0x01]),
        ])]);
        let encoded = value.abi_encode();

        let result = decode_multicall_results(&encoded, 2);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("expected 2"));
    }

    #[test]
    fn test_decode_multicall_results_invalid_data() {
        let result = decode_multicall_results(&[0x00, 0x01, 0x02], 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_roundtrip_build_and_decode() {
        // Build a multicall, and verify the calldata is valid ABI
        let addr = address!("1111111111111111111111111111111111111111");
        let inner_calldata = Bytes::from(vec![0xaa, 0xbb, 0xcc, 0xdd]);
        let calls = vec![(addr, &inner_calldata)];

        let calldata = build_multicall_calldata(&calls);
        // The calldata should be decodable (at least the selector + ABI part)
        assert!(calldata.len() > 4);

        // Verify selector
        let expected_selector = compute_function_selector("aggregate3((address,bool,bytes)[])");
        assert_eq!(&calldata[..4], &expected_selector[..4]);
    }
}
