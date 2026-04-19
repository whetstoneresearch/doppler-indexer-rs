//! Arrow schema and parquet writing for decoded Solana events and instructions.
//!
//! Decoded events use a fixed structural schema with parameters serialized as
//! JSON, rather than the per-event-type flattened schema used by the EVM
//! decoder.  This keeps the writer generic across all program/event types.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, FixedSizeBinaryArray, FixedSizeBinaryBuilder, StringArray, UInt16Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::transformations::context::{DecodedAccountState, DecodedEvent};
use crate::types::chain::{ChainAddress, LogPosition, TxId};
use crate::types::decoded::DecodedValue;

/// Build the Arrow schema for decoded Solana event parquet files.
///
/// Schema:
/// - slot: UInt64
/// - block_timestamp: UInt64
/// - transaction_signature: FixedSizeBinary(64)
/// - program_id: FixedSizeBinary(32)
/// - instruction_index: UInt16
/// - inner_instruction_index: UInt16 (nullable)
/// - source_name: Utf8
/// - event_name: Utf8
/// - params: Utf8 (JSON-serialized)
pub fn build_decoded_event_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("block_timestamp", DataType::UInt64, false),
        Field::new(
            "transaction_signature",
            DataType::FixedSizeBinary(64),
            false,
        ),
        Field::new("program_id", DataType::FixedSizeBinary(32), false),
        Field::new("instruction_index", DataType::UInt16, false),
        Field::new("inner_instruction_index", DataType::UInt16, true),
        Field::new("source_name", DataType::Utf8, false),
        Field::new("event_name", DataType::Utf8, false),
        Field::new("params", DataType::Utf8, false),
    ]))
}

/// Build the Arrow schema for decoded Solana account-state parquet files.
pub fn build_decoded_account_state_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("block_timestamp", DataType::UInt64, false),
        Field::new("account_address", DataType::FixedSizeBinary(32), false),
        Field::new("owner_program", DataType::FixedSizeBinary(32), false),
        Field::new("source_name", DataType::Utf8, false),
        Field::new("account_type", DataType::Utf8, false),
        Field::new("fields", DataType::Utf8, false),
    ]))
}

/// Extract the Solana transaction signature bytes from a [`TxId`].
fn tx_signature_bytes(tx_id: &TxId) -> [u8; 64] {
    match tx_id {
        TxId::Solana(sig) => *sig,
        TxId::Evm(_) => [0u8; 64],
    }
}

/// Extract the Solana program ID bytes from a [`ChainAddress`].
fn program_id_bytes(addr: &ChainAddress) -> [u8; 32] {
    match addr {
        ChainAddress::Solana(pubkey) => *pubkey,
        ChainAddress::Evm(_) => [0u8; 32],
    }
}

fn account_address_bytes(addr: &ChainAddress) -> [u8; 32] {
    match addr {
        ChainAddress::Solana(pubkey) => *pubkey,
        ChainAddress::Evm(_) => [0u8; 32],
    }
}

/// Extract instruction_index and inner_instruction_index from a [`LogPosition`].
fn instruction_indices(pos: &LogPosition) -> (u16, Option<u16>) {
    match pos {
        LogPosition::Solana {
            instruction_index,
            inner_instruction_index,
        } => (*instruction_index, *inner_instruction_index),
        LogPosition::Evm { .. } => (0, None),
    }
}

/// Serialize params to JSON string.
fn params_to_json(params: &HashMap<Arc<str>, DecodedValue>) -> String {
    serde_json::to_string(params).unwrap_or_else(|_| "{}".to_string())
}

/// Write decoded events to a parquet file.
///
/// Uses the fixed structural schema from [`build_decoded_event_schema`].
pub fn write_decoded_events_to_parquet(
    events: &[DecodedEvent],
    schema: &Arc<Schema>,
    output_path: &Path,
) -> Result<(), std::io::Error> {
    let mut arrays: Vec<ArrayRef> = Vec::new();

    // slot (block_number)
    let arr: UInt64Array = events.iter().map(|e| Some(e.block_number)).collect();
    arrays.push(Arc::new(arr));

    // block_timestamp
    let arr: UInt64Array = events.iter().map(|e| Some(e.block_timestamp)).collect();
    arrays.push(Arc::new(arr));

    // transaction_signature (FixedSizeBinary(64))
    if events.is_empty() {
        arrays.push(Arc::new(FixedSizeBinaryBuilder::new(64).finish()));
    } else {
        let arr = FixedSizeBinaryArray::try_from_iter(
            events
                .iter()
                .map(|e| tx_signature_bytes(&e.transaction_id))
                .map(|sig| sig.to_vec()),
        )
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        arrays.push(Arc::new(arr));
    }

    // program_id (FixedSizeBinary(32))
    if events.is_empty() {
        arrays.push(Arc::new(FixedSizeBinaryBuilder::new(32).finish()));
    } else {
        let arr = FixedSizeBinaryArray::try_from_iter(
            events
                .iter()
                .map(|e| program_id_bytes(&e.contract_address))
                .map(|pid| pid.to_vec()),
        )
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        arrays.push(Arc::new(arr));
    }

    // instruction_index
    let arr: UInt16Array = events
        .iter()
        .map(|e| {
            let (ix, _) = instruction_indices(&e.position);
            Some(ix)
        })
        .collect();
    arrays.push(Arc::new(arr));

    // inner_instruction_index (nullable)
    let arr: UInt16Array = events
        .iter()
        .map(|e| {
            let (_, inner) = instruction_indices(&e.position);
            inner
        })
        .collect();
    arrays.push(Arc::new(arr));

    // source_name
    let arr: StringArray = events
        .iter()
        .map(|e| Some(e.source_name.as_str()))
        .collect();
    arrays.push(Arc::new(arr));

    // event_name
    let arr: StringArray = events.iter().map(|e| Some(e.event_name.as_str())).collect();
    arrays.push(Arc::new(arr));

    // params (JSON)
    let arr: StringArray = events
        .iter()
        .map(|e| Some(params_to_json(&e.params)))
        .collect();
    arrays.push(Arc::new(arr));

    let batch = RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    crate::storage::atomic_write_parquet_fast(&batch, output_path)?;
    Ok(())
}

/// Write decoded account states to a parquet file.
pub fn write_decoded_account_states_to_parquet(
    account_states: &[DecodedAccountState],
    schema: &Arc<Schema>,
    output_path: &Path,
) -> Result<(), std::io::Error> {
    let mut arrays: Vec<ArrayRef> = Vec::new();

    let slot_arr: UInt64Array = account_states
        .iter()
        .map(|state| Some(state.block_number))
        .collect();
    arrays.push(Arc::new(slot_arr));

    let ts_arr: UInt64Array = account_states
        .iter()
        .map(|state| Some(state.block_timestamp))
        .collect();
    arrays.push(Arc::new(ts_arr));

    if account_states.is_empty() {
        arrays.push(Arc::new(FixedSizeBinaryBuilder::new(32).finish()));
    } else {
        let arr = FixedSizeBinaryArray::try_from_iter(
            account_states
                .iter()
                .map(|state| account_address_bytes(&state.account_address).to_vec()),
        )
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        arrays.push(Arc::new(arr));
    }

    if account_states.is_empty() {
        arrays.push(Arc::new(FixedSizeBinaryBuilder::new(32).finish()));
    } else {
        let arr = FixedSizeBinaryArray::try_from_iter(
            account_states
                .iter()
                .map(|state| account_address_bytes(&state.owner_program).to_vec()),
        )
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        arrays.push(Arc::new(arr));
    }

    let source_arr: StringArray = account_states
        .iter()
        .map(|state| Some(state.source_name.as_str()))
        .collect();
    arrays.push(Arc::new(source_arr));

    let account_type_arr: StringArray = account_states
        .iter()
        .map(|state| Some(state.account_type.as_str()))
        .collect();
    arrays.push(Arc::new(account_type_arr));

    let fields_arr: StringArray = account_states
        .iter()
        .map(|state| Some(params_to_json(&state.fields)))
        .collect();
    arrays.push(Arc::new(fields_arr));

    let batch = RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    crate::storage::atomic_write_parquet_fast(&batch, output_path)?;
    Ok(())
}

fn json_to_decoded_fields(json: &str) -> Result<HashMap<Arc<str>, DecodedValue>, std::io::Error> {
    let parsed: HashMap<String, DecodedValue> = serde_json::from_str(json)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    Ok(parsed
        .into_iter()
        .map(|(key, value)| (Arc::<str>::from(key), value))
        .collect())
}

/// Read decoded Solana events from parquet.
pub fn read_decoded_events_from_parquet(path: &Path) -> Result<Vec<DecodedEvent>, std::io::Error> {
    use arrow::array::Array;

    let file = std::fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    let reader = builder
        .build()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;

    let mut events = Vec::new();
    for batch_result in reader {
        let batch = batch_result
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;

        let slots = batch
            .column_by_name("slot")
            .and_then(|col| col.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "missing slot"))?;
        let timestamps = batch
            .column_by_name("block_timestamp")
            .and_then(|col| col.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "missing block_timestamp")
            })?;
        let signatures = batch
            .column_by_name("transaction_signature")
            .and_then(|col| col.as_any().downcast_ref::<FixedSizeBinaryArray>())
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "missing transaction_signature",
                )
            })?;
        let program_ids = batch
            .column_by_name("program_id")
            .and_then(|col| col.as_any().downcast_ref::<FixedSizeBinaryArray>())
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "missing program_id")
            })?;
        let instruction_indices = batch
            .column_by_name("instruction_index")
            .and_then(|col| col.as_any().downcast_ref::<UInt16Array>())
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "missing instruction_index")
            })?;
        let inner_instruction_indices = batch
            .column_by_name("inner_instruction_index")
            .and_then(|col| col.as_any().downcast_ref::<UInt16Array>())
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "missing inner_instruction_index",
                )
            })?;
        let source_names = batch
            .column_by_name("source_name")
            .and_then(|col| col.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "missing source_name")
            })?;
        let event_names = batch
            .column_by_name("event_name")
            .and_then(|col| col.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "missing event_name")
            })?;
        let params = batch
            .column_by_name("params")
            .and_then(|col| col.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "missing params")
            })?;

        for row in 0..batch.num_rows() {
            let signature: [u8; 64] = signatures.value(row).try_into().map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "transaction_signature is not 64 bytes",
                )
            })?;
            let program_id: [u8; 32] = program_ids.value(row).try_into().map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "program_id is not 32 bytes",
                )
            })?;

            events.push(DecodedEvent {
                block_number: slots.value(row),
                block_timestamp: timestamps.value(row),
                transaction_id: TxId::Solana(signature),
                position: LogPosition::Solana {
                    instruction_index: instruction_indices.value(row),
                    inner_instruction_index: if inner_instruction_indices.is_null(row) {
                        None
                    } else {
                        Some(inner_instruction_indices.value(row))
                    },
                },
                contract_address: ChainAddress::Solana(program_id),
                source_name: source_names.value(row).to_string(),
                event_name: event_names.value(row).to_string(),
                event_signature: event_names.value(row).to_string(),
                params: json_to_decoded_fields(params.value(row))?,
            });
        }
    }

    Ok(events)
}

/// Read decoded Solana account states from parquet.
pub fn read_decoded_account_states_from_parquet(
    path: &Path,
) -> Result<Vec<DecodedAccountState>, std::io::Error> {
    let file = std::fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    let reader = builder
        .build()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;

    let mut account_states = Vec::new();
    for batch_result in reader {
        let batch = batch_result
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        let slots = batch
            .column_by_name("slot")
            .and_then(|col| col.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "missing slot"))?;
        let timestamps = batch
            .column_by_name("block_timestamp")
            .and_then(|col| col.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "missing block_timestamp")
            })?;
        let account_addresses = batch
            .column_by_name("account_address")
            .and_then(|col| col.as_any().downcast_ref::<FixedSizeBinaryArray>())
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "missing account_address")
            })?;
        let owner_programs = batch
            .column_by_name("owner_program")
            .and_then(|col| col.as_any().downcast_ref::<FixedSizeBinaryArray>())
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "missing owner_program")
            })?;
        let source_names = batch
            .column_by_name("source_name")
            .and_then(|col| col.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "missing source_name")
            })?;
        let account_types = batch
            .column_by_name("account_type")
            .and_then(|col| col.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "missing account_type")
            })?;
        let fields = batch
            .column_by_name("fields")
            .and_then(|col| col.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "missing fields")
            })?;

        for row in 0..batch.num_rows() {
            let account_address: [u8; 32] =
                account_addresses.value(row).try_into().map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "account_address is not 32 bytes",
                    )
                })?;
            let owner_program: [u8; 32] = owner_programs.value(row).try_into().map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "owner_program is not 32 bytes",
                )
            })?;

            account_states.push(DecodedAccountState {
                block_number: slots.value(row),
                block_timestamp: timestamps.value(row),
                account_address: ChainAddress::Solana(account_address),
                owner_program: ChainAddress::Solana(owner_program),
                source_name: source_names.value(row).to_string(),
                account_type: account_types.value(row).to_string(),
                fields: json_to_decoded_fields(fields.value(row))?,
            });
        }
    }

    Ok(account_states)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::chain::{ChainAddress, LogPosition, TxId};
    use crate::types::decoded::DecodedValue;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn make_decoded_event(
        slot: u64,
        source: &str,
        event: &str,
        program_id: [u8; 32],
        tx_sig: [u8; 64],
        ix: u16,
        inner: Option<u16>,
    ) -> DecodedEvent {
        let mut params = HashMap::new();
        params.insert(Arc::<str>::from("amount"), DecodedValue::Uint64(42));
        params.insert(
            Arc::<str>::from("name"),
            DecodedValue::String("test".to_string()),
        );

        DecodedEvent {
            block_number: slot,
            block_timestamp: 1_700_000_000,
            transaction_id: TxId::Solana(tx_sig),
            position: LogPosition::Solana {
                instruction_index: ix,
                inner_instruction_index: inner,
            },
            contract_address: ChainAddress::Solana(program_id),
            source_name: source.to_string(),
            event_name: event.to_string(),
            event_signature: format!("{}:{}", source, event),
            params,
        }
    }

    fn read_parquet_row_count(path: &Path) -> usize {
        use parquet::file::reader::{FileReader, SerializedFileReader};
        let file = std::fs::File::open(path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        reader.metadata().file_metadata().num_rows() as usize
    }

    #[test]
    fn test_decoded_event_schema_field_count() {
        let schema = build_decoded_event_schema();
        assert_eq!(schema.fields().len(), 9);
    }

    #[test]
    fn test_decoded_event_schema_field_names() {
        let schema = build_decoded_event_schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec![
                "slot",
                "block_timestamp",
                "transaction_signature",
                "program_id",
                "instruction_index",
                "inner_instruction_index",
                "source_name",
                "event_name",
                "params",
            ]
        );
    }

    #[test]
    fn test_write_decoded_events_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("decoded.parquet");
        let schema = build_decoded_event_schema();

        let events = vec![
            make_decoded_event(
                100,
                "spl_token",
                "Transfer",
                [0xAA; 32],
                [0xBB; 64],
                0,
                None,
            ),
            make_decoded_event(
                100,
                "spl_token",
                "Transfer",
                [0xAA; 32],
                [0xCC; 64],
                1,
                Some(2),
            ),
        ];

        write_decoded_events_to_parquet(&events, &schema, &path).unwrap();

        assert!(path.exists());
        assert_eq!(read_parquet_row_count(&path), 2);
    }

    #[test]
    fn test_write_decoded_events_empty() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("decoded_empty.parquet");
        let schema = build_decoded_event_schema();

        write_decoded_events_to_parquet(&[], &schema, &path).unwrap();

        assert!(path.exists());
        assert_eq!(read_parquet_row_count(&path), 0);
    }

    #[test]
    fn test_params_json_serialization() {
        let mut params = HashMap::new();
        params.insert(Arc::<str>::from("amount"), DecodedValue::Uint64(42));
        params.insert(Arc::<str>::from("flag"), DecodedValue::Bool(true));

        let json = params_to_json(&params);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        // Verify it parses back to valid JSON with expected keys
        assert!(parsed.is_object());
        let obj = parsed.as_object().unwrap();
        assert!(obj.contains_key("amount"));
        assert!(obj.contains_key("flag"));
    }

    #[test]
    fn test_write_decoded_events_roundtrip_reads_back() {
        use arrow::array::Array;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("decoded_read.parquet");
        let schema = build_decoded_event_schema();

        let events = vec![make_decoded_event(
            200,
            "whirlpool",
            "Swap",
            [0x11; 32],
            [0x22; 64],
            3,
            Some(1),
        )];

        write_decoded_events_to_parquet(&events, &schema, &path).unwrap();

        // Read back and verify
        let file = std::fs::File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let mut reader = builder.build().unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(batch.num_rows(), 1);

        // Verify slot
        let slot_arr = batch
            .column_by_name("slot")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(slot_arr.value(0), 200);

        // Verify source_name
        let source_arr = batch
            .column_by_name("source_name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(source_arr.value(0), "whirlpool");

        // Verify event_name
        let event_arr = batch
            .column_by_name("event_name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(event_arr.value(0), "Swap");

        // Verify instruction_index
        let ix_arr = batch
            .column_by_name("instruction_index")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap();
        assert_eq!(ix_arr.value(0), 3);

        // Verify inner_instruction_index (nullable)
        let inner_arr = batch
            .column_by_name("inner_instruction_index")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap();
        assert!(!inner_arr.is_null(0));
        assert_eq!(inner_arr.value(0), 1);

        // Verify params is valid JSON
        let params_arr = batch
            .column_by_name("params")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(params_arr.value(0)).unwrap();
        assert!(parsed.is_object());
    }
}
