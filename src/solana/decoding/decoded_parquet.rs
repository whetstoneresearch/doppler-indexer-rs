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

use crate::transformations::context::DecodedEvent;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::chain::{ChainAddress, LogPosition, TxId};
    use crate::types::decoded::DecodedValue;
    use std::collections::HashMap;
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
        params.insert("amount".to_string(), DecodedValue::Uint64(42));
        params.insert("name".to_string(), DecodedValue::String("test".to_string()));

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
        params.insert("amount".to_string(), DecodedValue::Uint64(42));
        params.insert("flag".to_string(), DecodedValue::Bool(true));

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
