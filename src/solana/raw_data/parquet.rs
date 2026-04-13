//! Arrow schemas and parquet writing for Solana raw data records.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, FixedSizeBinaryArray, FixedSizeBinaryBuilder, ListBuilder, UInt16Array,
    UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use super::types::{
    SolanaCollectionError, SolanaEventRecord, SolanaInstructionRecord, SolanaSlotRecord,
};

// ---------------------------------------------------------------------------
// Slot schema and writing
// ---------------------------------------------------------------------------

/// Build the Arrow schema for `SolanaSlotRecord` parquet files.
pub fn build_slot_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("block_time", DataType::Int64, true),
        Field::new("block_height", DataType::UInt64, true),
        Field::new("parent_slot", DataType::UInt64, false),
        Field::new("blockhash", DataType::FixedSizeBinary(32), false),
        Field::new("previous_blockhash", DataType::FixedSizeBinary(32), false),
        Field::new("transaction_count", DataType::UInt32, false),
        Field::new(
            "transaction_signatures",
            DataType::List(Arc::new(Field::new("item", DataType::FixedSizeBinary(64), true))),
            false,
        ),
    ]))
}

/// Write `SolanaSlotRecord`s to a parquet file.
pub fn write_slots_to_parquet(
    records: &[SolanaSlotRecord],
    schema: &Arc<Schema>,
    output_path: &Path,
) -> Result<(), SolanaCollectionError> {
    let mut arrays: Vec<ArrayRef> = Vec::new();

    // slot
    let arr: UInt64Array = records.iter().map(|r| Some(r.slot)).collect();
    arrays.push(Arc::new(arr));

    // block_time (nullable Int64)
    let arr: arrow::array::Int64Array = records.iter().map(|r| r.block_time).collect();
    arrays.push(Arc::new(arr));

    // block_height (nullable UInt64)
    let arr: UInt64Array = records.iter().map(|r| r.block_height).collect();
    arrays.push(Arc::new(arr));

    // parent_slot
    let arr: UInt64Array = records.iter().map(|r| Some(r.parent_slot)).collect();
    arrays.push(Arc::new(arr));

    // blockhash (FixedSizeBinary(32))
    let arr = if records.is_empty() {
        FixedSizeBinaryBuilder::new(32).finish()
    } else {
        FixedSizeBinaryArray::try_from_iter(records.iter().map(|r| r.blockhash.as_slice()))?
    };
    arrays.push(Arc::new(arr));

    // previous_blockhash (FixedSizeBinary(32))
    let arr = if records.is_empty() {
        FixedSizeBinaryBuilder::new(32).finish()
    } else {
        FixedSizeBinaryArray::try_from_iter(
            records.iter().map(|r| r.previous_blockhash.as_slice()),
        )?
    };
    arrays.push(Arc::new(arr));

    // transaction_count
    let arr: UInt32Array = records.iter().map(|r| Some(r.transaction_count)).collect();
    arrays.push(Arc::new(arr));

    // transaction_signatures (List(FixedSizeBinary(64)))
    let mut list_builder = ListBuilder::new(FixedSizeBinaryBuilder::new(64));
    for record in records {
        for sig in &record.transaction_signatures {
            list_builder.values().append_value(sig.as_slice())?;
        }
        list_builder.append(true);
    }
    arrays.push(Arc::new(list_builder.finish()));

    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    crate::storage::atomic_write_parquet_fast(&batch, output_path)?;
    Ok(())
}

/// Async wrapper around [`write_slots_to_parquet`].
pub async fn write_slots_to_parquet_async(
    records: Vec<SolanaSlotRecord>,
    schema: Arc<Schema>,
    output_path: PathBuf,
) -> Result<(), SolanaCollectionError> {
    tokio::task::spawn_blocking(move || write_slots_to_parquet(&records, &schema, &output_path))
        .await
        .map_err(|e| SolanaCollectionError::JoinError(e.to_string()))?
}

// ---------------------------------------------------------------------------
// Event schema and writing
// ---------------------------------------------------------------------------

/// Build the Arrow schema for `SolanaEventRecord` parquet files.
pub fn build_event_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("block_time", DataType::Int64, true),
        Field::new(
            "transaction_signature",
            DataType::FixedSizeBinary(64),
            false,
        ),
        Field::new("program_id", DataType::FixedSizeBinary(32), false),
        Field::new("event_discriminator", DataType::FixedSizeBinary(8), false),
        Field::new("event_data", DataType::Binary, false),
        Field::new("log_index", DataType::UInt32, false),
        Field::new("instruction_index", DataType::UInt16, false),
        Field::new("inner_instruction_index", DataType::UInt16, true),
    ]))
}

/// Write `SolanaEventRecord`s to a parquet file.
pub fn write_events_to_parquet(
    records: &[SolanaEventRecord],
    schema: &Arc<Schema>,
    output_path: &Path,
) -> Result<(), SolanaCollectionError> {
    let mut arrays: Vec<ArrayRef> = Vec::new();

    // slot
    let arr: UInt64Array = records.iter().map(|r| Some(r.slot)).collect();
    arrays.push(Arc::new(arr));

    // block_time (nullable Int64)
    let arr: arrow::array::Int64Array = records.iter().map(|r| r.block_time).collect();
    arrays.push(Arc::new(arr));

    // transaction_signature (FixedSizeBinary(64))
    let arr = if records.is_empty() {
        FixedSizeBinaryBuilder::new(64).finish()
    } else {
        FixedSizeBinaryArray::try_from_iter(
            records.iter().map(|r| r.transaction_signature.as_slice()),
        )?
    };
    arrays.push(Arc::new(arr));

    // program_id (FixedSizeBinary(32))
    let arr = if records.is_empty() {
        FixedSizeBinaryBuilder::new(32).finish()
    } else {
        FixedSizeBinaryArray::try_from_iter(records.iter().map(|r| r.program_id.as_slice()))?
    };
    arrays.push(Arc::new(arr));

    // event_discriminator (FixedSizeBinary(8))
    let arr = if records.is_empty() {
        FixedSizeBinaryBuilder::new(8).finish()
    } else {
        FixedSizeBinaryArray::try_from_iter(
            records.iter().map(|r| r.event_discriminator.as_slice()),
        )?
    };
    arrays.push(Arc::new(arr));

    // event_data (Binary)
    let arr: BinaryArray = records
        .iter()
        .map(|r| Some(r.event_data.as_slice()))
        .collect();
    arrays.push(Arc::new(arr));

    // log_index
    let arr: UInt32Array = records.iter().map(|r| Some(r.log_index)).collect();
    arrays.push(Arc::new(arr));

    // instruction_index
    let arr: UInt16Array = records.iter().map(|r| Some(r.instruction_index)).collect();
    arrays.push(Arc::new(arr));

    // inner_instruction_index (nullable UInt16)
    let arr: UInt16Array = records.iter().map(|r| r.inner_instruction_index).collect();
    arrays.push(Arc::new(arr));

    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    crate::storage::atomic_write_parquet_fast(&batch, output_path)?;
    Ok(())
}

/// Async wrapper around [`write_events_to_parquet`].
pub async fn write_events_to_parquet_async(
    records: Vec<SolanaEventRecord>,
    schema: Arc<Schema>,
    output_path: PathBuf,
) -> Result<(), SolanaCollectionError> {
    tokio::task::spawn_blocking(move || write_events_to_parquet(&records, &schema, &output_path))
        .await
        .map_err(|e| SolanaCollectionError::JoinError(e.to_string()))?
}

// ---------------------------------------------------------------------------
// Instruction schema and writing
// ---------------------------------------------------------------------------

/// Build the Arrow schema for `SolanaInstructionRecord` parquet files.
pub fn build_instruction_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("block_time", DataType::Int64, true),
        Field::new(
            "transaction_signature",
            DataType::FixedSizeBinary(64),
            false,
        ),
        Field::new("program_id", DataType::FixedSizeBinary(32), false),
        Field::new("data", DataType::Binary, false),
        Field::new(
            "accounts",
            DataType::List(Arc::new(Field::new("item", DataType::FixedSizeBinary(32), true))),
            false,
        ),
        Field::new("instruction_index", DataType::UInt16, false),
        Field::new("inner_instruction_index", DataType::UInt16, true),
    ]))
}

/// Write `SolanaInstructionRecord`s to a parquet file.
pub fn write_instructions_to_parquet(
    records: &[SolanaInstructionRecord],
    schema: &Arc<Schema>,
    output_path: &Path,
) -> Result<(), SolanaCollectionError> {
    let mut arrays: Vec<ArrayRef> = Vec::new();

    // slot
    let arr: UInt64Array = records.iter().map(|r| Some(r.slot)).collect();
    arrays.push(Arc::new(arr));

    // block_time (nullable Int64)
    let arr: arrow::array::Int64Array = records.iter().map(|r| r.block_time).collect();
    arrays.push(Arc::new(arr));

    // transaction_signature (FixedSizeBinary(64))
    let arr = if records.is_empty() {
        FixedSizeBinaryBuilder::new(64).finish()
    } else {
        FixedSizeBinaryArray::try_from_iter(
            records.iter().map(|r| r.transaction_signature.as_slice()),
        )?
    };
    arrays.push(Arc::new(arr));

    // program_id (FixedSizeBinary(32))
    let arr = if records.is_empty() {
        FixedSizeBinaryBuilder::new(32).finish()
    } else {
        FixedSizeBinaryArray::try_from_iter(records.iter().map(|r| r.program_id.as_slice()))?
    };
    arrays.push(Arc::new(arr));

    // data (Binary)
    let arr: BinaryArray = records
        .iter()
        .map(|r| Some(r.data.as_slice()))
        .collect();
    arrays.push(Arc::new(arr));

    // accounts (List(FixedSizeBinary(32)))
    let mut list_builder = ListBuilder::new(FixedSizeBinaryBuilder::new(32));
    for record in records {
        for account in &record.accounts {
            list_builder.values().append_value(account.as_slice())?;
        }
        list_builder.append(true);
    }
    arrays.push(Arc::new(list_builder.finish()));

    // instruction_index
    let arr: UInt16Array = records.iter().map(|r| Some(r.instruction_index)).collect();
    arrays.push(Arc::new(arr));

    // inner_instruction_index (nullable UInt16)
    let arr: UInt16Array = records.iter().map(|r| r.inner_instruction_index).collect();
    arrays.push(Arc::new(arr));

    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    crate::storage::atomic_write_parquet_fast(&batch, output_path)?;
    Ok(())
}

/// Async wrapper around [`write_instructions_to_parquet`].
pub async fn write_instructions_to_parquet_async(
    records: Vec<SolanaInstructionRecord>,
    schema: Arc<Schema>,
    output_path: PathBuf,
) -> Result<(), SolanaCollectionError> {
    tokio::task::spawn_blocking(move || {
        write_instructions_to_parquet(&records, &schema, &output_path)
    })
    .await
    .map_err(|e| SolanaCollectionError::JoinError(e.to_string()))?
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn sample_slot_records() -> Vec<SolanaSlotRecord> {
        vec![
            SolanaSlotRecord {
                slot: 250_000_000,
                block_time: Some(1_700_000_000),
                block_height: Some(200_000_000),
                parent_slot: 249_999_999,
                blockhash: [1u8; 32],
                previous_blockhash: [2u8; 32],
                transaction_count: 2,
                transaction_signatures: vec![[3u8; 64], [4u8; 64]],
            },
            SolanaSlotRecord {
                slot: 250_000_001,
                block_time: None,
                block_height: None,
                parent_slot: 250_000_000,
                blockhash: [5u8; 32],
                previous_blockhash: [1u8; 32],
                transaction_count: 0,
                transaction_signatures: vec![],
            },
        ]
    }

    fn sample_event_records() -> Vec<SolanaEventRecord> {
        vec![
            SolanaEventRecord {
                slot: 100,
                block_time: Some(1_700_000_000),
                transaction_signature: [0xAA; 64],
                program_id: [0xBB; 32],
                event_discriminator: [0xCC; 8],
                event_data: vec![1, 2, 3],
                log_index: 0,
                instruction_index: 0,
                inner_instruction_index: None,
            },
            SolanaEventRecord {
                slot: 100,
                block_time: Some(1_700_000_000),
                transaction_signature: [0xAA; 64],
                program_id: [0xBB; 32],
                event_discriminator: [0xDD; 8],
                event_data: vec![4, 5],
                log_index: 1,
                instruction_index: 1,
                inner_instruction_index: Some(3),
            },
        ]
    }

    fn sample_instruction_records() -> Vec<SolanaInstructionRecord> {
        vec![
            SolanaInstructionRecord {
                slot: 200,
                block_time: Some(1_700_000_000),
                transaction_signature: [0x11; 64],
                program_id: [0x22; 32],
                data: vec![10, 20, 30],
                accounts: vec![[0x33; 32], [0x44; 32]],
                instruction_index: 0,
                inner_instruction_index: None,
            },
            SolanaInstructionRecord {
                slot: 200,
                block_time: None,
                transaction_signature: [0x11; 64],
                program_id: [0x55; 32],
                data: vec![],
                accounts: vec![],
                instruction_index: 0,
                inner_instruction_index: Some(0),
            },
        ]
    }

    fn read_parquet_row_count(path: &Path) -> usize {
        use parquet::file::reader::{FileReader, SerializedFileReader};
        let file = std::fs::File::open(path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let metadata = reader.metadata();
        metadata.file_metadata().num_rows() as usize
    }

    // -----------------------------------------------------------------------
    // Slot schema + write tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_slot_schema_field_count() {
        let schema = build_slot_schema();
        assert_eq!(schema.fields().len(), 8);
    }

    #[test]
    fn test_slot_schema_field_names() {
        let schema = build_slot_schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec![
                "slot",
                "block_time",
                "block_height",
                "parent_slot",
                "blockhash",
                "previous_blockhash",
                "transaction_count",
                "transaction_signatures",
            ]
        );
    }

    #[test]
    fn test_write_slots_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("slots.parquet");
        let schema = build_slot_schema();
        let records = sample_slot_records();

        write_slots_to_parquet(&records, &schema, &path).unwrap();

        assert!(path.exists());
        assert_eq!(read_parquet_row_count(&path), 2);
    }

    #[test]
    fn test_write_slots_empty() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("slots_empty.parquet");
        let schema = build_slot_schema();

        write_slots_to_parquet(&[], &schema, &path).unwrap();

        assert!(path.exists());
        assert_eq!(read_parquet_row_count(&path), 0);
    }

    // -----------------------------------------------------------------------
    // Event schema + write tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_event_schema_field_count() {
        let schema = build_event_schema();
        assert_eq!(schema.fields().len(), 9);
    }

    #[test]
    fn test_event_schema_field_names() {
        let schema = build_event_schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec![
                "slot",
                "block_time",
                "transaction_signature",
                "program_id",
                "event_discriminator",
                "event_data",
                "log_index",
                "instruction_index",
                "inner_instruction_index",
            ]
        );
    }

    #[test]
    fn test_write_events_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("events.parquet");
        let schema = build_event_schema();
        let records = sample_event_records();

        write_events_to_parquet(&records, &schema, &path).unwrap();

        assert!(path.exists());
        assert_eq!(read_parquet_row_count(&path), 2);
    }

    #[test]
    fn test_write_events_empty() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("events_empty.parquet");
        let schema = build_event_schema();

        write_events_to_parquet(&[], &schema, &path).unwrap();

        assert!(path.exists());
        assert_eq!(read_parquet_row_count(&path), 0);
    }

    // -----------------------------------------------------------------------
    // Instruction schema + write tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_instruction_schema_field_count() {
        let schema = build_instruction_schema();
        assert_eq!(schema.fields().len(), 8);
    }

    #[test]
    fn test_instruction_schema_field_names() {
        let schema = build_instruction_schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec![
                "slot",
                "block_time",
                "transaction_signature",
                "program_id",
                "data",
                "accounts",
                "instruction_index",
                "inner_instruction_index",
            ]
        );
    }

    #[test]
    fn test_write_instructions_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("instructions.parquet");
        let schema = build_instruction_schema();
        let records = sample_instruction_records();

        write_instructions_to_parquet(&records, &schema, &path).unwrap();

        assert!(path.exists());
        assert_eq!(read_parquet_row_count(&path), 2);
    }

    #[test]
    fn test_write_instructions_empty() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("instructions_empty.parquet");
        let schema = build_instruction_schema();

        write_instructions_to_parquet(&[], &schema, &path).unwrap();

        assert!(path.exists());
        assert_eq!(read_parquet_row_count(&path), 0);
    }

    // -----------------------------------------------------------------------
    // Async wrapper tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_write_slots_async() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("slots_async.parquet");
        let schema = build_slot_schema();
        let records = sample_slot_records();

        write_slots_to_parquet_async(records, schema, path.clone())
            .await
            .unwrap();

        assert!(path.exists());
        assert_eq!(read_parquet_row_count(&path), 2);
    }

    #[tokio::test]
    async fn test_write_events_async() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("events_async.parquet");
        let schema = build_event_schema();
        let records = sample_event_records();

        write_events_to_parquet_async(records, schema, path.clone())
            .await
            .unwrap();

        assert!(path.exists());
        assert_eq!(read_parquet_row_count(&path), 2);
    }

    #[tokio::test]
    async fn test_write_instructions_async() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("instructions_async.parquet");
        let schema = build_instruction_schema();
        let records = sample_instruction_records();

        write_instructions_to_parquet_async(records, schema, path.clone())
            .await
            .unwrap();

        assert!(path.exists());
        assert_eq!(read_parquet_row_count(&path), 2);
    }
}
