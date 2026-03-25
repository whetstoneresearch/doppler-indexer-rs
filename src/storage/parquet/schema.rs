//! Parquet schema inspection utilities.

use std::collections::HashSet;
use std::fs::File;
use std::path::Path;

use parquet::file::reader::{FileReader, SerializedFileReader};

/// Read function names from a raw parquet file's schema.
///
/// Raw "once" columns are named `{function_name}_result`. This function strips
/// the suffix and returns the base function names. It is the unified replacement
/// for `read_raw_parquet_function_names` (decoding) and `read_parquet_column_names`
/// (collection), which were identical.
pub fn read_raw_parquet_function_names(path: &Path) -> HashSet<String> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(_) => return HashSet::new(),
    };

    let reader = match SerializedFileReader::new(file) {
        Ok(r) => r,
        Err(_) => return HashSet::new(),
    };

    let schema = reader.metadata().file_metadata().schema_descr();
    let mut fn_names = HashSet::new();

    for field in schema.columns() {
        let name = field.name();
        if let Some(fn_name) = name.strip_suffix("_result") {
            fn_names.insert(fn_name.to_string());
        }
    }

    fn_names
}
