use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;

static BENCH_FILE: Mutex<Option<File>> = Mutex::new(None);

pub fn init(path: &Path) -> std::io::Result<()> {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)?;

    let mut guard = BENCH_FILE.lock().unwrap();
    *guard = Some(file);

    // Write CSV header
    if let Some(f) = guard.as_mut() {
        writeln!(f, "collector,range_start,range_end,record_count,rpc_ms,process_ms,write_ms")?;
    }

    Ok(())
}

pub fn record(
    collector: &str,
    range_start: u64,
    range_end: u64,
    record_count: usize,
    rpc_time: Duration,
    process_time: Duration,
    write_time: Duration,
) {
    let mut guard = BENCH_FILE.lock().unwrap();
    if let Some(f) = guard.as_mut() {
        let _ = writeln!(
            f,
            "{},{},{},{},{},{},{}",
            collector,
            range_start,
            range_end,
            record_count,
            rpc_time.as_millis(),
            process_time.as_millis(),
            write_time.as_millis()
        );
        let _ = f.flush();
    }
}
