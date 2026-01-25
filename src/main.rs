mod raw_data;
mod rpc;
mod types;

use std::path::Path;

use types::config::indexer::IndexerConfig;

fn main() {
    let config = IndexerConfig::load(Path::new("config/config.json"));

    match config {
        Ok(config) => println!("{:?}", config),
        Err(e) => eprintln!("Error loading config: {}", e),
    }
}
