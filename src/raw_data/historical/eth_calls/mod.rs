pub(crate) mod config;
pub(crate) mod event_triggers;
pub(crate) mod factory;
pub(crate) mod frequency;
pub(crate) mod helpers;
pub(crate) mod multicall;
pub(crate) mod once_calls;
pub(crate) mod parquet_io;
pub(crate) mod postprocessing;
pub(crate) mod regular_calls;
pub(crate) mod types;

pub use config::*;
pub use event_triggers::*;
pub use types::*;

// Re-export from multicall (types + functions used by event_triggers and other internal callers)
pub(crate) use multicall::{
    execute_multicalls_generic, BlockMulticall, EventCallMeta, MulticallSlotGeneric,
};

// Re-export from regular_calls
pub(crate) use regular_calls::{
    process_factory_range, process_factory_range_multicall, process_range, process_range_multicall,
};

// Re-export from once_calls
pub(crate) use once_calls::{
    process_factory_once_calls, process_factory_once_calls_multicall, process_once_calls_multicall,
    process_once_calls_regular,
};

pub(crate) use factory::*;
pub(crate) use parquet_io::*;
