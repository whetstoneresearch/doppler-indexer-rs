use metrics::{describe_counter, describe_gauge, describe_histogram};

/// Register metric descriptions for live mode (call once at startup).
pub fn describe_live_metrics() {
    describe_counter!(
        "live_ws_disconnects_total",
        "Total WebSocket disconnect events"
    );
    describe_counter!(
        "live_ws_reconnects_total",
        "Total WebSocket reconnect events"
    );
    describe_counter!(
        "live_reorgs_total",
        "Total chain reorganizations detected"
    );
    describe_histogram!(
        "live_reorg_depth",
        "Depth of detected chain reorganizations"
    );
    describe_histogram!(
        "live_compaction_cycle_duration_seconds",
        "Duration of a compaction cycle"
    );
    describe_gauge!(
        "live_compaction_stuck_blocks",
        "Blocks stuck awaiting transformation retry"
    );
    describe_gauge!(
        "live_compaction_blocks_pending",
        "Live blocks awaiting compaction"
    );
    describe_histogram!(
        "live_block_arrival_interval_seconds",
        "Time between consecutive block arrivals"
    );
    describe_histogram!(
        "live_block_processing_duration_seconds",
        "End-to-end duration to process a live block"
    );
    describe_counter!(
        "live_blocks_processed_total",
        "Total live blocks processed"
    );
    describe_counter!(
        "live_gap_detections_total",
        "Gap detection events"
    );
    describe_histogram!(
        "live_backfill_duration_seconds",
        "Duration of backfill operations"
    );
    describe_counter!(
        "live_backfill_blocks_total",
        "Total blocks backfilled"
    );
}
