//! Block frequency filtering for eth_call collection.

use super::types::BlockInfo;
use crate::types::config::eth_call::Frequency;

pub(crate) fn filter_blocks_for_frequency<'a>(
    blocks: &'a [BlockInfo],
    frequency: &Frequency,
    last_call_timestamp: Option<u64>,
) -> Vec<&'a BlockInfo> {
    match frequency {
        Frequency::EveryBlock => blocks.iter().collect(),
        Frequency::Once => vec![],        // handled separately
        Frequency::OnEvents(_) => vec![], // handled separately via event triggers
        Frequency::EveryNBlocks(n) => blocks.iter().filter(|b| b.block_number % n == 0).collect(),
        Frequency::Duration(secs) => {
            let mut result = vec![];
            let mut last_ts = last_call_timestamp.unwrap_or(0);
            for block in blocks {
                if block.timestamp >= last_ts + secs {
                    result.push(block);
                    last_ts = block.timestamp;
                }
            }
            result
        }
    }
}
