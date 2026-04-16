//! ProgramLogParser and event extraction from Solana transaction log messages.
//!
//! Solana programs emit events via `msg!()` macro calls that appear in the
//! transaction's `logMessages` array. The runtime also injects structured lines
//! for program invocations:
//!
//! - `"Program <pubkey> invoke [N]"` — program invocation at CPI depth N
//! - `"Program <pubkey> success"` — program completed successfully
//! - `"Program <pubkey> failed: <reason>"` — program failed
//! - `"Program data: <base64>"` — event data emitted by the current program
//!
//! This module parses these log lines to extract typed event records.

use std::collections::HashSet;

use base64::Engine;
use solana_sdk::pubkey::Pubkey;

use super::types::SolanaEventRecord;

// ---------------------------------------------------------------------------
// ProgramLogParser
// ---------------------------------------------------------------------------

/// Stateful parser that walks log lines for a single transaction, tracking
/// program invocation depth and instruction indices to produce
/// [`SolanaEventRecord`]s.
pub struct ProgramLogParser {
    /// Stack of currently invoked programs (outermost at index 0).
    program_stack: Vec<Pubkey>,
    /// Accumulated events.
    events: Vec<SolanaEventRecord>,
    /// Index of the current top-level instruction (incremented on each
    /// depth-1 invoke). Starts at `u16::MAX` so the first increment wraps
    /// to 0.
    top_level_instruction_index: u16,
    /// Running count of inner (CPI) instructions within the current
    /// top-level instruction.
    inner_instruction_count: u16,
    /// Running log index for events within this transaction.
    log_index: u32,
    /// Whether we have seen the first invoke yet.
    has_started: bool,
}

impl ProgramLogParser {
    pub fn new() -> Self {
        Self {
            program_stack: Vec::new(),
            events: Vec::new(),
            top_level_instruction_index: 0,
            inner_instruction_count: 0,
            log_index: 0,
            has_started: false,
        }
    }

    /// Parse a single log line, maintaining the program invocation stack
    /// and instruction position tracking.
    ///
    /// # Trust assumption
    ///
    /// Programs can emit arbitrary messages via `msg!()`. A malicious program
    /// could emit `"Program data: <fake_base64>"` to inject spoofed events.
    /// There is no runtime-level defense against this — callers must apply
    /// their own program-id allowlist when consuming events.
    pub fn parse_log_line(
        &mut self,
        line: &str,
        slot: u64,
        block_time: Option<i64>,
        tx_sig: &[u8; 64],
    ) {
        // Fast path: all interesting lines start with "Program ".
        let Some(rest) = line.strip_prefix("Program ") else {
            return;
        };

        // --- "Program data: <base64>" ---
        if let Some(b64) = rest.strip_prefix("data: ") {
            self.handle_data(b64, slot, block_time, tx_sig);
            return;
        }

        // --- "Program <pubkey> invoke [N]" ---
        if let Some((pubkey_str, depth_str)) = rest.rsplit_once(" invoke [") {
            let depth_str = depth_str.trim_end_matches(']');
            if let Ok(depth) = depth_str.parse::<u32>() {
                self.handle_invoke(pubkey_str, depth);
            }
            return;
        }

        // --- "Program <pubkey> success" ---
        if let Some(_pubkey_str) = rest.strip_suffix(" success") {
            self.handle_pop();
            return;
        }

        // --- "Program <pubkey> failed: <reason>" ---
        if rest.contains(" failed: ") {
            self.handle_pop();
            return;
        }

        // All other log lines are ignored (arbitrary msg!() output).
    }

    /// Consume the parser and return all extracted events.
    pub fn into_events(self) -> Vec<SolanaEventRecord> {
        self.events
    }

    /// Whether log truncation was detected.
    ///
    /// If the program stack is non-empty after all log lines have been parsed,
    /// the logs were likely truncated (Solana has a 10 000-line log limit per
    /// transaction).
    pub fn is_truncated(&self) -> bool {
        !self.program_stack.is_empty()
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    fn handle_invoke(&mut self, pubkey_str: &str, depth: u32) {
        let Ok(pubkey) = pubkey_str.parse::<Pubkey>() else {
            return;
        };

        if depth == 1 {
            // New top-level instruction.
            if self.has_started {
                self.top_level_instruction_index = self.top_level_instruction_index.wrapping_add(1);
            }
            self.has_started = true;
            self.inner_instruction_count = 0;
        } else {
            // CPI call — increment inner counter.
            self.inner_instruction_count = self.inner_instruction_count.wrapping_add(1);
        }

        self.program_stack.push(pubkey);
    }

    fn handle_pop(&mut self) {
        self.program_stack.pop();
    }

    fn handle_data(&mut self, b64: &str, slot: u64, block_time: Option<i64>, tx_sig: &[u8; 64]) {
        let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(b64.trim()) else {
            return;
        };

        // Need at least 8 bytes for the discriminator.
        if decoded.len() < 8 {
            return;
        }

        // The event belongs to the program at the top of the stack.
        let Some(program) = self.program_stack.last() else {
            return;
        };

        let mut discriminator = [0u8; 8];
        discriminator.copy_from_slice(&decoded[..8]);
        let event_data = decoded[8..].to_vec();

        let inner_instruction_index = if self.program_stack.len() > 1 {
            // We're inside a CPI — record the inner instruction index.
            // inner_instruction_count was already incremented when the
            // invoke [2+] was parsed, so use count - 1.
            Some(self.inner_instruction_count.saturating_sub(1))
        } else {
            None
        };

        self.events.push(SolanaEventRecord {
            slot,
            block_time,
            transaction_signature: *tx_sig,
            program_id: program.to_bytes(),
            event_discriminator: discriminator,
            event_data,
            log_index: self.log_index,
            instruction_index: self.top_level_instruction_index,
            inner_instruction_index,
        });

        self.log_index += 1;
    }
}

impl Default for ProgramLogParser {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Convenience function
// ---------------------------------------------------------------------------

/// Extract events from a single transaction's log messages.
///
/// Returns an empty vec if the transaction has no logs. Filters events
/// to only those emitted by one of the `configured_programs`.
pub fn extract_events_from_logs(
    log_messages: &[String],
    slot: u64,
    block_time: Option<i64>,
    tx_signature: &[u8; 64],
    configured_programs: &HashSet<[u8; 32]>,
) -> Vec<SolanaEventRecord> {
    if log_messages.is_empty() {
        return Vec::new();
    }

    let mut parser = ProgramLogParser::new();
    for line in log_messages {
        parser.parse_log_line(line, slot, block_time, tx_signature);
    }

    if parser.is_truncated() {
        tracing::warn!(
            slot,
            sig = hex::encode(tx_signature),
            "Log messages appear truncated (program stack non-empty after parsing)"
        );
    }

    parser
        .into_events()
        .into_iter()
        .filter(|e| configured_programs.contains(&e.program_id))
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create a dummy 64-byte transaction signature.
    fn test_sig() -> [u8; 64] {
        [0xAA; 64]
    }

    /// Helper: base64-encode bytes.
    fn b64(bytes: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }

    // -- ProgramLogParser unit tests --

    #[test]
    fn test_simple_event_one_top_level_instruction() {
        let mut parser = ProgramLogParser::new();
        let sig = test_sig();
        let slot = 100;
        let bt = Some(1_700_000_000i64);

        // 8-byte discriminator + 4 bytes data
        let event_bytes: Vec<u8> = vec![1, 2, 3, 4, 5, 6, 7, 8, 0xDE, 0xAD, 0xBE, 0xEF];

        let logs = vec![
            format!("Program 11111111111111111111111111111111 invoke [1]"),
            format!("Program data: {}", b64(&event_bytes)),
            "Program 11111111111111111111111111111111 success".to_string(),
        ];

        for line in &logs {
            parser.parse_log_line(line, slot, bt, &sig);
        }

        assert!(!parser.is_truncated());
        let events = parser.into_events();
        assert_eq!(events.len(), 1);

        let ev = &events[0];
        assert_eq!(ev.slot, slot);
        assert_eq!(ev.block_time, bt);
        assert_eq!(ev.transaction_signature, sig);
        assert_eq!(ev.event_discriminator, [1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(ev.event_data, vec![0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(ev.instruction_index, 0);
        assert_eq!(ev.inner_instruction_index, None);
        assert_eq!(ev.log_index, 0);

        // Program ID should be the system program
        let expected_pid = Pubkey::default(); // 11111111... = default/system
        assert_eq!(ev.program_id, expected_pid.to_bytes());
    }

    #[test]
    fn test_cpi_event() {
        let mut parser = ProgramLogParser::new();
        let sig = test_sig();

        // Program A (outer) invokes Program B (inner), B emits event.
        let program_a = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
        let program_b = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";

        let event_bytes: Vec<u8> = vec![10, 20, 30, 40, 50, 60, 70, 80, 99];

        let logs = vec![
            format!("Program {program_a} invoke [1]"),
            format!("Program {program_b} invoke [2]"),
            format!("Program data: {}", b64(&event_bytes)),
            format!("Program {program_b} success"),
            format!("Program {program_a} success"),
        ];

        for line in &logs {
            parser.parse_log_line(line, 200, None, &sig);
        }

        assert!(!parser.is_truncated());
        let events = parser.into_events();
        assert_eq!(events.len(), 1);

        let ev = &events[0];
        assert_eq!(ev.instruction_index, 0);
        assert_eq!(ev.inner_instruction_index, Some(0));

        // Event should be attributed to program B (the top of stack when data was emitted).
        let expected_pid: Pubkey = program_b.parse().unwrap();
        assert_eq!(ev.program_id, expected_pid.to_bytes());
    }

    #[test]
    fn test_multiple_top_level_instructions() {
        let mut parser = ProgramLogParser::new();
        let sig = test_sig();

        let prog = "11111111111111111111111111111111";
        let event_bytes: Vec<u8> = vec![0; 8]; // minimal discriminator, no extra data

        // First instruction — no event
        let logs = vec![
            format!("Program {prog} invoke [1]"),
            format!("Program {prog} success"),
            // Second instruction — emits event
            format!("Program {prog} invoke [1]"),
            format!("Program data: {}", b64(&event_bytes)),
            format!("Program {prog} success"),
        ];

        for line in &logs {
            parser.parse_log_line(line, 300, None, &sig);
        }

        let events = parser.into_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].instruction_index, 1);
        assert_eq!(events[0].inner_instruction_index, None);
    }

    #[test]
    fn test_failed_program_pops_stack() {
        let mut parser = ProgramLogParser::new();
        let sig = test_sig();

        let prog_a = "11111111111111111111111111111111";
        let prog_b = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

        let logs = vec![
            format!("Program {prog_a} invoke [1]"),
            format!("Program {prog_b} invoke [2]"),
            format!("Program {prog_b} failed: insufficient funds"),
            format!("Program {prog_a} success"),
        ];

        for line in &logs {
            parser.parse_log_line(line, 400, None, &sig);
        }

        assert!(!parser.is_truncated());
        assert!(parser.into_events().is_empty());
    }

    #[test]
    fn test_empty_logs() {
        let parser = ProgramLogParser::new();
        // No lines parsed at all
        assert!(!parser.is_truncated());
        assert!(parser.into_events().is_empty());
    }

    #[test]
    fn test_short_data_skipped() {
        let mut parser = ProgramLogParser::new();
        let sig = test_sig();
        let prog = "11111111111111111111111111111111";

        // Only 4 bytes — less than 8, should be skipped
        let short_data = vec![1, 2, 3, 4];

        let logs = vec![
            format!("Program {prog} invoke [1]"),
            format!("Program data: {}", b64(&short_data)),
            format!("Program {prog} success"),
        ];

        for line in &logs {
            parser.parse_log_line(line, 500, None, &sig);
        }

        assert!(parser.into_events().is_empty());
    }

    #[test]
    fn test_exactly_8_bytes_produces_event_with_empty_data() {
        let mut parser = ProgramLogParser::new();
        let sig = test_sig();
        let prog = "11111111111111111111111111111111";

        let exactly_8 = vec![1, 2, 3, 4, 5, 6, 7, 8];

        let logs = vec![
            format!("Program {prog} invoke [1]"),
            format!("Program data: {}", b64(&exactly_8)),
            format!("Program {prog} success"),
        ];

        for line in &logs {
            parser.parse_log_line(line, 500, None, &sig);
        }

        let events = parser.into_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_discriminator, [1, 2, 3, 4, 5, 6, 7, 8]);
        assert!(events[0].event_data.is_empty());
    }

    #[test]
    fn test_truncation_detection() {
        let mut parser = ProgramLogParser::new();
        let sig = test_sig();
        let prog = "11111111111111111111111111111111";

        // Invoke with no matching success/failed → truncated
        let logs = vec![format!("Program {prog} invoke [1]")];

        for line in &logs {
            parser.parse_log_line(line, 600, None, &sig);
        }

        assert!(parser.is_truncated());
    }

    #[test]
    fn test_non_program_log_lines_ignored() {
        let mut parser = ProgramLogParser::new();
        let sig = test_sig();

        let logs = vec![
            "Transfer: 1000 lamports".to_string(),
            "Some random log message".to_string(),
            "".to_string(),
        ];

        for line in &logs {
            parser.parse_log_line(line, 700, None, &sig);
        }

        assert!(!parser.is_truncated());
        assert!(parser.into_events().is_empty());
    }

    #[test]
    fn test_data_outside_invoke_ignored() {
        let mut parser = ProgramLogParser::new();
        let sig = test_sig();

        // "Program data:" line without any invoke on the stack
        let event_bytes: Vec<u8> = vec![0; 16];
        let logs = vec![format!("Program data: {}", b64(&event_bytes))];

        for line in &logs {
            parser.parse_log_line(line, 800, None, &sig);
        }

        assert!(parser.into_events().is_empty());
    }

    #[test]
    fn test_invalid_base64_skipped() {
        let mut parser = ProgramLogParser::new();
        let sig = test_sig();
        let prog = "11111111111111111111111111111111";

        let logs = vec![
            format!("Program {prog} invoke [1]"),
            "Program data: !!!not_valid_base64!!!".to_string(),
            format!("Program {prog} success"),
        ];

        for line in &logs {
            parser.parse_log_line(line, 900, None, &sig);
        }

        assert!(parser.into_events().is_empty());
    }

    #[test]
    fn test_invalid_pubkey_in_invoke_ignored() {
        let mut parser = ProgramLogParser::new();
        let sig = test_sig();

        let logs = vec!["Program not_a_valid_pubkey invoke [1]".to_string()];

        for line in &logs {
            parser.parse_log_line(line, 1000, None, &sig);
        }

        // Invalid pubkey should not be pushed onto stack
        assert!(!parser.is_truncated());
    }

    #[test]
    fn test_multiple_events_incrementing_log_index() {
        let mut parser = ProgramLogParser::new();
        let sig = test_sig();
        let prog = "11111111111111111111111111111111";

        let event_bytes: Vec<u8> = vec![0; 16];

        let logs = vec![
            format!("Program {prog} invoke [1]"),
            format!("Program data: {}", b64(&event_bytes)),
            format!("Program data: {}", b64(&event_bytes)),
            format!("Program {prog} success"),
        ];

        for line in &logs {
            parser.parse_log_line(line, 1100, None, &sig);
        }

        let events = parser.into_events();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].log_index, 0);
        assert_eq!(events[1].log_index, 1);
    }

    #[test]
    fn test_nested_cpi_depth_3() {
        let mut parser = ProgramLogParser::new();
        let sig = test_sig();

        let prog_a = "11111111111111111111111111111111";
        let prog_b = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
        let prog_c = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";

        let event_bytes: Vec<u8> = vec![0; 16];

        let logs = vec![
            format!("Program {prog_a} invoke [1]"),
            format!("Program {prog_b} invoke [2]"),
            format!("Program {prog_c} invoke [3]"),
            format!("Program data: {}", b64(&event_bytes)),
            format!("Program {prog_c} success"),
            format!("Program {prog_b} success"),
            format!("Program {prog_a} success"),
        ];

        for line in &logs {
            parser.parse_log_line(line, 1200, None, &sig);
        }

        let events = parser.into_events();
        assert_eq!(events.len(), 1);

        let ev = &events[0];
        // Event emitted by prog_c at depth 3
        let expected_pid: Pubkey = prog_c.parse().unwrap();
        assert_eq!(ev.program_id, expected_pid.to_bytes());
        assert_eq!(ev.instruction_index, 0);
        // Inner instruction index: depth-2 invoke incremented to 1,
        // depth-3 invoke incremented to 2. The event at depth > 1
        // uses inner_instruction_count - 1.
        assert!(ev.inner_instruction_index.is_some());
    }

    // -- extract_events_from_logs tests --

    #[test]
    fn test_extract_events_from_logs_empty() {
        let result = extract_events_from_logs(&[], 100, None, &test_sig(), &HashSet::new());
        assert!(result.is_empty());
    }

    #[test]
    fn test_extract_events_program_filtering() {
        let sig = test_sig();
        let prog_a = "11111111111111111111111111111111";
        let prog_b = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

        let event_bytes: Vec<u8> = vec![0; 16];

        let logs = vec![
            format!("Program {prog_a} invoke [1]"),
            format!("Program data: {}", b64(&event_bytes)),
            format!("Program {prog_a} success"),
            format!("Program {prog_b} invoke [1]"),
            format!("Program data: {}", b64(&event_bytes)),
            format!("Program {prog_b} success"),
        ];

        // Only configure prog_b
        let prog_b_key: Pubkey = prog_b.parse().unwrap();
        let mut configured = HashSet::new();
        configured.insert(prog_b_key.to_bytes());

        let events = extract_events_from_logs(
            &logs.iter().map(String::from).collect::<Vec<_>>(),
            100,
            None,
            &sig,
            &configured,
        );

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].program_id, prog_b_key.to_bytes());
    }

    #[test]
    fn test_extract_events_all_programs_configured() {
        let sig = test_sig();
        let prog = "11111111111111111111111111111111";

        let event_bytes: Vec<u8> = vec![0; 16];

        let logs = vec![
            format!("Program {prog} invoke [1]"),
            format!("Program data: {}", b64(&event_bytes)),
            format!("Program {prog} success"),
        ];

        let prog_key: Pubkey = prog.parse().unwrap();
        let mut configured = HashSet::new();
        configured.insert(prog_key.to_bytes());

        let events = extract_events_from_logs(
            &logs.iter().map(String::from).collect::<Vec<_>>(),
            42,
            Some(999),
            &sig,
            &configured,
        );

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].slot, 42);
        assert_eq!(events[0].block_time, Some(999));
    }

    #[test]
    fn test_extract_events_no_configured_programs() {
        let sig = test_sig();
        let prog = "11111111111111111111111111111111";

        let event_bytes: Vec<u8> = vec![0; 16];

        let logs = vec![
            format!("Program {prog} invoke [1]"),
            format!("Program data: {}", b64(&event_bytes)),
            format!("Program {prog} success"),
        ];

        // Empty configured set → all events filtered out
        let events = extract_events_from_logs(
            &logs.iter().map(String::from).collect::<Vec<_>>(),
            42,
            None,
            &sig,
            &HashSet::new(),
        );

        assert!(events.is_empty());
    }

    #[test]
    fn test_multiple_instructions_with_cpi_events() {
        let mut parser = ProgramLogParser::new();
        let sig = test_sig();

        let prog_a = "11111111111111111111111111111111";
        let prog_b = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

        let event_bytes_1: Vec<u8> = vec![1, 0, 0, 0, 0, 0, 0, 0, 0xAA];
        let event_bytes_2: Vec<u8> = vec![2, 0, 0, 0, 0, 0, 0, 0, 0xBB];

        // First top-level: A calls B, B emits event
        let logs = vec![
            format!("Program {prog_a} invoke [1]"),
            format!("Program {prog_b} invoke [2]"),
            format!("Program data: {}", b64(&event_bytes_1)),
            format!("Program {prog_b} success"),
            format!("Program {prog_a} success"),
            // Second top-level: A directly emits event
            format!("Program {prog_a} invoke [1]"),
            format!("Program data: {}", b64(&event_bytes_2)),
            format!("Program {prog_a} success"),
        ];

        for line in &logs {
            parser.parse_log_line(line, 2000, None, &sig);
        }

        let events = parser.into_events();
        assert_eq!(events.len(), 2);

        // First event: from CPI
        assert_eq!(events[0].instruction_index, 0);
        assert_eq!(events[0].inner_instruction_index, Some(0));
        assert_eq!(events[0].event_discriminator[0], 1);

        // Second event: from top-level
        assert_eq!(events[1].instruction_index, 1);
        assert_eq!(events[1].inner_instruction_index, None);
        assert_eq!(events[1].event_discriminator[0], 2);
    }
}
