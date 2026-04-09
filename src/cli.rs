//! CLI argument parsing and indexer mode detection.
//!
//! Centralizes all flag parsing into an `IndexerMode` enum so that
//! `main.rs` works with a single discriminated union instead of
//! scattered boolean flags.

use anyhow::Context;

use crate::types::shared::repair::RepairScope;

/// The top-level operating mode of the indexer.
///
/// Invalid flag combinations (e.g. `--live-only` + `--decode-only`) are
/// rejected at parse time, so downstream code can pattern-match without
/// worrying about impossible states.
#[derive(Debug, Clone)]
pub enum IndexerMode {
    /// Full historical + live pipeline (the default).
    Full {
        catch_up_only: bool,
        repair: bool,
        repair_scope: Option<RepairScope>,
    },
    /// Decode existing raw parquet files without collection or transformations.
    DecodeOnly {
        repair: bool,
        repair_scope: Option<RepairScope>,
    },
    /// Skip historical processing and start directly in live mode.
    LiveOnly,
    /// Run eth_call repair passes only — no collection, transformations, or live mode.
    RepairOnly {
        repair_scope: Option<RepairScope>,
    },
}

impl IndexerMode {
    pub fn is_decode_only(&self) -> bool {
        matches!(self, IndexerMode::DecodeOnly { .. })
    }

    pub fn is_live_only(&self) -> bool {
        matches!(self, IndexerMode::LiveOnly)
    }

    pub fn is_catch_up_only(&self) -> bool {
        matches!(
            self,
            IndexerMode::Full {
                catch_up_only: true,
                ..
            }
        )
    }

    /// Whether the mode requires a database pool (i.e. not decode-only or repair-only).
    pub fn needs_database(&self) -> bool {
        matches!(self, IndexerMode::Full { .. } | IndexerMode::LiveOnly)
    }

    /// Whether the mode needs a WebSocket connection for live subscriptions.
    pub fn needs_websocket(&self) -> bool {
        match self {
            IndexerMode::Full {
                catch_up_only,
                repair,
                ..
            } => !catch_up_only && !repair,
            IndexerMode::LiveOnly => true,
            IndexerMode::DecodeOnly { .. } | IndexerMode::RepairOnly { .. } => false,
        }
    }

    /// Whether repair passes should run.
    pub fn is_repair(&self) -> bool {
        match self {
            IndexerMode::Full { repair, .. } | IndexerMode::DecodeOnly { repair, .. } => *repair,
            IndexerMode::RepairOnly { .. } => true,
            IndexerMode::LiveOnly => false,
        }
    }

    /// The repair scope, if any.
    pub fn repair_scope(&self) -> Option<&RepairScope> {
        match self {
            IndexerMode::Full { repair_scope, .. }
            | IndexerMode::DecodeOnly { repair_scope, .. }
            | IndexerMode::RepairOnly { repair_scope } => repair_scope.as_ref(),
            IndexerMode::LiveOnly => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Flag parsing helpers
// ---------------------------------------------------------------------------

/// Collect all values for a repeatable `--flag value` or `--flag=value` argument,
/// splitting on commas.
pub fn collect_flag_values(args: &[String], flag: &str) -> anyhow::Result<Vec<String>> {
    let mut values = Vec::new();
    let mut idx = 0usize;

    while idx < args.len() {
        let arg = &args[idx];
        if arg == flag {
            let Some(next) = args.get(idx + 1) else {
                anyhow::bail!("Missing value for {}", flag);
            };
            if next.starts_with("--") {
                anyhow::bail!("Missing value for {}", flag);
            }
            values.extend(
                next.split(',')
                    .map(str::trim)
                    .filter(|v| !v.is_empty())
                    .map(ToString::to_string),
            );
            idx += 2;
            continue;
        }

        let prefix = format!("{}=", flag);
        if let Some(raw) = arg.strip_prefix(&prefix) {
            values.extend(
                raw.split(',')
                    .map(str::trim)
                    .filter(|v| !v.is_empty())
                    .map(ToString::to_string),
            );
        }

        idx += 1;
    }

    Ok(values)
}

/// Parse an optional `--flag <u64>` or `--flag=<u64>` argument.
pub fn parse_optional_u64_flag(args: &[String], flag: &str) -> anyhow::Result<Option<u64>> {
    let mut value = None;
    let mut idx = 0usize;

    while idx < args.len() {
        let arg = &args[idx];
        if arg == flag {
            let Some(next) = args.get(idx + 1) else {
                anyhow::bail!("Missing value for {}", flag);
            };
            if next.starts_with("--") {
                anyhow::bail!("Missing value for {}", flag);
            }
            value = Some(
                next.parse::<u64>()
                    .with_context(|| format!("Invalid numeric value '{}' for {}", next, flag))?,
            );
            idx += 2;
            continue;
        }

        let prefix = format!("{}=", flag);
        if let Some(raw) = arg.strip_prefix(&prefix) {
            value = Some(
                raw.parse::<u64>()
                    .with_context(|| format!("Invalid numeric value '{}' for {}", raw, flag))?,
            );
        }

        idx += 1;
    }

    Ok(value)
}

/// Build a `RepairScope` from `--from-block`, `--to-block`, `--source`, `--function` flags.
pub fn build_repair_scope(args: &[String]) -> anyhow::Result<Option<RepairScope>> {
    let from_block = parse_optional_u64_flag(args, "--from-block")?;
    let to_block = parse_optional_u64_flag(args, "--to-block")?;
    let sources = collect_flag_values(args, "--source")?;
    let functions = collect_flag_values(args, "--function")?;

    if let (Some(from_block), Some(to_block)) = (from_block, to_block) {
        anyhow::ensure!(
            from_block <= to_block,
            "--from-block ({}) cannot be greater than --to-block ({})",
            from_block,
            to_block
        );
    }

    let scope = RepairScope {
        from_block,
        to_block,
        sources: (!sources.is_empty()).then(|| sources.into_iter().collect()),
        functions: (!functions.is_empty()).then(|| functions.into_iter().collect()),
    };

    if scope.is_unscoped() {
        Ok(None)
    } else {
        Ok(Some(scope))
    }
}

/// Parse CLI arguments into an `IndexerMode`, rejecting invalid flag combinations.
pub fn parse_mode(args: &[String]) -> anyhow::Result<IndexerMode> {
    let decode_only = args.iter().any(|a| a == "--decode-only");
    let live_only = args.iter().any(|a| a == "--live-only");
    let catch_up_only = args.iter().any(|a| a == "--catch-up-only");
    let repair_only = args.iter().any(|a| a == "--repair-only");
    let repair_flag = args.iter().any(|a| a == "--repair");
    let repair = repair_flag || repair_only;
    let repair_scope = build_repair_scope(args)?;

    // Validate mutually exclusive flags
    if live_only && decode_only {
        anyhow::bail!("Cannot use --live-only and --decode-only together");
    }
    if catch_up_only && live_only {
        anyhow::bail!("Cannot use --catch-up-only and --live-only together");
    }
    if catch_up_only && decode_only {
        anyhow::bail!("Cannot use --catch-up-only and --decode-only together");
    }
    if repair_only && live_only {
        anyhow::bail!("Cannot use --repair-only and --live-only together");
    }
    if repair_only && decode_only {
        anyhow::bail!("Cannot use --repair-only and --decode-only together");
    }
    if repair_only && catch_up_only {
        anyhow::bail!("Cannot use --repair-only and --catch-up-only together");
    }
    if repair_flag && live_only {
        anyhow::bail!("Cannot use --repair and --live-only together");
    }
    if repair_scope.is_some() && !repair {
        anyhow::bail!(
            "--from-block/--to-block/--source/--function require --repair or --repair-only"
        );
    }

    // Build the mode
    let mode = if repair_only {
        IndexerMode::RepairOnly { repair_scope }
    } else if decode_only {
        IndexerMode::DecodeOnly {
            repair,
            repair_scope,
        }
    } else if live_only {
        IndexerMode::LiveOnly
    } else {
        IndexerMode::Full {
            catch_up_only,
            repair,
            repair_scope,
        }
    };

    Ok(mode)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(flags: &[&str]) -> Vec<String> {
        // Prepend a fake binary name like real CLI args
        std::iter::once("indexer")
            .chain(flags.iter().copied())
            .map(String::from)
            .collect()
    }

    #[test]
    fn default_is_full_mode() {
        let mode = parse_mode(&args(&[])).unwrap();
        assert!(matches!(
            mode,
            IndexerMode::Full {
                catch_up_only: false,
                repair: false,
                repair_scope: None
            }
        ));
        assert!(mode.needs_database());
        assert!(mode.needs_websocket());
        assert!(!mode.is_decode_only());
        assert!(!mode.is_live_only());
        assert!(!mode.is_catch_up_only());
        assert!(!mode.is_repair());
    }

    #[test]
    fn decode_only_mode() {
        let mode = parse_mode(&args(&["--decode-only"])).unwrap();
        assert!(mode.is_decode_only());
        assert!(!mode.needs_database());
        assert!(!mode.needs_websocket());
    }

    #[test]
    fn live_only_mode() {
        let mode = parse_mode(&args(&["--live-only"])).unwrap();
        assert!(mode.is_live_only());
        assert!(mode.needs_database());
        assert!(mode.needs_websocket());
    }

    #[test]
    fn catch_up_only_mode() {
        let mode = parse_mode(&args(&["--catch-up-only"])).unwrap();
        assert!(mode.is_catch_up_only());
        assert!(mode.needs_database());
        assert!(!mode.needs_websocket());
    }

    #[test]
    fn repair_only_mode() {
        let mode = parse_mode(&args(&["--repair-only"])).unwrap();
        assert!(matches!(mode, IndexerMode::RepairOnly { repair_scope: None }));
        assert!(!mode.needs_database());
        assert!(!mode.needs_websocket());
        assert!(mode.is_repair());
    }

    #[test]
    fn full_with_repair() {
        let mode = parse_mode(&args(&["--repair"])).unwrap();
        assert!(matches!(
            mode,
            IndexerMode::Full {
                catch_up_only: false,
                repair: true,
                ..
            }
        ));
        assert!(mode.is_repair());
        // repair mode still needs database but not websocket
        assert!(mode.needs_database());
        assert!(!mode.needs_websocket());
    }

    #[test]
    fn repair_scope_requires_repair_flag() {
        let err = parse_mode(&args(&["--from-block", "100"])).unwrap_err();
        assert!(
            err.to_string()
                .contains("--from-block/--to-block/--source/--function require --repair"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn conflicting_flags_rejected() {
        assert!(parse_mode(&args(&["--live-only", "--decode-only"])).is_err());
        assert!(parse_mode(&args(&["--catch-up-only", "--live-only"])).is_err());
        assert!(parse_mode(&args(&["--catch-up-only", "--decode-only"])).is_err());
        assert!(parse_mode(&args(&["--repair-only", "--live-only"])).is_err());
        assert!(parse_mode(&args(&["--repair-only", "--decode-only"])).is_err());
        assert!(parse_mode(&args(&["--repair-only", "--catch-up-only"])).is_err());
        assert!(parse_mode(&args(&["--repair", "--live-only"])).is_err());
    }

    #[test]
    fn repair_only_with_scope() {
        let mode = parse_mode(&args(&[
            "--repair-only",
            "--from-block",
            "100",
            "--to-block",
            "200",
        ]))
        .unwrap();
        assert!(mode.is_repair());
        let scope = mode.repair_scope().unwrap();
        assert_eq!(scope.from_block, Some(100));
        assert_eq!(scope.to_block, Some(200));
    }

    #[test]
    fn collect_flag_values_multiple() {
        let a = args(&["--source", "A,B", "--source", "C"]);
        let values = collect_flag_values(&a, "--source").unwrap();
        assert_eq!(values, vec!["A", "B", "C"]);
    }

    #[test]
    fn collect_flag_values_equals() {
        let a = args(&["--source=X,Y"]);
        let values = collect_flag_values(&a, "--source").unwrap();
        assert_eq!(values, vec!["X", "Y"]);
    }

    #[test]
    fn parse_optional_u64_flag_basic() {
        let a = args(&["--from-block", "42"]);
        assert_eq!(parse_optional_u64_flag(&a, "--from-block").unwrap(), Some(42));
        assert_eq!(parse_optional_u64_flag(&a, "--to-block").unwrap(), None);
    }

    #[test]
    fn build_repair_scope_from_to() {
        let a = args(&["--from-block", "10", "--to-block", "20"]);
        let scope = build_repair_scope(&a).unwrap().unwrap();
        assert_eq!(scope.from_block, Some(10));
        assert_eq!(scope.to_block, Some(20));
    }

    #[test]
    fn build_repair_scope_invalid_range() {
        let a = args(&["--from-block", "100", "--to-block", "50"]);
        assert!(build_repair_scope(&a).is_err());
    }
}
