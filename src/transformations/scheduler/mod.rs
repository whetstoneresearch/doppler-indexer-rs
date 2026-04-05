//! DAG-aware handler scheduling primitives.
//!
//! Provides [`CompletionTracker`] and [`DagScheduler`], the pipelined
//! dependency-aware scheduler that replaces the sequential topological loop
//! in `run_handler_catchup` (Phase 2) and the dep-unaware task spawn in
//! `process_range` (Phase 3). See `docs/concurrent_handlers/design.md`.
//!
//! # Unit of work
//!
//! Every atomic work unit is a `(handler, range_start)` pair represented by
//! [`WorkItem`]. For each dependency edge `B depends on A`, no execution of
//! `B` on range `R` starts until `A` on range `R` is recorded as completed
//! in the [`CompletionTracker`]. Different ranges of the same DAG proceed
//! independently and concurrently.
//!
//! # Layering
//!
//! [`DagScheduler`] is pure: it knows only about tracker state, a global
//! concurrency semaphore, and a caller-supplied async runner closure. DB
//! writes, handler invocation, and context construction live in the runner
//! (wired up in Phase 2). This keeps the scheduler fully testable without
//! a `DbPool`.
//!
//! [`CompletionTracker`]: tracker::CompletionTracker
//! [`DagScheduler`]: dag::DagScheduler
//! [`WorkItem`]: dag::WorkItem

// Phase 1 adds these primitives with no production callers yet; Phase 2 wires
// them into catchup. Silence dead-code warnings until then.
#![allow(dead_code)]

pub(crate) mod dag;
pub(crate) mod tracker;
