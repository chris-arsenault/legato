//! Shared domain types for Legato components.

use std::path::PathBuf;

/// Stable server-assigned identifier for a library file.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FileId(pub u64);

/// Relative importance assigned to an explicit or speculative prefetch range.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum PrefetchPriority {
    /// Must be resident before the DAW begins touching the project.
    P0,
    /// Needed during initial instrument/plugin load.
    P1,
    /// User-visible if missing, but not necessarily required at launch.
    P2,
    /// Best-effort speculative warmup.
    P3,
}

/// Block-oriented byte range tied to a stable file identifier.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct BlockRange {
    /// The stable identifier of the target file.
    pub file_id: FileId,
    /// The inclusive starting byte offset.
    pub start_offset: u64,
    /// Number of fixed-size blocks requested from the start offset.
    pub block_count: u32,
}

/// A project-derived request to warm data into the client cache.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrefetchRequest {
    /// Requested ranges ordered by the planner.
    pub ranges: Vec<PrefetchPlanEntry>,
    /// Highest priority that must be durably resident before returning.
    pub wait_through: PrefetchPriority,
}

/// A single file/range request paired with an explicit priority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrefetchPlanEntry {
    /// Range to make resident in the local cache.
    pub range: BlockRange,
    /// Scheduling priority for the range.
    pub priority: PrefetchPriority,
}

/// A path emitted by a project parser before the client resolves it remotely.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrefetchHintPath {
    /// Canonical library path or a path that can be resolved against the mount.
    pub path: PathBuf,
    /// Starting byte offset for the requested warmup.
    pub start_offset: u64,
    /// Number of blocks associated with the hint.
    pub block_count: u32,
    /// Scheduling priority assigned by the parser.
    pub priority: PrefetchPriority,
}

#[cfg(test)]
mod tests {
    use super::{BlockRange, FileId, PrefetchPlanEntry, PrefetchPriority, PrefetchRequest};

    #[test]
    fn prefetch_request_keeps_priority_ordering_explicit() {
        let request = PrefetchRequest {
            ranges: vec![
                PrefetchPlanEntry {
                    range: BlockRange {
                        file_id: FileId(7),
                        start_offset: 0,
                        block_count: 1,
                    },
                    priority: PrefetchPriority::P0,
                },
                PrefetchPlanEntry {
                    range: BlockRange {
                        file_id: FileId(7),
                        start_offset: 1 << 20,
                        block_count: 2,
                    },
                    priority: PrefetchPriority::P2,
                },
            ],
            wait_through: PrefetchPriority::P1,
        };

        assert_eq!(request.ranges.len(), 2);
        assert_eq!(request.wait_through, PrefetchPriority::P1);
    }
}
