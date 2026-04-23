//! Shared domain types for Legato components.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Stable server-assigned identifier for a library file.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct FileId(pub u64);

/// Relative importance assigned to an explicit or speculative prefetch range.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
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

/// Fixed-size block range retained for block-transport compatibility.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct BlockRange {
    /// The stable identifier of the target file.
    pub file_id: FileId,
    /// The inclusive starting byte offset.
    pub start_offset: u64,
    /// Number of fixed-size blocks requested from the start offset.
    pub block_count: u32,
}

/// Semantic extent range tied to a stable file identifier.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ExtentRange {
    /// Stable identifier of the target file.
    pub file_id: FileId,
    /// Logical extent index within the file layout.
    pub extent_index: u32,
    /// The inclusive starting byte offset.
    pub file_offset: u64,
    /// Logical extent length in bytes.
    pub length: u64,
}

/// A project-derived request to warm data into the client cache.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrefetchRequest {
    /// Requested extents ordered by the planner.
    pub extents: Vec<PrefetchPlanEntry>,
    /// Highest priority that must be durably resident before returning.
    pub wait_through: PrefetchPriority,
}

/// A single file/range request paired with an explicit priority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrefetchPlanEntry {
    /// Extent to make resident in the local cache.
    pub extent: ExtentRange,
    /// Scheduling priority for the range.
    pub priority: PrefetchPriority,
}

/// A path emitted by a project parser before the client resolves it remotely.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrefetchHintPath {
    /// Canonical library path or a path that can be resolved against the mount.
    pub path: PathBuf,
    /// Starting byte offset for the requested warmup.
    pub file_offset: u64,
    /// Total byte length requested for the warmup.
    pub length: u64,
    /// Scheduling priority assigned by the parser.
    pub priority: PrefetchPriority,
}

/// Supported native client platform backends.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ClientPlatform {
    /// macOS user-space mount backend.
    Macos,
    /// Windows WinFSP-style mount backend.
    Windows,
}

/// Read-only filesystem operations exposed by the native mount adapters.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FilesystemOperation {
    /// Resolve a path to attributes.
    Lookup,
    /// Return inode-like metadata for one file or directory.
    GetAttr,
    /// Enumerate direct children of a directory.
    ReadDir,
    /// Open a file for reading.
    Open,
    /// Read byte ranges from an opened file.
    Read,
    /// Release a previously opened file handle.
    Release,
    /// Attempt to create a file or directory.
    Create,
    /// Attempt to write file contents.
    Write,
    /// Attempt to rename a path.
    Rename,
    /// Attempt to remove a path.
    Unlink,
}

/// Normalized file attributes returned by the adapter layer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FilesystemAttributes {
    /// Stable server-assigned file identifier.
    pub file_id: FileId,
    /// Canonical library path represented by these attributes.
    pub path: PathBuf,
    /// Whether the target is a directory.
    pub is_dir: bool,
    /// Logical file size in bytes.
    pub size: u64,
    /// Modification time in nanoseconds since the Unix epoch.
    pub mtime_ns: u64,
    /// Fixed block size exposed by the mount.
    pub block_size: u32,
    /// Whether the mount should present the path as writable.
    pub read_only: bool,
}

/// Cross-platform error categories surfaced by adapter operations.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FilesystemError {
    /// The path does not exist or cannot be resolved.
    NotFound,
    /// The caller attempted a mutating operation on the read-only mount.
    ReadOnly,
    /// The request references a stale server-local handle.
    StaleHandle,
    /// The server is temporarily unreachable or reconnecting.
    Transient,
    /// The request was malformed for the current filesystem state.
    InvalidInput,
}

/// Shared read-only contract for native filesystem adapters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FilesystemSemantics {
    /// Whether the mount denies all mutating operations.
    pub read_only: bool,
    /// Whether directory listings are returned in sorted path order.
    pub deterministic_readdir: bool,
    /// Whether directory timestamps are synthesized from the metadata plane.
    pub stable_directory_mtime: bool,
}

impl Default for FilesystemSemantics {
    fn default() -> Self {
        Self {
            read_only: true,
            deterministic_readdir: true,
            stable_directory_mtime: true,
        }
    }
}

impl FilesystemSemantics {
    /// Returns whether the operation should be denied by read-only policy.
    #[must_use]
    pub fn denies(&self, operation: FilesystemOperation) -> bool {
        self.read_only
            && matches!(
                operation,
                FilesystemOperation::Create
                    | FilesystemOperation::Write
                    | FilesystemOperation::Rename
                    | FilesystemOperation::Unlink
            )
    }
}

/// Platform-specific errno/status values derived from shared adapter errors.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PlatformErrorCode {
    /// Stable symbolic name used in tests and diagnostics.
    pub symbolic_name: &'static str,
    /// Numeric status/errno value for the target platform.
    pub raw_code: i32,
}

/// Maps the shared adapter error taxonomy to platform-specific status codes.
#[must_use]
pub fn platform_error_code(platform: ClientPlatform, error: FilesystemError) -> PlatformErrorCode {
    match (platform, error) {
        (ClientPlatform::Macos, FilesystemError::NotFound) => PlatformErrorCode {
            symbolic_name: "ENOENT",
            raw_code: 2,
        },
        (ClientPlatform::Macos, FilesystemError::ReadOnly) => PlatformErrorCode {
            symbolic_name: "EROFS",
            raw_code: 30,
        },
        (ClientPlatform::Macos, FilesystemError::StaleHandle) => PlatformErrorCode {
            symbolic_name: "ESTALE",
            raw_code: 70,
        },
        (ClientPlatform::Macos, FilesystemError::Transient) => PlatformErrorCode {
            symbolic_name: "EAGAIN",
            raw_code: 35,
        },
        (ClientPlatform::Macos, FilesystemError::InvalidInput) => PlatformErrorCode {
            symbolic_name: "EINVAL",
            raw_code: 22,
        },
        (ClientPlatform::Windows, FilesystemError::NotFound) => PlatformErrorCode {
            symbolic_name: "STATUS_OBJECT_NAME_NOT_FOUND",
            raw_code: 0xC000_0034_u32 as i32,
        },
        (ClientPlatform::Windows, FilesystemError::ReadOnly) => PlatformErrorCode {
            symbolic_name: "STATUS_MEDIA_WRITE_PROTECTED",
            raw_code: 0xC000_00A2_u32 as i32,
        },
        (ClientPlatform::Windows, FilesystemError::StaleHandle) => PlatformErrorCode {
            symbolic_name: "STATUS_FILE_INVALID",
            raw_code: 0xC000_0098_u32 as i32,
        },
        (ClientPlatform::Windows, FilesystemError::Transient) => PlatformErrorCode {
            symbolic_name: "STATUS_RETRY",
            raw_code: 0xC000_022D_u32 as i32,
        },
        (ClientPlatform::Windows, FilesystemError::InvalidInput) => PlatformErrorCode {
            symbolic_name: "STATUS_INVALID_PARAMETER",
            raw_code: 0xC000_000D_u32 as i32,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ClientPlatform, ExtentRange, FileId, FilesystemError, FilesystemOperation,
        FilesystemSemantics, PrefetchPlanEntry, PrefetchPriority, PrefetchRequest,
        platform_error_code,
    };

    #[test]
    fn prefetch_request_keeps_priority_ordering_explicit() {
        let request = PrefetchRequest {
            extents: vec![
                PrefetchPlanEntry {
                    extent: ExtentRange {
                        file_id: FileId(7),
                        extent_index: 0,
                        file_offset: 0,
                        length: 1 << 20,
                    },
                    priority: PrefetchPriority::P0,
                },
                PrefetchPlanEntry {
                    extent: ExtentRange {
                        file_id: FileId(7),
                        extent_index: 1,
                        file_offset: 1 << 20,
                        length: 2 << 20,
                    },
                    priority: PrefetchPriority::P2,
                },
            ],
            wait_through: PrefetchPriority::P1,
        };

        assert_eq!(request.extents.len(), 2);
        assert_eq!(request.wait_through, PrefetchPriority::P1);
    }

    #[test]
    fn read_only_semantics_block_mutating_operations() {
        let semantics = FilesystemSemantics::default();

        assert!(semantics.denies(FilesystemOperation::Write));
        assert!(semantics.denies(FilesystemOperation::Rename));
        assert!(!semantics.denies(FilesystemOperation::Read));
        assert!(!semantics.denies(FilesystemOperation::ReadDir));
    }

    #[test]
    fn platform_error_code_maps_shared_errors_consistently() {
        assert_eq!(
            platform_error_code(ClientPlatform::Macos, FilesystemError::ReadOnly).symbolic_name,
            "EROFS"
        );
        assert_eq!(
            platform_error_code(ClientPlatform::Windows, FilesystemError::Transient).symbolic_name,
            "STATUS_RETRY"
        );
    }
}
