//! macOS-specific filesystem adapter scaffolding.

use legato_client_core::ClientRuntime;
use legato_types::{
    ClientPlatform, FileId, FilesystemAttributes, FilesystemError, FilesystemOperation,
    FilesystemSemantics, platform_error_code,
};

/// Adapter wrapper for the eventual macOS filesystem implementation.
#[derive(Debug)]
pub struct MacosFilesystem {
    runtime: ClientRuntime,
    mount_point: String,
    semantics: FilesystemSemantics,
}

/// Planned macOS mount operation derived from the shared filesystem contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MacosOperation {
    /// Resolve a path through lookup/getattr semantics.
    Lookup {
        /// Canonical path being resolved.
        path: String,
    },
    /// Enumerate directory entries.
    ReadDir {
        /// Canonical directory path being enumerated.
        path: String,
    },
    /// Open a file for reading.
    Open {
        /// Canonical file path being opened.
        path: String,
    },
    /// Read a range from an open handle.
    Read {
        /// Stable file identifier for diagnostics.
        file_id: FileId,
        /// Starting byte offset for the read.
        offset: u64,
        /// Requested read length.
        size: u32,
    },
    /// Reject a mutating operation as read-only.
    RejectWrite {
        /// Original operation that was denied.
        operation: FilesystemOperation,
        /// Platform errno returned to the kernel.
        errno: i32,
    },
}

impl MacosFilesystem {
    /// Creates a new macOS adapter shell around the shared client runtime.
    #[must_use]
    pub fn new(runtime: ClientRuntime, mount_point: impl Into<String>) -> Self {
        Self {
            runtime,
            mount_point: mount_point.into(),
            semantics: FilesystemSemantics::default(),
        }
    }

    /// Returns a stable platform identifier for diagnostics and tests.
    #[must_use]
    pub fn platform_name(&self) -> &'static str {
        let _ = &self.runtime;
        "macos"
    }

    /// Returns the configured mount point.
    #[must_use]
    pub fn mount_point(&self) -> &str {
        &self.mount_point
    }

    /// Returns the shared filesystem semantics applied by the adapter.
    #[must_use]
    pub fn semantics(&self) -> FilesystemSemantics {
        self.semantics
    }

    /// Converts a shared operation into the macOS adapter plan.
    #[must_use]
    pub fn plan_operation(&self, operation: FilesystemOperation, path: &str) -> MacosOperation {
        if self.semantics.denies(operation) {
            return MacosOperation::RejectWrite {
                operation,
                errno: platform_error_code(ClientPlatform::Macos, FilesystemError::ReadOnly)
                    .raw_code,
            };
        }

        match operation {
            FilesystemOperation::Lookup | FilesystemOperation::GetAttr => MacosOperation::Lookup {
                path: String::from(path),
            },
            FilesystemOperation::ReadDir => MacosOperation::ReadDir {
                path: String::from(path),
            },
            FilesystemOperation::Open => MacosOperation::Open {
                path: String::from(path),
            },
            FilesystemOperation::Read => MacosOperation::Read {
                file_id: FileId(0),
                offset: 0,
                size: 0,
            },
            FilesystemOperation::Release => MacosOperation::Lookup {
                path: String::from(path),
            },
            FilesystemOperation::Create
            | FilesystemOperation::Write
            | FilesystemOperation::Rename
            | FilesystemOperation::Unlink => unreachable!("read-only operations return early"),
        }
    }

    /// Converts shared metadata into the macOS adapter attribute shape.
    #[must_use]
    pub fn translate_attributes(&self, attributes: &FilesystemAttributes) -> MacosAttributes {
        MacosAttributes {
            inode: attributes.file_id.0,
            size: attributes.size,
            mtime_ns: attributes.mtime_ns,
            directory: attributes.is_dir,
            read_only: attributes.read_only,
            block_size: attributes.block_size,
        }
    }
}

/// Adapter-local attribute representation suitable for a future macFUSE binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MacosAttributes {
    /// Stable inode-like identifier.
    pub inode: u64,
    /// File size in bytes.
    pub size: u64,
    /// Modification time in nanoseconds since the Unix epoch.
    pub mtime_ns: u64,
    /// Whether the entry is a directory.
    pub directory: bool,
    /// Whether the entry is writable.
    pub read_only: bool,
    /// Exposed block size.
    pub block_size: u32,
}

#[cfg(test)]
mod tests {
    use super::{MacosFilesystem, MacosOperation};
    use legato_client_core::{ClientConfig, ClientRuntime};
    use legato_types::{FileId, FilesystemAttributes, FilesystemOperation};

    #[test]
    fn adapter_is_constructible_on_non_macos_hosts() {
        let adapter = MacosFilesystem::new(
            ClientRuntime::new(ClientConfig::default()),
            "/Volumes/Legato",
        );
        assert_eq!(adapter.platform_name(), "macos");
        assert_eq!(adapter.mount_point(), "/Volumes/Legato");
    }

    #[test]
    fn mutating_operations_are_rejected_as_read_only() {
        let adapter = MacosFilesystem::new(
            ClientRuntime::new(ClientConfig::default()),
            "/Volumes/Legato",
        );
        let plan = adapter.plan_operation(FilesystemOperation::Write, "/Kontakt/piano.nki");

        assert!(matches!(plan, MacosOperation::RejectWrite { .. }));
    }

    #[test]
    fn attributes_translate_into_macos_shape() {
        let adapter = MacosFilesystem::new(
            ClientRuntime::new(ClientConfig::default()),
            "/Volumes/Legato",
        );
        let attrs = adapter.translate_attributes(&FilesystemAttributes {
            file_id: FileId(7),
            path: "/srv/libraries/Kontakt/piano.nki".into(),
            is_dir: false,
            size: 4096,
            mtime_ns: 55,
            block_size: 4096,
            read_only: true,
        });

        assert_eq!(attrs.inode, 7);
        assert_eq!(attrs.size, 4096);
        assert!(attrs.read_only);
    }
}
