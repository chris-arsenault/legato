//! Windows-specific filesystem adapter scaffolding.

use legato_client_core::ClientRuntime;
use legato_types::{
    ClientPlatform, FileId, FilesystemAttributes, FilesystemError, FilesystemOperation,
    FilesystemSemantics, platform_error_code,
};

/// Adapter wrapper for the eventual WinFSP-backed filesystem implementation.
#[derive(Debug)]
pub struct WindowsFilesystem {
    runtime: ClientRuntime,
    mount_point: String,
    semantics: FilesystemSemantics,
}

/// Planned Windows adapter operations for a future WinFSP integration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WindowsOperation {
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
    /// Read a byte range from an open file.
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
        /// Windows NTSTATUS returned to the caller.
        nt_status: i32,
    },
}

impl WindowsFilesystem {
    /// Creates a new Windows adapter shell around the shared client runtime.
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
        "windows"
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

    /// Converts a shared operation into the Windows adapter plan.
    #[must_use]
    pub fn plan_operation(&self, operation: FilesystemOperation, path: &str) -> WindowsOperation {
        if self.semantics.denies(operation) {
            return WindowsOperation::RejectWrite {
                operation,
                nt_status: platform_error_code(ClientPlatform::Windows, FilesystemError::ReadOnly)
                    .raw_code,
            };
        }

        match operation {
            FilesystemOperation::Lookup | FilesystemOperation::GetAttr => {
                WindowsOperation::Lookup {
                    path: String::from(path),
                }
            }
            FilesystemOperation::ReadDir => WindowsOperation::ReadDir {
                path: String::from(path),
            },
            FilesystemOperation::Open => WindowsOperation::Open {
                path: String::from(path),
            },
            FilesystemOperation::Read => WindowsOperation::Read {
                file_id: FileId(0),
                offset: 0,
                size: 0,
            },
            FilesystemOperation::Release => WindowsOperation::Lookup {
                path: String::from(path),
            },
            FilesystemOperation::Create
            | FilesystemOperation::Write
            | FilesystemOperation::Rename
            | FilesystemOperation::Unlink => unreachable!("read-only operations return early"),
        }
    }

    /// Converts shared metadata into the Windows adapter attribute shape.
    #[must_use]
    pub fn translate_attributes(&self, attributes: &FilesystemAttributes) -> WindowsAttributes {
        WindowsAttributes {
            file_index: attributes.file_id.0,
            allocation_size: attributes.size,
            end_of_file: attributes.size,
            mtime_ns: attributes.mtime_ns,
            directory: attributes.is_dir,
            read_only: attributes.read_only,
        }
    }
}

/// Adapter-local attribute representation suitable for a future WinFSP binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WindowsAttributes {
    /// Stable file index exposed through the adapter.
    pub file_index: u64,
    /// Allocated size in bytes.
    pub allocation_size: u64,
    /// Logical end-of-file size in bytes.
    pub end_of_file: u64,
    /// Modification time in nanoseconds since the Unix epoch.
    pub mtime_ns: u64,
    /// Whether the entry is a directory.
    pub directory: bool,
    /// Whether the entry is writable.
    pub read_only: bool,
}

#[cfg(test)]
mod tests {
    use super::{WindowsFilesystem, WindowsOperation};
    use legato_client_core::{ClientConfig, ClientRuntime};
    use legato_types::{FileId, FilesystemAttributes, FilesystemOperation};

    #[test]
    fn adapter_is_constructible_on_non_windows_hosts() {
        let adapter =
            WindowsFilesystem::new(ClientRuntime::new(ClientConfig::default()), "L:\\Legato");
        assert_eq!(adapter.platform_name(), "windows");
        assert_eq!(adapter.mount_point(), "L:\\Legato");
    }

    #[test]
    fn mutating_operations_are_rejected_as_read_only() {
        let adapter =
            WindowsFilesystem::new(ClientRuntime::new(ClientConfig::default()), "L:\\Legato");
        let plan = adapter.plan_operation(FilesystemOperation::Rename, "\\Kontakt\\piano.nki");

        assert!(matches!(plan, WindowsOperation::RejectWrite { .. }));
    }

    #[test]
    fn attributes_translate_into_windows_shape() {
        let adapter =
            WindowsFilesystem::new(ClientRuntime::new(ClientConfig::default()), "L:\\Legato");
        let attrs = adapter.translate_attributes(&FilesystemAttributes {
            file_id: FileId(7),
            path: "C:\\Legato\\Kontakt\\piano.nki".into(),
            is_dir: false,
            size: 4096,
            mtime_ns: 55,
            block_size: 4096,
            read_only: true,
        });

        assert_eq!(attrs.file_index, 7);
        assert_eq!(attrs.end_of_file, 4096);
        assert!(attrs.read_only);
    }
}
