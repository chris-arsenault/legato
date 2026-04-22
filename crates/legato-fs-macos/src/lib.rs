//! macOS-specific adapter wrappers over the shared Legato filesystem service.

use legato_client_core::{FilesystemOpenHandle, FilesystemService, FilesystemServiceError};
use legato_proto::DirectoryEntry;
use legato_types::{
    ClientPlatform, FilesystemAttributes, FilesystemError, FilesystemOperation,
    FilesystemSemantics, PlatformErrorCode, platform_error_code,
};

/// Adapter wrapper for the macOS filesystem surface.
#[derive(Debug)]
pub struct MacosFilesystem {
    mount_point: String,
    semantics: FilesystemSemantics,
}

/// Adapter-local directory entry representation for macOS mount bindings.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MacosDirectoryEntry {
    /// Entry name relative to the containing directory.
    pub name: String,
    /// Stable inode-like identifier.
    pub inode: u64,
    /// Canonical target path.
    pub path: String,
    /// Whether the entry is a directory.
    pub directory: bool,
}

/// Adapter-local opened file representation for macOS mount bindings.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MacosOpenFile {
    /// Stable local handle returned to the mount layer.
    pub handle: u64,
    /// Attributes captured at open time.
    pub attributes: MacosAttributes,
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

impl MacosFilesystem {
    /// Creates a new macOS adapter around the shared mount point configuration.
    #[must_use]
    pub fn new(mount_point: impl Into<String>) -> Self {
        Self {
            mount_point: mount_point.into(),
            semantics: FilesystemSemantics::default(),
        }
    }

    /// Returns a stable platform identifier for diagnostics and tests.
    #[must_use]
    pub fn platform_name(&self) -> &'static str {
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

    /// Returns the macOS error code for one attempted operation.
    #[must_use]
    pub fn error_code(
        &self,
        operation: FilesystemOperation,
        error: FilesystemError,
    ) -> PlatformErrorCode {
        let _ = operation;
        platform_error_code(ClientPlatform::Macos, error)
    }

    /// Resolves one path through the shared filesystem service.
    pub async fn lookup(
        &self,
        service: &mut FilesystemService,
        path: &str,
    ) -> Result<MacosAttributes, PlatformErrorCode> {
        service
            .lookup(path)
            .await
            .map(|attributes| self.translate_attributes(&attributes))
            .map_err(map_error)
    }

    /// Enumerates one directory through the shared filesystem service.
    pub async fn read_dir(
        &self,
        service: &mut FilesystemService,
        path: &str,
    ) -> Result<Vec<MacosDirectoryEntry>, PlatformErrorCode> {
        service
            .read_dir(path)
            .await
            .map(|entries| entries.into_iter().map(translate_directory_entry).collect())
            .map_err(map_error)
    }

    /// Opens one file through the shared filesystem service.
    pub async fn open(
        &self,
        service: &mut FilesystemService,
        path: &str,
    ) -> Result<MacosOpenFile, PlatformErrorCode> {
        let handle = service.open(path).await.map_err(map_error)?;
        Ok(MacosOpenFile {
            handle: handle.local_handle,
            attributes: self.translate_attributes(&attributes_from_open_handle(&handle)),
        })
    }

    /// Reads one byte range from a previously opened file.
    pub async fn read(
        &self,
        service: &mut FilesystemService,
        handle: u64,
        offset: u64,
        size: u32,
    ) -> Result<Vec<u8>, PlatformErrorCode> {
        service.read(handle, offset, size).await.map_err(map_error)
    }

    /// Releases one open file handle.
    pub async fn release(
        &self,
        service: &mut FilesystemService,
        handle: u64,
    ) -> Result<(), PlatformErrorCode> {
        service.release(handle).await.map_err(map_error)
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

fn map_error(error: FilesystemServiceError) -> PlatformErrorCode {
    let kind = match error {
        FilesystemServiceError::NotFound(_) => FilesystemError::NotFound,
        FilesystemServiceError::UnknownHandle(_) => FilesystemError::StaleHandle,
        FilesystemServiceError::InvalidRead { .. } => FilesystemError::InvalidInput,
        FilesystemServiceError::Transport(_) | FilesystemServiceError::Cache(_) => {
            FilesystemError::Transient
        }
    };
    platform_error_code(ClientPlatform::Macos, kind)
}

fn translate_directory_entry(entry: DirectoryEntry) -> MacosDirectoryEntry {
    MacosDirectoryEntry {
        name: entry.name,
        inode: entry.file_id,
        path: entry.path,
        directory: entry.is_dir,
    }
}

fn attributes_from_open_handle(handle: &FilesystemOpenHandle) -> FilesystemAttributes {
    FilesystemAttributes {
        file_id: handle.file_id,
        path: handle.path.clone().into(),
        is_dir: false,
        size: handle.size,
        mtime_ns: 0,
        block_size: handle.block_size,
        read_only: true,
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use legato_client_core::{ClientConfig, ClientTlsConfig, FilesystemService, RetryPolicy};
    use legato_server::{
        LiveServer, ServerConfig, ServerTlsConfig, ensure_server_tls_materials,
        issue_client_tls_bundle, load_runtime_tls,
    };
    use legato_types::{FileId, FilesystemAttributes, FilesystemOperation};
    use tempfile::tempdir;
    use tokio::net::TcpListener;

    use super::MacosFilesystem;

    fn local_client_config(endpoint: String, bundle_dir: &Path, server_name: &str) -> ClientConfig {
        ClientConfig {
            endpoint,
            tls: ClientTlsConfig::local_dev(bundle_dir, server_name),
            retry: RetryPolicy {
                initial_delay_ms: 0,
                max_delay_ms: 0,
                multiplier: 2,
            },
            ..ClientConfig::default()
        }
    }

    #[test]
    fn adapter_is_constructible_on_non_macos_hosts() {
        let adapter = MacosFilesystem::new("/Volumes/Legato");
        assert_eq!(adapter.platform_name(), "macos");
        assert_eq!(adapter.mount_point(), "/Volumes/Legato");
    }

    #[test]
    fn read_only_semantics_map_to_macos_error_codes() {
        let adapter = MacosFilesystem::new("/Volumes/Legato");
        let code = adapter.error_code(
            FilesystemOperation::Write,
            legato_types::FilesystemError::ReadOnly,
        );

        assert_eq!(code.symbolic_name, "EROFS");
    }

    #[test]
    fn attributes_translate_into_macos_shape() {
        let adapter = MacosFilesystem::new("/Volumes/Legato");
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

    #[tokio::test]
    async fn macos_adapter_serves_real_lookup_readdir_and_read() {
        let fixture = tempdir().expect("tempdir should be created");
        let library_root = fixture.path().join("library");
        let state_dir = fixture.path().join("state");
        let tls_dir = fixture.path().join("tls");
        fs::create_dir_all(library_root.join("Kontakt")).expect("library tree should be created");
        let sample_path = library_root.join("Kontakt").join("piano.nki");
        fs::write(&sample_path, b"hello legato").expect("sample should be written");

        let mut config = ServerConfig {
            bind_address: String::from("127.0.0.1:0"),
            library_root: library_root.to_string_lossy().into_owned(),
            state_dir: state_dir.to_string_lossy().into_owned(),
            tls_dir: tls_dir.to_string_lossy().into_owned(),
            tls: ServerTlsConfig::local_dev(&tls_dir),
        };
        config.tls.server_names = vec![String::from("127.0.0.1"), String::from("localhost")];
        ensure_server_tls_materials(Path::new(&config.tls_dir), &config.tls)
            .expect("tls materials should be created");

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let address = listener.local_addr().expect("addr should be available");
        let server = LiveServer::bootstrap(config.clone()).expect("server should bootstrap");
        let bound = server
            .bind(
                listener,
                Some(load_runtime_tls(&config.tls).expect("runtime tls should load")),
            )
            .await
            .expect("server should bind");

        let bundle_dir = fixture.path().join("bundle");
        issue_client_tls_bundle(
            Path::new(&config.tls_dir),
            &config.tls,
            "studio-mac",
            &bundle_dir,
        )
        .expect("client bundle should be issued");

        let mut service = FilesystemService::connect(
            local_client_config(address.to_string(), &bundle_dir, "localhost"),
            "studio-mac",
            fixture.path().join("client-state").as_path(),
        )
        .await
        .expect("service should connect");
        let adapter = MacosFilesystem::new("/Volumes/Legato");

        let attrs = adapter
            .lookup(&mut service, sample_path.to_string_lossy().as_ref())
            .await
            .expect("lookup should succeed");
        assert_eq!(attrs.inode, 1);

        let entries = adapter
            .read_dir(
                &mut service,
                library_root.join("Kontakt").to_string_lossy().as_ref(),
            )
            .await
            .expect("readdir should succeed");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "piano.nki");

        let open = adapter
            .open(&mut service, sample_path.to_string_lossy().as_ref())
            .await
            .expect("open should succeed");
        let slice = adapter
            .read(&mut service, open.handle, 6, 6)
            .await
            .expect("read should succeed");
        assert_eq!(slice, b"legato");

        adapter
            .release(&mut service, open.handle)
            .await
            .expect("release should succeed");
        bound.shutdown().await.expect("server should shut down");
    }
}
