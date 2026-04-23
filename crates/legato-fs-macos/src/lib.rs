//! macOS-specific adapter wrappers over the shared Legato filesystem service.

#[cfg(target_os = "macos")]
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{
    fmt,
    path::{Path, PathBuf},
};

use legato_client_core::{FilesystemOpenHandle, FilesystemService, FilesystemServiceError};
use legato_proto::DirectoryEntry;
use legato_types::{
    ClientPlatform, FilesystemAttributes, FilesystemError, FilesystemOperation,
    FilesystemSemantics, PlatformErrorCode, platform_error_code,
};
use tokio::sync::Mutex;

#[cfg(target_os = "macos")]
use unifuse::{
    DirEntry as MountDirEntry, FileAttr as MountFileAttr, FileHandle as MountFileHandle,
    FileType as MountFileType, FsError as MountFsError, MountOptions, OpenFlags, StatFs,
    UniFuseFilesystem, UniFuseHost,
};

/// Adapter wrapper for the macOS filesystem surface.
#[derive(Debug)]
pub struct MacosFilesystem {
    mount_point: String,
    semantics: FilesystemSemantics,
}

/// Shared mount state used by the macOS runtime adapter.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
#[derive(Debug)]
pub struct MacosMountService {
    service: Mutex<FilesystemService>,
    library_root: String,
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

/// Result of preparing the configured mount point before handing it to macFUSE.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MountPointReadiness {
    /// The mount point already existed and was usable.
    Ready,
    /// The mount point did not exist and was created.
    Created,
}

/// Clear local mount point conflicts surfaced before macFUSE starts.
#[derive(Debug)]
pub enum MountPointError {
    /// The configured mount point exists but is not a directory.
    NotDirectory(PathBuf),
    /// The configured mount point is a non-empty directory and may already be mounted or busy.
    BusyDirectory(PathBuf),
    /// The mount point could not be inspected or created.
    Io(std::io::Error),
}

impl fmt::Display for MountPointError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotDirectory(path) => {
                write!(
                    formatter,
                    "mount point is not a directory: {}",
                    path.display()
                )
            }
            Self::BusyDirectory(path) => write!(
                formatter,
                "mount point is not empty; unmount or clear it before starting Legato: {}",
                path.display()
            ),
            Self::Io(error) => write!(formatter, "mount point check failed: {error}"),
        }
    }
}

impl std::error::Error for MountPointError {}

impl From<std::io::Error> for MountPointError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
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

impl MacosMountService {
    /// Creates one shared mount service around the connected filesystem runtime.
    #[must_use]
    pub fn new(service: FilesystemService, library_root: impl Into<String>) -> Self {
        Self {
            service: Mutex::new(service),
            library_root: library_root.into(),
        }
    }

    #[cfg_attr(not(target_os = "macos"), allow(dead_code))]
    fn canonical_path(&self, virtual_path: &Path) -> String {
        map_virtual_path(&self.library_root, virtual_path)
    }
}

/// Returns whether the current macOS host appears able to mount the filesystem.
#[cfg(target_os = "macos")]
#[must_use]
pub fn mount_runtime_available() -> bool {
    UniFuseHost::<MacosMountService>::is_available()
}

/// Mounts the Legato filesystem on macOS and blocks until the mount exits.
#[cfg(target_os = "macos")]
pub async fn mount(
    service: FilesystemService,
    mount_point: impl AsRef<Path>,
    library_root: impl Into<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mount_point = mount_point.as_ref().to_path_buf();
    let _ = unmount_existing_mount(&mount_point);
    prepare_mount_point(&mount_point)?;

    let host = UniFuseHost::new(MacosMountService::new(service, library_root));
    let options = MountOptions {
        fs_name: String::from("legato"),
        allow_other: false,
        read_only: true,
    };

    host.mount(&mount_point, &options)
        .await
        .map_err(|error| -> Box<dyn std::error::Error> { Box::new(error) })
}

/// Ensures the mount point is a usable empty directory before mounting.
pub fn prepare_mount_point(path: &Path) -> Result<MountPointReadiness, MountPointError> {
    if !path.exists() {
        std::fs::create_dir_all(path)?;
        return Ok(MountPointReadiness::Created);
    }
    if !path.is_dir() {
        return Err(MountPointError::NotDirectory(path.to_path_buf()));
    }
    if std::fs::read_dir(path)?.next().is_some() {
        return Err(MountPointError::BusyDirectory(path.to_path_buf()));
    }
    Ok(MountPointReadiness::Ready)
}

#[cfg(target_os = "macos")]
fn unmount_existing_mount(path: &Path) -> Result<(), std::io::Error> {
    let path = path.to_string_lossy().into_owned();
    let _ = std::process::Command::new("diskutil")
        .args(["unmount", "force", &path])
        .status();
    let _ = std::process::Command::new("umount").arg(&path).status();
    Ok(())
}

#[cfg(target_os = "macos")]
impl UniFuseFilesystem for MacosMountService {
    async fn getattr(&self, path: &Path) -> Result<MountFileAttr, MountFsError> {
        let attributes = self
            .service
            .lock()
            .await
            .lookup(&self.canonical_path(path))
            .await
            .map_err(map_mount_error)?;
        Ok(attributes_to_mount_attr(&attributes))
    }

    async fn lookup(
        &self,
        parent: &Path,
        name: &std::ffi::OsStr,
    ) -> Result<MountFileAttr, MountFsError> {
        let path = if parent == Path::new("/") {
            PathBuf::from("/").join(name)
        } else {
            parent.join(name)
        };
        self.getattr(&path).await
    }

    async fn open(&self, path: &Path, flags: OpenFlags) -> Result<MountFileHandle, MountFsError> {
        if flags.write {
            return Err(MountFsError::NotSupported);
        }

        let handle = self
            .service
            .lock()
            .await
            .open(&self.canonical_path(path))
            .await
            .map_err(map_mount_error)?;
        Ok(MountFileHandle(handle.local_handle))
    }

    async fn read(
        &self,
        _path: &Path,
        fh: MountFileHandle,
        offset: u64,
        size: u32,
    ) -> Result<Vec<u8>, MountFsError> {
        self.service
            .lock()
            .await
            .read(fh.0, offset, size)
            .await
            .map_err(map_mount_error)
    }

    async fn release(&self, _path: &Path, fh: MountFileHandle) -> Result<(), MountFsError> {
        self.service
            .lock()
            .await
            .release(fh.0)
            .await
            .map_err(map_mount_error)
    }

    async fn readdir(&self, path: &Path) -> Result<Vec<MountDirEntry>, MountFsError> {
        let entries = self
            .service
            .lock()
            .await
            .read_dir(&self.canonical_path(path))
            .await
            .map_err(map_mount_error)?;
        Ok(entries
            .into_iter()
            .map(|entry| MountDirEntry {
                name: entry.name,
                kind: entry_kind(entry.is_dir),
            })
            .collect())
    }

    async fn statfs(&self, _path: &Path) -> Result<StatFs, MountFsError> {
        Ok(StatFs {
            blocks: 1_048_576,
            bfree: 0,
            bavail: 0,
            files: 1_000_000,
            ffree: 0,
            bsize: 1 << 20,
            namelen: 255,
        })
    }
}

fn map_error(error: FilesystemServiceError) -> PlatformErrorCode {
    let kind = match error {
        FilesystemServiceError::NotFound(_) => FilesystemError::NotFound,
        FilesystemServiceError::UnknownHandle(_) => FilesystemError::StaleHandle,
        FilesystemServiceError::InvalidRead { .. } => FilesystemError::InvalidInput,
        FilesystemServiceError::Transport(_) | FilesystemServiceError::Store(_) => {
            FilesystemError::Transient
        }
    };
    platform_error_code(ClientPlatform::Macos, kind)
}

#[cfg(target_os = "macos")]
fn map_mount_error(error: FilesystemServiceError) -> MountFsError {
    match error {
        FilesystemServiceError::NotFound(_) => MountFsError::NotFound,
        FilesystemServiceError::UnknownHandle(_) => {
            MountFsError::Other(String::from("stale handle"))
        }
        FilesystemServiceError::InvalidRead { .. } => {
            MountFsError::Other(String::from("invalid read"))
        }
        FilesystemServiceError::Transport(error) => MountFsError::Other(error.to_string()),
        FilesystemServiceError::Store(error) => MountFsError::Other(error.to_string()),
    }
}

#[cfg(target_os = "macos")]
fn attributes_to_mount_attr(attributes: &FilesystemAttributes) -> MountFileAttr {
    let timestamp = timestamp_from_ns(attributes.mtime_ns);
    MountFileAttr {
        size: attributes.size,
        blocks: attributes.size.div_ceil(512),
        atime: timestamp,
        mtime: timestamp,
        ctime: timestamp,
        crtime: timestamp,
        kind: entry_kind(attributes.is_dir),
        perm: if attributes.is_dir { 0o555 } else { 0o444 },
        nlink: if attributes.is_dir { 2 } else { 1 },
        uid: 0,
        gid: 0,
        rdev: 0,
        flags: 0,
    }
}

#[cfg(target_os = "macos")]
fn entry_kind(is_dir: bool) -> MountFileType {
    if is_dir {
        MountFileType::Directory
    } else {
        MountFileType::RegularFile
    }
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
        mtime_ns: handle.mtime_ns,
        block_size: handle
            .extents
            .first()
            .map_or(0, |extent| extent.length.min(u64::from(u32::MAX)) as u32),
        read_only: true,
    }
}

#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn map_virtual_path(library_root: &str, virtual_path: &Path) -> String {
    let mut mapped = PathBuf::from(library_root);
    for component in virtual_path.components() {
        if let std::path::Component::Normal(segment) = component {
            mapped.push(segment);
        }
    }
    mapped.to_string_lossy().into_owned()
}

#[cfg(target_os = "macos")]
fn timestamp_from_ns(nanoseconds: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_nanos(nanoseconds)
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

    use super::{
        MacosFilesystem, MountPointError, MountPointReadiness, map_virtual_path,
        prepare_mount_point,
    };

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

    #[test]
    fn virtual_root_maps_into_library_root() {
        assert_eq!(
            map_virtual_path("/srv/libraries", Path::new("/")),
            "/srv/libraries"
        );
        assert_eq!(
            map_virtual_path("/srv/libraries", Path::new("/Kontakt/piano.nki")),
            "/srv/libraries/Kontakt/piano.nki"
        );
    }

    #[test]
    fn mount_point_preflight_creates_empty_directory_and_rejects_conflicts() {
        let fixture = tempdir().expect("tempdir should be created");
        let missing = fixture.path().join("Legato");
        assert_eq!(
            prepare_mount_point(&missing).expect("missing mount point should be created"),
            MountPointReadiness::Created
        );
        assert_eq!(
            prepare_mount_point(&missing).expect("empty mount point should be ready"),
            MountPointReadiness::Ready
        );

        let busy = fixture.path().join("Busy");
        fs::create_dir_all(&busy).expect("busy dir should be created");
        fs::write(busy.join("leftover"), b"mounted").expect("busy marker should be written");
        assert!(matches!(
            prepare_mount_point(&busy),
            Err(MountPointError::BusyDirectory(_))
        ));

        let file = fixture.path().join("file");
        fs::write(&file, b"not a directory").expect("file should be written");
        assert!(matches!(
            prepare_mount_point(&file),
            Err(MountPointError::NotDirectory(_))
        ));
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
