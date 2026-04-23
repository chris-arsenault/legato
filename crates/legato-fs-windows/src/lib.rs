//! Windows-specific adapter wrappers over the shared Legato filesystem service.

#[cfg(target_os = "windows")]
use std::ffi::c_void;
use std::{
    fmt,
    path::{Path, PathBuf},
};

use legato_client_core::{FilesystemOpenHandle, FilesystemService, FilesystemServiceError};
use legato_prefetch::prefetch_opened_project;
use legato_proto::DirectoryEntry;
use legato_types::{
    ClientPlatform, FilesystemAttributes, FilesystemError, FilesystemOperation,
    FilesystemSemantics, PlatformErrorCode, platform_error_code,
};
#[cfg(target_os = "windows")]
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Mutex;
#[cfg(target_os = "windows")]
use winfsp::{
    FspError, U16CStr,
    filesystem::{
        DirBuffer, DirInfo, DirMarker, FileInfo, FileSecurity, FileSystemContext, OpenFileInfo,
        VolumeInfo, WideNameInfo,
    },
    host::{FileSystemHost, VolumeParams},
};
#[cfg(target_os = "windows")]
use winfsp_sys::FILE_ACCESS_RIGHTS;

#[cfg(target_os = "windows")]
const FILE_ATTRIBUTE_READONLY: u32 = 0x0000_0001;
#[cfg(target_os = "windows")]
const FILE_ATTRIBUTE_DIRECTORY: u32 = 0x0000_0010;
#[cfg(target_os = "windows")]
const FILE_ATTRIBUTE_ARCHIVE: u32 = 0x0000_0020;
#[cfg(target_os = "windows")]
const WINDOWS_TICKS_PER_SECOND: u64 = 10_000_000;
#[cfg(target_os = "windows")]
const WINDOWS_UNIX_EPOCH_OFFSET_SECONDS: u64 = 11_644_473_600;

/// Adapter wrapper for the Windows filesystem surface.
#[derive(Debug)]
pub struct WindowsFilesystem {
    mount_point: String,
    semantics: FilesystemSemantics,
}

/// Shared mount state used by the Windows runtime adapter.
#[cfg_attr(not(target_os = "windows"), allow(dead_code))]
pub struct WindowsMountService {
    service: Mutex<FilesystemService>,
    library_root: String,
    #[cfg(target_os = "windows")]
    runtime: Runtime,
}

impl fmt::Debug for WindowsMountService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WindowsMountService")
            .field("library_root", &self.library_root)
            .finish_non_exhaustive()
    }
}

#[cfg(target_os = "windows")]
#[derive(Debug)]
pub struct WinfspFileContext {
    path: String,
    local_handle: Option<u64>,
    attributes: WindowsAttributes,
    directory_buffer: DirBuffer,
}

/// Adapter-local directory entry representation for Windows mount bindings.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WindowsDirectoryEntry {
    /// Entry name relative to the containing directory.
    pub name: String,
    /// Stable file index.
    pub file_index: u64,
    /// Canonical target path.
    pub path: String,
    /// Whether the entry is a directory.
    pub directory: bool,
}

/// Adapter-local opened file representation for Windows mount bindings.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WindowsOpenFile {
    /// Stable local handle returned to the mount layer.
    pub handle: u64,
    /// Attributes captured at open time.
    pub attributes: WindowsAttributes,
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

/// Result of preparing the configured mount point before handing it to WinFSP.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MountPointReadiness {
    /// The mount point already existed and was usable.
    Ready,
    /// The mount point did not exist and was created.
    Created,
}

/// Clear local mount point conflicts surfaced before WinFSP starts.
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

impl WindowsFilesystem {
    /// Creates a new Windows adapter around the shared mount point configuration.
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

    /// Returns the Windows error code for one attempted operation.
    #[must_use]
    pub fn error_code(
        &self,
        operation: FilesystemOperation,
        error: FilesystemError,
    ) -> PlatformErrorCode {
        let _ = operation;
        platform_error_code(ClientPlatform::Windows, error)
    }

    /// Resolves one path through the shared filesystem service.
    pub async fn lookup(
        &self,
        service: &mut FilesystemService,
        path: &str,
    ) -> Result<WindowsAttributes, PlatformErrorCode> {
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
    ) -> Result<Vec<WindowsDirectoryEntry>, PlatformErrorCode> {
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
    ) -> Result<WindowsOpenFile, PlatformErrorCode> {
        let handle = service.open(path).await.map_err(map_error)?;
        if let Err(error) = prefetch_opened_project(service, &handle).await {
            eprintln!("legato project prefetch skipped for {path}: {error}");
        }
        Ok(WindowsOpenFile {
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

impl WindowsMountService {
    /// Creates one shared mount service around the connected filesystem runtime.
    #[must_use]
    pub fn new(service: FilesystemService, library_root: impl Into<String>) -> Self {
        Self {
            service: Mutex::new(service),
            library_root: library_root.into(),
            #[cfg(target_os = "windows")]
            runtime: Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("windows mount runtime should be created"),
        }
    }

    #[cfg_attr(not(target_os = "windows"), allow(dead_code))]
    fn canonical_path(&self, virtual_path: &Path) -> String {
        map_virtual_path(&self.library_root, virtual_path)
    }
}

/// Returns whether the current Windows host appears able to mount the filesystem.
#[cfg(target_os = "windows")]
#[must_use]
pub fn mount_runtime_available() -> bool {
    winfsp::winfsp_init().is_ok()
}

/// Mounts the Legato filesystem on Windows and blocks until the mount exits.
#[cfg(target_os = "windows")]
pub async fn mount(
    service: FilesystemService,
    mount_point: impl AsRef<Path>,
    library_root: impl Into<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mount_point = mount_point.as_ref().to_path_buf();
    prepare_mount_point(&mount_point)?;
    let _winfsp = winfsp::winfsp_init()?;
    let mut volume_params = VolumeParams::new();
    volume_params
        .filesystem_name("legato")
        .read_only_volume(true)
        .case_sensitive_search(false)
        .case_preserved_names(true)
        .unicode_on_disk(true)
        .sectors_per_allocation_unit(1)
        .sector_size(4096)
        .max_component_length(255)
        .file_info_timeout(1_000)
        .dir_info_timeout(1_000);

    let mut host = FileSystemHost::new(
        volume_params,
        WindowsMountService::new(service, library_root),
    )?;
    host.mount(mount_point)?;
    host.start()?;
    std::future::pending::<()>().await;
    Ok(())
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

#[cfg(target_os = "windows")]
impl FileSystemContext for WindowsMountService {
    type FileContext = WinfspFileContext;

    fn get_security_by_name(
        &self,
        file_name: &U16CStr,
        _security_descriptor: Option<&mut [c_void]>,
        _reparse_point_resolver: impl FnOnce(&U16CStr) -> Option<FileSecurity>,
    ) -> winfsp::Result<FileSecurity> {
        let path = self.winfsp_path(file_name);
        let attributes = self.lookup_attributes(&path)?;
        Ok(FileSecurity {
            reparse: false,
            sz_security_descriptor: 0,
            attributes: file_attributes(&attributes),
        })
    }

    fn open(
        &self,
        file_name: &U16CStr,
        _create_options: u32,
        _granted_access: FILE_ACCESS_RIGHTS,
        file_info: &mut OpenFileInfo,
    ) -> winfsp::Result<Self::FileContext> {
        let path = self.winfsp_path(file_name);
        let attributes = self.lookup_attributes(&path)?;
        let local_handle = if attributes.directory {
            None
        } else {
            Some(self.open_file(&path, file_info.as_mut())?)
        };
        fill_file_info(file_info.as_mut(), &attributes);
        Ok(WinfspFileContext {
            path,
            local_handle,
            attributes,
            directory_buffer: DirBuffer::new(),
        })
    }

    fn close(&self, context: Self::FileContext) {
        if let Some(handle) = context.local_handle {
            let _ = self.release_file(handle);
        }
    }

    fn flush(
        &self,
        context: Option<&Self::FileContext>,
        file_info: &mut FileInfo,
    ) -> winfsp::Result<()> {
        if let Some(context) = context {
            fill_file_info(file_info, &context.attributes);
        }
        Ok(())
    }

    fn get_file_info(
        &self,
        context: &Self::FileContext,
        file_info: &mut FileInfo,
    ) -> winfsp::Result<()> {
        fill_file_info(file_info, &context.attributes);
        Ok(())
    }

    fn read_directory(
        &self,
        context: &Self::FileContext,
        _pattern: Option<&U16CStr>,
        marker: DirMarker<'_>,
        buffer: &mut [u8],
    ) -> winfsp::Result<u32> {
        if !context.attributes.directory {
            return Err(FspError::IO(std::io::ErrorKind::NotADirectory));
        }
        if marker.is_none() {
            self.fill_directory_buffer(context)?;
        }
        Ok(context.directory_buffer.read(marker, buffer))
    }

    fn read(
        &self,
        context: &Self::FileContext,
        buffer: &mut [u8],
        offset: u64,
    ) -> winfsp::Result<u32> {
        let handle = context
            .local_handle
            .ok_or(FspError::IO(std::io::ErrorKind::IsADirectory))?;
        let bytes = self.read_file(handle, offset, buffer.len() as u32)?;
        let bytes_read = bytes.len();
        buffer[..bytes_read].copy_from_slice(&bytes);
        Ok(bytes_read as u32)
    }

    fn get_volume_info(&self, out_volume_info: &mut VolumeInfo) -> winfsp::Result<()> {
        out_volume_info.total_size = 1 << 40;
        out_volume_info.free_size = 0;
        out_volume_info.set_volume_label("Legato");
        Ok(())
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
    platform_error_code(ClientPlatform::Windows, kind)
}

#[cfg(target_os = "windows")]
impl WindowsMountService {
    fn winfsp_path(&self, file_name: &U16CStr) -> String {
        self.canonical_path(Path::new(&file_name.to_string_lossy()))
    }

    fn lookup_attributes(&self, path: &str) -> winfsp::Result<WindowsAttributes> {
        self.runtime.block_on(async {
            self.service
                .lock()
                .await
                .lookup(path)
                .await
                .map(|attributes| WindowsFilesystem::new("").translate_attributes(&attributes))
                .map_err(map_mount_error)
        })
    }

    fn open_file(&self, path: &str, file_info: &mut FileInfo) -> winfsp::Result<u64> {
        self.runtime.block_on(async {
            let handle = self
                .service
                .lock()
                .await
                .open(path)
                .await
                .map_err(map_mount_error)?;
            let attributes = WindowsFilesystem::new("")
                .translate_attributes(&attributes_from_open_handle(&handle));
            fill_file_info(file_info, &attributes);
            Ok(handle.local_handle)
        })
    }

    fn read_file(&self, handle: u64, offset: u64, size: u32) -> winfsp::Result<Vec<u8>> {
        self.runtime.block_on(async {
            self.service
                .lock()
                .await
                .read(handle, offset, size)
                .await
                .map_err(map_mount_error)
        })
    }

    fn release_file(&self, handle: u64) -> winfsp::Result<()> {
        self.runtime.block_on(async {
            self.service
                .lock()
                .await
                .release(handle)
                .await
                .map_err(map_mount_error)
        })
    }

    fn fill_directory_buffer(&self, context: &WinfspFileContext) -> winfsp::Result<()> {
        let entries = self.runtime.block_on(async {
            self.service
                .lock()
                .await
                .read_dir(&context.path)
                .await
                .map_err(map_mount_error)
        })?;
        let lock = context
            .directory_buffer
            .acquire(true, Some(entries.len().saturating_add(2) as u32))?;
        write_dir_entry(&lock, ".", &context.attributes)?;
        write_dir_entry(&lock, "..", &context.attributes)?;
        for entry in entries {
            let mut attributes = self.lookup_attributes(&entry.path)?;
            attributes.file_index = entry.file_id;
            attributes.directory = entry.is_dir;
            write_dir_entry(&lock, &entry.name, &attributes)?;
        }
        Ok(())
    }
}

#[cfg(target_os = "windows")]
fn map_mount_error(error: FilesystemServiceError) -> FspError {
    match error {
        FilesystemServiceError::NotFound(_) => FspError::IO(std::io::ErrorKind::NotFound),
        FilesystemServiceError::UnknownHandle(_) => FspError::IO(std::io::ErrorKind::InvalidInput),
        FilesystemServiceError::InvalidRead { .. } => {
            FspError::IO(std::io::ErrorKind::InvalidInput)
        }
        FilesystemServiceError::Transport(_) | FilesystemServiceError::Store(_) => {
            FspError::NTSTATUS(0xC000_00E9u32 as i32)
        }
    }
}

#[cfg(target_os = "windows")]
fn write_dir_entry(
    lock: &winfsp::filesystem::DirBufferLock<'_>,
    name: &str,
    attributes: &WindowsAttributes,
) -> winfsp::Result<()> {
    let mut dir_info = DirInfo::<255>::new();
    fill_file_info(dir_info.file_info_mut(), attributes);
    dir_info.set_name(name)?;
    lock.write(&mut dir_info)
}

#[cfg(target_os = "windows")]
fn fill_file_info(file_info: &mut FileInfo, attributes: &WindowsAttributes) {
    file_info.file_attributes = file_attributes(attributes);
    file_info.reparse_tag = 0;
    file_info.allocation_size = if attributes.directory {
        0
    } else {
        attributes.allocation_size
    };
    file_info.file_size = if attributes.directory {
        0
    } else {
        attributes.end_of_file
    };
    let timestamp = filetime_from_unix_ns(attributes.mtime_ns);
    file_info.creation_time = timestamp;
    file_info.last_access_time = timestamp;
    file_info.last_write_time = timestamp;
    file_info.change_time = timestamp;
    file_info.index_number = attributes.file_index;
    file_info.hard_links = if attributes.directory { 2 } else { 1 };
    file_info.ea_size = 0;
}

#[cfg(target_os = "windows")]
fn file_attributes(attributes: &WindowsAttributes) -> u32 {
    let kind = if attributes.directory {
        FILE_ATTRIBUTE_DIRECTORY
    } else {
        FILE_ATTRIBUTE_ARCHIVE
    };
    let write_mode = if attributes.read_only {
        FILE_ATTRIBUTE_READONLY
    } else {
        0
    };
    kind | write_mode
}

fn translate_directory_entry(entry: DirectoryEntry) -> WindowsDirectoryEntry {
    WindowsDirectoryEntry {
        name: entry.name,
        file_index: entry.file_id,
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

#[cfg_attr(not(target_os = "windows"), allow(dead_code))]
fn map_virtual_path(library_root: &str, virtual_path: &Path) -> String {
    let mut mapped = PathBuf::from(library_root);
    for component in virtual_path.components() {
        if let std::path::Component::Normal(segment) = component {
            mapped.push(segment);
        }
    }
    mapped.to_string_lossy().into_owned()
}

#[cfg(target_os = "windows")]
fn filetime_from_unix_ns(nanoseconds: u64) -> u64 {
    WINDOWS_UNIX_EPOCH_OFFSET_SECONDS
        .saturating_mul(WINDOWS_TICKS_PER_SECOND)
        .saturating_add(nanoseconds / 100)
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
        MountPointError, MountPointReadiness, WindowsFilesystem, map_virtual_path,
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
    fn adapter_is_constructible_on_non_windows_hosts() {
        let adapter = WindowsFilesystem::new("L:\\Legato");
        assert_eq!(adapter.platform_name(), "windows");
        assert_eq!(adapter.mount_point(), "L:\\Legato");
    }

    #[test]
    fn read_only_semantics_map_to_windows_error_codes() {
        let adapter = WindowsFilesystem::new("L:\\Legato");
        let code = adapter.error_code(
            FilesystemOperation::Rename,
            legato_types::FilesystemError::ReadOnly,
        );

        assert_eq!(code.symbolic_name, "STATUS_MEDIA_WRITE_PROTECTED");
    }

    #[test]
    fn attributes_translate_into_windows_shape() {
        let adapter = WindowsFilesystem::new("L:\\Legato");
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
    async fn windows_adapter_serves_real_lookup_readdir_and_read() {
        let fixture = tempdir().expect("tempdir should be created");
        let library_root = fixture.path().join("library");
        let state_dir = fixture.path().join("state");
        let tls_dir = fixture.path().join("tls");
        fs::create_dir_all(library_root.join("Strings")).expect("library tree should be created");
        let sample_path = library_root.join("Strings").join("long.ncw");
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
            "studio-win",
            &bundle_dir,
        )
        .expect("client bundle should be issued");

        let mut service = FilesystemService::connect(
            local_client_config(address.to_string(), &bundle_dir, "localhost"),
            "studio-win",
            fixture.path().join("client-state").as_path(),
        )
        .await
        .expect("service should connect");
        let adapter = WindowsFilesystem::new("L:\\Legato");

        let attrs = adapter
            .lookup(&mut service, sample_path.to_string_lossy().as_ref())
            .await
            .expect("lookup should succeed");
        assert_ne!(attrs.file_index, 0);

        let entries = adapter
            .read_dir(
                &mut service,
                library_root.join("Strings").to_string_lossy().as_ref(),
            )
            .await
            .expect("readdir should succeed");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "long.ncw");

        let open = adapter
            .open(&mut service, sample_path.to_string_lossy().as_ref())
            .await
            .expect("open should succeed");
        let slice = adapter
            .read(&mut service, open.handle, 0, 5)
            .await
            .expect("read should succeed");
        assert_eq!(slice, b"hello");

        adapter
            .release(&mut service, open.handle)
            .await
            .expect("release should succeed");
        bound.shutdown().await.expect("server should shut down");
    }
}
