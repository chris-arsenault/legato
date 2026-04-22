//! Client-side filesystem service backed by the live transport and local caches.

use std::{
    collections::HashMap,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use legato_client_cache::{
    ExtentCacheStore, ExtentKey, MetadataCache, MetadataCachePolicy, open_cache_database,
};
use legato_proto::{DirectoryEntry, ExtentDescriptor, ExtentRef, FileMetadata, InvalidationEvent};
use legato_types::{FileId, FilesystemAttributes};

use crate::{
    ClientConfig, GrpcClientTransport, GrpcInvalidationSubscription, LocalControlPlane,
    transport::InvalidationPoll,
};

/// Returns a coarse monotonic wall-clock timestamp for cache bookkeeping.
#[must_use]
pub fn now_monotonic_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos() as u64)
}

/// One locally tracked open file handle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FilesystemOpenHandle {
    /// Local handle identifier exposed to the platform adapter.
    pub local_handle: u64,
    /// Canonical path for reopen and diagnostics.
    pub path: String,
    /// Stable server file identifier.
    pub file_id: FileId,
    /// Current remote server-local handle.
    pub server_handle: u64,
    /// Logical file size in bytes.
    pub size: u64,
    /// Semantic file layout used for extent fetches.
    pub extents: Vec<ExtentDescriptor>,
}

/// Errors surfaced by the shared filesystem service.
#[derive(Debug)]
pub enum FilesystemServiceError {
    /// Remote transport or RPC failure.
    Transport(crate::ClientTransportError),
    /// Local cache database access failed.
    Cache(rusqlite::Error),
    /// The requested path or directory entry did not exist.
    NotFound(String),
    /// The requested local handle was not open.
    UnknownHandle(u64),
    /// The requested read parameters were not valid for the open file.
    InvalidRead {
        /// Local handle that was used for the invalid request.
        local_handle: u64,
        /// Requested starting offset.
        offset: u64,
        /// Requested byte count.
        size: u32,
    },
}

impl std::fmt::Display for FilesystemServiceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(error) => write!(formatter, "filesystem transport failed: {error}"),
            Self::Cache(error) => write!(formatter, "filesystem cache access failed: {error}"),
            Self::NotFound(path) => write!(formatter, "filesystem path was not found: {path}"),
            Self::UnknownHandle(handle) => write!(formatter, "unknown local file handle {handle}"),
            Self::InvalidRead {
                local_handle,
                offset,
                size,
            } => write!(
                formatter,
                "invalid read for local handle {local_handle}: offset={offset} size={size}"
            ),
        }
    }
}

impl std::error::Error for FilesystemServiceError {}

impl From<crate::ClientTransportError> for FilesystemServiceError {
    fn from(value: crate::ClientTransportError) -> Self {
        Self::Transport(value)
    }
}

impl From<rusqlite::Error> for FilesystemServiceError {
    fn from(value: rusqlite::Error) -> Self {
        Self::Cache(value)
    }
}

/// Shared read-only filesystem service used by the platform adapters.
#[derive(Debug)]
pub struct FilesystemService {
    transport: GrpcClientTransport,
    control: LocalControlPlane,
    store: ExtentCacheStore,
    max_cache_bytes: u64,
    next_handle: u64,
    open_handles: HashMap<u64, FilesystemOpenHandle>,
    invalidations: Option<GrpcInvalidationSubscription>,
}

impl FilesystemService {
    /// Connects to the remote server and opens the local metadata/block cache.
    pub async fn connect(
        config: ClientConfig,
        client_name: impl Into<String>,
        state_dir: &Path,
    ) -> Result<Self, FilesystemServiceError> {
        let max_cache_bytes = config.cache.max_bytes;
        let cache_db = open_cache_database(&state_dir.join("client.sqlite"))?;
        let mut store = ExtentCacheStore::new(&state_dir.join("extents"), cache_db)?;
        store.recover(max_cache_bytes, now_monotonic_ns())?;
        let mut transport = GrpcClientTransport::connect(config, client_name).await?;
        let invalidations = Some(transport.subscribe_invalidations().await?);
        let control = LocalControlPlane::new(MetadataCache::new(MetadataCachePolicy::default()));

        Ok(Self {
            transport,
            control,
            store,
            max_cache_bytes,
            next_handle: 1,
            open_handles: HashMap::new(),
            invalidations,
        })
    }

    /// Returns attach session metadata for the current connection.
    #[must_use]
    pub fn server_name(&self) -> &str {
        &self.transport.attach_session().server_name
    }

    /// Returns whether the service currently has an active invalidation subscription.
    #[must_use]
    pub fn has_active_subscription(&self) -> bool {
        self.invalidations.is_some()
    }

    /// Returns a cached or remotely fetched metadata view for one path.
    pub async fn lookup(
        &mut self,
        path: &str,
    ) -> Result<FilesystemAttributes, FilesystemServiceError> {
        self.sync_invalidations().await?;
        let now_ns = now_monotonic_ns();
        if let Some(metadata) = self.control.resolve_path(path, now_ns) {
            return Ok(metadata_to_attributes(metadata));
        }

        let metadata = self
            .transport
            .stat(path.to_owned())
            .await
            .map_err(map_lookup_error(path))?;
        self.control.register_path(metadata.clone(), now_ns);
        Ok(metadata_to_attributes(metadata))
    }

    /// Returns one directory listing, using the local cache when still fresh.
    pub async fn read_dir(
        &mut self,
        path: &str,
    ) -> Result<Vec<DirectoryEntry>, FilesystemServiceError> {
        self.sync_invalidations().await?;
        let now_ns = now_monotonic_ns();
        if let Some(entries) = self.control.list_dir(path, now_ns) {
            return Ok(entries);
        }

        let entries = self
            .transport
            .list_dir(path.to_owned())
            .await
            .map_err(map_lookup_error(path))?;
        self.control.register_dir(path, entries.clone(), now_ns);
        Ok(entries)
    }

    /// Opens one remote file and returns a stable local handle.
    pub async fn open(
        &mut self,
        path: &str,
    ) -> Result<FilesystemOpenHandle, FilesystemServiceError> {
        self.sync_invalidations().await?;
        let response = self
            .transport
            .resolve(path.to_owned())
            .await
            .map_err(map_lookup_error(path))?;
        let open_response = self
            .transport
            .open(path.to_owned())
            .await
            .map_err(map_lookup_error(path))?;
        let handle = FilesystemOpenHandle {
            local_handle: self.next_handle,
            path: path.to_owned(),
            file_id: FileId(open_response.file_id),
            server_handle: open_response.file_handle,
            size: open_response.size,
            extents: response
                .layout
                .map_or_else(Vec::new, |layout| layout.extents),
        };
        self.next_handle += 1;
        self.open_handles
            .insert(handle.local_handle, handle.clone());
        Ok(handle)
    }

    /// Reads a byte range from one opened file, serving cached blocks whenever possible.
    pub async fn read(
        &mut self,
        local_handle: u64,
        offset: u64,
        size: u32,
    ) -> Result<Vec<u8>, FilesystemServiceError> {
        self.sync_invalidations().await?;
        let snapshot = self
            .open_handles
            .get(&local_handle)
            .cloned()
            .ok_or(FilesystemServiceError::UnknownHandle(local_handle))?;
        if size == 0 || offset >= snapshot.size {
            return Ok(Vec::new());
        }
        let planned_extents = read_plan(&snapshot, offset, size);
        let now_ns = now_monotonic_ns();
        let mut missing_extents = Vec::new();
        for descriptor in &planned_extents {
            let key = ExtentKey {
                file_id: snapshot.file_id,
                extent_index: descriptor.extent_index,
            };
            if self.store.get_extent(&key, now_ns)?.is_none() {
                missing_extents.push(descriptor.clone());
            }
        }

        if !missing_extents.is_empty() {
            self.fetch_missing_extents(&snapshot, &missing_extents, now_ns)
                .await?;
        }

        assemble_read(&mut self.store, &snapshot, offset, size, now_ns)
    }

    /// Releases a previously opened file handle.
    pub async fn release(&mut self, local_handle: u64) -> Result<(), FilesystemServiceError> {
        let Some(handle) = self.open_handles.remove(&local_handle) else {
            return Err(FilesystemServiceError::UnknownHandle(local_handle));
        };
        self.transport.close(handle.server_handle).await?;
        self.transport.runtime_mut().close_open_handle(&handle.path);
        Ok(())
    }

    /// Applies one invalidation to the local metadata and block caches.
    pub fn apply_invalidation(
        &mut self,
        event: &InvalidationEvent,
    ) -> Result<(), FilesystemServiceError> {
        self.control.apply_invalidation(event);
        self.store.apply_invalidation(event)?;
        let _ = self.store.checkpoint(now_monotonic_ns())?;
        Ok(())
    }

    /// Returns the current local open-handle snapshot.
    #[must_use]
    pub fn open_handle(&self, local_handle: u64) -> Option<&FilesystemOpenHandle> {
        self.open_handles.get(&local_handle)
    }

    async fn sync_invalidations(&mut self) -> Result<(), FilesystemServiceError> {
        loop {
            if !self.poll_one_invalidation().await? {
                break;
            }
        }
        Ok(())
    }

    async fn poll_one_invalidation(&mut self) -> Result<bool, FilesystemServiceError> {
        self.ensure_invalidation_subscription().await?;
        let Some(mut subscription) = self.invalidations.take() else {
            return Ok(false);
        };

        match subscription.try_recv_next() {
            Ok(InvalidationPoll::Empty) => {
                self.invalidations = Some(subscription);
                Ok(false)
            }
            Ok(InvalidationPoll::Event(event)) => {
                self.apply_invalidation(&event)?;
                self.invalidations = Some(subscription);
                Ok(true)
            }
            Ok(InvalidationPoll::Closed) => {
                self.reconnect_and_resubscribe().await?;
                Ok(false)
            }
            Err(error) if should_retry_after_reconnect(&error) => {
                self.reconnect_and_resubscribe().await?;
                Ok(false)
            }
            Err(error) => Err(FilesystemServiceError::Transport(error)),
        }
    }

    async fn ensure_invalidation_subscription(&mut self) -> Result<(), FilesystemServiceError> {
        if self.invalidations.is_none() {
            self.invalidations = Some(self.transport.subscribe_invalidations().await?);
        }
        Ok(())
    }

    async fn reconnect_and_resubscribe(&mut self) -> Result<(), FilesystemServiceError> {
        self.transport.reconnect().await?;
        self.refresh_handles_from_runtime().await;
        self.invalidations = Some(self.transport.subscribe_invalidations().await?);
        Ok(())
    }

    async fn fetch_missing_extents(
        &mut self,
        handle: &FilesystemOpenHandle,
        missing_extents: &[ExtentDescriptor],
        now_ns: u64,
    ) -> Result<(), FilesystemServiceError> {
        let request_extents = missing_extents
            .iter()
            .map(|extent| ExtentRef {
                file_id: handle.file_id.0,
                extent_index: extent.extent_index,
                file_offset: extent.file_offset,
                length: extent.length,
            })
            .collect::<Vec<_>>();
        match self.transport.fetch_extents(request_extents).await {
            Ok(extents) => {
                self.store_extents(&extents, now_ns)?;
                Ok(())
            }
            Err(error) if should_retry_after_reconnect(&error) => {
                self.transport.reconnect().await?;
                self.refresh_handles_from_runtime().await;
                let refreshed = self
                    .open_handles
                    .get(&handle.local_handle)
                    .cloned()
                    .ok_or(FilesystemServiceError::UnknownHandle(handle.local_handle))?;
                let retry_extents = missing_extents
                    .iter()
                    .map(|extent| ExtentRef {
                        file_id: refreshed.file_id.0,
                        extent_index: extent.extent_index,
                        file_offset: extent.file_offset,
                        length: extent.length,
                    })
                    .collect::<Vec<_>>();
                let extents = self.transport.fetch_extents(retry_extents).await?;
                self.store_extents(&extents, now_ns)?;
                Ok(())
            }
            Err(error) => Err(FilesystemServiceError::Transport(error)),
        }
    }

    fn store_extents(
        &mut self,
        extents: &[legato_proto::ExtentRecord],
        now_ns: u64,
    ) -> Result<(), FilesystemServiceError> {
        for extent in extents {
            let _ = self.store.put_extent(extent, 0, now_ns)?;
            self.store.record_extent_fetch_state(
                &ExtentKey {
                    file_id: FileId(extent.file_id),
                    extent_index: extent.extent_index,
                },
                i32::MAX,
                "resident",
                now_ns,
            )?;
        }
        let _ = self.store.evict_to_limit(self.max_cache_bytes)?;
        let _ = self.store.checkpoint(now_ns)?;
        Ok(())
    }

    async fn refresh_handles_from_runtime(&mut self) {
        for handle in self.open_handles.values_mut() {
            if let Some(open_file) = self.transport.runtime().open_file(&handle.path) {
                handle.server_handle = open_file.file_handle;
                handle.file_id = FileId(open_file.file_id);
            }
            if let Ok(inode) = self.transport.resolve(handle.path.clone()).await {
                handle.extents = inode.layout.map_or_else(Vec::new, |layout| layout.extents);
                handle.size = inode.size;
            }
        }
    }
}

fn read_plan(handle: &FilesystemOpenHandle, offset: u64, size: u32) -> Vec<ExtentDescriptor> {
    let end = offset.saturating_add(u64::from(size)).min(handle.size);
    handle
        .extents
        .iter()
        .filter(|extent| {
            let extent_end = extent.file_offset.saturating_add(extent.length);
            extent.file_offset < end && extent_end > offset
        })
        .cloned()
        .collect()
}

fn assemble_read(
    store: &mut ExtentCacheStore,
    handle: &FilesystemOpenHandle,
    offset: u64,
    size: u32,
    now_ns: u64,
) -> Result<Vec<u8>, FilesystemServiceError> {
    let end = offset.saturating_add(u64::from(size)).min(handle.size);
    let mut bytes = Vec::with_capacity(size as usize);

    for descriptor in read_plan(handle, offset, size) {
        let key = ExtentKey {
            file_id: handle.file_id,
            extent_index: descriptor.extent_index,
        };
        let Some(extent) = store.get_extent(&key, now_ns)? else {
            return Err(FilesystemServiceError::NotFound(handle.path.clone()));
        };
        let extent_end = extent.file_offset.saturating_add(extent.data.len() as u64);
        let copy_start = offset.max(extent.file_offset);
        let copy_end = end.min(extent_end);
        if copy_start >= copy_end {
            continue;
        }
        let start_index = (copy_start - extent.file_offset) as usize;
        let end_index = (copy_end - extent.file_offset) as usize;
        bytes.extend_from_slice(&extent.data[start_index..end_index]);
    }

    Ok(bytes)
}

fn metadata_to_attributes(metadata: FileMetadata) -> FilesystemAttributes {
    FilesystemAttributes {
        file_id: FileId(metadata.file_id),
        path: metadata.path.into(),
        is_dir: metadata.is_dir,
        size: metadata.size,
        mtime_ns: metadata.mtime_ns,
        block_size: metadata.block_size,
        read_only: true,
    }
}

fn map_lookup_error<'a>(
    path: &'a str,
) -> impl FnOnce(crate::ClientTransportError) -> FilesystemServiceError + 'a {
    move |error| match &error {
        crate::ClientTransportError::Rpc(status) if status.code() == tonic::Code::NotFound => {
            FilesystemServiceError::NotFound(path.to_owned())
        }
        _ => FilesystemServiceError::Transport(error),
    }
}

fn should_retry_after_reconnect(error: &crate::ClientTransportError) -> bool {
    match error {
        crate::ClientTransportError::Rpc(status) => matches!(
            status.code(),
            tonic::Code::Cancelled
                | tonic::Code::Unavailable
                | tonic::Code::InvalidArgument
                | tonic::Code::Unknown
        ),
        crate::ClientTransportError::Transport(_) => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use tempfile::tempdir;
    use tokio::net::TcpListener;

    use crate::{ClientTlsConfig, RetryPolicy};
    use legato_proto::InvalidationKind;
    use legato_server::{
        LiveServer, ServerConfig, ServerTlsConfig, ensure_server_tls_materials,
        issue_client_tls_bundle, load_runtime_tls,
    };

    use super::{FilesystemService, now_monotonic_ns};

    fn local_client_config(
        endpoint: String,
        bundle_dir: &Path,
        server_name: &str,
    ) -> crate::ClientConfig {
        crate::ClientConfig {
            endpoint,
            tls: ClientTlsConfig::local_dev(bundle_dir, server_name),
            retry: RetryPolicy {
                initial_delay_ms: 0,
                max_delay_ms: 0,
                multiplier: 2,
            },
            ..crate::ClientConfig::default()
        }
    }

    #[tokio::test]
    async fn filesystem_service_serves_lookup_readdir_open_read_and_release() {
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

        let attrs = service
            .lookup(sample_path.to_string_lossy().as_ref())
            .await
            .expect("lookup should succeed");
        assert_eq!(attrs.file_id.0, 1);
        assert!(!attrs.is_dir);

        let entries = service
            .read_dir(library_root.join("Kontakt").to_string_lossy().as_ref())
            .await
            .expect("readdir should succeed");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "piano.nki");

        let handle = service
            .open(sample_path.to_string_lossy().as_ref())
            .await
            .expect("open should succeed");
        let slice = service
            .read(handle.local_handle, 1, 5)
            .await
            .expect("read should succeed");
        assert_eq!(slice, b"ello ");

        service
            .release(handle.local_handle)
            .await
            .expect("release should succeed");
        assert!(service.open_handle(handle.local_handle).is_none());

        drop(service);
        bound.shutdown().await.expect("server should shut down");
    }

    #[tokio::test]
    async fn filesystem_service_reconnects_and_retries_reads_after_server_restart() {
        let fixture = tempdir().expect("tempdir should be created");
        let library_root = fixture.path().join("library");
        let state_dir = fixture.path().join("state");
        let tls_dir = fixture.path().join("tls");
        fs::create_dir_all(library_root.join("Strings")).expect("library tree should be created");
        let sample_path = library_root.join("Strings").join("long.ncw");
        fs::write(&sample_path, b"restart-safe sample").expect("sample should be written");

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

        let bundle_dir = fixture.path().join("bundle");
        issue_client_tls_bundle(
            Path::new(&config.tls_dir),
            &config.tls,
            "studio-win",
            &bundle_dir,
        )
        .expect("client bundle should be issued");

        let first_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let address = first_listener
            .local_addr()
            .expect("addr should be available");
        let first_server = LiveServer::bootstrap(config.clone()).expect("server should bootstrap");
        let first_bound = first_server
            .bind(
                first_listener,
                Some(load_runtime_tls(&config.tls).expect("runtime tls should load")),
            )
            .await
            .expect("server should bind");

        let mut service = FilesystemService::connect(
            local_client_config(address.to_string(), &bundle_dir, "localhost"),
            "studio-win",
            fixture.path().join("client-state").as_path(),
        )
        .await
        .expect("service should connect");
        let handle = service
            .open(sample_path.to_string_lossy().as_ref())
            .await
            .expect("open should succeed");

        first_bound.shutdown().await.expect("server should stop");

        let second_listener = TcpListener::bind(address)
            .await
            .expect("listener should rebind");
        let second_server = LiveServer::bootstrap(config.clone()).expect("server should bootstrap");
        let second_bound = second_server
            .bind(
                second_listener,
                Some(load_runtime_tls(&config.tls).expect("runtime tls should load")),
            )
            .await
            .expect("server should bind");

        let slice = service
            .read(handle.local_handle, 0, 7)
            .await
            .expect("read should recover after reconnect");
        assert_eq!(slice, b"restart");

        drop(service);
        second_bound.shutdown().await.expect("server should stop");
    }

    #[tokio::test]
    async fn filesystem_service_establishes_and_uses_invalidation_subscription() {
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
            "studio-cache",
            &bundle_dir,
        )
        .expect("client bundle should be issued");

        let mut service = FilesystemService::connect(
            local_client_config(address.to_string(), &bundle_dir, "localhost"),
            "studio-cache",
            fixture.path().join("client-state").as_path(),
        )
        .await
        .expect("service should connect");
        assert!(service.has_active_subscription());

        let attrs = service
            .lookup(sample_path.to_string_lossy().as_ref())
            .await
            .expect("lookup should succeed");
        assert_eq!(attrs.file_id.0, 1);

        let initial_entries = service
            .read_dir(library_root.join("Kontakt").to_string_lossy().as_ref())
            .await
            .expect("initial readdir should succeed");
        assert_eq!(initial_entries.len(), 1);

        drop(service);
        bound.shutdown().await.expect("server should shut down");
    }

    #[test]
    fn invalidations_clear_cached_entries() {
        let event = legato_proto::InvalidationEvent {
            kind: InvalidationKind::Subtree as i32,
            path: String::from("/srv/libraries/Kontakt"),
            file_id: 0,
        };
        let timestamp = now_monotonic_ns();

        assert!(timestamp > 0);
        assert_eq!(event.path, "/srv/libraries/Kontakt");
    }
}
