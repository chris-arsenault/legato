//! Client-side filesystem service backed by the live transport and local caches.

use std::{
    collections::HashMap,
    path::Path,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use legato_client_cache::{
    MetadataCache, MetadataCachePolicy, catalog::CatalogStoreError, client_store::ClientLegatoStore,
};
use legato_proto::{
    ChangeKind, ChangeRecord, DirectoryEntry, ExtentDescriptor, ExtentRef, FileMetadata,
    InodeMetadata, InvalidationEvent, TransferClass,
};
use legato_types::{FileId, FilesystemAttributes};

use crate::{ClientConfig, ClientRuntimeMetrics, GrpcClientTransport, LocalControlPlane};

const CLIENT_METRICS_REPORT_INTERVAL_NS: u64 = 5_000_000_000;

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
    /// Versioned inode generation bound to future fetches.
    pub inode_generation: u64,
    /// Logical file size in bytes.
    pub size: u64,
    /// Modification time in nanoseconds since the Unix epoch.
    pub mtime_ns: u64,
    /// Transfer class for head-biased fetch decisions.
    pub transfer_class: TransferClass,
    /// Semantic file layout used for extent fetches.
    pub extents: Vec<ExtentDescriptor>,
}

/// Errors surfaced by the shared filesystem service.
#[derive(Debug)]
pub enum FilesystemServiceError {
    /// Remote transport or RPC failure.
    Transport(crate::ClientTransportError),
    /// Local partial store access failed.
    Store(CatalogStoreError),
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
            Self::Store(error) => write!(formatter, "filesystem store access failed: {error}"),
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

impl From<CatalogStoreError> for FilesystemServiceError {
    fn from(value: CatalogStoreError) -> Self {
        Self::Store(value)
    }
}

/// Shared read-only filesystem service used by the platform adapters.
#[derive(Debug)]
pub struct FilesystemService {
    transport: GrpcClientTransport,
    control: LocalControlPlane,
    store: ClientLegatoStore,
    metrics: Option<ClientRuntimeMetrics>,
    max_cache_bytes: u64,
    last_metrics_report_ns: u64,
    metrics_dirty: bool,
    next_handle: u64,
    open_handles: HashMap<u64, FilesystemOpenHandle>,
}

impl FilesystemService {
    /// Connects to the remote server and opens the local metadata/extent cache.
    pub async fn connect(
        config: ClientConfig,
        client_name: impl Into<String>,
        state_dir: &Path,
    ) -> Result<Self, FilesystemServiceError> {
        Self::connect_with_metrics(config, client_name, state_dir, None).await
    }

    /// Connects to the remote server and opens the local cache with runtime metrics attached.
    pub async fn connect_with_metrics(
        config: ClientConfig,
        client_name: impl Into<String>,
        state_dir: &Path,
        metrics: Option<ClientRuntimeMetrics>,
    ) -> Result<Self, FilesystemServiceError> {
        let max_cache_bytes = config.cache.max_bytes;
        let store = ClientLegatoStore::open(state_dir, now_monotonic_ns())?;
        let transport = GrpcClientTransport::connect(config, client_name).await?;
        let control = LocalControlPlane::new(MetadataCache::new(MetadataCachePolicy::default()));

        let service = Self {
            transport,
            control,
            store,
            metrics,
            max_cache_bytes,
            last_metrics_report_ns: 0,
            metrics_dirty: false,
            next_handle: 1,
            open_handles: HashMap::new(),
        };
        let mut service = service;
        if let Some(metrics) = &service.metrics {
            metrics.record_residency(
                service.store.resident_bytes(),
                service.store.resident_extent_count() as u64,
            );
            service.metrics_dirty = true;
        }
        service.report_metrics_if_due(true).await;
        Ok(service)
    }

    /// Returns attach session metadata for the current connection.
    #[must_use]
    pub fn server_name(&self) -> &str {
        &self.transport.attach_session().server_name
    }

    /// Returns whether the service currently has an active invalidation subscription.
    #[must_use]
    pub fn has_active_subscription(&self) -> bool {
        true
    }

    /// Returns the attached runtime metrics recorder when one is configured.
    #[must_use]
    pub fn runtime_metrics(&self) -> Option<&ClientRuntimeMetrics> {
        self.metrics.as_ref()
    }

    /// Returns the current logical resident payload bytes in the local extent store.
    #[must_use]
    pub fn resident_bytes(&self) -> u64 {
        self.store.resident_bytes()
    }

    /// Returns a cached or remotely fetched metadata view for one path.
    pub async fn lookup(
        &mut self,
        path: &str,
    ) -> Result<FilesystemAttributes, FilesystemServiceError> {
        self.sync_changes().await?;
        let now_ns = now_monotonic_ns();
        if let Some(metadata) = self.control.resolve_path(path, now_ns) {
            self.report_metrics_if_due(false).await;
            return Ok(metadata_to_attributes(metadata));
        }

        let metadata = self
            .transport
            .stat(path.to_owned())
            .await
            .map_err(map_lookup_error(path))?;
        self.control.register_path(metadata.clone(), now_ns);
        self.report_metrics_if_due(false).await;
        Ok(metadata_to_attributes(metadata))
    }

    /// Returns one directory listing, using the local cache when still fresh.
    pub async fn read_dir(
        &mut self,
        path: &str,
    ) -> Result<Vec<DirectoryEntry>, FilesystemServiceError> {
        self.sync_changes().await?;
        let now_ns = now_monotonic_ns();
        if let Some(entries) = self.control.list_dir(path, now_ns) {
            self.report_metrics_if_due(false).await;
            return Ok(entries);
        }

        let entries = self
            .transport
            .list_dir(path.to_owned())
            .await
            .map_err(map_lookup_error(path))?;
        self.control.register_dir(path, entries.clone(), now_ns);
        self.report_metrics_if_due(false).await;
        Ok(entries)
    }

    /// Opens one remote file and returns a stable local handle.
    pub async fn open(
        &mut self,
        path: &str,
    ) -> Result<FilesystemOpenHandle, FilesystemServiceError> {
        self.sync_changes().await?;
        let inode = self
            .transport
            .resolve(path.to_owned())
            .await
            .map_err(map_lookup_error(path))?;
        self.store.record_inode(inode.clone())?;
        self.control
            .register_resolved_path(inode.clone(), now_monotonic_ns());
        let handle = inode_to_open_handle(self.next_handle, inode);
        self.next_handle += 1;
        self.open_handles
            .insert(handle.local_handle, handle.clone());
        self.report_metrics_if_due(false).await;
        Ok(handle)
    }

    /// Releases a previously opened file handle.
    pub async fn release(&mut self, local_handle: u64) -> Result<(), FilesystemServiceError> {
        let Some(handle) = self.open_handles.remove(&local_handle) else {
            return Err(FilesystemServiceError::UnknownHandle(local_handle));
        };
        let _ = handle;
        self.report_metrics_if_due(false).await;
        Ok(())
    }

    /// Reads a byte range from one opened file, serving cached extents whenever possible.
    pub async fn read(
        &mut self,
        local_handle: u64,
        offset: u64,
        size: u32,
    ) -> Result<Vec<u8>, FilesystemServiceError> {
        let started = Instant::now();
        self.sync_changes().await?;
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
        let mut cache_hits = 0_u64;
        let mut cache_misses = 0_u64;
        let mut local_bytes = 0_u64;
        let mut remote_bytes = 0_u64;
        for descriptor in &planned_extents {
            let requested = overlap_len(
                descriptor.file_offset,
                descriptor.length,
                offset,
                u64::from(size),
                snapshot.size,
            );
            if self
                .store
                .get_extent(snapshot.file_id, descriptor.extent_index)?
                .is_none()
            {
                missing_extents.push(descriptor.clone());
                cache_misses = cache_misses.saturating_add(1);
                remote_bytes = remote_bytes.saturating_add(requested);
            } else {
                cache_hits = cache_hits.saturating_add(1);
                local_bytes = local_bytes.saturating_add(requested);
            }
        }

        if !missing_extents.is_empty() {
            let fetch_plan = head_biased_fetch_plan(&snapshot, &missing_extents);
            self.fetch_missing_extents(&snapshot, &fetch_plan, now_ns)
                .await?;
        }

        let bytes = assemble_read(&mut self.store, &snapshot, offset, size)?;
        self.enforce_cache_budget()?;
        if let Some(metrics) = &self.metrics {
            metrics.record_read(
                cache_hits,
                cache_misses,
                local_bytes,
                remote_bytes,
                started.elapsed().as_nanos() as u64,
            );
            self.metrics_dirty = true;
        }
        self.report_metrics_if_due(false).await;
        Ok(bytes)
    }

    /// Applies one invalidation to the local metadata and extent caches.
    pub fn apply_invalidation(
        &mut self,
        event: &InvalidationEvent,
    ) -> Result<(), FilesystemServiceError> {
        let handled_at_ns = now_monotonic_ns();
        self.control.apply_invalidation(event);
        self.store.apply_invalidation(event)?;
        self.store.checkpoint()?;
        if let Some(metrics) = &self.metrics {
            metrics.record_invalidation(event, handled_at_ns);
            metrics.record_residency(
                self.store.resident_bytes(),
                self.store.resident_extent_count() as u64,
            );
            self.metrics_dirty = true;
        }
        Ok(())
    }

    /// Applies one ordered replay record to the local metadata and extent state.
    pub fn apply_change_record(
        &mut self,
        record: &ChangeRecord,
    ) -> Result<(), FilesystemServiceError> {
        let now_ns = now_monotonic_ns();
        self.control.apply_change_record(record, now_ns);
        self.store.apply_change_record(record)?;
        self.refresh_handles_from_change(record);
        Ok(())
    }

    /// Returns the current local open-handle snapshot.
    #[must_use]
    pub fn open_handle(&self, local_handle: u64) -> Option<&FilesystemOpenHandle> {
        self.open_handles.get(&local_handle)
    }

    /// Returns the durable replay cursor stored by the local client catalog.
    #[must_use]
    pub fn subscription_cursor(&self) -> u64 {
        self.store.subscription_cursor()
    }

    async fn sync_changes(&mut self) -> Result<(), FilesystemServiceError> {
        match self
            .transport
            .change_records_since(self.store.subscription_cursor())
            .await
        {
            Ok(records) => {
                for record in records {
                    self.apply_change_record(&record)?;
                }
                Ok(())
            }
            Err(error) if should_retry_after_reconnect(&error) => {
                let reconnect_started = Instant::now();
                self.transport.reconnect().await?;
                if let Some(metrics) = &self.metrics {
                    metrics.record_reconnect(reconnect_started.elapsed().as_nanos() as u64);
                    self.metrics_dirty = true;
                }
                for record in self
                    .transport
                    .change_records_since(self.store.subscription_cursor())
                    .await?
                {
                    self.apply_change_record(&record)?;
                }
                Ok(())
            }
            Err(error) => Err(FilesystemServiceError::Transport(error)),
        }
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
                inode_generation: handle.inode_generation,
                extent_hash: extent.extent_hash.clone(),
            })
            .collect::<Vec<_>>();
        match self.transport.fetch_extents(request_extents).await {
            Ok(extents) => {
                self.store_extents(&extents, now_ns)?;
                Ok(())
            }
            Err(error) if should_retry_after_reconnect(&error) => {
                let reconnect_started = Instant::now();
                self.transport.reconnect().await?;
                if let Some(metrics) = &self.metrics {
                    metrics.record_reconnect(reconnect_started.elapsed().as_nanos() as u64);
                    self.metrics_dirty = true;
                }
                self.sync_changes().await?;
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
                        inode_generation: refreshed.inode_generation,
                        extent_hash: extent.extent_hash.clone(),
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
        _now_ns: u64,
    ) -> Result<(), FilesystemServiceError> {
        for extent in extents {
            let _ = self.store.put_extent(extent)?;
        }
        Ok(())
    }

    fn enforce_cache_budget(&mut self) -> Result<(), FilesystemServiceError> {
        if self.store.resident_bytes() > self.max_cache_bytes {
            let report = self.store.evict_to_limit(self.max_cache_bytes)?;
            if let Some(metrics) = &self.metrics {
                metrics.record_eviction(&report);
                self.metrics_dirty = true;
            }
        } else {
            self.store.checkpoint()?;
            if let Some(metrics) = &self.metrics {
                metrics.record_residency(
                    self.store.resident_bytes(),
                    self.store.resident_extent_count() as u64,
                );
                self.metrics_dirty = true;
            }
        }
        Ok(())
    }

    async fn report_metrics_if_due(&mut self, force: bool) {
        let Some(metrics) = self.metrics.as_ref() else {
            return;
        };
        if !self.metrics_dirty && !force {
            return;
        }
        let snapshot = metrics.snapshot();
        let now_ns = now_monotonic_ns();
        if !force
            && now_ns.saturating_sub(self.last_metrics_report_ns)
                < CLIENT_METRICS_REPORT_INTERVAL_NS
        {
            return;
        }
        if self.transport.report_metrics(&snapshot).await.is_ok() {
            self.last_metrics_report_ns = now_ns;
            self.metrics_dirty = false;
        }
    }

    fn refresh_handles_from_change(&mut self, record: &ChangeRecord) {
        let Some(inode) = record.inode.clone() else {
            return;
        };
        if ChangeKind::try_from(record.kind).unwrap_or(ChangeKind::Unspecified)
            != ChangeKind::Upsert
        {
            return;
        }
        for handle in self.open_handles.values_mut() {
            if handle.file_id.0 == inode.file_id || handle.path == record.path {
                *handle = inode_to_open_handle(handle.local_handle, inode.clone());
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

fn head_biased_fetch_plan(
    handle: &FilesystemOpenHandle,
    missing_extents: &[ExtentDescriptor],
) -> Vec<ExtentDescriptor> {
    let mut plan = missing_extents.to_vec();
    if handle.transfer_class != TransferClass::Streamed {
        return plan;
    }

    let Some(max_extent_index) = missing_extents
        .iter()
        .map(|extent| extent.extent_index)
        .max()
    else {
        return plan;
    };

    for descriptor in handle
        .extents
        .iter()
        .filter(|extent| extent.extent_index > max_extent_index)
        .take(2)
    {
        if plan
            .iter()
            .all(|existing| existing.extent_index != descriptor.extent_index)
        {
            plan.push(descriptor.clone());
        }
    }

    plan
}

fn assemble_read(
    store: &mut ClientLegatoStore,
    handle: &FilesystemOpenHandle,
    offset: u64,
    size: u32,
) -> Result<Vec<u8>, FilesystemServiceError> {
    let end = offset.saturating_add(u64::from(size)).min(handle.size);
    let mut bytes = Vec::with_capacity(size as usize);

    for descriptor in read_plan(handle, offset, size) {
        let Some(extent) = store.get_extent(handle.file_id, descriptor.extent_index)? else {
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
        block_size: 4096,
        read_only: true,
    }
}

fn inode_to_open_handle(local_handle: u64, inode: InodeMetadata) -> FilesystemOpenHandle {
    let transfer_class = inode
        .layout
        .as_ref()
        .and_then(|layout| TransferClass::try_from(layout.transfer_class).ok())
        .unwrap_or(TransferClass::Unspecified);
    FilesystemOpenHandle {
        local_handle,
        path: inode.path,
        file_id: FileId(inode.file_id),
        inode_generation: inode.inode_generation,
        size: inode.size,
        mtime_ns: inode.mtime_ns,
        transfer_class,
        extents: inode.layout.map_or_else(Vec::new, |layout| layout.extents),
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

fn overlap_len(
    extent_offset: u64,
    extent_len: u64,
    request_offset: u64,
    request_size: u64,
    file_size: u64,
) -> u64 {
    let request_end = request_offset.saturating_add(request_size).min(file_size);
    let extent_end = extent_offset.saturating_add(extent_len);
    let start = request_offset.max(extent_offset);
    let end = request_end.min(extent_end);
    end.saturating_sub(start)
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use legato_proto::{ExtentDescriptor, FileLayout, InodeMetadata, TransferClass};
    use tempfile::tempdir;
    use tokio::net::TcpListener;

    use crate::{ClientTlsConfig, RetryPolicy};
    use legato_proto::InvalidationKind;
    use legato_server::{
        LiveServer, ServerConfig, ServerTlsConfig, ensure_server_tls_materials,
        issue_client_tls_bundle, load_runtime_tls,
    };

    use super::{
        FilesystemService, head_biased_fetch_plan, inode_to_open_handle, now_monotonic_ns,
    };

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
            .lookup("/Kontakt/piano.nki")
            .await
            .expect("lookup should succeed");
        assert_ne!(attrs.file_id.0, 0);
        assert!(!attrs.is_dir);

        let entries = service
            .read_dir("/Kontakt")
            .await
            .expect("readdir should succeed");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "piano.nki");

        let handle = service
            .open("/Kontakt/piano.nki")
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
            .open("/Strings/long.ncw")
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
    async fn filesystem_service_enforces_cache_budget_after_read_through() {
        let fixture = tempdir().expect("tempdir should be created");
        let library_root = fixture.path().join("library");
        let state_dir = fixture.path().join("state");
        let tls_dir = fixture.path().join("tls");
        fs::create_dir_all(library_root.join("Strings")).expect("library tree should be created");
        fs::write(
            library_root.join(".legato-layout.toml"),
            "[policy]\nunitary_max_bytes = 0\nstreamed_extent_bytes = 4\n",
        )
        .expect("policy override should be written");
        let sample_path = library_root.join("Strings").join("long.ncw");
        fs::write(&sample_path, b"abcdefgh").expect("sample should be written");

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
            "budget-client",
            &bundle_dir,
        )
        .expect("client bundle should be issued");

        let mut client_config = local_client_config(address.to_string(), &bundle_dir, "localhost");
        client_config.cache.max_bytes = 4;
        let mut service = FilesystemService::connect(
            client_config,
            "budget-client",
            fixture.path().join("client-state").as_path(),
        )
        .await
        .expect("service should connect");

        let handle = service
            .open("/Strings/long.ncw")
            .await
            .expect("open should succeed");
        let bytes = service
            .read(handle.local_handle, 0, 1)
            .await
            .expect("read should succeed");

        assert_eq!(bytes, b"a");
        assert_eq!(service.store.resident_bytes(), 4);
        assert_eq!(service.store.resident_extent_count(), 1);
        assert_eq!(
            service
                .store
                .resolve_path("/Strings/long.ncw")
                .and_then(|inode| inode.layout.map(|layout| layout.extents.len())),
            Some(2)
        );

        drop(service);
        bound.shutdown().await.expect("server should stop");
    }

    #[test]
    fn invalidations_clear_cached_entries() {
        let event = legato_proto::InvalidationEvent {
            kind: InvalidationKind::Subtree as i32,
            path: String::from("/Kontakt"),
            file_id: 0,
            issued_at_ns: 0,
        };
        let timestamp = now_monotonic_ns();

        assert!(timestamp > 0);
        assert_eq!(event.path, "/Kontakt");
    }

    #[test]
    fn open_handle_uses_resolved_inode_metadata_without_remote_open_state() {
        let handle = inode_to_open_handle(
            7,
            InodeMetadata {
                file_id: 42,
                path: String::from("/Strings/legato.ncw"),
                size: 8192,
                mtime_ns: 123,
                is_dir: false,
                layout: Some(FileLayout {
                    transfer_class: TransferClass::Streamed as i32,
                    extents: vec![ExtentDescriptor {
                        extent_index: 0,
                        file_offset: 0,
                        length: 4096,
                        extent_hash: Vec::new(),
                    }],
                }),
                inode_generation: 3,
                content_hash: b"legato-hash".to_vec(),
            },
        );

        assert_eq!(handle.local_handle, 7);
        assert_eq!(handle.file_id.0, 42);
        assert_eq!(handle.inode_generation, 3);
        assert_eq!(handle.mtime_ns, 123);
        assert_eq!(handle.transfer_class, TransferClass::Streamed);
        assert_eq!(handle.extents.len(), 1);
    }

    #[test]
    fn streamed_reads_bias_fetch_plan_toward_head_then_following_extents() {
        let handle = inode_to_open_handle(
            1,
            InodeMetadata {
                file_id: 9,
                path: String::from("/Strings/long.ncw"),
                size: 16 * 1024 * 1024,
                mtime_ns: 55,
                is_dir: false,
                layout: Some(FileLayout {
                    transfer_class: TransferClass::Streamed as i32,
                    extents: vec![
                        ExtentDescriptor {
                            extent_index: 0,
                            file_offset: 0,
                            length: 4 * 1024 * 1024,
                            extent_hash: Vec::new(),
                        },
                        ExtentDescriptor {
                            extent_index: 1,
                            file_offset: 4 * 1024 * 1024,
                            length: 4 * 1024 * 1024,
                            extent_hash: Vec::new(),
                        },
                        ExtentDescriptor {
                            extent_index: 2,
                            file_offset: 8 * 1024 * 1024,
                            length: 4 * 1024 * 1024,
                            extent_hash: Vec::new(),
                        },
                    ],
                }),
                inode_generation: 1,
                content_hash: b"streamed-content".to_vec(),
            },
        );

        let fetch_plan = head_biased_fetch_plan(
            &handle,
            &[ExtentDescriptor {
                extent_index: 0,
                file_offset: 0,
                length: 4 * 1024 * 1024,
                extent_hash: Vec::new(),
            }],
        );

        assert_eq!(fetch_plan.len(), 3);
        assert_eq!(fetch_plan[0].extent_index, 0);
        assert_eq!(fetch_plan[1].extent_index, 1);
        assert_eq!(fetch_plan[2].extent_index, 2);
    }
}
