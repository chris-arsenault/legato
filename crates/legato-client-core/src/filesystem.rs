//! Client-side filesystem service backed by the live transport and local caches.

use std::{
    collections::{BTreeSet, HashMap},
    path::Path,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
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
const CHANGE_SYNC_INTERVAL_NS: u64 = 1_000_000_000;
const SLOW_OPERATION_WARN_AFTER: Duration = Duration::from_millis(250);
const ROOT_FILE_ID: u64 = 1;

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
    /// The requested operation needs the server, but no remote transport is currently usable.
    Unavailable(String),
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
            Self::Unavailable(reason) => {
                write!(formatter, "filesystem remote is unavailable: {reason}")
            }
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
    config: ClientConfig,
    client_name: String,
    server_name: String,
    transport: Option<GrpcClientTransport>,
    control: LocalControlPlane,
    store: ClientLegatoStore,
    metrics: Option<ClientRuntimeMetrics>,
    max_cache_bytes: u64,
    transport_attempts: u32,
    next_transport_attempt_ns: u64,
    last_change_sync_ns: u64,
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
        let client_name = client_name.into();
        let max_cache_bytes = config.cache.max_bytes;
        let server_name = config.tls.server_name.clone();
        let store = ClientLegatoStore::open(state_dir, now_monotonic_ns())?;
        let control = LocalControlPlane::new(MetadataCache::new(MetadataCachePolicy::default()));

        let service = Self {
            config,
            client_name,
            server_name,
            transport: None,
            control,
            store,
            metrics,
            max_cache_bytes,
            transport_attempts: 0,
            next_transport_attempt_ns: 0,
            last_change_sync_ns: 0,
            last_metrics_report_ns: 0,
            metrics_dirty: false,
            next_handle: 1,
            open_handles: HashMap::new(),
        };
        let mut service = service;
        if let Err(error) = service.connect_transport_now().await {
            tracing::warn!(
                error = %error,
                state_dir = %state_dir.display(),
                "client remote transport unavailable; mounting from local store"
            );
        }
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
        &self.server_name
    }

    /// Returns whether the service currently has an active invalidation subscription.
    #[must_use]
    pub fn has_active_subscription(&self) -> bool {
        self.transport.is_some()
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
        let started = Instant::now();
        let now_ns = now_monotonic_ns();
        if let Some(metadata) = self.lookup_local_metadata(path, now_ns) {
            self.report_metrics_if_due(false).await;
            log_slow_operation("lookup", path, "cache", started.elapsed());
            return Ok(metadata_to_attributes(metadata));
        }
        if path == "/" {
            let metadata = synthetic_root_metadata();
            self.control.register_path(metadata.clone(), now_ns);
            self.report_metrics_if_due(false).await;
            log_slow_operation("lookup", path, "synthetic", started.elapsed());
            return Ok(metadata_to_attributes(metadata));
        }

        self.sync_changes_if_due(false).await?;
        if let Some(metadata) = self.lookup_local_metadata(path, now_ns) {
            self.report_metrics_if_due(false).await;
            log_slow_operation("lookup", path, "store", started.elapsed());
            return Ok(metadata_to_attributes(metadata));
        }

        let metadata = self
            .remote_stat(path)
            .await
            .map_err(map_lookup_error(path))?;
        self.store.record_metadata(metadata.clone())?;
        self.control.register_path(metadata.clone(), now_ns);
        self.report_metrics_if_due(false).await;
        log_slow_operation("lookup", path, "remote", started.elapsed());
        Ok(metadata_to_attributes(metadata))
    }

    /// Returns one directory listing, using the local cache when still fresh.
    pub async fn read_dir(
        &mut self,
        path: &str,
    ) -> Result<Vec<DirectoryEntry>, FilesystemServiceError> {
        let started = Instant::now();
        let now_ns = now_monotonic_ns();
        if let Some(entries) = self.lookup_local_directory(path, now_ns) {
            self.report_metrics_if_due(false).await;
            log_slow_operation("read_dir", path, "cache", started.elapsed());
            return Ok(entries);
        }

        self.sync_changes_if_due(false).await?;
        if let Some(entries) = self.lookup_local_directory(path, now_ns) {
            self.report_metrics_if_due(false).await;
            log_slow_operation("read_dir", path, "store", started.elapsed());
            return Ok(entries);
        }

        let directory_metadata = match self.directory_metadata(path, now_ns).await {
            Ok(metadata) => metadata,
            Err(error) if path == "/" && is_remote_unavailable(&error) => {
                self.report_metrics_if_due(false).await;
                log_slow_operation("read_dir", path, "offline-empty", started.elapsed());
                return Ok(Vec::new());
            }
            Err(error) => return Err(map_lookup_error(path)(error)),
        };
        if !directory_metadata.is_dir {
            return Err(FilesystemServiceError::NotFound(path.to_owned()));
        }
        let entries = match self.remote_list_dir(path).await {
            Ok(entries) => entries,
            Err(error) if path == "/" && is_remote_unavailable(&error) => {
                self.report_metrics_if_due(false).await;
                log_slow_operation("read_dir", path, "offline-empty", started.elapsed());
                return Ok(Vec::new());
            }
            Err(error) => return Err(map_lookup_error(path)(error)),
        };
        let entries = sanitize_directory_entries(path, entries);
        self.store.record_metadata(directory_metadata.clone())?;
        self.store
            .record_directory(path, FileId(directory_metadata.file_id), entries.clone())?;
        self.control.register_path(directory_metadata, now_ns);
        self.control.register_dir(path, entries.clone(), now_ns);
        self.report_metrics_if_due(false).await;
        log_slow_operation("read_dir", path, "remote", started.elapsed());
        Ok(entries)
    }

    /// Opens one remote file and returns a stable local handle.
    pub async fn open(
        &mut self,
        path: &str,
    ) -> Result<FilesystemOpenHandle, FilesystemServiceError> {
        let started = Instant::now();
        let now_ns = now_monotonic_ns();
        if let Some(inode) = self.store.resolve_path(path).filter(|inode| !inode.is_dir) {
            self.control.register_resolved_path(inode.clone(), now_ns);
            let handle = inode_to_open_handle(self.next_handle, inode);
            self.next_handle += 1;
            self.open_handles
                .insert(handle.local_handle, handle.clone());
            self.report_metrics_if_due(false).await;
            log_slow_operation("open", path, "store", started.elapsed());
            return Ok(handle);
        }

        self.sync_changes_if_due(false).await?;
        if let Some(inode) = self.store.resolve_path(path).filter(|inode| !inode.is_dir) {
            self.control.register_resolved_path(inode.clone(), now_ns);
            let handle = inode_to_open_handle(self.next_handle, inode);
            self.next_handle += 1;
            self.open_handles
                .insert(handle.local_handle, handle.clone());
            self.report_metrics_if_due(false).await;
            log_slow_operation("open", path, "store", started.elapsed());
            return Ok(handle);
        }

        let inode = self
            .remote_resolve(path)
            .await
            .map_err(map_lookup_error(path))?;
        if inode.is_dir {
            return Err(FilesystemServiceError::NotFound(path.to_owned()));
        }
        self.store.record_inode(inode.clone())?;
        self.control.register_resolved_path(inode.clone(), now_ns);
        let handle = inode_to_open_handle(self.next_handle, inode);
        self.next_handle += 1;
        self.open_handles
            .insert(handle.local_handle, handle.clone());
        self.report_metrics_if_due(false).await;
        log_slow_operation("open", path, "remote", started.elapsed());
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
        self.sync_changes_if_due(false).await?;
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

        let bytes = assemble_read(&mut self.store, &snapshot, offset, size, now_ns)?;
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
        log_slow_read(
            snapshot.path.as_str(),
            offset,
            size,
            cache_hits,
            cache_misses,
            local_bytes,
            remote_bytes,
            started.elapsed(),
        );
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

    /// Opportunistically refreshes local catalog state from the server.
    pub async fn refresh_remote_state(&mut self) -> Result<(), FilesystemServiceError> {
        self.sync_changes_if_due(true).await
    }

    fn lookup_local_metadata(&mut self, path: &str, now_ns: u64) -> Option<FileMetadata> {
        if let Some(metadata) = self.control.resolve_path(path, now_ns) {
            return Some(metadata);
        }
        let inode = self.store.resolve_path(path)?;
        self.control.register_resolved_path(inode.clone(), now_ns);
        Some(inode_to_file_metadata(&inode))
    }

    fn lookup_local_directory(&mut self, path: &str, now_ns: u64) -> Option<Vec<DirectoryEntry>> {
        if let Some(entries) = self.control.list_dir(path, now_ns) {
            return Some(entries);
        }
        let entries = self.store.list_directory(path)?;
        self.control.register_dir(path, entries.clone(), now_ns);
        Some(entries)
    }

    async fn directory_metadata(
        &mut self,
        path: &str,
        now_ns: u64,
    ) -> Result<FileMetadata, FilesystemServiceError> {
        if let Some(metadata) = self.lookup_local_metadata(path, now_ns) {
            return Ok(metadata);
        }
        if path == "/" {
            return Ok(synthetic_root_metadata());
        }
        self.remote_stat(path).await
    }

    async fn remote_stat(&mut self, path: &str) -> Result<FileMetadata, FilesystemServiceError> {
        self.ensure_transport_available().await?;
        let result = self
            .transport
            .as_mut()
            .expect("transport should be available")
            .stat(path.to_owned())
            .await;
        match result {
            Ok(metadata) => Ok(metadata),
            Err(error) if should_retry_after_reconnect(&error) => {
                self.reconnect_transport_now()
                    .await
                    .map_err(FilesystemServiceError::Transport)?;
                self.transport
                    .as_mut()
                    .expect("transport should be available")
                    .stat(path.to_owned())
                    .await
                    .map_err(|error| self.handle_remote_error(error))
            }
            Err(error) => Err(self.handle_remote_error(error)),
        }
    }

    async fn remote_list_dir(
        &mut self,
        path: &str,
    ) -> Result<Vec<DirectoryEntry>, FilesystemServiceError> {
        self.ensure_transport_available().await?;
        let result = self
            .transport
            .as_mut()
            .expect("transport should be available")
            .list_dir(path.to_owned())
            .await;
        match result {
            Ok(entries) => Ok(entries),
            Err(error) if should_retry_after_reconnect(&error) => {
                self.reconnect_transport_now()
                    .await
                    .map_err(FilesystemServiceError::Transport)?;
                self.transport
                    .as_mut()
                    .expect("transport should be available")
                    .list_dir(path.to_owned())
                    .await
                    .map_err(|error| self.handle_remote_error(error))
            }
            Err(error) => Err(self.handle_remote_error(error)),
        }
    }

    async fn remote_resolve(
        &mut self,
        path: &str,
    ) -> Result<InodeMetadata, FilesystemServiceError> {
        self.ensure_transport_available().await?;
        let result = self
            .transport
            .as_mut()
            .expect("transport should be available")
            .resolve(path.to_owned())
            .await;
        match result {
            Ok(inode) => Ok(inode),
            Err(error) if should_retry_after_reconnect(&error) => {
                self.reconnect_transport_now()
                    .await
                    .map_err(FilesystemServiceError::Transport)?;
                self.transport
                    .as_mut()
                    .expect("transport should be available")
                    .resolve(path.to_owned())
                    .await
                    .map_err(|error| self.handle_remote_error(error))
            }
            Err(error) => Err(self.handle_remote_error(error)),
        }
    }

    async fn remote_fetch_extents(
        &mut self,
        extents: Vec<ExtentRef>,
    ) -> Result<Vec<legato_proto::ExtentRecord>, FilesystemServiceError> {
        self.ensure_transport_available().await?;
        let result = self
            .transport
            .as_mut()
            .expect("transport should be available")
            .fetch_extents(extents.clone())
            .await;
        match result {
            Ok(records) => Ok(records),
            Err(error) if should_retry_after_reconnect(&error) => {
                self.reconnect_transport_now()
                    .await
                    .map_err(FilesystemServiceError::Transport)?;
                self.transport
                    .as_mut()
                    .expect("transport should be available")
                    .fetch_extents(extents)
                    .await
                    .map_err(|error| self.handle_remote_error(error))
            }
            Err(error) => Err(self.handle_remote_error(error)),
        }
    }

    async fn remote_change_records_since(
        &mut self,
        cursor: u64,
    ) -> Result<Vec<ChangeRecord>, FilesystemServiceError> {
        self.ensure_transport_available().await?;
        let result = self
            .transport
            .as_mut()
            .expect("transport should be available")
            .change_records_since(cursor)
            .await;
        match result {
            Ok(records) => Ok(records),
            Err(error) if should_retry_after_reconnect(&error) => {
                self.reconnect_transport_now()
                    .await
                    .map_err(FilesystemServiceError::Transport)?;
                self.transport
                    .as_mut()
                    .expect("transport should be available")
                    .change_records_since(cursor)
                    .await
                    .map_err(|error| self.handle_remote_error(error))
            }
            Err(error) => Err(self.handle_remote_error(error)),
        }
    }

    async fn ensure_transport_available(&mut self) -> Result<(), FilesystemServiceError> {
        if self.transport.is_some() {
            return Ok(());
        }
        let now_ns = now_monotonic_ns();
        if now_ns < self.next_transport_attempt_ns {
            return Err(FilesystemServiceError::Unavailable(format!(
                "retry backoff active for {} ms",
                self.next_transport_attempt_ns.saturating_sub(now_ns) / 1_000_000
            )));
        }
        self.connect_transport_now()
            .await
            .map_err(FilesystemServiceError::Transport)
    }

    async fn connect_transport_now(&mut self) -> Result<(), crate::ClientTransportError> {
        match GrpcClientTransport::connect(self.config.clone(), self.client_name.clone()).await {
            Ok(transport) => {
                self.server_name = transport.attach_session().server_name.clone();
                self.transport = Some(transport);
                self.transport_attempts = 0;
                self.next_transport_attempt_ns = 0;
                tracing::info!(
                    server_name = self.server_name.as_str(),
                    "client remote transport connected"
                );
                Ok(())
            }
            Err(error) => {
                self.record_transport_failure(&error);
                Err(error)
            }
        }
    }

    async fn reconnect_transport_now(&mut self) -> Result<(), crate::ClientTransportError> {
        let result = if let Some(transport) = self.transport.as_mut() {
            transport.reconnect().await.map(|_| ())
        } else {
            return self.connect_transport_now().await;
        };
        match result {
            Ok(()) => {
                if let Some(transport) = self.transport.as_ref() {
                    self.server_name = transport.attach_session().server_name.clone();
                }
                self.transport_attempts = 0;
                self.next_transport_attempt_ns = 0;
                tracing::info!(
                    server_name = self.server_name.as_str(),
                    "client remote transport reconnected"
                );
                Ok(())
            }
            Err(error) => {
                self.record_transport_failure(&error);
                Err(error)
            }
        }
    }

    fn handle_remote_error(
        &mut self,
        error: crate::ClientTransportError,
    ) -> FilesystemServiceError {
        if should_retry_after_reconnect(&error) {
            self.record_transport_failure(&error);
        }
        FilesystemServiceError::Transport(error)
    }

    fn record_transport_failure(&mut self, error: &crate::ClientTransportError) {
        self.transport = None;
        self.transport_attempts = self.transport_attempts.saturating_add(1);
        let delay_ms = retry_delay_ms(&self.config.retry, self.transport_attempts);
        self.next_transport_attempt_ns =
            now_monotonic_ns().saturating_add(delay_ms.saturating_mul(1_000_000));
        tracing::warn!(
            error = %error,
            attempts = self.transport_attempts,
            retry_delay_ms = delay_ms,
            "client remote transport unavailable"
        );
    }

    async fn sync_changes_if_due(&mut self, force: bool) -> Result<(), FilesystemServiceError> {
        let now_ns = now_monotonic_ns();
        if !force && now_ns.saturating_sub(self.last_change_sync_ns) < CHANGE_SYNC_INTERVAL_NS {
            return Ok(());
        }
        self.last_change_sync_ns = now_ns;
        let started = Instant::now();
        match self
            .remote_change_records_since(self.store.subscription_cursor())
            .await
        {
            Ok(records) => {
                let record_count = records.len();
                for record in records {
                    self.apply_change_record(&record)?;
                }
                log_slow_change_sync(record_count, started.elapsed());
                Ok(())
            }
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "client change sync skipped; local catalog remains active"
                );
                Ok(())
            }
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
        match self.remote_fetch_extents(request_extents).await {
            Ok(extents) => {
                self.store_extents(&extents, now_ns)?;
                Ok(())
            }
            Err(error) => Err(error),
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
        let result = if let Some(transport) = self.transport.as_mut() {
            transport.report_metrics(&snapshot).await
        } else {
            return;
        };
        match result {
            Ok(()) => {
                self.last_metrics_report_ns = now_ns;
                self.metrics_dirty = false;
            }
            Err(error) => {
                if should_retry_after_reconnect(&error) {
                    self.record_transport_failure(&error);
                }
                tracing::warn!(
                    error = %error,
                    samples = snapshot.len(),
                    "client metrics report failed"
                );
            }
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
    now_ns: u64,
) -> Result<Vec<u8>, FilesystemServiceError> {
    let end = offset.saturating_add(u64::from(size)).min(handle.size);
    let mut bytes = Vec::with_capacity(size as usize);

    for descriptor in read_plan(handle, offset, size) {
        let Some(extent) = store.get_extent(handle.file_id, descriptor.extent_index)? else {
            return Err(FilesystemServiceError::Unavailable(format!(
                "extent {} for {} is not resident",
                descriptor.extent_index, handle.path
            )));
        };
        store.touch_extent(handle.file_id, descriptor.extent_index, now_ns)?;
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

fn inode_to_file_metadata(inode: &InodeMetadata) -> FileMetadata {
    FileMetadata {
        file_id: inode.file_id,
        path: inode.path.clone(),
        size: inode.size,
        mtime_ns: inode.mtime_ns,
        content_hash: inode.content_hash.clone(),
        is_dir: inode.is_dir,
    }
}

fn synthetic_root_metadata() -> FileMetadata {
    FileMetadata {
        file_id: ROOT_FILE_ID,
        path: String::from("/"),
        size: 0,
        mtime_ns: 0,
        content_hash: Vec::new(),
        is_dir: true,
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
) -> impl FnOnce(FilesystemServiceError) -> FilesystemServiceError + 'a {
    move |error| match &error {
        FilesystemServiceError::Transport(crate::ClientTransportError::Rpc(status))
            if status.code() == tonic::Code::NotFound =>
        {
            FilesystemServiceError::NotFound(path.to_owned())
        }
        _ => error,
    }
}

fn is_remote_unavailable(error: &FilesystemServiceError) -> bool {
    match error {
        FilesystemServiceError::Unavailable(_) => true,
        FilesystemServiceError::Transport(error) => should_retry_after_reconnect(error),
        _ => false,
    }
}

fn retry_delay_ms(policy: &crate::RetryPolicy, attempts: u32) -> u64 {
    if policy.initial_delay_ms == 0 {
        return 0;
    }
    let max_delay_ms = policy.max_delay_ms.max(policy.initial_delay_ms);
    let multiplier = u64::from(policy.multiplier.max(1));
    let mut delay = policy.initial_delay_ms;
    for _ in 1..attempts {
        delay = delay.saturating_mul(multiplier).min(max_delay_ms);
    }
    delay
}

fn log_slow_operation(
    operation: &'static str,
    path: &str,
    source: &'static str,
    elapsed: Duration,
) {
    if elapsed >= SLOW_OPERATION_WARN_AFTER {
        tracing::warn!(
            operation,
            path,
            source,
            elapsed_ms = elapsed.as_millis() as u64,
            "slow client filesystem operation"
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn log_slow_read(
    path: &str,
    offset: u64,
    size: u32,
    cache_hits: u64,
    cache_misses: u64,
    local_bytes: u64,
    remote_bytes: u64,
    elapsed: Duration,
) {
    if elapsed >= SLOW_OPERATION_WARN_AFTER {
        tracing::warn!(
            operation = "read",
            path,
            offset,
            size,
            cache_hits,
            cache_misses,
            local_bytes,
            remote_bytes,
            elapsed_ms = elapsed.as_millis() as u64,
            "slow client filesystem operation"
        );
    }
}

fn log_slow_change_sync(records: usize, elapsed: Duration) {
    if elapsed >= SLOW_OPERATION_WARN_AFTER {
        tracing::warn!(
            operation = "sync_changes",
            records,
            elapsed_ms = elapsed.as_millis() as u64,
            "slow client filesystem operation"
        );
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

fn sanitize_directory_entries(path: &str, entries: Vec<DirectoryEntry>) -> Vec<DirectoryEntry> {
    let mut names = BTreeSet::new();
    let parent = normalize_listing_path(path);
    let mut sanitized = entries
        .into_iter()
        .filter(|entry| {
            !entry.name.is_empty()
                && entry.name != "."
                && entry.name != ".."
                && normalize_listing_path(&entry.path) != parent
                && names.insert(entry.name.clone())
        })
        .collect::<Vec<_>>();
    sanitized.sort_by(|left, right| left.name.cmp(&right.name));
    sanitized
}

fn normalize_listing_path(path: &str) -> String {
    let mut normalized = String::from("/");
    let components = path
        .split(['/', '\\'])
        .filter(|component| !component.is_empty() && *component != ".");
    normalized.push_str(&components.collect::<Vec<_>>().join("/"));
    if normalized.len() > 1 && normalized.ends_with('/') {
        normalized.pop();
    }
    normalized
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

    use legato_client_cache::client_store::ClientLegatoStore;
    use legato_proto::{
        DirectoryEntry, ExtentDescriptor, ExtentRecord, FileLayout, InodeMetadata, TransferClass,
    };
    use legato_types::FileId;
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

    #[test]
    fn directory_listing_sanitizer_drops_self_and_duplicate_entries() {
        let entries = super::sanitize_directory_entries(
            "/samples",
            vec![
                DirectoryEntry {
                    name: String::from("pack-b"),
                    path: String::from("/samples/pack-b"),
                    is_dir: true,
                    file_id: 2,
                },
                DirectoryEntry {
                    name: String::from("."),
                    path: String::from("/samples"),
                    is_dir: true,
                    file_id: 1,
                },
                DirectoryEntry {
                    name: String::from("pack-a"),
                    path: String::from("/samples/pack-a"),
                    is_dir: true,
                    file_id: 3,
                },
                DirectoryEntry {
                    name: String::from("pack-a"),
                    path: String::from("/samples/pack-a-duplicate"),
                    is_dir: true,
                    file_id: 4,
                },
                DirectoryEntry {
                    name: String::from("self"),
                    path: String::from("\\samples"),
                    is_dir: true,
                    file_id: 5,
                },
            ],
        );

        assert_eq!(
            entries
                .into_iter()
                .map(|entry| entry.name)
                .collect::<Vec<_>>(),
            vec![String::from("pack-a"), String::from("pack-b")]
        );
    }

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

    fn unavailable_client_config(root: &Path, client_name: &str) -> crate::ClientConfig {
        let tls_dir = root.join(format!("tls-{client_name}"));
        let bundle_dir = root.join(format!("bundle-{client_name}"));
        let mut server_tls = ServerTlsConfig::local_dev(&tls_dir);
        server_tls.server_names = vec![String::from("127.0.0.1"), String::from("localhost")];
        ensure_server_tls_materials(&tls_dir, &server_tls)
            .expect("tls materials should be created");
        issue_client_tls_bundle(&tls_dir, &server_tls, client_name, &bundle_dir)
            .expect("client bundle should be issued");
        local_client_config(String::from("127.0.0.1:1"), &bundle_dir, "localhost")
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
            bootstrap: Default::default(),
        };
        config.tls.server_names = vec![String::from("127.0.0.1"), String::from("localhost")];
        ensure_server_tls_materials(Path::new(&config.tls_dir), &config.tls)
            .expect("tls materials should be created");

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let address = listener.local_addr().expect("addr should be available");
        let server = LiveServer::bootstrap(config.clone()).expect("server should bootstrap");
        let first_bound = server
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
        first_bound
            .shutdown()
            .await
            .expect("server should shut down");
    }

    #[tokio::test]
    async fn filesystem_service_mounts_from_local_store_when_remote_is_unavailable() {
        let fixture = tempdir().expect("tempdir should be created");
        let client_state = fixture.path().join("client-state");
        let mut store = ClientLegatoStore::open(&client_state, 100).expect("store should open");
        store
            .record_inode(InodeMetadata {
                file_id: 1,
                path: String::from("/"),
                size: 0,
                mtime_ns: 1,
                is_dir: true,
                layout: Some(FileLayout {
                    transfer_class: TransferClass::Unitary as i32,
                    extents: Vec::new(),
                }),
                inode_generation: 1,
                content_hash: Vec::new(),
            })
            .expect("root inode should record");
        store
            .record_directory(
                "/",
                FileId(1),
                vec![DirectoryEntry {
                    name: String::from("cached.wav"),
                    path: String::from("/cached.wav"),
                    is_dir: false,
                    file_id: 7,
                }],
            )
            .expect("root directory should record");
        store
            .record_inode(InodeMetadata {
                file_id: 7,
                path: String::from("/cached.wav"),
                size: 6,
                mtime_ns: 2,
                is_dir: false,
                layout: Some(FileLayout {
                    transfer_class: TransferClass::Unitary as i32,
                    extents: vec![ExtentDescriptor {
                        extent_index: 0,
                        file_offset: 0,
                        length: 6,
                        extent_hash: Vec::new(),
                    }],
                }),
                inode_generation: 1,
                content_hash: b"cached".to_vec(),
            })
            .expect("file inode should record");
        store
            .put_extent(&ExtentRecord {
                file_id: 7,
                extent_index: 0,
                file_offset: 0,
                data: b"cached".to_vec(),
                extent_hash: Vec::new(),
                transfer_class: TransferClass::Unitary as i32,
            })
            .expect("extent should store");
        store.checkpoint().expect("checkpoint should write");
        drop(store);

        let mut service = FilesystemService::connect(
            unavailable_client_config(fixture.path(), "offline-client"),
            "offline-client",
            &client_state,
        )
        .await
        .expect("service should mount from local store");

        assert!(!service.has_active_subscription());
        assert_eq!(service.server_name(), "localhost");
        let root = service.lookup("/").await.expect("root should resolve");
        assert!(root.is_dir);
        let entries = service.read_dir("/").await.expect("root should list");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path, "/cached.wav");
        let attrs = service
            .lookup("/cached.wav")
            .await
            .expect("cached file should resolve");
        assert_eq!(attrs.size, 6);
        let handle = service
            .open("/cached.wav")
            .await
            .expect("cached file should open");
        let bytes = service
            .read(handle.local_handle, 0, 6)
            .await
            .expect("resident bytes should read");

        assert_eq!(bytes, b"cached");
    }

    #[tokio::test]
    async fn filesystem_service_exposes_empty_root_without_catalog_or_remote() {
        let fixture = tempdir().expect("tempdir should be created");
        let client_state = fixture.path().join("empty-client-state");
        let mut service = FilesystemService::connect(
            unavailable_client_config(fixture.path(), "empty-offline-client"),
            "empty-offline-client",
            &client_state,
        )
        .await
        .expect("service should mount without a populated catalog");

        let root = service.lookup("/").await.expect("root should resolve");
        let entries = service.read_dir("/").await.expect("root should list");

        assert!(root.is_dir);
        assert!(entries.is_empty());
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
            bootstrap: Default::default(),
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
            bootstrap: Default::default(),
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
