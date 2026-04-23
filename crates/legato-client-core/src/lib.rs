//! Shared runtime state for native Legato clients.

mod filesystem;
mod metrics;
mod transport;

use std::{collections::HashMap, fs, io::Cursor, path::Path, sync::Arc};

use legato_client_cache::{
    CacheConfig, MetadataCache, MetadataCacheLookup,
    catalog::CatalogStoreError,
    client_store::{ClientLegatoStore, ResidentExtent},
};
use legato_proto::{
    AttachRequest, ChangeKind, ChangeRecord, DirectoryEntry, ExtentDescriptor, ExtentRecord,
    FileLayout, FileMetadata, InodeMetadata, InvalidationEvent, InvalidationKind, PROTOCOL_VERSION,
    PrefetchPriority as ProtoPrefetchPriority, default_capabilities,
};
use legato_types::{
    ExtentRange, FileId, PrefetchHintPath, PrefetchPlanEntry, PrefetchPriority, PrefetchRequest,
};
use rustls::{
    ClientConfig as RustlsClientConfig, RootCertStore,
    pki_types::{CertificateDer, PrivateKeyDer},
};
use serde::{Deserialize, Serialize};

pub use filesystem::{
    FilesystemOpenHandle, FilesystemService, FilesystemServiceError, now_monotonic_ns,
};
pub use metrics::{ClientRuntimeMetrics, PrefetchMetricsReport};
pub use transport::{
    ChangePoll, ClientAttachSession, ClientTransportError, GrpcChangeSubscription,
    GrpcClientTransport, GrpcInvalidationSubscription, InvalidationPoll,
};

/// Immutable settings used to bootstrap a client runtime.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
pub struct ClientConfig {
    /// Logical endpoint name used for diagnostics.
    pub endpoint: String,
    /// Cache settings for the local runtime.
    pub cache: CacheConfig,
    /// mTLS materials and DNS identity for the remote server.
    pub tls: ClientTlsConfig,
    /// Retry policy for transient connection failures.
    pub retry: RetryPolicy,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            endpoint: String::from("legato.lan:7823"),
            cache: CacheConfig::default(),
            tls: ClientTlsConfig::default(),
            retry: RetryPolicy::default(),
        }
    }
}

/// TLS settings used by the client transport.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
pub struct ClientTlsConfig {
    /// PEM-encoded CA bundle that verifies the server certificate.
    pub ca_cert_path: String,
    /// PEM-encoded client certificate presented to the server.
    pub client_cert_path: String,
    /// PEM-encoded client private key.
    pub client_key_path: String,
    /// DNS name expected in the server certificate.
    pub server_name: String,
}

impl Default for ClientTlsConfig {
    fn default() -> Self {
        Self {
            ca_cert_path: String::from("/etc/legato/certs/server-ca.pem"),
            client_cert_path: String::from("/etc/legato/certs/client.pem"),
            client_key_path: String::from("/etc/legato/certs/client-key.pem"),
            server_name: String::from("legato.lan"),
        }
    }
}

impl ClientTlsConfig {
    /// Returns a local-development certificate layout rooted at the provided directory.
    #[must_use]
    pub fn local_dev(base_dir: &Path, server_name: &str) -> Self {
        Self {
            ca_cert_path: base_dir
                .join("server-ca.pem")
                .to_string_lossy()
                .into_owned(),
            client_cert_path: base_dir.join("client.pem").to_string_lossy().into_owned(),
            client_key_path: base_dir
                .join("client-key.pem")
                .to_string_lossy()
                .into_owned(),
            server_name: server_name.to_owned(),
        }
    }
}

/// Exponential reconnect policy for the client transport.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
pub struct RetryPolicy {
    /// Base reconnect delay after the first transient failure.
    pub initial_delay_ms: u64,
    /// Maximum reconnect delay after repeated failures.
    pub max_delay_ms: u64,
    /// Multiplicative backoff factor applied after each failure.
    pub multiplier: u32,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            initial_delay_ms: 250,
            max_delay_ms: 5_000,
            multiplier: 2,
        }
    }
}

/// The current transport state for the client runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionStatus {
    /// No active transport is currently established.
    Disconnected,
    /// The client is backing off before the next reconnect attempt.
    Backoff {
        /// Consecutive reconnect attempt count that produced this delay.
        attempt: u32,
        /// Milliseconds to wait before the next reconnect attempt.
        delay_ms: u64,
    },
    /// A transport session is active.
    Connected {
        /// Monotonic generation incremented for each successful reconnect.
        generation: u64,
        /// Whether the invalidation subscription has been restored.
        subscription_active: bool,
    },
}

/// Reconnect work required to restore the session after a disconnect.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReconnectPlan {
    /// Attach request for the next transport generation.
    pub attach: AttachRequest,
    /// Whether the invalidation subscription should be re-established.
    pub resubscribe: bool,
}

/// Result of applying reconnect completion work after a new transport is established.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryCompletion {
    /// Paths whose handles were successfully refreshed.
    pub reopened_paths: Vec<String>,
    /// Root invalidation to apply when the server generation changed.
    pub invalidation: Option<InvalidationEvent>,
}

/// Errors encountered while loading the client TLS configuration.
#[derive(Debug)]
pub enum ClientTlsError {
    /// Underlying filesystem access failed.
    Io(std::io::Error),
    /// A PEM block could not be parsed.
    Pem(std::io::Error),
    /// The configured CA bundle was empty.
    MissingCaBundle(String),
    /// The configured client certificate chain was empty.
    MissingClientCertificate(String),
    /// The configured private key was missing.
    MissingPrivateKey(String),
    /// rustls rejected the resulting client TLS configuration.
    Rustls(rustls::Error),
}

impl std::fmt::Display for ClientTlsError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "client TLS file IO failed: {error}"),
            Self::Pem(error) => write!(formatter, "client TLS PEM parsing failed: {error}"),
            Self::MissingCaBundle(path) => write!(formatter, "no CA certificates found in {path}"),
            Self::MissingClientCertificate(path) => {
                write!(formatter, "no client certificates found in {path}")
            }
            Self::MissingPrivateKey(path) => write!(formatter, "no private key found in {path}"),
            Self::Rustls(error) => write!(formatter, "client TLS config failed: {error}"),
        }
    }
}

impl std::error::Error for ClientTlsError {}

/// Stateful runtime shell for attach, reconnect, and stale-handle recovery planning.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientRuntime {
    config: ClientConfig,
    generation: u64,
    session_status: SessionStatus,
    failure_count: u32,
}

impl ClientRuntime {
    /// Creates a runtime with the provided immutable configuration.
    #[must_use]
    pub fn new(config: ClientConfig) -> Self {
        Self {
            config,
            generation: 0,
            session_status: SessionStatus::Disconnected,
            failure_count: 0,
        }
    }

    /// Builds the initial attach request for the configured client runtime.
    #[must_use]
    pub fn attach_request(&self, client_name: &str) -> AttachRequest {
        AttachRequest {
            protocol_version: PROTOCOL_VERSION,
            client_name: client_name.to_owned(),
            desired_capabilities: default_capabilities(),
        }
    }

    /// Validates the configured client mTLS files and marks the session connected.
    pub fn connect(
        &mut self,
        client_name: &str,
    ) -> Result<(AttachRequest, Arc<RustlsClientConfig>), ClientTlsError> {
        let tls = build_tls_client_config(&self.config.tls)?;
        self.mark_transport_ready(false);
        Ok((self.attach_request(client_name), tls))
    }

    /// Marks the invalidation subscription active for the current generation.
    pub fn mark_subscription_active(&mut self) {
        if let SessionStatus::Connected {
            generation,
            subscription_active: _,
        } = self.session_status
        {
            self.session_status = SessionStatus::Connected {
                generation,
                subscription_active: true,
            };
        }
    }

    /// Marks the transport unavailable and returns the next reconnect delay.
    pub fn mark_transport_unavailable(&mut self) -> u64 {
        self.failure_count = self.failure_count.saturating_add(1);
        let delay_ms = backoff_delay_ms(&self.config.retry, self.failure_count);
        self.session_status = SessionStatus::Backoff {
            attempt: self.failure_count,
            delay_ms,
        };

        delay_ms
    }

    /// Marks the runtime connected for a new transport generation.
    pub fn mark_transport_ready(&mut self, subscription_active: bool) -> u64 {
        self.generation += 1;
        self.failure_count = 0;
        self.session_status = SessionStatus::Connected {
            generation: self.generation,
            subscription_active,
        };
        self.generation
    }

    /// Builds the reconnect work needed to restore subscriptions and stale handles.
    #[must_use]
    pub fn reconnect_plan(&self, client_name: &str) -> ReconnectPlan {
        ReconnectPlan {
            attach: self.attach_request(client_name),
            resubscribe: true,
        }
    }

    /// Applies reconnect results and returns any invalidation required for local caches.
    #[must_use]
    pub fn complete_reconnect(&mut self, invalidation_root: Option<&str>) -> RecoveryCompletion {
        RecoveryCompletion {
            reopened_paths: Vec::new(),
            invalidation: invalidation_root.map(|path| InvalidationEvent {
                kind: InvalidationKind::Subtree as i32,
                path: String::from(path),
                file_id: 0,
                issued_at_ns: filesystem::now_monotonic_ns(),
            }),
        }
    }

    /// Returns a shared reference to the runtime configuration.
    #[must_use]
    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    /// Returns the current session status.
    #[must_use]
    pub fn session_status(&self) -> &SessionStatus {
        &self.session_status
    }
}

/// Builds a TLS 1.3-only client config with client certificates for mTLS.
pub fn build_tls_client_config(
    config: &ClientTlsConfig,
) -> Result<Arc<RustlsClientConfig>, ClientTlsError> {
    let ca_certs = load_certificates(&config.ca_cert_path, ClientTlsError::MissingCaBundle)?;
    let client_chain = load_certificates(
        &config.client_cert_path,
        ClientTlsError::MissingClientCertificate,
    )?;
    let client_key = load_private_key(&config.client_key_path)?;

    let mut roots = RootCertStore::empty();
    let (added, _ignored) = roots.add_parsable_certificates(ca_certs);
    if added == 0 {
        return Err(ClientTlsError::MissingCaBundle(config.ca_cert_path.clone()));
    }

    let provider = rustls::crypto::ring::default_provider();
    let builder = RustlsClientConfig::builder_with_provider(provider.into())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(ClientTlsError::Rustls)?;
    let mut client_config = builder
        .with_root_certificates(roots)
        .with_client_auth_cert(client_chain, client_key)
        .map_err(ClientTlsError::Rustls)?;
    client_config.alpn_protocols = vec![b"h2".to_vec()];

    Ok(Arc::new(client_config))
}

fn load_certificates<F>(
    path: &str,
    missing_error: F,
) -> Result<Vec<CertificateDer<'static>>, ClientTlsError>
where
    F: FnOnce(String) -> ClientTlsError,
{
    let contents = fs::read(path).map_err(ClientTlsError::Io)?;
    let certificates = rustls_pemfile::certs(&mut Cursor::new(contents))
        .collect::<Result<Vec<_>, _>>()
        .map_err(ClientTlsError::Pem)?;

    if certificates.is_empty() {
        return Err(missing_error(String::from(path)));
    }

    Ok(certificates)
}

fn load_private_key(path: &str) -> Result<PrivateKeyDer<'static>, ClientTlsError> {
    let contents = fs::read(path).map_err(ClientTlsError::Io)?;
    let Some(private_key) =
        rustls_pemfile::private_key(&mut Cursor::new(contents)).map_err(ClientTlsError::Pem)?
    else {
        return Err(ClientTlsError::MissingPrivateKey(String::from(path)));
    };

    Ok(private_key)
}

fn backoff_delay_ms(policy: &RetryPolicy, failure_count: u32) -> u64 {
    let exponent = failure_count.saturating_sub(1);
    let mut delay_ms = policy.initial_delay_ms;
    for _ in 0..exponent {
        delay_ms = delay_ms.saturating_mul(u64::from(policy.multiplier));
        if delay_ms >= policy.max_delay_ms {
            return policy.max_delay_ms;
        }
    }
    delay_ms.min(policy.max_delay_ms)
}

/// Result of one coordinated read or prefetch planning step.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchPlan {
    /// Extents already resident in the local cache.
    pub cached: Vec<ResidentExtent>,
    /// Extents that still require a server-side transfer.
    pub missing: Vec<ExtentRange>,
    /// Number of active waiters now attached to the requested keys.
    pub waiter_count: usize,
}

/// In-memory coordinator that deduplicates overlapping extent fetches.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FetchCoordinator {
    inflight_waiters: HashMap<PrefetchKey, usize>,
}

impl FetchCoordinator {
    /// Creates an empty fetch coordinator.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inflight_waiters: HashMap::new(),
        }
    }

    /// Plans the cache lookups and remote fetches required for one semantic extent.
    pub fn prepare_extent(
        &mut self,
        store: &mut ClientLegatoStore,
        extent: &ExtentRange,
    ) -> Result<FetchPlan, CatalogStoreError> {
        let key = PrefetchKey {
            file_id: extent.file_id,
            extent_index: extent.extent_index,
        };
        if let Some(cached) = store.get_extent(extent.file_id, extent.extent_index)? {
            return Ok(FetchPlan {
                cached: vec![cached],
                missing: Vec::new(),
                waiter_count: 0,
            });
        }

        let waiters = self.inflight_waiters.entry(key).or_insert(0);
        *waiters += 1;
        Ok(FetchPlan {
            cached: Vec::new(),
            missing: if *waiters == 1 {
                vec![extent.clone()]
            } else {
                Vec::new()
            },
            waiter_count: *waiters,
        })
    }

    /// Completes one fetched extent and records it in the local cache.
    pub fn complete_extent(
        &mut self,
        store: &mut ClientLegatoStore,
        extent: &ExtentRange,
        data: &[u8],
    ) -> Result<ResidentExtent, CatalogStoreError> {
        let key = PrefetchKey {
            file_id: extent.file_id,
            extent_index: extent.extent_index,
        };
        self.inflight_waiters.remove(&key);
        store.put_extent(&ExtentRecord {
            file_id: extent.file_id.0,
            extent_index: extent.extent_index,
            file_offset: extent.file_offset,
            data: data.to_vec(),
            extent_hash: Vec::new(),
            transfer_class: 0,
        })
    }
}

/// Outcome of one synchronous client-side prefetch execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrefetchExecution {
    /// Extents accepted by the local executor.
    pub accepted: Vec<ExtentRange>,
    /// Extents guaranteed resident for the requested wait-through priority.
    pub completed: Vec<ExtentRange>,
}

/// Priority-ordered schedule emitted before prefetch execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrefetchSchedule {
    /// Extents in execution order.
    pub extents: Vec<PrefetchPlanEntry>,
    /// Highest priority the caller waits through before returning.
    pub wait_through: PrefetchPriority,
}

/// Tracks cache residency and executes prefetch requests through the local cache store.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PrefetchExecutor {
    residency: HashMap<PrefetchKey, PrefetchPriority>,
    execution_generation: u64,
}

impl PrefetchExecutor {
    /// Creates an empty executor.
    #[must_use]
    pub fn new() -> Self {
        Self {
            residency: HashMap::new(),
            execution_generation: 0,
        }
    }

    /// Executes a prefetch request synchronously using the provided fetch source.
    pub fn execute_with_source<F>(
        &mut self,
        request: &PrefetchRequest,
        coordinator: &mut FetchCoordinator,
        store: &mut ClientLegatoStore,
        mut source: F,
    ) -> Result<PrefetchExecution, CatalogStoreError>
    where
        F: FnMut(&ExtentRange) -> Vec<u8>,
    {
        let schedule = schedule_prefetch_request(request);
        self.execution_generation = self.execution_generation.saturating_add(1);

        let mut accepted = Vec::new();
        let mut completed = Vec::new();

        for entry in &schedule.extents {
            accepted.push(entry.extent.clone());
            let key = PrefetchKey {
                file_id: entry.extent.file_id,
                extent_index: entry.extent.extent_index,
            };

            let plan = coordinator.prepare_extent(store, &entry.extent)?;
            for extent in plan.missing {
                let data = source(&extent);
                let cached = coordinator.complete_extent(store, &extent, &data)?;
                store.pin_extent(
                    cached.file_id,
                    cached.extent_index,
                    entry.priority,
                    self.execution_generation,
                )?;
                self.residency.insert(
                    PrefetchKey {
                        file_id: cached.file_id,
                        extent_index: cached.extent_index,
                    },
                    entry.priority,
                );
            }

            for extent in plan.cached {
                store.pin_extent(
                    extent.file_id,
                    extent.extent_index,
                    entry.priority,
                    self.execution_generation,
                )?;
                self.residency.insert(
                    PrefetchKey {
                        file_id: extent.file_id,
                        extent_index: extent.extent_index,
                    },
                    entry.priority,
                );
            }

            self.residency.insert(key, entry.priority);
            if priority_satisfies_wait(entry.priority, schedule.wait_through) {
                completed.push(entry.extent.clone());
            }
        }
        store.checkpoint()?;

        Ok(PrefetchExecution {
            accepted,
            completed,
        })
    }

    /// Returns the most recent pin generation used by the executor.
    #[must_use]
    pub fn current_pin_generation(&self) -> u64 {
        self.execution_generation
    }

    /// Returns whether the extent is resident at or above the required priority.
    #[must_use]
    pub fn is_extent_resident(
        &self,
        extent: &ExtentRange,
        required_priority: PrefetchPriority,
    ) -> bool {
        let key = PrefetchKey {
            file_id: extent.file_id,
            extent_index: extent.extent_index,
        };
        self.residency
            .get(&key)
            .is_some_and(|priority| priority_satisfies_wait(*priority, required_priority))
    }
}

/// Local control-plane facade used by `legato-prefetch` and the client runtime.
#[derive(Clone, Debug)]
pub struct LocalControlPlane {
    metadata_cache: MetadataCache,
    canonical_paths: HashMap<String, InodeMetadata>,
    canonical_directories: HashMap<String, Vec<DirectoryEntry>>,
    fetch_coordinator: FetchCoordinator,
    prefetch_executor: PrefetchExecutor,
}

impl LocalControlPlane {
    /// Creates a local control plane with the provided metadata cache.
    #[must_use]
    pub fn new(metadata_cache: MetadataCache) -> Self {
        Self {
            metadata_cache,
            canonical_paths: HashMap::new(),
            canonical_directories: HashMap::new(),
            fetch_coordinator: FetchCoordinator::new(),
            prefetch_executor: PrefetchExecutor::new(),
        }
    }

    /// Registers a canonical resolved path for later local prefetch requests.
    pub fn register_path(&mut self, metadata: FileMetadata, now_ns: u64) {
        self.register_resolved_path(
            InodeMetadata {
                file_id: metadata.file_id,
                path: metadata.path,
                size: metadata.size,
                mtime_ns: metadata.mtime_ns,
                is_dir: metadata.is_dir,
                layout: None,
                inode_generation: 1,
                content_hash: metadata.content_hash,
            },
            now_ns,
        );
    }

    /// Registers a canonical resolved inode, including semantic layout when available.
    pub fn register_resolved_path(&mut self, inode: InodeMetadata, now_ns: u64) {
        self.metadata_cache.put_stat(
            &inode.path,
            Some(inode_metadata_to_file_metadata(&inode)),
            now_ns,
        );
        self.canonical_paths.insert(inode.path.clone(), inode);
    }

    /// Registers a canonical directory listing for later local read-dir requests.
    pub fn register_dir(&mut self, path: &str, entries: Vec<DirectoryEntry>, now_ns: u64) {
        self.metadata_cache
            .put_dir(path, Some(entries.clone()), now_ns);
        self.canonical_directories.insert(path.to_owned(), entries);
    }

    /// Applies an invalidation to the cached metadata state.
    pub fn apply_invalidation(&mut self, event: &InvalidationEvent) {
        self.metadata_cache.apply_invalidation(event);
        self.canonical_paths
            .retain(|path, _| !path_is_invalidated(path, &event.path));
        self.canonical_directories
            .retain(|path, _| !path_is_invalidated(path, &event.path));
    }

    /// Applies one ordered replay record to the local canonical metadata view.
    pub fn apply_change_record(&mut self, record: &ChangeRecord, now_ns: u64) {
        match ChangeKind::try_from(record.kind).unwrap_or(ChangeKind::Unspecified) {
            ChangeKind::Upsert => {
                if let Some(inode) = record.inode.clone() {
                    self.register_resolved_path(inode.clone(), now_ns);
                    if inode.is_dir && !record.entries.is_empty() {
                        self.register_dir(&inode.path, record.entries.clone(), now_ns);
                    }
                }
            }
            ChangeKind::Delete | ChangeKind::Invalidate => {
                self.apply_invalidation(&InvalidationEvent {
                    kind: InvalidationKind::Subtree as i32,
                    path: record.path.clone(),
                    file_id: record.file_id,
                    issued_at_ns: 0,
                });
            }
            ChangeKind::Checkpoint | ChangeKind::Unspecified => {}
        }
    }

    /// Resolves a path through the local metadata cache.
    pub fn resolve_path(&mut self, path: &str, now_ns: u64) -> Option<FileMetadata> {
        match self.metadata_cache.stat(path, now_ns) {
            MetadataCacheLookup::Hit(metadata) => metadata,
            MetadataCacheLookup::Miss => {
                let metadata = self
                    .canonical_paths
                    .get(path)
                    .map(inode_metadata_to_file_metadata);
                self.metadata_cache.put_stat(path, metadata.clone(), now_ns);
                metadata
            }
        }
    }

    /// Resolves a directory listing through the local metadata cache.
    pub fn list_dir(&mut self, path: &str, now_ns: u64) -> Option<Vec<DirectoryEntry>> {
        match self.metadata_cache.list_dir(path, now_ns) {
            MetadataCacheLookup::Hit(entries) => entries,
            MetadataCacheLookup::Miss => {
                let entries = self.canonical_directories.get(path).cloned();
                self.metadata_cache.put_dir(path, entries.clone(), now_ns);
                entries
            }
        }
    }

    /// Resolves hint paths and executes prefetch through the local client runtime.
    pub fn prefetch_paths<F>(
        &mut self,
        hints: &[PrefetchHintPath],
        wait_through: PrefetchPriority,
        store: &mut ClientLegatoStore,
        now_ns: u64,
        source: F,
    ) -> Result<PrefetchExecution, CatalogStoreError>
    where
        F: FnMut(&ExtentRange) -> Vec<u8>,
    {
        let _ = now_ns;
        let mut extents = Vec::new();
        for hint in hints {
            if let Some(inode) = self
                .canonical_paths
                .get(hint.path.to_string_lossy().as_ref())
                .cloned()
            {
                store.record_inode(inode.clone())?;
                extents.extend(extents_for_hint(&inode, hint).into_iter().map(|extent| {
                    PrefetchPlanEntry {
                        extent,
                        priority: hint.priority,
                    }
                }));
            }
        }

        let request = PrefetchRequest {
            extents,
            wait_through,
        };

        self.prefetch_executor.execute_with_source(
            &request,
            &mut self.fetch_coordinator,
            store,
            source,
        )
    }

    /// Returns whether the supplied range is already resident locally.
    #[must_use]
    pub fn is_extent_resident(
        &self,
        extent: &ExtentRange,
        required_priority: PrefetchPriority,
    ) -> bool {
        self.prefetch_executor
            .is_extent_resident(extent, required_priority)
    }
}

fn inode_metadata_to_file_metadata(inode: &InodeMetadata) -> FileMetadata {
    FileMetadata {
        file_id: inode.file_id,
        path: inode.path.clone(),
        size: inode.size,
        mtime_ns: inode.mtime_ns,
        content_hash: inode.content_hash.clone(),
        is_dir: inode.is_dir,
    }
}

fn extents_for_hint(inode: &InodeMetadata, hint: &PrefetchHintPath) -> Vec<ExtentRange> {
    if inode.is_dir {
        return Vec::new();
    }

    let requested_end = hint.file_offset.saturating_add(hint.length).min(inode.size);
    let layout = inode
        .layout
        .clone()
        .unwrap_or_else(|| synthesize_unitary_layout(inode.size));

    layout
        .extents
        .into_iter()
        .filter(|extent| {
            let extent_end = extent.file_offset.saturating_add(extent.length);
            extent.file_offset < requested_end && extent_end > hint.file_offset
        })
        .map(extent_descriptor_to_range(inode.file_id))
        .collect()
}

fn synthesize_unitary_layout(size: u64) -> FileLayout {
    FileLayout {
        transfer_class: 0,
        extents: vec![ExtentDescriptor {
            extent_index: 0,
            file_offset: 0,
            length: size.max(1),
            extent_hash: Vec::new(),
        }],
    }
}

fn extent_descriptor_to_range(file_id: u64) -> impl Fn(ExtentDescriptor) -> ExtentRange {
    move |extent| ExtentRange {
        file_id: FileId(file_id),
        extent_index: extent.extent_index,
        file_offset: extent.file_offset,
        length: extent.length,
    }
}

/// Converts a local prefetch priority into the protobuf wire representation.
#[must_use]
pub fn proto_prefetch_priority(priority: PrefetchPriority) -> i32 {
    match priority {
        PrefetchPriority::P0 => ProtoPrefetchPriority::P0 as i32,
        PrefetchPriority::P1 => ProtoPrefetchPriority::P1 as i32,
        PrefetchPriority::P2 => ProtoPrefetchPriority::P2 as i32,
        PrefetchPriority::P3 => ProtoPrefetchPriority::P3 as i32,
    }
}

/// Sorts a prefetch request into deterministic priority order for execution.
#[must_use]
pub fn schedule_prefetch_request(request: &PrefetchRequest) -> PrefetchSchedule {
    let mut extents = request.extents.clone();
    extents.sort_by(|left, right| {
        prefetch_priority_ordinal(left.priority)
            .cmp(&prefetch_priority_ordinal(right.priority))
            .then_with(|| left.extent.file_id.cmp(&right.extent.file_id))
            .then_with(|| left.extent.file_offset.cmp(&right.extent.file_offset))
            .then_with(|| left.extent.extent_index.cmp(&right.extent.extent_index))
    });
    PrefetchSchedule {
        extents,
        wait_through: request.wait_through,
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct PrefetchKey {
    file_id: FileId,
    extent_index: u32,
}

fn priority_satisfies_wait(priority: PrefetchPriority, wait_through: PrefetchPriority) -> bool {
    prefetch_priority_ordinal(priority) <= prefetch_priority_ordinal(wait_through)
}

fn prefetch_priority_ordinal(priority: PrefetchPriority) -> u8 {
    match priority {
        PrefetchPriority::P0 => 0,
        PrefetchPriority::P1 => 1,
        PrefetchPriority::P2 => 2,
        PrefetchPriority::P3 => 3,
    }
}

fn path_is_invalidated(path: &str, invalidated_root: &str) -> bool {
    invalidated_root == "/"
        || path == invalidated_root
        || path
            .strip_prefix(invalidated_root)
            .is_some_and(|suffix| suffix.is_empty() || suffix.starts_with('/'))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{
        ClientConfig, ClientRuntime, ClientTlsConfig, ClientTlsError, FetchCoordinator,
        LocalControlPlane, PrefetchExecution, PrefetchExecutor, ReconnectPlan, RetryPolicy,
        SessionStatus, build_tls_client_config, schedule_prefetch_request,
    };
    use legato_client_cache::{
        MetadataCache, MetadataCachePolicy, client_store::ClientLegatoStore,
    };
    use legato_proto::{
        ChangeKind, ChangeRecord, DirectoryEntry, ExtentDescriptor, FileLayout, FileMetadata,
        InodeMetadata, InvalidationEvent, InvalidationKind, PROTOCOL_VERSION,
    };
    use legato_types::{
        ExtentRange, FileId, PrefetchHintPath, PrefetchPlanEntry, PrefetchPriority, PrefetchRequest,
    };
    use tempfile::tempdir;

    const TEST_CERT_PEM: &str = "-----BEGIN CERTIFICATE-----\nMIIC/zCCAeegAwIBAgIUKWr7nJpAOz9K1vWUN3gheRvIy/8wDQYJKoZIhvcNAQEL\nBQAwDzENMAsGA1UEAwwEdGVzdDAeFw0yNjA0MjIwMDU0MDlaFw0yNzA0MjIwMDU0\nMDlaMA8xDTALBgNVBAMMBHRlc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEK\nAoIBAQDEIfTpZUMQggMrqrDW9DMykuBUtQs7C0MAzs/WZjxTYaPfiONPYOvJ3n+e\nruGti/ypIxNijZPksrINYbh5PQpZ+Vo+bJml2K0S0d3EwDGEfLVEC8JNYUgKbCdZ\nvGuno/2KT4d5NnJNtVkxGZFh4KTFnpwhhbJH7lGt2VbvXLcJtQM+vgHpihz6QZxX\nR+L+LSNmaM8MZxU8MtbdyLKdey745osovkjdi+IKmkXb0ySra1fzgmXDaWMThOXy\nTh5UuD5n0RuUf5U9kRrpNc2/WxKx60mqdVA0BPHpOZyvEH9Nop9ZctVF1WKUGAzf\nvEYfeo2/OVW/+l1owNSb1CGWBcglAgMBAAGjUzBRMB0GA1UdDgQWBBQTHES/FEdh\nBSSzvS3vdZyNTnqunzAfBgNVHSMEGDAWgBQTHES/FEdhBSSzvS3vdZyNTnqunzAP\nBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQBnXNfXPXQ8l89Cmy7D\ntoRjdWhPc1auU6U6LmZME5TcrQsDTEUlux2u4C2X+qTygZY/bJT8aum4D9LJlEh2\nY8tr/8yz2+jcoNu+tDmHs/OTTUuJfw03Gztbj/m0+nZBPEhmU2VK+t5SWUuJen+3\nEnE5oP2jByDR9AR/z9QPUqDgvP8wsuAvZ6mSZoP9iF3AGNLY8OF9j0BLBXSwHGkM\ncHJsVQvNJ+BOpn6KxsLxLl8DG4fwQ9RCBdhrSr3gQxYMWNnLmqbpeGDE+wQQWDEM\nPSvoKbrOwJyAO8RYUTTG0shPGm5J7tb1ZBJfITtfS4uNBRU8RLpDXFXk1hTKys+y\nEnAC\n-----END CERTIFICATE-----\n";
    const TEST_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDEIfTpZUMQggMr\nqrDW9DMykuBUtQs7C0MAzs/WZjxTYaPfiONPYOvJ3n+eruGti/ypIxNijZPksrIN\nYbh5PQpZ+Vo+bJml2K0S0d3EwDGEfLVEC8JNYUgKbCdZvGuno/2KT4d5NnJNtVkx\nGZFh4KTFnpwhhbJH7lGt2VbvXLcJtQM+vgHpihz6QZxXR+L+LSNmaM8MZxU8Mtbd\nyLKdey745osovkjdi+IKmkXb0ySra1fzgmXDaWMThOXyTh5UuD5n0RuUf5U9kRrp\nNc2/WxKx60mqdVA0BPHpOZyvEH9Nop9ZctVF1WKUGAzfvEYfeo2/OVW/+l1owNSb\n1CGWBcglAgMBAAECggEAE4jsS1jWIq1IYo+IOIivFsDxTg6QNUvMWya+JbUeGhH2\nD/wi49Ob+InMPUJe4Pm56yC+vAis69Dukg2jIZQ2VTrURbZsYUvhBShZBdE8vVzs\ncwAW1L01PzCBhNsS/+oCBUYhkK2fGeVPomfRBqYo0zQWifI2jRqMERw+H+4knvc3\nC40YUj8pjA02LTsSPOLAs1hg0ukUI9SvzzRqCweONlL9JUmxKREec6Ju6M+ReHrU\nFYzY8Drd336N4yEQhfKyfAIFbHwvgM7S1pYL3hMisxWUz6cTZ0dXJQ0RFs23KBQ+\nWF00pImP25FevsaumBBZVgTLeUcENZgNNd+QD9VjsQKBgQD8qm/b2lUnV68tyGc+\nZCN8TnHe9BXIjCEpeEFRQjQFG9RUa+gS9BMEszWqfwmTDmUHu/dLGjfwnFUOXtl1\nhR69FslexvU4xp0auBCGOlymT7JtDPFvdBIM9LqaX5w3ObJXb4/dBdAm+8OK6ClC\n8h/GDHeCyJiT5pJV5VUlE+s/TQKBgQDGuImwCXZER50cKXLA4UxmR+gHM/18u/WI\nTMrsvfLGaOC9mZDfvQbB0cyN5p9RuWhCb7UMigWdv7SBA149bf7KQVev/2wfwR9Z\n7YmBKo4wNq2ovUlmkdZxtxdKKgIPBdLjtKUuL5Rsogcd7yAgWKJ5y6fmnLBrz4zD\nx6nBwz9wOQKBgQDsNhbPTgWv+zytq55B6PJ34wp36m93BvJ1x5Qg+KiTYhoWNq9H\nEOG60iPI2m1ECwAOw/6EOuWzTyQBhFD+mk5LbsMhlRVqV9xGP3BLXMKDRRzE8IXC\nsZuyexT8/4eW5ZzCO20er7GS0GpWMYdpq9xilgMgxJJIKxYBsZ3xRPb4PQKBgEfq\n5TmmUvznBf75KSSQ5PtnLpvcvvJze6q2UAJZxBD2R8+WUg4G9PkUGnmIa0RCW28f\nymAdW2b5yDOgqmyE8F72QuvY/qKHW/dJtife5NKiFzsoNfY+9WL2JiGbDl+tdeMe\nr2EFqyudgAHfVrseGL8Ha15Ueqyp0oHQMqmDJeRRAoGBANMeTYR6a05xmT7ZDPHT\nNX0syIu42Yys3ZC9bNlke7iWDntNoyC0CfAqDNMKVDomMbxcs2nFqL3TiVIu5Kev\nGnn3tLiJjv/LC3F90gVhcwlN87/nNXlEPfeoOPVUImU/3Tq11lYH6JXU69sARRqQ\n8YgEyQYcCpY0679sL4W1s/w1\n-----END PRIVATE KEY-----\n";

    #[test]
    fn runtime_builds_attach_requests_from_workspace_defaults() {
        let runtime = ClientRuntime::new(ClientConfig::default());
        let attach = runtime.attach_request("legatofs");

        assert_eq!(attach.protocol_version, PROTOCOL_VERSION);
        assert_eq!(attach.client_name, "legatofs");
        assert_eq!(runtime.config().endpoint, "legato.lan:7823");
        assert_eq!(runtime.config().retry, RetryPolicy::default());
    }

    #[test]
    fn tls_client_config_builds_for_local_dev_materials() {
        let root = tempdir().expect("tempdir should be created");
        let tls = ClientTlsConfig::local_dev(root.path(), "legato.lan");
        fs::write(&tls.ca_cert_path, TEST_CERT_PEM).expect("ca cert should be written");
        fs::write(&tls.client_cert_path, TEST_CERT_PEM).expect("client cert should be written");
        fs::write(&tls.client_key_path, TEST_KEY_PEM).expect("client key should be written");

        let config = build_tls_client_config(&tls).expect("client tls should build");

        assert_eq!(config.alpn_protocols, vec![b"h2".to_vec()]);
    }

    #[test]
    fn connect_and_reconnect_plan_restores_subscription() {
        let root = tempdir().expect("tempdir should be created");
        let tls = ClientTlsConfig::local_dev(root.path(), "legato.lan");
        fs::write(&tls.ca_cert_path, TEST_CERT_PEM).expect("ca cert should be written");
        fs::write(&tls.client_cert_path, TEST_CERT_PEM).expect("client cert should be written");
        fs::write(&tls.client_key_path, TEST_KEY_PEM).expect("client key should be written");

        let mut runtime = ClientRuntime::new(ClientConfig {
            tls,
            ..ClientConfig::default()
        });

        let (attach, _tls_config) = runtime.connect("legatofs").expect("connect should succeed");
        assert_eq!(attach.client_name, "legatofs");
        assert_eq!(
            runtime.session_status(),
            &SessionStatus::Connected {
                generation: 1,
                subscription_active: false,
            }
        );

        runtime.mark_subscription_active();

        let delay_ms = runtime.mark_transport_unavailable();
        assert_eq!(delay_ms, 250);

        let plan = runtime.reconnect_plan("legatofs");
        assert_eq!(
            plan,
            ReconnectPlan {
                attach: runtime.attach_request("legatofs"),
                resubscribe: true,
            }
        );
    }

    #[test]
    fn reconnect_completion_invalidates_root_without_server_handles() {
        let mut runtime = ClientRuntime::new(ClientConfig::default());
        runtime.mark_transport_ready(true);

        let plan = runtime.reconnect_plan("legatofs");
        let completion = runtime.complete_reconnect(Some("/"));

        assert!(plan.resubscribe);
        assert!(completion.reopened_paths.is_empty());
        let invalidation = completion
            .invalidation
            .expect("root reconnect should invalidate cached metadata");
        assert_eq!(invalidation.kind, InvalidationKind::Subtree as i32);
        assert_eq!(invalidation.path, "/");
        assert_eq!(invalidation.file_id, 0);
        assert!(invalidation.issued_at_ns > 0);
    }

    #[test]
    fn retry_backoff_caps_at_configured_maximum() {
        let mut runtime = ClientRuntime::new(ClientConfig {
            retry: RetryPolicy {
                initial_delay_ms: 100,
                max_delay_ms: 350,
                multiplier: 3,
            },
            ..ClientConfig::default()
        });

        assert_eq!(runtime.mark_transport_unavailable(), 100);
        assert_eq!(runtime.mark_transport_unavailable(), 300);
        assert_eq!(runtime.mark_transport_unavailable(), 350);
        assert_eq!(
            runtime.session_status(),
            &SessionStatus::Backoff {
                attempt: 3,
                delay_ms: 350,
            }
        );
    }

    #[test]
    fn missing_ca_bundle_is_rejected() {
        let root = tempdir().expect("tempdir should be created");
        let tls = ClientTlsConfig::local_dev(root.path(), "legato.lan");
        fs::write(&tls.ca_cert_path, "").expect("ca cert should be written");
        fs::write(&tls.client_cert_path, TEST_CERT_PEM).expect("client cert should be written");
        fs::write(&tls.client_key_path, TEST_KEY_PEM).expect("client key should be written");

        let error = build_tls_client_config(&tls).expect_err("empty CA bundle should be rejected");
        assert!(
            matches!(error, ClientTlsError::MissingCaBundle(_)),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn fetch_coordinator_deduplicates_overlapping_extents() {
        let temp = tempdir().expect("tempdir should be created");
        let mut store =
            ClientLegatoStore::open(temp.path().join("state"), 100).expect("store should open");
        let mut coordinator = FetchCoordinator::new();

        store
            .record_inode(sample_prefetch_inode())
            .expect("inode should record");
        let first = coordinator
            .prepare_extent(
                &mut store,
                &ExtentRange {
                    file_id: FileId(7),
                    extent_index: 0,
                    file_offset: 0,
                    length: 4096,
                },
            )
            .expect("first fetch plan should succeed");
        assert_eq!(first.missing.len(), 1);

        let second = coordinator
            .prepare_extent(
                &mut store,
                &ExtentRange {
                    file_id: FileId(7),
                    extent_index: 0,
                    file_offset: 0,
                    length: 4096,
                },
            )
            .expect("second fetch plan should succeed");
        assert!(second.missing.is_empty());

        let _ = coordinator
            .complete_extent(
                &mut store,
                &ExtentRange {
                    file_id: FileId(7),
                    extent_index: 0,
                    file_offset: 0,
                    length: 4096,
                },
                b"abcd",
            )
            .expect("completed extent should persist");
        let cached = coordinator
            .prepare_extent(
                &mut store,
                &ExtentRange {
                    file_id: FileId(7),
                    extent_index: 0,
                    file_offset: 0,
                    length: 4096,
                },
            )
            .expect("cached fetch plan should succeed");
        assert_eq!(cached.cached.len(), 1);
        assert!(cached.missing.is_empty());
    }

    #[test]
    fn prefetch_executor_tracks_residency_by_priority() {
        let temp = tempdir().expect("tempdir should be created");
        let mut store =
            ClientLegatoStore::open(temp.path().join("state"), 100).expect("store should open");
        store
            .record_inode(sample_prefetch_inode())
            .expect("inode should record");
        let mut coordinator = FetchCoordinator::new();
        let mut executor = PrefetchExecutor::new();
        let request = PrefetchRequest {
            extents: vec![
                PrefetchPlanEntry {
                    extent: ExtentRange {
                        file_id: FileId(7),
                        extent_index: 0,
                        file_offset: 0,
                        length: 4096,
                    },
                    priority: PrefetchPriority::P0,
                },
                PrefetchPlanEntry {
                    extent: ExtentRange {
                        file_id: FileId(7),
                        extent_index: 1,
                        file_offset: 4096,
                        length: 4096,
                    },
                    priority: PrefetchPriority::P2,
                },
            ],
            wait_through: PrefetchPriority::P1,
        };

        let execution = executor
            .execute_with_source(&request, &mut coordinator, &mut store, |extent| {
                format!("extent-{}", extent.extent_index).into_bytes()
            })
            .expect("prefetch execution should succeed");

        assert_eq!(execution.accepted.len(), 2);
        assert_eq!(execution.completed.len(), 1);
        assert!(executor.is_extent_resident(
            &ExtentRange {
                file_id: FileId(7),
                extent_index: 0,
                file_offset: 0,
                length: 4096,
            },
            PrefetchPriority::P1,
        ));
        assert!(!executor.is_extent_resident(
            &ExtentRange {
                file_id: FileId(7),
                extent_index: 1,
                file_offset: 4096,
                length: 4096,
            },
            PrefetchPriority::P1,
        ));
    }

    #[test]
    fn local_control_plane_resolves_paths_prefetches_and_refreshes_on_invalidation() {
        let temp = tempdir().expect("tempdir should be created");
        let mut store =
            ClientLegatoStore::open(temp.path().join("state"), 100).expect("store should open");
        let mut control =
            LocalControlPlane::new(MetadataCache::new(MetadataCachePolicy::default()));
        control.register_resolved_path(
            InodeMetadata {
                file_id: 7,
                path: String::from("/Kontakt/piano.nki"),
                size: 8192,
                mtime_ns: 10,
                is_dir: false,
                layout: Some(FileLayout {
                    transfer_class: 0,
                    extents: vec![
                        ExtentDescriptor {
                            extent_index: 0,
                            file_offset: 0,
                            length: 4096,
                            extent_hash: Vec::new(),
                        },
                        ExtentDescriptor {
                            extent_index: 1,
                            file_offset: 4096,
                            length: 4096,
                            extent_hash: Vec::new(),
                        },
                    ],
                }),
                inode_generation: 1,
                content_hash: b"prefetch-content".to_vec(),
            },
            100,
        );

        let resolved = control
            .resolve_path("/Kontakt/piano.nki", 101)
            .expect("path should resolve");
        assert_eq!(resolved.file_id, 7);

        let execution = control
            .prefetch_paths(
                &[PrefetchHintPath {
                    path: "/Kontakt/piano.nki".into(),
                    file_offset: 0,
                    length: 8192,
                    priority: PrefetchPriority::P0,
                }],
                PrefetchPriority::P1,
                &mut store,
                200,
                |extent| format!("prefetch-{}", extent.extent_index).into_bytes(),
            )
            .expect("control-plane prefetch should succeed");
        assert_eq!(
            execution,
            PrefetchExecution {
                accepted: vec![
                    ExtentRange {
                        file_id: FileId(7),
                        extent_index: 0,
                        file_offset: 0,
                        length: 4096,
                    },
                    ExtentRange {
                        file_id: FileId(7),
                        extent_index: 1,
                        file_offset: 4096,
                        length: 4096,
                    },
                ],
                completed: vec![
                    ExtentRange {
                        file_id: FileId(7),
                        extent_index: 0,
                        file_offset: 0,
                        length: 4096,
                    },
                    ExtentRange {
                        file_id: FileId(7),
                        extent_index: 1,
                        file_offset: 4096,
                        length: 4096,
                    },
                ],
            }
        );
        assert!(control.is_extent_resident(
            &ExtentRange {
                file_id: FileId(7),
                extent_index: 0,
                file_offset: 0,
                length: 4096,
            },
            PrefetchPriority::P1,
        ));

        control.apply_invalidation(&InvalidationEvent {
            kind: InvalidationKind::File as i32,
            path: String::from("/Kontakt/piano.nki"),
            file_id: 7,
            issued_at_ns: 0,
        });
        assert!(control.resolve_path("/Kontakt/piano.nki", 201).is_none());
    }

    #[test]
    fn local_control_plane_applies_replay_records_for_directory_batches() {
        let mut control =
            LocalControlPlane::new(MetadataCache::new(MetadataCachePolicy::default()));

        control.apply_change_record(
            &ChangeRecord {
                sequence: 1,
                kind: ChangeKind::Upsert as i32,
                file_id: 11,
                path: String::from("/Kontakt"),
                inode: Some(InodeMetadata {
                    file_id: 11,
                    path: String::from("/Kontakt"),
                    size: 0,
                    mtime_ns: 10,
                    is_dir: true,
                    layout: Some(FileLayout {
                        transfer_class: 0,
                        extents: Vec::new(),
                    }),
                    inode_generation: 1,
                    content_hash: Vec::new(),
                }),
                entries: vec![DirectoryEntry {
                    name: String::from("piano.nki"),
                    path: String::from("/Kontakt/piano.nki"),
                    is_dir: false,
                    file_id: 7,
                }],
            },
            100,
        );
        control.apply_change_record(
            &ChangeRecord {
                sequence: 2,
                kind: ChangeKind::Upsert as i32,
                file_id: 7,
                path: String::from("/Kontakt/piano.nki"),
                inode: Some(sample_prefetch_inode()),
                entries: Vec::new(),
            },
            101,
        );

        let entries = control
            .list_dir("/Kontakt", 102)
            .expect("directory should resolve from replay");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "piano.nki");
        let metadata = control
            .resolve_path("/Kontakt/piano.nki", 102)
            .expect("file metadata should resolve from replay");
        assert_eq!(metadata.file_id, 7);

        control.apply_change_record(
            &ChangeRecord {
                sequence: 3,
                kind: ChangeKind::Delete as i32,
                file_id: 7,
                path: String::from("/Kontakt/piano.nki"),
                inode: None,
                entries: Vec::new(),
            },
            103,
        );
        assert!(control.resolve_path("/Kontakt/piano.nki", 104).is_none());
    }

    fn sample_prefetch_inode() -> InodeMetadata {
        InodeMetadata {
            file_id: 7,
            path: String::from("/Kontakt/piano.nki"),
            size: 8192,
            mtime_ns: 10,
            is_dir: false,
            layout: Some(FileLayout {
                transfer_class: 0,
                extents: vec![
                    ExtentDescriptor {
                        extent_index: 0,
                        file_offset: 0,
                        length: 4096,
                        extent_hash: Vec::new(),
                    },
                    ExtentDescriptor {
                        extent_index: 1,
                        file_offset: 4096,
                        length: 4096,
                        extent_hash: Vec::new(),
                    },
                ],
            }),
            inode_generation: 1,
            content_hash: b"prefetch-content".to_vec(),
        }
    }

    #[test]
    fn reconnect_completion_can_drive_control_plane_refresh_end_to_end() {
        let mut runtime = ClientRuntime::new(ClientConfig::default());
        runtime.mark_transport_ready(true);
        runtime.mark_transport_unavailable();

        let mut control =
            LocalControlPlane::new(MetadataCache::new(MetadataCachePolicy::default()));
        control.register_path(
            FileMetadata {
                file_id: 11,
                path: String::from("/Strings/long.ncw"),
                size: 64,
                mtime_ns: 1,
                content_hash: Vec::new(),
                is_dir: false,
            },
            10,
        );
        let completion = runtime.complete_reconnect(Some("/"));

        control.apply_invalidation(
            &completion
                .invalidation
                .expect("server restart should invalidate the control plane"),
        );
        assert!(control.resolve_path("/Strings/long.ncw", 11).is_none());

        control.register_path(
            FileMetadata {
                file_id: 11,
                path: String::from("/Strings/long.ncw"),
                size: 64,
                mtime_ns: 2,
                content_hash: Vec::new(),
                is_dir: false,
            },
            12,
        );
        assert!(control.resolve_path("/Strings/long.ncw", 13).is_some());
    }

    #[test]
    fn prefetch_requests_are_scheduled_in_priority_then_offset_order() {
        let schedule = schedule_prefetch_request(&PrefetchRequest {
            extents: vec![
                PrefetchPlanEntry {
                    extent: ExtentRange {
                        file_id: FileId(3),
                        extent_index: 1,
                        file_offset: 4096,
                        length: 4096,
                    },
                    priority: PrefetchPriority::P2,
                },
                PrefetchPlanEntry {
                    extent: ExtentRange {
                        file_id: FileId(2),
                        extent_index: 2,
                        file_offset: 8192,
                        length: 4096,
                    },
                    priority: PrefetchPriority::P0,
                },
                PrefetchPlanEntry {
                    extent: ExtentRange {
                        file_id: FileId(2),
                        extent_index: 0,
                        file_offset: 0,
                        length: 4096,
                    },
                    priority: PrefetchPriority::P0,
                },
            ],
            wait_through: PrefetchPriority::P1,
        });

        assert_eq!(schedule.wait_through, PrefetchPriority::P1);
        assert_eq!(schedule.extents[0].extent.file_offset, 0);
        assert_eq!(schedule.extents[1].extent.file_offset, 8192);
        assert_eq!(schedule.extents[2].priority, PrefetchPriority::P2);
    }
}
