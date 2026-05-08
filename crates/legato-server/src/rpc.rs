//! gRPC runtime wiring for the Legato server daemon.

use std::{
    collections::BTreeMap,
    net::SocketAddr,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::Duration,
    time::Instant,
};

use legato_foundation::{MetricKind as LocalMetricKind, MetricSample};
use tokio::{
    net::TcpListener,
    sync::{Mutex, mpsc},
    task::JoinHandle,
    time::{Instant as TokioInstant, MissedTickBehavior, Sleep, interval, timeout},
};
use tokio_stream::{
    Stream, StreamExt,
    wrappers::{ReceiverStream, TcpListenerStream},
};
use tonic::{
    Request, Response, Status,
    transport::{Certificate, Identity, Server as TransportServer, ServerTlsConfig},
};

use legato_client_cache::catalog::{CatalogInode, CatalogStore, inode_to_proto};
use legato_proto::{
    AttachRequest, AttachResponse, ChangeRecord, DirectoryEntry, ExtentRecord, FetchRequest,
    FileMetadata, HintRequest, HintResponse, ListDirRequest, ListDirResponse, MetricKind,
    ReportClientMetricsRequest, ReportClientMetricsResponse, ReportedMetric, ResolvePathRequest,
    ResolvePathResponse, ResolveRequest, ResolveResponse, StatRequest, StatResponse,
    SubscribeChangesRequest, SubscribeRequest,
    legato_server::{Legato, LegatoServer},
};

const SLOW_RPC_WARN_AFTER: Duration = Duration::from_millis(250);
const WATCH_RECONCILE_DEBOUNCE: Duration = Duration::from_millis(750);
use legato_types::FileId;

use crate::{
    InvalidationHub, NotificationAction, Server, ServerConfig, ServerRuntimeMetrics, WatchBackend,
    canonical::logical_request_path, create_poll_watcher, create_recommended_watcher,
    plan_notification_result, reconcile_library_root_to_store, subtree_invalidation,
};

type InvalidationStream =
    Pin<Box<dyn Stream<Item = Result<legato_proto::InvalidationEvent, Status>> + Send + 'static>>;
type FetchStream = Pin<Box<dyn Stream<Item = Result<ExtentRecord, Status>> + Send + 'static>>;
type ChangeStream = Pin<Box<dyn Stream<Item = Result<ChangeRecord, Status>> + Send + 'static>>;

/// PEM-encoded TLS materials loaded from the configured server certificate paths.
#[derive(Clone, Debug)]
pub struct RuntimeTlsConfig {
    /// Listener certificate chain PEM.
    pub server_cert_pem: Vec<u8>,
    /// Listener private key PEM.
    pub server_key_pem: Vec<u8>,
    /// Client CA bundle PEM used for mTLS verification.
    pub client_ca_pem: Vec<u8>,
}

/// Handle for a bound gRPC server task that can be shut down gracefully.
#[derive(Debug)]
pub struct BoundServer {
    shutdown_signal: tokio::sync::oneshot::Sender<()>,
    task: JoinHandle<Result<(), tonic::transport::Error>>,
    watch_task: JoinHandle<()>,
    watcher: ActiveWatcher,
    invalidations: Arc<Mutex<InvalidationHub>>,
}

impl BoundServer {
    /// Signals shutdown and waits for the underlying transport task to exit.
    pub async fn shutdown(self) -> Result<(), Box<dyn std::error::Error>> {
        let Self {
            shutdown_signal,
            mut task,
            watch_task,
            watcher,
            invalidations,
        } = self;
        let _ = shutdown_signal.send(());
        watch_task.abort();
        drop(watcher);
        invalidations.lock().await.clear_subscribers();
        match timeout(Duration::from_secs(5), &mut task).await {
            Ok(task_result) => task_result??,
            Err(_elapsed) => {
                task.abort();
                let _ = task.await;
            }
        }
        Ok(())
    }
}

/// Live network-facing Legato server runtime.
#[derive(Debug)]
pub struct LiveServer {
    shell: Server,
    config: ServerConfig,
    catalog: Arc<Mutex<CatalogStore>>,
    invalidations: Arc<Mutex<InvalidationHub>>,
    metrics: Option<ServerRuntimeMetrics>,
}

impl LiveServer {
    /// Bootstraps the live server state from the configured library and state directories.
    pub fn bootstrap(config: ServerConfig) -> Result<Self, Box<dyn std::error::Error>> {
        Self::bootstrap_with_metrics(config, None)
    }

    /// Bootstraps the live server state from the configured library and state directories.
    pub fn bootstrap_with_metrics(
        config: ServerConfig,
        metrics: Option<ServerRuntimeMetrics>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let started = Instant::now();
        let stats = reconcile_library_root_to_store(
            Path::new(&config.state_dir),
            Path::new(&config.library_root),
        )?;
        let catalog = CatalogStore::open(Path::new(&config.state_dir), 0)?;
        if let Some(metrics) = &metrics {
            metrics.record_bootstrap_reconcile(&stats, started.elapsed().as_nanos() as u64);
        }
        log_reconcile_summary("bootstrap", &stats, started.elapsed());
        let invalidations = InvalidationHub::new("/");

        Ok(Self {
            shell: Server::new(config.clone()),
            config,
            catalog: Arc::new(Mutex::new(catalog)),
            invalidations: Arc::new(Mutex::new(invalidations)),
            metrics,
        })
    }

    /// Binds the gRPC transport to an already-created listener.
    pub async fn bind(
        self,
        listener: TcpListener,
        tls: Option<RuntimeTlsConfig>,
    ) -> Result<BoundServer, Box<dyn std::error::Error>> {
        let incoming = TcpListenerStream::new(listener);
        let (shutdown_signal, shutdown_receiver) = tokio::sync::oneshot::channel::<()>();
        let (watch_sender, watch_receiver) = mpsc::unbounded_channel();

        let watcher = ActiveWatcher::create(
            Path::new(&self.config.library_root),
            WatchBackend::Recommended,
            watch_sender,
        )?;
        let watch_task = spawn_watch_task(
            watch_receiver,
            PathBuf::from(&self.config.library_root),
            PathBuf::from(&self.config.state_dir),
            Arc::clone(&self.catalog),
            Arc::clone(&self.invalidations),
        );

        let mut builder = TransportServer::builder();
        if let Some(tls) = tls {
            builder = builder.tls_config(
                ServerTlsConfig::new()
                    .identity(Identity::from_pem(tls.server_cert_pem, tls.server_key_pem))
                    .client_ca_root(Certificate::from_pem(tls.client_ca_pem)),
            )?;
        }

        let invalidations = Arc::clone(&self.invalidations);
        let task = tokio::spawn(async move {
            builder
                .add_service(LegatoServer::new(self))
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_receiver.await;
                })
                .await
        });

        Ok(BoundServer {
            shutdown_signal,
            task,
            watch_task,
            watcher,
            invalidations,
        })
    }
}

#[allow(dead_code)]
#[derive(Debug)]
enum ActiveWatcher {
    Recommended(notify::RecommendedWatcher),
    Poll(notify::PollWatcher),
}

impl ActiveWatcher {
    fn create(
        library_root: &Path,
        backend: WatchBackend,
        sender: mpsc::UnboundedSender<notify::Result<notify::Event>>,
    ) -> notify::Result<Self> {
        match backend {
            WatchBackend::Recommended => {
                let recommended_sender = sender.clone();
                create_recommended_watcher(library_root, move |event| {
                    let _ = recommended_sender.send(event);
                })
                .map(Self::Recommended)
                .or_else(|_| {
                    create_poll_watcher(library_root, Duration::from_secs(2), move |event| {
                        let _ = sender.send(event);
                    })
                    .map(Self::Poll)
                })
            }
            WatchBackend::Poll { interval } => {
                create_poll_watcher(library_root, interval, move |event| {
                    let _ = sender.send(event);
                })
                .map(Self::Poll)
            }
        }
    }
}

fn spawn_watch_task(
    mut receiver: mpsc::UnboundedReceiver<notify::Result<notify::Event>>,
    library_root: PathBuf,
    state_dir: PathBuf,
    catalog: Arc<Mutex<CatalogStore>>,
    invalidations: Arc<Mutex<InvalidationHub>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(result) = receiver.recv().await {
            let action = plan_notification_result(&library_root, result);
            if matches!(action, NotificationAction::Ignore) {
                continue;
            }
            let channel_closed =
                debounce_watch_events(&mut receiver, &library_root, WATCH_RECONCILE_DEBOUNCE).await;
            tracing::info!("server library reconcile started");
            let reconcile_started = Instant::now();
            let stats = match reconcile_library_root_to_store(&state_dir, &library_root) {
                Ok(stats) => stats,
                Err(error) => {
                    tracing::warn!(error = %error, "server library reconcile failed");
                    if channel_closed {
                        break;
                    }
                    continue;
                }
            };
            log_reconcile_summary("watch", &stats, reconcile_started.elapsed());
            let reopened_catalog = match CatalogStore::open(&state_dir, 0) {
                Ok(catalog) => catalog,
                Err(error) => {
                    tracing::warn!(error = %error, "server catalog reopen failed after reconcile");
                    if channel_closed {
                        break;
                    }
                    continue;
                }
            };
            *catalog.lock().await = reopened_catalog;

            let mut hub = invalidations.lock().await;
            hub.publish(subtree_invalidation("/", 0));
            if channel_closed {
                break;
            }
        }
    })
}

async fn debounce_watch_events(
    receiver: &mut mpsc::UnboundedReceiver<notify::Result<notify::Event>>,
    library_root: &Path,
    delay: Duration,
) -> bool {
    // SMB imports can emit thousands of notifications; wait for a quiet window
    // before the expensive full-library reconcile.
    let sleep = tokio::time::sleep(delay);
    tokio::pin!(sleep);
    drain_watch_events_until(receiver, library_root, &mut sleep, delay).await
}

async fn drain_watch_events_until(
    receiver: &mut mpsc::UnboundedReceiver<notify::Result<notify::Event>>,
    library_root: &Path,
    sleep: &mut Pin<&mut Sleep>,
    delay: Duration,
) -> bool {
    loop {
        tokio::select! {
            _ = sleep.as_mut() => return false,
            maybe_result = receiver.recv() => {
                let Some(result) = maybe_result else {
                    return true;
                };
                let action = plan_notification_result(library_root, result);
                if !matches!(action, NotificationAction::Ignore) {
                    sleep.as_mut().reset(TokioInstant::now() + delay);
                }
            }
        }
    }
}

#[tonic::async_trait]
impl Legato for LiveServer {
    type SubscribeStream = InvalidationStream;
    type FetchStream = FetchStream;
    type SubscribeChangesStream = ChangeStream;

    async fn attach(
        &self,
        request: Request<AttachRequest>,
    ) -> Result<Response<AttachResponse>, Status> {
        let desired_capabilities = request.into_inner().desired_capabilities;
        let response = self.shell.attach_response(&desired_capabilities);
        tracing::info!(
            desired_capabilities = desired_capabilities.len(),
            protocol_version = response.protocol_version,
            server_name = response.server_name.as_str(),
            "client attached"
        );
        Ok(Response::new(response))
    }

    async fn resolve(
        &self,
        request: Request<ResolveRequest>,
    ) -> Result<Response<ResolveResponse>, Status> {
        let started = Instant::now();
        let request_path = request.into_inner().path;
        let path = logical_request_path(Path::new(&self.config.library_root), &request_path)
            .map_err(|error| {
                let message = error.to_string();
                log_rpc_failure(
                    "resolve",
                    &request_path,
                    "invalid_argument",
                    &message,
                    started.elapsed(),
                );
                Status::invalid_argument(message)
            })?;
        let inode = match self.catalog.lock().await.resolve_path(&path).cloned() {
            Some(inode) => inode_to_proto(inode),
            None => {
                log_rpc_failure(
                    "resolve",
                    &path,
                    "not_found",
                    "path not found",
                    started.elapsed(),
                );
                return Err(Status::not_found("path not found"));
            }
        };
        log_slow_rpc("resolve", &path, started.elapsed());
        Ok(Response::new(ResolveResponse { inode: Some(inode) }))
    }

    async fn fetch(
        &self,
        request: Request<FetchRequest>,
    ) -> Result<Response<Self::FetchStream>, Status> {
        let rpc_started = Instant::now();
        let request = request.into_inner();
        let requested_extents = request.extents.len();
        let catalog = self.catalog.lock().await;
        let mut records = Vec::with_capacity(request.extents.len());
        let mut response_bytes = 0_usize;
        for extent in request.extents {
            let Some(inode) = catalog.resolve_file_id(FileId(extent.file_id)) else {
                log_fetch_failure(
                    "file id not found",
                    requested_extents,
                    rpc_started.elapsed(),
                );
                return Err(Status::not_found("file id not found"));
            };
            if inode.inode_generation != extent.inode_generation {
                log_fetch_failure(
                    "stale inode generation",
                    requested_extents,
                    rpc_started.elapsed(),
                );
                return Err(Status::failed_precondition("stale inode generation"));
            }
            let Some(catalog_extent) = inode.extents.iter().find(|candidate| {
                candidate.extent_index == extent.extent_index
                    && candidate.file_offset == extent.file_offset
                    && candidate.length == extent.length
                    && (extent.extent_hash.is_empty()
                        || candidate.payload_hash == extent.extent_hash)
            }) else {
                log_fetch_failure("extent not found", requested_extents, rpc_started.elapsed());
                return Err(Status::not_found("extent not found"));
            };
            let started = Instant::now();
            let data = match catalog.read_extent_payload(catalog_extent) {
                Ok(data) => data,
                Err(error) => {
                    let status = map_catalog_error(error);
                    log_fetch_failure(status.message(), requested_extents, rpc_started.elapsed());
                    return Err(status);
                }
            };
            if let Some(metrics) = &self.metrics {
                metrics.record_extent_fetch(
                    crate::ExtentFetchSource::CacheHit,
                    data.len(),
                    started.elapsed().as_nanos() as u64,
                );
            }
            response_bytes = response_bytes.saturating_add(data.len());
            records.push(ExtentRecord {
                file_id: inode.file_id.0,
                extent_index: catalog_extent.extent_index,
                file_offset: catalog_extent.file_offset,
                extent_hash: catalog_extent.payload_hash.clone(),
                transfer_class: catalog_extent.transfer_class,
                data,
            });
        }
        let stream = tokio_stream::iter(records.into_iter().map(Ok));
        log_fetch_rpc(requested_extents, response_bytes, rpc_started.elapsed());
        Ok(Response::new(Box::pin(stream)))
    }

    async fn hint(&self, request: Request<HintRequest>) -> Result<Response<HintResponse>, Status> {
        let request = request.into_inner();
        Ok(Response::new(HintResponse {
            accepted: request.extents,
            completed: Vec::new(),
        }))
    }

    async fn report_client_metrics(
        &self,
        request: Request<ReportClientMetricsRequest>,
    ) -> Result<Response<ReportClientMetricsResponse>, Status> {
        let request = request.into_inner();
        let samples = request
            .samples
            .iter()
            .map(reported_metric_to_sample)
            .collect::<Result<Vec<_>, _>>()?;
        let accepted_samples = self.metrics.as_ref().map_or(0, |metrics| {
            metrics.record_client_snapshot(&request.client_name, &samples)
        });
        tracing::info!(
            client_name = request.client_name.as_str(),
            samples = samples.len(),
            accepted_samples,
            "client metrics report accepted"
        );
        Ok(Response::new(ReportClientMetricsResponse {
            accepted_samples: accepted_samples as u32,
        }))
    }

    async fn subscribe_changes(
        &self,
        request: Request<SubscribeChangesRequest>,
    ) -> Result<Response<Self::SubscribeChangesStream>, Status> {
        let since_sequence = request.into_inner().since_sequence;
        let (initial_records, initial_checkpoint, mut next_sequence) = {
            let catalog = self.catalog.lock().await;
            let records = catalog
                .change_records_since(since_sequence)
                .map_err(map_catalog_error)?;
            let current_sequence = catalog.last_sequence();
            let checkpoint = ChangeRecord {
                sequence: current_sequence,
                kind: legato_proto::ChangeKind::Checkpoint as i32,
                file_id: 0,
                path: format!("checkpoint:{current_sequence}"),
                inode: None,
                entries: Vec::new(),
            };
            (records, checkpoint, current_sequence)
        };
        let catalog = Arc::clone(&self.catalog);
        let (sender, receiver) = mpsc::channel(16);
        let _task = tokio::spawn(async move {
            let mut ticker = interval(Duration::from_millis(250));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                let records = {
                    let catalog = catalog.lock().await;
                    catalog
                        .change_records_since(next_sequence)
                        .map_err(map_catalog_error)
                };
                match records {
                    Ok(records) => {
                        for record in records {
                            next_sequence = record.sequence;
                            if sender.send(Ok(record)).await.is_err() {
                                return;
                            }
                        }
                    }
                    Err(status) => {
                        let _ = sender.send(Err(status)).await;
                        return;
                    }
                }
                ticker.tick().await;
            }
        });
        let stream = tokio_stream::iter(initial_records.into_iter().map(Ok))
            .chain(tokio_stream::iter(std::iter::once(Ok(initial_checkpoint))))
            .chain(ReceiverStream::new(receiver));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn stat(&self, request: Request<StatRequest>) -> Result<Response<StatResponse>, Status> {
        let started = Instant::now();
        let request_path = request.into_inner().path;
        let path = logical_request_path(Path::new(&self.config.library_root), &request_path)
            .map_err(|error| {
                let message = error.to_string();
                log_rpc_failure(
                    "stat",
                    &request_path,
                    "invalid_argument",
                    &message,
                    started.elapsed(),
                );
                Status::invalid_argument(message)
            })?;
        let Some(inode) = self.catalog.lock().await.resolve_path(&path).cloned() else {
            log_rpc_failure(
                "stat",
                &path,
                "not_found",
                "path not found",
                started.elapsed(),
            );
            return Err(Status::not_found("path not found"));
        };
        let response = StatResponse {
            metadata: Some(catalog_inode_to_metadata(inode)),
        };
        log_slow_rpc("stat", &path, started.elapsed());
        Ok(Response::new(response))
    }

    async fn list_dir(
        &self,
        request: Request<ListDirRequest>,
    ) -> Result<Response<ListDirResponse>, Status> {
        let started = Instant::now();
        let request_path = request.into_inner().path;
        let path = logical_request_path(Path::new(&self.config.library_root), &request_path)
            .map_err(|error| {
                let message = error.to_string();
                log_rpc_failure(
                    "list_dir",
                    &request_path,
                    "invalid_argument",
                    &message,
                    started.elapsed(),
                );
                Status::invalid_argument(message)
            })?;
        let Some(entries) = self.catalog.lock().await.list_directory(&path) else {
            log_rpc_failure(
                "list_dir",
                &path,
                "not_found",
                "directory not found",
                started.elapsed(),
            );
            return Err(Status::not_found("directory not found"));
        };
        let response = ListDirResponse {
            entries: entries
                .into_iter()
                .map(|entry| DirectoryEntry {
                    name: entry.name,
                    path: entry.path,
                    is_dir: entry.is_dir,
                    file_id: entry.file_id.0,
                })
                .collect(),
        };
        log_slow_rpc_with_count("list_dir", &path, response.entries.len(), started.elapsed());
        Ok(Response::new(response))
    }

    async fn resolve_path(
        &self,
        request: Request<ResolvePathRequest>,
    ) -> Result<Response<ResolvePathResponse>, Status> {
        let started = Instant::now();
        let request_path = request.into_inner().path;
        let path = logical_request_path(Path::new(&self.config.library_root), &request_path)
            .map_err(|error| {
                let message = error.to_string();
                log_rpc_failure(
                    "resolve_path",
                    &request_path,
                    "invalid_argument",
                    &message,
                    started.elapsed(),
                );
                Status::invalid_argument(message)
            })?;
        let Some(inode) = self.catalog.lock().await.resolve_path(&path).cloned() else {
            log_rpc_failure(
                "resolve_path",
                &path,
                "not_found",
                "path not found",
                started.elapsed(),
            );
            return Err(Status::not_found("path not found"));
        };
        let response = ResolvePathResponse {
            metadata: Some(catalog_inode_to_metadata(inode)),
        };
        log_slow_rpc("resolve_path", &path, started.elapsed());
        Ok(Response::new(response))
    }

    async fn subscribe(
        &self,
        _request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let subscription = self.invalidations.lock().await.subscribe();
        let hub = Arc::clone(&self.invalidations);
        let subscriber_id = subscription.subscriber_id;
        let initial_events = subscription.initial_events;

        let stream = async_stream::try_stream! {
            for event in initial_events {
                yield event;
            }

            let mut ticker = interval(Duration::from_millis(250));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                let events = {
                    let mut hub = hub.lock().await;
                    match hub.drain(subscriber_id) {
                        Some(events) => events,
                        None => break,
                    }
                };
                for event in events {
                    yield event;
                }
            }
        };

        Ok(Response::new(Box::pin(stream)))
    }
}

/// Loads the PEM materials needed by tonic's transport TLS configuration.
pub fn load_runtime_tls(
    config: &crate::ServerTlsConfig,
) -> Result<RuntimeTlsConfig, std::io::Error> {
    Ok(RuntimeTlsConfig {
        server_cert_pem: std::fs::read(&config.cert_path)?,
        server_key_pem: std::fs::read(&config.key_path)?,
        client_ca_pem: std::fs::read(&config.client_ca_path)?,
    })
}

/// Parses the configured listener bind address.
pub fn parse_bind_address(address: &str) -> Result<SocketAddr, std::net::AddrParseError> {
    address.parse()
}

fn map_catalog_error(error: legato_client_cache::catalog::CatalogStoreError) -> Status {
    Status::internal(error.to_string())
}

fn catalog_inode_to_metadata(inode: CatalogInode) -> FileMetadata {
    FileMetadata {
        file_id: inode.file_id.0,
        path: inode.path,
        size: inode.size,
        mtime_ns: inode.mtime_ns as u64,
        content_hash: inode.content_hash,
        is_dir: inode.is_dir,
    }
}

fn reported_metric_to_sample(metric: &ReportedMetric) -> Result<MetricSample, Status> {
    let kind = match MetricKind::try_from(metric.kind).unwrap_or(MetricKind::Unspecified) {
        MetricKind::Counter => LocalMetricKind::Counter,
        MetricKind::Gauge => LocalMetricKind::Gauge,
        MetricKind::Unspecified => {
            return Err(Status::invalid_argument("reported metric kind is required"));
        }
    };
    Ok(MetricSample {
        name: metric.name.clone(),
        kind,
        help: metric.help.clone(),
        labels: metric
            .labels
            .iter()
            .map(|label| (label.key.clone(), label.value.clone()))
            .collect::<BTreeMap<_, _>>(),
        value: metric.value,
    })
}

fn log_slow_rpc(operation: &'static str, path: &str, elapsed: Duration) {
    tracing::debug!(
        operation,
        path,
        elapsed_ms = elapsed.as_millis() as u64,
        "server rpc"
    );
    if elapsed >= SLOW_RPC_WARN_AFTER {
        tracing::warn!(
            operation,
            path,
            elapsed_ms = elapsed.as_millis() as u64,
            "slow server rpc"
        );
    }
}

fn log_rpc_failure(
    operation: &'static str,
    path: &str,
    status: &'static str,
    error: &str,
    elapsed: Duration,
) {
    if status == "not_found" {
        tracing::debug!(
            operation,
            path,
            status,
            error,
            elapsed_ms = elapsed.as_millis() as u64,
            "server rpc failed"
        );
    } else {
        tracing::warn!(
            operation,
            path,
            status,
            error,
            elapsed_ms = elapsed.as_millis() as u64,
            "server rpc failed"
        );
    }
}

fn log_slow_rpc_with_count(operation: &'static str, path: &str, count: usize, elapsed: Duration) {
    tracing::info!(
        operation,
        path,
        count,
        elapsed_ms = elapsed.as_millis() as u64,
        "server rpc"
    );
    if elapsed >= SLOW_RPC_WARN_AFTER {
        tracing::warn!(
            operation,
            path,
            count,
            elapsed_ms = elapsed.as_millis() as u64,
            "slow server rpc"
        );
    }
}

fn log_fetch_failure(error: &str, requested_extents: usize, elapsed: Duration) {
    tracing::warn!(
        operation = "fetch",
        requested_extents,
        error,
        elapsed_ms = elapsed.as_millis() as u64,
        "server rpc failed"
    );
}

fn log_fetch_rpc(extents: usize, bytes: usize, elapsed: Duration) {
    tracing::info!(
        operation = "fetch",
        extents,
        bytes,
        elapsed_ms = elapsed.as_millis() as u64,
        "server rpc"
    );
    if elapsed >= SLOW_RPC_WARN_AFTER {
        tracing::warn!(
            operation = "fetch",
            extents,
            bytes,
            elapsed_ms = elapsed.as_millis() as u64,
            "slow server rpc"
        );
    }
}

fn log_reconcile_summary(phase: &'static str, stats: &crate::ReconcileStats, elapsed: Duration) {
    let changed_records = stats
        .directories_created
        .saturating_add(stats.directories_updated)
        .saturating_add(stats.directories_deleted)
        .saturating_add(stats.files_created)
        .saturating_add(stats.files_updated)
        .saturating_add(stats.files_deleted);
    tracing::info!(
        phase,
        directories_created = stats.directories_created,
        directories_updated = stats.directories_updated,
        directories_deleted = stats.directories_deleted,
        files_created = stats.files_created,
        files_updated = stats.files_updated,
        files_deleted = stats.files_deleted,
        changed_records,
        elapsed_ms = elapsed.as_millis() as u64,
        "server library reconcile completed"
    );
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path, sync::Arc, time::Duration};

    use tokio::net::TcpListener;
    use tokio::sync::{Mutex, mpsc};
    use tokio_stream::StreamExt;
    use tonic::transport::{Channel, ClientTlsConfig};

    use legato_client_cache::catalog::CatalogStore;
    use legato_proto::{
        AttachRequest, Capability, ExtentRef, FetchRequest, ResolvePathRequest, ResolveRequest,
        StatRequest, SubscribeChangesRequest, TransferClass, legato_client::LegatoClient,
    };
    use tempfile::tempdir;

    use super::{LiveServer, load_runtime_tls, spawn_watch_task};
    use crate::{
        InvalidationHub, ServerConfig, ensure_server_tls_materials, issue_client_tls_bundle,
        reconcile_library_root_to_store,
    };

    #[tokio::test]
    async fn grpc_runtime_serves_attach_and_metadata_requests() {
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
            tls: crate::ServerTlsConfig::local_dev(&tls_dir),
            bootstrap: Default::default(),
        };
        config.tls.server_names = vec![String::from("127.0.0.1"), String::from("localhost")];
        ensure_server_tls_materials(Path::new(&config.tls_dir), &config.tls)
            .expect("tls materials should be created");

        let server = LiveServer::bootstrap(config.clone()).expect("server should bootstrap");
        let tls = load_runtime_tls(&config.tls).expect("runtime tls should load");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let address = listener
            .local_addr()
            .expect("listener addr should be available");
        let bound = server
            .bind(listener, Some(tls))
            .await
            .expect("server should bind");

        let bundle_dir = fixture.path().join("bundle");
        crate::issue_client_tls_bundle(
            Path::new(&config.tls_dir),
            &config.tls,
            "client-test",
            &bundle_dir,
        )
        .expect("client bundle should be issued");

        let channel = Channel::from_shared(format!("https://{address}"))
            .expect("channel uri should be valid")
            .tls_config(
                ClientTlsConfig::new()
                    .ca_certificate(tonic::transport::Certificate::from_pem(
                        fs::read(bundle_dir.join("server-ca.pem")).expect("server ca should load"),
                    ))
                    .identity(tonic::transport::Identity::from_pem(
                        fs::read(bundle_dir.join("client.pem")).expect("client cert should load"),
                        fs::read(bundle_dir.join("client-key.pem"))
                            .expect("client key should load"),
                    ))
                    .domain_name("localhost"),
            )
            .expect("tls config should be valid")
            .connect()
            .await
            .expect("client should connect");
        let mut client = LegatoClient::new(channel);

        let attach = client
            .attach(AttachRequest {
                protocol_version: legato_proto::PROTOCOL_VERSION,
                client_name: String::from("test-client"),
                desired_capabilities: vec![Capability::Metadata as i32],
            })
            .await
            .expect("attach should succeed")
            .into_inner();
        assert_eq!(attach.server_name, "legato-server");
        assert_eq!(
            attach.negotiated_capabilities,
            vec![Capability::Metadata as i32]
        );

        let stat = client
            .stat(StatRequest {
                path: sample_path.to_string_lossy().into_owned(),
            })
            .await
            .expect("stat should succeed")
            .into_inner();
        let stat_metadata = stat.metadata.expect("metadata should be present");
        assert_eq!(stat_metadata.path, "/Kontakt/piano.nki");
        assert_eq!(
            stat_metadata.content_hash,
            blake3::hash(b"hello legato").as_bytes()
        );

        let inode = client
            .resolve(ResolveRequest {
                path: String::from("/Kontakt/piano.nki"),
            })
            .await
            .expect("resolve should succeed")
            .into_inner()
            .inode
            .expect("inode should be present");
        let layout = inode.layout.expect("layout should be present");
        let extent = layout
            .extents
            .first()
            .expect("sample file should have an extent");
        let extents = client
            .fetch(FetchRequest {
                extents: vec![ExtentRef {
                    file_id: inode.file_id,
                    extent_index: extent.extent_index,
                    file_offset: extent.file_offset,
                    length: extent.length,
                    inode_generation: inode.inode_generation,
                    extent_hash: extent.extent_hash.clone(),
                }],
            })
            .await
            .expect("fetch should succeed")
            .into_inner()
            .collect::<Result<Vec<_>, _>>()
            .await
            .expect("extent stream should collect");
        assert_eq!(extents.len(), 1);
        assert_eq!(extents[0].data, b"hello legato");

        let resolved = client
            .resolve_path(ResolvePathRequest {
                path: String::from("/Kontakt/piano.nki"),
            })
            .await
            .expect("resolve should succeed")
            .into_inner();
        assert_eq!(
            resolved.metadata.expect("metadata should be present").path,
            "/Kontakt/piano.nki"
        );

        bound
            .shutdown()
            .await
            .expect("server should shut down cleanly");
    }

    #[tokio::test]
    async fn grpc_runtime_serves_reset_fetch_and_change_stream() {
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
            tls: crate::ServerTlsConfig::local_dev(&tls_dir),
            bootstrap: Default::default(),
        };
        config.tls.server_names = vec![String::from("127.0.0.1"), String::from("localhost")];
        ensure_server_tls_materials(Path::new(&config.tls_dir), &config.tls)
            .expect("tls materials should be created");

        let server = LiveServer::bootstrap(config.clone()).expect("server should bootstrap");
        let tls = load_runtime_tls(&config.tls).expect("runtime tls should load");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let address = listener
            .local_addr()
            .expect("listener addr should be available");
        let bound = server
            .bind(listener, Some(tls))
            .await
            .expect("server should bind");

        let bundle_dir = fixture.path().join("bundle");
        crate::issue_client_tls_bundle(
            Path::new(&config.tls_dir),
            &config.tls,
            "client-test",
            &bundle_dir,
        )
        .expect("client bundle should be issued");

        let channel = Channel::from_shared(format!("https://{address}"))
            .expect("channel uri should be valid")
            .tls_config(
                ClientTlsConfig::new()
                    .ca_certificate(tonic::transport::Certificate::from_pem(
                        fs::read(bundle_dir.join("server-ca.pem")).expect("server ca should load"),
                    ))
                    .identity(tonic::transport::Identity::from_pem(
                        fs::read(bundle_dir.join("client.pem")).expect("client cert should load"),
                        fs::read(bundle_dir.join("client-key.pem"))
                            .expect("client key should load"),
                    ))
                    .domain_name("localhost"),
            )
            .expect("tls config should be valid")
            .connect()
            .await
            .expect("client should connect");
        let mut client = LegatoClient::new(channel);

        let resolved = client
            .resolve(ResolveRequest {
                path: String::from("/Kontakt/piano.nki"),
            })
            .await
            .expect("resolve should succeed")
            .into_inner();
        let inode = resolved.inode.expect("inode should be present");
        let layout = inode.layout.expect("layout should be present");
        assert_ne!(inode.file_id, 0);
        assert_eq!(layout.transfer_class, TransferClass::Unitary as i32);
        assert_eq!(layout.extents.len(), 1);

        let fetched = client
            .fetch(FetchRequest {
                extents: vec![ExtentRef {
                    file_id: inode.file_id,
                    extent_index: 0,
                    file_offset: 0,
                    length: inode.size,
                    inode_generation: inode.inode_generation,
                    extent_hash: layout.extents[0].extent_hash.clone(),
                }],
            })
            .await
            .expect("fetch should succeed")
            .into_inner()
            .collect::<Result<Vec<_>, _>>()
            .await
            .expect("extent stream should collect");
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].data, b"hello legato");

        let mut stream = client
            .subscribe_changes(SubscribeChangesRequest { since_sequence: 0 })
            .await
            .expect("subscribe changes should succeed")
            .into_inner();
        let first = stream
            .message()
            .await
            .expect("change stream should yield")
            .expect("first record should exist");
        let second = stream
            .message()
            .await
            .expect("change stream should yield")
            .expect("second record should exist");
        assert_eq!(first.kind, legato_proto::ChangeKind::Upsert as i32);
        assert_eq!(second.kind, legato_proto::ChangeKind::Upsert as i32);
        assert!(second.sequence > first.sequence);

        bound
            .shutdown()
            .await
            .expect("server should shut down cleanly");
    }

    #[tokio::test]
    async fn watch_task_reconciles_new_file_and_publishes_invalidation() {
        let fixture = tempdir().expect("tempdir should be created");
        let library_root = fixture.path().join("library");
        fs::create_dir_all(library_root.join("Kontakt")).expect("library tree should be created");
        let existing_path = library_root.join("Kontakt").join("existing.nki");
        fs::write(&existing_path, b"existing").expect("existing file should be written");

        let state_dir = fixture.path().join("state");
        reconcile_library_root_to_store(&state_dir, &library_root)
            .expect("initial canonical reconcile should succeed");

        let catalog = Arc::new(Mutex::new(
            CatalogStore::open(&state_dir, 0).expect("catalog should open"),
        ));
        let invalidations = Arc::new(Mutex::new(InvalidationHub::new(String::from("/"))));
        let subscription = invalidations.lock().await.subscribe();
        let (sender, receiver) = mpsc::unbounded_channel();
        let watch_task = spawn_watch_task(
            receiver,
            library_root.clone(),
            state_dir.clone(),
            Arc::clone(&catalog),
            Arc::clone(&invalidations),
        );

        let new_path = library_root.join("Kontakt").join("new.nki");
        fs::write(&new_path, b"new file").expect("new file should be written");
        sender
            .send(Ok(notify::Event {
                kind: notify::EventKind::Create(notify::event::CreateKind::File),
                paths: vec![new_path.clone()],
                attrs: Default::default(),
            }))
            .expect("event should be sent");

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                {
                    let catalog = catalog.lock().await;
                    if catalog.resolve_path("/Kontakt/new.nki").is_some() {
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("watch task should reconcile the new path");

        let queued = invalidations
            .lock()
            .await
            .drain(subscription.subscriber_id)
            .expect("subscriber should exist");
        assert!(
            queued.iter().any(|event| event.path == "/"),
            "expected root invalidation for /"
        );

        watch_task.abort();
    }

    #[tokio::test]
    async fn subscribe_changes_yields_checkpoint_boundary_when_caught_up() {
        let fixture = tempdir().expect("tempdir should be created");
        let library_root = fixture.path().join("library");
        let state_dir = fixture.path().join("state");
        let tls_dir = fixture.path().join("tls");
        fs::create_dir_all(library_root.join("Kontakt")).expect("library tree should be created");
        fs::write(
            library_root.join("Kontakt").join("piano.nki"),
            b"hello legato",
        )
        .expect("sample should be written");

        let mut config = ServerConfig {
            bind_address: String::from("127.0.0.1:0"),
            library_root: library_root.to_string_lossy().into_owned(),
            state_dir: state_dir.to_string_lossy().into_owned(),
            tls_dir: tls_dir.to_string_lossy().into_owned(),
            tls: crate::ServerTlsConfig::local_dev(&tls_dir),
            bootstrap: Default::default(),
        };
        config.tls.server_names = vec![String::from("127.0.0.1"), String::from("localhost")];
        ensure_server_tls_materials(Path::new(&config.tls_dir), &config.tls)
            .expect("tls materials should be created");
        let runtime_tls = load_runtime_tls(&config.tls).expect("runtime tls should load");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let address = listener.local_addr().expect("listener addr should exist");
        let server = LiveServer::bootstrap(config.clone()).expect("server should bootstrap");
        let bound = server
            .bind(listener, Some(runtime_tls))
            .await
            .expect("server should bind");

        let bundle_dir = fixture.path().join("bundle");
        issue_client_tls_bundle(
            Path::new(&config.tls_dir),
            &config.tls,
            "studio-checkpoint",
            &bundle_dir,
        )
        .expect("client bundle should be issued");

        let channel = tonic::transport::Endpoint::from_shared(format!("https://{}", address))
            .expect("endpoint should parse")
            .tls_config(
                tonic::transport::ClientTlsConfig::new()
                    .ca_certificate(tonic::transport::Certificate::from_pem(
                        fs::read(bundle_dir.join("server-ca.pem")).expect("ca should exist"),
                    ))
                    .identity(tonic::transport::Identity::from_pem(
                        fs::read(bundle_dir.join("client.pem")).expect("client cert should exist"),
                        fs::read(bundle_dir.join("client-key.pem"))
                            .expect("client key should exist"),
                    ))
                    .domain_name("localhost"),
            )
            .expect("tls config should be valid")
            .connect()
            .await
            .expect("client should connect");
        let mut client = LegatoClient::new(channel);

        let current_sequence = CatalogStore::open(&state_dir, 0)
            .expect("catalog should reopen")
            .last_sequence();
        assert!(current_sequence > 0, "baseline sequence should exist");

        let mut stream = client
            .subscribe_changes(SubscribeChangesRequest {
                since_sequence: current_sequence,
            })
            .await
            .expect("caught-up subscribe should succeed")
            .into_inner();
        let mut saw_response = false;
        let mut saw_boundary = false;
        for _ in 0..4 {
            let Some(record) = tokio::time::timeout(Duration::from_secs(5), stream.message())
                .await
                .expect("stream should respond promptly")
                .expect("stream read should succeed")
            else {
                break;
            };
            saw_response = true;
            saw_boundary |= record.kind == legato_proto::ChangeKind::Checkpoint as i32;
            if saw_boundary {
                break;
            }
        }
        assert!(saw_response, "caught-up replay should respond promptly");
        assert!(
            saw_boundary,
            "caught-up replay should surface a checkpoint boundary"
        );

        bound
            .shutdown()
            .await
            .expect("server should shut down cleanly");
    }

    #[tokio::test]
    async fn subscribe_changes_yields_checkpoint_boundary_after_initial_backlog() {
        let fixture = tempdir().expect("tempdir should be created");
        let library_root = fixture.path().join("library");
        let state_dir = fixture.path().join("state");
        let tls_dir = fixture.path().join("tls");
        fs::create_dir_all(library_root.join("Kontakt")).expect("library tree should be created");
        fs::write(
            library_root.join("Kontakt").join("piano.nki"),
            b"hello legato",
        )
        .expect("sample should be written");

        let mut config = ServerConfig {
            bind_address: String::from("127.0.0.1:0"),
            library_root: library_root.to_string_lossy().into_owned(),
            state_dir: state_dir.to_string_lossy().into_owned(),
            tls_dir: tls_dir.to_string_lossy().into_owned(),
            tls: crate::ServerTlsConfig::local_dev(&tls_dir),
            bootstrap: Default::default(),
        };
        config.tls.server_names = vec![String::from("127.0.0.1"), String::from("localhost")];
        ensure_server_tls_materials(Path::new(&config.tls_dir), &config.tls)
            .expect("tls materials should be created");
        let runtime_tls = load_runtime_tls(&config.tls).expect("runtime tls should load");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let address = listener.local_addr().expect("listener addr should exist");
        let server = LiveServer::bootstrap(config.clone()).expect("server should bootstrap");
        let bound = server
            .bind(listener, Some(runtime_tls))
            .await
            .expect("server should bind");

        let bundle_dir = fixture.path().join("bundle");
        issue_client_tls_bundle(
            Path::new(&config.tls_dir),
            &config.tls,
            "studio-backlog",
            &bundle_dir,
        )
        .expect("client bundle should be issued");

        let channel = tonic::transport::Endpoint::from_shared(format!("https://{}", address))
            .expect("endpoint should parse")
            .tls_config(
                tonic::transport::ClientTlsConfig::new()
                    .ca_certificate(tonic::transport::Certificate::from_pem(
                        fs::read(bundle_dir.join("server-ca.pem")).expect("ca should exist"),
                    ))
                    .identity(tonic::transport::Identity::from_pem(
                        fs::read(bundle_dir.join("client.pem")).expect("client cert should exist"),
                        fs::read(bundle_dir.join("client-key.pem"))
                            .expect("client key should exist"),
                    ))
                    .domain_name("localhost"),
            )
            .expect("tls config should be valid")
            .connect()
            .await
            .expect("client should connect");
        let mut client = LegatoClient::new(channel);

        let mut stream = client
            .subscribe_changes(SubscribeChangesRequest { since_sequence: 0 })
            .await
            .expect("backlog subscribe should succeed")
            .into_inner();
        let mut saw_upsert = false;
        let mut saw_boundary = false;
        for _ in 0..32 {
            let Some(record) = tokio::time::timeout(Duration::from_secs(5), stream.message())
                .await
                .expect("stream should respond promptly")
                .expect("stream read should succeed")
            else {
                break;
            };
            saw_upsert |= record.kind == legato_proto::ChangeKind::Upsert as i32;
            saw_boundary |= record.kind == legato_proto::ChangeKind::Checkpoint as i32;
            if saw_upsert && saw_boundary {
                break;
            }
        }
        assert!(saw_upsert, "backlog replay should include current upserts");
        assert!(
            saw_boundary,
            "backlog replay should surface a checkpoint boundary"
        );

        bound
            .shutdown()
            .await
            .expect("server should shut down cleanly");
    }
}
