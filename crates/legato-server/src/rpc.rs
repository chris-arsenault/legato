//! gRPC runtime wiring for the Legato server daemon.

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use tokio::{
    net::TcpListener,
    sync::{Mutex, mpsc},
    task::JoinHandle,
    time::{MissedTickBehavior, interval},
};
use tokio_stream::{Stream, wrappers::TcpListenerStream};
use tonic::{
    Request, Response, Status,
    transport::{Certificate, Identity, Server as TransportServer, ServerTlsConfig},
};

use legato_proto::{
    AttachRequest, AttachResponse, BlockResponse, CloseRequest, CloseResponse, ListDirRequest,
    ListDirResponse, OpenRequest, OpenResponse, PrefetchRequest, PrefetchResponse,
    ReadBlocksRequest, ResolvePathRequest, ResolvePathResponse, StatRequest, StatResponse,
    SubscribeRequest,
    legato_server::{Legato, LegatoServer},
};

use crate::{
    InvalidationHub, MetadataService, Server, ServerConfig, WatchBackend, create_poll_watcher,
    create_recommended_watcher, open_metadata_database, reconcile_library_root,
};

type BlockStream = Pin<Box<dyn Stream<Item = Result<BlockResponse, Status>> + Send + 'static>>;
type InvalidationStream =
    Pin<Box<dyn Stream<Item = Result<legato_proto::InvalidationEvent, Status>> + Send + 'static>>;

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
}

impl BoundServer {
    /// Signals shutdown and waits for the underlying transport task to exit.
    pub async fn shutdown(self) -> Result<(), Box<dyn std::error::Error>> {
        let _ = self.shutdown_signal.send(());
        self.watch_task.abort();
        drop(self.watcher);
        self.task.await??;
        Ok(())
    }
}

/// Live network-facing Legato server runtime.
#[derive(Debug)]
pub struct LiveServer {
    shell: Server,
    config: ServerConfig,
    metadata: Arc<Mutex<MetadataService>>,
    invalidations: Arc<Mutex<InvalidationHub>>,
}

impl LiveServer {
    /// Bootstraps the live server state from the configured library and state directories.
    pub fn bootstrap(config: ServerConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let database_path = Path::new(&config.state_dir).join("server.sqlite");
        let mut connection = open_metadata_database(&database_path)?;
        reconcile_library_root(&mut connection, Path::new(&config.library_root))?;
        let metadata = MetadataService::new(connection);
        let invalidations = InvalidationHub::new(config.library_root.clone());

        Ok(Self {
            shell: Server::new(config.clone()),
            config,
            metadata: Arc::new(Mutex::new(metadata)),
            invalidations: Arc::new(Mutex::new(invalidations)),
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
            Arc::clone(&self.metadata),
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
    metadata: Arc<Mutex<MetadataService>>,
    invalidations: Arc<Mutex<InvalidationHub>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(result) = receiver.recv().await {
            let invalidation_events = {
                let mut metadata = metadata.lock().await;
                match metadata.apply_notification(&library_root, result) {
                    Ok((_stats, invalidation_events)) => invalidation_events,
                    Err(_error) => continue,
                }
            };

            if !invalidation_events.is_empty() {
                let mut hub = invalidations.lock().await;
                hub.publish_all(invalidation_events);
            }
        }
    })
}

#[tonic::async_trait]
impl Legato for LiveServer {
    type ReadBlocksStream = BlockStream;
    type SubscribeStream = InvalidationStream;

    async fn attach(
        &self,
        _request: Request<AttachRequest>,
    ) -> Result<Response<AttachResponse>, Status> {
        Ok(Response::new(self.shell.attach_response()))
    }

    async fn stat(&self, request: Request<StatRequest>) -> Result<Response<StatResponse>, Status> {
        let response = self
            .metadata
            .lock()
            .await
            .stat(request.into_inner())
            .map_err(map_storage_error)?
            .ok_or_else(|| Status::not_found("path not found"))?;
        Ok(Response::new(response))
    }

    async fn list_dir(
        &self,
        request: Request<ListDirRequest>,
    ) -> Result<Response<ListDirResponse>, Status> {
        let response = self
            .metadata
            .lock()
            .await
            .list_dir(request.into_inner())
            .map_err(map_storage_error)?
            .ok_or_else(|| Status::not_found("directory not found"))?;
        Ok(Response::new(response))
    }

    async fn resolve_path(
        &self,
        request: Request<ResolvePathRequest>,
    ) -> Result<Response<ResolvePathResponse>, Status> {
        let response = self
            .metadata
            .lock()
            .await
            .resolve_path(request.into_inner())
            .map_err(map_storage_error)?
            .ok_or_else(|| Status::not_found("path not found"))?;
        Ok(Response::new(response))
    }

    async fn open(&self, request: Request<OpenRequest>) -> Result<Response<OpenResponse>, Status> {
        let response = self
            .metadata
            .lock()
            .await
            .open(request.into_inner())
            .map_err(map_storage_error)?
            .ok_or_else(|| Status::not_found("file not found"))?;
        Ok(Response::new(response))
    }

    async fn read_blocks(
        &self,
        request: Request<ReadBlocksRequest>,
    ) -> Result<Response<Self::ReadBlocksStream>, Status> {
        let blocks = self
            .metadata
            .lock()
            .await
            .read_blocks(request.into_inner())
            .map_err(map_storage_error)?;
        Ok(Response::new(Box::pin(tokio_stream::iter(
            blocks.into_iter().map(Ok),
        ))))
    }

    async fn prefetch(
        &self,
        request: Request<PrefetchRequest>,
    ) -> Result<Response<PrefetchResponse>, Status> {
        let request = request.into_inner();
        Ok(Response::new(PrefetchResponse {
            accepted: request.ranges,
            completed: Vec::new(),
        }))
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

    async fn close(
        &self,
        request: Request<CloseRequest>,
    ) -> Result<Response<CloseResponse>, Status> {
        let response = self.metadata.lock().await.close(request.into_inner());
        Ok(Response::new(response))
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

fn map_storage_error(error: rusqlite::Error) -> Status {
    match error {
        rusqlite::Error::InvalidParameterName(message) => Status::invalid_argument(message),
        other => Status::internal(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path, sync::Arc, time::Duration};

    use tokio::net::TcpListener;
    use tokio::sync::{Mutex, mpsc};
    use tokio_stream::StreamExt;
    use tonic::transport::{Channel, ClientTlsConfig};

    use legato_proto::{
        AttachRequest, Capability, OpenRequest, ReadBlocksRequest, ResolvePathRequest, StatRequest,
        legato_client::LegatoClient,
    };
    use tempfile::tempdir;

    use super::{LiveServer, load_runtime_tls, spawn_watch_task};
    use crate::{
        InvalidationHub, MetadataService, ServerConfig, ensure_server_tls_materials,
        open_metadata_database, reconcile_library_root,
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

        let stat = client
            .stat(StatRequest {
                path: sample_path.to_string_lossy().into_owned(),
            })
            .await
            .expect("stat should succeed")
            .into_inner();
        assert_eq!(
            stat.metadata.expect("metadata should be present").path,
            sample_path.to_string_lossy()
        );

        let opened = client
            .open(OpenRequest {
                path: sample_path.to_string_lossy().into_owned(),
            })
            .await
            .expect("open should succeed")
            .into_inner();
        let blocks = client
            .read_blocks(ReadBlocksRequest {
                ranges: vec![legato_proto::BlockRequest {
                    file_handle: opened.file_handle,
                    start_offset: 0,
                    block_count: 1,
                }],
            })
            .await
            .expect("read should succeed")
            .into_inner()
            .collect::<Result<Vec<_>, _>>()
            .await
            .expect("block stream should collect");
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].data, b"hello legato");

        let resolved = client
            .resolve_path(ResolvePathRequest {
                path: sample_path.to_string_lossy().into_owned(),
            })
            .await
            .expect("resolve should succeed")
            .into_inner();
        assert_eq!(
            resolved.metadata.expect("metadata should be present").path,
            sample_path.to_string_lossy()
        );

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

        let mut connection =
            open_metadata_database(&fixture.path().join("server.sqlite")).expect("db should open");
        reconcile_library_root(&mut connection, &library_root)
            .expect("initial reconcile should succeed");

        let metadata = Arc::new(Mutex::new(MetadataService::new(connection)));
        let invalidations = Arc::new(Mutex::new(InvalidationHub::new(
            library_root.to_string_lossy().into_owned(),
        )));
        let subscription = invalidations.lock().await.subscribe();
        let (sender, receiver) = mpsc::unbounded_channel();
        let watch_task = spawn_watch_task(
            receiver,
            library_root.clone(),
            Arc::clone(&metadata),
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
                    let metadata = metadata.lock().await;
                    if metadata
                        .stat(StatRequest {
                            path: new_path.to_string_lossy().into_owned(),
                        })
                        .expect("stat should succeed")
                        .is_some()
                    {
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
        let expected_invalidation_path = new_path
            .parent()
            .expect("new file should have a parent directory")
            .to_string_lossy()
            .into_owned();
        assert!(
            queued
                .iter()
                .any(|event| event.path == expected_invalidation_path),
            "expected invalidation for {}",
            expected_invalidation_path
        );

        watch_task.abort();
    }
}
