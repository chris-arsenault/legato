//! Live gRPC client transport used by native Legato clients.

use std::{fs, time::Duration};

use legato_foundation::{MetricKind as LocalMetricKind, MetricSample};
use tokio::{
    sync::mpsc,
    task::JoinHandle,
    time::{sleep, timeout},
};
use tonic::{
    Status,
    transport::{
        Certificate, Channel, ClientTlsConfig as TonicClientTlsConfig, Endpoint, Identity,
    },
};

use crate::{ClientConfig, ClientRuntime, ClientTlsConfig, ClientTlsError, RecoveryCompletion};
use legato_proto::{
    AttachResponse, ChangeKind, ChangeRecord, DirectoryEntry, ExtentRecord, ExtentRef,
    FetchRequest, FileMetadata, InodeMetadata, InvalidationEvent, ListDirRequest, MetricKind,
    MetricLabel, PROTOCOL_VERSION, ReportClientMetricsRequest, ReportedMetric, ResolvePathRequest,
    ResolveRequest, StatRequest, SubscribeChangesRequest, SubscribeRequest,
    legato_client::LegatoClient,
};

/// Session metadata returned after a successful attach.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientAttachSession {
    /// Server name reported by the remote daemon.
    pub server_name: String,
    /// Capabilities negotiated for the current connection.
    pub negotiated_capabilities: Vec<i32>,
}

/// Streaming invalidation subscription tied to one live gRPC session.
#[derive(Debug)]
pub struct GrpcInvalidationSubscription {
    receiver: mpsc::Receiver<InvalidationDelivery>,
    task: JoinHandle<()>,
}

/// Streaming change subscription tied to one live gRPC session.
#[derive(Debug)]
pub struct GrpcChangeSubscription {
    receiver: mpsc::Receiver<ChangeDelivery>,
    task: JoinHandle<()>,
}

#[derive(Debug)]
enum InvalidationDelivery {
    Event(InvalidationEvent),
    Closed,
    Error(ClientTransportError),
}

#[derive(Debug)]
enum ChangeDelivery {
    Record(ChangeRecord),
    Closed,
    Error(ClientTransportError),
}

/// Non-blocking invalidation poll result.
#[derive(Debug)]
pub enum InvalidationPoll {
    /// One invalidation event is ready.
    Event(InvalidationEvent),
    /// The stream is still open but currently idle.
    Empty,
    /// The remote side closed the subscription.
    Closed,
}

/// Non-blocking change poll result.
#[derive(Debug)]
pub enum ChangePoll {
    /// One ordered change record is ready.
    Record(ChangeRecord),
    /// The stream is still open but currently idle.
    Empty,
    /// The remote side closed the subscription.
    Closed,
}

impl GrpcInvalidationSubscription {
    /// Receives the next invalidation from the remote stream.
    pub async fn recv_next(&mut self) -> Result<Option<InvalidationEvent>, ClientTransportError> {
        match self.receiver.recv().await {
            Some(InvalidationDelivery::Event(event)) => Ok(Some(event)),
            Some(InvalidationDelivery::Closed) | None => Ok(None),
            Some(InvalidationDelivery::Error(error)) => Err(error),
        }
    }

    /// Polls the next invalidation without waiting.
    pub fn try_recv_next(&mut self) -> Result<InvalidationPoll, ClientTransportError> {
        match self.receiver.try_recv() {
            Ok(InvalidationDelivery::Event(event)) => Ok(InvalidationPoll::Event(event)),
            Ok(InvalidationDelivery::Closed) => Ok(InvalidationPoll::Closed),
            Ok(InvalidationDelivery::Error(error)) => Err(error),
            Err(mpsc::error::TryRecvError::Empty) => Ok(InvalidationPoll::Empty),
            Err(mpsc::error::TryRecvError::Disconnected) => Ok(InvalidationPoll::Closed),
        }
    }
}

impl Drop for GrpcInvalidationSubscription {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl GrpcChangeSubscription {
    /// Receives the next ordered change record from the remote stream.
    pub async fn recv_next(&mut self) -> Result<Option<ChangeRecord>, ClientTransportError> {
        match self.receiver.recv().await {
            Some(ChangeDelivery::Record(record)) => Ok(Some(record)),
            Some(ChangeDelivery::Closed) | None => Ok(None),
            Some(ChangeDelivery::Error(error)) => Err(error),
        }
    }

    /// Polls the next change record without waiting.
    pub fn try_recv_next(&mut self) -> Result<ChangePoll, ClientTransportError> {
        match self.receiver.try_recv() {
            Ok(ChangeDelivery::Record(record)) => Ok(ChangePoll::Record(record)),
            Ok(ChangeDelivery::Closed) => Ok(ChangePoll::Closed),
            Ok(ChangeDelivery::Error(error)) => Err(error),
            Err(mpsc::error::TryRecvError::Empty) => Ok(ChangePoll::Empty),
            Err(mpsc::error::TryRecvError::Disconnected) => Ok(ChangePoll::Closed),
        }
    }
}

impl Drop for GrpcChangeSubscription {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Errors returned by the live gRPC client transport.
#[derive(Debug)]
pub enum ClientTransportError {
    /// Local mTLS files could not be loaded or parsed.
    Tls(ClientTlsError),
    /// Raw PEM materials required by tonic could not be loaded.
    Io(std::io::Error),
    /// The configured endpoint URI is not valid for tonic.
    InvalidEndpoint(String),
    /// Channel establishment failed before an RPC was issued.
    Transport(tonic::transport::Error),
    /// The remote server rejected an RPC.
    Rpc(Status),
    /// The server negotiated a different protocol version.
    ProtocolVersionMismatch {
        /// Protocol version expected by this client build.
        expected: u32,
        /// Protocol version returned by the remote server.
        actual: u32,
    },
    /// The server omitted required metadata in a response.
    MissingField(&'static str),
}

impl std::fmt::Display for ClientTransportError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tls(error) => write!(formatter, "client TLS configuration failed: {error}"),
            Self::Io(error) => write!(formatter, "client transport file IO failed: {error}"),
            Self::InvalidEndpoint(endpoint) => {
                write!(formatter, "invalid client endpoint URI: {endpoint}")
            }
            Self::Transport(error) => write!(formatter, "client transport failed: {error}"),
            Self::Rpc(error) => write!(formatter, "server RPC failed: {error}"),
            Self::ProtocolVersionMismatch { expected, actual } => write!(
                formatter,
                "server protocol version {actual} did not match client version {expected}"
            ),
            Self::MissingField(field) => write!(formatter, "server response missing {field}"),
        }
    }
}

impl std::error::Error for ClientTransportError {}

impl From<ClientTlsError> for ClientTransportError {
    fn from(value: ClientTlsError) -> Self {
        Self::Tls(value)
    }
}

impl From<std::io::Error> for ClientTransportError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<tonic::transport::Error> for ClientTransportError {
    fn from(value: tonic::transport::Error) -> Self {
        Self::Transport(value)
    }
}

impl From<Status> for ClientTransportError {
    fn from(value: Status) -> Self {
        Self::Rpc(value)
    }
}

/// Stateful live transport wrapper around the generated gRPC client.
#[derive(Debug)]
pub struct GrpcClientTransport {
    client_name: String,
    runtime: ClientRuntime,
    client: LegatoClient<Channel>,
    stream_client: LegatoClient<Channel>,
    attach: ClientAttachSession,
}

impl GrpcClientTransport {
    /// Connects to the configured Legato server over mTLS and completes the attach handshake.
    pub async fn connect(
        config: ClientConfig,
        client_name: impl Into<String>,
    ) -> Result<Self, ClientTransportError> {
        let client_name = client_name.into();
        let mut runtime = ClientRuntime::new(config);
        let mut client = connect_client(runtime.config()).await?;
        let attach = attach_session(&mut client, runtime.attach_request(&client_name)).await?;
        let mut stream_client = connect_client(runtime.config()).await?;
        let _ = attach_session(
            &mut stream_client,
            runtime.attach_request(&format!("{client_name}-stream")),
        )
        .await?;
        runtime.mark_transport_ready(false);

        Ok(Self {
            client_name,
            runtime,
            client,
            stream_client,
            attach,
        })
    }

    /// Returns the active attach session metadata.
    #[must_use]
    pub fn attach_session(&self) -> &ClientAttachSession {
        &self.attach
    }

    /// Returns the current runtime state shell.
    #[must_use]
    pub fn runtime(&self) -> &ClientRuntime {
        &self.runtime
    }

    /// Returns mutable access to the current runtime shell.
    pub fn runtime_mut(&mut self) -> &mut ClientRuntime {
        &mut self.runtime
    }

    /// Re-establishes the channel, re-attaches, and re-opens stale handles.
    pub async fn reconnect(&mut self) -> Result<RecoveryCompletion, ClientTransportError> {
        let delay_ms = self.runtime.mark_transport_unavailable();
        if delay_ms > 0 {
            sleep(Duration::from_millis(delay_ms)).await;
        }

        let plan = self.runtime.reconnect_plan(&self.client_name);
        let mut client = connect_client(self.runtime.config()).await?;
        let attach = attach_session(&mut client, plan.attach).await?;
        let mut stream_client = connect_client(self.runtime.config()).await?;
        let _ = attach_session(
            &mut stream_client,
            self.runtime
                .attach_request(&format!("{}-stream", self.client_name)),
        )
        .await?;
        self.runtime.mark_transport_ready(false);

        let recovery = self.runtime.complete_reconnect(None);
        self.client = client;
        self.stream_client = stream_client;
        self.attach = attach;
        Ok(recovery)
    }

    /// Fetches file metadata by path.
    pub async fn stat(
        &mut self,
        path: impl Into<String>,
    ) -> Result<FileMetadata, ClientTransportError> {
        let response = self
            .client
            .stat(StatRequest { path: path.into() })
            .await?
            .into_inner();
        response
            .metadata
            .ok_or(ClientTransportError::MissingField("stat.metadata"))
    }

    /// Lists one directory by path.
    pub async fn list_dir(
        &mut self,
        path: impl Into<String>,
    ) -> Result<Vec<DirectoryEntry>, ClientTransportError> {
        Ok(self
            .client
            .list_dir(ListDirRequest { path: path.into() })
            .await?
            .into_inner()
            .entries)
    }

    /// Resolves one canonical path through the server metadata index.
    pub async fn resolve_path(
        &mut self,
        path: impl Into<String>,
    ) -> Result<FileMetadata, ClientTransportError> {
        let response = self
            .client
            .resolve_path(ResolvePathRequest { path: path.into() })
            .await?
            .into_inner();
        response
            .metadata
            .ok_or(ClientTransportError::MissingField("resolve_path.metadata"))
    }

    /// Resolves one path to semantic inode metadata and layout.
    pub async fn resolve(
        &mut self,
        path: impl Into<String>,
    ) -> Result<InodeMetadata, ClientTransportError> {
        let path = path.into();
        let response = self
            .client
            .resolve(ResolveRequest { path: path.clone() })
            .await?
            .into_inner();
        response
            .inode
            .ok_or(ClientTransportError::MissingField("resolve.inode"))
    }

    /// Fetches one or more semantic extents from the remote server.
    pub async fn fetch_extents(
        &mut self,
        extents: Vec<ExtentRef>,
    ) -> Result<Vec<ExtentRecord>, ClientTransportError> {
        let mut stream = self
            .client
            .fetch(FetchRequest { extents })
            .await?
            .into_inner();
        let mut records = Vec::new();
        while let Some(record) = stream.message().await? {
            records.push(record);
        }
        Ok(records)
    }

    /// Reports one full client metrics snapshot to the server aggregation surface.
    pub async fn report_metrics(
        &mut self,
        samples: &[MetricSample],
    ) -> Result<(), ClientTransportError> {
        let reported = samples
            .iter()
            .map(|sample| ReportedMetric {
                name: sample.name.clone(),
                kind: match sample.kind {
                    LocalMetricKind::Counter => MetricKind::Counter as i32,
                    LocalMetricKind::Gauge => MetricKind::Gauge as i32,
                },
                help: sample.help.clone(),
                labels: sample
                    .labels
                    .iter()
                    .map(|(key, value)| MetricLabel {
                        key: key.clone(),
                        value: value.clone(),
                    })
                    .collect(),
                value: sample.value,
            })
            .collect();
        self.client
            .report_client_metrics(ReportClientMetricsRequest {
                client_name: self.client_name.clone(),
                samples: reported,
            })
            .await?;
        Ok(())
    }

    /// Loads ordered change records after the supplied sequence cursor.
    pub async fn change_records_since(
        &mut self,
        since_sequence: u64,
    ) -> Result<Vec<ChangeRecord>, ClientTransportError> {
        let mut client = self.stream_client.clone();
        let mut stream = client
            .subscribe_changes(SubscribeChangesRequest { since_sequence })
            .await?
            .into_inner();
        let mut records = Vec::new();
        loop {
            match timeout(Duration::from_secs(5), stream.message()).await {
                Ok(Ok(Some(record))) => {
                    if ChangeKind::try_from(record.kind).unwrap_or(ChangeKind::Unspecified)
                        == ChangeKind::Checkpoint
                    {
                        break;
                    }
                    records.push(record);
                }
                Ok(Ok(None)) => break,
                Ok(Err(error)) => return Err(error.into()),
                Err(_) => break,
            }
        }
        Ok(records)
    }

    /// Subscribes to the live ordered change stream for the current transport generation.
    pub async fn subscribe_changes(
        &mut self,
        since_sequence: u64,
    ) -> Result<GrpcChangeSubscription, ClientTransportError> {
        let mut client = self.stream_client.clone();
        self.runtime.mark_subscription_active();
        let (sender, receiver) = mpsc::channel(16);
        let task = tokio::spawn(async move {
            let mut stream = match client
                .subscribe_changes(SubscribeChangesRequest { since_sequence })
                .await
            {
                Ok(response) => response.into_inner(),
                Err(error) => {
                    let _ = sender
                        .send(ChangeDelivery::Error(ClientTransportError::from(error)))
                        .await;
                    return;
                }
            };
            loop {
                match stream.message().await {
                    Ok(Some(record)) => {
                        if sender.send(ChangeDelivery::Record(record)).await.is_err() {
                            return;
                        }
                    }
                    Ok(None) => {
                        let _ = sender.send(ChangeDelivery::Closed).await;
                        return;
                    }
                    Err(error) => {
                        let _ = sender
                            .send(ChangeDelivery::Error(ClientTransportError::from(error)))
                            .await;
                        return;
                    }
                }
            }
        });
        Ok(GrpcChangeSubscription { receiver, task })
    }

    /// Subscribes to the server invalidation stream for the current transport generation.
    pub async fn subscribe_invalidations(
        &mut self,
    ) -> Result<GrpcInvalidationSubscription, ClientTransportError> {
        let mut stream = self
            .stream_client
            .subscribe(SubscribeRequest {})
            .await?
            .into_inner();
        self.runtime.mark_subscription_active();
        let (sender, receiver) = mpsc::channel(16);
        let task = tokio::spawn(async move {
            loop {
                match stream.message().await {
                    Ok(Some(event)) => {
                        if sender
                            .send(InvalidationDelivery::Event(event))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    Ok(None) => {
                        let _ = sender.send(InvalidationDelivery::Closed).await;
                        return;
                    }
                    Err(error) => {
                        let _ = sender
                            .send(InvalidationDelivery::Error(ClientTransportError::from(
                                error,
                            )))
                            .await;
                        return;
                    }
                }
            }
        });
        Ok(GrpcInvalidationSubscription { receiver, task })
    }
}

async fn connect_client(
    config: &ClientConfig,
) -> Result<LegatoClient<Channel>, ClientTransportError> {
    let endpoint = Endpoint::from_shared(endpoint_uri(&config.endpoint))
        .map_err(|_error| ClientTransportError::InvalidEndpoint(config.endpoint.clone()))?
        .tls_config(load_tonic_tls_config(&config.tls)?)?;
    let channel = endpoint.connect().await?;
    Ok(LegatoClient::new(channel))
}

async fn attach_session(
    client: &mut LegatoClient<Channel>,
    request: legato_proto::AttachRequest,
) -> Result<ClientAttachSession, ClientTransportError> {
    let response = client.attach(request).await?.into_inner();
    ensure_protocol_version(&response)?;

    Ok(ClientAttachSession {
        server_name: response.server_name,
        negotiated_capabilities: response.negotiated_capabilities,
    })
}

fn ensure_protocol_version(response: &AttachResponse) -> Result<(), ClientTransportError> {
    if response.protocol_version != PROTOCOL_VERSION {
        return Err(ClientTransportError::ProtocolVersionMismatch {
            expected: PROTOCOL_VERSION,
            actual: response.protocol_version,
        });
    }
    Ok(())
}

fn endpoint_uri(endpoint: &str) -> String {
    if endpoint.contains("://") {
        return endpoint.to_owned();
    }
    format!("https://{endpoint}")
}

fn load_tonic_tls_config(
    config: &ClientTlsConfig,
) -> Result<TonicClientTlsConfig, ClientTransportError> {
    let _ = crate::build_tls_client_config(config)?;
    let ca_cert = fs::read(&config.ca_cert_path)?;
    let client_cert = fs::read(&config.client_cert_path)?;
    let client_key = fs::read(&config.client_key_path)?;

    Ok(TonicClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(ca_cert))
        .identity(Identity::from_pem(client_cert, client_key))
        .domain_name(config.server_name.clone()))
}

#[cfg(test)]
mod tests {
    use super::endpoint_uri;

    #[test]
    fn endpoint_uri_adds_https_scheme_when_missing() {
        assert_eq!(endpoint_uri("legato.lan:7823"), "https://legato.lan:7823");
        assert_eq!(
            endpoint_uri("https://legato.lan:7823"),
            "https://legato.lan:7823"
        );
    }
}
