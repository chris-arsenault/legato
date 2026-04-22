//! Live gRPC client transport used by native Legato clients.

use std::{fs, time::Duration};

use tokio::time::sleep;
use tonic::{
    Status,
    transport::{
        Certificate, Channel, ClientTlsConfig as TonicClientTlsConfig, Endpoint, Identity,
    },
};

use crate::{ClientConfig, ClientRuntime, ClientTlsConfig, ClientTlsError, RecoveryCompletion};
use legato_proto::{
    AttachResponse, BlockRequest, BlockResponse, CloseRequest, CloseResponse, DirectoryEntry,
    FileMetadata, ListDirRequest, OpenRequest, OpenResponse, PROTOCOL_VERSION, PrefetchRequest,
    PrefetchResponse, ReadBlocksRequest, ResolvePathRequest, StatRequest,
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
        runtime.mark_transport_ready(false);

        Ok(Self {
            client_name,
            runtime,
            client,
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

    /// Re-establishes the channel, re-attaches, and re-opens stale handles.
    pub async fn reconnect(&mut self) -> Result<RecoveryCompletion, ClientTransportError> {
        let delay_ms = self.runtime.mark_transport_unavailable();
        if delay_ms > 0 {
            sleep(Duration::from_millis(delay_ms)).await;
        }

        let plan = self.runtime.reconnect_plan(&self.client_name);
        let mut client = connect_client(self.runtime.config()).await?;
        let attach = attach_session(&mut client, plan.attach).await?;
        self.runtime.mark_transport_ready(false);

        let mut reopened = Vec::with_capacity(plan.reopen_requests.len());
        for request in plan.reopen_requests {
            let path = request.path.clone();
            let response = client.open(request).await?.into_inner();
            reopened.push((path, response));
        }

        let recovery = self.runtime.complete_reconnect(&reopened, None);
        self.client = client;
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

    /// Opens one remote file and records the returned server-local handle.
    pub async fn open(
        &mut self,
        path: impl Into<String>,
    ) -> Result<OpenResponse, ClientTransportError> {
        let path = path.into();
        let response = self
            .client
            .open(OpenRequest { path: path.clone() })
            .await?
            .into_inner();
        self.runtime.record_open_handle(&path, &response);
        Ok(response)
    }

    /// Streams one or more block ranges from the remote server and collects them in order.
    pub async fn read_blocks(
        &mut self,
        ranges: Vec<BlockRequest>,
    ) -> Result<Vec<BlockResponse>, ClientTransportError> {
        let mut stream = self
            .client
            .read_blocks(ReadBlocksRequest { ranges })
            .await?
            .into_inner();
        let mut blocks = Vec::new();
        while let Some(block) = stream.message().await? {
            blocks.push(block);
        }
        Ok(blocks)
    }

    /// Sends a prefetch request to the remote server.
    pub async fn prefetch(
        &mut self,
        request: PrefetchRequest,
    ) -> Result<PrefetchResponse, ClientTransportError> {
        Ok(self.client.prefetch(request).await?.into_inner())
    }

    /// Closes one remote file handle.
    pub async fn close(&mut self, file_handle: u64) -> Result<CloseResponse, ClientTransportError> {
        Ok(self
            .client
            .close(CloseRequest { file_handle })
            .await?
            .into_inner())
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
