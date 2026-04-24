//! LAN bootstrap endpoint for zero-touch client registration.

use std::{io, net::SocketAddr, path::Path, time::Duration};

use legato_foundation::ShutdownToken;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    task::JoinHandle,
    time::timeout,
};

use crate::{
    ClientBundleManifest, ClientBundlePayload, ServerConfig, issue_client_tls_bundle_payload,
    parse_bind_address,
};

const DISCOVERY_MAGIC: &[u8] = b"LEGATO_DISCOVER_V1";
const DEFAULT_BOOTSTRAP_PATH: &str = "/v1/client-bundles";
const DEFAULT_DISCOVERY_PORT: u16 = 7825;

/// Server-side client bootstrap configuration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct ClientBootstrapConfig {
    /// Whether the unauthenticated LAN bootstrap service is enabled.
    pub enabled: bool,
    /// HTTP bind address used by installers to request a client bundle.
    pub bind_address: String,
    /// UDP bind address used for LAN discovery.
    pub discovery_bind_address: String,
    /// Optional externally reachable bootstrap URL advertised through discovery.
    pub advertised_bootstrap_url: Option<String>,
    /// Optional endpoint written into issued client configs.
    pub advertised_endpoint: Option<String>,
    /// Optional TLS server name written into issued client configs.
    pub server_name: Option<String>,
}

impl Default for ClientBootstrapConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bind_address: String::from("0.0.0.0:7824"),
            discovery_bind_address: format!("0.0.0.0:{DEFAULT_DISCOVERY_PORT}"),
            advertised_bootstrap_url: None,
            advertised_endpoint: None,
            server_name: None,
        }
    }
}

/// Request body accepted by `POST /v1/client-bundles`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ClientBootstrapRequest {
    /// Logical client name for the issued certificate.
    pub client_name: String,
    /// Optional mount-point preference selected by the installer UI.
    pub mount_point: Option<String>,
    /// Optional virtual library root selected by the installer UI.
    pub library_root: Option<String>,
}

/// UDP discovery response consumed by installers.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ClientBootstrapAdvertisement {
    /// Protocol marker for defensive parsing.
    pub service: String,
    /// Bootstrap protocol version.
    pub version: u32,
    /// HTTP URL for client-bundle requests, when the server can advertise one.
    pub bootstrap_url: Option<String>,
    /// Default TLS server name for generated client configs.
    pub server_name: Option<String>,
}

/// Spawned bootstrap service tasks.
#[derive(Debug)]
pub struct ClientBootstrapServices {
    http_task: JoinHandle<()>,
    discovery_task: JoinHandle<()>,
}

impl ClientBootstrapServices {
    /// Spawns HTTP bootstrap and UDP discovery services when enabled.
    pub async fn spawn(
        config: ServerConfig,
        shutdown: ShutdownToken,
    ) -> Result<Option<Self>, io::Error> {
        if !config.bootstrap.enabled {
            return Ok(None);
        }

        let http_listener = TcpListener::bind(&config.bootstrap.bind_address).await?;
        let discovery_socket = UdpSocket::bind(&config.bootstrap.discovery_bind_address).await?;
        let http_config = config.clone();
        let http_shutdown = shutdown.clone();
        let http_task = tokio::spawn(async move {
            serve_http(http_listener, http_config, http_shutdown).await;
        });
        let discovery_task = tokio::spawn(async move {
            serve_discovery(discovery_socket, config, shutdown).await;
        });

        Ok(Some(Self {
            http_task,
            discovery_task,
        }))
    }

    /// Returns whether both background tasks are still running.
    #[must_use]
    pub fn is_running(&self) -> bool {
        !self.http_task.is_finished() && !self.discovery_task.is_finished()
    }
}

async fn serve_http(listener: TcpListener, config: ServerConfig, shutdown: ShutdownToken) {
    while !shutdown.is_shutdown_requested() {
        let accept = timeout(Duration::from_millis(250), listener.accept()).await;
        let Ok(Ok((stream, peer))) = accept else {
            continue;
        };
        let config = config.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_http_connection(stream, peer, config).await {
                tracing::warn!(error = %error, "client bootstrap request failed");
            }
        });
    }
}

async fn serve_discovery(socket: UdpSocket, config: ServerConfig, shutdown: ShutdownToken) {
    let mut buffer = [0_u8; 512];
    while !shutdown.is_shutdown_requested() {
        let received = timeout(Duration::from_millis(250), socket.recv_from(&mut buffer)).await;
        let Ok(Ok((length, peer))) = received else {
            continue;
        };
        if &buffer[..length] != DISCOVERY_MAGIC {
            continue;
        }
        let advertisement = discovery_advertisement(&config);
        let Ok(payload) = serde_json::to_vec(&advertisement) else {
            continue;
        };
        let _ = socket.send_to(&payload, peer).await;
    }
}

async fn handle_http_connection(
    mut stream: TcpStream,
    peer: SocketAddr,
    config: ServerConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let request = read_http_request(&mut stream).await?;
    let response = route_http_request(&request, peer, &config);
    stream.write_all(response.as_bytes()).await?;
    Ok(())
}

fn route_http_request(request: &HttpRequest, peer: SocketAddr, config: &ServerConfig) -> String {
    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/v1/bootstrap") => {
            let advertisement = discovery_advertisement(config);
            json_response(200, &advertisement)
        }
        ("POST", DEFAULT_BOOTSTRAP_PATH) => match issue_bundle_response(request, peer, config) {
            Ok(payload) => json_response(200, &payload),
            Err(error) => text_response(400, &error.to_string()),
        },
        _ => text_response(404, "not found"),
    }
}

fn issue_bundle_response(
    request: &HttpRequest,
    peer: SocketAddr,
    config: &ServerConfig,
) -> Result<ClientBundlePayload, Box<dyn std::error::Error>> {
    let payload: ClientBootstrapRequest = serde_json::from_slice(&request.body)?;
    if payload.client_name.trim().is_empty() {
        return Err("client_name is required".into());
    }
    let manifest = ClientBundleManifest::for_issue(
        payload.client_name.trim(),
        Some(resolve_client_endpoint(request, peer, config)?),
        resolve_server_name(config),
        payload.mount_point,
        payload.library_root.or_else(|| Some(String::from("/"))),
    );
    Ok(issue_client_tls_bundle_payload(
        Path::new(&config.tls_dir),
        &config.tls,
        manifest,
    )?)
}

fn resolve_client_endpoint(
    request: &HttpRequest,
    peer: SocketAddr,
    config: &ServerConfig,
) -> Result<String, Box<dyn std::error::Error>> {
    if let Some(endpoint) = &config.bootstrap.advertised_endpoint {
        return Ok(endpoint.clone());
    }

    let grpc_port = parse_bind_address(&config.bind_address)?.port();
    let host = request
        .header("host")
        .and_then(|host| {
            host.rsplit_once(':')
                .map(|(host, _port)| host)
                .or(Some(host))
        })
        .filter(|host| !host.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| peer.ip().to_string());
    Ok(format!("{host}:{grpc_port}"))
}

fn resolve_server_name(config: &ServerConfig) -> Option<String> {
    config
        .bootstrap
        .server_name
        .clone()
        .or_else(|| config.tls.server_names.first().cloned())
}

fn discovery_advertisement(config: &ServerConfig) -> ClientBootstrapAdvertisement {
    ClientBootstrapAdvertisement {
        service: String::from("legato"),
        version: 1,
        bootstrap_url: config.bootstrap.advertised_bootstrap_url.clone(),
        server_name: resolve_server_name(config),
    }
}

#[derive(Debug, Eq, PartialEq)]
struct HttpRequest {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl HttpRequest {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _value)| key.eq_ignore_ascii_case(name))
            .map(|(_key, value)| value.as_str())
    }
}

async fn read_http_request(
    stream: &mut TcpStream,
) -> Result<HttpRequest, Box<dyn std::error::Error>> {
    let mut buffer = Vec::new();
    loop {
        let mut chunk = [0_u8; 1024];
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
        if has_complete_http_request(&buffer)? {
            return parse_http_request(buffer);
        }
        if buffer.len() > 64 * 1024 {
            return Err("bootstrap request is too large".into());
        }
    }

    parse_http_request(buffer)
}

fn has_complete_http_request(buffer: &[u8]) -> Result<bool, Box<dyn std::error::Error>> {
    let Some(header_end) = buffer.windows(4).position(|window| window == b"\r\n\r\n") else {
        return Ok(false);
    };
    let header_text = std::str::from_utf8(&buffer[..header_end])?;
    let content_length = header_text
        .lines()
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _value)| name.trim().eq_ignore_ascii_case("content-length"))
        .and_then(|(_name, value)| value.trim().parse::<usize>().ok())
        .unwrap_or(0);
    Ok(buffer.len() >= header_end + 4 + content_length)
}

fn parse_http_request(buffer: Vec<u8>) -> Result<HttpRequest, Box<dyn std::error::Error>> {
    let header_end = buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or("invalid HTTP request: missing header terminator")?;
    let header_text = std::str::from_utf8(&buffer[..header_end])?;
    let mut lines = header_text.lines();
    let request_line = lines.next().ok_or("invalid HTTP request: empty request")?;
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts
        .next()
        .ok_or("missing HTTP method")?
        .to_owned();
    let path = request_parts.next().ok_or("missing HTTP path")?.to_owned();
    let mut headers = Vec::new();
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        headers.push((name.trim().to_owned(), value.trim().to_owned()));
    }
    let content_length = headers
        .iter()
        .find(|(name, _value)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_name, value)| value.parse::<usize>().ok())
        .unwrap_or(0);
    let body_start = header_end + 4;
    let mut body = buffer.get(body_start..).unwrap_or_default().to_vec();
    body.truncate(content_length);

    Ok(HttpRequest {
        method,
        path,
        headers,
        body,
    })
}

fn json_response(status: u16, payload: &impl Serialize) -> String {
    let body = serde_json::to_string(payload).expect("bootstrap response should serialize");
    raw_response(status, "application/json", &body)
}

fn text_response(status: u16, body: &str) -> String {
    raw_response(status, "text/plain; charset=utf-8", body)
}

fn raw_response(status: u16, content_type: &str, body: &str) -> String {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        _ => "Error",
    };
    format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use tempfile::tempdir;

    use super::{
        ClientBootstrapConfig, ClientBootstrapRequest, HttpRequest, has_complete_http_request,
        parse_http_request, route_http_request,
    };
    use crate::{ServerConfig, ServerTlsConfig};

    #[test]
    fn parse_http_request_reads_method_path_headers_and_body() {
        let request = parse_http_request(
            b"POST /v1/client-bundles HTTP/1.1\r\nHost: legato.lan:7824\r\nContent-Length: 19\r\n\r\n{\"client_name\":\"x\"}"
                .to_vec(),
        )
        .expect("request should parse");

        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/v1/client-bundles");
        assert_eq!(request.header("host"), Some("legato.lan:7824"));
        assert_eq!(request.body, br#"{"client_name":"x"}"#);
    }

    #[test]
    fn complete_http_request_detection_waits_for_body() {
        assert!(
            !has_complete_http_request(
                b"POST /v1/client-bundles HTTP/1.1\r\nContent-Length: 19\r\n\r\n{\"client_name\":\"x\"",
            )
            .expect("partial request should parse")
        );
        assert!(
            has_complete_http_request(
                b"POST /v1/client-bundles HTTP/1.1\r\nContent-Length: 19\r\n\r\n{\"client_name\":\"x\"}",
            )
            .expect("complete request should parse")
        );
    }

    #[test]
    fn bootstrap_endpoint_issues_bundle_payload() {
        let fixture = tempdir().expect("tempdir should be created");
        let tls_dir = fixture.path().join("tls");
        let config = ServerConfig {
            bind_address: String::from("0.0.0.0:7823"),
            library_root: String::from("/srv/libraries"),
            state_dir: fixture.path().join("state").to_string_lossy().into_owned(),
            tls_dir: tls_dir.to_string_lossy().into_owned(),
            tls: ServerTlsConfig::local_dev(&tls_dir),
            bootstrap: ClientBootstrapConfig {
                advertised_endpoint: Some(String::from("legato.lan:7823")),
                server_name: Some(String::from("legato.lan")),
                ..ClientBootstrapConfig::default()
            },
        };
        let body = serde_json::to_vec(&ClientBootstrapRequest {
            client_name: String::from("studio-win"),
            mount_point: Some(String::from("L:\\Legato")),
            library_root: Some(String::from("/")),
        })
        .expect("request body should serialize");
        let request = HttpRequest {
            method: String::from("POST"),
            path: String::from("/v1/client-bundles"),
            headers: vec![(String::from("host"), String::from("10.0.0.2:7824"))],
            body,
        };

        let response = route_http_request(
            &request,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 3)), 55555),
            &config,
        );

        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("studio-win"));
        assert!(response.contains("server_ca_pem"));
        assert!(response.contains("client_key_pem"));
        assert!(response.contains("legato.lan:7823"));
    }
}
