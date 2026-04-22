//! Shared runtime state for native Legato clients.

use std::{collections::HashMap, fs, io::Cursor, path::Path, sync::Arc};

use legato_client_cache::CacheConfig;
use legato_proto::{
    AttachRequest, OpenRequest, OpenResponse, PROTOCOL_VERSION, default_capabilities,
};
use rustls::{
    ClientConfig as RustlsClientConfig, RootCertStore,
    pki_types::{CertificateDer, PrivateKeyDer},
};

/// Immutable settings used to bootstrap a client runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
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
#[derive(Clone, Debug, Eq, PartialEq)]
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
#[derive(Clone, Debug, Eq, PartialEq)]
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
    /// File reopen requests required to replace stale server-local handles.
    pub reopen_requests: Vec<OpenRequest>,
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

/// State for one logical open file that must be refreshed after reconnect.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenFileState {
    /// Canonical library path used to re-open the file after reconnect.
    pub path: String,
    /// Stable file identifier returned by the server.
    pub file_id: u64,
    /// Current server-local file handle.
    pub file_handle: u64,
    /// Server-advertised block size used by the read path.
    pub block_size: u32,
    /// Whether the handle became stale due to transport loss.
    pub stale: bool,
}

/// Stateful runtime shell for attach, reconnect, and stale-handle recovery planning.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientRuntime {
    config: ClientConfig,
    generation: u64,
    session_status: SessionStatus,
    failure_count: u32,
    open_files: HashMap<String, OpenFileState>,
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
            open_files: HashMap::new(),
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
        self.generation += 1;
        self.failure_count = 0;
        self.session_status = SessionStatus::Connected {
            generation: self.generation,
            subscription_active: false,
        };
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

    /// Records one open file handle that should be re-opened after reconnect.
    pub fn record_open_handle(&mut self, path: &str, response: &OpenResponse) {
        self.open_files.insert(
            String::from(path),
            OpenFileState {
                path: String::from(path),
                file_id: response.file_id,
                file_handle: response.file_handle,
                block_size: response.block_size,
                stale: false,
            },
        );
    }

    /// Updates the tracked handle after a successful reopen.
    pub fn refresh_open_handle(&mut self, path: &str, response: &OpenResponse) {
        self.record_open_handle(path, response);
    }

    /// Marks the transport unavailable and returns the next reconnect delay.
    pub fn mark_transport_unavailable(&mut self) -> u64 {
        self.failure_count = self.failure_count.saturating_add(1);
        let delay_ms = backoff_delay_ms(&self.config.retry, self.failure_count);
        self.session_status = SessionStatus::Backoff {
            attempt: self.failure_count,
            delay_ms,
        };

        for open_file in self.open_files.values_mut() {
            open_file.stale = true;
        }

        delay_ms
    }

    /// Builds the reconnect work needed to restore subscriptions and stale handles.
    #[must_use]
    pub fn reconnect_plan(&self, client_name: &str) -> ReconnectPlan {
        let resubscribe = matches!(
            self.session_status,
            SessionStatus::Connected {
                subscription_active: true,
                ..
            }
        ) || self.open_files.values().any(|open_file| open_file.stale);

        let mut reopen_requests = self
            .open_files
            .values()
            .filter(|open_file| open_file.stale)
            .map(|open_file| OpenRequest {
                path: open_file.path.clone(),
            })
            .collect::<Vec<_>>();
        reopen_requests.sort_by(|left, right| left.path.cmp(&right.path));

        ReconnectPlan {
            attach: self.attach_request(client_name),
            resubscribe,
            reopen_requests,
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

    /// Returns the tracked open file state for the given path.
    #[must_use]
    pub fn open_file(&self, path: &str) -> Option<&OpenFileState> {
        self.open_files.get(path)
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

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{
        ClientConfig, ClientRuntime, ClientTlsConfig, ClientTlsError, ReconnectPlan, RetryPolicy,
        SessionStatus, build_tls_client_config,
    };
    use legato_proto::{OpenRequest, OpenResponse, PROTOCOL_VERSION};
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
    fn connect_and_reconnect_plan_restore_subscription_and_stale_handles() {
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
        runtime.record_open_handle(
            "/srv/libraries/Kontakt/piano.nki",
            &OpenResponse {
                file_handle: 7,
                file_id: 42,
                size: 1024,
                mtime_ns: 77,
                content_hash: Vec::new(),
                block_size: 4096,
            },
        );

        let delay_ms = runtime.mark_transport_unavailable();
        assert_eq!(delay_ms, 250);
        assert!(
            runtime
                .open_file("/srv/libraries/Kontakt/piano.nki")
                .expect("open file should be tracked")
                .stale
        );

        let plan = runtime.reconnect_plan("legatofs");
        assert_eq!(
            plan,
            ReconnectPlan {
                attach: runtime.attach_request("legatofs"),
                resubscribe: true,
                reopen_requests: vec![OpenRequest {
                    path: String::from("/srv/libraries/Kontakt/piano.nki"),
                }],
            }
        );

        runtime.refresh_open_handle(
            "/srv/libraries/Kontakt/piano.nki",
            &OpenResponse {
                file_handle: 8,
                file_id: 42,
                size: 1024,
                mtime_ns: 77,
                content_hash: Vec::new(),
                block_size: 4096,
            },
        );
        assert_eq!(
            runtime
                .open_file("/srv/libraries/Kontakt/piano.nki")
                .expect("open file should be refreshed")
                .file_handle,
            8
        );
        assert!(
            !runtime
                .open_file("/srv/libraries/Kontakt/piano.nki")
                .expect("open file should be refreshed")
                .stale
        );
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
}
