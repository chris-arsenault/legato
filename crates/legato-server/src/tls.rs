//! TLS bootstrap helpers for the server daemon.

use std::{
    fs,
    io::Cursor,
    path::{Path, PathBuf},
    sync::Arc,
};

use rustls::{
    RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer},
    server::WebPkiClientVerifier,
};
use serde::Deserialize;

/// TLS material configuration for the server listener.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct ServerTlsConfig {
    /// PEM-encoded certificate chain presented by the server.
    pub cert_path: String,
    /// PEM-encoded private key matching `cert_path`.
    pub key_path: String,
    /// PEM-encoded client CA bundle used for mTLS validation.
    pub client_ca_path: String,
}

impl Default for ServerTlsConfig {
    fn default() -> Self {
        Self {
            cert_path: String::from("/etc/legato/certs/server.pem"),
            key_path: String::from("/etc/legato/certs/server-key.pem"),
            client_ca_path: String::from("/etc/legato/certs/client-ca.pem"),
        }
    }
}

impl ServerTlsConfig {
    /// Returns a local-development certificate layout rooted at the provided directory.
    #[must_use]
    pub fn local_dev(base_dir: &Path) -> Self {
        Self {
            cert_path: base_dir.join("server.pem").to_string_lossy().into_owned(),
            key_path: base_dir
                .join("server-key.pem")
                .to_string_lossy()
                .into_owned(),
            client_ca_path: base_dir
                .join("client-ca.pem")
                .to_string_lossy()
                .into_owned(),
        }
    }
}

/// Fail-fast errors encountered while loading listener TLS state.
#[derive(Debug)]
pub enum TlsConfigError {
    /// Underlying filesystem access failed.
    Io(std::io::Error),
    /// A PEM block could not be parsed.
    Pem(std::io::Error),
    /// The configured certificate chain was empty.
    MissingCertificates(PathBuf),
    /// No usable private key was found.
    MissingPrivateKey(PathBuf),
    /// The configured client CA bundle contained no trust anchors.
    MissingClientCa(PathBuf),
    /// rustls rejected the resulting TLS configuration.
    Rustls(rustls::Error),
    /// rustls rejected the client certificate verifier configuration.
    Verifier(rustls::server::VerifierBuilderError),
}

impl std::fmt::Display for TlsConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "tls file IO failed: {error}"),
            Self::Pem(error) => write!(formatter, "tls PEM parsing failed: {error}"),
            Self::MissingCertificates(path) => {
                write!(
                    formatter,
                    "no server certificates found in {}",
                    path.display()
                )
            }
            Self::MissingPrivateKey(path) => {
                write!(formatter, "no private key found in {}", path.display())
            }
            Self::MissingClientCa(path) => {
                write!(
                    formatter,
                    "no client CA certificates found in {}",
                    path.display()
                )
            }
            Self::Rustls(error) => write!(formatter, "rustls server config failed: {error}"),
            Self::Verifier(error) => write!(formatter, "client verifier setup failed: {error}"),
        }
    }
}

impl std::error::Error for TlsConfigError {}

/// Loads TLS files and builds a TLS 1.3-only rustls server configuration with mTLS.
pub fn build_tls_server_config(
    config: &ServerTlsConfig,
) -> Result<Arc<ServerConfig>, TlsConfigError> {
    let cert_path = PathBuf::from(&config.cert_path);
    let key_path = PathBuf::from(&config.key_path);
    let client_ca_path = PathBuf::from(&config.client_ca_path);

    let cert_chain = load_certificates(&cert_path)?;
    let private_key = load_private_key(&key_path)?;
    let client_roots = load_client_roots(&client_ca_path)?;
    let verifier = WebPkiClientVerifier::builder(Arc::new(client_roots))
        .build()
        .map_err(TlsConfigError::Verifier)?;

    let provider = rustls::crypto::ring::default_provider();
    let builder = ServerConfig::builder_with_provider(provider.into())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(TlsConfigError::Rustls)?;
    let mut server_config = builder
        .with_client_cert_verifier(verifier)
        .with_single_cert(cert_chain, private_key)
        .map_err(TlsConfigError::Rustls)?;
    server_config.alpn_protocols = vec![b"h2".to_vec()];

    Ok(Arc::new(server_config))
}

fn load_certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsConfigError> {
    let contents = fs::read(path).map_err(TlsConfigError::Io)?;
    let certificates = rustls_pemfile::certs(&mut Cursor::new(contents))
        .collect::<Result<Vec<_>, _>>()
        .map_err(TlsConfigError::Pem)?;

    if certificates.is_empty() {
        return Err(TlsConfigError::MissingCertificates(path.to_path_buf()));
    }

    Ok(certificates)
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsConfigError> {
    let contents = fs::read(path).map_err(TlsConfigError::Io)?;
    let Some(private_key) =
        rustls_pemfile::private_key(&mut Cursor::new(contents)).map_err(TlsConfigError::Pem)?
    else {
        return Err(TlsConfigError::MissingPrivateKey(path.to_path_buf()));
    };

    Ok(private_key)
}

fn load_client_roots(path: &Path) -> Result<RootCertStore, TlsConfigError> {
    let certificates = load_certificates(path)?;
    let mut roots = RootCertStore::empty();
    let (added, _ignored) = roots.add_parsable_certificates(certificates);
    if added == 0 {
        return Err(TlsConfigError::MissingClientCa(path.to_path_buf()));
    }
    Ok(roots)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::{ServerTlsConfig, TlsConfigError, build_tls_server_config};

    const TEST_CERT_PEM: &str = "-----BEGIN CERTIFICATE-----\nMIIC/zCCAeegAwIBAgIUKWr7nJpAOz9K1vWUN3gheRvIy/8wDQYJKoZIhvcNAQEL\nBQAwDzENMAsGA1UEAwwEdGVzdDAeFw0yNjA0MjIwMDU0MDlaFw0yNzA0MjIwMDU0\nMDlaMA8xDTALBgNVBAMMBHRlc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEK\nAoIBAQDEIfTpZUMQggMrqrDW9DMykuBUtQs7C0MAzs/WZjxTYaPfiONPYOvJ3n+e\nruGti/ypIxNijZPksrINYbh5PQpZ+Vo+bJml2K0S0d3EwDGEfLVEC8JNYUgKbCdZ\nvGuno/2KT4d5NnJNtVkxGZFh4KTFnpwhhbJH7lGt2VbvXLcJtQM+vgHpihz6QZxX\nR+L+LSNmaM8MZxU8MtbdyLKdey745osovkjdi+IKmkXb0ySra1fzgmXDaWMThOXy\nTh5UuD5n0RuUf5U9kRrpNc2/WxKx60mqdVA0BPHpOZyvEH9Nop9ZctVF1WKUGAzf\nvEYfeo2/OVW/+l1owNSb1CGWBcglAgMBAAGjUzBRMB0GA1UdDgQWBBQTHES/FEdh\nBSSzvS3vdZyNTnqunzAfBgNVHSMEGDAWgBQTHES/FEdhBSSzvS3vdZyNTnqunzAP\nBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQBnXNfXPXQ8l89Cmy7D\ntoRjdWhPc1auU6U6LmZME5TcrQsDTEUlux2u4C2X+qTygZY/bJT8aum4D9LJlEh2\nY8tr/8yz2+jcoNu+tDmHs/OTTUuJfw03Gztbj/m0+nZBPEhmU2VK+t5SWUuJen+3\nEnE5oP2jByDR9AR/z9QPUqDgvP8wsuAvZ6mSZoP9iF3AGNLY8OF9j0BLBXSwHGkM\ncHJsVQvNJ+BOpn6KxsLxLl8DG4fwQ9RCBdhrSr3gQxYMWNnLmqbpeGDE+wQQWDEM\nPSvoKbrOwJyAO8RYUTTG0shPGm5J7tb1ZBJfITtfS4uNBRU8RLpDXFXk1hTKys+y\nEnAC\n-----END CERTIFICATE-----\n";
    const TEST_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDEIfTpZUMQggMr\nqrDW9DMykuBUtQs7C0MAzs/WZjxTYaPfiONPYOvJ3n+eruGti/ypIxNijZPksrIN\nYbh5PQpZ+Vo+bJml2K0S0d3EwDGEfLVEC8JNYUgKbCdZvGuno/2KT4d5NnJNtVkx\nGZFh4KTFnpwhhbJH7lGt2VbvXLcJtQM+vgHpihz6QZxXR+L+LSNmaM8MZxU8Mtbd\nyLKdey745osovkjdi+IKmkXb0ySra1fzgmXDaWMThOXyTh5UuD5n0RuUf5U9kRrp\nNc2/WxKx60mqdVA0BPHpOZyvEH9Nop9ZctVF1WKUGAzfvEYfeo2/OVW/+l1owNSb\n1CGWBcglAgMBAAECggEAE4jsS1jWIq1IYo+IOIivFsDxTg6QNUvMWya+JbUeGhH2\nD/wi49Ob+InMPUJe4Pm56yC+vAis69Dukg2jIZQ2VTrURbZsYUvhBShZBdE8vVzs\ncwAW1L01PzCBhNsS/+oCBUYhkK2fGeVPomfRBqYo0zQWifI2jRqMERw+H+4knvc3\nC40YUj8pjA02LTsSPOLAs1hg0ukUI9SvzzRqCweONlL9JUmxKREec6Ju6M+ReHrU\nFYzY8Drd336N4yEQhfKyfAIFbHwvgM7S1pYL3hMisxWUz6cTZ0dXJQ0RFs23KBQ+\nWF00pImP25FevsaumBBZVgTLeUcENZgNNd+QD9VjsQKBgQD8qm/b2lUnV68tyGc+\nZCN8TnHe9BXIjCEpeEFRQjQFG9RUa+gS9BMEszWqfwmTDmUHu/dLGjfwnFUOXtl1\nhR69FslexvU4xp0auBCGOlymT7JtDPFvdBIM9LqaX5w3ObJXb4/dBdAm+8OK6ClC\n8h/GDHeCyJiT5pJV5VUlE+s/TQKBgQDGuImwCXZER50cKXLA4UxmR+gHM/18u/WI\nTMrsvfLGaOC9mZDfvQbB0cyN5p9RuWhCb7UMigWdv7SBA149bf7KQVev/2wfwR9Z\n7YmBKo4wNq2ovUlmkdZxtxdKKgIPBdLjtKUuL5Rsogcd7yAgWKJ5y6fmnLBrz4zD\nx6nBwz9wOQKBgQDsNhbPTgWv+zytq55B6PJ34wp36m93BvJ1x5Qg+KiTYhoWNq9H\nEOG60iPI2m1ECwAOw/6EOuWzTyQBhFD+mk5LbsMhlRVqV9xGP3BLXMKDRRzE8IXC\nsZuyexT8/4eW5ZzCO20er7GS0GpWMYdpq9xilgMgxJJIKxYBsZ3xRPb4PQKBgEfq\n5TmmUvznBf75KSSQ5PtnLpvcvvJze6q2UAJZxBD2R8+WUg4G9PkUGnmIa0RCW28f\nymAdW2b5yDOgqmyE8F72QuvY/qKHW/dJtife5NKiFzsoNfY+9WL2JiGbDl+tdeMe\nr2EFqyudgAHfVrseGL8Ha15Ueqyp0oHQMqmDJeRRAoGBANMeTYR6a05xmT7ZDPHT\nNX0syIu42Yys3ZC9bNlke7iWDntNoyC0CfAqDNMKVDomMbxcs2nFqL3TiVIu5Kev\nGnn3tLiJjv/LC3F90gVhcwlN87/nNXlEPfeoOPVUImU/3Tq11lYH6JXU69sARRqQ\n8YgEyQYcCpY0679sL4W1s/w1\n-----END PRIVATE KEY-----\n";

    #[test]
    fn local_dev_config_uses_expected_filenames() {
        let root = tempdir().expect("tempdir should be created");
        let config = ServerTlsConfig::local_dev(root.path());

        assert!(config.cert_path.ends_with("server.pem"));
        assert!(config.key_path.ends_with("server-key.pem"));
        assert!(config.client_ca_path.ends_with("client-ca.pem"));
    }

    #[test]
    fn build_tls_server_config_loads_tls13_mutual_tls_settings() {
        let root = tempdir().expect("tempdir should be created");
        let config = ServerTlsConfig::local_dev(root.path());
        fs::write(&config.cert_path, TEST_CERT_PEM).expect("server cert should be written");
        fs::write(&config.key_path, TEST_KEY_PEM).expect("server key should be written");
        fs::write(&config.client_ca_path, TEST_CERT_PEM).expect("client ca should be written");

        let tls = build_tls_server_config(&config).expect("tls config should build");

        assert_eq!(tls.alpn_protocols, vec![b"h2".to_vec()]);
    }

    #[test]
    fn build_tls_server_config_rejects_missing_client_ca() {
        let root = tempdir().expect("tempdir should be created");
        let config = ServerTlsConfig::local_dev(root.path());
        fs::write(&config.cert_path, TEST_CERT_PEM).expect("server cert should be written");
        fs::write(&config.key_path, TEST_KEY_PEM).expect("server key should be written");
        fs::write(&config.client_ca_path, "").expect("client ca should be written");

        let error = build_tls_server_config(&config).expect_err("missing client CA should fail");
        assert!(
            matches!(
                error,
                TlsConfigError::MissingCertificates(_) | TlsConfigError::MissingClientCa(_)
            ),
            "unexpected error: {error}"
        );
    }
}
