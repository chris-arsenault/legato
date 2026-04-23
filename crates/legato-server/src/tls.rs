//! TLS bootstrap helpers for the server daemon.

use std::{
    fs,
    io::Cursor,
    net::IpAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DistinguishedName, DnType, IsCa, Issuer,
    KeyPair, SanType,
};
use rustls::{
    RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer},
    server::WebPkiClientVerifier,
};
use serde::{Deserialize, Serialize};

/// TLS material configuration for the server listener.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct ServerTlsConfig {
    /// PEM-encoded certificate chain presented by the server.
    pub cert_path: String,
    /// PEM-encoded private key matching `cert_path`.
    pub key_path: String,
    /// PEM-encoded client CA bundle used for mTLS validation.
    pub client_ca_path: String,
    /// Server names or IPs embedded into the generated server certificate SAN.
    pub server_names: Vec<String>,
}

impl Default for ServerTlsConfig {
    fn default() -> Self {
        Self {
            cert_path: String::from("/etc/legato/certs/server.pem"),
            key_path: String::from("/etc/legato/certs/server-key.pem"),
            client_ca_path: String::from("/etc/legato/certs/client-ca.pem"),
            server_names: vec![
                String::from("legato.lan"),
                String::from("localhost"),
                String::from("legato-server"),
            ],
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
            server_names: vec![String::from("legato.lan"), String::from("localhost")],
        }
    }
}

/// Files emitted when the server bootstraps its local CA and listener identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootstrappedServerTlsPaths {
    /// CA certificate trusted by clients and by the server for client cert auth.
    pub server_ca_cert_path: PathBuf,
    /// CA private key used to issue client certificates.
    pub server_ca_key_path: PathBuf,
    /// Listener certificate chain.
    pub server_cert_path: PathBuf,
    /// Listener private key.
    pub server_key_path: PathBuf,
    /// Client CA bundle used by the server. Mirrors the CA certificate.
    pub client_ca_path: PathBuf,
}

/// Installation metadata emitted alongside one issued client bundle.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ClientBundleManifest {
    /// Logical client name embedded into the client certificate.
    pub client_name: String,
    /// Suggested server endpoint for install-time config hydration.
    pub endpoint: Option<String>,
    /// Suggested TLS server name for client validation.
    pub server_name: Option<String>,
    /// Optional install-time mount point override.
    pub mount_point: Option<String>,
    /// Optional install-time virtual library root override.
    pub library_root: Option<String>,
    /// Time when the bundle was issued.
    pub issued_at_unix_ms: u64,
}

impl ClientBundleManifest {
    /// Creates a client-bundle manifest from server-side defaults and optional overrides.
    #[must_use]
    pub fn for_issue(
        client_name: &str,
        endpoint: Option<String>,
        server_name: Option<String>,
        mount_point: Option<String>,
        library_root: Option<String>,
    ) -> Self {
        let issued_at_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_millis() as u64);
        Self {
            client_name: client_name.to_owned(),
            endpoint,
            server_name,
            mount_point,
            library_root,
            issued_at_unix_ms,
        }
    }
}

/// Fail-fast errors encountered while loading listener TLS state.
#[derive(Debug)]
pub enum TlsConfigError {
    /// Underlying filesystem access failed.
    Io {
        /// Path that could not be read.
        path: PathBuf,
        /// Underlying filesystem error.
        source: std::io::Error,
    },
    /// A PEM block could not be parsed.
    Pem {
        /// Path that contained invalid PEM.
        path: PathBuf,
        /// Underlying parsing error.
        source: std::io::Error,
    },
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
    /// rcgen rejected certificate parameters or signing.
    Rcgen(rcgen::Error),
}

impl std::fmt::Display for TlsConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io { path, source } => {
                write!(
                    formatter,
                    "tls file IO failed for {}: {source}",
                    path.display()
                )
            }
            Self::Pem { path, source } => {
                write!(
                    formatter,
                    "tls PEM parsing failed for {}: {source}",
                    path.display()
                )
            }
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
            Self::Rcgen(error) => write!(formatter, "certificate generation failed: {error}"),
        }
    }
}

impl std::error::Error for TlsConfigError {}

/// Ensures that the server-side CA, listener certificate, and client trust bundle exist.
///
/// On first boot this generates a local CA plus a server certificate and persists them to disk.
/// Subsequent boots reuse the existing material.
pub fn ensure_server_tls_materials(
    tls_dir: &Path,
    config: &ServerTlsConfig,
) -> Result<BootstrappedServerTlsPaths, TlsConfigError> {
    fs::create_dir_all(tls_dir).map_err(|source| TlsConfigError::Io {
        path: tls_dir.to_path_buf(),
        source,
    })?;

    let server_ca_cert_path = tls_dir.join("server-ca.pem");
    let server_ca_key_path = tls_dir.join("server-ca-key.pem");
    let server_cert_path = PathBuf::from(&config.cert_path);
    let server_key_path = PathBuf::from(&config.key_path);
    let client_ca_path = PathBuf::from(&config.client_ca_path);

    let have_bootstrap = [
        &server_ca_cert_path,
        &server_ca_key_path,
        &server_cert_path,
        &server_key_path,
        &client_ca_path,
    ]
    .iter()
    .all(|path| path.exists());

    if !have_bootstrap {
        generate_server_tls_materials(
            tls_dir,
            &server_ca_cert_path,
            &server_ca_key_path,
            &server_cert_path,
            &server_key_path,
            &client_ca_path,
            &config.server_names,
        )?;
    }

    Ok(BootstrappedServerTlsPaths {
        server_ca_cert_path,
        server_ca_key_path,
        server_cert_path,
        server_key_path,
        client_ca_path,
    })
}

/// Issues a new client certificate bundle signed by the server-local CA.
pub fn issue_client_tls_bundle(
    tls_dir: &Path,
    config: &ServerTlsConfig,
    client_name: &str,
    output_dir: &Path,
) -> Result<(), TlsConfigError> {
    let paths = ensure_server_tls_materials(tls_dir, config)?;
    fs::create_dir_all(output_dir).map_err(|source| TlsConfigError::Io {
        path: output_dir.to_path_buf(),
        source,
    })?;

    let issuer = load_ca_issuer(&paths.server_ca_cert_path, &paths.server_ca_key_path)?;
    let client_key = KeyPair::generate().map_err(TlsConfigError::Rcgen)?;
    let mut client_params =
        CertificateParams::new(vec![client_name.to_owned()]).map_err(TlsConfigError::Rcgen)?;
    let mut distinguished_name = DistinguishedName::new();
    distinguished_name.push(DnType::CommonName, client_name);
    distinguished_name.push(DnType::OrganizationName, "Legato");
    client_params.distinguished_name = distinguished_name;
    let client_cert = client_params
        .signed_by(&client_key, &issuer)
        .map_err(TlsConfigError::Rcgen)?;

    write_file(&output_dir.join("client.pem"), client_cert.pem(), 0o644)?;
    write_file(
        &output_dir.join("client-key.pem"),
        client_key.serialize_pem(),
        0o600,
    )?;
    fs::copy(&paths.server_ca_cert_path, output_dir.join("server-ca.pem")).map_err(|source| {
        TlsConfigError::Io {
            path: output_dir.join("server-ca.pem"),
            source,
        }
    })?;

    Ok(())
}

/// Writes installation metadata alongside an issued client bundle.
pub fn write_client_bundle_manifest(
    output_dir: &Path,
    manifest: &ClientBundleManifest,
) -> Result<(), TlsConfigError> {
    let payload = serde_json::to_string_pretty(manifest).map_err(|error| TlsConfigError::Io {
        path: output_dir.join("bundle.json"),
        source: std::io::Error::other(error),
    })?;
    write_file(&output_dir.join("bundle.json"), payload, 0o644)
}

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
    let contents = fs::read(path).map_err(|source| TlsConfigError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let certificates = rustls_pemfile::certs(&mut Cursor::new(contents))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| TlsConfigError::Pem {
            path: path.to_path_buf(),
            source,
        })?;

    if certificates.is_empty() {
        return Err(TlsConfigError::MissingCertificates(path.to_path_buf()));
    }

    Ok(certificates)
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsConfigError> {
    let contents = fs::read(path).map_err(|source| TlsConfigError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let Some(private_key) =
        rustls_pemfile::private_key(&mut Cursor::new(contents)).map_err(|source| {
            TlsConfigError::Pem {
                path: path.to_path_buf(),
                source,
            }
        })?
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

fn generate_server_tls_materials(
    _tls_dir: &Path,
    server_ca_cert_path: &Path,
    server_ca_key_path: &Path,
    server_cert_path: &Path,
    server_key_path: &Path,
    client_ca_path: &Path,
    server_names: &[String],
) -> Result<(), TlsConfigError> {
    let ca_key = KeyPair::generate().map_err(TlsConfigError::Rcgen)?;
    let mut ca_params = CertificateParams::new(Vec::new()).map_err(TlsConfigError::Rcgen)?;
    let mut ca_dn = DistinguishedName::new();
    ca_dn.push(DnType::CommonName, "Legato Local CA");
    ca_dn.push(DnType::OrganizationName, "Legato");
    ca_params.distinguished_name = ca_dn;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let issuer = CertifiedIssuer::self_signed(ca_params, ca_key).map_err(TlsConfigError::Rcgen)?;

    let server_key = KeyPair::generate().map_err(TlsConfigError::Rcgen)?;
    let mut server_params =
        CertificateParams::new(server_names.to_vec()).map_err(TlsConfigError::Rcgen)?;
    let mut server_dn = DistinguishedName::new();
    server_dn.push(DnType::CommonName, "legato-server");
    server_dn.push(DnType::OrganizationName, "Legato");
    server_params.distinguished_name = server_dn;
    server_params.subject_alt_names = server_names
        .iter()
        .map(|name| match name.parse::<IpAddr>() {
            Ok(ip) => SanType::IpAddress(ip),
            Err(_) => SanType::DnsName(name.clone().try_into().expect("valid dns name")),
        })
        .collect();
    let server_cert = server_params
        .signed_by(&server_key, &issuer)
        .map_err(TlsConfigError::Rcgen)?;

    write_file(server_ca_cert_path, issuer.pem(), 0o644)?;
    write_file(server_ca_key_path, issuer.key().serialize_pem(), 0o600)?;
    write_file(server_cert_path, server_cert.pem(), 0o644)?;
    write_file(server_key_path, server_key.serialize_pem(), 0o600)?;
    write_file(client_ca_path, issuer.pem(), 0o644)?;

    Ok(())
}

fn load_ca_issuer(
    cert_path: &Path,
    key_path: &Path,
) -> Result<Issuer<'static, KeyPair>, TlsConfigError> {
    let cert_pem = fs::read_to_string(cert_path).map_err(|source| TlsConfigError::Io {
        path: cert_path.to_path_buf(),
        source,
    })?;
    let key_pem = fs::read_to_string(key_path).map_err(|source| TlsConfigError::Io {
        path: key_path.to_path_buf(),
        source,
    })?;
    let key = KeyPair::from_pem(&key_pem).map_err(TlsConfigError::Rcgen)?;
    Issuer::from_ca_cert_pem(&cert_pem, key).map_err(TlsConfigError::Rcgen)
}

fn write_file(path: &Path, contents: String, mode: u32) -> Result<(), TlsConfigError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| TlsConfigError::Io {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    fs::write(path, contents).map_err(|source| TlsConfigError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let permissions = fs::Permissions::from_mode(mode);
        fs::set_permissions(path, permissions).map_err(|source| TlsConfigError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::{
        ClientBundleManifest, ServerTlsConfig, TlsConfigError, build_tls_server_config,
        ensure_server_tls_materials, issue_client_tls_bundle, write_client_bundle_manifest,
    };

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

    #[test]
    fn build_tls_server_config_reports_missing_cert_path() {
        let root = tempdir().expect("tempdir should be created");
        let config = ServerTlsConfig::local_dev(root.path());

        let error = build_tls_server_config(&config).expect_err("missing files should fail");
        match error {
            TlsConfigError::Io { path, .. } => {
                assert!(
                    path.ends_with("server.pem"),
                    "unexpected path: {}",
                    path.display()
                );
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn ensure_server_tls_materials_bootstraps_missing_files() {
        let root = tempdir().expect("tempdir should be created");
        let tls_dir = root.path().join("certs");
        let config = ServerTlsConfig::local_dev(&tls_dir);

        let paths = ensure_server_tls_materials(&tls_dir, &config)
            .expect("server tls materials should bootstrap");

        assert!(paths.server_ca_cert_path.exists());
        assert!(paths.server_ca_key_path.exists());
        assert!(paths.server_cert_path.exists());
        assert!(paths.server_key_path.exists());
        assert!(paths.client_ca_path.exists());
    }

    #[test]
    fn issue_client_tls_bundle_writes_client_materials() {
        let root = tempdir().expect("tempdir should be created");
        let tls_dir = root.path().join("server-certs");
        let output_dir = root.path().join("client-bundle");
        let config = ServerTlsConfig::local_dev(&tls_dir);

        issue_client_tls_bundle(&tls_dir, &config, "studio-mac", &output_dir)
            .expect("client bundle should be issued");

        assert!(output_dir.join("client.pem").exists());
        assert!(output_dir.join("client-key.pem").exists());
        assert!(output_dir.join("server-ca.pem").exists());
    }

    #[test]
    fn bundle_manifest_writes_install_metadata() {
        let root = tempdir().expect("tempdir should be created");
        let output_dir = root.path().join("client-bundle");
        fs::create_dir_all(&output_dir).expect("bundle dir should exist");
        let manifest = ClientBundleManifest::for_issue(
            "studio-mac",
            Some(String::from("legato.lan:7823")),
            Some(String::from("legato.lan")),
            None,
            Some(String::from("/srv/libraries")),
        );

        write_client_bundle_manifest(&output_dir, &manifest)
            .expect("bundle manifest should be written");

        let written = fs::read_to_string(output_dir.join("bundle.json"))
            .expect("bundle manifest should be readable");
        let parsed: ClientBundleManifest =
            serde_json::from_str(&written).expect("bundle manifest should parse");
        assert_eq!(parsed.client_name, "studio-mac");
        assert_eq!(parsed.endpoint.as_deref(), Some("legato.lan:7823"));
        assert_eq!(parsed.server_name.as_deref(), Some("legato.lan"));
        assert_eq!(parsed.library_root.as_deref(), Some("/srv/libraries"));
        assert!(parsed.issued_at_unix_ms > 0);
    }
}
