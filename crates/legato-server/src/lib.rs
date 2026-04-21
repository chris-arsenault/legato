//! Server-side bootstrap types for the Legato daemon.

use legato_proto::{AttachResponse, PROTOCOL_VERSION, default_capabilities};
use serde::Deserialize;

/// Immutable bootstrap configuration for the server daemon.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct ServerConfig {
    /// TCP bind address for the gRPC listener.
    pub bind_address: String,
    /// Root directory containing the canonical read-only library dataset.
    pub library_root: String,
    /// Writable directory for server metadata and runtime state.
    pub state_dir: String,
    /// Directory containing mounted TLS materials.
    pub tls_dir: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: String::from("0.0.0.0:7823"),
            library_root: String::from("/srv/libraries"),
            state_dir: String::from("/var/lib/legato"),
            tls_dir: String::from("/etc/legato/certs"),
        }
    }
}

/// Minimal server shell used while the full runtime is implemented.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Server {
    config: ServerConfig,
}

impl Server {
    /// Creates a new server shell from the provided configuration.
    #[must_use]
    pub fn new(config: ServerConfig) -> Self {
        Self { config }
    }

    /// Returns the current attach response for the server runtime.
    #[must_use]
    pub fn attach_response(&self) -> AttachResponse {
        let _ = &self.config;
        AttachResponse {
            protocol_version: PROTOCOL_VERSION,
            negotiated_capabilities: default_capabilities(),
            server_name: String::from("legato-server"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Server, ServerConfig};
    use legato_proto::PROTOCOL_VERSION;

    #[test]
    fn server_bootstrap_matches_workspace_protocol_version() {
        let server = Server::new(ServerConfig::default());
        assert_eq!(server.attach_response().protocol_version, PROTOCOL_VERSION);
        assert_eq!(ServerConfig::default().library_root, "/srv/libraries");
    }
}
