//! Server-side bootstrap types for the Legato daemon.

use legato_proto::{AttachResponse, PROTOCOL_VERSION, default_capabilities};

/// Immutable bootstrap configuration for the server daemon.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServerConfig {
    /// TCP bind address for the gRPC listener.
    pub bind_address: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: String::from("0.0.0.0:7823"),
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
    }
}
