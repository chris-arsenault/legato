//! Shared runtime state for native Legato clients.

use legato_client_cache::CacheConfig;
use legato_proto::{AttachRequest, PROTOCOL_VERSION, default_capabilities};

/// Immutable settings used to bootstrap a client runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientConfig {
    /// Logical endpoint name used for diagnostics.
    pub endpoint: String,
    /// Cache settings for the local runtime.
    pub cache: CacheConfig,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            endpoint: String::from("legato.lan:7823"),
            cache: CacheConfig::default(),
        }
    }
}

/// Small runtime shell used to verify crate wiring before real logic lands.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientRuntime {
    config: ClientConfig,
}

impl ClientRuntime {
    /// Creates a runtime with the provided immutable configuration.
    #[must_use]
    pub fn new(config: ClientConfig) -> Self {
        Self { config }
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

    /// Returns a shared reference to the runtime configuration.
    #[must_use]
    pub fn config(&self) -> &ClientConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::{ClientConfig, ClientRuntime};
    use legato_proto::PROTOCOL_VERSION;

    #[test]
    fn runtime_builds_attach_requests_from_workspace_defaults() {
        let runtime = ClientRuntime::new(ClientConfig::default());
        let attach = runtime.attach_request("legatofs");

        assert_eq!(attach.protocol_version, PROTOCOL_VERSION);
        assert_eq!(attach.client_name, "legatofs");
        assert_eq!(runtime.config().endpoint, "legato.lan:7823");
    }
}
