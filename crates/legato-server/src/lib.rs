//! Server-side bootstrap types for the Legato daemon.

pub mod canonical;

mod bootstrap;
mod invalidation;
mod layout;
mod metrics;
mod rpc;
mod tls;
mod watcher;

pub use bootstrap::{
    ClientBootstrapAdvertisement, ClientBootstrapConfig, ClientBootstrapRequest,
    ClientBootstrapServices,
};
pub use canonical::{CanonicalStoreError, reconcile_library_root_to_store};
pub use invalidation::{InvalidationHub, InvalidationSubscription, subtree_invalidation};
pub use layout::{DEFAULT_POLICY_FILE, LayoutDecision, LayoutPolicy, is_policy_path, policy_path};
use legato_proto::{AttachResponse, PROTOCOL_VERSION, negotiate_capabilities};
pub use metrics::ServerRuntimeMetrics;
pub use rpc::{BoundServer, LiveServer, RuntimeTlsConfig, load_runtime_tls, parse_bind_address};
use serde::Deserialize;
pub use tls::{
    BootstrappedServerTlsPaths, ClientBundleManifest, ClientBundlePayload, ServerTlsConfig,
    TlsConfigError, build_tls_server_config, ensure_server_tls_materials, issue_client_tls_bundle,
    issue_client_tls_bundle_payload, write_client_bundle_manifest, write_client_bundle_payload,
};
pub use watcher::{
    NotificationAction, WatchBackend, create_poll_watcher, create_recommended_watcher,
    plan_notification_result,
};

/// Summary of one library-to-store reconciliation pass.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReconcileStats {
    /// Directories inserted into the active catalog.
    pub directories_created: u64,
    /// Directories whose metadata or membership was refreshed.
    pub directories_updated: u64,
    /// Directory records removed from the active catalog.
    pub directories_deleted: u64,
    /// Files inserted into the active catalog.
    pub files_created: u64,
    /// Files whose metadata or content extents were refreshed.
    pub files_updated: u64,
    /// File records removed from the active catalog.
    pub files_deleted: u64,
}

/// Source label used for server extent-fetch metrics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExtentFetchSource {
    /// The extent was returned from the canonical Legato store.
    CacheHit,
    /// The extent had to be imported from the source library.
    SourceRead,
}

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
    /// TLS certificate and mTLS trust material used by the listener.
    #[serde(default)]
    pub tls: ServerTlsConfig,
    /// Unauthenticated LAN bootstrap endpoint used by installers before mTLS exists.
    #[serde(default)]
    pub bootstrap: ClientBootstrapConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: String::from("0.0.0.0:7823"),
            library_root: String::from("/srv/libraries"),
            state_dir: String::from("/var/lib/legato"),
            tls_dir: String::from("/etc/legato/certs"),
            tls: ServerTlsConfig::default(),
            bootstrap: ClientBootstrapConfig::default(),
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
    pub fn attach_response(&self, desired_capabilities: &[i32]) -> AttachResponse {
        let _ = &self.config;
        AttachResponse {
            protocol_version: PROTOCOL_VERSION,
            negotiated_capabilities: negotiate_capabilities(desired_capabilities),
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
        assert_eq!(
            server.attach_response(&[]).protocol_version,
            PROTOCOL_VERSION
        );
        assert_eq!(ServerConfig::default().library_root, "/srv/libraries");
    }
}
