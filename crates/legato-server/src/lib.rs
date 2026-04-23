//! Server-side bootstrap types for the Legato daemon.

mod api;
mod extent;
mod index;
mod invalidation;
mod layout;
mod metrics;
mod rpc;
mod schema;
mod tls;
mod watcher;

use std::{fs, path::Path};

pub use api::{CatalogEntry, MetadataService};
pub use extent::{ExtentFetchSource, FetchedExtent, ServerExtentStore};
pub use index::{ReconcileStats, reconcile_library_root, reconcile_paths};
pub use invalidation::{InvalidationHub, InvalidationSubscription, subtree_invalidation};
pub use layout::{DEFAULT_POLICY_FILE, LayoutDecision, LayoutPolicy, is_policy_path, policy_path};
use legato_proto::{AttachResponse, PROTOCOL_VERSION, default_capabilities};
pub use metrics::ServerRuntimeMetrics;
pub use rpc::{BoundServer, LiveServer, RuntimeTlsConfig, load_runtime_tls, parse_bind_address};
use rusqlite::Connection;
pub use schema::{SERVER_SCHEMA_VERSION, server_migrations};
use serde::Deserialize;
pub use tls::{
    BootstrappedServerTlsPaths, ClientBundleManifest, ServerTlsConfig, TlsConfigError,
    build_tls_server_config, ensure_server_tls_materials, issue_client_tls_bundle,
    write_client_bundle_manifest,
};
pub use watcher::{
    NotificationAction, WatchBackend, apply_notification_result, create_poll_watcher,
    create_recommended_watcher, invalidation_events_for_action, plan_notification_result,
};

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
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: String::from("0.0.0.0:7823"),
            library_root: String::from("/srv/libraries"),
            state_dir: String::from("/var/lib/legato"),
            tls_dir: String::from("/etc/legato/certs"),
            tls: ServerTlsConfig::default(),
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

/// Opens the server metadata database, applying the current schema if needed.
pub fn open_metadata_database(path: &Path) -> rusqlite::Result<Connection> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    }

    let mut connection = Connection::open(path)?;
    configure_database(&connection)?;
    migrate_metadata_database(&mut connection)?;
    Ok(connection)
}

/// Applies metadata schema migrations to the provided connection.
pub fn migrate_metadata_database(connection: &mut Connection) -> rusqlite::Result<()> {
    let current_version: u32 =
        connection.pragma_query_value(None, "user_version", |row| row.get(0))?;

    if current_version >= SERVER_SCHEMA_VERSION {
        return Ok(());
    }

    let transaction = connection.transaction()?;
    for migration in server_migrations()
        .iter()
        .filter(|migration| migration.version > current_version)
    {
        transaction.execute_batch(migration.sql)?;
        transaction.pragma_update(None, "user_version", migration.version)?;
    }
    transaction.commit()
}

fn configure_database(connection: &Connection) -> rusqlite::Result<()> {
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    connection.pragma_update(None, "busy_timeout", 5_000_i64)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{SERVER_SCHEMA_VERSION, Server, ServerConfig, open_metadata_database};
    use legato_proto::PROTOCOL_VERSION;
    use tempfile::tempdir;

    #[test]
    fn server_bootstrap_matches_workspace_protocol_version() {
        let server = Server::new(ServerConfig::default());
        assert_eq!(server.attach_response().protocol_version, PROTOCOL_VERSION);
        assert_eq!(ServerConfig::default().library_root, "/srv/libraries");
    }

    #[test]
    fn metadata_database_migrations_create_expected_tables() {
        let temp = tempdir().expect("tempdir should be created");
        let path = temp.path().join("state").join("server.sqlite");

        let connection = open_metadata_database(&path).expect("metadata database should open");

        let journal_mode: String = connection
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .expect("journal mode should be readable");
        let schema_version: u32 = connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .expect("user version should be readable");
        let mut statement = connection
            .prepare(
                "SELECT name FROM sqlite_master \
                 WHERE type = 'table' AND name IN ('change_log', 'files', 'directories', 'watches', 'server_state') \
                 ORDER BY name",
            )
            .expect("table inspection statement should prepare");
        let table_names = statement
            .query_map([], |row| row.get::<_, String>(0))
            .expect("table inspection should run")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("table names should be collected");

        assert_eq!(journal_mode.to_lowercase(), "wal");
        assert_eq!(schema_version, SERVER_SCHEMA_VERSION);
        assert_eq!(
            table_names,
            vec![
                String::from("change_log"),
                String::from("directories"),
                String::from("files"),
                String::from("server_state"),
                String::from("watches"),
            ]
        );
    }
}
