//! Versioned SQLite schema metadata for the server metadata database.

/// A single ordered metadata-database migration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Migration {
    /// Monotonic schema version applied by the migration.
    pub version: u32,
    /// Stable human-readable migration name.
    pub name: &'static str,
    /// SQL batch applied when moving to this schema version.
    pub sql: &'static str,
}

/// Current schema version for the server metadata database.
pub const SERVER_SCHEMA_VERSION: u32 = 2;

/// Returns the ordered list of migrations for the server metadata database.
#[must_use]
pub fn server_migrations() -> &'static [Migration] {
    &[
        Migration {
            version: 1,
            name: "init_server_metadata",
            sql: include_str!("../migrations/0001_init_server_metadata.sql"),
        },
        Migration {
            version: 2,
            name: "add_filesystem_identity",
            sql: include_str!("../migrations/0002_add_filesystem_identity.sql"),
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::{SERVER_SCHEMA_VERSION, server_migrations};

    #[test]
    fn latest_migration_matches_exported_schema_version() {
        let migrations = server_migrations();

        assert_eq!(
            migrations
                .last()
                .expect("migration list should be non-empty")
                .version,
            SERVER_SCHEMA_VERSION
        );
    }
}
