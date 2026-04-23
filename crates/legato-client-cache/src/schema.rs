//! Versioned SQLite schema metadata for the client cache database.

/// A single ordered cache-database migration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Migration {
    /// Monotonic schema version applied by the migration.
    pub version: u32,
    /// Stable human-readable migration name.
    pub name: &'static str,
    /// SQL batch applied when moving to this schema version.
    pub sql: &'static str,
}

/// Current schema version for the client cache database.
pub const CLIENT_CACHE_SCHEMA_VERSION: u32 = 3;

/// Returns the ordered list of migrations for the client cache database.
#[must_use]
pub fn cache_migrations() -> &'static [Migration] {
    &[
        Migration {
            version: 1,
            name: "init_client_cache",
            sql: include_str!("../migrations/0001_init_client_cache.sql"),
        },
        Migration {
            version: 2,
            name: "add_extent_store",
            sql: include_str!("../migrations/0002_add_extent_store.sql"),
        },
        Migration {
            version: 3,
            name: "drop_block_cache_tables",
            sql: include_str!("../migrations/0003_drop_block_cache_tables.sql"),
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::{CLIENT_CACHE_SCHEMA_VERSION, cache_migrations};

    #[test]
    fn latest_migration_matches_exported_schema_version() {
        let migrations = cache_migrations();

        assert_eq!(
            migrations
                .last()
                .expect("migration list should be non-empty")
                .version,
            CLIENT_CACHE_SCHEMA_VERSION
        );
    }
}
