//! Local block-cache types shared by client-side components.

mod schema;

use std::{fs, path::Path};

use legato_types::{BlockRange, FileId};
use rusqlite::Connection;
pub use schema::{CLIENT_CACHE_SCHEMA_VERSION, cache_migrations};

/// Identity for a single block cache entry.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CacheKey {
    /// Stable identifier of the file containing the block.
    pub file_id: FileId,
    /// Block-aligned starting offset in bytes.
    pub start_offset: u64,
}

impl From<&BlockRange> for CacheKey {
    fn from(range: &BlockRange) -> Self {
        Self {
            file_id: range.file_id,
            start_offset: range.start_offset,
        }
    }
}

/// Minimal cache configuration used by the shared client runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CacheConfig {
    /// Total maximum size of the cache in bytes.
    pub max_bytes: u64,
    /// Fixed block size used by the cache.
    pub block_size: u32,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_bytes: 1_500 * 1024 * 1024 * 1024,
            block_size: 1 << 20,
        }
    }
}

/// Opens the client cache database, applying the current schema if needed.
pub fn open_cache_database(path: &Path) -> rusqlite::Result<Connection> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    }

    let mut connection = Connection::open(path)?;
    configure_database(&connection)?;
    migrate_cache_database(&mut connection)?;
    Ok(connection)
}

/// Applies cache schema migrations to the provided connection.
pub fn migrate_cache_database(connection: &mut Connection) -> rusqlite::Result<()> {
    let current_version: u32 =
        connection.pragma_query_value(None, "user_version", |row| row.get(0))?;

    if current_version >= CLIENT_CACHE_SCHEMA_VERSION {
        return Ok(());
    }

    let transaction = connection.transaction()?;
    for migration in cache_migrations()
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
    use super::{CLIENT_CACHE_SCHEMA_VERSION, CacheConfig, CacheKey, open_cache_database};
    use legato_types::{BlockRange, FileId};
    use tempfile::tempdir;

    #[test]
    fn cache_key_is_derived_from_block_identity() {
        let range = BlockRange {
            file_id: FileId(42),
            start_offset: 2 << 20,
            block_count: 1,
        };

        let key = CacheKey::from(&range);

        assert_eq!(key.file_id, FileId(42));
        assert_eq!(key.start_offset, 2 << 20);
        assert_eq!(CacheConfig::default().block_size, 1 << 20);
    }

    #[test]
    fn cache_database_migrations_create_expected_tables() {
        let temp = tempdir().expect("tempdir should be created");
        let path = temp.path().join("cache").join("client.sqlite");

        let connection = open_cache_database(&path).expect("cache database should open");

        let journal_mode: String = connection
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .expect("journal mode should be readable");
        let schema_version: u32 = connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .expect("user version should be readable");
        let mut statement = connection
            .prepare(
                "SELECT name FROM sqlite_master \
                 WHERE type = 'table' AND name IN ('cache_entries', 'pins', 'fetch_state', 'client_state') \
                 ORDER BY name",
            )
            .expect("table inspection statement should prepare");
        let table_names = statement
            .query_map([], |row| row.get::<_, String>(0))
            .expect("table inspection should run")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("table names should be collected");

        assert_eq!(journal_mode.to_lowercase(), "wal");
        assert_eq!(schema_version, CLIENT_CACHE_SCHEMA_VERSION);
        assert_eq!(
            table_names,
            vec![
                String::from("cache_entries"),
                String::from("client_state"),
                String::from("fetch_state"),
                String::from("pins"),
            ]
        );
    }
}
