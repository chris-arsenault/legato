//! Local metadata and block-cache primitives shared by client-side components.

mod schema;

use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use legato_proto::{DirectoryEntry, FileMetadata, InvalidationEvent, InvalidationKind};
use legato_types::{BlockRange, FileId};
use rusqlite::{Connection, OptionalExtension, params};
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

/// TTL policy for the in-memory metadata cache.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataCachePolicy {
    /// TTL applied to positive metadata and directory hits.
    pub ttl_ns: u64,
    /// TTL applied to negative lookups.
    pub negative_ttl_ns: u64,
}

impl Default for MetadataCachePolicy {
    fn default() -> Self {
        Self {
            ttl_ns: 5_000_000_000,
            negative_ttl_ns: 1_000_000_000,
        }
    }
}

/// Result of an in-memory metadata cache lookup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetadataCacheLookup<T> {
    /// The requested value was found and is still fresh.
    Hit(T),
    /// No fresh cache entry exists.
    Miss,
}

/// In-memory metadata cache for stat and readdir results.
#[derive(Clone, Debug, Default)]
pub struct MetadataCache {
    policy: MetadataCachePolicy,
    stat_entries: HashMap<String, MetadataEntry<Option<FileMetadata>>>,
    dir_entries: HashMap<String, MetadataEntry<Option<Vec<DirectoryEntry>>>>,
}

#[derive(Clone, Debug)]
struct MetadataEntry<T> {
    value: T,
    expires_at_ns: u64,
}

impl MetadataCache {
    /// Creates an empty metadata cache with the provided policy.
    #[must_use]
    pub fn new(policy: MetadataCachePolicy) -> Self {
        Self {
            policy,
            stat_entries: HashMap::new(),
            dir_entries: HashMap::new(),
        }
    }

    /// Stores the result of a stat/resolve operation.
    pub fn put_stat(&mut self, path: &str, metadata: Option<FileMetadata>, now_ns: u64) {
        self.stat_entries.insert(
            String::from(path),
            MetadataEntry {
                expires_at_ns: now_ns + ttl_for_option(self.policy, metadata.as_ref()),
                value: metadata,
            },
        );
    }

    /// Stores the result of a list-dir operation.
    pub fn put_dir(&mut self, path: &str, entries: Option<Vec<DirectoryEntry>>, now_ns: u64) {
        self.dir_entries.insert(
            String::from(path),
            MetadataEntry {
                expires_at_ns: now_ns + ttl_for_option(self.policy, entries.as_ref()),
                value: entries,
            },
        );
    }

    /// Returns a fresh cached stat/resolve result when available.
    pub fn stat(&mut self, path: &str, now_ns: u64) -> MetadataCacheLookup<Option<FileMetadata>> {
        lookup_entry(&mut self.stat_entries, path, now_ns)
    }

    /// Returns a fresh cached directory listing when available.
    pub fn list_dir(
        &mut self,
        path: &str,
        now_ns: u64,
    ) -> MetadataCacheLookup<Option<Vec<DirectoryEntry>>> {
        lookup_entry(&mut self.dir_entries, path, now_ns)
    }

    /// Applies an invalidation and removes affected cached metadata.
    pub fn apply_invalidation(&mut self, event: &InvalidationEvent) {
        let kind = InvalidationKind::try_from(event.kind).unwrap_or(InvalidationKind::Unspecified);
        match kind {
            InvalidationKind::File => {
                self.stat_entries.remove(&event.path);
                if let Some(parent) = Path::new(&event.path).parent() {
                    self.dir_entries.remove(parent.to_string_lossy().as_ref());
                }
            }
            InvalidationKind::Directory => {
                self.stat_entries.remove(&event.path);
                self.dir_entries.remove(&event.path);
                if let Some(parent) = Path::new(&event.path).parent() {
                    self.dir_entries.remove(parent.to_string_lossy().as_ref());
                }
            }
            InvalidationKind::Subtree | InvalidationKind::Unspecified => {
                self.stat_entries
                    .retain(|path, _| !path_starts_with(path, &event.path));
                self.dir_entries
                    .retain(|path, _| !path_starts_with(path, &event.path));
            }
        }
    }
}

/// Block data returned from the local cache store after integrity verification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CachedBlock {
    /// Identity of the cached block.
    pub key: CacheKey,
    /// Verified on-disk block bytes.
    pub data: Vec<u8>,
    /// Stored content hash for the block.
    pub content_hash: Vec<u8>,
    /// Current pin generation for eviction policy.
    pub pin_generation: u64,
}

/// Persistent client-side block cache backed by the cache SQLite DB and block files.
#[derive(Debug)]
pub struct BlockCacheStore {
    root_dir: PathBuf,
    connection: Connection,
}

impl BlockCacheStore {
    /// Creates a block cache store rooted at the provided directory.
    pub fn new(root_dir: &Path, connection: Connection) -> rusqlite::Result<Self> {
        fs::create_dir_all(root_dir)
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        Ok(Self {
            root_dir: root_dir.to_path_buf(),
            connection,
        })
    }

    /// Inserts or replaces one verified block in the local cache.
    pub fn put_block(
        &mut self,
        key: &CacheKey,
        block_count: u32,
        data: &[u8],
        pin_generation: u64,
        now_ns: u64,
    ) -> rusqlite::Result<CachedBlock> {
        let content_hash = blake3::hash(data).as_bytes().to_vec();
        let relative_path = block_storage_relative_path(&content_hash);
        let absolute_path = self.root_dir.join(&relative_path);
        if let Some(parent) = absolute_path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        }
        fs::write(&absolute_path, data)
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;

        self.connection.execute(
            "INSERT INTO cache_entries (
                 file_id, start_offset, block_count, content_hash, content_size, storage_relative_path,
                 last_access_ns, pin_generation, state
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'ready')
             ON CONFLICT(file_id, start_offset) DO UPDATE SET
                 block_count = excluded.block_count,
                 content_hash = excluded.content_hash,
                 content_size = excluded.content_size,
                 storage_relative_path = excluded.storage_relative_path,
                 last_access_ns = excluded.last_access_ns,
                 pin_generation = excluded.pin_generation,
                 state = 'ready'",
            params![
                key.file_id.0 as i64,
                key.start_offset as i64,
                block_count as i64,
                content_hash,
                data.len() as i64,
                relative_path.to_string_lossy(),
                now_ns as i64,
                pin_generation as i64
            ],
        )?;

        Ok(CachedBlock {
            key: key.clone(),
            data: data.to_vec(),
            content_hash: blake3::hash(data).as_bytes().to_vec(),
            pin_generation,
        })
    }

    /// Returns a verified cached block when it exists locally.
    pub fn get_block(
        &mut self,
        key: &CacheKey,
        now_ns: u64,
    ) -> rusqlite::Result<Option<CachedBlock>> {
        let row = self
            .connection
            .query_row(
                "SELECT content_hash, storage_relative_path, pin_generation
                 FROM cache_entries
                 WHERE file_id = ?1 AND start_offset = ?2 AND state = 'ready'",
                params![key.file_id.0 as i64, key.start_offset as i64],
                |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .optional()?;

        let Some((content_hash, relative_path, pin_generation)) = row else {
            return Ok(None);
        };

        let absolute_path = self.root_dir.join(relative_path);
        let data = fs::read(&absolute_path)
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        let actual_hash = blake3::hash(&data).as_bytes().to_vec();
        if actual_hash != content_hash {
            self.connection.execute(
                "DELETE FROM cache_entries WHERE file_id = ?1 AND start_offset = ?2",
                params![key.file_id.0 as i64, key.start_offset as i64],
            )?;
            let _ = fs::remove_file(&absolute_path);
            return Ok(None);
        }

        self.connection.execute(
            "UPDATE cache_entries SET last_access_ns = ?3 WHERE file_id = ?1 AND start_offset = ?2",
            params![key.file_id.0 as i64, key.start_offset as i64, now_ns as i64],
        )?;

        Ok(Some(CachedBlock {
            key: key.clone(),
            data,
            content_hash,
            pin_generation: pin_generation as u64,
        }))
    }

    /// Removes all cached blocks for one file, used to maintain metadata/block coherence.
    pub fn invalidate_file(&mut self, file_id: FileId) -> rusqlite::Result<usize> {
        let paths = collect_storage_paths_for_file(&self.connection, file_id)?;
        let deleted = self.connection.execute(
            "DELETE FROM cache_entries WHERE file_id = ?1",
            [file_id.0 as i64],
        )?;
        for path in paths {
            let _ = fs::remove_file(self.root_dir.join(path));
        }
        Ok(deleted)
    }

    /// Applies an invalidation to the block cache when file identity is known.
    pub fn apply_invalidation(&mut self, event: &InvalidationEvent) -> rusqlite::Result<()> {
        let kind = InvalidationKind::try_from(event.kind).unwrap_or(InvalidationKind::Unspecified);
        if matches!(
            kind,
            InvalidationKind::File | InvalidationKind::Directory | InvalidationKind::Subtree
        ) && event.file_id != 0
        {
            let _ = self.invalidate_file(FileId(event.file_id))?;
        }
        Ok(())
    }

    /// Records fetch state for one block range.
    pub fn record_fetch_state(
        &mut self,
        range: &BlockRange,
        priority: i32,
        state: &str,
        now_ns: u64,
    ) -> rusqlite::Result<()> {
        self.connection.execute(
            "INSERT INTO fetch_state (file_id, start_offset, block_count, priority, state, updated_at_ns)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(file_id, start_offset) DO UPDATE SET
                 block_count = excluded.block_count,
                 priority = excluded.priority,
                 state = excluded.state,
                 updated_at_ns = excluded.updated_at_ns",
            params![
                range.file_id.0 as i64,
                range.start_offset as i64,
                range.block_count as i64,
                priority,
                state,
                now_ns as i64
            ],
        )?;
        Ok(())
    }

    /// Records one pin generation for eviction-sensitive ranges.
    pub fn record_pin(
        &mut self,
        generation: u64,
        reason: &str,
        created_at_ns: u64,
    ) -> rusqlite::Result<()> {
        self.connection.execute(
            "INSERT OR REPLACE INTO pins (generation, reason, created_at_ns) VALUES (?1, ?2, ?3)",
            params![generation as i64, reason, created_at_ns as i64],
        )?;
        Ok(())
    }

    /// Exposes the underlying SQLite connection for higher-level orchestration tests.
    #[must_use]
    pub fn connection(&self) -> &Connection {
        &self.connection
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

fn ttl_for_option<T>(policy: MetadataCachePolicy, value: Option<&T>) -> u64 {
    if value.is_some() {
        policy.ttl_ns
    } else {
        policy.negative_ttl_ns
    }
}

fn lookup_entry<T: Clone>(
    entries: &mut HashMap<String, MetadataEntry<T>>,
    path: &str,
    now_ns: u64,
) -> MetadataCacheLookup<T> {
    let Some(entry) = entries.get(path) else {
        return MetadataCacheLookup::Miss;
    };
    if entry.expires_at_ns <= now_ns {
        entries.remove(path);
        return MetadataCacheLookup::Miss;
    }
    MetadataCacheLookup::Hit(entry.value.clone())
}

fn path_starts_with(path: &str, root: &str) -> bool {
    path == root
        || path
            .strip_prefix(root)
            .is_some_and(|suffix| suffix.is_empty() || suffix.starts_with('/'))
}

fn block_storage_relative_path(content_hash: &[u8]) -> PathBuf {
    let hex = content_hash
        .iter()
        .flat_map(|byte| [hex_char(byte >> 4), hex_char(byte & 0x0f)])
        .collect::<String>();
    PathBuf::from(&hex[0..2]).join(hex)
}

fn collect_storage_paths_for_file(
    connection: &Connection,
    file_id: FileId,
) -> rusqlite::Result<Vec<String>> {
    let mut statement = connection.prepare(
        "SELECT storage_relative_path FROM cache_entries WHERE file_id = ?1 ORDER BY start_offset",
    )?;
    statement
        .query_map([file_id.0 as i64], |row| row.get::<_, String>(0))?
        .collect()
}

fn hex_char(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        10..=15 => (b'a' + (value - 10)) as char,
        _ => '0',
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BlockCacheStore, CLIENT_CACHE_SCHEMA_VERSION, CacheConfig, CacheKey, MetadataCache,
        MetadataCacheLookup, MetadataCachePolicy, open_cache_database,
    };
    use legato_proto::{DirectoryEntry, FileMetadata, InvalidationEvent, InvalidationKind};
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
    fn metadata_cache_applies_ttl_negative_entries_and_invalidations() {
        let mut cache = MetadataCache::new(MetadataCachePolicy {
            ttl_ns: 10,
            negative_ttl_ns: 5,
        });
        cache.put_stat(
            "/srv/libraries/Kontakt/piano.nki",
            Some(FileMetadata {
                file_id: 7,
                path: String::from("/srv/libraries/Kontakt/piano.nki"),
                size: 9,
                mtime_ns: 3,
                content_hash: Vec::new(),
                is_dir: false,
                block_size: 4096,
            }),
            100,
        );
        cache.put_dir(
            "/srv/libraries/Kontakt",
            Some(vec![DirectoryEntry {
                name: String::from("piano.nki"),
                path: String::from("/srv/libraries/Kontakt/piano.nki"),
                is_dir: false,
                file_id: 7,
            }]),
            100,
        );
        cache.put_stat("/srv/libraries/Missing.nki", None, 100);

        assert!(matches!(
            cache.stat("/srv/libraries/Kontakt/piano.nki", 105),
            MetadataCacheLookup::Hit(Some(_))
        ));
        assert!(matches!(
            cache.stat("/srv/libraries/Missing.nki", 104),
            MetadataCacheLookup::Hit(None)
        ));
        assert!(matches!(
            cache.stat("/srv/libraries/Missing.nki", 106),
            MetadataCacheLookup::Miss
        ));

        cache.apply_invalidation(&InvalidationEvent {
            kind: InvalidationKind::File as i32,
            path: String::from("/srv/libraries/Kontakt/piano.nki"),
            file_id: 7,
        });
        assert!(matches!(
            cache.stat("/srv/libraries/Kontakt/piano.nki", 105),
            MetadataCacheLookup::Miss
        ));
        assert!(matches!(
            cache.list_dir("/srv/libraries/Kontakt", 105),
            MetadataCacheLookup::Miss
        ));
    }

    #[test]
    fn block_cache_store_round_trips_and_verifies_integrity() {
        let temp = tempdir().expect("tempdir should be created");
        let connection = open_cache_database(&temp.path().join("cache").join("client.sqlite"))
            .expect("cache database should open");
        let mut store = BlockCacheStore::new(&temp.path().join("blocks"), connection)
            .expect("store should open");
        let key = CacheKey {
            file_id: FileId(7),
            start_offset: 0,
        };

        let cached = store
            .put_block(&key, 1, b"fixture", 2, 100)
            .expect("block should be cached");
        let loaded = store
            .get_block(&key, 200)
            .expect("cached block should load")
            .expect("cached block should exist");

        assert_eq!(cached.data, b"fixture");
        assert_eq!(loaded.data, b"fixture");
        assert_eq!(loaded.pin_generation, 2);
    }

    #[test]
    fn block_cache_store_invalidates_corrupt_or_stale_entries() {
        let temp = tempdir().expect("tempdir should be created");
        let connection = open_cache_database(&temp.path().join("cache").join("client.sqlite"))
            .expect("cache database should open");
        let mut store = BlockCacheStore::new(&temp.path().join("blocks"), connection)
            .expect("store should open");
        let key = CacheKey {
            file_id: FileId(7),
            start_offset: 0,
        };
        let cached = store
            .put_block(&key, 1, b"fixture", 2, 100)
            .expect("block should be cached");
        let path: String = store
            .connection()
            .query_row(
                "SELECT storage_relative_path FROM cache_entries WHERE file_id = 7 AND start_offset = 0",
                [],
                |row| row.get(0),
            )
            .expect("storage path should exist");
        std::fs::write(temp.path().join("blocks").join(path), b"corrupt")
            .expect("fixture should be corrupted");

        assert!(
            store
                .get_block(&key, 200)
                .expect("corrupt entry read should succeed")
                .is_none()
        );
        assert_eq!(cached.key.file_id, FileId(7));
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
