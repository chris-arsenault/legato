//! Local metadata and block-cache primitives shared by client-side components.

mod schema;

use std::{
    collections::{BTreeSet, HashMap},
    fs,
    path::{Path, PathBuf},
};

use legato_proto::{
    DirectoryEntry, ExtentRecord, FileMetadata, InvalidationEvent, InvalidationKind, TransferClass,
};
use legato_types::{BlockRange, FileId};
use rusqlite::{Connection, OptionalExtension, params};
pub use schema::{CLIENT_CACHE_SCHEMA_VERSION, cache_migrations};
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

const EXTENT_STORE_DIRTY_STATE_KEY: &str = "extent_store.dirty";
const EXTENT_STORE_CHECKPOINT_KEY: &str = "extent_store.checkpoint";
const EXTENT_STORE_PIN_GENERATION_KEY: &str = "extent_store.pin_generation";

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
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
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

/// Identity for one semantic extent in the local store.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ExtentKey {
    /// Stable identifier of the file containing the extent.
    pub file_id: FileId,
    /// Logical extent index within the file layout.
    pub extent_index: u32,
}

/// Extent data returned from the local extent store after integrity verification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CachedExtent {
    /// Identity of the cached extent.
    pub key: ExtentKey,
    /// File-relative starting offset represented by the extent.
    pub file_offset: u64,
    /// Verified on-disk extent bytes.
    pub data: Vec<u8>,
    /// Stored content hash for the extent.
    pub content_hash: Vec<u8>,
    /// Transfer class persisted for the extent's file layout.
    pub transfer_class: TransferClass,
    /// Current pin generation for eviction policy.
    pub pin_generation: u64,
}

/// Summary of one cache maintenance pass.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CacheMaintenanceReport {
    /// Total bytes represented by cache entries before the maintenance pass.
    pub bytes_before: u64,
    /// Total bytes represented by cache entries after the maintenance pass.
    pub bytes_after: u64,
    /// Number of cache entries removed by maintenance.
    pub entries_removed: usize,
    /// Number of corrupt or missing entries repaired from the metadata database.
    pub repaired_entries: usize,
    /// Number of orphaned files removed from the block store.
    pub orphan_files_removed: usize,
    /// Total bytes reclaimed across removed entries and orphan files.
    pub reclaimed_bytes: u64,
}

/// Durable checkpoint summary for the local extent store.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ExtentStoreCheckpoint {
    /// Monotonic checkpoint format version for future upgrades.
    pub version: u32,
    /// Timestamp at which the checkpoint was committed.
    pub updated_at_ns: u64,
    /// Current logical bytes tracked by the extent catalog.
    pub total_bytes: u64,
    /// Number of extent entries present in the local residency catalog.
    pub extent_entries: u64,
    /// Highest pin generation allocated by the local store.
    pub pin_generation: u64,
}

/// Summary of one extent-store compaction pass.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ExtentStoreCompactionReport {
    /// Number of stale extent fetch-state rows removed.
    pub stale_fetch_rows_removed: usize,
    /// Number of unreferenced pin rows removed.
    pub stale_pins_removed: usize,
    /// Number of empty directories removed from the extent root.
    pub empty_directories_removed: usize,
}

/// Summary of one restart or startup recovery pass.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtentStoreRecoveryReport {
    /// Whether the store was marked dirty before recovery began.
    pub recovered_dirty_store: bool,
    /// Checkpoint observed before recovery, when present.
    pub checkpoint_before: Option<ExtentStoreCheckpoint>,
    /// Checkpoint committed after recovery completed.
    pub checkpoint_after: ExtentStoreCheckpoint,
    /// Repair report executed during recovery.
    pub repair: CacheMaintenanceReport,
    /// Compaction report executed during recovery.
    pub compaction: ExtentStoreCompactionReport,
    /// Eviction report executed during recovery.
    pub eviction: CacheMaintenanceReport,
}

/// Persistent client-side block cache backed by the cache SQLite DB and block files.
#[derive(Debug)]
pub struct BlockCacheStore {
    root_dir: PathBuf,
    connection: Connection,
}

/// Persistent client-side extent store backed by the cache SQLite DB and extent files.
#[derive(Debug)]
pub struct ExtentCacheStore {
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

    /// Returns the logical size of all tracked cache entries.
    pub fn total_size_bytes(&self) -> rusqlite::Result<u64> {
        self.connection
            .query_row(
                "SELECT COALESCE(SUM(content_size), 0) FROM cache_entries",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|total| total.max(0) as u64)
    }

    /// Evicts least-recently-used, oldest-pin entries until the cache fits within the limit.
    pub fn evict_to_limit(&mut self, max_bytes: u64) -> rusqlite::Result<CacheMaintenanceReport> {
        let bytes_before = self.total_size_bytes()?;
        if bytes_before <= max_bytes {
            return Ok(CacheMaintenanceReport {
                bytes_before,
                bytes_after: bytes_before,
                ..CacheMaintenanceReport::default()
            });
        }

        let mut report = CacheMaintenanceReport {
            bytes_before,
            bytes_after: bytes_before,
            ..CacheMaintenanceReport::default()
        };
        let mut statement = self.connection.prepare(
            "SELECT file_id, start_offset, content_size, storage_relative_path
             FROM cache_entries
             ORDER BY pin_generation ASC, last_access_ns ASC, start_offset ASC",
        )?;
        let victims = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        let mut remaining = bytes_before;
        for (file_id, start_offset, content_size, relative_path) in victims {
            if remaining <= max_bytes {
                break;
            }
            self.connection.execute(
                "DELETE FROM cache_entries WHERE file_id = ?1 AND start_offset = ?2",
                params![file_id, start_offset],
            )?;
            let _ = fs::remove_file(self.root_dir.join(&relative_path));
            report.entries_removed += 1;
            report.reclaimed_bytes = report
                .reclaimed_bytes
                .saturating_add(content_size.max(0) as u64);
            remaining = remaining.saturating_sub(content_size.max(0) as u64);
        }

        report.bytes_after = self.total_size_bytes()?;
        Ok(report)
    }

    /// Removes corrupt, missing, and orphaned cache artifacts from the local store.
    pub fn repair(&mut self) -> rusqlite::Result<CacheMaintenanceReport> {
        let bytes_before = self.total_size_bytes()?;
        let mut report = CacheMaintenanceReport {
            bytes_before,
            bytes_after: bytes_before,
            ..CacheMaintenanceReport::default()
        };
        let mut statement = self.connection.prepare(
            "SELECT file_id, start_offset, content_hash, content_size, storage_relative_path
             FROM cache_entries",
        )?;
        let entries = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut referenced_paths = BTreeSet::new();

        for (file_id, start_offset, expected_hash, content_size, relative_path) in entries {
            let absolute_path = self.root_dir.join(&relative_path);
            referenced_paths.insert(relative_path.clone());
            let healthy = fs::read(&absolute_path)
                .ok()
                .is_some_and(|data| blake3::hash(&data).as_bytes() == expected_hash.as_slice());

            if healthy {
                continue;
            }

            self.connection.execute(
                "DELETE FROM cache_entries WHERE file_id = ?1 AND start_offset = ?2",
                params![file_id, start_offset],
            )?;
            let _ = fs::remove_file(&absolute_path);
            report.entries_removed += 1;
            report.repaired_entries += 1;
            report.reclaimed_bytes = report
                .reclaimed_bytes
                .saturating_add(content_size.max(0) as u64);
        }

        for entry in WalkDir::new(&self.root_dir)
            .into_iter()
            .filter_map(Result::ok)
        {
            if !entry.file_type().is_file() {
                continue;
            }
            let relative_path = entry
                .path()
                .strip_prefix(&self.root_dir)
                .expect("walkdir entries should remain under the root")
                .to_string_lossy()
                .into_owned();
            if referenced_paths.contains(&relative_path) {
                continue;
            }

            let size = entry.metadata().map_or(0, |metadata| metadata.len());
            fs::remove_file(entry.path())
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
            report.orphan_files_removed += 1;
            report.reclaimed_bytes = report.reclaimed_bytes.saturating_add(size);
        }

        report.bytes_after = self.total_size_bytes()?;
        Ok(report)
    }
}

impl ExtentCacheStore {
    /// Creates an extent store rooted at the provided directory.
    pub fn new(root_dir: &Path, connection: Connection) -> rusqlite::Result<Self> {
        fs::create_dir_all(root_dir)
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        Ok(Self {
            root_dir: root_dir.to_path_buf(),
            connection,
        })
    }

    /// Returns the current highest pin generation known to the extent store.
    pub fn current_pin_generation(&self) -> rusqlite::Result<u64> {
        if let Some(value) = self.client_state_value(EXTENT_STORE_PIN_GENERATION_KEY)? {
            return Ok(value.parse::<u64>().unwrap_or(0));
        }

        self.connection
            .query_row("SELECT COALESCE(MAX(generation), 0) FROM pins", [], |row| {
                row.get::<_, i64>(0)
            })
            .map(|value| value.max(0) as u64)
    }

    /// Allocates and persists the next pin generation for eviction-sensitive residency.
    pub fn begin_pin_generation(
        &mut self,
        reason: &str,
        created_at_ns: u64,
    ) -> rusqlite::Result<u64> {
        let generation = self.current_pin_generation()?.saturating_add(1);
        self.record_pin(generation, reason, created_at_ns)?;
        Ok(generation)
    }

    /// Inserts or replaces one verified extent in the local store.
    pub fn put_extent(
        &mut self,
        record: &ExtentRecord,
        pin_generation: u64,
        now_ns: u64,
    ) -> rusqlite::Result<CachedExtent> {
        self.mark_dirty()?;
        let transfer_class =
            TransferClass::try_from(record.transfer_class).unwrap_or(TransferClass::Unspecified);
        let content_hash = blake3::hash(&record.data).as_bytes().to_vec();
        let relative_path = extent_storage_relative_path(record.file_id, record.extent_index);
        let absolute_path = self.root_dir.join(&relative_path);
        if let Some(parent) = absolute_path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        }
        fs::write(&absolute_path, &record.data)
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;

        self.connection.execute(
            "INSERT INTO extent_entries (
                 file_id, extent_index, file_offset, extent_length, transfer_class, content_hash,
                 content_size, storage_relative_path, last_access_ns, pin_generation, state
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 'ready')
             ON CONFLICT(file_id, extent_index) DO UPDATE SET
                 file_offset = excluded.file_offset,
                 extent_length = excluded.extent_length,
                 transfer_class = excluded.transfer_class,
                 content_hash = excluded.content_hash,
                 content_size = excluded.content_size,
                 storage_relative_path = excluded.storage_relative_path,
                 last_access_ns = excluded.last_access_ns,
                 pin_generation = excluded.pin_generation,
                 state = 'ready'",
            params![
                record.file_id as i64,
                record.extent_index as i64,
                record.file_offset as i64,
                record.data.len() as i64,
                record.transfer_class,
                content_hash,
                record.data.len() as i64,
                relative_path.to_string_lossy(),
                now_ns as i64,
                pin_generation as i64,
            ],
        )?;

        Ok(CachedExtent {
            key: ExtentKey {
                file_id: FileId(record.file_id),
                extent_index: record.extent_index,
            },
            file_offset: record.file_offset,
            data: record.data.clone(),
            content_hash: blake3::hash(&record.data).as_bytes().to_vec(),
            transfer_class,
            pin_generation,
        })
    }

    /// Returns a verified cached extent when it exists locally.
    pub fn get_extent(
        &mut self,
        key: &ExtentKey,
        now_ns: u64,
    ) -> rusqlite::Result<Option<CachedExtent>> {
        let row = self
            .connection
            .query_row(
                "SELECT file_offset, transfer_class, content_hash, storage_relative_path, pin_generation
                 FROM extent_entries
                 WHERE file_id = ?1 AND extent_index = ?2 AND state = 'ready'",
                params![key.file_id.0 as i64, key.extent_index as i64],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, i64>(4)?,
                    ))
                },
            )
            .optional()?;

        let Some((file_offset, transfer_class, content_hash, relative_path, pin_generation)) = row
        else {
            return Ok(None);
        };

        let absolute_path = self.root_dir.join(&relative_path);
        let data = fs::read(&absolute_path)
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        let actual_hash = blake3::hash(&data).as_bytes().to_vec();
        if actual_hash != content_hash {
            self.connection.execute(
                "DELETE FROM extent_entries WHERE file_id = ?1 AND extent_index = ?2",
                params![key.file_id.0 as i64, key.extent_index as i64],
            )?;
            let _ = fs::remove_file(&absolute_path);
            self.mark_dirty()?;
            return Ok(None);
        }

        self.connection.execute(
            "UPDATE extent_entries SET last_access_ns = ?3 WHERE file_id = ?1 AND extent_index = ?2",
            params![key.file_id.0 as i64, key.extent_index as i64, now_ns as i64],
        )?;

        Ok(Some(CachedExtent {
            key: key.clone(),
            file_offset: file_offset as u64,
            data,
            content_hash,
            transfer_class: TransferClass::try_from(transfer_class as i32)
                .unwrap_or(TransferClass::Unspecified),
            pin_generation: pin_generation as u64,
        }))
    }

    /// Removes all cached extents for one file.
    pub fn invalidate_file(&mut self, file_id: FileId) -> rusqlite::Result<usize> {
        self.mark_dirty()?;
        let paths = collect_extent_storage_paths_for_file(&self.connection, file_id)?;
        let deleted = self.connection.execute(
            "DELETE FROM extent_entries WHERE file_id = ?1",
            [file_id.0 as i64],
        )?;
        for path in paths {
            let _ = fs::remove_file(self.root_dir.join(path));
        }
        Ok(deleted)
    }

    /// Applies an invalidation to the extent store when file identity is known.
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

    /// Records fetch state for one extent.
    pub fn record_extent_fetch_state(
        &mut self,
        key: &ExtentKey,
        priority: i32,
        state: &str,
        now_ns: u64,
    ) -> rusqlite::Result<()> {
        self.mark_dirty()?;
        self.connection.execute(
            "INSERT INTO extent_fetch_state (file_id, extent_index, priority, state, updated_at_ns)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(file_id, extent_index) DO UPDATE SET
                 priority = excluded.priority,
                 state = excluded.state,
                 updated_at_ns = excluded.updated_at_ns",
            params![
                key.file_id.0 as i64,
                key.extent_index as i64,
                priority,
                state,
                now_ns as i64
            ],
        )?;
        Ok(())
    }

    /// Records one pin generation for eviction-sensitive extents.
    pub fn record_pin(
        &mut self,
        generation: u64,
        reason: &str,
        created_at_ns: u64,
    ) -> rusqlite::Result<()> {
        self.mark_dirty()?;
        self.connection.execute(
            "INSERT OR REPLACE INTO pins (generation, reason, created_at_ns) VALUES (?1, ?2, ?3)",
            params![generation as i64, reason, created_at_ns as i64],
        )?;
        self.set_client_state_value(EXTENT_STORE_PIN_GENERATION_KEY, &generation.to_string())?;
        Ok(())
    }

    /// Exposes the underlying SQLite connection for higher-level orchestration tests.
    #[must_use]
    pub fn connection(&self) -> &Connection {
        &self.connection
    }

    /// Returns the logical size of all tracked extent entries.
    pub fn total_size_bytes(&self) -> rusqlite::Result<u64> {
        self.connection
            .query_row(
                "SELECT COALESCE(SUM(content_size), 0) FROM extent_entries",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|total| total.max(0) as u64)
    }

    /// Evicts least-recently-used, oldest-pin extents until the store fits within the limit.
    pub fn evict_to_limit(&mut self, max_bytes: u64) -> rusqlite::Result<CacheMaintenanceReport> {
        let bytes_before = self.total_size_bytes()?;
        if bytes_before <= max_bytes {
            return Ok(CacheMaintenanceReport {
                bytes_before,
                bytes_after: bytes_before,
                ..CacheMaintenanceReport::default()
            });
        }

        let mut report = CacheMaintenanceReport {
            bytes_before,
            bytes_after: bytes_before,
            ..CacheMaintenanceReport::default()
        };
        let mut statement = self.connection.prepare(
            "SELECT extent_entries.file_id, extent_entries.extent_index, extent_entries.content_size,
                    extent_entries.storage_relative_path
             FROM extent_entries
             LEFT JOIN extent_fetch_state
               ON extent_fetch_state.file_id = extent_entries.file_id
              AND extent_fetch_state.extent_index = extent_entries.extent_index
             ORDER BY extent_entries.pin_generation ASC,
                      COALESCE(extent_fetch_state.priority, 10_000) DESC,
                      extent_entries.last_access_ns ASC,
                      extent_entries.extent_index ASC",
        )?;
        let victims = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        let mut remaining = bytes_before;
        for (file_id, extent_index, content_size, relative_path) in victims {
            if remaining <= max_bytes {
                break;
            }
            self.connection.execute(
                "DELETE FROM extent_entries WHERE file_id = ?1 AND extent_index = ?2",
                params![file_id, extent_index],
            )?;
            let _ = fs::remove_file(self.root_dir.join(&relative_path));
            report.entries_removed += 1;
            report.reclaimed_bytes = report
                .reclaimed_bytes
                .saturating_add(content_size.max(0) as u64);
            remaining = remaining.saturating_sub(content_size.max(0) as u64);
        }

        report.bytes_after = self.total_size_bytes()?;
        Ok(report)
    }

    /// Compacts stale extent metadata and trims empty directories from the extent root.
    pub fn compact(&mut self) -> rusqlite::Result<ExtentStoreCompactionReport> {
        let stale_fetch_rows_removed = self.connection.execute(
            "DELETE FROM extent_fetch_state
             WHERE NOT EXISTS (
                 SELECT 1
                 FROM extent_entries
                 WHERE extent_entries.file_id = extent_fetch_state.file_id
                   AND extent_entries.extent_index = extent_fetch_state.extent_index
             )",
            [],
        )?;
        let stale_pins_removed = self.connection.execute(
            "DELETE FROM pins
             WHERE generation NOT IN (
                 SELECT DISTINCT pin_generation
                 FROM extent_entries
                 WHERE pin_generation > 0
             )",
            [],
        )?;
        let empty_directories_removed = trim_empty_directories(&self.root_dir)?;

        Ok(ExtentStoreCompactionReport {
            stale_fetch_rows_removed,
            stale_pins_removed,
            empty_directories_removed,
        })
    }

    /// Removes corrupt, missing, and orphaned extent artifacts from the local store.
    pub fn repair(&mut self) -> rusqlite::Result<CacheMaintenanceReport> {
        let bytes_before = self.total_size_bytes()?;
        let mut report = CacheMaintenanceReport {
            bytes_before,
            bytes_after: bytes_before,
            ..CacheMaintenanceReport::default()
        };
        let mut statement = self.connection.prepare(
            "SELECT file_id, extent_index, content_hash, content_size, storage_relative_path
             FROM extent_entries",
        )?;
        let entries = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut referenced_paths = BTreeSet::new();

        for (file_id, extent_index, expected_hash, content_size, relative_path) in entries {
            let absolute_path = self.root_dir.join(&relative_path);
            referenced_paths.insert(relative_path.clone());
            let healthy = fs::read(&absolute_path)
                .ok()
                .is_some_and(|data| blake3::hash(&data).as_bytes() == expected_hash.as_slice());

            if healthy {
                continue;
            }

            self.connection.execute(
                "DELETE FROM extent_entries WHERE file_id = ?1 AND extent_index = ?2",
                params![file_id, extent_index],
            )?;
            let _ = fs::remove_file(&absolute_path);
            report.entries_removed += 1;
            report.repaired_entries += 1;
            report.reclaimed_bytes = report
                .reclaimed_bytes
                .saturating_add(content_size.max(0) as u64);
        }

        for entry in WalkDir::new(&self.root_dir)
            .into_iter()
            .filter_map(Result::ok)
        {
            if !entry.file_type().is_file() {
                continue;
            }
            let relative_path = entry
                .path()
                .strip_prefix(&self.root_dir)
                .expect("walkdir entries should remain under the root")
                .to_string_lossy()
                .into_owned();
            if referenced_paths.contains(&relative_path) {
                continue;
            }

            let size = entry.metadata().map_or(0, |metadata| metadata.len());
            fs::remove_file(entry.path())
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
            report.orphan_files_removed += 1;
            report.reclaimed_bytes = report.reclaimed_bytes.saturating_add(size);
        }

        report.bytes_after = self.total_size_bytes()?;
        Ok(report)
    }

    /// Returns the latest committed extent-store checkpoint when one exists.
    pub fn checkpoint(&mut self, now_ns: u64) -> rusqlite::Result<ExtentStoreCheckpoint> {
        let checkpoint = ExtentStoreCheckpoint {
            version: 1,
            updated_at_ns: now_ns,
            total_bytes: self.total_size_bytes()?,
            extent_entries: self
                .connection
                .query_row("SELECT COUNT(*) FROM extent_entries", [], |row| {
                    row.get::<_, i64>(0)
                })?
                .max(0) as u64,
            pin_generation: self.current_pin_generation()?,
        };
        let json = serde_json::to_string(&checkpoint)
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        self.set_client_state_value(EXTENT_STORE_CHECKPOINT_KEY, &json)?;
        self.set_client_state_value(EXTENT_STORE_DIRTY_STATE_KEY, "0")?;
        Ok(checkpoint)
    }

    /// Loads the most recent extent-store checkpoint from durable client state.
    pub fn load_checkpoint(&self) -> rusqlite::Result<Option<ExtentStoreCheckpoint>> {
        self.client_state_value(EXTENT_STORE_CHECKPOINT_KEY)?
            .map(|value| {
                serde_json::from_str(&value)
                    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
            })
            .transpose()
    }

    /// Returns whether the extent store was left dirty by a prior uncheckpointed mutation.
    pub fn is_dirty(&self) -> rusqlite::Result<bool> {
        Ok(self
            .client_state_value(EXTENT_STORE_DIRTY_STATE_KEY)?
            .is_some_and(|value| value == "1"))
    }

    /// Repairs, compacts, evicts, and checkpoints the local extent store for startup recovery.
    pub fn recover(
        &mut self,
        max_bytes: u64,
        now_ns: u64,
    ) -> rusqlite::Result<ExtentStoreRecoveryReport> {
        let checkpoint_before = self.load_checkpoint()?;
        let recovered_dirty_store = self.is_dirty()? || checkpoint_before.is_none();
        let repair = if recovered_dirty_store {
            self.repair()?
        } else {
            let bytes = self.total_size_bytes()?;
            CacheMaintenanceReport {
                bytes_before: bytes,
                bytes_after: bytes,
                ..CacheMaintenanceReport::default()
            }
        };
        let compaction = self.compact()?;
        let eviction = self.evict_to_limit(max_bytes)?;
        let checkpoint_after = self.checkpoint(now_ns)?;

        Ok(ExtentStoreRecoveryReport {
            recovered_dirty_store,
            checkpoint_before,
            checkpoint_after,
            repair,
            compaction,
            eviction,
        })
    }

    fn mark_dirty(&self) -> rusqlite::Result<()> {
        self.set_client_state_value(EXTENT_STORE_DIRTY_STATE_KEY, "1")
    }

    fn client_state_value(&self, key: &str) -> rusqlite::Result<Option<String>> {
        self.connection
            .query_row(
                "SELECT value FROM client_state WHERE key = ?1",
                [key],
                |row| row.get::<_, String>(0),
            )
            .optional()
    }

    fn set_client_state_value(&self, key: &str, value: &str) -> rusqlite::Result<()> {
        self.connection.execute(
            "INSERT INTO client_state (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )?;
        Ok(())
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

fn extent_storage_relative_path(file_id: u64, extent_index: u32) -> PathBuf {
    PathBuf::from(file_id.to_string()).join(format!("{extent_index}.bin"))
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

fn collect_extent_storage_paths_for_file(
    connection: &Connection,
    file_id: FileId,
) -> rusqlite::Result<Vec<String>> {
    let mut statement = connection.prepare(
        "SELECT storage_relative_path FROM extent_entries WHERE file_id = ?1 ORDER BY extent_index",
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

fn trim_empty_directories(root_dir: &Path) -> rusqlite::Result<usize> {
    let mut removed = 0;
    let mut directories = WalkDir::new(root_dir)
        .min_depth(1)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_dir())
        .map(walkdir::DirEntry::into_path)
        .collect::<Vec<_>>();
    directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));

    for directory in directories {
        if fs::read_dir(&directory)
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?
            .next()
            .is_none()
        {
            fs::remove_dir(&directory)
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
            removed += 1;
        }
    }

    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::{
        BlockCacheStore, CLIENT_CACHE_SCHEMA_VERSION, CacheConfig, CacheKey, CachedExtent,
        ExtentCacheStore, ExtentKey, MetadataCache, MetadataCacheLookup, MetadataCachePolicy,
        block_storage_relative_path, open_cache_database,
    };
    use legato_proto::{
        DirectoryEntry, ExtentRecord, FileMetadata, InvalidationEvent, InvalidationKind,
        TransferClass,
    };
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
                 WHERE type = 'table' AND name IN ('cache_entries', 'client_state', 'extent_entries', 'extent_fetch_state', 'fetch_state', 'pins') \
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
                String::from("extent_entries"),
                String::from("extent_fetch_state"),
                String::from("fetch_state"),
                String::from("pins"),
            ]
        );
    }

    #[test]
    fn extent_cache_store_round_trips_and_verifies_integrity() {
        let temp = tempdir().expect("tempdir should be created");
        let connection = open_cache_database(&temp.path().join("cache").join("client.sqlite"))
            .expect("cache database should open");
        let mut store = ExtentCacheStore::new(&temp.path().join("extents"), connection)
            .expect("store should open");

        let cached = store
            .put_extent(
                &ExtentRecord {
                    file_id: 7,
                    extent_index: 1,
                    file_offset: 4096,
                    data: b"fixture-extent".to_vec(),
                    extent_hash: Vec::new(),
                    transfer_class: TransferClass::Streamed as i32,
                },
                2,
                100,
            )
            .expect("extent should be cached");
        let loaded = store
            .get_extent(
                &ExtentKey {
                    file_id: FileId(7),
                    extent_index: 1,
                },
                200,
            )
            .expect("cached extent should load")
            .expect("cached extent should exist");

        assert_eq!(
            cached,
            CachedExtent {
                key: ExtentKey {
                    file_id: FileId(7),
                    extent_index: 1,
                },
                file_offset: 4096,
                data: b"fixture-extent".to_vec(),
                content_hash: blake3::hash(b"fixture-extent").as_bytes().to_vec(),
                transfer_class: TransferClass::Streamed,
                pin_generation: 2,
            }
        );
        assert_eq!(loaded.data, b"fixture-extent");
        assert_eq!(loaded.file_offset, 4096);
        assert_eq!(loaded.transfer_class, TransferClass::Streamed);
    }

    #[test]
    fn extent_cache_store_evicts_low_utility_and_older_pin_generations_first() {
        let temp = tempdir().expect("tempdir should be created");
        let connection = open_cache_database(&temp.path().join("cache").join("client.sqlite"))
            .expect("cache database should open");
        let mut store = ExtentCacheStore::new(&temp.path().join("extents"), connection)
            .expect("store should open");

        let older_generation = store
            .begin_pin_generation("prefetch", 100)
            .expect("older generation should allocate");
        let newer_generation = store
            .begin_pin_generation("prefetch", 200)
            .expect("newer generation should allocate");

        for (extent_index, pin_generation, priority, last_access_ns) in [
            (0_u32, older_generation, 3_i32, 110_u64),
            (1_u32, older_generation, 0_i32, 120_u64),
            (2_u32, newer_generation, 0_i32, 130_u64),
        ] {
            let key = ExtentKey {
                file_id: FileId(7),
                extent_index,
            };
            let _ = store
                .put_extent(
                    &ExtentRecord {
                        file_id: 7,
                        extent_index,
                        file_offset: u64::from(extent_index) * 4096,
                        data: format!("extent-{extent_index}").into_bytes(),
                        extent_hash: Vec::new(),
                        transfer_class: TransferClass::Streamed as i32,
                    },
                    pin_generation,
                    last_access_ns,
                )
                .expect("extent should be inserted");
            store
                .record_extent_fetch_state(&key, priority, "resident", last_access_ns)
                .expect("fetch state should be recorded");
            let _ = store
                .get_extent(&key, last_access_ns)
                .expect("extent should load")
                .expect("extent should exist");
        }

        let report = store
            .evict_to_limit(("extent-1".len() + "extent-2".len()) as u64)
            .expect("eviction should succeed");

        assert_eq!(report.entries_removed, 1);
        assert!(
            store
                .get_extent(
                    &ExtentKey {
                        file_id: FileId(7),
                        extent_index: 0,
                    },
                    500,
                )
                .expect("lookup should succeed")
                .is_none()
        );
        assert!(
            store
                .get_extent(
                    &ExtentKey {
                        file_id: FileId(7),
                        extent_index: 1,
                    },
                    500,
                )
                .expect("lookup should succeed")
                .is_some()
        );
        assert!(
            store
                .get_extent(
                    &ExtentKey {
                        file_id: FileId(7),
                        extent_index: 2,
                    },
                    500,
                )
                .expect("lookup should succeed")
                .is_some()
        );
    }

    #[test]
    fn extent_cache_store_compacts_stale_state_and_empty_directories() {
        let temp = tempdir().expect("tempdir should be created");
        let connection = open_cache_database(&temp.path().join("cache").join("client.sqlite"))
            .expect("cache database should open");
        let mut store = ExtentCacheStore::new(&temp.path().join("extents"), connection)
            .expect("store should open");
        let generation = store
            .begin_pin_generation("prefetch", 100)
            .expect("generation should allocate");
        let _ = store
            .put_extent(
                &ExtentRecord {
                    file_id: 7,
                    extent_index: 1,
                    file_offset: 0,
                    data: b"fixture".to_vec(),
                    extent_hash: Vec::new(),
                    transfer_class: TransferClass::Streamed as i32,
                },
                generation,
                101,
            )
            .expect("extent should be inserted");

        store
            .record_extent_fetch_state(
                &ExtentKey {
                    file_id: FileId(9),
                    extent_index: 3,
                },
                3,
                "queued",
                102,
            )
            .expect("stale fetch state should insert");
        store
            .record_pin(generation + 1, "orphan", 103)
            .expect("orphan pin should insert");

        let empty_dir = temp.path().join("extents").join("ff").join("empty");
        std::fs::create_dir_all(&empty_dir).expect("empty directory should exist");

        let report = store.compact().expect("compaction should succeed");

        assert_eq!(report.stale_fetch_rows_removed, 1);
        assert_eq!(report.stale_pins_removed, 1);
        assert!(report.empty_directories_removed >= 1);
        assert!(
            store
                .connection()
                .query_row(
                    "SELECT COUNT(*) FROM extent_fetch_state WHERE file_id = 9 AND extent_index = 3",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("fetch-state count should load")
                == 0
        );
        assert!(!empty_dir.exists());
    }

    #[test]
    fn extent_cache_store_recovery_repairs_dirty_state_and_writes_checkpoint() {
        let temp = tempdir().expect("tempdir should be created");
        let connection = open_cache_database(&temp.path().join("cache").join("client.sqlite"))
            .expect("cache database should open");
        let mut store = ExtentCacheStore::new(&temp.path().join("extents"), connection)
            .expect("store should open");

        let generation = store
            .begin_pin_generation("prefetch", 100)
            .expect("generation should allocate");
        let _ = store
            .put_extent(
                &ExtentRecord {
                    file_id: 7,
                    extent_index: 0,
                    file_offset: 0,
                    data: b"fixture".to_vec(),
                    extent_hash: Vec::new(),
                    transfer_class: TransferClass::Streamed as i32,
                },
                generation,
                101,
            )
            .expect("extent should be inserted");

        let relative_path: String = store
            .connection()
            .query_row(
                "SELECT storage_relative_path FROM extent_entries WHERE file_id = 7 AND extent_index = 0",
                [],
                |row| row.get(0),
            )
            .expect("storage path should load");
        std::fs::write(temp.path().join("extents").join(&relative_path), b"corrupt")
            .expect("extent file should be corrupted");

        let orphan = temp.path().join("extents").join("aa").join("orphan.bin");
        std::fs::create_dir_all(orphan.parent().expect("orphan parent should exist"))
            .expect("orphan directory should be created");
        std::fs::write(&orphan, b"orphan").expect("orphan file should be created");

        assert!(store.is_dirty().expect("dirty state should load"));

        let report = store.recover(1024, 300).expect("recovery should succeed");

        assert!(report.recovered_dirty_store);
        assert_eq!(report.repair.repaired_entries, 1);
        assert_eq!(report.repair.orphan_files_removed, 1);
        assert!(!store.is_dirty().expect("dirty state should load"));
        assert_eq!(
            store
                .load_checkpoint()
                .expect("checkpoint should load")
                .expect("checkpoint should exist")
                .updated_at_ns,
            300
        );
        assert!(!orphan.exists());
    }

    #[test]
    fn block_cache_store_evicts_oldest_entries_to_fit_budget() {
        let temp = tempdir().expect("tempdir should be created");
        let connection = open_cache_database(&temp.path().join("cache").join("client.sqlite"))
            .expect("cache database should open");
        let mut store = BlockCacheStore::new(&temp.path().join("blocks"), connection)
            .expect("store should open");

        for index in 0..3_u64 {
            let key = CacheKey {
                file_id: FileId(7),
                start_offset: index * 4096,
            };
            store
                .put_block(
                    &key,
                    1,
                    format!("fixture-{index}").as_bytes(),
                    index + 1,
                    100 + index,
                )
                .expect("block should be inserted");
        }

        let report = store
            .evict_to_limit("fixture-2".len() as u64 + 1)
            .expect("eviction should succeed");

        assert!(report.entries_removed >= 2);
        assert!(
            store.total_size_bytes().expect("size should load") <= "fixture-2".len() as u64 + 1
        );
        assert!(
            store
                .get_block(
                    &CacheKey {
                        file_id: FileId(7),
                        start_offset: 8192,
                    },
                    200,
                )
                .expect("latest block lookup should succeed")
                .is_some()
        );
    }

    #[test]
    fn block_cache_store_repairs_corrupt_entries_and_orphans() {
        let temp = tempdir().expect("tempdir should be created");
        let connection = open_cache_database(&temp.path().join("cache").join("client.sqlite"))
            .expect("cache database should open");
        let mut store = BlockCacheStore::new(&temp.path().join("blocks"), connection)
            .expect("store should open");
        let key = CacheKey {
            file_id: FileId(9),
            start_offset: 0,
        };
        let data = b"fixture";
        let hash = blake3::hash(data).as_bytes().to_vec();
        let relative = block_storage_relative_path(&hash);
        store
            .put_block(&key, 1, data, 1, 100)
            .expect("block should be inserted");
        std::fs::write(temp.path().join("blocks").join(&relative), b"corrupt")
            .expect("cached block should be corrupted");

        let orphan = temp.path().join("blocks").join("aa").join("orphan.bin");
        std::fs::create_dir_all(orphan.parent().expect("orphan should have a parent"))
            .expect("orphan directory should exist");
        std::fs::write(&orphan, b"orphan").expect("orphan file should be created");

        let report = store.repair().expect("repair should succeed");

        assert_eq!(report.repaired_entries, 1);
        assert_eq!(report.orphan_files_removed, 1);
        assert!(
            store
                .get_block(&key, 200)
                .expect("lookup should succeed")
                .is_none()
        );
        assert!(!orphan.exists());
    }
}
