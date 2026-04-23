//! Local metadata and extent-store primitives shared by client-side components.

pub mod catalog;
pub mod client_store;
pub mod segment;

use std::{collections::HashMap, path::Path};

use legato_proto::{DirectoryEntry, FileMetadata, InvalidationEvent, InvalidationKind};
use serde::{Deserialize, Serialize};

/// Minimal cache configuration used by the shared client runtime.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
pub struct CacheConfig {
    /// Total maximum size of the cache in bytes.
    pub max_bytes: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_bytes: 1_500 * 1024 * 1024 * 1024,
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
    root == "/"
        || path == root
        || path
            .strip_prefix(root)
            .is_some_and(|suffix| suffix.is_empty() || suffix.starts_with('/'))
}

#[cfg(test)]
mod tests {
    use super::{CacheConfig, MetadataCache, MetadataCacheLookup, MetadataCachePolicy};
    use legato_proto::{DirectoryEntry, FileMetadata, InvalidationEvent, InvalidationKind};

    #[test]
    fn cache_config_defaults_to_large_local_store() {
        assert!(CacheConfig::default().max_bytes > 1_000_000_000);
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
            issued_at_ns: 0,
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
}
