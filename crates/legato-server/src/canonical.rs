//! Server-side ingest into the canonical Legato record store.

use std::{
    collections::{BTreeMap, HashSet},
    fs,
    io::{self, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    time::UNIX_EPOCH,
};

use legato_client_cache::catalog::{
    CatalogDirectory, CatalogDirectoryEntry, CatalogExtent, CatalogInode, CatalogStore,
    CatalogStoreError, CatalogTombstone,
};
use legato_proto::TransferClass;
use legato_types::FileId;
use walkdir::WalkDir;

use crate::{LayoutPolicy, ReconcileStats, is_policy_path};

/// Reconciles an existing source library into the canonical Legato store.
pub fn reconcile_library_root_to_store(
    store_root: impl AsRef<Path>,
    library_root: impl AsRef<Path>,
) -> Result<ReconcileStats, CanonicalStoreError> {
    let library_root =
        fs::canonicalize(library_root.as_ref()).map_err(|source| CanonicalStoreError::Io {
            path: library_root.as_ref().to_path_buf(),
            source,
        })?;
    let policy = LayoutPolicy::load(&library_root)
        .map_err(|source| CanonicalStoreError::Policy(source.to_string()))?;
    let mut catalog = CatalogStore::open(store_root, current_time_ns()?)?;
    let previously_active = catalog.active_paths().into_iter().collect::<HashSet<_>>();
    let mut seen = HashSet::new();
    let mut stats = ReconcileStats::default();

    for entry in WalkDir::new(&library_root).sort_by_file_name() {
        let entry = entry.map_err(|source| CanonicalStoreError::Io {
            path: library_root.clone(),
            source: io::Error::other(source),
        })?;
        if entry.file_type().is_file() && is_policy_path(&library_root, entry.path()) {
            continue;
        }

        let path = normalize_path(entry.path());
        seen.insert(path.clone());
        if entry.file_type().is_dir() {
            reconcile_directory(&mut catalog, &library_root, entry.path(), &mut stats)?;
        } else if entry.file_type().is_file() {
            reconcile_file(&mut catalog, entry.path(), &policy, &mut stats)?;
        }
    }

    for stale_path in previously_active.difference(&seen) {
        let stale_inode = catalog.resolve_path(stale_path).cloned();
        catalog.append_tombstone(CatalogTombstone {
            path: stale_path.clone(),
            file_id: stale_inode.as_ref().map(|inode| inode.file_id),
        })?;
        if stale_inode.is_some_and(|inode| inode.is_dir) {
            stats.directories_deleted += 1;
        } else {
            stats.files_deleted += 1;
        }
    }

    let _ = catalog.checkpoint()?;
    Ok(stats)
}

fn reconcile_directory(
    catalog: &mut CatalogStore,
    library_root: &Path,
    path: &Path,
    stats: &mut ReconcileStats,
) -> Result<(), CanonicalStoreError> {
    let metadata = fs::metadata(path).map_err(|source| CanonicalStoreError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let path_string = normalize_path(path);
    let inode = CatalogInode::directory(
        file_id_for_path(&path_string),
        &path_string,
        mtime_ns(&metadata)?,
    );
    let existing_inode = catalog.resolve_path(&path_string).cloned();
    if existing_inode.as_ref() != Some(&inode) {
        if existing_inode.is_some() {
            stats.directories_updated += 1;
        } else {
            stats.directories_created += 1;
        }
        let _ = catalog.append_inode(inode.clone())?;
    }

    let directory = CatalogDirectory {
        directory_id: inode.file_id,
        path: path_string.clone(),
        entries: directory_entries(library_root, path)?,
    };
    let existing_entries = catalog.list_directory(&path_string).map(entries_to_map);
    if existing_entries.as_ref() != Some(&directory.entries) {
        let _ = catalog.append_directory(directory)?;
    }
    Ok(())
}

fn reconcile_file(
    catalog: &mut CatalogStore,
    path: &Path,
    policy: &LayoutPolicy,
    stats: &mut ReconcileStats,
) -> Result<(), CanonicalStoreError> {
    let metadata = fs::metadata(path).map_err(|source| CanonicalStoreError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let path_string = normalize_path(path);
    let decision = policy.file_decision(&path_string, metadata.len(), false);
    let extents = append_file_extents(
        catalog,
        path,
        &decision.transfer_class,
        decision.extent_bytes,
    )?;
    let inode = CatalogInode::file(
        file_id_for_path(&path_string),
        &path_string,
        metadata.len(),
        mtime_ns(&metadata)?,
        decision.transfer_class,
        extents,
    );
    let existing = catalog.resolve_path(&path_string).cloned();
    if existing.as_ref() == Some(&inode) {
        return Ok(());
    }
    if existing.is_some() {
        stats.files_updated += 1;
    } else {
        stats.files_created += 1;
    }
    let _ = catalog.append_inode(inode)?;
    Ok(())
}

fn append_file_extents(
    catalog: &mut CatalogStore,
    path: &Path,
    transfer_class: &TransferClass,
    extent_bytes: u64,
) -> Result<Vec<CatalogExtent>, CanonicalStoreError> {
    let mut file = fs::File::open(path).map_err(|source| CanonicalStoreError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let size = file
        .metadata()
        .map_err(|source| CanonicalStoreError::Io {
            path: path.to_path_buf(),
            source,
        })?
        .len();
    let extent_length = match transfer_class {
        TransferClass::Unitary => size.max(1),
        _ => extent_bytes.max(1),
    };
    let extent_count = if size == 0 {
        1
    } else {
        size.div_ceil(extent_length)
    };
    let mut extents = Vec::with_capacity(extent_count as usize);
    for extent_index in 0..extent_count {
        let file_offset = extent_index * extent_length;
        let length = if size == 0 {
            0
        } else {
            std::cmp::min(extent_length, size - file_offset)
        };
        file.seek(SeekFrom::Start(file_offset))
            .map_err(|source| CanonicalStoreError::Io {
                path: path.to_path_buf(),
                source,
            })?;
        let mut payload = vec![0_u8; length as usize];
        file.read_exact(&mut payload)
            .map_err(|source| CanonicalStoreError::Io {
                path: path.to_path_buf(),
                source,
            })?;
        extents.push(catalog.append_extent_payload(
            extent_index as u32,
            file_offset,
            *transfer_class,
            &payload,
        )?);
    }
    Ok(extents)
}

fn directory_entries(
    library_root: &Path,
    path: &Path,
) -> Result<BTreeMap<String, CatalogDirectoryEntry>, CanonicalStoreError> {
    let mut entries = BTreeMap::new();
    for entry in fs::read_dir(path).map_err(|source| CanonicalStoreError::Io {
        path: path.to_path_buf(),
        source,
    })? {
        let entry = entry.map_err(|source| CanonicalStoreError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        let child_path = entry.path();
        if entry
            .file_type()
            .map_err(|source| CanonicalStoreError::Io {
                path: child_path.clone(),
                source,
            })?
            .is_file()
            && is_policy_path(library_root, &child_path)
        {
            continue;
        }
        let child_path_string = normalize_path(&child_path);
        let name = entry.file_name().to_string_lossy().into_owned();
        entries.insert(
            name.clone(),
            CatalogDirectoryEntry {
                name,
                path: child_path_string.clone(),
                file_id: file_id_for_path(&child_path_string),
                is_dir: entry
                    .file_type()
                    .map_err(|source| CanonicalStoreError::Io {
                        path: child_path.clone(),
                        source,
                    })?
                    .is_dir(),
            },
        );
    }
    Ok(entries)
}

fn entries_to_map(entries: Vec<CatalogDirectoryEntry>) -> BTreeMap<String, CatalogDirectoryEntry> {
    entries
        .into_iter()
        .map(|entry| (entry.name.clone(), entry))
        .collect()
}

fn file_id_for_path(path: &str) -> FileId {
    let hash = blake3::hash(path.as_bytes());
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&hash.as_bytes()[0..8]);
    FileId(u64::from_le_bytes(bytes).max(1))
}

fn normalize_path(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn mtime_ns(metadata: &fs::Metadata) -> Result<i64, CanonicalStoreError> {
    let duration = metadata
        .modified()
        .map_err(|source| CanonicalStoreError::Io {
            path: PathBuf::new(),
            source,
        })?
        .duration_since(UNIX_EPOCH)
        .map_err(|source| CanonicalStoreError::Policy(source.to_string()))?;
    Ok(duration.as_nanos() as i64)
}

fn current_time_ns() -> Result<u64, CanonicalStoreError> {
    Ok(std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|source| CanonicalStoreError::Policy(source.to_string()))?
        .as_nanos() as u64)
}

/// Error returned while ingesting the canonical store.
#[derive(Debug)]
pub enum CanonicalStoreError {
    /// Filesystem IO failed.
    Io {
        /// Path involved in the failed operation.
        path: PathBuf,
        /// Source IO error.
        source: io::Error,
    },
    /// Catalog store operation failed.
    Catalog(CatalogStoreError),
    /// Layout policy failed to load.
    Policy(String),
}

impl From<CatalogStoreError> for CanonicalStoreError {
    fn from(value: CatalogStoreError) -> Self {
        Self::Catalog(value)
    }
}

impl std::fmt::Display for CanonicalStoreError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io { path, source } => {
                write!(
                    formatter,
                    "canonical store IO failed for {}: {source}",
                    path.display()
                )
            }
            Self::Catalog(source) => write!(formatter, "{source}"),
            Self::Policy(source) => write!(formatter, "layout policy failed: {source}"),
        }
    }
}

impl std::error::Error for CanonicalStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Catalog(source) => Some(source),
            Self::Policy(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::reconcile_library_root_to_store;
    use legato_client_cache::catalog::CatalogStore;
    use tempfile::tempdir;

    #[test]
    fn canonical_ingest_writes_catalog_and_extent_records() {
        let temp = tempdir().expect("tempdir should exist");
        let library = temp.path().join("library");
        let store = temp.path().join("store");
        std::fs::create_dir_all(library.join("Kontakt")).expect("library should create");
        std::fs::write(library.join("Kontakt").join("piano.wav"), b"sample-payload")
            .expect("sample should write");

        let stats =
            reconcile_library_root_to_store(&store, &library).expect("ingest should succeed");
        let catalog = CatalogStore::open(&store, 200).expect("catalog should open");
        let file_path = library.join("Kontakt").join("piano.wav");
        let inode = catalog
            .resolve_path(&file_path.to_string_lossy())
            .expect("sample inode should resolve");

        assert_eq!(stats.files_created, 1);
        assert_eq!(inode.size, "sample-payload".len() as u64);
        assert_eq!(inode.extents.len(), 1);
        assert_eq!(
            inode.extents[0].payload_hash,
            blake3::hash(b"sample-payload").as_bytes()
        );
        assert!(catalog.list_directory(&library.to_string_lossy()).is_some());
    }

    #[test]
    fn canonical_ingest_reconciles_update_and_delete() {
        let temp = tempdir().expect("tempdir should exist");
        let library = temp.path().join("library");
        let store = temp.path().join("store");
        std::fs::create_dir_all(&library).expect("library should create");
        let sample = library.join("piano.wav");
        std::fs::write(&sample, b"one").expect("sample should write");
        let _ = reconcile_library_root_to_store(&store, &library).expect("initial ingest");

        std::fs::write(&sample, b"two-two").expect("sample should update");
        let update_stats =
            reconcile_library_root_to_store(&store, &library).expect("update ingest should work");
        let catalog = CatalogStore::open(&store, 300).expect("catalog should open");
        assert_eq!(update_stats.files_updated, 1);
        assert_eq!(
            catalog
                .resolve_path(&sample.to_string_lossy())
                .expect("updated inode should resolve")
                .size,
            7
        );

        std::fs::remove_file(&sample).expect("sample should delete");
        let delete_stats =
            reconcile_library_root_to_store(&store, &library).expect("delete ingest should work");
        let catalog = CatalogStore::open(&store, 400).expect("catalog should reopen");
        assert_eq!(delete_stats.files_deleted, 1);
        assert!(catalog.resolve_path(&sample.to_string_lossy()).is_none());
    }
}
