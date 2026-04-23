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
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

use crate::{LayoutPolicy, ReconcileStats, is_policy_path};

/// Reconciles an existing source library into the canonical Legato store.
pub fn reconcile_library_root_to_store(
    store_root: impl AsRef<Path>,
    library_root: impl AsRef<Path>,
) -> Result<ReconcileStats, CanonicalStoreError> {
    let store_root = store_root.as_ref().to_path_buf();
    let library_root =
        fs::canonicalize(library_root.as_ref()).map_err(|source| CanonicalStoreError::Io {
            path: library_root.as_ref().to_path_buf(),
            source,
        })?;
    let policy = LayoutPolicy::load(&library_root)
        .map_err(|source| CanonicalStoreError::Policy(source.to_string()))?;
    let mut catalog = CatalogStore::open(&store_root, current_time_ns()?)?;
    let mut identities = IdentityStore::open(&store_root)?;
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

        let path = logical_path(&library_root, entry.path())?;
        seen.insert(path.clone());
        if entry.file_type().is_dir() {
            reconcile_directory(
                &mut catalog,
                &mut identities,
                &library_root,
                entry.path(),
                &mut stats,
            )?;
        } else if entry.file_type().is_file() {
            reconcile_file(
                &mut catalog,
                &mut identities,
                &library_root,
                entry.path(),
                &policy,
                &mut stats,
            )?;
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

    identities.flush()?;
    let _ = catalog.checkpoint()?;
    Ok(stats)
}

fn reconcile_directory(
    catalog: &mut CatalogStore,
    identities: &mut IdentityStore,
    library_root: &Path,
    path: &Path,
    stats: &mut ReconcileStats,
) -> Result<(), CanonicalStoreError> {
    let metadata = fs::metadata(path).map_err(|source| CanonicalStoreError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let path_string = logical_path(library_root, path)?;
    let inode = CatalogInode::directory(
        identities.file_id_for(path, &metadata, true)?,
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
        entries: directory_entries(identities, library_root, path)?,
    };
    let existing_entries = catalog.list_directory(&path_string).map(entries_to_map);
    if existing_entries.as_ref() != Some(&directory.entries) {
        let _ = catalog.append_directory(directory)?;
    }
    Ok(())
}

fn reconcile_file(
    catalog: &mut CatalogStore,
    identities: &mut IdentityStore,
    library_root: &Path,
    path: &Path,
    policy: &LayoutPolicy,
    stats: &mut ReconcileStats,
) -> Result<(), CanonicalStoreError> {
    let metadata = fs::metadata(path).map_err(|source| CanonicalStoreError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let path_string = logical_path(library_root, path)?;
    let decision = policy.file_decision(&path.to_string_lossy(), metadata.len(), false);
    let extents = append_file_extents(
        catalog,
        path,
        &decision.transfer_class,
        decision.extent_bytes,
    )?;
    let inode = CatalogInode::file(
        identities.file_id_for(path, &metadata, false)?,
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
    identities: &mut IdentityStore,
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
        let child_file_type = entry
            .file_type()
            .map_err(|source| CanonicalStoreError::Io {
                path: child_path.clone(),
                source,
            })?;
        if child_file_type.is_file() && is_policy_path(library_root, &child_path) {
            continue;
        }
        let child_metadata = entry.metadata().map_err(|source| CanonicalStoreError::Io {
            path: child_path.clone(),
            source,
        })?;
        let child_path_string = logical_path(library_root, &child_path)?;
        let name = entry.file_name().to_string_lossy().into_owned();
        entries.insert(
            name.clone(),
            CatalogDirectoryEntry {
                name,
                path: child_path_string.clone(),
                file_id: identities.file_id_for(
                    &child_path,
                    &child_metadata,
                    child_file_type.is_dir(),
                )?,
                is_dir: child_file_type.is_dir(),
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

pub(crate) fn logical_request_path(
    library_root: &Path,
    path: &str,
) -> Result<String, CanonicalStoreError> {
    let request_path = Path::new(path);
    if request_path.is_absolute()
        && (request_path == library_root || request_path.starts_with(library_root))
    {
        return logical_path(library_root, request_path);
    }
    Ok(normalize_logical_path(path))
}

fn logical_path(library_root: &Path, path: &Path) -> Result<String, CanonicalStoreError> {
    let relative = path
        .strip_prefix(library_root)
        .map_err(|source| CanonicalStoreError::Policy(source.to_string()))?;
    Ok(normalize_relative_components(relative))
}

fn normalize_logical_path(path: &str) -> String {
    normalize_relative_components(Path::new(path))
}

fn normalize_relative_components(path: &Path) -> String {
    let mut components = Vec::new();
    for component in path.components() {
        match component {
            std::path::Component::Normal(segment) => {
                components.push(segment.to_string_lossy().into_owned());
            }
            std::path::Component::ParentDir => {
                let _ = components.pop();
            }
            std::path::Component::CurDir
            | std::path::Component::RootDir
            | std::path::Component::Prefix(_) => {}
        }
    }

    if components.is_empty() {
        String::from("/")
    } else {
        format!("/{}", components.join("/"))
    }
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

const IDENTITY_STORE_VERSION: u32 = 1;
const IDENTITY_STORE_FILE: &str = "file-identities.json";

#[derive(Debug)]
struct IdentityStore {
    path: PathBuf,
    state: IdentityStoreState,
    dirty: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct IdentityStoreState {
    version: u32,
    next_file_id: u64,
    object_ids: BTreeMap<String, u64>,
}

impl IdentityStore {
    fn open(root_dir: &Path) -> Result<Self, CanonicalStoreError> {
        fs::create_dir_all(root_dir).map_err(|source| CanonicalStoreError::Io {
            path: root_dir.to_path_buf(),
            source,
        })?;
        let path = root_dir.join(IDENTITY_STORE_FILE);
        let state = match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(CanonicalStoreError::Json)?,
            Err(source) if source.kind() == io::ErrorKind::NotFound => IdentityStoreState {
                version: IDENTITY_STORE_VERSION,
                next_file_id: 1,
                object_ids: BTreeMap::new(),
            },
            Err(source) => {
                return Err(CanonicalStoreError::Io { path, source });
            }
        };

        Ok(Self {
            path,
            state,
            dirty: false,
        })
    }

    fn file_id_for(
        &mut self,
        path: &Path,
        metadata: &fs::Metadata,
        is_dir: bool,
    ) -> Result<FileId, CanonicalStoreError> {
        let identity = source_identity_key(path, metadata, is_dir);
        if let Some(file_id) = self.state.object_ids.get(&identity) {
            return Ok(FileId(*file_id));
        }

        let file_id = FileId(self.state.next_file_id.max(1));
        self.state.next_file_id = file_id.0.saturating_add(1);
        self.state.object_ids.insert(identity, file_id.0);
        self.dirty = true;
        Ok(file_id)
    }

    fn flush(&mut self) -> Result<(), CanonicalStoreError> {
        if !self.dirty {
            return Ok(());
        }

        let payload = serde_json::to_vec_pretty(&self.state).map_err(CanonicalStoreError::Json)?;
        fs::write(&self.path, payload).map_err(|source| CanonicalStoreError::Io {
            path: self.path.clone(),
            source,
        })?;
        self.dirty = false;
        Ok(())
    }
}

#[cfg(target_family = "unix")]
fn source_identity_key(_path: &Path, metadata: &fs::Metadata, is_dir: bool) -> String {
    use std::os::unix::fs::MetadataExt;

    let kind = if is_dir { 'd' } else { 'f' };
    format!("{kind}:{}:{}", metadata.dev(), metadata.ino())
}

#[cfg(not(target_family = "unix"))]
fn source_identity_key(path: &Path, _metadata: &fs::Metadata, is_dir: bool) -> String {
    let kind = if is_dir { 'd' } else { 'f' };
    format!("{kind}:{}", path.to_string_lossy())
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
    /// Identity metadata serialization failed.
    Json(serde_json::Error),
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
            Self::Json(source) => write!(formatter, "canonical identity metadata failed: {source}"),
            Self::Policy(source) => write!(formatter, "layout policy failed: {source}"),
        }
    }
}

impl std::error::Error for CanonicalStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Catalog(source) => Some(source),
            Self::Json(source) => Some(source),
            Self::Policy(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{logical_request_path, reconcile_library_root_to_store};
    use legato_client_cache::catalog::CatalogStore;
    use std::path::Path;
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
        let inode = catalog
            .resolve_path("/Kontakt/piano.wav")
            .expect("sample inode should resolve");

        assert_eq!(stats.files_created, 1);
        assert_eq!(inode.size, "sample-payload".len() as u64);
        assert_eq!(inode.extents.len(), 1);
        assert_eq!(
            inode.extents[0].payload_hash,
            blake3::hash(b"sample-payload").as_bytes()
        );
        assert!(catalog.list_directory("/").is_some());
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
                .resolve_path("/piano.wav")
                .expect("updated inode should resolve")
                .size,
            7
        );

        std::fs::remove_file(&sample).expect("sample should delete");
        let delete_stats =
            reconcile_library_root_to_store(&store, &library).expect("delete ingest should work");
        let catalog = CatalogStore::open(&store, 400).expect("catalog should reopen");
        assert_eq!(delete_stats.files_deleted, 1);
        assert!(catalog.resolve_path("/piano.wav").is_none());
    }

    #[test]
    fn canonical_ingest_preserves_file_identity_across_rename() {
        let temp = tempdir().expect("tempdir should exist");
        let library = temp.path().join("library");
        let store = temp.path().join("store");
        std::fs::create_dir_all(library.join("Kontakt")).expect("library should create");
        let original = library.join("Kontakt").join("piano.wav");
        let renamed = library.join("Kontakt").join("strings.wav");
        std::fs::write(&original, b"same-data").expect("sample should write");

        let _ = reconcile_library_root_to_store(&store, &library).expect("initial ingest");
        let first_catalog = CatalogStore::open(&store, 300).expect("catalog should open");
        let original_id = first_catalog
            .resolve_path("/Kontakt/piano.wav")
            .expect("original inode should resolve")
            .file_id;

        std::fs::rename(&original, &renamed).expect("rename should succeed");
        let _ = reconcile_library_root_to_store(&store, &library).expect("rename ingest");
        let second_catalog = CatalogStore::open(&store, 400).expect("catalog should reopen");
        let renamed_inode = second_catalog
            .resolve_path("/Kontakt/strings.wav")
            .expect("renamed inode should resolve");

        assert_eq!(renamed_inode.file_id, original_id);
        assert!(second_catalog.resolve_path("/Kontakt/piano.wav").is_none());
    }

    #[test]
    fn request_paths_accept_legacy_absolute_and_logical_forms() {
        let library = Path::new("/srv/libraries");

        assert_eq!(
            logical_request_path(library, "/srv/libraries/Kontakt/piano.nki")
                .expect("legacy path should normalize"),
            "/Kontakt/piano.nki"
        );
        assert_eq!(
            logical_request_path(library, "/Kontakt/piano.nki")
                .expect("logical path should normalize"),
            "/Kontakt/piano.nki"
        );
    }
}
