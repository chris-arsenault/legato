//! Catalog state rebuilt from Legato records and checkpoints.

use std::{
    collections::BTreeMap,
    fs, io,
    path::{Path, PathBuf},
};

use legato_proto::{
    ChangeKind, ChangeRecord, DirectoryEntry, ExtentDescriptor, FileLayout, InodeMetadata,
    TransferClass,
};
use legato_types::FileId;
use serde::{Deserialize, Serialize};

use crate::segment::{
    SegmentStoreError, SegmentWriter, StoreRecord, StoreRecordKind, repair_incomplete_tail,
};

const CATALOG_CHECKPOINT_VERSION: u32 = 1;
const CATALOG_CHECKPOINT_FILE: &str = "catalog.json";

/// File extent location and integrity metadata in the catalog.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CatalogExtent {
    /// Logical extent index within the file.
    pub extent_index: u32,
    /// File-relative byte offset.
    pub file_offset: u64,
    /// Extent byte length.
    pub length: u64,
    /// BLAKE3 hash of the extent payload.
    pub payload_hash: Vec<u8>,
    /// Transfer class assigned during ingest.
    pub transfer_class: i32,
    /// Segment containing the locally resident extent payload when present.
    #[serde(default)]
    pub segment_id: Option<u64>,
    /// Byte offset of the resident extent payload record inside the segment.
    #[serde(default)]
    pub segment_offset: Option<u64>,
}

impl CatalogExtent {
    /// Returns whether the extent payload is resident in the local segment store.
    #[must_use]
    pub fn is_resident(&self) -> bool {
        self.segment_id.is_some() && self.segment_offset.is_some()
    }
}

/// Active inode metadata and extent map for a file or directory.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CatalogInode {
    /// Stable file identity.
    pub file_id: FileId,
    /// Absolute virtual library path.
    pub path: String,
    /// Monotonic inode generation for fetch binding.
    #[serde(default = "default_inode_generation")]
    pub inode_generation: u64,
    /// File size in bytes.
    pub size: u64,
    /// Modification time in nanoseconds.
    pub mtime_ns: i64,
    /// True for directories.
    pub is_dir: bool,
    /// Hash of the full file contents when known.
    #[serde(default)]
    pub content_hash: Vec<u8>,
    /// Assigned transfer class.
    pub transfer_class: i32,
    /// Authoritative extent map for regular files.
    pub extents: Vec<CatalogExtent>,
}

/// File-specific inode metadata and extent layout.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogFileState {
    /// Monotonic inode generation for fetch binding.
    pub inode_generation: u64,
    /// File size in bytes.
    pub size: u64,
    /// Modification time in nanoseconds.
    pub mtime_ns: i64,
    /// Hash of the full file contents.
    pub content_hash: Vec<u8>,
    /// Assigned transfer class.
    pub transfer_class: TransferClass,
    /// Authoritative extent map for regular files.
    pub extents: Vec<CatalogExtent>,
}

impl CatalogInode {
    /// Creates a directory inode.
    #[must_use]
    pub fn directory(file_id: FileId, path: impl Into<String>, mtime_ns: i64) -> Self {
        Self {
            file_id,
            path: path.into(),
            inode_generation: default_inode_generation(),
            size: 0,
            mtime_ns,
            is_dir: true,
            content_hash: Vec::new(),
            transfer_class: TransferClass::Unitary as i32,
            extents: Vec::new(),
        }
    }

    /// Creates a file inode.
    #[must_use]
    pub fn file(file_id: FileId, path: impl Into<String>, state: CatalogFileState) -> Self {
        Self {
            file_id,
            path: path.into(),
            inode_generation: state.inode_generation,
            size: state.size,
            mtime_ns: state.mtime_ns,
            is_dir: false,
            content_hash: state.content_hash,
            transfer_class: state.transfer_class as i32,
            extents: state.extents,
        }
    }
}

/// Directory membership record.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CatalogDirectory {
    /// Directory file identity.
    pub directory_id: FileId,
    /// Directory path.
    pub path: String,
    /// Child entries keyed by basename.
    pub entries: BTreeMap<String, CatalogDirectoryEntry>,
}

/// One child entry inside a directory.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CatalogDirectoryEntry {
    /// Child basename.
    pub name: String,
    /// Child full path.
    pub path: String,
    /// Child file identity.
    pub file_id: FileId,
    /// True when the child is a directory.
    pub is_dir: bool,
}

/// Tombstone for removing active catalog state.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CatalogTombstone {
    /// Path removed from the catalog.
    pub path: String,
    /// File identity removed from the catalog when known.
    pub file_id: Option<FileId>,
}

/// Durable catalog checkpoint.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CatalogCheckpoint {
    /// Checkpoint format version.
    pub version: u32,
    /// Highest record sequence included in the checkpoint.
    pub sequence: u64,
    /// Active path-to-file mapping.
    pub paths: BTreeMap<String, FileId>,
    /// Active inode records.
    pub inodes: BTreeMap<u64, CatalogInode>,
    /// Active directory records.
    pub directories: BTreeMap<u64, CatalogDirectory>,
    /// Durable subscription cursor.
    pub subscription_cursor: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
enum CatalogRecordPayload {
    Inode(CatalogInode),
    Directory(CatalogDirectory),
    Tombstone(CatalogTombstone),
    Checkpoint(CatalogCheckpoint),
    Cursor(u64),
}

/// Mutable catalog state backed by append-only segment records.
#[derive(Debug)]
pub struct CatalogStore {
    root_dir: PathBuf,
    checkpoint_dir: PathBuf,
    writer: SegmentWriter,
    next_sequence: u64,
    state: CatalogState,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct CatalogState {
    paths: BTreeMap<String, FileId>,
    inodes: BTreeMap<u64, CatalogInode>,
    directories: BTreeMap<u64, CatalogDirectory>,
    subscription_cursor: u64,
    last_sequence: u64,
}

impl CatalogStore {
    /// Opens a catalog store under the documented catalog, segment, and checkpoint layout.
    pub fn open(root_dir: impl AsRef<Path>, now_ns: u64) -> Result<Self, CatalogStoreError> {
        let root_dir = root_dir.as_ref().to_path_buf();
        let segment_dir = root_dir.join("segments");
        let checkpoint_dir = root_dir.join("checkpoints");
        fs::create_dir_all(&segment_dir).map_err(|source| CatalogStoreError::Io {
            path: segment_dir.clone(),
            source,
        })?;
        fs::create_dir_all(&checkpoint_dir).map_err(|source| CatalogStoreError::Io {
            path: checkpoint_dir.clone(),
            source,
        })?;

        let checkpoint = load_checkpoint_file(&checkpoint_dir)?;
        let mut state = checkpoint
            .as_ref()
            .map(CatalogState::from_checkpoint)
            .unwrap_or_default();
        replay_segments(&segment_dir, &mut state)?;
        let next_segment_id = next_segment_id(&segment_dir)?;
        let writer = SegmentWriter::create(
            segment_dir.join(segment_file_name(next_segment_id)),
            next_segment_id,
            now_ns,
        )?;
        let next_sequence = state.last_sequence.saturating_add(1);

        Ok(Self {
            root_dir,
            checkpoint_dir,
            writer,
            next_sequence,
            state,
        })
    }

    /// Returns the store root.
    #[must_use]
    pub fn root_dir(&self) -> &Path {
        &self.root_dir
    }

    /// Returns the highest durable or appended sequence number.
    #[must_use]
    pub fn last_sequence(&self) -> u64 {
        self.state.last_sequence
    }

    /// Returns the subscription cursor.
    #[must_use]
    pub fn subscription_cursor(&self) -> u64 {
        self.state.subscription_cursor
    }

    /// Appends an inode record and updates path/file indexes.
    pub fn append_inode(&mut self, inode: CatalogInode) -> Result<u64, CatalogStoreError> {
        self.append_payload(
            StoreRecordKind::Inode,
            CatalogRecordPayload::Inode(inode),
            apply_catalog_payload,
        )
    }

    /// Appends an extent payload record and returns its catalog location metadata.
    pub fn append_extent_payload(
        &mut self,
        extent_index: u32,
        file_offset: u64,
        transfer_class: TransferClass,
        payload: &[u8],
    ) -> Result<CatalogExtent, CatalogStoreError> {
        let sequence = self.next_sequence;
        let segment_id = self.writer.segment_id();
        let segment_offset = self.writer.current_offset()?;
        let _ = self
            .writer
            .append(StoreRecordKind::Extent, sequence, payload)?;
        self.state.last_sequence = sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        Ok(CatalogExtent {
            extent_index,
            file_offset,
            length: payload.len() as u64,
            payload_hash: blake3::hash(payload).as_bytes().to_vec(),
            transfer_class: transfer_class as i32,
            segment_id: Some(segment_id),
            segment_offset: Some(segment_offset),
        })
    }

    /// Appends a directory record and updates directory membership.
    pub fn append_directory(
        &mut self,
        directory: CatalogDirectory,
    ) -> Result<u64, CatalogStoreError> {
        self.append_payload(
            StoreRecordKind::Dirent,
            CatalogRecordPayload::Directory(directory),
            apply_catalog_payload,
        )
    }

    /// Appends a tombstone and removes active catalog state.
    pub fn append_tombstone(
        &mut self,
        tombstone: CatalogTombstone,
    ) -> Result<u64, CatalogStoreError> {
        self.append_payload(
            StoreRecordKind::Tombstone,
            CatalogRecordPayload::Tombstone(tombstone),
            apply_catalog_payload,
        )
    }

    /// Appends and persists a subscription cursor record.
    pub fn append_subscription_cursor(&mut self, cursor: u64) -> Result<u64, CatalogStoreError> {
        self.append_payload(
            StoreRecordKind::Checkpoint,
            CatalogRecordPayload::Cursor(cursor),
            apply_catalog_payload,
        )
    }

    /// Writes a compact checkpoint file and checkpoint segment record.
    pub fn checkpoint(&mut self) -> Result<CatalogCheckpoint, CatalogStoreError> {
        let checkpoint = self.state.to_checkpoint();
        let sequence = self.append_payload(
            StoreRecordKind::Checkpoint,
            CatalogRecordPayload::Checkpoint(checkpoint),
            apply_catalog_payload,
        )?;
        let checkpoint = self.state.to_checkpoint_with_sequence(sequence);
        write_checkpoint_file(&self.checkpoint_dir, &checkpoint)?;
        Ok(checkpoint)
    }

    /// Resolves an active path to inode metadata.
    #[must_use]
    pub fn resolve_path(&self, path: &str) -> Option<&CatalogInode> {
        self.state
            .paths
            .get(path)
            .and_then(|file_id| self.state.inodes.get(&file_id.0))
    }

    /// Resolves an active file ID to inode metadata.
    #[must_use]
    pub fn resolve_file_id(&self, file_id: FileId) -> Option<&CatalogInode> {
        self.state.inodes.get(&file_id.0)
    }

    /// Returns a directory listing by path.
    #[must_use]
    pub fn list_directory(&self, path: &str) -> Option<Vec<CatalogDirectoryEntry>> {
        self.resolve_path(path)
            .and_then(|inode| self.state.directories.get(&inode.file_id.0))
            .map(|directory| directory.entries.values().cloned().collect())
    }

    /// Returns active catalog paths.
    #[must_use]
    pub fn active_paths(&self) -> Vec<String> {
        self.state.paths.keys().cloned().collect()
    }

    /// Returns active inode records in stable file-id order.
    #[must_use]
    pub fn active_inodes(&self) -> Vec<CatalogInode> {
        self.state.inodes.values().cloned().collect()
    }

    /// Reads and verifies one resident extent payload from its canonical segment.
    pub fn read_extent_payload(
        &self,
        extent: &CatalogExtent,
    ) -> Result<Vec<u8>, CatalogStoreError> {
        let segment_id = extent
            .segment_id
            .ok_or(CatalogStoreError::NonResidentExtent {
                extent_index: extent.extent_index,
                file_offset: extent.file_offset,
            })?;
        let segment_offset = extent
            .segment_offset
            .ok_or(CatalogStoreError::NonResidentExtent {
                extent_index: extent.extent_index,
                file_offset: extent.file_offset,
            })?;
        let path = self
            .root_dir
            .join("segments")
            .join(segment_file_name(segment_id));
        let scan = repair_incomplete_tail(&path)?;
        let record = scan
            .records
            .into_iter()
            .find(|record| {
                record.kind == StoreRecordKind::Extent
                    && record.segment_offset == segment_offset
                    && record.payload_hash.as_slice() == extent.payload_hash.as_slice()
            })
            .ok_or(CatalogStoreError::MissingExtent {
                segment_id,
                segment_offset,
            })?;
        Ok(record.payload)
    }

    /// Loads ordered catalog change records after the supplied sequence cursor.
    pub fn change_records_since(
        &self,
        since_sequence: u64,
    ) -> Result<Vec<ChangeRecord>, CatalogStoreError> {
        let mut records = Vec::new();
        for path in segment_paths(&self.root_dir.join("segments"))? {
            let scan = repair_incomplete_tail(&path)?;
            for record in scan.records {
                if record.sequence <= since_sequence {
                    continue;
                }
                if let Some(change) = change_record_from_store_record(record)? {
                    records.push(change);
                }
            }
        }
        records.sort_by_key(|record| record.sequence);
        Ok(records)
    }

    fn append_payload(
        &mut self,
        kind: StoreRecordKind,
        payload: CatalogRecordPayload,
        apply: fn(&mut CatalogState, u64, CatalogRecordPayload),
    ) -> Result<u64, CatalogStoreError> {
        let sequence = self.next_sequence;
        let bytes = serde_json::to_vec(&payload).map_err(CatalogStoreError::Json)?;
        let _ = self.writer.append(kind, sequence, &bytes)?;
        apply(&mut self.state, sequence, payload);
        self.next_sequence = self.next_sequence.saturating_add(1);
        Ok(sequence)
    }
}

impl CatalogState {
    fn from_checkpoint(checkpoint: &CatalogCheckpoint) -> Self {
        Self {
            paths: checkpoint.paths.clone(),
            inodes: checkpoint.inodes.clone(),
            directories: checkpoint.directories.clone(),
            subscription_cursor: checkpoint.subscription_cursor,
            last_sequence: checkpoint.sequence,
        }
    }

    fn to_checkpoint(&self) -> CatalogCheckpoint {
        self.to_checkpoint_with_sequence(self.last_sequence)
    }

    fn to_checkpoint_with_sequence(&self, sequence: u64) -> CatalogCheckpoint {
        CatalogCheckpoint {
            version: CATALOG_CHECKPOINT_VERSION,
            sequence,
            paths: self.paths.clone(),
            inodes: self.inodes.clone(),
            directories: self.directories.clone(),
            subscription_cursor: self.subscription_cursor,
        }
    }
}

fn replay_segments(segment_dir: &Path, state: &mut CatalogState) -> Result<(), CatalogStoreError> {
    for path in segment_paths(segment_dir)? {
        let scan = repair_incomplete_tail(&path)?;
        for record in scan.records {
            if record.sequence <= state.last_sequence {
                continue;
            }
            replay_record(state, record)?;
        }
    }
    Ok(())
}

fn segment_paths(segment_dir: &Path) -> Result<Vec<PathBuf>, CatalogStoreError> {
    let mut segment_paths = fs::read_dir(segment_dir)
        .map_err(|source| CatalogStoreError::Io {
            path: segment_dir.to_path_buf(),
            source,
        })?
        .map(|entry| {
            entry
                .map(|entry| entry.path())
                .map_err(|source| CatalogStoreError::Io {
                    path: segment_dir.to_path_buf(),
                    source,
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    segment_paths.retain(|path| {
        path.extension()
            .is_some_and(|extension| extension == "lseg")
    });
    segment_paths.sort();
    Ok(segment_paths)
}

fn replay_record(state: &mut CatalogState, record: StoreRecord) -> Result<(), CatalogStoreError> {
    if record.kind == StoreRecordKind::Extent {
        state.last_sequence = record.sequence;
        return Ok(());
    }
    let payload = serde_json::from_slice::<CatalogRecordPayload>(&record.payload)
        .map_err(CatalogStoreError::Json)?;
    apply_catalog_payload(state, record.sequence, payload);
    Ok(())
}

fn apply_catalog_payload(state: &mut CatalogState, sequence: u64, payload: CatalogRecordPayload) {
    match payload {
        CatalogRecordPayload::Inode(inode) => {
            state.paths.insert(inode.path.clone(), inode.file_id);
            state.inodes.insert(inode.file_id.0, inode);
        }
        CatalogRecordPayload::Directory(directory) => {
            state
                .paths
                .insert(directory.path.clone(), directory.directory_id);
            state
                .directories
                .insert(directory.directory_id.0, directory);
        }
        CatalogRecordPayload::Tombstone(tombstone) => {
            let removed_file_id = tombstone
                .file_id
                .or_else(|| state.paths.get(&tombstone.path).copied());
            state.paths.remove(&tombstone.path);
            if let Some(file_id) = removed_file_id {
                if state
                    .inodes
                    .get(&file_id.0)
                    .is_some_and(|inode| inode.path == tombstone.path)
                {
                    state.inodes.remove(&file_id.0);
                }
                if state
                    .directories
                    .get(&file_id.0)
                    .is_some_and(|directory| directory.path == tombstone.path)
                {
                    state.directories.remove(&file_id.0);
                }
            }
            for directory in state.directories.values_mut() {
                directory
                    .entries
                    .retain(|_, entry| entry.path != tombstone.path);
            }
        }
        CatalogRecordPayload::Checkpoint(checkpoint) => {
            *state = CatalogState::from_checkpoint(&checkpoint);
        }
        CatalogRecordPayload::Cursor(cursor) => {
            state.subscription_cursor = cursor;
        }
    }
    state.last_sequence = sequence;
}

fn change_record_from_store_record(
    record: StoreRecord,
) -> Result<Option<ChangeRecord>, CatalogStoreError> {
    if record.kind == StoreRecordKind::Extent {
        return Ok(None);
    }
    let payload = serde_json::from_slice::<CatalogRecordPayload>(&record.payload)
        .map_err(CatalogStoreError::Json)?;
    let change = match payload {
        CatalogRecordPayload::Inode(inode) => Some(ChangeRecord {
            sequence: record.sequence,
            kind: ChangeKind::Upsert as i32,
            file_id: inode.file_id.0,
            path: inode.path.clone(),
            inode: Some(inode_to_proto(inode)),
            entries: Vec::new(),
        }),
        CatalogRecordPayload::Directory(directory) => Some(ChangeRecord {
            sequence: record.sequence,
            kind: ChangeKind::Upsert as i32,
            file_id: directory.directory_id.0,
            path: directory.path.clone(),
            inode: Some(InodeMetadata {
                file_id: directory.directory_id.0,
                path: directory.path,
                size: 0,
                mtime_ns: 0,
                is_dir: true,
                layout: Some(FileLayout {
                    transfer_class: TransferClass::Unitary as i32,
                    extents: Vec::new(),
                }),
                inode_generation: default_inode_generation(),
                content_hash: Vec::new(),
            }),
            entries: directory
                .entries
                .into_values()
                .map(|entry| DirectoryEntry {
                    name: entry.name,
                    path: entry.path,
                    is_dir: entry.is_dir,
                    file_id: entry.file_id.0,
                })
                .collect(),
        }),
        CatalogRecordPayload::Tombstone(tombstone) => Some(ChangeRecord {
            sequence: record.sequence,
            kind: ChangeKind::Delete as i32,
            file_id: tombstone.file_id.map_or(0, |file_id| file_id.0),
            path: tombstone.path,
            inode: None,
            entries: Vec::new(),
        }),
        CatalogRecordPayload::Checkpoint(checkpoint) => Some(ChangeRecord {
            sequence: record.sequence,
            kind: ChangeKind::Checkpoint as i32,
            file_id: 0,
            path: format!("checkpoint:{}", checkpoint.sequence),
            inode: None,
            entries: Vec::new(),
        }),
        CatalogRecordPayload::Cursor(_) => None,
    };
    Ok(change)
}

/// Converts a catalog inode into protocol metadata.
#[must_use]
pub fn inode_to_proto(inode: CatalogInode) -> InodeMetadata {
    InodeMetadata {
        file_id: inode.file_id.0,
        path: inode.path,
        size: inode.size,
        mtime_ns: inode.mtime_ns as u64,
        is_dir: inode.is_dir,
        layout: Some(FileLayout {
            transfer_class: inode.transfer_class,
            extents: inode
                .extents
                .into_iter()
                .map(|extent| ExtentDescriptor {
                    extent_index: extent.extent_index,
                    file_offset: extent.file_offset,
                    length: extent.length,
                    extent_hash: extent.payload_hash,
                })
                .collect(),
        }),
        inode_generation: inode.inode_generation,
        content_hash: inode.content_hash,
    }
}

fn default_inode_generation() -> u64 {
    1
}

fn load_checkpoint_file(
    checkpoint_dir: &Path,
) -> Result<Option<CatalogCheckpoint>, CatalogStoreError> {
    let path = checkpoint_dir.join(CATALOG_CHECKPOINT_FILE);
    match fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(CatalogStoreError::Json),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(CatalogStoreError::Io { path, source }),
    }
}

fn write_checkpoint_file(
    checkpoint_dir: &Path,
    checkpoint: &CatalogCheckpoint,
) -> Result<(), CatalogStoreError> {
    fs::create_dir_all(checkpoint_dir).map_err(|source| CatalogStoreError::Io {
        path: checkpoint_dir.to_path_buf(),
        source,
    })?;
    let path = checkpoint_dir.join(CATALOG_CHECKPOINT_FILE);
    let bytes = serde_json::to_vec_pretty(checkpoint).map_err(CatalogStoreError::Json)?;
    fs::write(&path, bytes).map_err(|source| CatalogStoreError::Io { path, source })
}

fn next_segment_id(segment_dir: &Path) -> Result<u64, CatalogStoreError> {
    let mut max_id = 0_u64;
    for entry in fs::read_dir(segment_dir).map_err(|source| CatalogStoreError::Io {
        path: segment_dir.to_path_buf(),
        source,
    })? {
        let path = entry
            .map_err(|source| CatalogStoreError::Io {
                path: segment_dir.to_path_buf(),
                source,
            })?
            .path();
        if path.extension().is_none_or(|extension| extension != "lseg") {
            continue;
        }
        if let Some(id) = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(|stem| stem.parse::<u64>().ok())
        {
            max_id = max_id.max(id);
        }
    }
    Ok(max_id.saturating_add(1))
}

fn segment_file_name(segment_id: u64) -> String {
    format!("{segment_id:020}.lseg")
}

/// Error returned by the catalog store.
#[derive(Debug)]
pub enum CatalogStoreError {
    /// Filesystem IO failed.
    Io {
        /// Path involved in the failed operation.
        path: PathBuf,
        /// Source IO error.
        source: io::Error,
    },
    /// Segment operation failed.
    Segment(SegmentStoreError),
    /// Catalog JSON serialization failed.
    Json(serde_json::Error),
    /// Extent layout is known but no resident payload is currently attached.
    NonResidentExtent {
        /// Logical extent index.
        extent_index: u32,
        /// File-relative byte offset.
        file_offset: u64,
    },
    /// Expected extent payload was not present in the referenced segment.
    MissingExtent {
        /// Segment identifier.
        segment_id: u64,
        /// Segment byte offset.
        segment_offset: u64,
    },
}

impl From<SegmentStoreError> for CatalogStoreError {
    fn from(value: SegmentStoreError) -> Self {
        Self::Segment(value)
    }
}

impl std::fmt::Display for CatalogStoreError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io { path, source } => {
                write!(
                    formatter,
                    "catalog IO failed for {}: {source}",
                    path.display()
                )
            }
            Self::Segment(source) => write!(formatter, "{source}"),
            Self::Json(source) => write!(formatter, "catalog JSON failed: {source}"),
            Self::NonResidentExtent {
                extent_index,
                file_offset,
            } => write!(
                formatter,
                "extent {extent_index} at file offset {file_offset} is not resident"
            ),
            Self::MissingExtent {
                segment_id,
                segment_offset,
            } => write!(
                formatter,
                "missing extent payload in segment {segment_id} at offset {segment_offset}"
            ),
        }
    }
}

impl std::error::Error for CatalogStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Segment(source) => Some(source),
            Self::Json(source) => Some(source),
            Self::NonResidentExtent { .. } => None,
            Self::MissingExtent { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, fs};

    use super::{
        CatalogDirectory, CatalogDirectoryEntry, CatalogExtent, CatalogFileState, CatalogInode,
        CatalogStore, CatalogTombstone,
    };
    use crate::segment::SegmentWriter;
    use legato_proto::TransferClass;
    use legato_types::FileId;
    use tempfile::tempdir;

    #[test]
    fn catalog_replays_inode_directory_and_tombstone_records() {
        let temp = tempdir().expect("tempdir should exist");
        let root = temp.path().join("store");
        let mut store = CatalogStore::open(&root, 100).expect("catalog should open");
        let root_inode = CatalogInode::directory(FileId(1), "/", 10);
        let file_inode = sample_inode();
        let file_entry = CatalogDirectoryEntry {
            name: String::from("piano.wav"),
            path: String::from("/piano.wav"),
            file_id: FileId(7),
            is_dir: false,
        };

        store
            .append_inode(root_inode)
            .expect("root inode should append");
        store
            .append_inode(file_inode.clone())
            .expect("file inode should append");
        store
            .append_directory(CatalogDirectory {
                directory_id: FileId(1),
                path: String::from("/"),
                entries: BTreeMap::from([(file_entry.name.clone(), file_entry.clone())]),
            })
            .expect("directory should append");

        assert_eq!(store.resolve_path("/piano.wav"), Some(&file_inode));
        assert_eq!(store.resolve_file_id(FileId(7)), Some(&file_inode));
        assert_eq!(store.list_directory("/"), Some(vec![file_entry]));

        drop(store);
        let mut reopened = CatalogStore::open(&root, 200).expect("catalog should reopen");
        assert_eq!(reopened.resolve_path("/piano.wav"), Some(&file_inode));

        reopened
            .append_tombstone(CatalogTombstone {
                path: String::from("/piano.wav"),
                file_id: Some(FileId(7)),
            })
            .expect("tombstone should append");
        assert!(reopened.resolve_path("/piano.wav").is_none());
        assert!(reopened.resolve_file_id(FileId(7)).is_none());
        assert_eq!(reopened.list_directory("/"), Some(Vec::new()));
    }

    #[test]
    fn checkpoint_bounds_replay_and_preserves_cursor() {
        let temp = tempdir().expect("tempdir should exist");
        let root = temp.path().join("store");
        let mut store = CatalogStore::open(&root, 100).expect("catalog should open");
        let inode = sample_inode();
        store
            .append_inode(inode.clone())
            .expect("inode should append");
        store
            .append_subscription_cursor(41)
            .expect("cursor should append");
        let checkpoint = store.checkpoint().expect("checkpoint should write");
        assert_eq!(checkpoint.subscription_cursor, 41);
        let checkpoint_sequence = checkpoint.sequence;
        drop(store);

        let reopened = CatalogStore::open(&root, 200).expect("catalog should reopen");

        assert_eq!(reopened.resolve_path("/piano.wav"), Some(&inode));
        assert_eq!(reopened.subscription_cursor(), 41);
        assert_eq!(reopened.last_sequence(), checkpoint_sequence);
    }

    #[test]
    fn replay_truncates_incomplete_tail_before_loading() {
        let temp = tempdir().expect("tempdir should exist");
        let root = temp.path().join("store");
        let mut store = CatalogStore::open(&root, 100).expect("catalog should open");
        store
            .append_inode(sample_inode())
            .expect("inode should append");
        let clean_len = fs::metadata(root.join("segments").join("00000000000000000001.lseg"))
            .expect("metadata should load")
            .len();
        drop(store);

        let tail_path = root.join("segments").join("00000000000000000001.lseg");
        let mut raw_writer = SegmentWriter::create(
            root.join("segments").join("00000000000000000002.lseg"),
            2,
            200,
        )
        .expect("raw segment should create");
        raw_writer
            .append(crate::segment::StoreRecordKind::Inode, 10, b"{\"broken\"")
            .expect("raw incomplete payload seed should append");
        drop(raw_writer);
        fs::OpenOptions::new()
            .write(true)
            .open(root.join("segments").join("00000000000000000002.lseg"))
            .expect("raw segment should open")
            .set_len(28 + 10)
            .expect("raw segment should be truncated");

        let reopened = CatalogStore::open(&root, 300).expect("catalog should repair and reopen");

        assert!(reopened.resolve_path("/piano.wav").is_some());
        assert!(fs::metadata(tail_path).expect("metadata should load").len() >= clean_len);
    }

    fn sample_inode() -> CatalogInode {
        CatalogInode::file(
            FileId(7),
            "/piano.wav",
            CatalogFileState {
                inode_generation: 1,
                size: 4096,
                mtime_ns: 11,
                content_hash: b"payload".to_vec(),
                transfer_class: TransferClass::Streamed,
                extents: vec![CatalogExtent {
                    extent_index: 0,
                    file_offset: 0,
                    length: 4096,
                    payload_hash: blake3::hash(b"payload").as_bytes().to_vec(),
                    transfer_class: TransferClass::Streamed as i32,
                    segment_id: Some(9),
                    segment_offset: Some(128),
                }],
            },
        )
    }
}
