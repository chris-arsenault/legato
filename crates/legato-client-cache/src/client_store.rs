//! Client partial-replica store built on Legato catalog and segment records.

use std::{collections::BTreeMap, fs, path::Path};

use legato_proto::{
    ChangeKind, ChangeRecord, DirectoryEntry, ExtentRecord, InodeMetadata, InvalidationEvent,
    InvalidationKind,
};
use legato_types::FileId;

use crate::catalog::{
    CatalogExtent, CatalogFileState, CatalogInode, CatalogStore, CatalogStoreError,
    CatalogTombstone, inode_to_proto,
};

/// Summary returned by client store maintenance operations.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClientStoreMaintenanceReport {
    /// Resident extent references before the operation.
    pub resident_extents_before: usize,
    /// Resident extent references after the operation.
    pub resident_extents_after: usize,
    /// Logical resident bytes before the operation.
    pub resident_bytes_before: u64,
    /// Logical resident bytes after the operation.
    pub resident_bytes_after: u64,
    /// Number of resident extent references removed.
    pub resident_extents_removed: usize,
    /// Logical resident bytes removed from active inode maps.
    pub resident_bytes_removed: u64,
}

/// Resident extent loaded from the local partial replica.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResidentExtent {
    /// File identity.
    pub file_id: FileId,
    /// Logical extent index.
    pub extent_index: u32,
    /// File-relative byte offset.
    pub file_offset: u64,
    /// Verified extent bytes.
    pub data: Vec<u8>,
    /// Stored payload hash.
    pub payload_hash: Vec<u8>,
}

/// Client-side partial Legato store.
#[derive(Debug)]
pub struct ClientLegatoStore {
    catalog: CatalogStore,
}

impl ClientLegatoStore {
    /// Opens a partial client store rooted at the configured state directory.
    pub fn open(state_dir: impl AsRef<Path>, now_ns: u64) -> Result<Self, CatalogStoreError> {
        let state_dir = state_dir.as_ref();
        for child in ["catalog", "segments", "checkpoints"] {
            fs::create_dir_all(state_dir.join(child)).map_err(|source| CatalogStoreError::Io {
                path: state_dir.join(child),
                source,
            })?;
        }
        Ok(Self {
            catalog: CatalogStore::open(state_dir, now_ns)?,
        })
    }

    /// Records authoritative inode metadata in the local catalog.
    pub fn record_inode(&mut self, inode: InodeMetadata) -> Result<(), CatalogStoreError> {
        let resident_extents = self
            .catalog
            .resolve_file_id(FileId(inode.file_id))
            .map(|existing| existing.extents.clone())
            .unwrap_or_default();
        let catalog_inode = proto_to_catalog_inode(inode, resident_extents);
        let _ = self.catalog.append_inode(catalog_inode)?;
        Ok(())
    }

    /// Records one canonical directory listing in the local catalog.
    pub fn record_directory(
        &mut self,
        path: &str,
        file_id: FileId,
        entries: Vec<DirectoryEntry>,
    ) -> Result<(), CatalogStoreError> {
        let directory = crate::catalog::CatalogDirectory {
            directory_id: file_id,
            path: path.to_owned(),
            entries: entries
                .into_iter()
                .map(|entry| {
                    (
                        entry.name.clone(),
                        crate::catalog::CatalogDirectoryEntry {
                            name: entry.name,
                            path: entry.path,
                            file_id: FileId(entry.file_id),
                            is_dir: entry.is_dir,
                        },
                    )
                })
                .collect(),
        };
        let _ = self.catalog.append_directory(directory)?;
        Ok(())
    }

    /// Returns authoritative inode metadata when known locally.
    #[must_use]
    pub fn resolve_path(&self, path: &str) -> Option<InodeMetadata> {
        self.catalog.resolve_path(path).cloned().map(inode_to_proto)
    }

    /// Returns the durable subscription cursor for replay resumption.
    #[must_use]
    pub fn subscription_cursor(&self) -> u64 {
        self.catalog.subscription_cursor()
    }

    /// Returns whether one extent is locally resident.
    pub fn get_extent(
        &self,
        file_id: FileId,
        extent_index: u32,
    ) -> Result<Option<ResidentExtent>, CatalogStoreError> {
        let Some(inode) = self.catalog.resolve_file_id(file_id) else {
            return Ok(None);
        };
        let Some(extent) = inode
            .extents
            .iter()
            .find(|extent| extent.extent_index == extent_index && extent.is_resident())
        else {
            return Ok(None);
        };
        let data = self.catalog.read_extent_payload(extent)?;
        Ok(Some(ResidentExtent {
            file_id,
            extent_index,
            file_offset: extent.file_offset,
            data,
            payload_hash: extent.payload_hash.clone(),
        }))
    }

    /// Returns total logical resident payload bytes currently referenced by active inodes.
    #[must_use]
    pub fn resident_bytes(&self) -> u64 {
        self.catalog
            .active_paths()
            .into_iter()
            .filter_map(|path| self.catalog.resolve_path(&path))
            .flat_map(|inode| inode.extents.iter().filter(|extent| extent.is_resident()))
            .map(|extent| extent.length)
            .sum()
    }

    /// Returns total resident extent references currently tracked by active inodes.
    #[must_use]
    pub fn resident_extent_count(&self) -> usize {
        self.catalog
            .active_paths()
            .into_iter()
            .filter_map(|path| self.catalog.resolve_path(&path))
            .map(|inode| {
                inode
                    .extents
                    .iter()
                    .filter(|extent| extent.is_resident())
                    .count()
            })
            .sum()
    }

    /// Appends a fetched extent and marks it resident in the local inode extent map.
    pub fn put_extent(
        &mut self,
        record: &ExtentRecord,
    ) -> Result<ResidentExtent, CatalogStoreError> {
        let file_id = FileId(record.file_id);
        let existing = self.catalog.resolve_file_id(file_id).cloned();
        let resident = self.catalog.append_extent_payload(
            record.extent_index,
            record.file_offset,
            record
                .transfer_class
                .try_into()
                .unwrap_or(legato_proto::TransferClass::Unspecified),
            &record.data,
        )?;
        let mut extents = existing
            .as_ref()
            .map(|inode| inode.extents.clone())
            .unwrap_or_default();
        let mut replaced = false;
        for extent in &mut extents {
            if extent.extent_index != resident.extent_index {
                continue;
            }
            extent.segment_id = resident.segment_id;
            extent.segment_offset = resident.segment_offset;
            if extent.payload_hash.is_empty() {
                extent.payload_hash = resident.payload_hash.clone();
            }
            if extent.length == 0 {
                extent.length = resident.length;
            }
            replaced = true;
        }
        if !replaced {
            extents.push(resident.clone());
        }
        extents.sort_by_key(|extent| extent.extent_index);

        let inode = if let Some(mut inode) = existing {
            inode.extents = extents;
            inode
        } else {
            CatalogInode::file(
                file_id,
                format!("file:{file_id:?}"),
                CatalogFileState {
                    inode_generation: 1,
                    size: record.file_offset.saturating_add(record.data.len() as u64),
                    mtime_ns: 0,
                    content_hash: Vec::new(),
                    transfer_class: record
                        .transfer_class
                        .try_into()
                        .unwrap_or(legato_proto::TransferClass::Unspecified),
                    extents,
                },
            )
        };
        let _ = self.catalog.append_inode(inode)?;
        Ok(ResidentExtent {
            file_id,
            extent_index: record.extent_index,
            file_offset: record.file_offset,
            data: record.data.clone(),
            payload_hash: resident.payload_hash,
        })
    }

    /// Applies an invalidation to local resident state.
    pub fn apply_invalidation(
        &mut self,
        event: &InvalidationEvent,
    ) -> Result<(), CatalogStoreError> {
        let kind = InvalidationKind::try_from(event.kind).unwrap_or(InvalidationKind::Unspecified);
        match kind {
            InvalidationKind::File | InvalidationKind::Directory => {
                let _ = self.catalog.append_tombstone(CatalogTombstone {
                    path: event.path.clone(),
                    file_id: (event.file_id != 0).then_some(FileId(event.file_id)),
                })?;
            }
            InvalidationKind::Subtree => {
                let matching_paths = self
                    .catalog
                    .active_paths()
                    .into_iter()
                    .filter(|path| path_starts_with(path, &event.path))
                    .collect::<Vec<_>>();
                for path in matching_paths {
                    let file_id = self.catalog.resolve_path(&path).map(|inode| inode.file_id);
                    let _ = self
                        .catalog
                        .append_tombstone(CatalogTombstone { path, file_id })?;
                }
            }
            InvalidationKind::Unspecified => {}
        }
        Ok(())
    }

    /// Applies one ordered change record and advances the local replay cursor.
    pub fn apply_change_record(&mut self, record: &ChangeRecord) -> Result<(), CatalogStoreError> {
        match ChangeKind::try_from(record.kind).unwrap_or(ChangeKind::Unspecified) {
            ChangeKind::Upsert => {
                if let Some(inode) = record.inode.clone() {
                    self.record_inode(inode)?;
                }
                if !record.entries.is_empty() {
                    self.record_directory(
                        &record.path,
                        FileId(record.file_id),
                        record.entries.clone(),
                    )?;
                }
            }
            ChangeKind::Delete | ChangeKind::Invalidate => {
                let _ = self.catalog.append_tombstone(CatalogTombstone {
                    path: record.path.clone(),
                    file_id: (record.file_id != 0).then_some(FileId(record.file_id)),
                })?;
            }
            ChangeKind::Checkpoint => {
                let _ = self.catalog.append_subscription_cursor(record.sequence)?;
                let _ = self.catalog.checkpoint()?;
                return Ok(());
            }
            ChangeKind::Unspecified => {}
        }
        let _ = self.catalog.append_subscription_cursor(record.sequence)?;
        Ok(())
    }

    /// Writes a compact checkpoint for the partial replica.
    pub fn checkpoint(&mut self) -> Result<(), CatalogStoreError> {
        let _ = self.catalog.checkpoint()?;
        Ok(())
    }

    /// Repairs replayable state and writes a fresh checkpoint.
    pub fn repair(&mut self) -> Result<ClientStoreMaintenanceReport, CatalogStoreError> {
        let report = self.current_report();
        self.checkpoint()?;
        Ok(report)
    }

    /// Writes a fresh checkpoint for currently active records.
    pub fn compact(&mut self) -> Result<ClientStoreMaintenanceReport, CatalogStoreError> {
        let report = self.current_report();
        self.checkpoint()?;
        Ok(report)
    }

    /// Drops resident extent references until logical resident bytes fit the configured limit.
    pub fn evict_to_limit(
        &mut self,
        max_resident_bytes: u64,
    ) -> Result<ClientStoreMaintenanceReport, CatalogStoreError> {
        let before = self.current_report();
        if before.resident_bytes_before <= max_resident_bytes {
            return Ok(before);
        }

        let mut remaining = before.resident_bytes_before;
        let mut removed_extents = 0_usize;
        let mut removed_bytes = 0_u64;
        for mut inode in self.catalog.active_inodes() {
            if remaining <= max_resident_bytes {
                break;
            }
            if inode.is_dir || inode.extents.iter().all(|extent| !extent.is_resident()) {
                continue;
            }

            inode.extents.sort_by_key(|extent| {
                (
                    std::cmp::Reverse(extent.file_offset),
                    std::cmp::Reverse(extent.extent_index),
                )
            });
            let mut updated_extents = Vec::with_capacity(inode.extents.len());
            for mut extent in inode.extents {
                if extent.is_resident() && remaining > max_resident_bytes {
                    remaining = remaining.saturating_sub(extent.length);
                    removed_extents += 1;
                    removed_bytes = removed_bytes.saturating_add(extent.length);
                    extent.segment_id = None;
                    extent.segment_offset = None;
                }
                updated_extents.push(extent);
            }
            updated_extents.sort_by_key(|extent| extent.extent_index);
            inode.extents = updated_extents;
            let _ = self.catalog.append_inode(inode)?;
        }
        self.checkpoint()?;

        Ok(ClientStoreMaintenanceReport {
            resident_extents_before: before.resident_extents_before,
            resident_extents_after: self.resident_extent_count(),
            resident_bytes_before: before.resident_bytes_before,
            resident_bytes_after: self.resident_bytes(),
            resident_extents_removed: removed_extents,
            resident_bytes_removed: removed_bytes,
        })
    }

    fn current_report(&self) -> ClientStoreMaintenanceReport {
        ClientStoreMaintenanceReport {
            resident_extents_before: self.resident_extent_count(),
            resident_extents_after: self.resident_extent_count(),
            resident_bytes_before: self.resident_bytes(),
            resident_bytes_after: self.resident_bytes(),
            resident_extents_removed: 0,
            resident_bytes_removed: 0,
        }
    }
}

fn proto_to_catalog_inode(
    inode: InodeMetadata,
    resident_extents: impl IntoIterator<Item = CatalogExtent>,
) -> CatalogInode {
    let resident_by_index = resident_extents
        .into_iter()
        .filter(|extent| extent.is_resident())
        .map(|extent| (extent.extent_index, extent))
        .collect::<BTreeMap<_, _>>();
    let extents = inode
        .layout
        .as_ref()
        .map(|layout| {
            layout
                .extents
                .iter()
                .map(|extent| {
                    let resident = resident_by_index
                        .get(&extent.extent_index)
                        .filter(|resident| {
                            resident.file_offset == extent.file_offset
                                && resident.length == extent.length
                                && (extent.extent_hash.is_empty()
                                    || resident.payload_hash == extent.extent_hash)
                        });
                    CatalogExtent {
                        extent_index: extent.extent_index,
                        file_offset: extent.file_offset,
                        length: extent.length,
                        payload_hash: if extent.extent_hash.is_empty() {
                            resident
                                .map(|resident| resident.payload_hash.clone())
                                .unwrap_or_default()
                        } else {
                            extent.extent_hash.clone()
                        },
                        transfer_class: layout.transfer_class,
                        segment_id: resident.and_then(|resident| resident.segment_id),
                        segment_offset: resident.and_then(|resident| resident.segment_offset),
                    }
                })
                .collect()
        })
        .unwrap_or_default();
    CatalogInode {
        file_id: FileId(inode.file_id),
        path: inode.path,
        inode_generation: inode.inode_generation,
        size: inode.size,
        mtime_ns: inode.mtime_ns as i64,
        is_dir: inode.is_dir,
        content_hash: inode.content_hash,
        transfer_class: inode.layout.map_or(0, |layout| layout.transfer_class),
        extents,
    }
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
    use super::ClientLegatoStore;
    use legato_proto::{
        ChangeKind, ChangeRecord, DirectoryEntry, ExtentDescriptor, ExtentRecord, FileLayout,
        InodeMetadata, InvalidationEvent, InvalidationKind, TransferClass,
    };
    use legato_types::FileId;
    use tempfile::tempdir;

    #[test]
    fn client_store_tracks_authoritative_layout_without_residency() {
        let temp = tempdir().expect("tempdir should exist");
        let state = temp.path().join("state");
        let mut store = ClientLegatoStore::open(&state, 100).expect("store should open");
        store
            .record_inode(sample_inode())
            .expect("inode should record");

        let inode = store
            .resolve_path("/library/piano.wav")
            .expect("inode should resolve");

        assert_eq!(inode.layout.expect("layout should exist").extents.len(), 1);
        assert_eq!(store.resident_extent_count(), 0);
        assert_eq!(store.resident_bytes(), 0);
        assert!(store.get_extent(FileId(7), 0).expect("lookup").is_none());
    }

    #[test]
    fn client_store_round_trips_resident_extent_after_reopen() {
        let temp = tempdir().expect("tempdir should exist");
        let state = temp.path().join("state");
        let mut store = ClientLegatoStore::open(&state, 100).expect("store should open");
        store
            .record_inode(sample_inode())
            .expect("inode should record");
        let resident = store
            .put_extent(&sample_extent(b"resident-data"))
            .expect("extent should store");
        assert_eq!(resident.data, b"resident-data");
        store.checkpoint().expect("checkpoint should write");
        drop(store);

        let reopened = ClientLegatoStore::open(&state, 200).expect("store should reopen");
        let loaded = reopened
            .get_extent(FileId(7), 0)
            .expect("lookup should work")
            .expect("extent should be resident");

        assert_eq!(loaded.data, b"resident-data");
        assert!(state.join("catalog").is_dir());
        assert!(state.join("segments").is_dir());
        assert!(state.join("checkpoints").is_dir());
    }

    #[test]
    fn client_store_rejects_corrupt_resident_extent() {
        let temp = tempdir().expect("tempdir should exist");
        let state = temp.path().join("state");
        let mut store = ClientLegatoStore::open(&state, 100).expect("store should open");
        store
            .record_inode(sample_inode())
            .expect("inode should record");
        store
            .put_extent(&sample_extent(b"resident-data"))
            .expect("extent should store");
        drop(store);

        let segment_path = state.join("segments").join("00000000000000000001.lseg");
        let mut bytes = std::fs::read(&segment_path).expect("segment should read");
        let last = bytes.last_mut().expect("segment has bytes");
        *last ^= 0xff;
        std::fs::write(&segment_path, bytes).expect("segment should corrupt");

        let error =
            ClientLegatoStore::open(&state, 200).expect_err("corrupt resident record should fail");

        assert!(error.to_string().contains("hash mismatch"));
    }

    #[test]
    fn client_store_evicts_resident_extents_to_limit() {
        let temp = tempdir().expect("tempdir should exist");
        let state = temp.path().join("state");
        let mut store = ClientLegatoStore::open(&state, 100).expect("store should open");
        store
            .record_inode(two_extent_inode())
            .expect("inode should record");
        store
            .put_extent(&sample_extent_at(0, 0, b"first"))
            .expect("first extent should store");
        store
            .put_extent(&sample_extent_at(1, 5, b"second"))
            .expect("second extent should store");

        let report = store.evict_to_limit(5).expect("eviction should succeed");

        assert_eq!(report.resident_extents_before, 2);
        assert_eq!(report.resident_extents_after, 1);
        assert_eq!(report.resident_bytes_after, 5);
        assert!(store.get_extent(FileId(7), 0).expect("lookup").is_some());
        assert!(store.get_extent(FileId(7), 1).expect("lookup").is_none());
        let inode = store
            .resolve_path("/library/piano.wav")
            .expect("inode should still resolve after eviction");
        assert_eq!(inode.layout.expect("layout should exist").extents.len(), 2);
    }

    #[test]
    fn client_store_persists_cursor_and_resumes_replay_after_reopen() {
        let temp = tempdir().expect("tempdir should exist");
        let state = temp.path().join("state");
        let mut store = ClientLegatoStore::open(&state, 100).expect("store should open");

        store
            .apply_change_record(&ChangeRecord {
                sequence: 1,
                kind: ChangeKind::Upsert as i32,
                file_id: 11,
                path: String::from("/Kontakt"),
                inode: Some(InodeMetadata {
                    file_id: 11,
                    path: String::from("/Kontakt"),
                    size: 0,
                    mtime_ns: 10,
                    is_dir: true,
                    layout: Some(FileLayout {
                        transfer_class: TransferClass::Unitary as i32,
                        extents: Vec::new(),
                    }),
                    inode_generation: 1,
                    content_hash: Vec::new(),
                }),
                entries: vec![DirectoryEntry {
                    name: String::from("piano.nki"),
                    path: String::from("/Kontakt/piano.nki"),
                    is_dir: false,
                    file_id: 7,
                }],
            })
            .expect("directory replay should apply");
        store
            .apply_change_record(&ChangeRecord {
                sequence: 2,
                kind: ChangeKind::Upsert as i32,
                file_id: 7,
                path: String::from("/Kontakt/piano.nki"),
                inode: Some(InodeMetadata {
                    file_id: 7,
                    path: String::from("/Kontakt/piano.nki"),
                    size: 13,
                    mtime_ns: 123,
                    is_dir: false,
                    layout: Some(FileLayout {
                        transfer_class: TransferClass::Streamed as i32,
                        extents: vec![ExtentDescriptor {
                            extent_index: 0,
                            file_offset: 0,
                            length: 13,
                            extent_hash: Vec::new(),
                        }],
                    }),
                    inode_generation: 1,
                    content_hash: b"resident-data".to_vec(),
                }),
                entries: Vec::new(),
            })
            .expect("file replay should apply");
        store
            .apply_change_record(&ChangeRecord {
                sequence: 3,
                kind: ChangeKind::Checkpoint as i32,
                file_id: 0,
                path: String::from("checkpoint:3"),
                inode: None,
                entries: Vec::new(),
            })
            .expect("checkpoint replay should apply");

        assert_eq!(store.subscription_cursor(), 3);
        assert!(store.resolve_path("/Kontakt").is_some());
        assert_eq!(
            store
                .resolve_path("/Kontakt/piano.nki")
                .and_then(|inode| inode.layout.map(|layout| layout.extents.len())),
            Some(1)
        );

        drop(store);

        let mut reopened = ClientLegatoStore::open(&state, 200).expect("store should reopen");
        assert_eq!(reopened.subscription_cursor(), 3);
        assert!(reopened.resolve_path("/Kontakt").is_some());
        assert_eq!(
            reopened
                .resolve_path("/Kontakt/piano.nki")
                .and_then(|inode| inode.layout.map(|layout| layout.extents.len())),
            Some(1)
        );

        reopened
            .apply_change_record(&ChangeRecord {
                sequence: 4,
                kind: ChangeKind::Delete as i32,
                file_id: 7,
                path: String::from("/Kontakt/piano.nki"),
                inode: None,
                entries: Vec::new(),
            })
            .expect("delete replay should apply after reopen");
        reopened
            .apply_change_record(&ChangeRecord {
                sequence: 5,
                kind: ChangeKind::Upsert as i32,
                file_id: 8,
                path: String::from("/Kontakt/strings.nki"),
                inode: Some(InodeMetadata {
                    file_id: 8,
                    path: String::from("/Kontakt/strings.nki"),
                    size: 21,
                    mtime_ns: 124,
                    is_dir: false,
                    layout: Some(FileLayout {
                        transfer_class: TransferClass::Streamed as i32,
                        extents: vec![ExtentDescriptor {
                            extent_index: 0,
                            file_offset: 0,
                            length: 21,
                            extent_hash: Vec::new(),
                        }],
                    }),
                    inode_generation: 2,
                    content_hash: b"strings-data".to_vec(),
                }),
                entries: Vec::new(),
            })
            .expect("replay resume should apply after reopen");

        assert_eq!(reopened.subscription_cursor(), 5);
        assert!(reopened.resolve_path("/Kontakt/piano.nki").is_none());
        assert!(reopened.resolve_path("/Kontakt/strings.nki").is_some());

        drop(reopened);

        let reopened_again = ClientLegatoStore::open(&state, 300).expect("store should reopen");
        assert_eq!(reopened_again.subscription_cursor(), 5);
        assert!(reopened_again.resolve_path("/Kontakt/piano.nki").is_none());
        assert_eq!(
            reopened_again
                .resolve_path("/Kontakt/strings.nki")
                .and_then(|inode| inode.layout.map(|layout| layout.extents.len())),
            Some(1)
        );
    }

    #[test]
    fn client_store_subtree_invalidation_removes_nested_paths_recursively() {
        let temp = tempdir().expect("tempdir should exist");
        let state = temp.path().join("state");
        let mut store = ClientLegatoStore::open(&state, 100).expect("store should open");
        store
            .record_inode(InodeMetadata {
                file_id: 7,
                path: String::from("/Kontakt/piano.nki"),
                size: 13,
                mtime_ns: 123,
                is_dir: false,
                layout: Some(FileLayout {
                    transfer_class: TransferClass::Streamed as i32,
                    extents: Vec::new(),
                }),
                inode_generation: 1,
                content_hash: b"resident-data".to_vec(),
            })
            .expect("root child should record");
        store
            .record_inode(InodeMetadata {
                file_id: 8,
                path: String::from("/Kontakt/Subdir/strings.nki"),
                size: 21,
                mtime_ns: 124,
                is_dir: false,
                layout: Some(FileLayout {
                    transfer_class: TransferClass::Streamed as i32,
                    extents: Vec::new(),
                }),
                inode_generation: 1,
                content_hash: b"strings".to_vec(),
            })
            .expect("nested child should record");

        store
            .apply_invalidation(&InvalidationEvent {
                kind: InvalidationKind::Subtree as i32,
                path: String::from("/Kontakt"),
                file_id: 0,
                issued_at_ns: 0,
            })
            .expect("subtree invalidation should apply");

        assert!(store.resolve_path("/Kontakt/piano.nki").is_none());
        assert!(store.resolve_path("/Kontakt/Subdir/strings.nki").is_none());
    }

    fn sample_inode() -> InodeMetadata {
        InodeMetadata {
            file_id: 7,
            path: String::from("/library/piano.wav"),
            size: 13,
            mtime_ns: 123,
            is_dir: false,
            layout: Some(FileLayout {
                transfer_class: TransferClass::Streamed as i32,
                extents: vec![ExtentDescriptor {
                    extent_index: 0,
                    file_offset: 0,
                    length: 13,
                    extent_hash: blake3::hash(b"resident-data").as_bytes().to_vec(),
                }],
            }),
            inode_generation: 1,
            content_hash: b"resident-data".to_vec(),
        }
    }

    fn sample_extent(data: &[u8]) -> ExtentRecord {
        sample_extent_at(0, 0, data)
    }

    fn sample_extent_at(extent_index: u32, file_offset: u64, data: &[u8]) -> ExtentRecord {
        ExtentRecord {
            file_id: 7,
            extent_index,
            file_offset,
            data: data.to_vec(),
            extent_hash: blake3::hash(data).as_bytes().to_vec(),
            transfer_class: TransferClass::Streamed as i32,
        }
    }

    fn two_extent_inode() -> InodeMetadata {
        InodeMetadata {
            file_id: 7,
            path: String::from("/library/piano.wav"),
            size: 11,
            mtime_ns: 123,
            is_dir: false,
            layout: Some(FileLayout {
                transfer_class: TransferClass::Streamed as i32,
                extents: vec![
                    ExtentDescriptor {
                        extent_index: 0,
                        file_offset: 0,
                        length: 5,
                        extent_hash: blake3::hash(b"first").as_bytes().to_vec(),
                    },
                    ExtentDescriptor {
                        extent_index: 1,
                        file_offset: 5,
                        length: 6,
                        extent_hash: blake3::hash(b"second").as_bytes().to_vec(),
                    },
                ],
            }),
            inode_generation: 1,
            content_hash: b"firstsecond".to_vec(),
        }
    }
}
