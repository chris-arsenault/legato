//! Client partial-replica store built on Legato catalog and segment records.

use std::{collections::BTreeMap, fs, path::Path};

use legato_proto::{ExtentRecord, InodeMetadata, InvalidationEvent, InvalidationKind};
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
        let catalog_inode = proto_to_catalog_inode(inode, resident_extents::EMPTY);
        let _ = self.catalog.append_inode(catalog_inode)?;
        Ok(())
    }

    /// Returns authoritative inode metadata when known locally.
    #[must_use]
    pub fn resolve_path(&self, path: &str) -> Option<InodeMetadata> {
        self.catalog.resolve_path(path).cloned().map(inode_to_proto)
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
            .find(|extent| extent.extent_index == extent_index)
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
            .flat_map(|inode| inode.extents.iter())
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
            .map(|inode| inode.extents.len())
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
        extents.retain(|extent| extent.extent_index != resident.extent_index);
        extents.push(resident.clone());
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
        if matches!(
            kind,
            InvalidationKind::File | InvalidationKind::Directory | InvalidationKind::Subtree
        ) {
            let _ = self.catalog.append_tombstone(CatalogTombstone {
                path: event.path.clone(),
                file_id: (event.file_id != 0).then_some(FileId(event.file_id)),
            })?;
        }
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
            if inode.is_dir || inode.extents.is_empty() {
                continue;
            }

            inode.extents.sort_by_key(|extent| {
                (
                    std::cmp::Reverse(extent.file_offset),
                    std::cmp::Reverse(extent.extent_index),
                )
            });
            let mut retained = Vec::new();
            for extent in inode.extents {
                if remaining > max_resident_bytes {
                    remaining = remaining.saturating_sub(extent.length);
                    removed_extents += 1;
                    removed_bytes = removed_bytes.saturating_add(extent.length);
                } else {
                    retained.push(extent);
                }
            }
            retained.sort_by_key(|extent| extent.extent_index);
            inode.extents = retained;
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
        .map(|extent| (extent.extent_index, extent))
        .collect::<BTreeMap<_, _>>();
    let extents = inode
        .layout
        .as_ref()
        .map(|layout| {
            layout
                .extents
                .iter()
                .filter_map(|extent| resident_by_index.get(&extent.extent_index).cloned())
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

mod resident_extents {
    use crate::catalog::CatalogExtent;

    pub const EMPTY: [CatalogExtent; 0] = [];
}

#[cfg(test)]
mod tests {
    use super::ClientLegatoStore;
    use legato_proto::{ExtentDescriptor, ExtentRecord, FileLayout, InodeMetadata, TransferClass};
    use legato_types::FileId;
    use tempfile::tempdir;

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
                    extent_hash: Vec::new(),
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
                        extent_hash: Vec::new(),
                    },
                    ExtentDescriptor {
                        extent_index: 1,
                        file_offset: 5,
                        length: 6,
                        extent_hash: Vec::new(),
                    },
                ],
            }),
            inode_generation: 1,
            content_hash: b"firstsecond".to_vec(),
        }
    }
}
