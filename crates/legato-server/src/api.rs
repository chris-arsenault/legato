//! Metadata RPC implementations over the server metadata database.

use std::path::Path;

use legato_proto::{
    ChangeKind, ChangeRecord, DirectoryEntry, FileMetadata, InodeMetadata, InvalidationEvent,
    ListDirRequest, ListDirResponse, ResolvePathRequest, ResolvePathResponse, StatRequest,
    StatResponse, TransferClass,
};
use rusqlite::{Connection, OptionalExtension};

use crate::{
    NotificationAction, ReconcileStats, invalidation_events_for_action, plan_notification_result,
    reconcile_library_root, reconcile_paths,
};

/// Server-side implementation of the non-block metadata RPC surface.
#[derive(Debug)]
pub struct MetadataService {
    connection: Connection,
}

/// Catalog-backed metadata for one resolved path.
#[derive(Clone, Debug)]
pub struct CatalogEntry {
    /// Base metadata exposed to legacy metadata callers.
    pub metadata: FileMetadata,
    /// Persisted layout classification for file content.
    pub transfer_class: Option<TransferClass>,
    /// Persisted preferred extent size for file content.
    pub extent_bytes: Option<u64>,
}

impl MetadataService {
    /// Creates a metadata service backed by the provided SQLite connection.
    #[must_use]
    pub fn new(connection: Connection) -> Self {
        Self { connection }
    }

    /// Returns metadata for the requested path when it exists.
    pub fn stat(&self, request: StatRequest) -> rusqlite::Result<Option<StatResponse>> {
        lookup_metadata(&self.connection, &request.path).map(|metadata| {
            metadata.map(|metadata| StatResponse {
                metadata: Some(metadata),
            })
        })
    }

    /// Resolves a path to stable metadata for prefetch planning or lookup.
    pub fn resolve_path(
        &self,
        request: ResolvePathRequest,
    ) -> rusqlite::Result<Option<ResolvePathResponse>> {
        lookup_catalog_entry(&self.connection, &request.path).map(|entry| {
            entry.map(|entry| ResolvePathResponse {
                metadata: Some(entry.metadata),
            })
        })
    }

    /// Resolves a path to catalog-backed metadata and persisted layout values.
    pub fn resolve_catalog_path(&self, path: &str) -> rusqlite::Result<Option<CatalogEntry>> {
        lookup_catalog_entry(&self.connection, path)
    }

    /// Resolves one catalog entry by stable file identifier.
    pub fn resolve_catalog_file_id(&self, file_id: u64) -> rusqlite::Result<Option<CatalogEntry>> {
        lookup_file_only_metadata_by_id(&self.connection, file_id)
    }

    /// Loads durable change records after the provided sequence cursor.
    pub fn change_records_since(&self, since_sequence: u64) -> rusqlite::Result<Vec<ChangeRecord>> {
        let mut statement = self.connection.prepare(
            "SELECT sequence, kind, file_id, path, is_dir, size, mtime_ns, transfer_class, extent_bytes
             FROM change_log
             WHERE sequence > ?1
             ORDER BY sequence",
        )?;
        statement
            .query_map([since_sequence as i64], |row| {
                let sequence: i64 = row.get(0)?;
                let kind: i64 = row.get(1)?;
                let file_id: i64 = row.get(2)?;
                let path: String = row.get(3)?;
                let is_dir: i64 = row.get(4)?;
                let size: i64 = row.get(5)?;
                let mtime_ns: i64 = row.get(6)?;
                let transfer_class: Option<i64> = row.get(7)?;
                let extent_bytes: Option<i64> = row.get(8)?;

                Ok(ChangeRecord {
                    sequence: sequence as u64,
                    kind: kind as i32,
                    file_id: file_id as u64,
                    path: path.clone(),
                    inode: if kind == ChangeKind::Delete as i64 {
                        None
                    } else {
                        Some(InodeMetadata {
                            file_id: file_id as u64,
                            path,
                            size: size as u64,
                            mtime_ns: mtime_ns as u64,
                            is_dir: is_dir != 0,
                            layout: transfer_class
                                .and_then(|transfer_class| {
                                    TransferClass::try_from(transfer_class as i32).ok()
                                })
                                .zip(extent_bytes)
                                .map(|(transfer_class, extent_bytes)| {
                                    crate::LayoutDecision {
                                        transfer_class,
                                        extent_bytes: extent_bytes as u64,
                                    }
                                    .file_layout(size as u64, is_dir != 0)
                                }),
                        })
                    },
                })
            })?
            .collect()
    }

    /// Lists the direct children of a directory path.
    pub fn list_dir(&self, request: ListDirRequest) -> rusqlite::Result<Option<ListDirResponse>> {
        let directory_id = self
            .connection
            .query_row(
                "SELECT directory_id FROM directories WHERE path = ?1",
                [request.path.as_str()],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;

        let Some(directory_id) = directory_id else {
            return Ok(None);
        };

        let mut entries = Vec::new();

        let mut directory_stmt = self.connection.prepare(
            "SELECT directory_id, path FROM directories
             WHERE parent_directory_id = ?1
             ORDER BY path",
        )?;
        let directory_rows = directory_stmt.query_map([directory_id], |row| {
            let child_id: i64 = row.get(0)?;
            let path: String = row.get(1)?;
            Ok(DirectoryEntry {
                name: entry_name(&path),
                path,
                is_dir: true,
                file_id: child_id as u64,
            })
        })?;
        for row in directory_rows {
            entries.push(row?);
        }

        let mut file_stmt = self.connection.prepare(
            "SELECT file_id, path FROM files
             WHERE directory_id = ?1
             ORDER BY path",
        )?;
        let file_rows = file_stmt.query_map([directory_id], |row| {
            let file_id: i64 = row.get(0)?;
            let path: String = row.get(1)?;
            Ok(DirectoryEntry {
                name: entry_name(&path),
                path,
                is_dir: false,
                file_id: file_id as u64,
            })
        })?;
        for row in file_rows {
            entries.push(row?);
        }

        Ok(Some(ListDirResponse { entries }))
    }

    /// Applies one filesystem notification result and returns the resulting stats and invalidations.
    pub fn apply_notification(
        &mut self,
        library_root: &Path,
        result: notify::Result<notify::Event>,
    ) -> rusqlite::Result<(ReconcileStats, Vec<InvalidationEvent>)> {
        let action = plan_notification_result(library_root, result);
        let stats = match &action {
            NotificationAction::FullRescan => {
                reconcile_library_root(&mut self.connection, library_root)?
            }
            NotificationAction::Paths(paths) => {
                reconcile_paths(&mut self.connection, library_root, paths)?
            }
        };
        let invalidations =
            invalidation_events_for_action(&self.connection, library_root, &action)?;
        Ok((stats, invalidations))
    }
}

fn lookup_metadata(connection: &Connection, path: &str) -> rusqlite::Result<Option<FileMetadata>> {
    if let Some(entry) = lookup_catalog_entry(connection, path)? {
        return Ok(Some(entry.metadata));
    }
    Ok(None)
}

fn lookup_directory_metadata(
    connection: &Connection,
    path: &str,
) -> rusqlite::Result<Option<CatalogEntry>> {
    connection
        .query_row(
            "SELECT directory_id, path, mtime_ns FROM directories WHERE path = ?1",
            [path],
            |row| {
                let directory_id: i64 = row.get(0)?;
                let path: String = row.get(1)?;
                let mtime_ns: i64 = row.get(2)?;
                Ok(CatalogEntry {
                    metadata: FileMetadata {
                        file_id: directory_id as u64,
                        path,
                        size: 0,
                        mtime_ns: mtime_ns as u64,
                        content_hash: Vec::new(),
                        is_dir: true,
                    },
                    transfer_class: None,
                    extent_bytes: None,
                })
            },
        )
        .optional()
}

fn lookup_catalog_entry(
    connection: &Connection,
    path: &str,
) -> rusqlite::Result<Option<CatalogEntry>> {
    if let Some(metadata) = lookup_directory_metadata(connection, path)? {
        return Ok(Some(metadata));
    }
    lookup_file_only_metadata(connection, path)
}

fn lookup_file_only_metadata_by_id(
    connection: &Connection,
    file_id: u64,
) -> rusqlite::Result<Option<CatalogEntry>> {
    connection
        .query_row(
            "SELECT file_id, path, size, mtime_ns, COALESCE(content_hash, x''),
                    transfer_class, extent_bytes
             FROM files
             WHERE file_id = ?1",
            [file_id as i64],
            |row| {
                let file_id: i64 = row.get(0)?;
                let path: String = row.get(1)?;
                let size: i64 = row.get(2)?;
                let mtime_ns: i64 = row.get(3)?;
                let content_hash: Vec<u8> = row.get(4)?;
                let transfer_class: i64 = row.get(5)?;
                let extent_bytes: i64 = row.get(6)?;
                Ok(CatalogEntry {
                    metadata: FileMetadata {
                        file_id: file_id as u64,
                        path,
                        size: size as u64,
                        mtime_ns: mtime_ns as u64,
                        content_hash,
                        is_dir: false,
                    },
                    transfer_class: TransferClass::try_from(transfer_class as i32).ok(),
                    extent_bytes: Some(extent_bytes as u64),
                })
            },
        )
        .optional()
}

fn lookup_file_only_metadata(
    connection: &Connection,
    path: &str,
) -> rusqlite::Result<Option<CatalogEntry>> {
    connection
        .query_row(
            "SELECT file_id, path, size, mtime_ns, COALESCE(content_hash, x''),
                    transfer_class, extent_bytes
             FROM files
             WHERE path = ?1",
            [path],
            |row| {
                let file_id: i64 = row.get(0)?;
                let path: String = row.get(1)?;
                let size: i64 = row.get(2)?;
                let mtime_ns: i64 = row.get(3)?;
                let content_hash: Vec<u8> = row.get(4)?;
                let transfer_class: i64 = row.get(5)?;
                let extent_bytes: i64 = row.get(6)?;
                Ok(CatalogEntry {
                    metadata: FileMetadata {
                        file_id: file_id as u64,
                        path,
                        size: size as u64,
                        mtime_ns: mtime_ns as u64,
                        content_hash,
                        is_dir: false,
                    },
                    transfer_class: TransferClass::try_from(transfer_class as i32).ok(),
                    extent_bytes: Some(extent_bytes as u64),
                })
            },
        )
        .optional()
}

fn entry_name(path: &str) -> String {
    Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .map(String::from)
        .unwrap_or_else(|| String::from(path))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use legato_proto::{ListDirRequest, ResolvePathRequest, StatRequest, TransferClass};
    use tempfile::{TempDir, tempdir};

    use super::MetadataService;
    use crate::{open_metadata_database, reconcile_library_root};

    #[test]
    fn stat_and_resolve_path_return_file_metadata() {
        let (_library_dir, _db_dir, library_root, service, sample_path) = build_service_fixture();

        let stat = service
            .stat(StatRequest {
                path: sample_path.clone(),
            })
            .expect("stat should succeed")
            .expect("sample file should exist");
        let resolved = service
            .resolve_path(ResolvePathRequest {
                path: library_root.join("Kontakt").to_string_lossy().into_owned(),
            })
            .expect("resolve should succeed")
            .expect("directory should resolve");

        assert_eq!(stat.metadata.expect("metadata").path, sample_path);
        assert!(resolved.metadata.expect("metadata").is_dir);
    }

    #[test]
    fn resolve_catalog_path_returns_persisted_layout_metadata() {
        let (_library_dir, _db_dir, _library_root, service, sample_path) = build_service_fixture();

        let entry = service
            .resolve_catalog_path(&sample_path)
            .expect("resolve should succeed")
            .expect("sample file should exist");

        assert_eq!(entry.metadata.path, sample_path);
        assert_eq!(entry.transfer_class, Some(TransferClass::Unitary));
        assert_eq!(entry.extent_bytes, Some(10));
    }

    #[test]
    fn change_records_since_replays_durable_catalog_mutations() {
        let (_library_dir, _db_dir, _library_root, service, sample_path) = build_service_fixture();

        let changes = service
            .change_records_since(0)
            .expect("change log should load");

        assert!(
            changes.iter().any(|change| {
                change.path == sample_path && change.kind == legato_proto::ChangeKind::Upsert as i32
            }),
            "expected durable upsert for the sample file"
        );
    }

    #[test]
    fn list_dir_returns_direct_children_only() {
        let (_library_dir, _db_dir, library_root, service, sample_path) = build_service_fixture();

        let listing = service
            .list_dir(ListDirRequest {
                path: library_root.to_string_lossy().into_owned(),
            })
            .expect("list dir should succeed")
            .expect("library root should exist");

        let names = listing
            .entries
            .into_iter()
            .map(|entry| entry.name)
            .collect::<Vec<_>>();

        assert_eq!(names, vec![String::from("Kontakt")]);

        let kontakt_listing = service
            .list_dir(ListDirRequest {
                path: library_root.join("Kontakt").to_string_lossy().into_owned(),
            })
            .expect("list dir should succeed")
            .expect("Kontakt dir should exist");
        let kontakt_names = kontakt_listing
            .entries
            .into_iter()
            .map(|entry| entry.path)
            .collect::<Vec<_>>();

        assert_eq!(kontakt_names, vec![sample_path]);
    }

    fn build_service_fixture() -> (
        TempDir,
        TempDir,
        std::path::PathBuf,
        MetadataService,
        String,
    ) {
        let fixture = tempdir().expect("fixture tempdir should be created");
        let library_root = fixture.path().join("libraries");
        fs::create_dir_all(library_root.join("Kontakt")).expect("library tree should be created");
        let sample_path = library_root.join("Kontakt").join("piano.nki");
        fs::write(&sample_path, b"abcdefghij").expect("fixture file should be written");

        let db_dir = tempdir().expect("db tempdir should be created");
        let mut connection =
            open_metadata_database(&db_dir.path().join("server.sqlite")).expect("db should open");
        reconcile_library_root(&mut connection, &library_root).expect("reconcile should succeed");
        (
            fixture,
            db_dir,
            library_root,
            MetadataService::new(connection),
            sample_path.to_string_lossy().into_owned(),
        )
    }
}
