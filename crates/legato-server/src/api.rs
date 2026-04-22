//! Metadata RPC implementations over the server metadata database.

use std::{
    collections::HashMap,
    fs::File,
    io::{self, Read, Seek, SeekFrom},
    path::Path,
};

use legato_proto::{
    BlockResponse, CloseRequest, CloseResponse, DirectoryEntry, FileMetadata, InvalidationEvent,
    ListDirRequest, ListDirResponse, OpenRequest, OpenResponse, ReadBlocksRequest,
    ResolvePathRequest, ResolvePathResponse, StatRequest, StatResponse,
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
    next_handle: u64,
    open_handles: HashMap<u64, OpenHandle>,
}

#[derive(Debug)]
struct OpenHandle {
    file_id: u64,
    path: String,
    block_size: u32,
}

impl MetadataService {
    /// Creates a metadata service backed by the provided SQLite connection.
    #[must_use]
    pub fn new(connection: Connection) -> Self {
        Self {
            connection,
            next_handle: 1,
            open_handles: HashMap::new(),
        }
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
        lookup_metadata(&self.connection, &request.path).map(|metadata| {
            metadata.map(|metadata| ResolvePathResponse {
                metadata: Some(metadata),
            })
        })
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

    /// Opens a file path and returns a server-local handle.
    pub fn open(&mut self, request: OpenRequest) -> rusqlite::Result<Option<OpenResponse>> {
        let Some(metadata) = lookup_file_only_metadata(&self.connection, &request.path)? else {
            return Ok(None);
        };

        let file_handle = self.next_handle;
        self.next_handle += 1;
        self.open_handles.insert(
            file_handle,
            OpenHandle {
                file_id: metadata.file_id,
                path: metadata.path.clone(),
                block_size: metadata.block_size,
            },
        );

        Ok(Some(OpenResponse {
            file_handle,
            file_id: metadata.file_id,
            size: metadata.size,
            mtime_ns: metadata.mtime_ns,
            content_hash: metadata.content_hash,
            block_size: metadata.block_size,
        }))
    }

    /// Reads one or more aligned block ranges from already opened file handles.
    pub fn read_blocks(&self, request: ReadBlocksRequest) -> rusqlite::Result<Vec<BlockResponse>> {
        let mut responses = Vec::new();

        for range in request.ranges {
            let handle = self.open_handles.get(&range.file_handle).ok_or_else(|| {
                invalid_request_error(format!("unknown file handle {}", range.file_handle))
            })?;
            let block_size = u64::from(handle.block_size);

            if block_size == 0 {
                return Err(invalid_request_error(format!(
                    "file handle {} has an invalid block size",
                    range.file_handle
                )));
            }
            if range.start_offset % block_size != 0 {
                return Err(invalid_request_error(format!(
                    "start offset {} is not aligned to block size {} for file handle {}",
                    range.start_offset, block_size, range.file_handle
                )));
            }

            let mut file = File::open(&handle.path).map_err(io_error)?;
            file.seek(SeekFrom::Start(range.start_offset))
                .map_err(io_error)?;

            for block_index in 0..range.block_count {
                let offset = range.start_offset + u64::from(block_index) * block_size;
                let Some(data) = read_block(&mut file, handle.block_size)? else {
                    break;
                };

                responses.push(BlockResponse {
                    file_handle: range.file_handle,
                    offset,
                    block_hash: blake3::hash(&data).as_bytes().to_vec(),
                    data,
                });
            }
        }

        Ok(responses)
    }

    /// Closes a previously issued file handle.
    #[must_use]
    pub fn close(&mut self, request: CloseRequest) -> CloseResponse {
        self.open_handles.remove(&request.file_handle);
        CloseResponse {}
    }

    /// Returns whether a handle is still open.
    #[must_use]
    pub fn is_handle_open(&self, file_handle: u64) -> bool {
        self.open_handles.contains_key(&file_handle)
    }

    /// Returns the file ID associated with an open handle.
    #[must_use]
    pub fn file_id_for_handle(&self, file_handle: u64) -> Option<u64> {
        self.open_handles
            .get(&file_handle)
            .map(|handle| handle.file_id)
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
    if let Some(metadata) = lookup_directory_metadata(connection, path)? {
        return Ok(Some(metadata));
    }

    lookup_file_only_metadata(connection, path)
}

fn lookup_directory_metadata(
    connection: &Connection,
    path: &str,
) -> rusqlite::Result<Option<FileMetadata>> {
    connection
        .query_row(
            "SELECT directory_id, path, mtime_ns FROM directories WHERE path = ?1",
            [path],
            |row| {
                let directory_id: i64 = row.get(0)?;
                let path: String = row.get(1)?;
                let mtime_ns: i64 = row.get(2)?;
                Ok(FileMetadata {
                    file_id: directory_id as u64,
                    path,
                    size: 0,
                    mtime_ns: mtime_ns as u64,
                    content_hash: Vec::new(),
                    is_dir: true,
                    block_size: 0,
                })
            },
        )
        .optional()
}

fn lookup_file_only_metadata(
    connection: &Connection,
    path: &str,
) -> rusqlite::Result<Option<FileMetadata>> {
    connection
        .query_row(
            "SELECT file_id, path, size, mtime_ns, COALESCE(content_hash, x''), block_size
             FROM files
             WHERE path = ?1",
            [path],
            |row| {
                let file_id: i64 = row.get(0)?;
                let path: String = row.get(1)?;
                let size: i64 = row.get(2)?;
                let mtime_ns: i64 = row.get(3)?;
                let content_hash: Vec<u8> = row.get(4)?;
                let block_size: i64 = row.get(5)?;
                Ok(FileMetadata {
                    file_id: file_id as u64,
                    path,
                    size: size as u64,
                    mtime_ns: mtime_ns as u64,
                    content_hash,
                    is_dir: false,
                    block_size: block_size as u32,
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

fn read_block(file: &mut File, block_size: u32) -> rusqlite::Result<Option<Vec<u8>>> {
    let mut buffer = vec![0_u8; block_size as usize];
    let bytes_read = file.read(&mut buffer).map_err(io_error)?;
    if bytes_read == 0 {
        return Ok(None);
    }
    buffer.truncate(bytes_read);
    Ok(Some(buffer))
}

fn io_error(error: io::Error) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(error))
}

fn invalid_request_error(message: String) -> rusqlite::Error {
    rusqlite::Error::InvalidParameterName(message)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use legato_proto::{
        CloseRequest, ListDirRequest, OpenRequest, ReadBlocksRequest, ResolvePathRequest,
        StatRequest,
    };
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

    #[test]
    fn open_and_close_manage_server_side_handles() {
        let (_library_dir, _db_dir, _library_root, mut service, sample_path) =
            build_service_fixture();

        let open = service
            .open(OpenRequest { path: sample_path })
            .expect("open should succeed")
            .expect("sample file should open");

        assert!(service.is_handle_open(open.file_handle));

        let _ = service.close(CloseRequest {
            file_handle: open.file_handle,
        });

        assert!(!service.is_handle_open(open.file_handle));
    }

    #[test]
    fn read_blocks_returns_aligned_ranges_with_tail_hashes() {
        let (_library_dir, _db_dir, _library_root, mut service, sample_path) =
            build_service_fixture();

        let open = service
            .open(OpenRequest { path: sample_path })
            .expect("open should succeed")
            .expect("sample file should open");

        let blocks = service
            .read_blocks(ReadBlocksRequest {
                ranges: vec![legato_proto::BlockRequest {
                    file_handle: open.file_handle,
                    start_offset: 0,
                    block_count: 4,
                }],
            })
            .expect("read blocks should succeed");

        assert_eq!(
            service.file_id_for_handle(open.file_handle),
            Some(open.file_id)
        );
        assert_eq!(blocks.len(), 3);
        assert_eq!(blocks[0].offset, 0);
        assert_eq!(blocks[0].data, b"abcd");
        assert_eq!(
            blocks[0].block_hash,
            blake3::hash(b"abcd").as_bytes().to_vec()
        );
        assert_eq!(blocks[1].offset, 4);
        assert_eq!(blocks[1].data, b"efgh");
        assert_eq!(
            blocks[1].block_hash,
            blake3::hash(b"efgh").as_bytes().to_vec()
        );
        assert_eq!(blocks[2].offset, 8);
        assert_eq!(blocks[2].data, b"ij");
        assert_eq!(
            blocks[2].block_hash,
            blake3::hash(b"ij").as_bytes().to_vec()
        );
    }

    #[test]
    fn read_blocks_rejects_unknown_handles_and_unaligned_offsets() {
        let (_library_dir, _db_dir, _library_root, mut service, sample_path) =
            build_service_fixture();

        let open = service
            .open(OpenRequest { path: sample_path })
            .expect("open should succeed")
            .expect("sample file should open");

        let unknown_handle_error = service
            .read_blocks(ReadBlocksRequest {
                ranges: vec![legato_proto::BlockRequest {
                    file_handle: open.file_handle + 99,
                    start_offset: 0,
                    block_count: 1,
                }],
            })
            .expect_err("unknown handles should be rejected");
        assert!(
            unknown_handle_error
                .to_string()
                .contains("unknown file handle"),
            "error should mention the unknown handle: {unknown_handle_error}"
        );

        let unaligned_error = service
            .read_blocks(ReadBlocksRequest {
                ranges: vec![legato_proto::BlockRequest {
                    file_handle: open.file_handle,
                    start_offset: 2,
                    block_count: 1,
                }],
            })
            .expect_err("unaligned offsets should be rejected");
        assert!(
            unaligned_error.to_string().contains("not aligned"),
            "error should mention alignment: {unaligned_error}"
        );
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
        connection
            .execute(
                "UPDATE files SET block_size = 4 WHERE path = ?1",
                [sample_path.to_string_lossy().as_ref()],
            )
            .expect("test block size should update");

        (
            fixture,
            db_dir,
            library_root,
            MetadataService::new(connection),
            sample_path.to_string_lossy().into_owned(),
        )
    }
}
