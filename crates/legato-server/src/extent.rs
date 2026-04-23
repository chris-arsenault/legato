//! Server-side semantic extent materialization.

use std::{
    fs,
    io::{self, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    time::Instant,
};

use legato_proto::{ExtentRecord, ExtentRef};

use crate::CatalogEntry;

/// Server-owned extent materialization root under the writable state directory.
#[derive(Clone, Debug)]
pub struct ServerExtentStore {
    root: PathBuf,
}

/// Source used to satisfy one extent fetch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExtentFetchSource {
    /// The extent was already materialized under the server state root.
    CacheHit,
    /// The extent had to be read from the canonical library dataset.
    SourceRead,
}

/// Result of one extent fetch, including materialization source and timing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchedExtent {
    /// Wire record returned to the caller.
    pub record: ExtentRecord,
    /// Where the payload was sourced from.
    pub source: ExtentFetchSource,
    /// Total fetch time in nanoseconds.
    pub elapsed_ns: u64,
}

impl ServerExtentStore {
    /// Creates an extent store rooted under `state_dir/extents`.
    #[must_use]
    pub fn new(state_dir: &Path) -> Self {
        Self {
            root: state_dir.join("extents"),
        }
    }

    /// Materializes one semantic extent and returns the wire record.
    pub fn fetch_extent(
        &self,
        entry: &CatalogEntry,
        extent: &ExtentRef,
    ) -> Result<FetchedExtent, io::Error> {
        let started = Instant::now();
        if entry.metadata.is_dir {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "directories do not have fetchable extents",
            ));
        }

        let transfer_class = entry.transfer_class.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "missing transfer class for file",
            )
        })?;
        let extent_bytes = entry.extent_bytes.ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "missing extent size for file")
        })?;
        let layout = crate::LayoutDecision {
            transfer_class,
            extent_bytes,
        }
        .file_layout(entry.metadata.size, false);
        let descriptor = layout
            .extents
            .iter()
            .find(|descriptor| descriptor.extent_index == extent.extent_index)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "unknown extent index"))?;

        if extent.file_offset != 0 && extent.file_offset != descriptor.file_offset {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "requested extent offset does not match catalog layout",
            ));
        }
        if extent.length != 0 && extent.length != descriptor.length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "requested extent length does not match catalog layout",
            ));
        }

        let materialized_path = self.materialized_path(
            entry.metadata.file_id,
            entry.metadata.mtime_ns,
            extent.extent_index,
        );
        let (data, source) = if materialized_path.exists() {
            (fs::read(&materialized_path)?, ExtentFetchSource::CacheHit)
        } else {
            let data = read_extent_from_source(
                Path::new(&entry.metadata.path),
                descriptor.file_offset,
                descriptor.length,
            )?;
            if let Some(parent) = materialized_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(&materialized_path, &data)?;
            (data, ExtentFetchSource::SourceRead)
        };

        Ok(FetchedExtent {
            record: ExtentRecord {
                file_id: entry.metadata.file_id,
                extent_index: extent.extent_index,
                file_offset: descriptor.file_offset,
                data: data.clone(),
                extent_hash: blake3::hash(&data).as_bytes().to_vec(),
                transfer_class: transfer_class as i32,
            },
            source,
            elapsed_ns: started.elapsed().as_nanos() as u64,
        })
    }

    fn materialized_path(&self, file_id: u64, mtime_ns: u64, extent_index: u32) -> PathBuf {
        self.root
            .join(file_id.to_string())
            .join(format!("{mtime_ns}-{extent_index}.bin"))
    }
}

fn read_extent_from_source(
    path: &Path,
    file_offset: u64,
    length: u64,
) -> Result<Vec<u8>, io::Error> {
    let mut file = fs::File::open(path)?;
    file.seek(SeekFrom::Start(file_offset))?;
    let mut buffer = vec![0_u8; length as usize];
    file.read_exact(&mut buffer)?;
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use legato_proto::{ExtentRef, FileMetadata, TransferClass};
    use tempfile::tempdir;

    use super::{ExtentFetchSource, ServerExtentStore};
    use crate::CatalogEntry;

    #[test]
    fn fetch_extent_materializes_and_hashes_catalog_slices() {
        let fixture = tempdir().expect("fixture should exist");
        let state_dir = fixture.path().join("state");
        let source_path = fixture.path().join("sample.wav");
        fs::write(&source_path, b"abcdefgh").expect("source should be written");

        let store = ServerExtentStore::new(&state_dir);
        let entry = CatalogEntry {
            metadata: FileMetadata {
                file_id: 7,
                path: source_path.to_string_lossy().into_owned(),
                size: 8,
                mtime_ns: 11,
                content_hash: Vec::new(),
                is_dir: false,
                block_size: 0,
            },
            transfer_class: Some(TransferClass::Random),
            extent_bytes: Some(4),
        };

        let extent = store
            .fetch_extent(
                &entry,
                &ExtentRef {
                    file_id: 7,
                    extent_index: 1,
                    file_offset: 4,
                    length: 4,
                },
            )
            .expect("extent fetch should succeed");

        assert_eq!(extent.record.data, b"efgh");
        assert_eq!(extent.record.file_offset, 4);
        assert_eq!(extent.record.transfer_class, TransferClass::Random as i32);
        assert_eq!(
            extent.record.extent_hash,
            blake3::hash(b"efgh").as_bytes().to_vec()
        );
        assert_eq!(extent.source, ExtentFetchSource::SourceRead);
        assert!(
            state_dir.join("extents/7/11-1.bin").exists(),
            "materialized extent should be persisted"
        );
    }

    #[test]
    fn fetch_extent_reports_cache_hit_for_materialized_extent() {
        let fixture = tempdir().expect("fixture should exist");
        let state_dir = fixture.path().join("state");
        let source_path = fixture.path().join("sample.wav");
        fs::write(&source_path, b"abcdefgh").expect("source should be written");

        let store = ServerExtentStore::new(&state_dir);
        let entry = CatalogEntry {
            metadata: FileMetadata {
                file_id: 7,
                path: source_path.to_string_lossy().into_owned(),
                size: 8,
                mtime_ns: 11,
                content_hash: Vec::new(),
                is_dir: false,
                block_size: 0,
            },
            transfer_class: Some(TransferClass::Random),
            extent_bytes: Some(4),
        };
        let extent_ref = ExtentRef {
            file_id: 7,
            extent_index: 1,
            file_offset: 4,
            length: 4,
        };

        let first = store
            .fetch_extent(&entry, &extent_ref)
            .expect("cold extent fetch should succeed");
        let second = store
            .fetch_extent(&entry, &extent_ref)
            .expect("warm extent fetch should succeed");

        assert_eq!(first.source, ExtentFetchSource::SourceRead);
        assert_eq!(second.source, ExtentFetchSource::CacheHit);
        assert_eq!(second.record.data, b"efgh");
    }
}
