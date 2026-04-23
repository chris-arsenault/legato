//! Append-only Legato segment file primitives.

use std::{
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, Write},
    path::{Path, PathBuf},
};

const SEGMENT_MAGIC: &[u8; 8] = b"LGTOSEG1";
const RECORD_MAGIC: &[u8; 4] = b"LREC";
const FOOTER_MAGIC: &[u8; 4] = b"LEND";
const SEGMENT_VERSION: u32 = 1;
const RECORD_HEADER_LEN: u64 = 4 + 1 + 8 + 8 + 32;
const FOOTER_LEN: u64 = 4 + 8 + 8;

/// Type tag for a Legato record stored inside a segment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum StoreRecordKind {
    /// File extent payload plus extent metadata.
    Extent = 1,
    /// File identity, metadata, and extent-map data.
    Inode = 2,
    /// Directory membership data.
    Dirent = 3,
    /// Logical deletion marker.
    Tombstone = 4,
    /// Durable recovery boundary.
    Checkpoint = 5,
}

impl TryFrom<u8> for StoreRecordKind {
    type Error = SegmentStoreError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Extent),
            2 => Ok(Self::Inode),
            3 => Ok(Self::Dirent),
            4 => Ok(Self::Tombstone),
            5 => Ok(Self::Checkpoint),
            other => Err(SegmentStoreError::UnknownRecordKind(other)),
        }
    }
}

impl From<StoreRecordKind> for u8 {
    fn from(value: StoreRecordKind) -> Self {
        value as u8
    }
}

/// Header stored at the front of each segment file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentHeader {
    /// Monotonic segment format version.
    pub version: u32,
    /// Logical identifier for this segment.
    pub segment_id: u64,
    /// Creation timestamp supplied by the caller.
    pub created_at_ns: u64,
}

/// Footer written when a segment is sealed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentFooter {
    /// Number of complete records in the segment.
    pub record_count: u64,
    /// Highest sequence number contained in the segment.
    pub last_sequence: u64,
}

/// One validated record read from a segment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreRecord {
    /// Record type.
    pub kind: StoreRecordKind,
    /// Globally ordered record sequence number.
    pub sequence: u64,
    /// Payload bytes.
    pub payload: Vec<u8>,
    /// BLAKE3 hash of the payload bytes.
    pub payload_hash: [u8; 32],
}

impl StoreRecord {
    /// Creates a record and computes its payload hash.
    #[must_use]
    pub fn new(kind: StoreRecordKind, sequence: u64, payload: Vec<u8>) -> Self {
        let payload_hash = *blake3::hash(&payload).as_bytes();
        Self {
            kind,
            sequence,
            payload,
            payload_hash,
        }
    }
}

/// Result of scanning a segment file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentScan {
    /// Segment header.
    pub header: SegmentHeader,
    /// Complete validated records.
    pub records: Vec<StoreRecord>,
    /// Footer when the segment has been sealed.
    pub footer: Option<SegmentFooter>,
    /// True when an incomplete tail was ignored or truncated.
    pub truncated_tail: bool,
}

/// Append-only writer for one active segment file.
#[derive(Debug)]
pub struct SegmentWriter {
    path: PathBuf,
    file: File,
    segment_id: u64,
    record_count: u64,
    last_sequence: u64,
    sealed: bool,
}

impl SegmentWriter {
    /// Creates a new segment file and writes its header.
    pub fn create(
        path: impl AsRef<Path>,
        segment_id: u64,
        created_at_ns: u64,
    ) -> Result<Self, SegmentStoreError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|source| SegmentStoreError::Io {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path)
            .map_err(|source| SegmentStoreError::Io {
                path: path.to_path_buf(),
                source,
            })?;

        file.write_all(SEGMENT_MAGIC)
            .and_then(|()| file.write_all(&SEGMENT_VERSION.to_le_bytes()))
            .and_then(|()| file.write_all(&segment_id.to_le_bytes()))
            .and_then(|()| file.write_all(&created_at_ns.to_le_bytes()))
            .map_err(|source| SegmentStoreError::Io {
                path: path.to_path_buf(),
                source,
            })?;

        Ok(Self {
            path: path.to_path_buf(),
            file,
            segment_id,
            record_count: 0,
            last_sequence: 0,
            sealed: false,
        })
    }

    /// Returns the logical ID of the segment being written.
    #[must_use]
    pub fn segment_id(&self) -> u64 {
        self.segment_id
    }

    /// Returns the byte offset where the next append will begin.
    pub fn current_offset(&mut self) -> Result<u64, SegmentStoreError> {
        self.file
            .stream_position()
            .map_err(|source| SegmentStoreError::Io {
                path: self.path.clone(),
                source,
            })
    }

    /// Appends one record to the segment.
    pub fn append(
        &mut self,
        kind: StoreRecordKind,
        sequence: u64,
        payload: &[u8],
    ) -> Result<StoreRecord, SegmentStoreError> {
        if self.sealed {
            return Err(SegmentStoreError::SegmentSealed {
                path: self.path.clone(),
            });
        }

        let record = StoreRecord::new(kind, sequence, payload.to_vec());
        self.file
            .write_all(RECORD_MAGIC)
            .and_then(|()| self.file.write_all(&[u8::from(kind)]))
            .and_then(|()| self.file.write_all(&sequence.to_le_bytes()))
            .and_then(|()| self.file.write_all(&(payload.len() as u64).to_le_bytes()))
            .and_then(|()| self.file.write_all(&record.payload_hash))
            .and_then(|()| self.file.write_all(payload))
            .map_err(|source| SegmentStoreError::Io {
                path: self.path.clone(),
                source,
            })?;
        self.record_count = self.record_count.saturating_add(1);
        self.last_sequence = sequence;
        Ok(record)
    }

    /// Seals the segment by writing an immutable footer.
    pub fn seal(&mut self) -> Result<SegmentFooter, SegmentStoreError> {
        if self.sealed {
            return Err(SegmentStoreError::SegmentSealed {
                path: self.path.clone(),
            });
        }

        let footer = SegmentFooter {
            record_count: self.record_count,
            last_sequence: self.last_sequence,
        };
        self.file
            .write_all(FOOTER_MAGIC)
            .and_then(|()| self.file.write_all(&footer.record_count.to_le_bytes()))
            .and_then(|()| self.file.write_all(&footer.last_sequence.to_le_bytes()))
            .and_then(|()| self.file.flush())
            .map_err(|source| SegmentStoreError::Io {
                path: self.path.clone(),
                source,
            })?;
        self.sealed = true;
        Ok(footer)
    }
}

/// Scans a segment file without mutating it.
pub fn scan_segment(path: impl AsRef<Path>) -> Result<SegmentScan, SegmentStoreError> {
    scan_segment_impl(path.as_ref(), false)
}

/// Scans a segment file and truncates an incomplete tail when one is present.
pub fn repair_incomplete_tail(path: impl AsRef<Path>) -> Result<SegmentScan, SegmentStoreError> {
    scan_segment_impl(path.as_ref(), true)
}

fn scan_segment_impl(path: &Path, repair: bool) -> Result<SegmentScan, SegmentStoreError> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(repair)
        .open(path)
        .map_err(|source| SegmentStoreError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    let header = read_segment_header(path, &mut file)?;
    let mut records = Vec::new();
    let mut footer = None;
    let mut truncated_tail = false;
    let mut last_good_offset = file
        .stream_position()
        .map_err(|source| SegmentStoreError::Io {
            path: path.to_path_buf(),
            source,
        })?;

    loop {
        let record_start = file
            .stream_position()
            .map_err(|source| SegmentStoreError::Io {
                path: path.to_path_buf(),
                source,
            })?;
        let magic = match read_magic_or_tail(path, &mut file)? {
            MagicRead::Complete(magic) => magic,
            MagicRead::End => break,
            MagicRead::Incomplete => {
                truncated_tail = true;
                break;
            }
        };

        if magic == *FOOTER_MAGIC {
            if let Some(segment_footer) = read_footer_body(path, &mut file)? {
                footer = Some(segment_footer);
                last_good_offset = record_start + FOOTER_LEN;
            } else {
                truncated_tail = true;
            }
            break;
        }

        if magic != *RECORD_MAGIC {
            return Err(SegmentStoreError::InvalidMagic {
                path: path.to_path_buf(),
                offset: record_start,
            });
        }

        match read_record_body(path, &mut file, record_start)? {
            TailRead::Complete(record) => {
                last_good_offset = record_start
                    + RECORD_HEADER_LEN
                    + u64::try_from(record.payload.len()).unwrap_or(u64::MAX);
                records.push(record);
            }
            TailRead::Incomplete => {
                truncated_tail = true;
                break;
            }
        }
    }

    if repair && truncated_tail {
        file.set_len(last_good_offset)
            .map_err(|source| SegmentStoreError::Io {
                path: path.to_path_buf(),
                source,
            })?;
    }

    Ok(SegmentScan {
        header,
        records,
        footer,
        truncated_tail,
    })
}

fn read_segment_header(path: &Path, file: &mut File) -> Result<SegmentHeader, SegmentStoreError> {
    let mut magic = [0_u8; 8];
    file.read_exact(&mut magic)
        .map_err(|source| SegmentStoreError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    if &magic != SEGMENT_MAGIC {
        return Err(SegmentStoreError::InvalidMagic {
            path: path.to_path_buf(),
            offset: 0,
        });
    }

    let version = read_u32(path, file)?;
    if version != SEGMENT_VERSION {
        return Err(SegmentStoreError::UnsupportedVersion(version));
    }

    Ok(SegmentHeader {
        version,
        segment_id: read_u64(path, file)?,
        created_at_ns: read_u64(path, file)?,
    })
}

enum MagicRead {
    Complete([u8; 4]),
    End,
    Incomplete,
}

fn read_magic_or_tail(path: &Path, file: &mut File) -> Result<MagicRead, SegmentStoreError> {
    let mut magic = [0_u8; 4];
    match file
        .read(&mut magic)
        .map_err(|source| SegmentStoreError::Io {
            path: path.to_path_buf(),
            source,
        })? {
        0 => Ok(MagicRead::End),
        4 => Ok(MagicRead::Complete(magic)),
        _ => Ok(MagicRead::Incomplete),
    }
}

enum TailRead {
    Complete(StoreRecord),
    Incomplete,
}

fn read_record_body(
    path: &Path,
    file: &mut File,
    record_start: u64,
) -> Result<TailRead, SegmentStoreError> {
    let mut fixed = [0_u8; 1 + 8 + 8 + 32];
    if !read_exact_or_tail(path, file, &mut fixed)? {
        return Ok(TailRead::Incomplete);
    }

    let kind = StoreRecordKind::try_from(fixed[0])?;
    let sequence = u64::from_le_bytes(copy_array::<8>(&fixed[1..9]));
    let payload_len = u64::from_le_bytes(copy_array::<8>(&fixed[9..17]));
    let mut payload_hash = [0_u8; 32];
    payload_hash.copy_from_slice(&fixed[17..49]);
    let mut payload = vec![0_u8; payload_len as usize];
    if !read_exact_or_tail(path, file, &mut payload)? {
        return Ok(TailRead::Incomplete);
    }

    let actual_hash = *blake3::hash(&payload).as_bytes();
    if actual_hash != payload_hash {
        return Err(SegmentStoreError::HashMismatch {
            path: path.to_path_buf(),
            offset: record_start,
            sequence,
        });
    }

    Ok(TailRead::Complete(StoreRecord {
        kind,
        sequence,
        payload,
        payload_hash,
    }))
}

fn read_footer_body(
    path: &Path,
    file: &mut File,
) -> Result<Option<SegmentFooter>, SegmentStoreError> {
    let mut body = [0_u8; 16];
    if !read_exact_or_tail(path, file, &mut body)? {
        return Ok(None);
    }
    Ok(Some(SegmentFooter {
        record_count: u64::from_le_bytes(copy_array::<8>(&body[0..8])),
        last_sequence: u64::from_le_bytes(copy_array::<8>(&body[8..16])),
    }))
}

fn copy_array<const N: usize>(slice: &[u8]) -> [u8; N] {
    let mut bytes = [0_u8; N];
    bytes.copy_from_slice(slice);
    bytes
}

fn read_exact_or_tail(
    path: &Path,
    file: &mut File,
    buffer: &mut [u8],
) -> Result<bool, SegmentStoreError> {
    match file.read_exact(buffer) {
        Ok(()) => Ok(true),
        Err(source) if source.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
        Err(source) => Err(SegmentStoreError::Io {
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn read_u32(path: &Path, file: &mut File) -> Result<u32, SegmentStoreError> {
    let mut bytes = [0_u8; 4];
    file.read_exact(&mut bytes)
        .map_err(|source| SegmentStoreError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64(path: &Path, file: &mut File) -> Result<u64, SegmentStoreError> {
    let mut bytes = [0_u8; 8];
    file.read_exact(&mut bytes)
        .map_err(|source| SegmentStoreError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    Ok(u64::from_le_bytes(bytes))
}

/// Error returned while reading or writing Legato segment files.
#[derive(Debug)]
pub enum SegmentStoreError {
    /// Filesystem IO failed.
    Io {
        /// Path involved in the failed operation.
        path: PathBuf,
        /// Source IO error.
        source: io::Error,
    },
    /// The file did not contain the expected segment, record, or footer magic.
    InvalidMagic {
        /// Segment path.
        path: PathBuf,
        /// Byte offset where the bad magic was observed.
        offset: u64,
    },
    /// Segment version is not supported by this code.
    UnsupportedVersion(u32),
    /// Record kind tag is unknown.
    UnknownRecordKind(u8),
    /// A sealed segment was used for another append or seal operation.
    SegmentSealed {
        /// Segment path.
        path: PathBuf,
    },
    /// A record payload hash did not match its stored hash.
    HashMismatch {
        /// Segment path.
        path: PathBuf,
        /// Record byte offset.
        offset: u64,
        /// Record sequence number.
        sequence: u64,
    },
}

impl fmt::Display for SegmentStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { path, source } => {
                write!(
                    formatter,
                    "segment IO failed for {}: {source}",
                    path.display()
                )
            }
            Self::InvalidMagic { path, offset } => {
                write!(
                    formatter,
                    "invalid segment magic in {} at offset {offset}",
                    path.display()
                )
            }
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported segment version {version}")
            }
            Self::UnknownRecordKind(kind) => write!(formatter, "unknown record kind {kind}"),
            Self::SegmentSealed { path } => {
                write!(formatter, "segment is sealed: {}", path.display())
            }
            Self::HashMismatch {
                path,
                offset,
                sequence,
            } => write!(
                formatter,
                "segment record hash mismatch in {} at offset {offset} for sequence {sequence}",
                path.display()
            ),
        }
    }
}

impl std::error::Error for SegmentStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        SegmentStoreError, SegmentWriter, StoreRecord, StoreRecordKind, repair_incomplete_tail,
        scan_segment,
    };
    use std::{
        fs::{self, OpenOptions},
        io::{Seek, SeekFrom, Write},
    };
    use tempfile::tempdir;

    #[test]
    fn segment_append_scan_and_seal_round_trip() {
        let temp = tempdir().expect("tempdir should be created");
        let path = temp.path().join("segments").join("00000001.lseg");
        let mut writer = SegmentWriter::create(&path, 1, 100).expect("segment should create");
        let expected = vec![
            writer
                .append(StoreRecordKind::Inode, 7, b"inode-record")
                .expect("inode should append"),
            writer
                .append(StoreRecordKind::Extent, 8, b"extent-record")
                .expect("extent should append"),
        ];
        let footer = writer.seal().expect("segment should seal");

        let scan = scan_segment(&path).expect("segment should scan");

        assert_eq!(scan.header.segment_id, 1);
        assert_eq!(scan.header.created_at_ns, 100);
        assert_eq!(scan.records, expected);
        assert_eq!(scan.footer, Some(footer));
        assert!(!scan.truncated_tail);
    }

    #[test]
    fn sealed_segment_rejects_more_records() {
        let temp = tempdir().expect("tempdir should be created");
        let path = temp.path().join("sealed.lseg");
        let mut writer = SegmentWriter::create(&path, 1, 100).expect("segment should create");
        let _ = writer
            .append(StoreRecordKind::Checkpoint, 1, b"checkpoint")
            .expect("record should append");
        let _ = writer.seal().expect("segment should seal");

        let error = writer
            .append(StoreRecordKind::Tombstone, 2, b"delete")
            .expect_err("sealed segment should reject append");

        assert!(matches!(error, SegmentStoreError::SegmentSealed { .. }));
    }

    #[test]
    fn scan_rejects_corrupt_record_payload_hash() {
        let temp = tempdir().expect("tempdir should be created");
        let path = temp.path().join("corrupt.lseg");
        let mut writer = SegmentWriter::create(&path, 1, 100).expect("segment should create");
        let _ = writer
            .append(StoreRecordKind::Extent, 1, b"healthy")
            .expect("record should append");
        drop(writer);

        let mut file = OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("segment should open for corruption");
        file.seek(SeekFrom::End(-1))
            .expect("seek to payload tail should work");
        file.write_all(b"x")
            .expect("payload byte should be corrupted");

        let error = scan_segment(&path).expect_err("corrupt hash should fail scan");

        assert!(matches!(error, SegmentStoreError::HashMismatch { .. }));
    }

    #[test]
    fn repair_truncates_incomplete_tail_record() {
        let temp = tempdir().expect("tempdir should be created");
        let path = temp.path().join("tail.lseg");
        let mut writer = SegmentWriter::create(&path, 1, 100).expect("segment should create");
        let expected = writer
            .append(StoreRecordKind::Dirent, 1, b"dirent")
            .expect("record should append");
        writer
            .append(StoreRecordKind::Extent, 2, b"incomplete")
            .expect("record should append");
        drop(writer);

        let complete_len = segment_len_for_records(std::slice::from_ref(&expected));
        fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("segment should open for truncate")
            .set_len(complete_len + 10)
            .expect("segment should be cut into second record");

        let scan = repair_incomplete_tail(&path).expect("tail repair should succeed");

        assert_eq!(scan.records, vec![expected]);
        assert!(scan.truncated_tail);
        assert_eq!(
            fs::metadata(&path)
                .expect("segment metadata should load")
                .len(),
            complete_len
        );
    }

    fn segment_len_for_records(records: &[StoreRecord]) -> u64 {
        8 + 4
            + 8
            + 8
            + records
                .iter()
                .map(|record| 4 + 1 + 8 + 8 + 32 + record.payload.len() as u64)
                .sum::<u64>()
    }
}
