//! Local block-cache types shared by client-side components.

use legato_types::{BlockRange, FileId};

/// Identity for a single block cache entry.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CacheKey {
    /// Stable identifier of the file containing the block.
    pub file_id: FileId,
    /// Block-aligned starting offset in bytes.
    pub start_offset: u64,
}

impl From<&BlockRange> for CacheKey {
    fn from(range: &BlockRange) -> Self {
        Self {
            file_id: range.file_id,
            start_offset: range.start_offset,
        }
    }
}

/// Minimal cache configuration used by the shared client runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CacheConfig {
    /// Total maximum size of the cache in bytes.
    pub max_bytes: u64,
    /// Fixed block size used by the cache.
    pub block_size: u32,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_bytes: 1_500 * 1024 * 1024 * 1024,
            block_size: 1 << 20,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CacheConfig, CacheKey};
    use legato_types::{BlockRange, FileId};

    #[test]
    fn cache_key_is_derived_from_block_identity() {
        let range = BlockRange {
            file_id: FileId(42),
            start_offset: 2 << 20,
            block_count: 1,
        };

        let key = CacheKey::from(&range);

        assert_eq!(key.file_id, FileId(42));
        assert_eq!(key.start_offset, 2 << 20);
        assert_eq!(CacheConfig::default().block_size, 1 << 20);
    }
}
