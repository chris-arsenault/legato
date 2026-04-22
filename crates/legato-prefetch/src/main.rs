//! Binary entrypoint for the Legato project-aware prefetch planner.

use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use legato_client_cache::{
    BlockCacheStore, MetadataCache, MetadataCachePolicy, open_cache_database,
};
use legato_client_core::LocalControlPlane;
use legato_foundation::{CommonProcessConfig, init_tracing, load_config};
use legato_proto::FileMetadata;
use legato_types::{FileId, PrefetchHintPath, PrefetchPriority};
use serde::Deserialize;

#[derive(Debug, Default, Deserialize)]
struct PrefetchProcessConfig {
    #[serde(default)]
    common: CommonProcessConfig,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let process_config = load_config::<PrefetchProcessConfig>(None, "LEGATO_PREFETCH")
        .unwrap_or_else(|_| PrefetchProcessConfig::default());
    init_tracing("legato-prefetch", &process_config.common.tracing)?;

    let hint = PrefetchHintPath {
        path: PathBuf::from("."),
        start_offset: 0,
        block_count: 1,
        priority: PrefetchPriority::P3,
    };
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos() as u64);
    let cache_root = std::env::temp_dir().join("legato-prefetch");
    let database = open_cache_database(&cache_root.join("client.sqlite"))?;
    let mut store = BlockCacheStore::new(&cache_root.join("blocks"), database)?;
    let mut control =
        LocalControlPlane::new(MetadataCache::new(MetadataCachePolicy::default()), 1 << 20);
    control.register_path(
        FileMetadata {
            file_id: FileId(1).0,
            path: String::from("."),
            size: 0,
            mtime_ns: now_ns,
            content_hash: Vec::new(),
            is_dir: false,
            block_size: 1 << 20,
        },
        now_ns,
    );
    let _ = control.prefetch_paths(
        &[hint],
        PrefetchPriority::P3,
        &mut store,
        now_ns,
        |_file_id, _offset| Vec::new(),
    );
    println!("legato-prefetch bootstrap ready");
    Ok(())
}
