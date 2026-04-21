//! Binary entrypoint for the Legato project-aware prefetch planner.

use std::path::PathBuf;

use legato_foundation::{CommonProcessConfig, FoundationError, init_tracing, load_config};
use legato_types::{PrefetchHintPath, PrefetchPriority};
use serde::Deserialize;

#[derive(Debug, Default, Deserialize)]
struct PrefetchProcessConfig {
    #[serde(default)]
    common: CommonProcessConfig,
}

fn main() -> Result<(), FoundationError> {
    let process_config = load_config::<PrefetchProcessConfig>(None, "LEGATO_PREFETCH")
        .unwrap_or_else(|_| PrefetchProcessConfig::default());
    init_tracing("legato-prefetch", &process_config.common.tracing)?;

    let _placeholder = PrefetchHintPath {
        path: PathBuf::from("."),
        start_offset: 0,
        block_count: 0,
        priority: PrefetchPriority::P3,
    };
    println!("legato-prefetch bootstrap ready");
    Ok(())
}
