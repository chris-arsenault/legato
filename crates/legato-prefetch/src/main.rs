//! Binary entrypoint for the Legato project-aware prefetch planner.

use std::path::PathBuf;

use legato_types::{PrefetchHintPath, PrefetchPriority};

fn main() {
    let _placeholder = PrefetchHintPath {
        path: PathBuf::from("."),
        start_offset: 0,
        block_count: 0,
        priority: PrefetchPriority::P3,
    };
    println!("legato-prefetch bootstrap ready");
}
