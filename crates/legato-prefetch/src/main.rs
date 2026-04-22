//! Binary entrypoint for the Legato project-aware prefetch planner.

use legato_foundation::{CommonProcessConfig, init_tracing, load_config};
use legato_prefetch::{parse_cli_args, run_cli_command};
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

    let command = parse_cli_args(std::env::args())?;
    let result = run_cli_command(command)?;
    println!("{}", result.output);
    std::process::exit(result.exit_code);
}
