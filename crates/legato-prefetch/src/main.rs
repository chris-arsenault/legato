//! Binary entrypoint for the Legato project-aware prefetch planner.

use legato_foundation::{
    CommonProcessConfig, ProcessTelemetry, ShutdownController, init_tracing, load_config,
};
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
    let shutdown = ShutdownController::new();
    let telemetry = ProcessTelemetry::new("legato-prefetch", &process_config.common.metrics);
    telemetry.record_startup();
    telemetry.set_lifecycle_state("bootstrap", 1);
    let _metrics_exporter = telemetry.spawn_exporter(shutdown.token())?;

    let command = parse_cli_args(std::env::args())?;
    let result = run_cli_command(command)?;
    telemetry.set_lifecycle_state("ready", 1);
    println!("{}", result.output);
    std::process::exit(result.exit_code);
}
