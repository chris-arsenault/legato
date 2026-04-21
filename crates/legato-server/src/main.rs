//! Binary entrypoint for the Legato server daemon.

use std::path::Path;

use legato_foundation::{CommonProcessConfig, FoundationError, init_tracing, load_config};
use legato_server::{Server, ServerConfig};
use serde::Deserialize;

#[derive(Debug, Default, Deserialize)]
struct ServerProcessConfig {
    #[serde(default)]
    common: CommonProcessConfig,
    #[serde(default)]
    server: ServerConfig,
}

fn main() -> Result<(), FoundationError> {
    let process_config = load_config::<ServerProcessConfig>(
        Some(Path::new("/etc/legato/server.toml")),
        "LEGATO_SERVER",
    )
    .unwrap_or_else(|_| ServerProcessConfig::default());
    init_tracing("legato-server", &process_config.common.tracing)?;

    let _server = Server::new(process_config.server);
    println!("legato-server bootstrap ready");
    Ok(())
}
