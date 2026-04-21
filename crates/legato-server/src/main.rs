//! Binary entrypoint for the Legato server daemon.

use legato_foundation::{CommonProcessConfig, FoundationError, init_tracing, load_config};
use legato_server::{Server, ServerConfig};
use serde::Deserialize;

#[derive(Debug, Default, Deserialize)]
struct ServerProcessConfig {
    #[serde(default)]
    common: CommonProcessConfig,
}

fn main() -> Result<(), FoundationError> {
    let process_config = load_config::<ServerProcessConfig>(None, "LEGATO_SERVER")
        .unwrap_or_else(|_| ServerProcessConfig::default());
    init_tracing("legato-server", &process_config.common.tracing)?;

    let _server = Server::new(ServerConfig::default());
    println!("legato-server bootstrap ready");
    Ok(())
}
