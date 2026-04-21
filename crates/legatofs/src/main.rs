//! Binary entrypoint for the native Legato filesystem client.

use legato_client_core::{ClientConfig, ClientRuntime};
use legato_foundation::{CommonProcessConfig, FoundationError, init_tracing, load_config};
use serde::Deserialize;

#[derive(Debug, Default, Deserialize)]
struct ClientProcessConfig {
    #[serde(default)]
    common: CommonProcessConfig,
}

fn main() -> Result<(), FoundationError> {
    let process_config = load_config::<ClientProcessConfig>(None, "LEGATO_FS")
        .unwrap_or_else(|_| ClientProcessConfig::default());
    init_tracing("legatofs", &process_config.common.tracing)?;

    let runtime = ClientRuntime::new(ClientConfig::default());

    #[cfg(target_os = "macos")]
    {
        let adapter = legato_fs_macos::MacosFilesystem::new(runtime);
        println!("legatofs bootstrap ready for {}", adapter.platform_name());
        return Ok(());
    }

    #[cfg(target_os = "windows")]
    {
        let adapter = legato_fs_windows::WindowsFilesystem::new(runtime);
        println!("legatofs bootstrap ready for {}", adapter.platform_name());
        return Ok(());
    }

    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        let _ = runtime;
        println!("legatofs bootstrap ready for unsupported-host development");
        Ok(())
    }
}
