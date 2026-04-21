//! Binary entrypoint for the native Legato filesystem client.

use legato_client_core::{ClientConfig, ClientRuntime};

fn main() {
    let runtime = ClientRuntime::new(ClientConfig::default());

    #[cfg(target_os = "macos")]
    {
        let adapter = legato_fs_macos::MacosFilesystem::new(runtime);
        println!("legatofs bootstrap ready for {}", adapter.platform_name());
        return;
    }

    #[cfg(target_os = "windows")]
    {
        let adapter = legato_fs_windows::WindowsFilesystem::new(runtime);
        println!("legatofs bootstrap ready for {}", adapter.platform_name());
        return;
    }

    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        let _ = runtime;
        println!("legatofs bootstrap ready for unsupported-host development");
    }
}
