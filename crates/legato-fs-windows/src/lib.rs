//! Windows-specific filesystem adapter scaffolding.

use legato_client_core::ClientRuntime;

/// Adapter wrapper for the eventual WinFSP-backed filesystem implementation.
#[derive(Debug)]
pub struct WindowsFilesystem {
    runtime: ClientRuntime,
}

impl WindowsFilesystem {
    /// Creates a new Windows adapter shell around the shared client runtime.
    #[must_use]
    pub fn new(runtime: ClientRuntime) -> Self {
        Self { runtime }
    }

    /// Returns a stable platform identifier for diagnostics and tests.
    #[must_use]
    pub fn platform_name(&self) -> &'static str {
        let _ = &self.runtime;
        "windows"
    }
}

#[cfg(test)]
mod tests {
    use super::WindowsFilesystem;
    use legato_client_core::{ClientConfig, ClientRuntime};

    #[test]
    fn adapter_is_constructible_on_non_windows_hosts() {
        let adapter = WindowsFilesystem::new(ClientRuntime::new(ClientConfig::default()));
        assert_eq!(adapter.platform_name(), "windows");
    }
}
