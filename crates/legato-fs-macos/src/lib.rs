//! macOS-specific filesystem adapter scaffolding.

use legato_client_core::ClientRuntime;

/// Adapter wrapper for the eventual macOS filesystem implementation.
#[derive(Debug)]
pub struct MacosFilesystem {
    runtime: ClientRuntime,
}

impl MacosFilesystem {
    /// Creates a new macOS adapter shell around the shared client runtime.
    #[must_use]
    pub fn new(runtime: ClientRuntime) -> Self {
        Self { runtime }
    }

    /// Returns a stable platform identifier for diagnostics and tests.
    #[must_use]
    pub fn platform_name(&self) -> &'static str {
        let _ = &self.runtime;
        "macos"
    }
}

#[cfg(test)]
mod tests {
    use super::MacosFilesystem;
    use legato_client_core::{ClientConfig, ClientRuntime};

    #[test]
    fn adapter_is_constructible_on_non_macos_hosts() {
        let adapter = MacosFilesystem::new(ClientRuntime::new(ClientConfig::default()));
        assert_eq!(adapter.platform_name(), "macos");
    }
}
