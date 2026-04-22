//! Binary entrypoint for the native Legato filesystem client.

use std::{
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use legato_client_cache::{BlockCacheStore, open_cache_database};
use legato_client_core::{ClientConfig, ClientRuntime, LocalControlPlane};
use legato_foundation::{
    CommonProcessConfig, ProcessTelemetry, ShutdownController, init_tracing, load_config,
};
use legato_proto::FileMetadata;
use legato_types::{
    ClientPlatform, FileId, FilesystemAttributes, FilesystemError, FilesystemSemantics,
    platform_error_code,
};
use serde::Deserialize;

#[derive(Debug, Default, Deserialize)]
struct ClientProcessConfig {
    #[serde(default)]
    common: CommonProcessConfig,
    #[serde(default)]
    client: ClientConfig,
    #[serde(default)]
    mount: MountConfig,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct MountConfig {
    #[serde(default)]
    mount_point: String,
    #[serde(default = "default_library_root")]
    library_root: String,
    #[serde(default = "default_state_dir")]
    state_dir: String,
}

impl Default for MountConfig {
    fn default() -> Self {
        Self {
            mount_point: default_mount_point(),
            library_root: default_library_root(),
            state_dir: default_state_dir(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StartupContext {
    platform: ClientPlatform,
    mount_point: String,
    semantics: FilesystemSemantics,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let process_config =
        load_config::<ClientProcessConfig>(Some(default_config_path()), "LEGATO_FS")
            .unwrap_or_else(|_| ClientProcessConfig::default());
    init_tracing("legatofs", &process_config.common.tracing)?;
    let shutdown = ShutdownController::new();
    let telemetry = ProcessTelemetry::new("legatofs", &process_config.common.metrics);
    telemetry.record_startup();
    telemetry.set_lifecycle_state("bootstrap", 1);
    let _metrics_exporter = telemetry.spawn_exporter(shutdown.token())?;

    let runtime = ClientRuntime::new(process_config.client.clone());
    let startup = startup_context(&process_config.mount);
    let control = control_plane_for_mount(&process_config.mount, startup.semantics)?;

    #[cfg(target_os = "macos")]
    {
        let adapter = legato_fs_macos::MacosFilesystem::new(runtime, startup.mount_point.clone());
        let _ = &mut control;
        telemetry.set_lifecycle_state("ready", 1);
        println!("legatofs bootstrap ready for {}", adapter.platform_name());
        return Ok(());
    }

    #[cfg(target_os = "windows")]
    {
        let adapter =
            legato_fs_windows::WindowsFilesystem::new(runtime, startup.mount_point.clone());
        let _ = &mut control;
        telemetry.set_lifecycle_state("ready", 1);
        println!("legatofs bootstrap ready for {}", adapter.platform_name());
        return Ok(());
    }

    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        let _ = runtime;
        let _ = control;
        telemetry.set_lifecycle_state("ready", 1);
        println!("legatofs bootstrap ready for unsupported-host development");
        Ok(())
    }
}

fn startup_context(mount: &MountConfig) -> StartupContext {
    #[cfg(target_os = "macos")]
    let platform = ClientPlatform::Macos;
    #[cfg(target_os = "windows")]
    let platform = ClientPlatform::Windows;
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    let platform = ClientPlatform::Macos;

    let semantics = FilesystemSemantics::default();
    let _ = platform_error_code(platform, FilesystemError::ReadOnly);

    StartupContext {
        platform,
        mount_point: mount.mount_point.clone(),
        semantics,
    }
}

fn control_plane_for_mount(
    mount: &MountConfig,
    semantics: FilesystemSemantics,
) -> Result<LocalControlPlane, Box<dyn std::error::Error>> {
    let database = open_cache_database(&Path::new(&mount.state_dir).join("client.sqlite"))?;
    let _store = BlockCacheStore::new(&Path::new(&mount.state_dir).join("blocks"), database)?;
    let mut control = LocalControlPlane::new(
        legato_client_cache::MetadataCache::new(legato_client_cache::MetadataCachePolicy::default()),
        1 << 20,
    );
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos() as u64);
    control.register_path(mount_root_attributes(mount, semantics), now_ns);
    Ok(control)
}

fn mount_root_attributes(mount: &MountConfig, semantics: FilesystemSemantics) -> FileMetadata {
    let attributes = FilesystemAttributes {
        file_id: FileId(1),
        path: PathBuf::from(&mount.library_root),
        is_dir: true,
        size: 0,
        mtime_ns: 0,
        block_size: 1 << 20,
        read_only: semantics.read_only,
    };

    FileMetadata {
        file_id: attributes.file_id.0,
        path: attributes.path.to_string_lossy().into_owned(),
        size: attributes.size,
        mtime_ns: attributes.mtime_ns,
        content_hash: Vec::new(),
        is_dir: attributes.is_dir,
        block_size: attributes.block_size,
    }
}

fn default_mount_point() -> String {
    #[cfg(target_os = "macos")]
    {
        return String::from("/Volumes/Legato");
    }
    #[cfg(target_os = "windows")]
    {
        return String::from("L:\\Legato");
    }
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        String::from("/tmp/legato")
    }
}

fn default_config_path() -> &'static Path {
    #[cfg(target_os = "macos")]
    {
        return Path::new("/Library/Application Support/Legato/legatofs.toml");
    }
    #[cfg(target_os = "windows")]
    {
        return Path::new("C:\\ProgramData\\Legato\\legatofs.toml");
    }
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        Path::new("/tmp/legatofs.toml")
    }
}

fn default_library_root() -> String {
    String::from("/srv/libraries")
}

fn default_state_dir() -> String {
    #[cfg(target_os = "macos")]
    {
        return String::from("/Library/Application Support/Legato");
    }
    #[cfg(target_os = "windows")]
    {
        return String::from("C:\\ProgramData\\Legato");
    }
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        String::from("/tmp/legato-state")
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ClientProcessConfig, MountConfig, default_config_path, default_mount_point,
        mount_root_attributes, startup_context,
    };
    use legato_types::{FilesystemOperation, FilesystemSemantics};

    #[test]
    fn mount_config_defaults_are_present() {
        let config = ClientProcessConfig::default();

        assert_eq!(config.mount.mount_point, default_mount_point());
        assert!(!config.mount.state_dir.is_empty());
    }

    #[test]
    fn startup_context_uses_read_only_semantics() {
        let startup = startup_context(&MountConfig::default());

        assert_eq!(startup.semantics, FilesystemSemantics::default());
        assert!(startup.semantics.denies(FilesystemOperation::Write));
    }

    #[test]
    fn mount_root_attributes_expose_directory_metadata() {
        let metadata =
            mount_root_attributes(&MountConfig::default(), FilesystemSemantics::default());

        assert!(metadata.is_dir);
        assert_eq!(metadata.file_id, 1);
    }

    #[test]
    fn default_config_path_is_present_for_platform() {
        assert!(!default_config_path().as_os_str().is_empty());
    }
}
