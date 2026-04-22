//! Binary entrypoint for the native Legato filesystem client.

use std::{
    env, fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use legato_client_cache::{BlockCacheStore, open_cache_database};
use legato_client_core::{ClientConfig, FilesystemService, LocalControlPlane};
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Some(command) = parse_command()? {
        return run_command(command).await;
    }

    let process_config =
        load_config::<ClientProcessConfig>(Some(default_config_path()), "LEGATO_FS")
            .unwrap_or_else(|_| ClientProcessConfig::default());
    init_tracing("legatofs", &process_config.common.tracing)?;
    let shutdown = ShutdownController::new();
    let telemetry = ProcessTelemetry::new("legatofs", &process_config.common.metrics);
    telemetry.record_startup();
    telemetry.set_lifecycle_state("bootstrap", 1);
    let _metrics_exporter = telemetry.spawn_exporter(shutdown.token())?;

    let startup = startup_context(&process_config.mount);
    let control = control_plane_for_mount(&process_config.mount, startup.semantics)?;
    let client_name = default_client_name();
    let service = FilesystemService::connect(
        process_config.client.clone(),
        &client_name,
        Path::new(&process_config.mount.state_dir),
    )
    .await?;
    let server_name = service.server_name().to_owned();

    #[cfg(target_os = "macos")]
    {
        let adapter = legato_fs_macos::MacosFilesystem::new(startup.mount_point.clone());
        let _ = service;
        let _ = &mut control;
        telemetry.set_lifecycle_state("ready", 1);
        println!(
            "legatofs connected to {} and bootstrap ready for {}",
            server_name,
            adapter.platform_name()
        );
        return Ok(());
    }

    #[cfg(target_os = "windows")]
    {
        let adapter = legato_fs_windows::WindowsFilesystem::new(startup.mount_point.clone());
        let _ = service;
        let _ = &mut control;
        telemetry.set_lifecycle_state("ready", 1);
        println!(
            "legatofs connected to {} and bootstrap ready for {}",
            server_name,
            adapter.platform_name()
        );
        return Ok(());
    }

    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        let _ = service;
        let _ = control;
        telemetry.set_lifecycle_state("ready", 1);
        println!(
            "legatofs connected to {} and bootstrap ready for unsupported-host development",
            server_name
        );
        Ok(())
    }
}

#[derive(Debug, Eq, PartialEq)]
enum Command {
    Install {
        bundle_dir: PathBuf,
        endpoint: String,
        server_name: String,
        mount_point: String,
        state_dir: PathBuf,
        library_root: String,
        force: bool,
    },
    Smoke {
        config_path: Option<PathBuf>,
        path: String,
        offset: u64,
        size: u32,
    },
}

fn parse_command() -> Result<Option<Command>, Box<dyn std::error::Error>> {
    parse_command_impl(env::args().skip(1))
}

fn parse_command_impl<I>(arguments: I) -> Result<Option<Command>, Box<dyn std::error::Error>>
where
    I: IntoIterator<Item = String>,
{
    let mut arguments = arguments.into_iter();
    let Some(command) = arguments.next() else {
        return Ok(None);
    };

    match command.as_str() {
        "install" => {
            let mut bundle_dir = None;
            let mut endpoint = None;
            let mut server_name = None;
            let mut mount_point = None;
            let mut state_dir = None;
            let mut library_root = None;
            let mut force = false;

            while let Some(argument) = arguments.next() {
                match argument.as_str() {
                    "--bundle-dir" => bundle_dir = arguments.next().map(PathBuf::from),
                    "--endpoint" => endpoint = arguments.next(),
                    "--server-name" => server_name = arguments.next(),
                    "--mount-point" => mount_point = arguments.next(),
                    "--state-dir" => state_dir = arguments.next().map(PathBuf::from),
                    "--library-root" => library_root = arguments.next(),
                    "--force" => force = true,
                    other => {
                        return Err(format!("unsupported argument for install: {other}").into());
                    }
                }
            }

            Ok(Some(Command::Install {
                bundle_dir: bundle_dir.ok_or("missing --bundle-dir for install")?,
                endpoint: endpoint.ok_or("missing --endpoint for install")?,
                server_name: server_name.ok_or("missing --server-name for install")?,
                mount_point: mount_point.unwrap_or_else(default_mount_point),
                state_dir: state_dir.unwrap_or_else(|| PathBuf::from(default_state_dir())),
                library_root: library_root.unwrap_or_else(default_library_root),
                force,
            }))
        }
        "smoke" => {
            let mut config_path = None;
            let mut path = None;
            let mut offset = 0_u64;
            let mut size = 4096_u32;

            while let Some(argument) = arguments.next() {
                match argument.as_str() {
                    "--config" => config_path = arguments.next().map(PathBuf::from),
                    "--path" => path = arguments.next(),
                    "--offset" => {
                        offset = arguments
                            .next()
                            .ok_or("missing value for --offset")?
                            .parse()?;
                    }
                    "--size" => {
                        size = arguments
                            .next()
                            .ok_or("missing value for --size")?
                            .parse()?;
                    }
                    other => return Err(format!("unsupported argument for smoke: {other}").into()),
                }
            }

            Ok(Some(Command::Smoke {
                config_path,
                path: path.ok_or("missing --path for smoke")?,
                offset,
                size,
            }))
        }
        other => Err(format!("unsupported legatofs command: {other}").into()),
    }
}

async fn run_command(command: Command) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        Command::Install {
            bundle_dir,
            endpoint,
            server_name,
            mount_point,
            state_dir,
            library_root,
            force,
        } => {
            install_client_bundle(
                &bundle_dir,
                &state_dir,
                &endpoint,
                &server_name,
                &mount_point,
                &library_root,
                force,
            )?;
            println!(
                "installed Legato client config into {}",
                state_dir.display()
            );
            Ok(())
        }
        Command::Smoke {
            config_path,
            path,
            offset,
            size,
        } => {
            let process_config =
                load_config::<ClientProcessConfig>(config_path.as_deref(), "LEGATO_FS")?;
            let mut service = FilesystemService::connect(
                process_config.client.clone(),
                default_client_name(),
                Path::new(&process_config.mount.state_dir),
            )
            .await?;
            let attributes = service.lookup(&path).await?;
            if attributes.is_dir {
                let entries = service.read_dir(&path).await?;
                println!(
                    "smoke ok: server={} path={} entries={}",
                    service.server_name(),
                    path,
                    entries.len()
                );
                return Ok(());
            }

            let handle = service.open(&path).await?;
            let bytes = service.read(handle.local_handle, offset, size).await?;
            service.release(handle.local_handle).await?;
            println!(
                "smoke ok: server={} path={} bytes={} offset={} size={}",
                service.server_name(),
                path,
                bytes.len(),
                offset,
                size
            );
            Ok(())
        }
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

fn install_client_bundle(
    bundle_dir: &Path,
    state_dir: &Path,
    endpoint: &str,
    server_name: &str,
    mount_point: &str,
    library_root: &str,
    force: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let cert_dir = state_dir.join("certs");
    fs::create_dir_all(&cert_dir)?;
    fs::create_dir_all(state_dir.join("blocks"))?;

    copy_required_bundle_file(bundle_dir, &cert_dir, "server-ca.pem")?;
    copy_required_bundle_file(bundle_dir, &cert_dir, "client.pem")?;
    copy_required_bundle_file(bundle_dir, &cert_dir, "client-key.pem")?;

    let config_path = state_dir.join("legatofs.toml");
    if config_path.exists() && !force {
        return Err(format!(
            "config already exists at {}; rerun with --force to overwrite",
            config_path.display()
        )
        .into());
    }

    fs::write(
        &config_path,
        render_client_config(state_dir, endpoint, server_name, mount_point, library_root),
    )?;
    Ok(())
}

fn copy_required_bundle_file(
    bundle_dir: &Path,
    cert_dir: &Path,
    file_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let source = bundle_dir.join(file_name);
    if !source.exists() {
        return Err(format!("bundle file is missing: {}", source.display()).into());
    }
    fs::copy(&source, cert_dir.join(file_name))?;
    Ok(())
}

fn render_client_config(
    state_dir: &Path,
    endpoint: &str,
    server_name: &str,
    mount_point: &str,
    library_root: &str,
) -> String {
    let state_dir_path = state_dir.to_path_buf();
    let state_dir = config_literal_path(&state_dir_path);
    let ca_cert_path = config_literal_path(&state_dir_path.join("certs").join("server-ca.pem"));
    let client_cert_path = config_literal_path(&state_dir_path.join("certs").join("client.pem"));
    let client_key_path = config_literal_path(&state_dir_path.join("certs").join("client-key.pem"));
    let mount_point = config_literal_string(mount_point);
    let library_root = config_literal_string(library_root);
    let server_name = config_literal_string(server_name);
    let endpoint = config_literal_string(endpoint);

    format!(
        "[common.tracing]\njson = false\nlevel = \"info\"\n\n\
         [common.metrics]\nprefix = \"legatofs\"\n\n\
         [client]\nendpoint = \"{endpoint}\"\n\n\
         [client.cache]\nmax_bytes = 1610612736000\nblock_size = 1048576\n\n\
         [client.tls]\nca_cert_path = \"{ca_cert_path}\"\n\
         client_cert_path = \"{client_cert_path}\"\n\
         client_key_path = \"{client_key_path}\"\n\
         server_name = \"{server_name}\"\n\n\
         [client.retry]\ninitial_delay_ms = 250\nmax_delay_ms = 5000\nmultiplier = 2\n\n\
         [mount]\nmount_point = \"{mount_point}\"\nlibrary_root = \"{library_root}\"\n\
         state_dir = \"{state_dir}\"\n"
    )
}

fn config_literal_path(path: &Path) -> String {
    config_literal_string(path.to_string_lossy().as_ref())
}

fn config_literal_string(value: &str) -> String {
    value.replace('\\', "\\\\")
}

fn default_client_name() -> String {
    env::var("LEGATO_CLIENT_NAME")
        .or_else(|_| env::var("HOSTNAME"))
        .or_else(|_| env::var("COMPUTERNAME"))
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| String::from("legatofs"))
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use super::{
        ClientProcessConfig, Command, MountConfig, default_client_name, default_config_path,
        default_mount_point, install_client_bundle, mount_root_attributes, parse_command_impl,
        startup_context,
    };
    use legato_types::{FilesystemOperation, FilesystemSemantics};
    use tempfile::tempdir;

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

    #[test]
    fn default_client_name_is_present() {
        assert!(!default_client_name().trim().is_empty());
    }

    #[test]
    fn parse_install_command() {
        let command = parse_command_impl([
            String::from("install"),
            String::from("--bundle-dir"),
            String::from("/tmp/bundle"),
            String::from("--endpoint"),
            String::from("legato.lan:7823"),
            String::from("--server-name"),
            String::from("legato.lan"),
            String::from("--mount-point"),
            String::from("/Volumes/Legato"),
            String::from("--state-dir"),
            String::from("/tmp/legato-state"),
            String::from("--library-root"),
            String::from("/srv/libraries"),
            String::from("--force"),
        ])
        .expect("command should parse");

        assert_eq!(
            command,
            Some(Command::Install {
                bundle_dir: PathBuf::from("/tmp/bundle"),
                endpoint: String::from("legato.lan:7823"),
                server_name: String::from("legato.lan"),
                mount_point: String::from("/Volumes/Legato"),
                state_dir: PathBuf::from("/tmp/legato-state"),
                library_root: String::from("/srv/libraries"),
                force: true,
            })
        );
    }

    #[test]
    fn parse_smoke_command() {
        let command = parse_command_impl([
            String::from("smoke"),
            String::from("--config"),
            String::from("/tmp/legato-state/legatofs.toml"),
            String::from("--path"),
            String::from("/srv/libraries/Kontakt/piano.nki"),
            String::from("--offset"),
            String::from("8"),
            String::from("--size"),
            String::from("16"),
        ])
        .expect("command should parse");

        assert_eq!(
            command,
            Some(Command::Smoke {
                config_path: Some(PathBuf::from("/tmp/legato-state/legatofs.toml")),
                path: String::from("/srv/libraries/Kontakt/piano.nki"),
                offset: 8,
                size: 16,
            })
        );
    }

    #[test]
    fn install_command_writes_config_and_cert_materials() {
        let fixture = tempdir().expect("tempdir should be created");
        let bundle_dir = fixture.path().join("bundle");
        let state_dir = fixture.path().join("state");
        fs::create_dir_all(&bundle_dir).expect("bundle dir should be created");
        fs::write(bundle_dir.join("server-ca.pem"), "ca").expect("server ca should be written");
        fs::write(bundle_dir.join("client.pem"), "client").expect("client cert should be written");
        fs::write(bundle_dir.join("client-key.pem"), "key").expect("client key should be written");

        install_client_bundle(
            &bundle_dir,
            &state_dir,
            "legato.lan:7823",
            "legato.lan",
            "/Volumes/Legato",
            "/srv/libraries",
            false,
        )
        .expect("install should succeed");

        let config =
            fs::read_to_string(state_dir.join("legatofs.toml")).expect("config should be readable");
        assert!(config.contains("endpoint = \"legato.lan:7823\""));
        assert!(state_dir.join("certs").join("server-ca.pem").exists());
        assert!(state_dir.join("certs").join("client.pem").exists());
        assert!(state_dir.join("certs").join("client-key.pem").exists());
        assert!(state_dir.join("blocks").exists());
    }
}
