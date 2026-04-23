//! Binary entrypoint for the native Legato filesystem client.

use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command as ProcessCommand,
    time::{SystemTime, UNIX_EPOCH},
};

use legato_client_cache::{ExtentCacheStore, open_cache_database};
use legato_client_core::{ClientConfig, FilesystemService, LocalControlPlane};
use legato_foundation::{
    CommonProcessConfig, ProcessTelemetry, ShutdownController, init_tracing, load_config,
};
use legato_proto::FileMetadata;
use legato_server::ClientBundleManifest;
use legato_types::{
    ClientPlatform, FileId, FilesystemAttributes, FilesystemError, FilesystemSemantics,
    platform_error_code,
};
use serde::Deserialize;
use tokio::{net::TcpStream, time::timeout};

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

    let runtime_config_path = runtime_config_path();
    let process_config =
        load_config::<ClientProcessConfig>(Some(&runtime_config_path), "LEGATO_FS")
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
        let _ = control;
        if !legato_fs_macos::mount_runtime_available() {
            return Err("macFUSE runtime not detected on this host".into());
        }
        telemetry.set_lifecycle_state("ready", 1);
        println!(
            "legatofs connected to {} and mounting on {} for {}",
            server_name,
            adapter.mount_point(),
            adapter.platform_name()
        );
        return legato_fs_macos::mount(
            service,
            Path::new(adapter.mount_point()),
            process_config.mount.library_root.clone(),
        )
        .await;
    }

    #[cfg(target_os = "windows")]
    {
        let adapter = legato_fs_windows::WindowsFilesystem::new(startup.mount_point.clone());
        let _ = control;
        if !legato_fs_windows::mount_runtime_available() {
            return Err("WinFSP runtime not detected on this host".into());
        }
        telemetry.set_lifecycle_state("ready", 1);
        println!(
            "legatofs connected to {} and mounting on {} for {}",
            server_name,
            adapter.mount_point(),
            adapter.platform_name()
        );
        return legato_fs_windows::mount(
            service,
            Path::new(adapter.mount_point()),
            process_config.mount.library_root.clone(),
        )
        .await;
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
    Cache {
        action: CacheCommand,
        config_path: Option<PathBuf>,
    },
    Doctor {
        config_path: Option<PathBuf>,
    },
    Service {
        action: ServiceCommand,
        config_path: Option<PathBuf>,
        force: bool,
    },
    Install {
        bundle_dir: PathBuf,
        endpoint: Option<String>,
        server_name: Option<String>,
        mount_point: Option<String>,
        state_dir: PathBuf,
        library_root: Option<String>,
        force: bool,
    },
    Smoke {
        config_path: Option<PathBuf>,
        path: String,
        offset: u64,
        size: u32,
    },
}

#[derive(Debug, Eq, PartialEq)]
enum CacheCommand {
    Status,
    Repair,
}

#[derive(Debug, Eq, PartialEq)]
enum ServiceCommand {
    Install,
    Uninstall,
    Start,
    Stop,
    Status,
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
        "cache" => {
            let action = arguments
                .next()
                .ok_or("missing cache action: expected status or repair")?;
            let action = match action.as_str() {
                "status" => CacheCommand::Status,
                "repair" => CacheCommand::Repair,
                other => return Err(format!("unsupported cache action: {other}").into()),
            };
            let mut config_path = None;
            while let Some(argument) = arguments.next() {
                match argument.as_str() {
                    "--config" => config_path = arguments.next().map(PathBuf::from),
                    other => return Err(format!("unsupported argument for cache: {other}").into()),
                }
            }
            Ok(Some(Command::Cache {
                action,
                config_path,
            }))
        }
        "doctor" => {
            let mut config_path = None;
            while let Some(argument) = arguments.next() {
                match argument.as_str() {
                    "--config" => config_path = arguments.next().map(PathBuf::from),
                    other => return Err(format!("unsupported argument for doctor: {other}").into()),
                }
            }
            Ok(Some(Command::Doctor { config_path }))
        }
        "service" => {
            let action = arguments.next().ok_or(
                "missing service action: expected install, uninstall, start, stop, or status",
            )?;
            let action = match action.as_str() {
                "install" => ServiceCommand::Install,
                "uninstall" => ServiceCommand::Uninstall,
                "start" => ServiceCommand::Start,
                "stop" => ServiceCommand::Stop,
                "status" => ServiceCommand::Status,
                other => return Err(format!("unsupported service action: {other}").into()),
            };
            let mut config_path = None;
            let mut force = false;
            while let Some(argument) = arguments.next() {
                match argument.as_str() {
                    "--config" => config_path = arguments.next().map(PathBuf::from),
                    "--force" => force = true,
                    other => {
                        return Err(format!("unsupported argument for service: {other}").into());
                    }
                }
            }
            Ok(Some(Command::Service {
                action,
                config_path,
                force,
            }))
        }
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
                endpoint,
                server_name,
                mount_point,
                state_dir: state_dir.unwrap_or_else(|| PathBuf::from(default_state_dir())),
                library_root,
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
        Command::Cache {
            action,
            config_path,
        } => {
            let process_config = load_config::<ClientProcessConfig>(
                config_path.as_deref().or(Some(default_config_path())),
                "LEGATO_FS",
            )?;
            let report = match action {
                CacheCommand::Status => cache_status_report(&process_config.mount)?,
                CacheCommand::Repair => cache_repair_report(
                    &process_config.mount,
                    process_config.client.cache.max_bytes,
                )?,
            };
            println!("{report}");
            Ok(())
        }
        Command::Doctor { config_path } => {
            let process_config = load_config::<ClientProcessConfig>(
                config_path.as_deref().or(Some(default_config_path())),
                "LEGATO_FS",
            )?;
            let report = client_doctor_report(&process_config).await?;
            println!("{report}");
            Ok(())
        }
        Command::Service {
            action,
            config_path,
            force,
        } => run_service_command(action, config_path, force),
        Command::Install {
            bundle_dir,
            endpoint,
            server_name,
            mount_point,
            state_dir,
            library_root,
            force,
        } => {
            let manifest = load_bundle_manifest(&bundle_dir)?;
            let endpoint = resolve_required_install_value(
                endpoint,
                manifest.as_ref().and_then(|bundle| bundle.endpoint.clone()),
                "--endpoint",
            )?;
            let server_name = resolve_required_install_value(
                server_name,
                manifest
                    .as_ref()
                    .and_then(|bundle| bundle.server_name.clone()),
                "--server-name",
            )?;
            let mount_point = resolve_optional_install_value(
                mount_point,
                manifest
                    .as_ref()
                    .and_then(|bundle| bundle.mount_point.clone()),
                default_mount_point,
            );
            let library_root = resolve_optional_install_value(
                library_root,
                manifest
                    .as_ref()
                    .and_then(|bundle| bundle.library_root.clone()),
                default_library_root,
            );
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

async fn client_doctor_report(
    process_config: &ClientProcessConfig,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut lines = vec![String::from("legatofs doctor")];

    require_readable_file(
        "server CA",
        Path::new(&process_config.client.tls.ca_cert_path),
    )?;
    require_readable_file(
        "client certificate",
        Path::new(&process_config.client.tls.client_cert_path),
    )?;
    require_readable_file(
        "client private key",
        Path::new(&process_config.client.tls.client_key_path),
    )?;
    lines.push(String::from("ok certs"));

    require_writable_directory("state_dir", Path::new(&process_config.mount.state_dir))?;
    require_writable_directory(
        "extent_dir",
        &Path::new(&process_config.mount.state_dir).join("extents"),
    )?;
    lines.push(format!("ok state_dir {}", process_config.mount.state_dir));

    check_mount_prerequisite()?;
    lines.push(String::from("ok mount_runtime"));

    check_endpoint_reachable(&process_config.client.endpoint).await?;
    lines.push(format!("ok endpoint {}", process_config.client.endpoint));

    Ok(lines.join("\n"))
}

fn cache_status_report(mount: &MountConfig) -> Result<String, Box<dyn std::error::Error>> {
    let database = open_cache_database(&Path::new(&mount.state_dir).join("client.sqlite"))?;
    let store = ExtentCacheStore::new(&Path::new(&mount.state_dir).join("extents"), database)?;
    let extent_count: i64 =
        store
            .connection()
            .query_row("SELECT COUNT(*) FROM extent_entries", [], |row| row.get(0))?;
    let bytes = store.total_size_bytes()?;
    let dirty = store.is_dirty()?;
    let checkpoint = store.load_checkpoint()?;
    let checkpoint_text = checkpoint.map_or_else(
        || String::from("none"),
        |checkpoint| {
            format!(
                "version={} updated_at_ns={} extent_entries={} total_bytes={}",
                checkpoint.version,
                checkpoint.updated_at_ns,
                checkpoint.extent_entries,
                checkpoint.total_bytes
            )
        },
    );

    Ok(format!(
        "legatofs cache status\nstate_dir {}\nextents {}\nbytes {}\ndirty {}\ncheckpoint {}",
        mount.state_dir, extent_count, bytes, dirty, checkpoint_text
    ))
}

fn cache_repair_report(
    mount: &MountConfig,
    max_cache_bytes: u64,
) -> Result<String, Box<dyn std::error::Error>> {
    let database = open_cache_database(&Path::new(&mount.state_dir).join("client.sqlite"))?;
    let mut store = ExtentCacheStore::new(&Path::new(&mount.state_dir).join("extents"), database)?;
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos() as u64);
    let repair = store.repair()?;
    let compaction = store.compact()?;
    let eviction = store.evict_to_limit(max_cache_bytes)?;
    let checkpoint = store.checkpoint(now_ns)?;

    Ok(format!(
        "legatofs cache repair\nrepaired_entries {}\nentries_removed {}\norphan_files_removed {}\nreclaimed_bytes {}\nstale_fetch_rows_removed {}\nstale_pins_removed {}\nempty_directories_removed {}\nevicted_entries {}\nbytes_after {}\ncheckpoint_updated_at_ns {}",
        repair.repaired_entries,
        repair.entries_removed,
        repair.orphan_files_removed,
        repair.reclaimed_bytes,
        compaction.stale_fetch_rows_removed,
        compaction.stale_pins_removed,
        compaction.empty_directories_removed,
        eviction.entries_removed,
        checkpoint.total_bytes,
        checkpoint.updated_at_ns
    ))
}

fn run_service_command(
    action: ServiceCommand,
    config_path: Option<PathBuf>,
    force: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let config_path = config_path.unwrap_or_else(runtime_config_path);
    match action {
        ServiceCommand::Install => install_mount_agent_service(&config_path, force),
        ServiceCommand::Uninstall => uninstall_mount_agent_service(),
        ServiceCommand::Start => start_mount_agent_service(),
        ServiceCommand::Stop => stop_mount_agent_service(),
        ServiceCommand::Status => status_mount_agent_service(),
    }
}

#[cfg(target_os = "macos")]
fn install_mount_agent_service(
    config_path: &Path,
    force: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    require_readable_file("legatofs config", config_path)?;
    let definition = macos_service_definition(config_path)?;
    fs::create_dir_all(&definition.log_dir)?;
    if definition.plist_path.exists() && !force {
        return Err(format!(
            "launchd plist already exists at {}; rerun with --force to overwrite",
            definition.plist_path.display()
        )
        .into());
    }
    fs::create_dir_all(
        definition
            .plist_path
            .parent()
            .ok_or("plist path has no parent")?,
    )?;
    fs::write(&definition.plist_path, definition.plist)?;
    println!(
        "installed launchd agent {} at {}\nlogs: {}",
        LEGATO_SERVICE_LABEL,
        definition.plist_path.display(),
        definition.log_dir.display()
    );
    Ok(())
}

#[cfg(target_os = "windows")]
fn install_mount_agent_service(
    config_path: &Path,
    force: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    require_readable_file("legatofs config", config_path)?;
    let task = windows_task_command(config_path)?;
    fs::create_dir_all(windows_log_dir())?;
    let mut command = ProcessCommand::new("schtasks");
    command.args([
        "/Create",
        "/TN",
        LEGATO_WINDOWS_TASK_NAME,
        "/SC",
        "ONLOGON",
        "/TR",
        &task,
    ]);
    if force {
        command.arg("/F");
    }
    run_process(command, "create scheduled task")?;
    println!(
        "installed scheduled task {}\nlogs: {}",
        LEGATO_WINDOWS_TASK_NAME,
        windows_log_dir().display()
    );
    Ok(())
}

#[cfg(not(any(target_os = "macos", target_os = "windows")))]
fn install_mount_agent_service(
    _config_path: &Path,
    _force: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    Err("legatofs service install is only supported on macOS and Windows".into())
}

#[cfg(target_os = "macos")]
fn uninstall_mount_agent_service() -> Result<(), Box<dyn std::error::Error>> {
    let definition = macos_service_definition(&runtime_config_path())?;
    let _ = stop_mount_agent_service();
    if definition.plist_path.exists() {
        fs::remove_file(&definition.plist_path)?;
    }
    println!("removed launchd agent {}", LEGATO_SERVICE_LABEL);
    Ok(())
}

#[cfg(target_os = "windows")]
fn uninstall_mount_agent_service() -> Result<(), Box<dyn std::error::Error>> {
    run_process(
        process_with_args(
            "schtasks",
            ["/Delete", "/TN", LEGATO_WINDOWS_TASK_NAME, "/F"],
        ),
        "delete scheduled task",
    )?;
    println!("removed scheduled task {}", LEGATO_WINDOWS_TASK_NAME);
    Ok(())
}

#[cfg(not(any(target_os = "macos", target_os = "windows")))]
fn uninstall_mount_agent_service() -> Result<(), Box<dyn std::error::Error>> {
    Err("legatofs service uninstall is only supported on macOS and Windows".into())
}

#[cfg(target_os = "macos")]
fn start_mount_agent_service() -> Result<(), Box<dyn std::error::Error>> {
    let definition = macos_service_definition(&runtime_config_path())?;
    run_process(
        process_with_args(
            "launchctl",
            [
                "bootstrap",
                &macos_launch_domain()?,
                definition.plist_path.to_string_lossy().as_ref(),
            ],
        ),
        "bootstrap launchd agent",
    )
    .or_else(|_| {
        run_process(
            process_with_args(
                "launchctl",
                [
                    "kickstart",
                    "-k",
                    &format!("{}/{}", macos_launch_domain()?, LEGATO_SERVICE_LABEL),
                ],
            ),
            "kickstart launchd agent",
        )
    })?;
    println!("started launchd agent {}", LEGATO_SERVICE_LABEL);
    Ok(())
}

#[cfg(target_os = "windows")]
fn start_mount_agent_service() -> Result<(), Box<dyn std::error::Error>> {
    run_process(
        process_with_args("schtasks", ["/Run", "/TN", LEGATO_WINDOWS_TASK_NAME]),
        "start scheduled task",
    )?;
    println!("started scheduled task {}", LEGATO_WINDOWS_TASK_NAME);
    Ok(())
}

#[cfg(not(any(target_os = "macos", target_os = "windows")))]
fn start_mount_agent_service() -> Result<(), Box<dyn std::error::Error>> {
    Err("legatofs service start is only supported on macOS and Windows".into())
}

#[cfg(target_os = "macos")]
fn stop_mount_agent_service() -> Result<(), Box<dyn std::error::Error>> {
    run_process(
        process_with_args(
            "launchctl",
            [
                "bootout",
                &format!("{}/{}", macos_launch_domain()?, LEGATO_SERVICE_LABEL),
            ],
        ),
        "stop launchd agent",
    )?;
    println!("stopped launchd agent {}", LEGATO_SERVICE_LABEL);
    Ok(())
}

#[cfg(target_os = "windows")]
fn stop_mount_agent_service() -> Result<(), Box<dyn std::error::Error>> {
    run_process(
        process_with_args("schtasks", ["/End", "/TN", LEGATO_WINDOWS_TASK_NAME]),
        "stop scheduled task",
    )?;
    println!("stopped scheduled task {}", LEGATO_WINDOWS_TASK_NAME);
    Ok(())
}

#[cfg(not(any(target_os = "macos", target_os = "windows")))]
fn stop_mount_agent_service() -> Result<(), Box<dyn std::error::Error>> {
    Err("legatofs service stop is only supported on macOS and Windows".into())
}

#[cfg(target_os = "macos")]
fn status_mount_agent_service() -> Result<(), Box<dyn std::error::Error>> {
    let definition = macos_service_definition(&runtime_config_path())?;
    println!(
        "legatofs service\nlabel {}\nplist {}\nlogs {}",
        LEGATO_SERVICE_LABEL,
        definition.plist_path.display(),
        definition.log_dir.display()
    );
    let _ = ProcessCommand::new("launchctl")
        .args([
            "print",
            &format!("{}/{}", macos_launch_domain()?, LEGATO_SERVICE_LABEL),
        ])
        .status();
    Ok(())
}

#[cfg(target_os = "windows")]
fn status_mount_agent_service() -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "legatofs service\ntask {}\nlogs {}",
        LEGATO_WINDOWS_TASK_NAME,
        windows_log_dir().display()
    );
    let _ = ProcessCommand::new("schtasks")
        .args([
            "/Query",
            "/TN",
            LEGATO_WINDOWS_TASK_NAME,
            "/V",
            "/FO",
            "LIST",
        ])
        .status();
    Ok(())
}

#[cfg(not(any(target_os = "macos", target_os = "windows")))]
fn status_mount_agent_service() -> Result<(), Box<dyn std::error::Error>> {
    println!("legatofs service is only supported on macOS and Windows");
    Ok(())
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
    let _store = ExtentCacheStore::new(&Path::new(&mount.state_dir).join("extents"), database)?;
    let mut control = LocalControlPlane::new(legato_client_cache::MetadataCache::new(
        legato_client_cache::MetadataCachePolicy::default(),
    ));
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
        block_size: 4096,
        read_only: semantics.read_only,
    };

    FileMetadata {
        file_id: attributes.file_id.0,
        path: attributes.path.to_string_lossy().into_owned(),
        size: attributes.size,
        mtime_ns: attributes.mtime_ns,
        content_hash: Vec::new(),
        is_dir: attributes.is_dir,
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

fn load_bundle_manifest(
    bundle_dir: &Path,
) -> Result<Option<ClientBundleManifest>, Box<dyn std::error::Error>> {
    let manifest_path = bundle_dir.join("bundle.json");
    if !manifest_path.exists() {
        return Ok(None);
    }
    Ok(Some(serde_json::from_slice(&fs::read(&manifest_path)?)?))
}

fn resolve_required_install_value(
    command_value: Option<String>,
    manifest_value: Option<String>,
    flag_name: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    command_value.or(manifest_value).ok_or_else(|| {
        format!("missing {flag_name} for install and no bundle manifest value was provided").into()
    })
}

fn resolve_optional_install_value(
    command_value: Option<String>,
    manifest_value: Option<String>,
    default: impl FnOnce() -> String,
) -> String {
    command_value.or(manifest_value).unwrap_or_else(default)
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

#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
const LEGATO_SERVICE_LABEL: &str = "com.legato.legatofs";
#[cfg_attr(not(target_os = "windows"), allow(dead_code))]
const LEGATO_WINDOWS_TASK_NAME: &str = "LegatoFS";

#[cfg(target_os = "macos")]
#[derive(Debug, Eq, PartialEq)]
struct MacosServiceDefinition {
    plist_path: PathBuf,
    log_dir: PathBuf,
    plist: String,
}

fn runtime_config_path() -> PathBuf {
    env::var("LEGATO_FS_CONFIG")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| default_config_path().to_path_buf())
}

#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn render_macos_launchd_plist(
    executable: &Path,
    config_path: &Path,
    stdout_log: &Path,
    stderr_log: &Path,
) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n\
         <plist version=\"1.0\">\n\
         <dict>\n\
           <key>Label</key><string>{}</string>\n\
           <key>ProgramArguments</key>\n\
           <array><string>{}</string></array>\n\
           <key>EnvironmentVariables</key>\n\
           <dict><key>LEGATO_FS_CONFIG</key><string>{}</string></dict>\n\
           <key>RunAtLoad</key><true/>\n\
           <key>KeepAlive</key><true/>\n\
           <key>StandardOutPath</key><string>{}</string>\n\
           <key>StandardErrorPath</key><string>{}</string>\n\
         </dict>\n\
         </plist>\n",
        LEGATO_SERVICE_LABEL,
        xml_escape(executable.to_string_lossy().as_ref()),
        xml_escape(config_path.to_string_lossy().as_ref()),
        xml_escape(stdout_log.to_string_lossy().as_ref()),
        xml_escape(stderr_log.to_string_lossy().as_ref())
    )
}

#[cfg(target_os = "macos")]
fn macos_service_definition(
    config_path: &Path,
) -> Result<MacosServiceDefinition, Box<dyn std::error::Error>> {
    let home = env::var("HOME").map_err(|_| "HOME is required to install a launchd agent")?;
    let launch_agents = PathBuf::from(&home).join("Library").join("LaunchAgents");
    let log_dir = PathBuf::from(&home)
        .join("Library")
        .join("Logs")
        .join("Legato");
    let stdout_log = log_dir.join("legatofs.out.log");
    let stderr_log = log_dir.join("legatofs.err.log");
    let executable = env::current_exe()?;
    Ok(MacosServiceDefinition {
        plist_path: launch_agents.join(format!("{LEGATO_SERVICE_LABEL}.plist")),
        log_dir,
        plist: render_macos_launchd_plist(&executable, config_path, &stdout_log, &stderr_log),
    })
}

#[cfg(target_os = "windows")]
fn windows_task_command(config_path: &Path) -> Result<String, Box<dyn std::error::Error>> {
    let executable = env::current_exe()?;
    Ok(windows_task_command_for_executable(
        &executable,
        config_path,
        &windows_log_dir(),
    ))
}

#[cfg_attr(not(target_os = "windows"), allow(dead_code))]
fn windows_task_command_for_executable(
    executable: &Path,
    config_path: &Path,
    log_dir: &Path,
) -> String {
    let stdout_log = log_dir.join("legatofs.out.log");
    let stderr_log = log_dir.join("legatofs.err.log");
    format!(
        "cmd.exe /C \"set LEGATO_FS_CONFIG={}&& \"{}\" >> \"{}\" 2>> \"{}\"\"",
        config_path.display(),
        executable.display(),
        stdout_log.display(),
        stderr_log.display()
    )
}

#[cfg_attr(not(target_os = "windows"), allow(dead_code))]
fn windows_log_dir() -> PathBuf {
    env::var("ProgramData")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("C:\\ProgramData"))
        .join("Legato")
        .join("logs")
}

#[cfg(target_os = "macos")]
fn macos_launch_domain() -> Result<String, Box<dyn std::error::Error>> {
    let output = ProcessCommand::new("id").arg("-u").output()?;
    if !output.status.success() {
        return Err("failed to determine current uid for launchctl domain".into());
    }
    let uid = String::from_utf8(output.stdout)?.trim().to_owned();
    Ok(format!("gui/{uid}"))
}

#[cfg_attr(not(any(target_os = "macos", target_os = "windows")), allow(dead_code))]
fn process_with_args<const N: usize>(program: &str, args: [&str; N]) -> ProcessCommand {
    let mut command = ProcessCommand::new(program);
    command.args(args);
    command
}

#[cfg_attr(not(any(target_os = "macos", target_os = "windows")), allow(dead_code))]
fn run_process(
    mut command: ProcessCommand,
    action: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let status = command.status()?;
    if !status.success() {
        return Err(format!("{action} failed with status {status}").into());
    }
    Ok(())
}

#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
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
    fs::create_dir_all(state_dir.join("extents"))?;

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

fn require_readable_file(label: &str, path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    if !path.is_file() {
        return Err(format!("{label} is not a readable file: {}", path.display()).into());
    }
    let _ = fs::File::open(path).map_err(|error| {
        format!(
            "{label} is not a readable file at {}: {error}",
            path.display()
        )
    })?;
    Ok(())
}

fn require_writable_directory(label: &str, path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(path)?;
    let probe = path.join(".legato-doctor-write");
    fs::write(&probe, b"ok")
        .map_err(|error| format!("{label} is not writable at {}: {error}", path.display()))?;
    let _ = fs::remove_file(probe);
    Ok(())
}

fn check_mount_prerequisite() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(target_os = "macos")]
    {
        if !legato_fs_macos::mount_runtime_available() {
            return Err("macFUSE runtime not detected".into());
        }
    }
    #[cfg(target_os = "windows")]
    {
        if !legato_fs_windows::mount_runtime_available() {
            return Err("WinFSP runtime not detected".into());
        }
    }
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        let _ = platform_error_code(ClientPlatform::Macos, FilesystemError::ReadOnly);
    }
    Ok(())
}

async fn check_endpoint_reachable(endpoint: &str) -> Result<(), Box<dyn std::error::Error>> {
    let socket = endpoint_socket(endpoint)?;
    timeout(
        std::time::Duration::from_secs(3),
        TcpStream::connect(&socket),
    )
    .await
    .map_err(|_| format!("endpoint timed out: {socket}"))?
    .map_err(|error| format!("endpoint is not reachable at {socket}: {error}"))?;
    Ok(())
}

fn endpoint_socket(endpoint: &str) -> Result<String, Box<dyn std::error::Error>> {
    let endpoint = endpoint
        .strip_prefix("https://")
        .or_else(|| endpoint.strip_prefix("http://"))
        .unwrap_or(endpoint);
    let endpoint = endpoint.trim_end_matches('/');
    if endpoint.is_empty() {
        return Err("client endpoint is empty".into());
    }
    if endpoint.rsplit_once(':').is_none() {
        return Err(format!("client endpoint must include host:port: {endpoint}").into());
    }
    Ok(endpoint.to_owned())
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
         [client.cache]\nmax_bytes = 1610612736000\n\n\
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
        CacheCommand, ClientProcessConfig, Command, MountConfig, cache_repair_report,
        cache_status_report, default_client_name, default_config_path, default_library_root,
        default_mount_point, default_state_dir, endpoint_socket, install_client_bundle,
        load_bundle_manifest, mount_root_attributes, open_cache_database, parse_command_impl,
        render_macos_launchd_plist, resolve_optional_install_value, resolve_required_install_value,
        startup_context, windows_task_command_for_executable,
    };
    use legato_client_cache::ExtentCacheStore;
    use legato_proto::{ExtentRecord, TransferClass};
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
                endpoint: Some(String::from("legato.lan:7823")),
                server_name: Some(String::from("legato.lan")),
                mount_point: Some(String::from("/Volumes/Legato")),
                state_dir: PathBuf::from("/tmp/legato-state"),
                library_root: Some(String::from("/srv/libraries")),
                force: true,
            })
        );
    }

    #[test]
    fn parse_install_command_allows_manifest_backed_defaults() {
        let command = parse_command_impl([
            String::from("install"),
            String::from("--bundle-dir"),
            String::from("/tmp/bundle"),
        ])
        .expect("command should parse");

        assert_eq!(
            command,
            Some(Command::Install {
                bundle_dir: PathBuf::from("/tmp/bundle"),
                endpoint: None,
                server_name: None,
                mount_point: None,
                state_dir: PathBuf::from(default_state_dir()),
                library_root: None,
                force: false,
            })
        );
    }

    #[test]
    fn parse_doctor_command() {
        let command = parse_command_impl([
            String::from("doctor"),
            String::from("--config"),
            String::from("/tmp/legatofs.toml"),
        ])
        .expect("command should parse");

        assert_eq!(
            command,
            Some(Command::Doctor {
                config_path: Some(PathBuf::from("/tmp/legatofs.toml")),
            })
        );
    }

    #[test]
    fn parse_cache_commands() {
        let status = parse_command_impl([
            String::from("cache"),
            String::from("status"),
            String::from("--config"),
            String::from("/tmp/legatofs.toml"),
        ])
        .expect("status command should parse");
        let repair = parse_command_impl([String::from("cache"), String::from("repair")])
            .expect("repair command should parse");

        assert_eq!(
            status,
            Some(Command::Cache {
                action: CacheCommand::Status,
                config_path: Some(PathBuf::from("/tmp/legatofs.toml")),
            })
        );
        assert_eq!(
            repair,
            Some(Command::Cache {
                action: CacheCommand::Repair,
                config_path: None,
            })
        );
    }

    #[test]
    fn parse_service_commands() {
        let command = parse_command_impl([
            String::from("service"),
            String::from("install"),
            String::from("--config"),
            String::from("/tmp/legatofs.toml"),
            String::from("--force"),
        ])
        .expect("service command should parse");

        assert_eq!(
            command,
            Some(Command::Service {
                action: super::ServiceCommand::Install,
                config_path: Some(PathBuf::from("/tmp/legatofs.toml")),
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
        assert!(state_dir.join("extents").exists());
    }

    #[test]
    fn install_command_uses_bundle_manifest_defaults() {
        let fixture = tempdir().expect("tempdir should be created");
        let bundle_dir = fixture.path().join("bundle");
        let state_dir = fixture.path().join("state");
        fs::create_dir_all(&bundle_dir).expect("bundle dir should be created");
        fs::write(bundle_dir.join("server-ca.pem"), "ca").expect("server ca should be written");
        fs::write(bundle_dir.join("client.pem"), "client").expect("client cert should be written");
        fs::write(bundle_dir.join("client-key.pem"), "key").expect("client key should be written");
        fs::write(
            bundle_dir.join("bundle.json"),
            r#"{
  "client_name":"studio-mac",
  "endpoint":"legato.lan:7823",
  "server_name":"legato.lan",
  "mount_point":"/Volumes/Legato",
  "library_root":"/srv/libraries",
  "issued_at_unix_ms":1
}"#,
        )
        .expect("bundle manifest should be written");

        let manifest = load_bundle_manifest(&bundle_dir)
            .expect("bundle manifest should load")
            .expect("bundle manifest should exist");
        install_client_bundle(
            &bundle_dir,
            &state_dir,
            &resolve_required_install_value(None, manifest.endpoint.clone(), "--endpoint")
                .expect("endpoint should resolve"),
            &resolve_required_install_value(None, manifest.server_name.clone(), "--server-name")
                .expect("server name should resolve"),
            &resolve_optional_install_value(
                None,
                manifest.mount_point.clone(),
                default_mount_point,
            ),
            &resolve_optional_install_value(
                None,
                manifest.library_root.clone(),
                default_library_root,
            ),
            false,
        )
        .expect("install should succeed");

        let config =
            fs::read_to_string(state_dir.join("legatofs.toml")).expect("config should be readable");
        assert!(config.contains("endpoint = \"legato.lan:7823\""));
        assert!(config.contains("server_name = \"legato.lan\""));
        assert!(config.contains("mount_point = \"/Volumes/Legato\""));
    }

    #[test]
    fn endpoint_socket_accepts_plain_and_https_endpoints() {
        assert_eq!(
            endpoint_socket("legato.lan:7823").expect("endpoint should parse"),
            "legato.lan:7823"
        );
        assert_eq!(
            endpoint_socket("https://legato.lan:7823").expect("endpoint should parse"),
            "legato.lan:7823"
        );
        assert!(endpoint_socket("legato.lan").is_err());
    }

    #[test]
    fn launchd_plist_runs_legatofs_with_config_and_logs() {
        let plist = render_macos_launchd_plist(
            &PathBuf::from("/Applications/Legato/legatofs"),
            &PathBuf::from("/Library/Application Support/Legato/legatofs.toml"),
            &PathBuf::from("/Users/me/Library/Logs/Legato/legatofs.out.log"),
            &PathBuf::from("/Users/me/Library/Logs/Legato/legatofs.err.log"),
        );

        assert!(plist.contains("com.legato.legatofs"));
        assert!(plist.contains("LEGATO_FS_CONFIG"));
        assert!(plist.contains("KeepAlive"));
        assert!(plist.contains("legatofs.err.log"));
    }

    #[test]
    fn windows_task_command_runs_legatofs_with_config_and_logs() {
        let command = windows_task_command_for_executable(
            &PathBuf::from("C:\\Program Files\\Legato\\legatofs.exe"),
            &PathBuf::from("C:\\ProgramData\\Legato\\legatofs.toml"),
            &PathBuf::from("C:\\ProgramData\\Legato\\logs"),
        );

        assert!(command.contains("LEGATO_FS_CONFIG=C:\\ProgramData\\Legato\\legatofs.toml"));
        assert!(command.contains("legatofs.exe"));
        assert!(command.contains("legatofs.out.log"));
        assert!(command.contains("legatofs.err.log"));
    }

    #[test]
    fn cache_status_and_repair_report_extent_store_state() {
        let fixture = tempdir().expect("tempdir should be created");
        let mount = MountConfig {
            mount_point: String::from("/tmp/legato-mount"),
            library_root: String::from("/srv/libraries"),
            state_dir: fixture.path().join("state").to_string_lossy().into_owned(),
        };
        let database = open_cache_database(&PathBuf::from(&mount.state_dir).join("client.sqlite"))
            .expect("cache database should open");
        let mut store =
            ExtentCacheStore::new(&PathBuf::from(&mount.state_dir).join("extents"), database)
                .expect("extent store should open");
        store
            .put_extent(
                &ExtentRecord {
                    file_id: 7,
                    extent_index: 0,
                    file_offset: 0,
                    data: b"fixture".to_vec(),
                    extent_hash: Vec::new(),
                    transfer_class: TransferClass::Unitary as i32,
                },
                1,
                100,
            )
            .expect("extent should be stored");

        let status = cache_status_report(&mount).expect("status should render");
        let repair = cache_repair_report(&mount, 1024).expect("repair should render");

        assert!(status.contains("extents 1"));
        assert!(status.contains("dirty true"));
        assert!(repair.contains("checkpoint_updated_at_ns"));
        assert!(
            cache_status_report(&mount)
                .expect("status should render")
                .contains("dirty false")
        );
    }
}
