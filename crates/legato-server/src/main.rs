//! Binary entrypoint for the Legato server daemon.

use std::{
    env,
    path::{Path, PathBuf},
};

use legato_client_cache::catalog::CatalogStore;
use legato_foundation::{
    CommonProcessConfig, ProcessTelemetry, ShutdownController, init_tracing, load_config,
};
use legato_server::{
    ClientBundleManifest, LiveServer, ServerConfig, ServerRuntimeMetrics, build_tls_server_config,
    ensure_server_tls_materials, issue_client_tls_bundle, load_runtime_tls, parse_bind_address,
    write_client_bundle_manifest,
};
use serde::Deserialize;

#[derive(Debug, Default, Deserialize)]
struct ServerProcessConfig {
    #[serde(default)]
    common: CommonProcessConfig,
    #[serde(default)]
    server: ServerConfig,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let command = parse_command()?;
    let config_path = command
        .as_ref()
        .and_then(Command::config_path)
        .map(Path::to_path_buf)
        .unwrap_or_else(default_config_path);
    let process_config = load_config::<ServerProcessConfig>(Some(&config_path), "LEGATO_SERVER")
        .unwrap_or_else(|_| ServerProcessConfig::default());

    if let Some(command) = command {
        return run_command(command, &process_config);
    }

    init_tracing("legato-server", &process_config.common.tracing)?;
    let shutdown = ShutdownController::new();
    let telemetry = ProcessTelemetry::new("legato-server", &process_config.common.metrics);
    telemetry.record_startup();
    telemetry.set_lifecycle_state("bootstrap", 1);
    let _metrics_exporter = telemetry.spawn_exporter(shutdown.token())?;
    let server_metrics = ServerRuntimeMetrics::new(telemetry.clone());
    let _client_metrics_cleanup = server_metrics.spawn_client_metrics_cleanup(shutdown.token());
    ensure_server_tls_materials(
        Path::new(&process_config.server.tls_dir),
        &process_config.server.tls,
    )?;
    build_tls_server_config(&process_config.server.tls)?;
    let runtime_tls = load_runtime_tls(&process_config.server.tls)?;
    let bind_address = parse_bind_address(&process_config.server.bind_address)?;
    let listener = tokio::net::TcpListener::bind(bind_address).await?;

    let server = LiveServer::bootstrap_with_metrics(process_config.server, Some(server_metrics))?;
    let bound = server.bind(listener, Some(runtime_tls)).await?;
    telemetry.set_lifecycle_state("ready", 1);
    println!("legato-server bootstrap ready");
    tokio::signal::ctrl_c().await?;
    bound.shutdown().await
}

#[derive(Debug, Eq, PartialEq)]
enum Command {
    Doctor {
        config_path: Option<PathBuf>,
    },
    IssueClient {
        name: String,
        output_dir: PathBuf,
        endpoint: Option<String>,
        server_name: Option<String>,
        mount_point: Option<String>,
        library_root: Option<String>,
    },
}

impl Command {
    fn config_path(&self) -> Option<&Path> {
        match self {
            Self::Doctor { config_path } => config_path.as_deref(),
            Self::IssueClient { .. } => None,
        }
    }
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
        "issue-client" => {
            let mut name = None;
            let mut output_dir = None;
            let mut endpoint = None;
            let mut server_name = None;
            let mut mount_point = None;
            let mut library_root = None;

            while let Some(argument) = arguments.next() {
                match argument.as_str() {
                    "--name" => name = arguments.next(),
                    "--output-dir" => output_dir = arguments.next().map(PathBuf::from),
                    "--endpoint" => endpoint = arguments.next(),
                    "--server-name" => server_name = arguments.next(),
                    "--mount-point" => mount_point = arguments.next(),
                    "--library-root" => library_root = arguments.next(),
                    other => {
                        return Err(
                            format!("unsupported argument for issue-client: {other}").into()
                        );
                    }
                }
            }

            let name = name.ok_or("missing --name for issue-client")?;
            let output_dir = output_dir.ok_or("missing --output-dir for issue-client")?;
            Ok(Some(Command::IssueClient {
                name,
                output_dir,
                endpoint,
                server_name,
                mount_point,
                library_root,
            }))
        }
        other => Err(format!("unsupported legato-server command: {other}").into()),
    }
}

fn run_command(
    command: Command,
    process_config: &ServerProcessConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        Command::Doctor { .. } => {
            let report = server_doctor_report(&process_config.server)?;
            println!("{report}");
            Ok(())
        }
        Command::IssueClient {
            name,
            output_dir,
            endpoint,
            server_name,
            mount_point,
            library_root,
        } => {
            issue_client_tls_bundle(
                Path::new(&process_config.server.tls_dir),
                &process_config.server.tls,
                &name,
                &output_dir,
            )?;
            let manifest = ClientBundleManifest::for_issue(
                &name,
                endpoint.or_else(|| Some(process_config.server.bind_address.clone())),
                server_name.or_else(|| process_config.server.tls.server_names.first().cloned()),
                mount_point,
                library_root,
            );
            write_client_bundle_manifest(&output_dir, &manifest)?;
            println!(
                "issued client bundle for {name} into {}",
                output_dir.display()
            );
            Ok(())
        }
    }
}

fn default_config_path() -> PathBuf {
    PathBuf::from("/etc/legato/server.toml")
}

fn server_doctor_report(config: &ServerConfig) -> Result<String, Box<dyn std::error::Error>> {
    let mut lines = vec![String::from("legato-server doctor")];

    require_directory("library_root", Path::new(&config.library_root))?;
    lines.push(format!("ok library_root {}", config.library_root));

    require_writable_directory("state_dir", Path::new(&config.state_dir))?;
    lines.push(format!("ok state_dir {}", config.state_dir));

    let bind_address = parse_bind_address(&config.bind_address)?;
    lines.push(format!("ok bind_address {bind_address}"));

    ensure_server_tls_materials(Path::new(&config.tls_dir), &config.tls)?;
    build_tls_server_config(&config.tls)?;
    lines.push(format!("ok tls_dir {}", config.tls_dir));

    let catalog = CatalogStore::open(&config.state_dir, 0)?;
    lines.push(format!(
        "ok canonical_store {} sequence={}",
        config.state_dir,
        catalog.last_sequence()
    ));

    Ok(lines.join("\n"))
}

fn require_directory(label: &str, path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    if !path.is_dir() {
        return Err(format!("{label} is not a directory: {}", path.display()).into());
    }
    Ok(())
}

fn require_writable_directory(label: &str, path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(path)?;
    let probe = path.join(".legato-doctor-write");
    std::fs::write(&probe, b"ok")
        .map_err(|error| format!("{label} is not writable at {}: {error}", path.display()))?;
    let _ = std::fs::remove_file(probe);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{Command, parse_command_impl, server_doctor_report};
    use legato_server::{ServerConfig, ServerTlsConfig};
    use tempfile::tempdir;

    fn parse_command_from<I, S>(arguments: I) -> Result<Option<Command>, Box<dyn std::error::Error>>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        parse_command_impl(
            arguments
                .into_iter()
                .map(Into::into)
                .collect::<Vec<String>>(),
        )
    }

    #[test]
    fn parse_issue_client_command() {
        let command = parse_command_from([
            "issue-client",
            "--name",
            "studio-mac",
            "--output-dir",
            "/tmp/studio-mac",
            "--endpoint",
            "legato.lan:7823",
            "--server-name",
            "legato.lan",
            "--mount-point",
            "/Volumes/Legato",
            "--library-root",
            "/srv/libraries",
        ])
        .expect("command should parse");

        assert_eq!(
            command,
            Some(Command::IssueClient {
                name: String::from("studio-mac"),
                output_dir: PathBuf::from("/tmp/studio-mac"),
                endpoint: Some(String::from("legato.lan:7823")),
                server_name: Some(String::from("legato.lan")),
                mount_point: Some(String::from("/Volumes/Legato")),
                library_root: Some(String::from("/srv/libraries")),
            })
        );
    }

    #[test]
    fn parse_doctor_command() {
        let command = parse_command_from(["doctor", "--config", "/tmp/server.toml"])
            .expect("command should parse");

        assert_eq!(
            command,
            Some(Command::Doctor {
                config_path: Some(PathBuf::from("/tmp/server.toml")),
            })
        );
    }

    #[test]
    fn server_doctor_report_checks_local_paths_and_store() {
        let fixture = tempdir().expect("tempdir should be created");
        let library_root = fixture.path().join("library");
        let state_dir = fixture.path().join("state");
        let tls_dir = fixture.path().join("tls");
        std::fs::create_dir_all(&library_root).expect("library root should be created");
        let mut tls = ServerTlsConfig::local_dev(&tls_dir);
        tls.server_names = vec![String::from("localhost")];
        let config = ServerConfig {
            bind_address: String::from("127.0.0.1:0"),
            library_root: library_root.to_string_lossy().into_owned(),
            state_dir: state_dir.to_string_lossy().into_owned(),
            tls_dir: tls_dir.to_string_lossy().into_owned(),
            tls,
        };

        let report = server_doctor_report(&config).expect("doctor should pass");

        assert!(report.contains("ok library_root"));
        assert!(report.contains("ok state_dir"));
        assert!(report.contains("ok tls_dir"));
        assert!(report.contains("ok canonical_store"));
    }
}
