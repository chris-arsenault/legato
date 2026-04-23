//! Binary entrypoint for the Legato server daemon.

use std::{
    env,
    path::{Path, PathBuf},
};

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
    let process_config = load_config::<ServerProcessConfig>(
        Some(Path::new("/etc/legato/server.toml")),
        "LEGATO_SERVER",
    )
    .unwrap_or_else(|_| ServerProcessConfig::default());

    if let Some(command) = parse_command()? {
        return run_command(command, &process_config);
    }

    init_tracing("legato-server", &process_config.common.tracing)?;
    let shutdown = ShutdownController::new();
    let telemetry = ProcessTelemetry::new("legato-server", &process_config.common.metrics);
    telemetry.record_startup();
    telemetry.set_lifecycle_state("bootstrap", 1);
    let _metrics_exporter = telemetry.spawn_exporter(shutdown.token())?;
    ensure_server_tls_materials(
        Path::new(&process_config.server.tls_dir),
        &process_config.server.tls,
    )?;
    build_tls_server_config(&process_config.server.tls)?;
    let runtime_tls = load_runtime_tls(&process_config.server.tls)?;
    let bind_address = parse_bind_address(&process_config.server.bind_address)?;
    let listener = tokio::net::TcpListener::bind(bind_address).await?;

    let server = LiveServer::bootstrap_with_metrics(
        process_config.server,
        Some(ServerRuntimeMetrics::new(telemetry.clone())),
    )?;
    let bound = server.bind(listener, Some(runtime_tls)).await?;
    telemetry.set_lifecycle_state("ready", 1);
    println!("legato-server bootstrap ready");
    tokio::signal::ctrl_c().await?;
    bound.shutdown().await
}

#[derive(Debug, Eq, PartialEq)]
enum Command {
    IssueClient {
        name: String,
        output_dir: PathBuf,
        endpoint: Option<String>,
        server_name: Option<String>,
        mount_point: Option<String>,
        library_root: Option<String>,
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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{Command, parse_command_impl};

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
}
