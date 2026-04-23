//! End-to-end coverage for the live gRPC client transport.

use std::{fs, path::Path};

use legato_client_core::{
    ClientConfig, ClientTlsConfig, GrpcClientTransport, RetryPolicy, SessionStatus,
};
use legato_proto::{BlockRequest, Capability, ChangeKind};
use legato_server::{
    LiveServer, ServerConfig, ServerTlsConfig, ensure_server_tls_materials,
    issue_client_tls_bundle, load_runtime_tls,
};
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::time::{Duration, sleep};

fn local_client_config(endpoint: String, bundle_dir: &Path, server_name: &str) -> ClientConfig {
    ClientConfig {
        endpoint,
        tls: ClientTlsConfig::local_dev(bundle_dir, server_name),
        retry: RetryPolicy {
            initial_delay_ms: 0,
            max_delay_ms: 0,
            multiplier: 2,
        },
        ..ClientConfig::default()
    }
}

#[tokio::test]
async fn grpc_client_transport_attaches_to_live_server_and_reads_blocks() {
    let fixture = tempdir().expect("tempdir should be created");
    let library_root = fixture.path().join("library");
    let state_dir = fixture.path().join("state");
    let tls_dir = fixture.path().join("tls");
    fs::create_dir_all(library_root.join("Kontakt")).expect("library tree should be created");
    let sample_path = library_root.join("Kontakt").join("piano.nki");
    fs::write(&sample_path, b"hello legato").expect("sample should be written");

    let mut config = ServerConfig {
        bind_address: String::from("127.0.0.1:0"),
        library_root: library_root.to_string_lossy().into_owned(),
        state_dir: state_dir.to_string_lossy().into_owned(),
        tls_dir: tls_dir.to_string_lossy().into_owned(),
        tls: ServerTlsConfig::local_dev(&tls_dir),
    };
    config.tls.server_names = vec![String::from("127.0.0.1"), String::from("localhost")];
    ensure_server_tls_materials(Path::new(&config.tls_dir), &config.tls)
        .expect("tls materials should be created");

    let server = LiveServer::bootstrap(config.clone()).expect("server should bootstrap");
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let address = listener
        .local_addr()
        .expect("listener addr should be available");
    let runtime_tls = load_runtime_tls(&config.tls).expect("runtime tls should load");
    let bound = server
        .bind(listener, Some(runtime_tls))
        .await
        .expect("server should bind");

    let bundle_dir = fixture.path().join("bundle");
    issue_client_tls_bundle(
        Path::new(&config.tls_dir),
        &config.tls,
        "studio-mac",
        &bundle_dir,
    )
    .expect("client bundle should be issued");

    let client_config = local_client_config(address.to_string(), &bundle_dir, "localhost");
    let mut transport = GrpcClientTransport::connect(client_config, "studio-mac")
        .await
        .expect("client should connect");

    assert_eq!(transport.attach_session().server_name, "legato-server");
    assert!(
        transport
            .attach_session()
            .negotiated_capabilities
            .contains(&(Capability::Metadata as i32))
    );

    let metadata = transport
        .stat(sample_path.to_string_lossy().into_owned())
        .await
        .expect("stat should succeed");
    assert_eq!(metadata.path, sample_path.to_string_lossy());

    let opened = transport
        .open(sample_path.to_string_lossy().into_owned())
        .await
        .expect("open should succeed");
    let blocks = transport
        .read_blocks(vec![BlockRequest {
            file_handle: opened.file_handle,
            start_offset: 0,
            block_count: 1,
        }])
        .await
        .expect("block read should succeed");
    assert_eq!(blocks.len(), 1);
    assert_eq!(blocks[0].data, b"hello legato");

    let entries = transport
        .list_dir(library_root.join("Kontakt").to_string_lossy().into_owned())
        .await
        .expect("list dir should succeed");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, "piano.nki");

    bound
        .shutdown()
        .await
        .expect("server should shut down cleanly");
}

#[tokio::test]
async fn grpc_client_transport_reconnects_and_reopens_stale_handles() {
    let fixture = tempdir().expect("tempdir should be created");
    let library_root = fixture.path().join("library");
    let state_dir = fixture.path().join("state");
    let tls_dir = fixture.path().join("tls");
    fs::create_dir_all(library_root.join("Strings")).expect("library tree should be created");
    let sample_path = library_root.join("Strings").join("long.ncw");
    fs::write(&sample_path, vec![0x5a; 4096]).expect("sample should be written");

    let mut config = ServerConfig {
        bind_address: String::from("127.0.0.1:0"),
        library_root: library_root.to_string_lossy().into_owned(),
        state_dir: state_dir.to_string_lossy().into_owned(),
        tls_dir: tls_dir.to_string_lossy().into_owned(),
        tls: ServerTlsConfig::local_dev(&tls_dir),
    };
    config.tls.server_names = vec![String::from("127.0.0.1"), String::from("localhost")];
    ensure_server_tls_materials(Path::new(&config.tls_dir), &config.tls)
        .expect("tls materials should be created");

    let bundle_dir = fixture.path().join("bundle");
    issue_client_tls_bundle(
        Path::new(&config.tls_dir),
        &config.tls,
        "studio-win",
        &bundle_dir,
    )
    .expect("client bundle should be issued");

    let first_server = LiveServer::bootstrap(config.clone()).expect("server should bootstrap");
    let first_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let address = first_listener
        .local_addr()
        .expect("listener addr should be available");
    let first_tls = load_runtime_tls(&config.tls).expect("runtime tls should load");
    let first_bound = first_server
        .bind(first_listener, Some(first_tls))
        .await
        .expect("server should bind");

    let client_config = local_client_config(address.to_string(), &bundle_dir, "localhost");
    let mut transport = GrpcClientTransport::connect(client_config, "studio-win")
        .await
        .expect("client should connect");
    let opened = transport
        .open(sample_path.to_string_lossy().into_owned())
        .await
        .expect("open should succeed");
    assert_eq!(
        transport.runtime().session_status(),
        &SessionStatus::Connected {
            generation: 1,
            subscription_active: false,
        }
    );

    first_bound
        .shutdown()
        .await
        .expect("server should shut down cleanly");

    let second_server = LiveServer::bootstrap(config).expect("server should bootstrap");
    let second_listener = TcpListener::bind(address)
        .await
        .expect("listener should rebind");
    let second_tls =
        load_runtime_tls(&ServerTlsConfig::local_dev(&tls_dir)).expect("runtime tls should load");
    let second_bound = second_server
        .bind(second_listener, Some(second_tls))
        .await
        .expect("server should bind");

    let recovery = transport
        .reconnect()
        .await
        .expect("reconnect should succeed");
    assert_eq!(
        recovery.reopened_paths,
        vec![sample_path.to_string_lossy().into_owned()]
    );
    assert_eq!(
        transport.runtime().session_status(),
        &SessionStatus::Connected {
            generation: 2,
            subscription_active: false,
        }
    );
    assert!(
        transport
            .runtime()
            .open_file(&sample_path.to_string_lossy())
            .is_some()
    );

    let blocks = transport
        .read_blocks(vec![BlockRequest {
            file_handle: opened.file_handle,
            start_offset: 0,
            block_count: 1,
        }])
        .await;
    assert!(blocks.is_ok(), "reads should work after reconnect");

    second_bound
        .shutdown()
        .await
        .expect("server should shut down cleanly");
}

#[tokio::test]
async fn grpc_client_transport_streams_change_records_after_upstream_mutation() {
    let fixture = tempdir().expect("tempdir should be created");
    let library_root = fixture.path().join("library");
    let state_dir = fixture.path().join("state");
    let tls_dir = fixture.path().join("tls");
    fs::create_dir_all(library_root.join("Kontakt")).expect("library tree should be created");
    let sample_path = library_root.join("Kontakt").join("piano.nki");
    fs::write(&sample_path, b"hello legato").expect("sample should be written");

    let mut config = ServerConfig {
        bind_address: String::from("127.0.0.1:0"),
        library_root: library_root.to_string_lossy().into_owned(),
        state_dir: state_dir.to_string_lossy().into_owned(),
        tls_dir: tls_dir.to_string_lossy().into_owned(),
        tls: ServerTlsConfig::local_dev(&tls_dir),
    };
    config.tls.server_names = vec![String::from("127.0.0.1"), String::from("localhost")];
    ensure_server_tls_materials(Path::new(&config.tls_dir), &config.tls)
        .expect("tls materials should be created");

    let server = LiveServer::bootstrap(config.clone()).expect("server should bootstrap");
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let address = listener
        .local_addr()
        .expect("listener addr should be available");
    let runtime_tls = load_runtime_tls(&config.tls).expect("runtime tls should load");
    let bound = server
        .bind(listener, Some(runtime_tls))
        .await
        .expect("server should bind");

    let bundle_dir = fixture.path().join("bundle");
    issue_client_tls_bundle(
        Path::new(&config.tls_dir),
        &config.tls,
        "studio-sync",
        &bundle_dir,
    )
    .expect("client bundle should be issued");

    let client_config = local_client_config(address.to_string(), &bundle_dir, "localhost");
    let mut transport = GrpcClientTransport::connect(client_config, "studio-sync")
        .await
        .expect("client should connect");
    let baseline = transport
        .change_records_since(0)
        .await
        .expect("baseline change records should load");
    let baseline_sequence = baseline
        .iter()
        .map(|record| record.sequence)
        .max()
        .unwrap_or(0);

    let new_sample_path = library_root.join("Kontakt").join("strings.nki");
    fs::write(&new_sample_path, b"fresh catalog data").expect("new sample should be written");

    let mut observed = Vec::new();
    for _attempt in 0..20 {
        observed = transport
            .change_records_since(baseline_sequence)
            .await
            .expect("change records should load");
        if observed
            .iter()
            .any(|record| record.path == new_sample_path.to_string_lossy())
        {
            break;
        }
        sleep(Duration::from_millis(250)).await;
    }

    assert!(
        observed.iter().any(|record| {
            record.path == new_sample_path.to_string_lossy()
                && record.kind == ChangeKind::Upsert as i32
        }),
        "expected new file to appear in ordered change records"
    );

    bound
        .shutdown()
        .await
        .expect("server should shut down cleanly");
}
