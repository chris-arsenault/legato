//! End-to-end coverage for the live gRPC client transport.

use std::{fs, path::Path};

use legato_client_core::{
    ClientConfig, ClientTlsConfig, GrpcClientTransport, RetryPolicy, SessionStatus,
};
use legato_proto::{Capability, ExtentRef, TransferClass};
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
async fn grpc_client_transport_attaches_resolves_and_fetches_extents() {
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
    assert!(
        !transport
            .attach_session()
            .negotiated_capabilities
            .contains(&(Capability::Hint as i32))
    );

    let metadata = transport
        .stat(String::from("/Kontakt/piano.nki"))
        .await
        .expect("stat should succeed");
    assert_eq!(metadata.path, "/Kontakt/piano.nki");

    let inode = transport
        .resolve(String::from("/Kontakt/piano.nki"))
        .await
        .expect("resolve should succeed");
    let layout = inode.layout.expect("file layout should be present");
    assert_eq!(layout.transfer_class, TransferClass::Unitary as i32);
    assert_eq!(inode.inode_generation, 1);
    assert!(!inode.content_hash.is_empty());
    let extent = layout
        .extents
        .first()
        .expect("sample file should have one extent");
    let records = transport
        .fetch_extents(vec![ExtentRef {
            file_id: inode.file_id,
            extent_index: extent.extent_index,
            file_offset: extent.file_offset,
            length: extent.length,
            inode_generation: inode.inode_generation,
            extent_hash: extent.extent_hash.clone(),
        }])
        .await
        .expect("extent fetch should succeed");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].data, b"hello legato");
    assert_eq!(records[0].transfer_class, TransferClass::Unitary as i32);
    assert_eq!(metadata.content_hash, inode.content_hash);

    let entries = transport
        .list_dir(String::from("/Kontakt"))
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
async fn grpc_client_transport_reconnects_and_fetches_extents_after_restart() {
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
    let inode = transport
        .resolve(String::from("/Strings/long.ncw"))
        .await
        .expect("resolve should succeed");
    fetch_first_extent(&mut transport, &inode)
        .await
        .expect("initial extent fetch should succeed");
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
    assert!(recovery.reopened_paths.is_empty());
    assert_eq!(
        transport.runtime().session_status(),
        &SessionStatus::Connected {
            generation: 2,
            subscription_active: false,
        }
    );
    let inode_after_restart = transport.resolve(String::from("/Strings/long.ncw")).await;
    assert!(
        inode_after_restart.is_ok(),
        "resolve should work after reconnect"
    );
    let records = fetch_first_extent(
        &mut transport,
        &inode_after_restart.expect("inode should be resolved after reconnect"),
    )
    .await
    .expect("fetch should work after reconnect");
    assert_eq!(records[0].data, vec![0x5a; 4096]);

    second_bound
        .shutdown()
        .await
        .expect("server should shut down cleanly");
}

#[tokio::test]
async fn grpc_client_transport_rejects_fetches_for_stale_inode_generation() {
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

    let original_inode = transport
        .resolve(String::from("/Kontakt/piano.nki"))
        .await
        .expect("resolve should succeed");
    let original_extent = original_inode
        .layout
        .as_ref()
        .and_then(|layout| layout.extents.first())
        .cloned()
        .expect("extent should be present");

    fs::write(&sample_path, b"updated payload").expect("sample should update");

    let mut refreshed_inode = None;
    for _attempt in 0..20 {
        let candidate = transport
            .resolve(String::from("/Kontakt/piano.nki"))
            .await
            .expect("resolve should continue succeeding");
        if candidate.inode_generation > original_inode.inode_generation {
            refreshed_inode = Some(candidate);
            break;
        }
        sleep(Duration::from_millis(250)).await;
    }

    let refreshed_inode = refreshed_inode.expect("inode generation should advance");
    let stale_error = transport
        .fetch_extents(vec![ExtentRef {
            file_id: original_inode.file_id,
            extent_index: original_extent.extent_index,
            file_offset: original_extent.file_offset,
            length: original_extent.length,
            inode_generation: original_inode.inode_generation,
            extent_hash: original_extent.extent_hash.clone(),
        }])
        .await
        .expect_err("stale fetch should fail");
    match stale_error {
        legato_client_core::ClientTransportError::Rpc(status) => {
            assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        }
        other => panic!("unexpected error: {other}"),
    }

    let refreshed_extent = refreshed_inode
        .layout
        .as_ref()
        .and_then(|layout| layout.extents.first())
        .cloned()
        .expect("refreshed extent should exist");
    let records = transport
        .fetch_extents(vec![ExtentRef {
            file_id: refreshed_inode.file_id,
            extent_index: refreshed_extent.extent_index,
            file_offset: refreshed_extent.file_offset,
            length: refreshed_extent.length,
            inode_generation: refreshed_inode.inode_generation,
            extent_hash: refreshed_extent.extent_hash.clone(),
        }])
        .await
        .expect("refreshed fetch should succeed");
    assert_eq!(records[0].data, b"updated payload");

    bound
        .shutdown()
        .await
        .expect("server should shut down cleanly");
}

async fn fetch_first_extent(
    transport: &mut GrpcClientTransport,
    inode: &legato_proto::InodeMetadata,
) -> Result<Vec<legato_proto::ExtentRecord>, legato_client_core::ClientTransportError> {
    let layout =
        inode
            .layout
            .as_ref()
            .ok_or(legato_client_core::ClientTransportError::MissingField(
                "inode.layout",
            ))?;
    let extent =
        layout
            .extents
            .first()
            .ok_or(legato_client_core::ClientTransportError::MissingField(
                "inode.layout.extents",
            ))?;
    transport
        .fetch_extents(vec![ExtentRef {
            file_id: inode.file_id,
            extent_index: extent.extent_index,
            file_offset: extent.file_offset,
            length: extent.length,
            inode_generation: inode.inode_generation,
            extent_hash: extent.extent_hash.clone(),
        }])
        .await
}
