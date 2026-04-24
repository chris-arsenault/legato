//! Cross-crate integration coverage for the v1 server/client/prefetch flow.

use std::{fs, io::Write, path::Path};

use flate2::{Compression, write::GzEncoder};
use legato_client_cache::{
    MetadataCache, MetadataCachePolicy,
    catalog::{CatalogStore, inode_to_proto},
    client_store::ClientLegatoStore,
};
use legato_client_core::{
    ClientConfig, ClientTlsConfig, FilesystemService, LocalControlPlane, RetryPolicy,
};
use legato_prefetch::analyze_project;
use legato_server::{
    LiveServer, ServerConfig, ServerTlsConfig, ensure_server_tls_materials,
    issue_client_tls_bundle, load_runtime_tls, reconcile_library_root_to_store,
};
use legato_types::{ExtentRange, FileId, PrefetchHintPath, PrefetchPriority};
use tempfile::tempdir;
use tokio::net::TcpListener;
use walkdir::WalkDir;

#[test]
fn indexed_server_and_client_prefetch_round_trip_sample_data() {
    let temp = tempdir().expect("tempdir should be created");
    let library_root = temp.path().join("library");
    let sample_path = library_root.join("Strings").join("long.ncw");
    fs::create_dir_all(sample_path.parent().expect("sample should have a parent"))
        .expect("library fixture directory should be created");
    fs::write(&sample_path, vec![0x5a; 8192]).expect("sample fixture should be written");

    let store_root = temp.path().join("server-state");
    reconcile_library_root_to_store(&store_root, &library_root).expect("reconcile should succeed");
    let catalog = CatalogStore::open(&store_root, 0).expect("catalog should open");
    let inode = inode_to_proto(
        catalog
            .resolve_path("/Strings/long.ncw")
            .expect("sample should resolve")
            .clone(),
    );
    let mut store =
        ClientLegatoStore::open(temp.path().join("client-state"), 1).expect("store should open");
    let mut control = LocalControlPlane::new(MetadataCache::new(MetadataCachePolicy::default()));
    control.register_resolved_path(inode.clone(), 1);

    let execution = control
        .prefetch_paths(
            &[PrefetchHintPath {
                path: Path::new("/Strings/long.ncw").to_path_buf(),
                file_offset: 0,
                length: inode.size,
                priority: PrefetchPriority::P0,
            }],
            PrefetchPriority::P1,
            &mut store,
            2,
            |extent| {
                std::fs::read(&sample_path).expect("sample should read")[(extent.file_offset
                    as usize)
                    ..(extent.file_offset + extent.length).min(inode.size) as usize]
                    .to_vec()
            },
        )
        .expect("prefetch should succeed");

    assert_eq!(execution.accepted.len(), 1);
    assert_eq!(execution.completed.len(), 1);
    assert!(control.is_extent_resident(
        &ExtentRange {
            file_id: FileId(inode.file_id),
            extent_index: 0,
            file_offset: 0,
            length: inode.size,
        },
        PrefetchPriority::P1,
    ));
}

#[test]
fn project_analysis_hints_feed_directly_into_prefetch_execution() {
    let temp = tempdir().expect("tempdir should be created");
    let library_root = temp.path().join("library");
    let sample_path = library_root.join("Drums").join("kick.wav");
    fs::create_dir_all(sample_path.parent().expect("sample should have a parent"))
        .expect("library fixture directory should be created");
    fs::write(&sample_path, vec![0x41; 4096]).expect("sample fixture should be written");

    let project_path = temp.path().join("session.als");
    let xml = format!(
        r#"<Ableton><Plugin Device="Kontakt"/><SampleRef Path="{}"/></Ableton>"#,
        "/Drums/kick.wav"
    );
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(xml.as_bytes())
        .expect("project xml should be compressed");
    fs::write(&project_path, encoder.finish().expect("gzip should finish"))
        .expect("project fixture should be written");

    let store_root = temp.path().join("server-state");
    reconcile_library_root_to_store(&store_root, &library_root).expect("reconcile should succeed");
    let analysis = analyze_project(&project_path).expect("analysis should succeed");
    let hint = analysis
        .hints
        .first()
        .expect("analysis should emit one hint");

    let catalog = CatalogStore::open(&store_root, 0).expect("catalog should open");
    let inode = inode_to_proto(
        catalog
            .resolve_path(&hint.path)
            .expect("sample should resolve")
            .clone(),
    );
    let mut store =
        ClientLegatoStore::open(temp.path().join("client-state"), 1).expect("store should open");
    let mut control = LocalControlPlane::new(MetadataCache::new(MetadataCachePolicy::default()));
    control.register_resolved_path(inode.clone(), 1);

    let execution = control
        .prefetch_paths(
            &[PrefetchHintPath {
                path: Path::new("/Drums/kick.wav").to_path_buf(),
                file_offset: hint.file_offset,
                length: hint.length,
                priority: hint.priority,
            }],
            analysis.wait_through,
            &mut store,
            2,
            |extent| {
                std::fs::read(&sample_path).expect("sample should read")[(extent.file_offset
                    as usize)
                    ..(extent.file_offset + extent.length).min(inode.size) as usize]
                    .to_vec()
            },
        )
        .expect("prefetch should succeed");

    assert_eq!(analysis.plugins, vec![String::from("Kontakt")]);
    assert_eq!(execution.completed.len(), 1);
    assert!(control.is_extent_resident(
        &ExtentRange {
            file_id: FileId(inode.file_id),
            extent_index: 0,
            file_offset: 0,
            length: inode.size,
        },
        PrefetchPriority::P1,
    ));
}

#[tokio::test]
async fn mounted_cold_read_reuses_persisted_extent_state_after_client_restart() {
    let fixture = tempdir().expect("tempdir should be created");
    let library_root = fixture.path().join("library");
    let state_dir = fixture.path().join("state");
    let tls_dir = fixture.path().join("tls");
    let client_state_dir = fixture.path().join("client-state");
    fs::create_dir_all(library_root.join("Strings")).expect("library tree should be created");
    let sample_path = library_root.join("Strings").join("long.ncw");
    fs::write(&sample_path, vec![0x5a; 8192]).expect("sample should be written");

    let mut config = ServerConfig {
        bind_address: String::from("127.0.0.1:0"),
        library_root: library_root.to_string_lossy().into_owned(),
        state_dir: state_dir.to_string_lossy().into_owned(),
        tls_dir: tls_dir.to_string_lossy().into_owned(),
        tls: ServerTlsConfig::local_dev(&tls_dir),
        bootstrap: Default::default(),
    };
    config.tls.server_names = vec![String::from("127.0.0.1"), String::from("localhost")];
    ensure_server_tls_materials(Path::new(&config.tls_dir), &config.tls)
        .expect("tls materials should be created");

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let address = listener.local_addr().expect("addr should be available");
    let server = LiveServer::bootstrap(config.clone()).expect("server should bootstrap");
    let bound = server
        .bind(
            listener,
            Some(load_runtime_tls(&config.tls).expect("runtime tls should load")),
        )
        .await
        .expect("server should bind");

    let bundle_dir = fixture.path().join("bundle");
    issue_client_tls_bundle(
        Path::new(&config.tls_dir),
        &config.tls,
        "studio-restart",
        &bundle_dir,
    )
    .expect("client bundle should be issued");

    let mut first_service = FilesystemService::connect(
        local_client_config(address.to_string(), &bundle_dir, "localhost"),
        "studio-restart",
        &client_state_dir,
    )
    .await
    .expect("first service should connect");
    let handle = first_service
        .open("/Strings/long.ncw")
        .await
        .expect("open should succeed");
    let first_read = first_service
        .read(handle.local_handle, 0, 4096)
        .await
        .expect("cold read should succeed");
    assert_eq!(first_read, vec![0x5a; 4096]);
    first_service
        .release(handle.local_handle)
        .await
        .expect("release should succeed");
    drop(first_service);

    let extent_files_after_first_read = count_extent_files(&client_state_dir.join("segments"));
    assert!(
        extent_files_after_first_read > 0,
        "cold read should materialize persisted extents"
    );

    let mut restarted_service = FilesystemService::connect(
        local_client_config(address.to_string(), &bundle_dir, "localhost"),
        "studio-restart",
        &client_state_dir,
    )
    .await
    .expect("restarted service should connect");
    let reopened = restarted_service
        .open("/Strings/long.ncw")
        .await
        .expect("open after restart should succeed");
    let second_read = restarted_service
        .read(reopened.local_handle, 0, 4096)
        .await
        .expect("warm read after restart should succeed");
    assert_eq!(second_read, first_read);
    restarted_service
        .release(reopened.local_handle)
        .await
        .expect("release should succeed");

    let extent_files_after_restart = count_extent_files(&client_state_dir.join("segments"));
    assert!(extent_files_after_restart >= extent_files_after_first_read);

    drop(restarted_service);
    bound.shutdown().await.expect("server should shut down");
}

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

fn count_extent_files(root: &Path) -> usize {
    if !root.exists() {
        return 0;
    }

    WalkDir::new(root)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file())
        .count()
}
