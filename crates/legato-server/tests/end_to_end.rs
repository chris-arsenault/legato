//! Cross-crate integration coverage for the MVP server/client/prefetch flow.

use std::{fs, io::Write};

use flate2::{Compression, write::GzEncoder};
use legato_client_cache::{
    BlockCacheStore, MetadataCache, MetadataCachePolicy, open_cache_database,
};
use legato_client_core::LocalControlPlane;
use legato_prefetch::analyze_project;
use legato_proto::{BlockRequest, OpenRequest, ReadBlocksRequest, ResolvePathRequest};
use legato_server::{MetadataService, open_metadata_database, reconcile_library_root};
use legato_types::{FileId, PrefetchHintPath, PrefetchPriority};
use tempfile::tempdir;

#[test]
fn indexed_server_and_client_prefetch_round_trip_sample_data() {
    let temp = tempdir().expect("tempdir should be created");
    let library_root = temp.path().join("library");
    let sample_path = library_root.join("Strings").join("long.ncw");
    fs::create_dir_all(sample_path.parent().expect("sample should have a parent"))
        .expect("library fixture directory should be created");
    fs::write(&sample_path, vec![0x5a; 8192]).expect("sample fixture should be written");

    let database_path = temp.path().join("server.sqlite");
    let mut connection = open_metadata_database(&database_path).expect("database should open");
    reconcile_library_root(&mut connection, &library_root).expect("reconcile should succeed");

    let mut service = MetadataService::new(
        open_metadata_database(&database_path).expect("database should reopen"),
    );
    let metadata = service
        .resolve_path(ResolvePathRequest {
            path: sample_path.to_string_lossy().into_owned(),
        })
        .expect("resolve should succeed")
        .expect("sample should resolve")
        .metadata
        .expect("metadata should be present");
    let open = service
        .open(OpenRequest {
            path: sample_path.to_string_lossy().into_owned(),
        })
        .expect("open should succeed")
        .expect("sample should open");

    let client_db =
        open_cache_database(&temp.path().join("client.sqlite")).expect("client cache should open");
    let mut store =
        BlockCacheStore::new(&temp.path().join("blocks"), client_db).expect("store should open");
    let mut control = LocalControlPlane::new(
        MetadataCache::new(MetadataCachePolicy::default()),
        metadata.block_size,
    );
    control.register_path(metadata.clone(), 1);

    let execution = control
        .prefetch_paths(
            &[PrefetchHintPath {
                path: sample_path.clone(),
                start_offset: 0,
                block_count: 1,
                priority: PrefetchPriority::P0,
            }],
            PrefetchPriority::P1,
            &mut store,
            2,
            |_file_id, offset| {
                service
                    .read_blocks(ReadBlocksRequest {
                        ranges: vec![BlockRequest {
                            file_handle: open.file_handle,
                            start_offset: offset,
                            block_count: 1,
                        }],
                    })
                    .expect("server read should succeed")
                    .into_iter()
                    .next()
                    .expect("one block should be returned")
                    .data
            },
        )
        .expect("prefetch should succeed");

    assert_eq!(execution.accepted.len(), 1);
    assert_eq!(execution.completed.len(), 1);
    assert!(control.is_range_resident(
        &legato_types::BlockRange {
            file_id: FileId(metadata.file_id),
            start_offset: 0,
            block_count: 1,
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
        sample_path.to_string_lossy()
    );
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(xml.as_bytes())
        .expect("project xml should be compressed");
    fs::write(&project_path, encoder.finish().expect("gzip should finish"))
        .expect("project fixture should be written");

    let database_path = temp.path().join("server.sqlite");
    let mut connection = open_metadata_database(&database_path).expect("database should open");
    reconcile_library_root(&mut connection, &library_root).expect("reconcile should succeed");
    let analysis = analyze_project(&project_path).expect("analysis should succeed");
    let hint = analysis
        .hints
        .first()
        .expect("analysis should emit one hint");

    let mut service = MetadataService::new(
        open_metadata_database(&database_path).expect("database should reopen"),
    );
    let metadata = service
        .resolve_path(ResolvePathRequest {
            path: hint.path.clone(),
        })
        .expect("resolve should succeed")
        .expect("sample should resolve")
        .metadata
        .expect("metadata should be present");
    let open = service
        .open(OpenRequest {
            path: hint.path.clone(),
        })
        .expect("open should succeed")
        .expect("sample should open");

    let client_db =
        open_cache_database(&temp.path().join("client.sqlite")).expect("client cache should open");
    let mut store =
        BlockCacheStore::new(&temp.path().join("blocks"), client_db).expect("store should open");
    let mut control = LocalControlPlane::new(
        MetadataCache::new(MetadataCachePolicy::default()),
        metadata.block_size,
    );
    control.register_path(metadata.clone(), 1);

    let execution = control
        .prefetch_paths(
            &[PrefetchHintPath {
                path: sample_path.clone(),
                start_offset: hint.start_offset,
                block_count: hint.block_count,
                priority: hint.priority,
            }],
            analysis.wait_through,
            &mut store,
            2,
            |_file_id, offset| {
                service
                    .read_blocks(ReadBlocksRequest {
                        ranges: vec![BlockRequest {
                            file_handle: open.file_handle,
                            start_offset: offset,
                            block_count: 1,
                        }],
                    })
                    .expect("server read should succeed")
                    .into_iter()
                    .next()
                    .expect("one block should be returned")
                    .data
            },
        )
        .expect("prefetch should succeed");

    assert_eq!(analysis.plugins, vec![String::from("Kontakt")]);
    assert_eq!(execution.completed.len(), 1);
    assert!(control.is_range_resident(
        &legato_types::BlockRange {
            file_id: FileId(metadata.file_id),
            start_offset: 0,
            block_count: 1,
        },
        PrefetchPriority::P1,
    ));
}
