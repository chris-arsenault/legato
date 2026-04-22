//! Cross-crate integration coverage for the MVP server/client/prefetch flow.

use std::{fs, io::Write};

use flate2::{Compression, write::GzEncoder};
use legato_client_cache::{
    ExtentCacheStore, MetadataCache, MetadataCachePolicy, open_cache_database,
};
use legato_client_core::LocalControlPlane;
use legato_prefetch::analyze_project;
use legato_proto::{ExtentDescriptor, FileLayout, InodeMetadata, OpenRequest};
use legato_server::{MetadataService, open_metadata_database, reconcile_library_root};
use legato_types::{ExtentRange, FileId, PrefetchHintPath, PrefetchPriority};
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
    let inode = catalog_entry_to_inode(
        service
            .resolve_catalog_path(sample_path.to_string_lossy().as_ref())
            .expect("resolve should succeed")
            .expect("sample should resolve"),
    );
    let _open = service
        .open(OpenRequest {
            path: sample_path.to_string_lossy().into_owned(),
        })
        .expect("resolve should succeed")
        .expect("sample should open");

    let client_db =
        open_cache_database(&temp.path().join("client.sqlite")).expect("client cache should open");
    let mut store =
        ExtentCacheStore::new(&temp.path().join("extents"), client_db).expect("store should open");
    let mut control = LocalControlPlane::new(MetadataCache::new(MetadataCachePolicy::default()));
    control.register_resolved_path(inode.clone(), 1);

    let execution = control
        .prefetch_paths(
            &[PrefetchHintPath {
                path: sample_path.clone(),
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
    let inode = catalog_entry_to_inode(
        service
            .resolve_catalog_path(&hint.path)
            .expect("resolve should succeed")
            .expect("sample should resolve"),
    );
    let _open = service
        .open(OpenRequest {
            path: hint.path.clone(),
        })
        .expect("resolve should succeed")
        .expect("sample should open");

    let client_db =
        open_cache_database(&temp.path().join("client.sqlite")).expect("client cache should open");
    let mut store =
        ExtentCacheStore::new(&temp.path().join("extents"), client_db).expect("store should open");
    let mut control = LocalControlPlane::new(MetadataCache::new(MetadataCachePolicy::default()));
    control.register_resolved_path(inode.clone(), 1);

    let execution = control
        .prefetch_paths(
            &[PrefetchHintPath {
                path: sample_path.clone(),
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

fn catalog_entry_to_inode(entry: legato_server::CatalogEntry) -> InodeMetadata {
    let layout =
        entry
            .transfer_class
            .zip(entry.extent_bytes)
            .map(|(transfer_class, extent_bytes)| FileLayout {
                transfer_class: transfer_class as i32,
                extents: build_extents(entry.metadata.size, extent_bytes),
            });
    InodeMetadata {
        file_id: entry.metadata.file_id,
        path: entry.metadata.path,
        size: entry.metadata.size,
        mtime_ns: entry.metadata.mtime_ns,
        is_dir: entry.metadata.is_dir,
        layout,
    }
}

fn build_extents(size: u64, extent_bytes: u64) -> Vec<ExtentDescriptor> {
    if size == 0 {
        return vec![ExtentDescriptor {
            extent_index: 0,
            file_offset: 0,
            length: 1,
            extent_hash: Vec::new(),
        }];
    }

    let mut extents = Vec::new();
    let mut file_offset = 0_u64;
    let mut extent_index = 0_u32;
    while file_offset < size {
        let length = extent_bytes.max(1).min(size - file_offset);
        extents.push(ExtentDescriptor {
            extent_index,
            file_offset,
            length,
            extent_hash: Vec::new(),
        });
        file_offset += length;
        extent_index += 1;
    }
    extents
}
