//! Criterion benchmarks for the extent-oriented Legato server workloads.
#![allow(missing_docs)]

use std::fs;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use legato_proto::ExtentRef;
use legato_server::{
    MetadataService, ServerExtentStore, open_metadata_database, reconcile_library_root,
};
use tempfile::tempdir;

fn benchmark_library_scan(c: &mut Criterion) {
    let temp = tempdir().expect("tempdir should be created");
    let library_root = temp.path().join("library");
    create_library_fixture(&library_root, 24, 16, 32 * 1024);

    c.bench_function("library_scan/reconcile_full_tree", |b| {
        b.iter_batched(
            || {
                let database_path = temp.path().join("scan.sqlite");
                let _ = fs::remove_file(&database_path);
                open_metadata_database(&database_path).expect("database should open")
            },
            |mut connection| {
                let stats = reconcile_library_root(&mut connection, &library_root)
                    .expect("reconcile should succeed");
                criterion::black_box(stats);
            },
            BatchSize::PerIteration,
        )
    });
}

fn benchmark_cold_resolve_and_extent_fetch(c: &mut Criterion) {
    let temp = tempdir().expect("tempdir should be created");
    let library_root = temp.path().join("library");
    create_library_fixture(&library_root, 12, 8, 256 * 1024);
    let sample_path = library_root.join("bank-00").join("sample-0000.wav");
    let state_dir = temp.path().join("state");

    let database_path = temp.path().join("runtime.sqlite");
    let mut connection = open_metadata_database(&database_path).expect("database should open");
    reconcile_library_root(&mut connection, &library_root).expect("reconcile should succeed");

    c.bench_function("client_resolve/cold_metadata_resolve", |b| {
        b.iter_batched(
            || {
                MetadataService::new(
                    open_metadata_database(&database_path).expect("database should reopen"),
                )
            },
            |service| {
                let response = service
                    .resolve_catalog_path(&sample_path.to_string_lossy())
                    .expect("resolve should succeed");
                criterion::black_box(response);
            },
            BatchSize::SmallInput,
        )
    });

    c.bench_function("extent_fetch/cold_materialization", |b| {
        b.iter_batched(
            || {
                let service = MetadataService::new(
                    open_metadata_database(&database_path).expect("database should reopen"),
                );
                let entry = service
                    .resolve_catalog_path(&sample_path.to_string_lossy())
                    .expect("resolve should succeed")
                    .expect("sample should exist");
                let extent_store = ServerExtentStore::new(&state_dir);
                let extent_ref = ExtentRef {
                    file_id: entry.metadata.file_id,
                    extent_index: 0,
                    file_offset: 0,
                    length: entry.extent_bytes.expect("extent size should be set"),
                };
                let _ = fs::remove_dir_all(
                    state_dir
                        .join("extents")
                        .join(entry.metadata.file_id.to_string()),
                );
                (extent_store, entry, extent_ref)
            },
            |(extent_store, entry, extent_ref)| {
                let extent = extent_store
                    .fetch_extent(&entry, &extent_ref)
                    .expect("extent fetch should succeed");
                criterion::black_box(extent);
            },
            BatchSize::SmallInput,
        )
    });

    c.bench_function("extent_fetch/warm_materialized_hit", |b| {
        let service = MetadataService::new(
            open_metadata_database(&database_path).expect("database should reopen"),
        );
        let entry = service
            .resolve_catalog_path(&sample_path.to_string_lossy())
            .expect("resolve should succeed")
            .expect("sample should exist");
        let extent_store = ServerExtentStore::new(&state_dir);
        let extent_ref = ExtentRef {
            file_id: entry.metadata.file_id,
            extent_index: 0,
            file_offset: 0,
            length: entry.extent_bytes.expect("extent size should be set"),
        };
        extent_store
            .fetch_extent(&entry, &extent_ref)
            .expect("warmup extent fetch should succeed");

        b.iter_batched(
            || (extent_store.clone(), entry.clone(), extent_ref),
            |(extent_store, entry, extent_ref)| {
                let extent = extent_store
                    .fetch_extent(&entry, &extent_ref)
                    .expect("warm fetch should succeed");
                criterion::black_box(extent);
            },
            BatchSize::SmallInput,
        )
    });
}

fn create_library_fixture(
    root: &std::path::Path,
    banks: usize,
    files_per_bank: usize,
    file_size: usize,
) {
    fs::create_dir_all(root).expect("library root should be created");
    for bank in 0..banks {
        let bank_dir = root.join(format!("bank-{bank:02}"));
        fs::create_dir_all(&bank_dir).expect("bank directory should be created");
        for file_index in 0..files_per_bank {
            let path = bank_dir.join(format!("sample-{file_index:04}.wav"));
            let payload = vec![(bank + file_index) as u8; file_size];
            fs::write(path, payload).expect("fixture file should be written");
        }
    }
}

criterion_group!(
    server_workloads,
    benchmark_library_scan,
    benchmark_cold_resolve_and_extent_fetch
);
criterion_main!(server_workloads);
