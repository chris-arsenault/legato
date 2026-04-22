//! Criterion benchmarks for the core MVP Legato server workloads.
#![allow(missing_docs)]

use std::fs;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use legato_proto::{BlockRequest, OpenRequest, ReadBlocksRequest};
use legato_server::{MetadataService, open_metadata_database, reconcile_library_root};
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

fn benchmark_cold_open_and_read(c: &mut Criterion) {
    let temp = tempdir().expect("tempdir should be created");
    let library_root = temp.path().join("library");
    create_library_fixture(&library_root, 12, 8, 256 * 1024);
    let sample_path = library_root.join("bank-00").join("sample-0000.wav");

    let database_path = temp.path().join("runtime.sqlite");
    let mut connection = open_metadata_database(&database_path).expect("database should open");
    reconcile_library_root(&mut connection, &library_root).expect("reconcile should succeed");

    c.bench_function("client_open/cold_metadata_open", |b| {
        b.iter_batched(
            || {
                MetadataService::new(
                    open_metadata_database(&database_path).expect("database should reopen"),
                )
            },
            |mut service| {
                let response = service
                    .open(OpenRequest {
                        path: sample_path.to_string_lossy().into_owned(),
                    })
                    .expect("open should succeed");
                criterion::black_box(response);
            },
            BatchSize::SmallInput,
        )
    });

    c.bench_function("read_blocks/playback_time_block_reads", |b| {
        b.iter_batched(
            || {
                let mut service = MetadataService::new(
                    open_metadata_database(&database_path).expect("database should reopen"),
                );
                let open = service
                    .open(OpenRequest {
                        path: sample_path.to_string_lossy().into_owned(),
                    })
                    .expect("open should succeed")
                    .expect("sample should exist");
                (service, open.file_handle)
            },
            |(service, file_handle)| {
                let blocks = service
                    .read_blocks(ReadBlocksRequest {
                        ranges: vec![BlockRequest {
                            file_handle,
                            start_offset: 0,
                            block_count: 4,
                        }],
                    })
                    .expect("read_blocks should succeed");
                criterion::black_box(blocks);
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
    benchmark_cold_open_and_read
);
criterion_main!(server_workloads);
