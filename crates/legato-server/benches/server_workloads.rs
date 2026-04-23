//! Criterion benchmarks for the extent-oriented Legato server workloads.
#![allow(missing_docs)]

use std::fs;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use legato_client_cache::catalog::CatalogStore;
use legato_server::reconcile_library_root_to_store;
use tempfile::tempdir;

fn benchmark_library_scan(c: &mut Criterion) {
    let temp = tempdir().expect("tempdir should be created");
    let library_root = temp.path().join("library");
    create_library_fixture(&library_root, 24, 16, 32 * 1024);

    c.bench_function("library_scan/reconcile_full_tree", |b| {
        b.iter_batched(
            || {
                let state_dir = temp.path().join("scan-store");
                let _ = fs::remove_dir_all(&state_dir);
                state_dir
            },
            |state_dir| {
                let stats = reconcile_library_root_to_store(&state_dir, &library_root)
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

    reconcile_library_root_to_store(&state_dir, &library_root).expect("reconcile should succeed");

    c.bench_function("client_resolve/cold_catalog_resolve", |b| {
        b.iter_batched(
            || CatalogStore::open(&state_dir, 0).expect("catalog should reopen"),
            |catalog| {
                let response = catalog.resolve_path(sample_path.to_string_lossy().as_ref());
                criterion::black_box(response);
            },
            BatchSize::SmallInput,
        )
    });

    c.bench_function("extent_fetch/canonical_segment_read", |b| {
        b.iter_batched(
            || {
                let catalog = CatalogStore::open(&state_dir, 0).expect("catalog should reopen");
                let inode = catalog
                    .resolve_path(sample_path.to_string_lossy().as_ref())
                    .expect("sample should exist");
                let extent = inode.extents.first().expect("extent should exist").clone();
                (catalog, extent)
            },
            |(catalog, extent)| {
                let data = catalog
                    .read_extent_payload(&extent)
                    .expect("warm fetch should succeed");
                criterion::black_box(data);
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
