//! Criterion benchmarks for the extent-oriented Legato server workloads.
#![allow(missing_docs)]

use std::{fs, path::Path};

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use legato_client_cache::catalog::CatalogStore;
use legato_client_cache::segment::scan_segment;
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
    let sample_path = "/bank-00/sample-0000.wav";
    let state_dir = temp.path().join("state");

    reconcile_library_root_to_store(&state_dir, &library_root).expect("reconcile should succeed");

    c.bench_function("client_resolve/cold_catalog_resolve", |b| {
        b.iter_batched(
            || CatalogStore::open(&state_dir, 0).expect("catalog should reopen"),
            |catalog| {
                let response = catalog.resolve_path(sample_path);
                criterion::black_box(response);
            },
            BatchSize::SmallInput,
        )
    });

    c.bench_function("extent_fetch/canonical_segment_read_indexed", |b| {
        b.iter_batched(
            || {
                let catalog = CatalogStore::open(&state_dir, 0).expect("catalog should reopen");
                let inode = catalog
                    .resolve_path(sample_path)
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

fn benchmark_warm_extent_lookup_indexed_vs_linear_scan(c: &mut Criterion) {
    let temp = tempdir().expect("tempdir should be created");
    let library_root = temp.path().join("library");
    create_streamed_lookup_fixture(&library_root);
    let state_dir = temp.path().join("state");

    reconcile_library_root_to_store(&state_dir, &library_root).expect("reconcile should succeed");
    let catalog = CatalogStore::open(&state_dir, 0).expect("catalog should reopen");
    let inode = catalog
        .resolve_path("/Samples/session-long.wav")
        .expect("sample should exist");
    let extent = inode
        .extents
        .last()
        .expect("streamed sample should include multiple extents")
        .clone();
    let segment_path = state_dir.join("segments").join(format!(
        "{:020}.lseg",
        extent.segment_id.expect("extent should be resident")
    ));
    assert!(
        inode.extents.len() >= 64,
        "fixture should generate enough extents to make linear scans visible"
    );

    c.bench_function("extent_fetch/indexed_last_extent_lookup", |b| {
        b.iter(|| {
            let payload = catalog
                .read_extent_payload(&extent)
                .expect("indexed warm lookup should succeed");
            criterion::black_box(payload);
        })
    });

    c.bench_function("extent_fetch/linear_scan_last_extent_lookup", |b| {
        b.iter(|| {
            let payload = linear_scan_extent_payload(&segment_path, &extent)
                .expect("linear scan warm lookup should succeed");
            criterion::black_box(payload);
        })
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

fn create_streamed_lookup_fixture(root: &Path) {
    fs::create_dir_all(root.join("Samples")).expect("samples root should be created");
    fs::write(
        root.join(".legato-layout.toml"),
        r#"
[policy]
unitary_max_bytes = 4096
streamed_extent_bytes = 65536
"#,
    )
    .expect("layout policy should be written");
    let payload = vec![0x5a; 8 * 1024 * 1024];
    fs::write(root.join("Samples").join("session-long.wav"), payload)
        .expect("streamed sample should be written");
}

fn linear_scan_extent_payload(
    segment_path: &Path,
    extent: &legato_client_cache::catalog::CatalogExtent,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let scan = scan_segment(segment_path)?;
    let record = scan
        .records
        .into_iter()
        .find(|record| Some(record.segment_offset) == extent.segment_offset)
        .ok_or_else(|| format!("record offset {:?} not found", extent.segment_offset))?;
    if record.payload_hash.as_slice() != extent.payload_hash.as_slice() {
        return Err(format!(
            "hash mismatch for linear scan at {:?}",
            extent.segment_offset
        )
        .into());
    }
    Ok(record.payload)
}

criterion_group!(
    server_workloads,
    benchmark_library_scan,
    benchmark_cold_resolve_and_extent_fetch,
    benchmark_warm_extent_lookup_indexed_vs_linear_scan
);
criterion_main!(server_workloads);
