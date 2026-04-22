# Legato Operations Runbook

This is a focused operations document. For the top-level project index, start at [README.md](../README.md) or [docs/INDEX.md](../docs/INDEX.md).

This document is the MVP deployment and recovery guide for running Legato as a Dockerized server on the TrueNAS side, with native clients mounting the library on macOS or Windows.

## Topology

- `legato-server` runs as a container, ideally scheduled by Komodo against the TrueNAS-hosted dataset.
- The server mounts the canonical read-only sample library at `/srv/libraries`.
- Server metadata and runtime state live under `/var/lib/legato`.
- TLS materials live under `/etc/legato/certs`.
- `legatofs` runs natively on client machines and maintains its SQLite/cache state locally.
- `legato-prefetch` can be invoked by the launcher or DAW helper flow before mount-time reads become latency-sensitive.

## TrueNAS + Komodo

1. Provision three persistent paths on the TrueNAS side:
   - Library dataset: mounted read-only into the container as `/srv/libraries`
   - State dataset: mounted read-write as `/var/lib/legato`
   - TLS secret/materials: mounted read-only as `/etc/legato/certs`
2. Use [compose.yaml](/home/dev/repos/legato/compose.yaml) as the Komodo workload source.
3. Keep `restart: unless-stopped` enabled so a TrueNAS or Docker daemon restart brings the service back automatically.
4. Set `LEGATO_SERVER__COMMON__TRACING__JSON=true` in container env for structured logs.
5. Expose the metrics port only on trusted networks if `common.metrics.bind_address` is configured.

For the current `apps` pool layout, a helper script is available at [create-legato-datasets.sh](/home/dev/repos/legato/deploy/truenas/create-legato-datasets.sh). It creates the Legato app datasets plus the SMB-ready `VST`, `samples`, and `kontakt` datasets under `/mnt/apps/shares/legato/`.

The canonical host-to-container mount mapping for that layout is:

- `/mnt/apps/shares/legato` -> `/srv/libraries` (read-only)
- `/mnt/apps/apps/legato` -> `/var/lib/legato`
- `/mnt/apps/apps/legato/config` -> `/etc/legato` (read-only)

Run it through `bash` on the TrueNAS host, not as a directly executed file from `/mnt/...`, because SCALE commonly applies execution restrictions to dataset-backed paths:

```bash
sudo bash deploy/truenas/create-legato-datasets.sh
```

Running `sudo ./deploy/truenas/create-legato-datasets.sh` can fail with a `sudo: process ... unexpected status 0x57f` error even though the script contents are valid.

## Server Configuration

The base example is [deploy/server/server.toml.example](/home/dev/repos/legato/deploy/server/server.toml.example).

- `server.bind_address`: gRPC/control-plane listener
- `server.library_root`: mounted sample-library root
- `server.state_dir`: SQLite metadata and watcher state
- `server.tls.*`: mTLS certificate chain and client CA
- `common.metrics.bind_address`: optional Prometheus scrape endpoint

Recommended overrides for production:

- Keep `common.tracing.level=info` by default and raise to `debug` only during incident work.
- Use a dedicated client CA for Legato mounts rather than a shared internal PKI root.
- Keep the library mount read-only inside the container so the server cannot mutate the sample dataset.

## Client Rollout

- macOS default state dir: `/Library/Application Support/Legato`
- Windows default state dir: `C:\ProgramData\Legato`
- macOS default mount point: `/Volumes/Legato`
- Windows default mount point: `L:\Legato`

Client validation flow:

1. Install the native binary.
2. Provide client certificate, key, and server CA.
3. Start the mount agent.
4. Verify the mount root appears and resolves indexed paths.
5. Run `legato-prefetch analyze <project>` against one representative session.

## Observability

Each binary now exposes:

- structured tracing via `init_tracing`
- process startup and lifecycle metrics
- optional Prometheus exposition when `common.metrics.bind_address` is set

Suggested starter alerts:

- process not scraped for 5m
- repeated reconnect cycles on a client
- cache repair activity above a low steady-state baseline
- library scan duration regression after dataset changes

## Cache Repair And Recovery

Client-side cache maintenance now supports:

- integrity verification on block read
- orphan/corrupt entry repair
- size-based eviction using pin generation and last access ordering

Operational recovery steps:

1. Stop the client service if corruption is suspected.
2. Run the client with cache-maintenance tooling once it exists as a surfaced command, or remove only the local cache state directory if a clean rebuild is faster.
3. Restart the client and verify reconnect/open/prefetch flow.
4. If the server restarted, expect a root invalidation and stale-handle reopen cycle on the client side.

## Benchmarking

The benchmark suite currently focuses on the three workloads that matter for the MVP:

- full library reconciliation scan
- cold metadata open
- playback-time aligned block reads

Run:

```bash
PATH=/usr/local/rustup/toolchains/stable-x86_64-unknown-linux-gnu/bin:$PATH cargo bench -p legato-server
```

Use the same dataset shape over time so comparisons remain meaningful.

## Integration Validation

Cross-crate integration coverage lives in [end_to_end.rs](/home/dev/repos/legato/crates/legato-server/tests/end_to_end.rs).

Run:

```bash
PATH=/usr/local/rustup/toolchains/stable-x86_64-unknown-linux-gnu/bin:$PATH cargo test -p legato-server --test end_to_end
```

Those tests verify:

- indexed server metadata can feed the client-side prefetch path
- `legato-prefetch` analysis hints can drive a real prefetch execution against server reads

## Upgrade Sequence

1. Push the new server image.
2. Restart the container under Komodo.
3. Confirm metrics and structured logs are healthy.
4. Let clients reconnect and reopen stale handles automatically.
5. Roll client binaries after the server is stable, not before.

## Failure Checklist

- Verify TLS material paths and certificate freshness first.
- Confirm the library dataset is mounted at the expected container path.
- Check that the server metadata DB is writable and not out of disk space.
- Scrape `/metrics` before restarting a process so you preserve incident clues.
- If read latency regresses, run the benchmark suite and compare against prior scan/open/read baselines.

## Shared CI And Publish Shape

Legato now exposes the Ahara shared CI/CD entrypoints at the repo root:

- [platform.yml](/home/dev/repos/legato/platform.yml)
- [Dockerfile](/home/dev/repos/legato/Dockerfile)
- [compose.yaml](/home/dev/repos/legato/compose.yaml)
- [secret-paths.yml](/home/dev/repos/legato/secret-paths.yml)
- [.github/workflows/ci.yml](/home/dev/repos/legato/.github/workflows/ci.yml)

The root `Dockerfile` is intentionally packaging-only. CI builds the Rust binary first, places it in `dist/`, and Docker only assembles the runtime image.
The root `compose.yaml` is the only supported Komodo stack definition. The older nested deploy entrypoints were removed to avoid split deployment paths.
