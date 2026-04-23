# Legato Operations Runbook

This is a focused operations document. For the top-level project index, start at [README.md](../README.md) or [docs/INDEX.md](../docs/INDEX.md).

This document is the deployment and recovery guide for running Legato as a Dockerized server on the TrueNAS side, with native clients mounting the library on macOS or Windows.

## Topology

- `legato-server` runs as a container, scheduled by Komodo against TrueNAS-hosted datasets.
- The server can import source library content from `/srv/libraries`.
- The canonical Legato store lives under `/var/lib/legato`.
- TLS materials live under `/etc/legato/certs`.
- `legatofs` runs natively on client machines and maintains a local Legato store under the configured state root.
- Supported project and preset opens trigger integrated prefetch through the mounted client. `legato-prefetch` remains available as optional manual tooling, but it requests warm-up through the mounted runtime rather than acting as a second cache writer.

## TrueNAS + Komodo

1. Provision three persistent paths on the TrueNAS side:
   - Library dataset: mounted read-only into the container as `/srv/libraries`
   - State dataset: mounted read-write as `/var/lib/legato`
   - Config/TLS dataset: mounted at `/etc/legato`
2. Use [compose.yaml](/home/dev/repos/legato/compose.yaml) as the Komodo workload source.
3. Keep `restart: unless-stopped` enabled so a TrueNAS or Docker daemon restart brings the service back automatically.
4. Set `LEGATO_SERVER__COMMON__TRACING__JSON=true` in container env for structured logs.
5. Expose the metrics port only on trusted networks if `common.metrics.bind_address` is configured.
6. Run the container as the same numeric UID/GID that owns the mounted datasets. The compose file defaults to `42173:42173`; override `LEGATO_UID` and `LEGATO_GID` in Komodo if your TrueNAS-side owner differs.

For the `apps` pool layout, a helper script is available at [create-legato-datasets.sh](/home/dev/repos/legato/deploy/truenas/create-legato-datasets.sh). It creates the Legato app datasets plus the SMB-ready `VST`, `samples`, and `kontakt` datasets under `/mnt/apps/shares/legato/`.

The canonical host-to-container mount mapping for that layout is:

- `/mnt/apps/shares/legato` -> `/srv/libraries` (read-only)
- `/mnt/apps/apps/legato` -> `/var/lib/legato`
- `/mnt/apps/apps/legato/config` -> `/etc/legato`

The compose stack runs `legato-server` as `${LEGATO_UID}:${LEGATO_GID}` so the process can read and write mounted datasets without relying on the image's baked-in default UID.

## TLS And Client Bundles

On first boot, `legato-server` generates its local CA and listener certificate under `/etc/legato/certs` if they do not already exist.

Server-managed TLS files:

- `server.pem`
- `server-key.pem`
- `client-ca.pem`
- `server-ca.pem`
- `server-ca-key.pem`

Client registration is handled by the server binary:

```bash
docker exec legato-server legato-server issue-client \
  --name studio-mac \
  --output-dir /tmp/studio-mac \
  --endpoint legato.lan:7823 \
  --server-name legato.lan
```

That writes:

- `client.pem`
- `client-key.pem`
- `server-ca.pem`
- `bundle.json`

Install the bundle on the client:

```bash
legatofs install --bundle-dir /tmp/studio-mac
```

For Windows:

```powershell
legatofs.exe install `
  --bundle-dir C:\Temp\studio-win
```

Override flags remain supported:

```bash
legatofs install \
  --bundle-dir /tmp/studio-mac \
  --mount-point /Volumes/Legato-Alt \
  --force
```

## Server Configuration

The base example is [deploy/server/server.toml.example](/home/dev/repos/legato/deploy/server/server.toml.example).

- `server.bind_address`: gRPC/control-plane listener
- `server.library_root`: mounted sample-library import root
- `server.state_dir`: canonical Legato store root
- `server.tls.*`: mTLS certificate chain and client CA
- `common.metrics.bind_address`: optional Prometheus scrape endpoint

Recommended defaults:

- Keep `common.tracing.level=info` by default and raise to `debug` only during incident work.
- Keep the source library mount read-only inside the container.

## Server State Layout

Under `/var/lib/legato`, the expected durable layout is:

- `catalog/`
  Current catalog checkpoints and compacted catalog files.
- `segments/`
  Append-only segment files containing canonical records.
- `checkpoints/`
  Recovery boundaries and replay metadata.
- `tmp/`
  Temporary files used during compaction and atomic replacement.

Under `/etc/legato/certs`, the expected durable layout is:

- `server.pem`
- `server-key.pem`
- `client-ca.pem`
- `server-ca.pem`
- `server-ca-key.pem`

## Client Rollout

- macOS default state dir: `/Library/Application Support/Legato`
- Windows default state dir: `C:\ProgramData\Legato`
- macOS default mount point: `/Volumes/Legato`
- Windows default mount point: `L:\Legato`

Client setup flow:

1. Install the native binary.
2. Issue a bundle from the server with `legato-server issue-client`.
3. Run `legatofs install` with the issued bundle.
4. Start the mount agent.
5. Verify the mount root appears and resolves indexed paths.
6. Open one representative project or preset through the mounted filesystem and confirm its referenced sample content becomes resident. Use `legato-prefetch run <mounted-project-path> --config <path-to-legatofs.toml>` only as an explicit diagnostic or manual warm-up request while the mount agent is running.

Replacement flow:

1. Reissue the client bundle on the server for the same logical client name.
2. Transfer the new bundle to the client host.
3. Reinstall with `legatofs install --bundle-dir <bundle> --force`.
4. Restart the native client runtime.

## Client State Layout

Under the chosen client state root, the expected durable layout is:

- `catalog/`
  Path, inode, directory, extent-map, residency, and subscription-cursor state.
- `segments/`
  Local append-only segment files containing resident records.
- `checkpoints/`
  Recovery boundaries and replay metadata.
- `certs/`
  Issued client bundle content.
- `legatofs.toml`
  Generated client config.

## Observability

Each binary exposes:

- structured tracing via `init_tracing`
- process startup and lifecycle metrics
- optional Prometheus exposition when `common.metrics.bind_address` is set

Suggested starter alerts:

- process not scraped for 5m
- repeated reconnect cycles on a client
- record subscription lag
- segment compaction failures
- catalog replay duration regression

## Store Repair And Recovery

Client-side maintenance supports:

- segment hash verification
- tail truncation for incomplete records
- catalog replay from checkpoints
- compaction of low-utility segments
- size-based eviction using pin state, priority, last access, and segment utility

Operational recovery steps:

1. Stop the client service if corruption is suspected.
2. Run the client repair command when available.
3. If a local partial replica is not worth repairing, remove the client state root and allow it to rebuild from the server.
4. Restart the client and verify resolve, fetch, subscribe, and prefetch flow.

## Benchmarking

Benchmark targets focus on local tuning:

- full library ingest or reconciliation scan
- catalog resolution
- semantic extent fetches
- segment replay and compaction

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

Those tests cover server, client runtime, prefetch, mounted reads, and restart behavior.

## Upgrade Sequence

1. Push the new server image.
2. Restart the container under Komodo.
3. Confirm metrics and structured logs are healthy.
4. Let clients reconnect and resume record subscription.
5. Roll client binaries after the server is stable.

## Failure Checklist

- Verify TLS material paths and certificate freshness first.
- Confirm the source library dataset is mounted at the expected container path.
- Check that the server store root is writable and not out of disk space.
- Scrape `/metrics` before restarting a process so incident clues are preserved.
- If read latency regresses, compare catalog, extent-fetch, and compaction benchmarks.

## Shared CI And Publish Shape

Legato exposes the Ahara shared CI/CD entrypoints at the repo root:

- [platform.yml](/home/dev/repos/legato/platform.yml)
- [Dockerfile](/home/dev/repos/legato/Dockerfile)
- [compose.yaml](/home/dev/repos/legato/compose.yaml)
- [secret-paths.yml](/home/dev/repos/legato/secret-paths.yml)
- [.github/workflows/ci.yml](/home/dev/repos/legato/.github/workflows/ci.yml)

The root `Dockerfile` is packaging-only. CI builds the Rust binary first, places it in `dist/`, and Docker assembles the runtime image.
The root `compose.yaml` is the supported Komodo stack definition.
