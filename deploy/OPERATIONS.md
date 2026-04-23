# Legato Operations Runbook

This is a focused operations document. For the top-level project index, start at [README.md](../README.md) or [docs/INDEX.md](../docs/INDEX.md).

This document is the deployment and recovery guide for running Legato as a Dockerized server on the TrueNAS side, with native clients mounting the library on macOS or Windows.

## Topology

- `legato-server` runs as a container, ideally scheduled by Komodo against the TrueNAS-hosted dataset.
- The server mounts the canonical read-only sample library at `/srv/libraries`.
- Server metadata and runtime state live under `/var/lib/legato`.
- TLS materials live under `/etc/legato/certs`.
- `legatofs` runs natively on client machines and maintains its SQLite metadata plus local extent store under the configured state root.
- `legato-prefetch` can be invoked by the launcher or DAW helper flow before mount-time reads become latency-sensitive.

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

For the current `apps` pool layout, a helper script is available at [create-legato-datasets.sh](/home/dev/repos/legato/deploy/truenas/create-legato-datasets.sh). It creates the Legato app datasets plus the SMB-ready `VST`, `samples`, and `kontakt` datasets under `/mnt/apps/shares/legato/`.

The canonical host-to-container mount mapping for that layout is:

- `/mnt/apps/shares/legato` -> `/srv/libraries` (read-only)
- `/mnt/apps/apps/legato` -> `/var/lib/legato`
- `/mnt/apps/apps/legato/config` -> `/etc/legato`

The compose stack also runs `legato-server` as `${LEGATO_UID}:${LEGATO_GID}` so the process can read and write those mounted datasets without relying on the image's baked-in default UID.

On first boot, `legato-server` now generates its own local CA and listener certificate under `/etc/legato/certs` if they do not already exist. You do not need to pre-stage:

- `server.pem`
- `server-key.pem`
- `client-ca.pem`
- `server-ca.pem`
- `server-ca-key.pem`

Client registration is handled by the server binary itself. To issue a client bundle after the server has bootstrapped its CA:

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

`bundle.json` carries the supported install-time defaults for endpoint and TLS server name, so the client install no longer needs those values typed manually unless you intentionally want to override them.

You can then copy that bundle to the client machine and install it with `legatofs` itself instead of manually placing certs and writing config:

```bash
legatofs install --bundle-dir /tmp/studio-mac
```

For Windows, pass a Windows state or mount path as needed:

```powershell
legatofs.exe install `
  --bundle-dir C:\Temp\studio-win
```

Override flags remain supported when the issued bundle metadata should not be used as-is:

```bash
legatofs install \
  --bundle-dir /tmp/studio-mac \
  --mount-point /Volumes/Legato-Alt \
  --force
```

The install command creates:

- `legatofs.toml`
- `certs/server-ca.pem`
- `certs/client.pem`
- `certs/client-key.pem`
- `extents/`

under the platform-default state directory unless `--state-dir` is explicitly provided.

Run it through `bash` on the TrueNAS host, not as a directly executed file from `/mnt/...`, because SCALE commonly applies execution restrictions to dataset-backed paths:

```bash
sudo bash deploy/truenas/create-legato-datasets.sh
```

Running `sudo ./deploy/truenas/create-legato-datasets.sh` can fail with a `sudo: process ... unexpected status 0x57f` error even though the script contents are valid.

## Server Configuration

The base example is [deploy/server/server.toml.example](/home/dev/repos/legato/deploy/server/server.toml.example).

- `server.bind_address`: gRPC/control-plane listener
- `server.library_root`: mounted sample-library root
- `server.state_dir`: server catalog database, extent materialization state, and recovery metadata
- `server.tls.*`: mTLS certificate chain and client CA
- `common.metrics.bind_address`: optional Prometheus scrape endpoint

Recommended overrides for production:

- Keep `common.tracing.level=info` by default and raise to `debug` only during incident work.
- Use a dedicated client CA for Legato mounts rather than a shared internal PKI root.
- Keep the library mount read-only inside the container so the server cannot mutate the sample dataset.

## Server State Layout

Under `/var/lib/legato`, the expected durable layout is:

- `server.sqlite`
  Catalog metadata, ordered change records, and server-side runtime state.
- `extents/`
  Materialized extent artifacts used by the semantic fetch path.
- future checkpoint or maintenance artifacts alongside the database as the store model expands.

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

Client validation flow:

1. Install the native binary.
2. Issue a bundle from the server with `legato-server issue-client`.
3. Run `legatofs install` with the issued bundle.
4. Start the mount agent.
5. Verify the mount root appears and resolves indexed paths.
6. Run `legato-prefetch analyze <project>` against one representative session.

Replacement flow:

1. Reissue the client bundle on the server for the same logical client name.
2. Transfer the new bundle to the client host.
3. Reinstall with `legatofs install --bundle-dir <bundle> --force`.
4. Restart the native client runtime.

## Client State Layout

Under the chosen client state root, the expected durable layout is:

- `client.sqlite`
  Metadata cache state, extent residency metadata, pin state, and recovery checkpoints.
- `extents/`
  Verified local extent files served by the mounted read path.
- `certs/`
  Issued client bundle content.
- `legatofs.toml`
  Generated client config.

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

- integrity verification on extent read
- orphan/corrupt entry repair
- size-based eviction using pin generation, fetch utility, and last access ordering
- startup recovery with checkpointing and metadata compaction

Operational recovery steps:

1. Stop the client service if corruption is suspected.
2. Use surfaced client maintenance tooling once it exists, or remove only the local client state directory if a clean rebuild is faster.
3. Restart the client and verify reconnect/open/prefetch flow.
4. If the server restarted, expect a root invalidation and stale-handle reopen cycle on the client side.

## Benchmarking

The benchmark suite currently focuses on the three workloads that matter for the current v1 store model:

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
