# System Shape

This document captures the application shape for Legato: a read-only, log-structured distributed filesystem specialized for sample-library workloads.

## Product Shape

Legato presents a TrueNAS-hosted sample library as a local read-only filesystem on macOS and Windows clients.

The useful workflow is:

- the canonical library is stored on the TrueNAS side in a Legato-managed store
- the server publishes catalog records and streams extent records
- clients keep partial local replicas on NVMe
- the mounted filesystem serves reads from local residency whenever possible
- project-aware prefetch warms the residency set before DAW reads become latency-sensitive

Legato is specialized for read-heavy sample playback. It is not a general-purpose writable distributed filesystem.

## Runtime Components

The system has three runtime components:

1. `legato-server`
2. `legatofs`
3. `legato-prefetch`

### `legato-server`

`legato-server` is the Rust service that runs on the TrueNAS side in a container.

Responsibilities:

- own the canonical Legato store
- ingest the library into file, directory, inode, and extent records
- classify file layouts for sample-library access patterns
- serve catalog resolution and extent fetches
- stream ordered catalog and content records to clients
- issue client TLS bundles
- expose metrics and structured logs

The server uses the TrueNAS dataset as durable storage for Legato segment and catalog files. It does not rely on SMB or NFS on the hot path.

### `legatofs`

`legatofs` is the native Rust client binary.

Responsibilities:

- mount the remote library as a local read-only filesystem
- maintain a local Legato store under the configured state directory
- track local extent residency in filesystem metadata
- serve cached reads from local segments
- fetch missing extents from the server
- replay ordered server records
- run cache repair, checkpointing, compaction, and eviction

The client store is a partial replica. It can contain metadata for more files than it has local data for.

### `legato-prefetch`

`legato-prefetch` owns project parsing and residency planning. The mounted client invokes it automatically when a supported project or preset is opened. The CLI remains available for manual analysis or explicit warm-up requests, but residency changes still flow through the mounted client runtime.

Responsibilities:

- parse project files and plugin state
- resolve referenced library paths
- walk inode extent maps
- compute prefetch priorities
- ask the local client runtime to make selected extents resident
- optionally wait until selected priorities are resident

`legato-prefetch` is shared logic with an optional CLI wrapper, not a standalone service. The mounted runtime remains the single writer for the local store.

## Deployment Shape

### Server Side

The server is deployed as a Docker container on the TrueNAS side and managed through Komodo.

Operational shape:

- one containerized Rust server
- read-only source-library mount when importing from an existing dataset
- read-write state mount for the canonical Legato store
- server-managed TLS material for mTLS

Suggested mount layout:

```text
/mnt/pool/libraries            -> /srv/libraries:ro
/mnt/pool/appdata/legato       -> /var/lib/legato
/mnt/pool/appdata/legato/tls   -> /etc/legato
```

Suggested server state layout:

```text
/var/lib/legato/
  segments/
  checkpoints/
```

The deployment is intentionally a single server process for the local TrueNAS workflow.

### Client Side

Clients run as native binaries.

Target platforms:

- macOS
- Windows

Default client state layout:

```text
<state-dir>/
  catalog/
  segments/
  checkpoints/
  certs/
  legatofs.toml
  prefetch-control.json
```

Linux is not a primary client target.

## Repository Shape

Legato is implemented as a Rust workspace with narrow crate boundaries.

Current crate roles:

- `legato-proto`
  RPC definitions, generated protobuf types, and protocol notes.
- `legato-types`
  Shared transport-neutral domain types.
- `legato-server`
  Server binary, canonical store access, ingest, extent fetches, record streaming, invalidations, watcher behavior, and benchmarks/tests.
- `legato-client-core`
  Shared client runtime, reconnect behavior, scheduling, invalidation handling, and local control-plane behavior.
- `legato-client-cache`
  Client-side Legato store primitives: segment records, catalog state, residency, checkpointing, repair, compaction, and eviction.
- `legato-fs-macos`
  macOS adapter surface.
- `legato-fs-windows`
  Windows adapter surface.
- `legatofs`
  Native client entrypoint binary.
- `legato-prefetch`
  CLI entrypoint and project-aware prefetch planning/parsing.
- `legato-foundation`
  Shared config, tracing, metrics, and runtime helpers.

## Technology Choices

### Language

Rust is used for all first-party components.

Reasons:

- shared implementation language across server and client
- good fit for network services, log-structured stores, binary parsing, and filesystem-adjacent code
- strong safety properties for code that handles local persistence and mount callbacks

### RPC And Transport

Legato uses gRPC over HTTP/2 with TLS 1.3 and mutual TLS.

Preferred libraries:

- `tonic`
- `prost`
- `rustls`

The protocol carries Legato filesystem records and extent references. The transport is typed, inspectable, and supported well in Rust.

### Persistence

Legato uses append-only segment files plus compacted catalog/checkpoint files.

Store files are ordinary host-filesystem files. Legato owns the semantic layout inside those files:

- records are append-only
- segment records are immutable after seal
- catalog updates are committed by appending and checkpointing
- recovery validates record hashes and replays from the last checkpoint

This gives the application a filesystem-specific persistence model without requiring a kernel filesystem or raw storage-device implementation.

### Filesystem Access And Change Tracking

The server can import from a mounted source dataset using ordinary filesystem access. The canonical served state is the Legato store.

Correctness model:

- import and reconciliation produce ordered Legato records
- subscriptions deliver ordered records to clients
- clients update local catalogs and residency state through replay

### Async Runtime

Legato uses `tokio`.

### Config, Logging, And Metrics

Legato uses:

- `serde`
- `toml`
- `tracing`
- Prometheus-compatible metrics

The server defaults toward structured JSON logs.

## Security Model

Legato uses mutual TLS between clients and server.

Authorization model:

- authenticated clients may read the library
- the DAW-facing mount is read-only
- client bundles are issued by the server

## Ownership Boundaries

Explicit ownership boundaries:

- the server owns the canonical Legato store
- clients own their local partial replicas and residency state
- `legato-prefetch` owns project parsing and prefetch planning
- platform adapters own only the mount integration surface

`legato-prefetch` talks to the local client runtime. Residency guarantees stay with the component that serves reads.

## Deliberate Boundaries

Legato does not target:

- write support through the DAW-facing mount
- POSIX-complete filesystem semantics
- multi-server catalog ownership
- built-in web UI
- plugin marketplace or third-party extension system
- generalized shared mutation workflows

## Explicit Trade-Offs

- Legato-managed store over host-file metadata:
  Chosen so metadata, extent data, residency, and recovery share one consistency model.
- User-space mount over kernel filesystem implementation:
  Chosen because FSKit/macFUSE and WinFSP provide the needed mount surface without kernel-level product scope.
- gRPC over raw socket protocol:
  Chosen for typed contracts and operational simplicity.
- Single server process over split services:
  Chosen to keep the local TrueNAS deployment debuggable.
- Read-only semantics over write support:
  Chosen because the target workflow is predictable sample-library playback.
