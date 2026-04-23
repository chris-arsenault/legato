# System Shape

This document captures the intended application shape for Legato. It focuses on the settled runtime structure, core technology choices, and intentional boundaries.

## Product Shape

Legato is a read-only, project-aware remote sample library system:

- the canonical library stays on the TrueNAS-side dataset
- the server indexes and serves that library
- clients mount it as a local read-only filesystem
- clients keep a local NVMe-backed cache
- project-aware prefetch warms the client cache before DAW reads become latency-sensitive

Legato is intentionally shaped as a focused product rather than a general distributed filesystem.

## Runtime Components

The system has three runtime components:

1. `legato-server`
2. `legatofs`
3. `legato-prefetch`

### `legato-server`

`legato-server` is the Rust service that runs on the TrueNAS side in a container. Its responsibilities are:

- indexing the library dataset
- resolving metadata and canonical paths
- serving semantic file extents
- emitting invalidations when library content changes
- exposing metrics and structured logs

It does not mount filesystems, and it does not depend on SMB or NFS on the hot path.

### `legatofs`

`legatofs` is the native Rust client binary. Its responsibilities are:

- mounting the remote library as a local read-only filesystem
- maintaining the local metadata and extent cache
- serving reads from cache whenever possible
- fetching cache misses from the server
- subscribing to invalidations
- owning cache residency and correctness

### `legato-prefetch`

`legato-prefetch` is the native CLI used before launching the DAW. Its responsibilities are:

- parsing project files and plugin state
- resolving referenced library paths
- computing prefetch ranges and priorities
- asking the local client runtime to prefetch those ranges
- optionally waiting until selected priorities are resident

`legato-prefetch` is a binary integration point, not a standalone service.

## Deployment Shape

### Server Side

The server is deployed as a Docker container on the TrueNAS side and managed through Komodo.

The operational model is:

- one containerized Rust server
- read-only mount of the canonical library dataset
- read-write mount for local application state
- server-managed TLS material for mTLS

Suggested mount layout:

```text
/mnt/pool/libraries            -> /srv/libraries:ro
/mnt/pool/appdata/legato       -> /var/lib/legato
/mnt/pool/appdata/legato/tls   -> /var/lib/legato/tls
```

Suggested server state layout:

```text
/var/lib/legato/
  server.sqlite
  config/
  tmp/
```

The initial deployment shape is intentionally a single container. Legato does not split API, indexing, or background work into separate services unless a concrete bottleneck justifies it.

### Client Side

Clients run as native binaries, not containers.

Initial target platforms are:

- macOS
- Windows

Linux is not a priority target.

## Repository Shape

Legato is implemented as a Rust workspace with narrow crate boundaries.

Current crate roles:

- `legato-proto`
  RPC definitions, generated protobuf types, and protocol compatibility rules.
- `legato-types`
  Shared transport-neutral domain types.
- `legato-server`
  Server binary, metadata/index handling, extent fetches, invalidations, watcher behavior, and benchmarks/tests.
- `legato-client-core`
  Shared client runtime, reconnect behavior, scheduling, invalidation handling, and local control-plane behavior.
- `legato-client-cache`
  Client-side extent metadata, local extent storage, repair, and eviction logic.
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

The reasons are practical:

- shared implementation language across server and client
- good fit for network services, caches, and binary parsing
- strong safety properties for filesystem-adjacent code

### RPC And Transport

Legato uses gRPC over HTTP/2 with TLS 1.3 and mutual TLS.

Preferred libraries:

- `tonic`
- `prost`
- `rustls`

This is a typed, inspectable transport with mature Rust support and good streaming behavior.

### Persistence

Legato uses SQLite for both:

- server-local metadata/state
- client-local cache metadata/state

Server-side reasons:

- the metadata index is local to one server instance
- operational overhead stays low
- read-heavy local metadata fits SQLite well

Client-side reasons:

- cache metadata is machine-local
- SQLite is reliable, embedded, and easy to inspect during debugging

SQLite is intentional here. The first version does not depend on shared Postgres infrastructure.

### Local Extent Storage

Client cached extents are stored as local files on disk, with SQLite tracking metadata and residency state.

This keeps verification and repair straightforward.

### Filesystem Access And Change Tracking

The server reads from the mounted dataset using ordinary filesystem access.

The correctness model is:

- filesystem notifications accelerate freshness
- reconciliation preserves correctness

Legato does not assume notifications are perfect.

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

The first version uses mutual TLS between clients and server.

The authorization model is intentionally simple:

- authenticated clients may read the library
- fine-grained ACLs are not part of the first version

## Ownership Boundaries

The system has explicit ownership boundaries:

- the TrueNAS dataset is canonical content
- `legato-server` owns server metadata and indexing state
- `legatofs` owns the local metadata cache and extent cache
- `legato-prefetch` owns project parsing and prefetch planning only

`legato-prefetch` talks to the local client runtime, not directly to the server. Cache ownership and residency guarantees stay in one place.

## Deliberate Non-Goals

These are intentional exclusions from the first implementation:

- write support
- POSIX-complete filesystem semantics
- multi-server clustering
- shared external database state
- built-in web UI
- plugin ecosystem
- generalized distributed cache coordination

## Explicit Trade-Offs

- SQLite over Postgres:
  Chosen to minimize operational complexity for server-local and client-local state.
- Native clients over containerized clients:
  Chosen because filesystem integration and local cache management are OS-specific.
- gRPC over a custom wire protocol:
  Chosen for typed contracts and simpler implementation unless a measured bottleneck proves otherwise.
- Single server process over split services:
  Chosen to keep deployment and debugging simple for a local single-user deployment.
- Read-only semantics over write support:
  Chosen because the useful workflow is fast, predictable library reads rather than shared mutation workflows.
