# Repository Structure

This document describes the shape of the workspace so you can find the right code quickly.

## Top Level

- `Cargo.toml`
  Workspace manifest for all crates.
- `crates/`
  Rust crates that implement the server, client, protocol, and shared libraries.
- `deploy/`
  Deployment assets and operational documentation.

## Runtime Pieces

- `crates/legato-server`
  The server-side implementation. This is where library indexing, metadata APIs, extent fetches, invalidation fanout, TLS bootstrap, and watcher handling live.
- `crates/legatofs`
  The native client entrypoint. It wires together config loading, telemetry bootstrap, local cache/control-plane setup, and the platform adapter.
- `crates/legato-prefetch`
  The prefetch helper. It analyzes DAW or plugin-state inputs and emits or executes prefetch hints.

## Shared Client Libraries

- `crates/legato-client-core`
  Client runtime logic, reconnect handling, prefetch scheduling, and local control-plane behavior.
- `crates/legato-client-cache`
  SQLite-backed cache metadata, extent storage, cache verification, eviction, and repair support.

## Shared Foundations

- `crates/legato-foundation`
  Cross-cutting config, tracing, metrics, and shutdown helpers.
- `crates/legato-types`
  Shared domain types and platform-neutral error or filesystem semantics.
- `crates/legato-proto`
  Protobuf schema, generated bindings, and compatibility rules.

## Platform Adapters

- `crates/legato-fs-macos`
  macOS-facing adapter surface.
- `crates/legato-fs-windows`
  Windows-facing adapter surface.

## Tests And Benchmarks

- `crates/legato-server/tests/end_to_end.rs`
  Cross-crate integration coverage for the server, client cache/control plane, and prefetch path.
- `crates/legato-server/benches/server_workloads.rs`
  Benchmark targets for full scan, cold open, and semantic extent fetches.
- Unit tests live alongside implementation files inside each crate.

## Deployment Docs

- `deploy/server/server.toml.example`
  Example server configuration for the runtime inside the deployed container.
- `deploy/client/README.md`
  Packaging expectations for native clients.
- `deploy/OPERATIONS.md`
  Operational overview for deployment, observability, and recovery.

## Practical Navigation

- If you care about wire shape, start in `legato-proto`.
- If you care about server behavior, start in `legato-server`.
- If you care about client mount or recovery behavior, start in `legatofs`, then `legato-client-core`, then `legato-client-cache`.
- If you care about prefetch behavior, start in `legato-prefetch`, then follow calls into `legato-client-core`.
