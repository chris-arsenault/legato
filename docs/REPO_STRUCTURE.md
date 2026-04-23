# Repository Structure

This document describes the shape of the workspace so you can find the right code quickly.

## Top Level

- `Cargo.toml`
  Workspace manifest for all crates.
- `crates/`
  Rust crates that implement the server, client, protocol, store, and shared libraries.
- `deploy/`
  Deployment assets and operational documentation.
- `docs/`
  Architecture, behavior, and workflow documentation.

## Runtime Pieces

- `crates/legato-server`
  Server runtime, canonical Legato store access, catalog publication, extent streaming, invalidation fanout, TLS bootstrap, watcher handling, and server benchmarks.
- `crates/legatofs`
  Native client entrypoint. It wires config loading, telemetry bootstrap, local store setup, project-facing commands, and the platform adapter.
- `crates/legato-prefetch`
  Project-aware prefetch helper. It analyzes DAW and plugin state, resolves referenced library paths, and emits or executes prefetch hints.

## Shared Client Libraries

- `crates/legato-client-core`
  Client runtime logic, reconnect handling, prefetch scheduling, residency coordination, and local control-plane behavior.
- `crates/legato-client-cache`
  Client-side Legato store primitives: segment records, catalog state, residency, checkpointing, repair, compaction, and eviction.

## Shared Foundations

- `crates/legato-foundation`
  Cross-cutting config, tracing, metrics, and shutdown helpers.
- `crates/legato-types`
  Shared domain types and platform-neutral filesystem semantics.
- `crates/legato-proto`
  Protobuf schema, generated bindings, and protocol notes.

## Platform Adapters

- `crates/legato-fs-macos`
  macOS-facing adapter surface.
- `crates/legato-fs-windows`
  Windows-facing adapter surface.

## Tests And Benchmarks

- `crates/legato-server/tests/end_to_end.rs`
  Cross-crate integration coverage for server, client store/control plane, mount reads, and prefetch behavior.
- `crates/legato-server/benches/server_workloads.rs`
  Benchmark targets for library scan, catalog resolution, and semantic extent fetches.
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
- If you care about store layout, start with `docs/design/TRANSFER_LAYOUT_AND_STORE_MODEL.md`, then `legato-client-cache` and `legato-server`.
- If you care about server behavior, start in `legato-server`.
- If you care about client mount or recovery behavior, start in `legatofs`, then `legato-client-core`, then `legato-client-cache`.
- If you care about prefetch behavior, start in `legato-prefetch`, then follow calls into `legato-client-core`.
