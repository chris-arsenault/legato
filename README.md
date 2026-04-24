# Legato

Legato is a Rust workspace for a read-only, log-structured sample-library filesystem. A TrueNAS-side server owns the canonical Legato store, macOS and Windows clients mount partial local replicas, and project-aware prefetch makes playback-time reads hit local NVMe whenever possible.

At a high level, this repo contains three runtime pieces:

- `legato-server`: the containerized TrueNAS-side daemon that serves the canonical store, publishes catalog records, streams extent records, and issues client bundles.
- `legatofs`: the native client binary that mounts the library, maintains the local Legato store, and serves reads from resident extents.
- `legato-prefetch`: the project-aware parser and prefetch engine used by mount-triggered project opens and optional manual tooling routed through the mounted client runtime.

## Contents

- [Documentation Index](docs/INDEX.md)
- [Repository Structure](docs/REPO_STRUCTURE.md)
- [Development Workflow](docs/DEVELOPMENT.md)
- [System Shape](docs/architecture/SYSTEM_SHAPE.md)
- [Protocol And Behavior](docs/design/PROTOCOL_AND_BEHAVIOR.md)
- [Transfer Layout And Store Model](docs/design/TRANSFER_LAYOUT_AND_STORE_MODEL.md)
- [Operations Runbook](deploy/OPERATIONS.md)
- [Observability Guide](deploy/OBSERVABILITY.md)
- [Client Installation Guide](deploy/client/README.md)
- [Protocol Versioning](crates/legato-proto/PROTO_VERSIONING.md)
- [Agent/Contributor Index](CLAUDE.md)

## Workspace Map

- `crates/legato-server`
  Server runtime, canonical store access, catalog publication, extent streaming, TLS bootstrap, integration tests, and benchmarks.
- `crates/legatofs`
  Native client entrypoint and mount bootstrap for macOS and Windows adapters.
- `crates/legato-prefetch`
  Project parsing and prefetch logic for ALS, NKI, and plugin-state inputs, plus the optional CLI wrapper that asks the mounted client runtime to warm residency.
- `crates/legato-client-core`
  Shared client runtime behavior, reconnect planning, prefetch scheduling, and local control-plane logic.
- `crates/legato-client-cache`
  Client-side Legato store primitives: segment records, catalog state, residency, checkpointing, repair, compaction, and eviction.
- `crates/legato-foundation`
  Shared config loading, tracing, metrics, and shutdown helpers.
- `crates/legato-proto`
  Protobuf definitions, generated bindings, and protocol notes.
- `crates/legato-types`
  Shared domain types used across the workspace.
- `crates/legato-fs-macos`
  macOS adapter surface for the native client.
- `crates/legato-fs-windows`
  Windows adapter surface for the native client.

## Start Here

- Read [docs/INDEX.md](docs/INDEX.md) for the documentation map.
- Read [docs/REPO_STRUCTURE.md](docs/REPO_STRUCTURE.md) to orient yourself before opening code.
- Read [docs/architecture/SYSTEM_SHAPE.md](docs/architecture/SYSTEM_SHAPE.md) for the application shape and trade-offs.
- Read [docs/design/TRANSFER_LAYOUT_AND_STORE_MODEL.md](docs/design/TRANSFER_LAYOUT_AND_STORE_MODEL.md) for the storage format and transfer model.
- Read [docs/design/PROTOCOL_AND_BEHAVIOR.md](docs/design/PROTOCOL_AND_BEHAVIOR.md) for the behavioral contract.
- Read [deploy/OPERATIONS.md](deploy/OPERATIONS.md) for deployment shape and operational commands.

## Scope

This repository targets a personal Legato deployment:

- a Rust server container served through Komodo on TrueNAS
- a read-only sample-library mount over TrueNAS-hosted data
- native macOS and Windows client binaries
- a log-structured Legato store on server and clients
- local NVMe-backed partial residency on clients
- project-aware prefetch planning for DAW workflows

The docs describe this local workflow and repository structure. They do not describe product packaging, multi-tenant hosting, or public SaaS deployment.
