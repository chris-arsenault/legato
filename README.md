# Legato

Legato is a Rust workspace for serving a read-only sample library from a TrueNAS-side server and exposing it to macOS or Windows clients through a native filesystem mount.

At a high level, this repo contains three things:

- `legato-server`: the Docker-friendly server that indexes the library, serves metadata and blocks, and emits invalidations.
- `legatofs`: the native client binary that owns local cache state and mounts the remote library on end-user machines.
- `legato-prefetch`: the project-aware helper that analyzes DAW or plugin state and warms the client cache before reads become latency-sensitive.

## Contents

- [Project Overview](docs/INDEX.md)
- [Repository Structure](docs/REPO_STRUCTURE.md)
- [Development Workflow](docs/DEVELOPMENT.md)
- [System Shape](docs/architecture/SYSTEM_SHAPE.md)
- [Protocol And Behavior](docs/design/PROTOCOL_AND_BEHAVIOR.md)
- [Operations Runbook](deploy/OPERATIONS.md)
- [Client Packaging Notes](deploy/client/README.md)
- [Protocol Versioning](crates/legato-proto/PROTO_VERSIONING.md)
- [Agent/Contributor Index](CLAUDE.md)

## Workspace Map

- `crates/legato-server`
  High-level server runtime, metadata database, reconciliation, watcher handling, TLS bootstrap, integration tests, and benchmarks.
- `crates/legatofs`
  Native client entrypoint and mount bootstrap for macOS and Windows adapters.
- `crates/legato-prefetch`
  CLI and parsing logic for ALS, NKI, and plugin-state driven prefetch planning.
- `crates/legato-client-core`
  Shared client runtime behavior, reconnect planning, prefetch scheduling, and local control-plane logic.
- `crates/legato-client-cache`
  SQLite-backed cache metadata, block storage, repair, and eviction primitives.
- `crates/legato-foundation`
  Shared config loading, tracing, metrics, and shutdown helpers.
- `crates/legato-proto`
  Protobuf definitions, generated bindings, and wire-compatibility notes.
- `crates/legato-types`
  Shared domain types used across the workspace.
- `crates/legato-fs-macos`
  macOS adapter surface for the native client.
- `crates/legato-fs-windows`
  Windows adapter surface for the native client.

## Start Here

- Read [docs/INDEX.md](docs/INDEX.md) for the documentation map.
- Read [docs/REPO_STRUCTURE.md](docs/REPO_STRUCTURE.md) if you want to orient yourself in the workspace before opening code.
- Read [docs/architecture/SYSTEM_SHAPE.md](docs/architecture/SYSTEM_SHAPE.md) if you want the final application shape and intentional trade-offs.
- Read [docs/design/PROTOCOL_AND_BEHAVIOR.md](docs/design/PROTOCOL_AND_BEHAVIOR.md) if you want the final behavioral contract without the exploratory design notes.
- Read [deploy/OPERATIONS.md](deploy/OPERATIONS.md) if you care about deployment shape, runtime topology, or operational concerns.
- Read [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) if you want the common build, test, lint, and benchmark commands.

## Current Scope

This repository currently documents and implements the MVP shape:

- a Rust workspace
- a SQLite-backed metadata and client-cache model
- a Docker-oriented server deployment shape
- native macOS and Windows client entrypoints
- project-aware prefetch planning
- integration coverage and benchmark targets for the core flows

The documentation here is meant to help you navigate the repo and understand its shape. It is not intended to replace reading the code in the crates that implement each subsystem.
