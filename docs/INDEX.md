# Documentation Index

This directory is the short-form map of the repository documentation.

## Root Docs

- [README.md](../README.md)
  Short overview of the repository, runtime pieces, and where to go next.
- [CLAUDE.md](../CLAUDE.md)
  Short working index for contributors or agents navigating the repo.
- [AGENTS.md](../AGENTS.md)
  Same working index for tools that read `AGENTS.md`.

## Repository Guides

- [REPO_STRUCTURE.md](REPO_STRUCTURE.md)
  High-level map of the workspace, crates, and where major responsibilities live.
- [DEVELOPMENT.md](DEVELOPMENT.md)
  Common build, test, lint, and benchmark commands plus a brief workflow summary.
- [architecture/SYSTEM_SHAPE.md](architecture/SYSTEM_SHAPE.md)
  Final system shape, technology choices, deployment model, and deliberate boundaries.
- [design/PROTOCOL_AND_BEHAVIOR.md](design/PROTOCOL_AND_BEHAVIOR.md)
  Behavioral contract for protocol, store replication, caching, prefetch, and failure handling.
- [design/TRANSFER_LAYOUT_AND_STORE_MODEL.md](design/TRANSFER_LAYOUT_AND_STORE_MODEL.md)
  Transfer classification, extent model, segment format, catalog model, and residency rules.

## Focused Reference Docs

- [deploy/OPERATIONS.md](../deploy/OPERATIONS.md)
  Local TrueNAS/Komodo deployment shape, client registration, and recovery notes.
- [platform.yml](../platform.yml)
  Shared Ahara CI manifest that declares stack, deploy shape, and Rust artifact outputs.
- [compose.yaml](../compose.yaml)
  Root Komodo/TrueNAS stack definition used by the shared deploy workflow.
- [deploy/client/README.md](../deploy/client/README.md)
  Packaging, install layout, and upgrade expectations for native clients.
- [crates/legato-proto/PROTO_VERSIONING.md](../crates/legato-proto/PROTO_VERSIONING.md)
  Protocol namespace and field-number rules.

## Reading Strategy

- Start with [REPO_STRUCTURE.md](REPO_STRUCTURE.md) if you need orientation.
- Read [architecture/SYSTEM_SHAPE.md](architecture/SYSTEM_SHAPE.md) for the final application shape.
- Read [design/TRANSFER_LAYOUT_AND_STORE_MODEL.md](design/TRANSFER_LAYOUT_AND_STORE_MODEL.md) before changing store or cache behavior.
- Read the code after that. These docs frame the workspace; they do not duplicate implementation detail.
