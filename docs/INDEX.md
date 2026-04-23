# Documentation Index

This directory is the short-form map of the repository documentation.

## Root Docs

- [README.md](../README.md)
  Short overview of what the repository is, what is in it, and where to go next.
- [CLAUDE.md](../CLAUDE.md)
  Short working index for contributors or agents navigating the repo.

## Repository Guides

- [REPO_STRUCTURE.md](REPO_STRUCTURE.md)
  High-level map of the workspace, crates, and where major responsibilities live.
- [DEVELOPMENT.md](DEVELOPMENT.md)
  Common build, test, lint, and benchmark commands plus a brief workflow summary.
- [architecture/SYSTEM_SHAPE.md](architecture/SYSTEM_SHAPE.md)
  Final system shape, technology choices, deployment model, and deliberate non-goals.
- [design/PROTOCOL_AND_BEHAVIOR.md](design/PROTOCOL_AND_BEHAVIOR.md)
  Final behavioral contract for protocol, caching, prefetch, and failure handling.
- [design/TRANSFER_LAYOUT_AND_STORE_MODEL.md](design/TRANSFER_LAYOUT_AND_STORE_MODEL.md)
  Transfer classification, extent model, and store-shape rules.

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
  Wire-compatibility and protobuf namespace rules.

## Reading Strategy

- Start with [REPO_STRUCTURE.md](REPO_STRUCTURE.md) if you need orientation.
- Move to the crate or deployment document closest to your area of interest.
- Read the code after that. The docs here are meant to frame the workspace, not to duplicate implementation detail.
