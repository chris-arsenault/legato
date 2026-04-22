# Documentation Index

This directory is the short-form map of the repository documentation.

## Root Docs

- [README.md](../README.md)
  Public-facing overview of what the repository is, what is in it, and where to go next.
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

## Focused Reference Docs

- [deploy/OPERATIONS.md](../deploy/OPERATIONS.md)
  Deployment shape, runtime topology, observability, and recovery guidance.
- [deploy/client/README.md](../deploy/client/README.md)
  Packaging, install layout, and upgrade expectations for native clients.
- [crates/legato-proto/PROTO_VERSIONING.md](../crates/legato-proto/PROTO_VERSIONING.md)
  Wire-compatibility and protobuf namespace rules.

## Reading Strategy

- Start with [REPO_STRUCTURE.md](REPO_STRUCTURE.md) if you need orientation.
- Move to the crate or deployment document closest to your area of interest.
- Read the code after that. The docs here are meant to frame the workspace, not to duplicate implementation detail.
