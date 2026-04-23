# Development Workflow

This is the minimal workflow guide for working in the Legato workspace.

## Tooling

- Rust workspace managed with Cargo
- Edition `2024`
- Workspace lint policy in the root `Cargo.toml`
- Shared Ahara CI driven by the root `platform.yml`
- Native client package jobs for macOS and Windows

## Common Commands

```bash
cargo fmt --all
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

## Focused Commands

```bash
cargo test -p legato-server --test end_to_end
cargo bench -p legato-server --no-run
```

## Shared CI Entry Points

- `.github/workflows/ci.yml`
  Shared CI plus native client package builds.
- `platform.yml`
  Declares the project stack and deployable Rust binary artifact.
- `Dockerfile`
  Packaging-only image build that expects the CI workflow to populate `dist/legato-server`.
- `compose.yaml`
  Root Komodo/TrueNAS stack file used by the shared deploy action.
- `secret-paths.yml`
  Reserved path map for SSM-backed Komodo environment injection.

## What The Checks Cover

- `fmt`
  Workspace formatting.
- `test`
  Unit tests across crates plus integration coverage for protocol, store, mount, and prefetch behavior.
- `clippy`
  Workspace linting at warning-as-error level.
- `bench --no-run`
  Verifies benchmark targets still compile.

## Editing Guidance

- Keep root docs short and index-like.
- Put deployment-specific prose under `deploy/`.
- Put protocol-specific prose next to `legato-proto`.
- Keep store-format prose in `docs/design/TRANSFER_LAYOUT_AND_STORE_MODEL.md`.
- Prefer crate-local tests for implementation details and integration tests for cross-crate behavior.

## Before You Push

1. Run formatting.
2. Run workspace tests.
3. Run Clippy with `-D warnings`.
4. If you changed protocol, store, mount, benchmark, or integration surfaces, run the focused command for that surface.

## What This Document Is Not

This file is intentionally not a contributor handbook or architecture deep dive. Its job is to point you at the standard validation loop and the right local docs, then get out of the way.
