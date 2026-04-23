# Development Workflow

This is the minimal workflow guide for working in the Legato workspace.

## Tooling

- Rust workspace managed with Cargo
- Edition `2024`
- Workspace lint policy in the root `Cargo.toml`
- SQLite used for server metadata and client cache state
- Shared Ahara CI driven by the root `platform.yml`

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
  Placeholder for SSM-backed Komodo environment injection.

## What The Checks Cover

- `fmt`
  Workspace formatting.
- `test`
  Unit tests across the crates plus the current server-side integration suite.
- `clippy`
  Workspace linting at warning-as-error level.
- `bench --no-run`
  Verifies the benchmark targets still compile.

## Editing Guidance

- Keep root docs short and index-like.
- Put deployment-specific prose under `deploy/`.
- Put protocol-specific prose next to `legato-proto`.
- Prefer crate-local tests for implementation details and integration tests for cross-crate behavior.

## Before You Push

1. Run formatting.
2. Run the workspace tests.
3. Run Clippy with `-D warnings`.
4. If you changed the benchmark or integration surfaces, compile the benchmark target and run the targeted integration test.

## What This Document Is Not

This file is intentionally not a contributor handbook or architecture deep dive. Its job is to point you at the standard validation loop and the right local docs, then get out of the way.
