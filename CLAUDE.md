# CLAUDE.md

This is a lightweight working index for agents operating in the Legato repository.

## Contents

- [Documentation Index](docs/INDEX.md)
- [Repository Structure](docs/REPO_STRUCTURE.md)
- [Development Workflow](docs/DEVELOPMENT.md)
- [System Shape](docs/architecture/SYSTEM_SHAPE.md)
- [Protocol And Behavior](docs/design/PROTOCOL_AND_BEHAVIOR.md)
- [Operations Runbook](deploy/OPERATIONS.md)
- [Platform Manifest](platform.yml)
- [Komodo Compose Stack](compose.yaml)
- [Client Packaging Notes](deploy/client/README.md)
- [Protocol Versioning](crates/legato-proto/PROTO_VERSIONING.md)

## Repository Shape

- `crates/legato-server`
  Server-side metadata, reconciliation, watchers, invalidations, TLS bootstrap, integration tests, and benchmarks.
- `crates/legatofs`
  Native client entrypoint and mount bootstrap.
- `crates/legato-prefetch`
  Project-aware prefetch CLI and parsers.
- `crates/legato-client-core`
  Shared client runtime, reconnect logic, and prefetch execution.
- `crates/legato-client-cache`
  SQLite-backed cache metadata and block-cache maintenance.
- `crates/legato-foundation`
  Shared config, telemetry, and runtime helpers.
- `crates/legato-proto`
  Wire contract and generated protobuf bindings.
- `crates/legato-types`
  Shared domain model types.
- `deploy/`
  Server deployment assets, operations notes, and client packaging notes.

## Development Lifecycle

The standard loop for implementation work in this repo is:

1. Read the relevant local docs and target crate entrypoints.
2. Make the smallest coherent change that completes the current ticket or batch.
3. Run formatting, tests, and linting.
4. Run focused integration or benchmark compile checks if the touched area needs them.
5. Commit with an intentional message.
6. Push to the working branch or `main`, depending on the active workflow.
7. Update the related ticket with the implementation result and close it when complete.

Root docs should stay short and index-like. Detailed explanation belongs in focused docs under `docs/`, `deploy/`, or next to the relevant crate.

## Ticket Maintenance

When work is tracked in GitHub issues:

- keep one issue per coherent slice of deliverable work
- comment when a ticket is implemented and include the commit hash
- close tickets only after code is on the target branch
- close epic or milestone tracker issues only after all scoped child work is complete
- avoid leaving the issue board out of sync with repository state

Typical `gh` commands:

```bash
gh issue list --state open
gh issue view <number>
gh issue comment <number> --body "Implemented in <commit> and pushed to <branch>."
gh issue close <number> --comment "Closing as completed by <commit>."
```

## CI And Verification Commands

Baseline verification:

```bash
cargo fmt --all
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Focused checks:

```bash
cargo test -p legato-server --test end_to_end
cargo bench -p legato-server --no-run
```

Use the focused commands when changing integration surfaces, benchmark targets, or the server-side read/prefetch path.

## Working Guidance

- Use the root docs as navigation aids, not as substitutes for reading the code.
- Keep `README.md`, `CLAUDE.md`, and `AGENTS.md` short and link outward.
- Put implementation-detail prose in the most local document that makes sense.
- Prefer updating focused docs over expanding root docs into long narratives.
- Keep the repo state, ticket state, and verification state aligned before stopping.
