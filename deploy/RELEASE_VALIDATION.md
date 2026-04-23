# Release Validation

This document defines the v1 release-validation matrix for Legato. It separates checks that are automated in CI from checks that still require operator validation on the target platforms.

## Automated

- Shared workspace verification:
  `make ci`
- Local release smoke:
  [deploy/release/validate-local.sh](/home/dev/repos/legato/deploy/release/validate-local.sh)
- Coverage exercised by the local smoke:
  - server startup
  - TLS bootstrap
  - client bundle issuance
  - bundle manifest emission
  - client bundle installation into runtime state
  - manifest-backed install-time config hydration
  - client config generation
  - metadata lookup
  - file open and block read
  - server restart followed by fresh client smoke access
- Existing integration coverage that remains part of the release gate:
  - `cargo test -p legato-server --test end_to_end`
  - `cargo test -p legato-client-core --test transport`

## CI Gate

The GitHub Actions workflow now includes a `release-validation` job after the shared `ci` job. It builds release binaries and runs the local smoke validation script.

This does not replace platform packaging jobs. It complements them by proving that the shipped server and client binaries can execute the basic release flow end to end.

## Manual

The following checks still require platform or environment-specific validation:

- TrueNAS and Komodo deployment:
  - compose stack uses the real dataset mount layout
  - container runs under the intended UID/GID
  - server can read the mounted library dataset and write state/config datasets
- macOS installer:
  - `.pkg` installs `legatofs`
  - `legatofs install --bundle-dir <bundle>` writes config and certs under `/Library/Application Support/Legato`
  - the installed binary can connect to the target server and complete a smoke read
- Windows installer:
  - `.exe` installs `legatofs.exe`
  - `legatofs.exe install --bundle-dir <bundle>` writes config and certs under `C:\ProgramData\Legato`
  - the installed binary can connect to the target server and complete a smoke read
- Native mount behavior:
  - the intended macOS mount backend exposes the library at `/Volumes/Legato`
  - the intended Windows mount backend exposes the library at `L:\Legato`
  - DAWs and plugins can resolve files through that mounted path
- Prefetch behavior on target systems:
  - `legato-prefetch analyze` produces expected hints for representative projects
  - warmed reads hit the local cache on a real client machine

## Release Checklist

- `make ci` passes locally or in CI.
- `release-validation` job passes in GitHub Actions.
- macOS packaging job uploads a usable `.pkg`.
- Windows packaging job uploads a usable `.exe`.
- One TrueNAS/Komodo deployment smoke passes against the target dataset layout.
- One macOS client smoke passes from an installed package.
- One Windows client smoke passes from an installed package.
- One representative prefetch workflow passes against a real project on a real client.
