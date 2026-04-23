# Legatofs Client Packaging And Installation

This is a focused client-packaging note. For the top-level project index, start at [README.md](../../README.md) or [docs/INDEX.md](../../docs/INDEX.md).

This document defines the installation and upgrade shape for the native Legato client on macOS and Windows.

## Common Layout

- Binary name: `legatofs`
- Runtime state:
  - macOS: `/Library/Application Support/Legato`
  - Windows: `C:\ProgramData\Legato`
- Catalog root: `<state_dir>/catalog`
- Segment root: `<state_dir>/segments`
- Checkpoint root: `<state_dir>/checkpoints`
- Certificate root: `<state_dir>/certs`
- Generated client config: `<state_dir>/legatofs.toml`
- Mounted-runtime control manifest: `<state_dir>/prefetch-control.json`
- Certificates:
  - server CA under the client certificate root
  - client certificate under the client certificate root
  - client key under the client certificate root
- Default mount points:
  - macOS: `/Volumes/Legato`
  - Windows: `L:\Legato`

## macOS

- Packaging format: `.pkg`
- Installed binary target: `/usr/local/bin/legatofs`
- Installed registration helper: `/usr/local/bin/legato-register-client`
- Config root: `/Library/Application Support/Legato`
- Default installed config: `/Library/Application Support/Legato/legatofs.toml`
- Installer build script: `deploy/client/package-macos.sh`
- Installer output: `artifacts/macos/*.pkg`
- Client-bundle install command:
  `legatofs install --bundle-dir <bundle>`
- Mount-agent registration:
  `legatofs service install`
- Packaged registration helper:
  `legato-register-client --bundle-dir <bundle>`
- Startup model: user launchd agent running the installed binary with `LEGATO_FS_CONFIG` pointed at the generated config.
- Filesystem framework expectation: macFUSE-compatible user-space mount integration.
- Upgrade behavior:
  - replace the binary in place
  - preserve `catalog/`, `segments/`, `checkpoints/`, and cert material
  - preserve an existing `legatofs.toml` if already configured

## Windows

- Packaging format: installer `.exe` built with Inno Setup
- Installed binary target: `C:\Program Files\Legato\legatofs.exe`
- Installed registration helper: `C:\Program Files\Legato\register-client.ps1`
- Config root: `C:\ProgramData\Legato`
- Default installed config: `C:\ProgramData\Legato\legatofs.toml`
- Installer build script: `deploy/client/package-windows.ps1`
- Installer output: `artifacts/windows/*.exe`
- Installer configuration prompts:
  - optional client bundle directory
  - server endpoint
  - TLS server name
  - mount point
- Client-bundle install command:
  `legatofs.exe install --bundle-dir <bundle>`
- Mount-agent registration:
  `legatofs.exe service install`
- If the installer is given a valid bundle directory, it runs `legatofs.exe install` automatically during setup.
- Startup model: per-user scheduled task running the installed binary with `LEGATO_FS_CONFIG` pointed at the generated config.
- Filesystem framework expectation: WinFSP-backed user-space filesystem.
- Upgrade behavior:
  - replace the binary in place
  - preserve `catalog\`, `segments\`, `checkpoints\`, and cert material
  - preserve an existing `legatofs.toml` if already configured

## Client State Model

- `catalog/` stores path, inode, directory, extent-map, residency, and subscription-cursor state.
- `segments/` stores local append-only segment files containing resident records.
- `checkpoints/` stores compacted recovery boundaries.
- `certs/` stores the server CA plus issued client certificate and key.
- `bundle.json` in the issued bundle can carry install-time defaults such as endpoint and server name.
- `legatofs.toml` is generated from the install command and should be preserved across upgrades.
- `prefetch-control.json` is written only while `legatofs` is running and allows manual `legato-prefetch run` requests to route through the mounted runtime instead of acting as a second writer.

## Store Integrity Rules

- Do not delete `catalog/`, `segments/`, or `checkpoints/` during normal upgrades.
- Validate segment records by hash before serving data.
- Truncate incomplete tail records during startup recovery.
- Rebuild in-memory indexes from catalog checkpoints plus replay.
- If cert paths change, fail fast at startup rather than mounting a partially configured filesystem.

## Registration Flow

The client registration flow is:

1. Issue a client bundle on the server with `legato-server issue-client`.
2. Transfer the resulting bundle directory to the client machine.
3. Run `legatofs install` against that bundle. If `bundle.json` is present, endpoint and TLS server-name settings are hydrated from the bundle automatically.
4. Start `legatofs` with the generated `legatofs.toml`.

`legatofs install` creates the config file, cert layout, catalog directory, segment directory, and checkpoint directory under the chosen state root. Command-line flags override bundle defaults when a site-specific endpoint, mount point, or virtual library root must differ from the issued bundle.

## Renewal And Replacement

- Reissue a client bundle from the server with `legato-server issue-client --name <client> --output-dir <bundle>`.
- Reinstall on the client with `legatofs install --bundle-dir <bundle> --force`.
- Treat lost client keys as certificate replacement events: issue a new bundle and reinstall rather than attempting in-place key recovery.
