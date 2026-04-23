# Legatofs Client Packaging And Installation

This is a focused client-packaging note. For the top-level project index, start at [README.md](../../README.md) or [docs/INDEX.md](../../docs/INDEX.md).

This document defines the installation and upgrade shape for the native Legato client on macOS and Windows.

## Common Layout

- Binary name: `legatofs`
- Runtime state:
  - macOS: `/Library/Application Support/Legato`
  - Windows: `C:\ProgramData\Legato`
- Cache database: `<state_dir>/client.sqlite`
- Extent store root: `<state_dir>/extents`
- Certificate root: `<state_dir>/certs`
- Generated client config: `<state_dir>/legatofs.toml`
- Certificates:
  - server CA: `/etc/legato/certs/server-ca.pem` equivalent under the chosen client config root
  - client certificate: `/etc/legato/certs/client.pem`
  - client key: `/etc/legato/certs/client-key.pem`
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
- Packaged registration helper:
  `legato-register-client --bundle-dir <bundle>`
- Startup model: packaged binary plus bundle/config hydration through the shared install command. Automatic startup is not implemented yet.
- Filesystem framework expectation: macFUSE-compatible user-space mount integration
- Upgrade behavior:
  - replace the binary in place
  - preserve `client.sqlite`, `extents/`, and cert material
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
- If the installer is given a valid bundle directory, it runs `legatofs.exe install` automatically during setup.
- Startup model: packaged binary plus install-time bundle/config hydration through the shared install command. Automatic startup is not implemented yet.
- Filesystem framework expectation: WinFSP-backed user-space filesystem
- Upgrade behavior:
  - replace the binary in place
  - preserve `client.sqlite`, `extents\`, and cert material
  - preserve an existing `legatofs.toml` if already configured

## Client State Model

- `client.sqlite` stores metadata cache state, extent residency metadata, pin state, and checkpoints.
- `extents/` stores the materialized local extent files used by the read path.
- `certs/` stores the server CA plus issued client certificate and key.
- `bundle.json` in the issued bundle can carry install-time defaults such as endpoint and server name.
- `legatofs.toml` is generated from the install command and should be preserved across upgrades.

## Cache Integrity Rules

- Never delete `client.sqlite` or `extents/` during normal upgrades.
- If the cache schema changes, run migrations at startup before mounting.
- If extent integrity verification fails, remove only the affected cached extent and refetch it.
- If cert paths change, fail fast at startup rather than mounting a partially configured filesystem.

## Registration Flow

The current end-to-end client registration flow is:

1. Issue a client bundle on the server with `legato-server issue-client`.
2. Transfer the resulting bundle directory to the client machine.
3. Run `legatofs install` against that bundle. If `bundle.json` is present, endpoint and TLS server-name settings are hydrated from the bundle automatically.
4. Start `legatofs` with the generated `legatofs.toml`.

`legatofs install` creates the config file, cert layout, and extent-store directory under the chosen state root. Command-line flags still override bundle defaults when a site-specific endpoint, mount point, or virtual library root must differ from the issued bundle.

## Renewal And Replacement

- Reissue a client bundle from the server with `legato-server issue-client --name <client> --output-dir <bundle>`.
- Reinstall on the client with `legatofs install --bundle-dir <bundle> --force`.
- Treat lost client keys as certificate replacement events: issue a new bundle and reinstall rather than attempting in-place key recovery.
