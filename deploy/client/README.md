# Legatofs Client Packaging And Installation

This is a focused client-packaging note. For the top-level project index, start at [README.md](../../README.md) or [docs/INDEX.md](../../docs/INDEX.md).

This document defines the installation and upgrade shape for the native Legato client on macOS and Windows.

## Common Layout

- Binary name: `legatofs`
- Runtime state:
  - macOS: `/Library/Application Support/Legato`
  - Windows: `C:\ProgramData\Legato`
- Cache database: `<state_dir>/client.sqlite`
- Block cache root: `<state_dir>/blocks`
- Certificates:
  - server CA: `/etc/legato/certs/server-ca.pem` equivalent under the chosen client config root
  - client certificate: `/etc/legato/certs/client.pem`
  - client key: `/etc/legato/certs/client-key.pem`
- Default mount points:
  - macOS: `/Volumes/Legato`
  - Windows: `L:\Legato`

## macOS

- Packaging format: signed `.pkg`
- Installed binary target: `/usr/local/bin/legatofs`
- Config root: `/Library/Application Support/Legato`
- Default installed config: `/Library/Application Support/Legato/legatofs.toml`
- Installer build script: `deploy/client/package-macos.sh`
- Installer output: `artifacts/macos/*.pkg`
- Startup model: packaged binary and config assets today; service registration remains a later step once the runtime is a persistent mount daemon
- Filesystem framework expectation: macFUSE-compatible user-space mount integration
- Upgrade behavior:
  - replace the binary in place
  - preserve `client.sqlite`, `blocks/`, and cert material
  - preserve an existing `legatofs.toml` if already configured

## Windows

- Packaging format: installer `.exe` built with Inno Setup
- Installed binary target: `C:\Program Files\Legato\legatofs.exe`
- Config root: `C:\ProgramData\Legato`
- Default installed config: `C:\ProgramData\Legato\legatofs.toml`
- Installer build script: `deploy/client/package-windows.ps1`
- Installer output: `artifacts/windows/*.exe`
- Installer configuration prompts:
  - server endpoint
  - TLS server name
  - mount point
- Startup model: packaged binary and config assets today; service registration remains a later step once the runtime is a persistent mount daemon
- Filesystem framework expectation: WinFSP-backed user-space filesystem
- Upgrade behavior:
  - replace the binary in place
  - preserve `client.sqlite`, `blocks\`, and cert material
  - preserve an existing `legatofs.toml` if already configured

## Cache Integrity Rules

- Never delete `client.sqlite` or `blocks/` during normal upgrades.
- If the cache schema changes, run migrations at startup before mounting.
- If block integrity verification fails, remove only the affected cached block and refetch it.
- If cert paths change, fail fast at startup rather than mounting a partially configured filesystem.
