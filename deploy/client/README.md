# Legatofs Client Packaging And Installation

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
- Startup model: `launchd` agent or daemon that brings up the mount on login/system boot, depending on deployment preference
- Filesystem framework expectation: macFUSE-compatible user-space mount integration
- Upgrade behavior:
  - stop the mount service
  - replace the binary in place
  - preserve `client.sqlite`, `blocks/`, and cert material
  - restart the service and allow the client runtime to reconnect and reopen stale handles

## Windows

- Packaging format: MSI
- Installed binary target: `C:\Program Files\Legato\legatofs.exe`
- Config root: `C:\ProgramData\Legato`
- Startup model: Windows service with mount lifecycle managed at service start/stop
- Filesystem framework expectation: WinFSP-backed user-space filesystem
- Upgrade behavior:
  - stop the service and unmount the filesystem
  - replace the binary in place
  - preserve `client.sqlite`, `blocks\`, and cert material
  - restart the service and allow the client runtime to reconnect and rebuild server-local handles

## Cache Integrity Rules

- Never delete `client.sqlite` or `blocks/` during normal upgrades.
- If the cache schema changes, run migrations at startup before mounting.
- If block integrity verification fails, remove only the affected cached block and refetch it.
- If cert paths change, fail fast at startup rather than mounting a partially configured filesystem.
