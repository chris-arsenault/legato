# Legato Client Installation

This guide explains how to install and run the native `legatofs` client on macOS and Windows.

The client does three things:

- mounts the server-owned Legato library as a read-only local filesystem
- keeps a local partial cache under the client state directory
- uses a server-issued bundle for mutual TLS and default connection settings

Start with [deploy/OPERATIONS.md](../OPERATIONS.md) if the server is not already running.

## Prerequisites

- A running `legato-server` reachable from the client.
- A client bundle issued by the server. The bundle directory must contain `server-ca.pem`, `client.pem`, `client-key.pem`, and preferably `bundle.json`.
- The native client installer from the GitHub Release tagged `v0.8`.
- macOS clients need a macFUSE-compatible runtime installed.
- Windows clients need WinFSP installed.

Issue a bundle on the server:

```bash
docker exec legato-server legato-server issue-client \
  --name studio-mac \
  --output-dir /tmp/studio-mac \
  --endpoint legato.lan:7823 \
  --server-name legato.lan
```

Transfer the generated bundle directory to the target client machine before running the client install command.

## macOS Install

Install the package:

```bash
sudo installer -pkg legatofs-0.8-macos.pkg -target /
```

Register the client bundle:

```bash
sudo legato-register-client --bundle-dir /path/to/studio-mac
```

That command writes the runtime config to:

```text
/Library/Application Support/Legato/legatofs.toml
```

Install and start the launchd agent:

```bash
legatofs service install
legatofs service start
legatofs service status
```

By default the mount appears at:

```text
/Volumes/Legato
```

Logs are written under:

```text
~/Library/Logs/Legato/
```

## Windows Install

Run the installer from an elevated PowerShell session:

```powershell
Start-Process .\legatofs-0.8-windows.exe -Verb RunAs -Wait
```

The installer prompts for an optional bundle directory, server endpoint, TLS server name, and mount point. If you provide a valid bundle directory during setup, the installer registers the client automatically.

If you skipped bundle registration during setup, register it afterward:

```powershell
& "C:\Program Files\Legato\legatofs.exe" install `
  --bundle-dir C:\Temp\studio-win
```

That command writes the runtime config to:

```text
C:\ProgramData\Legato\legatofs.toml
```

Install and start the scheduled task:

```powershell
& "C:\Program Files\Legato\legatofs.exe" service install
& "C:\Program Files\Legato\legatofs.exe" service start
& "C:\Program Files\Legato\legatofs.exe" service status
```

By default the mount appears at:

```text
L:\Legato
```

Logs are written under:

```text
C:\ProgramData\Legato\logs\
```

## Verify The Client

Check the generated config and host prerequisites:

```bash
legatofs doctor
```

Check local cache state:

```bash
legatofs cache status
```

Run a mounted read smoke test against a known path in the Legato library:

```bash
legatofs smoke --path /path/inside/library --offset 0 --size 4096
```

Open a representative DAW project or preset through the mounted filesystem. Supported project and preset opens trigger prefetch through the mounted runtime.

Use manual prefetch only for diagnostics or intentional warm-up:

```bash
legato-prefetch run /Volumes/Legato/path/to/project.als \
  --config "/Library/Application Support/Legato/legatofs.toml"
```

## Common Overrides

If the bundle does not include `bundle.json`, provide the endpoint and TLS server name explicitly:

```bash
legatofs install \
  --bundle-dir /path/to/bundle \
  --endpoint legato.lan:7823 \
  --server-name legato.lan
```

Override the mount point or state directory:

```bash
legatofs install \
  --bundle-dir /path/to/bundle \
  --mount-point /Volumes/Legato-Alt \
  --state-dir "/Library/Application Support/Legato" \
  --force
```

The `--force` flag overwrites the generated `legatofs.toml`; it does not delete the local cache directories.

## Upgrade

1. Stop the service.
2. Install the newer `.pkg` or `.exe`.
3. Start the service again.
4. Run `legatofs service status` and `legatofs doctor`.

The installer and registration commands are designed to preserve:

- `catalog/`
- `segments/`
- `checkpoints/`
- `certs/`
- an existing `legatofs.toml`, unless you rerun `legatofs install --force`

## Renewal Or Replacement

Reissue the bundle from the server when a client certificate is expiring, lost, or intentionally replaced:

```bash
docker exec legato-server legato-server issue-client \
  --name studio-mac \
  --output-dir /tmp/studio-mac \
  --endpoint legato.lan:7823 \
  --server-name legato.lan
```

Then reinstall the bundle on the client:

```bash
legatofs install --bundle-dir /path/to/new-bundle --force
legatofs service stop
legatofs service start
```

## Remove The Service

Remove the background service without deleting client state:

```bash
legatofs service stop
legatofs service uninstall
```

Delete the state directory only if you intentionally want the client to rebuild its local cache from the server.

## Runtime Layout

Default macOS paths:

- State directory: `/Library/Application Support/Legato`
- Config: `/Library/Application Support/Legato/legatofs.toml`
- Mount point: `/Volumes/Legato`
- Logs: `~/Library/Logs/Legato/`

Default Windows paths:

- State directory: `C:\ProgramData\Legato`
- Config: `C:\ProgramData\Legato\legatofs.toml`
- Mount point: `L:\Legato`
- Logs: `C:\ProgramData\Legato\logs\`

Inside the state directory:

- `catalog/` stores path, inode, directory, extent-map, residency, and subscription-cursor state.
- `segments/` stores local resident extent records.
- `checkpoints/` stores compacted recovery boundaries.
- `certs/` stores the server CA, client certificate, and client key.
- `prefetch-control.json` exists only while the mounted runtime is active.
