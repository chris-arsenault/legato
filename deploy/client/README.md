# Legato Client Installation

The normal client install flow is:

1. Download the macOS `.pkg` or Windows `.exe` from the `v0.8` release.
2. Run the installer.
3. Accept the defaults, or change the mount point in the installer UI.

The installer/setup path contacts the Legato server, asks it for a client certificate bundle, writes the local config, installs the background service, and starts the mount. You should not need to copy bundle directories, edit TOML, or run registration commands by hand.

## What The Server Provides

`legato-server` exposes three client-facing ports:

- `7823`: the mTLS filesystem API used after setup
- `7824`: the HTTP bootstrap endpoint used by installers before the client has a certificate
- `7825/udp`: LAN discovery for finding the bootstrap endpoint

The bootstrap endpoint is:

```text
http://<server>:7824/v1/client-bundles
```

Installers first try UDP discovery. If discovery is blocked by the network, enter the bootstrap URL in the setup UI.

## macOS

Install the package:

```bash
sudo installer -pkg legatofs-0.8-macos.pkg -target /
```

The package attempts LAN discovery, registers the client, installs the launchd agent for the logged-in user, and starts the client with the default mount point.

If you need to override discovery or the mount point after installation, run the setup helper:

```bash
legato-setup-client
```

The helper prompts for:

- bootstrap URL, optional because LAN discovery is the default
- client name, defaulting to the host name
- mount point, defaulting to `/Volumes/Legato`

The helper reinstalls the launchd agent and restarts the client.

Default macOS runtime paths:

- Mount point: `/Volumes/Legato`
- Config: `/Library/Application Support/Legato/legatofs.toml`
- Logs: `~/Library/Logs/Legato/`

## Windows

Run the installer from an elevated PowerShell session or by double-clicking it and accepting the UAC prompt:

```powershell
Start-Process .\legatofs-0.8-windows.exe -Verb RunAs -Wait
```

The installer prompts for:

- bootstrap URL, optional because LAN discovery is the default
- client name, defaulting to the computer name
- mount point, defaulting to `L:\Legato`

The installer registers the client, installs the scheduled task, and starts the client before it exits.

Default Windows runtime paths:

- Mount point: `L:\Legato`
- Config: `C:\ProgramData\Legato\legatofs.toml`
- Logs: `C:\ProgramData\Legato\logs\`

## Verify

After install, verify that the mount exists:

- macOS: open `/Volumes/Legato`
- Windows: open `L:\Legato`

If you need a command-line check:

```bash
legatofs service status
legatofs doctor
```

## Upgrade

Install the newer client package over the old one. The installer preserves the client cache, certificates, and generated config unless setup is explicitly rerun with `--force`.

Preserved state:

- `catalog/`
- `segments/`
- `checkpoints/`
- `certs/`
- `legatofs.toml`

## Advanced Recovery

These commands are for break-glass recovery, not normal setup.

Re-run client setup from discovery:

```bash
legatofs install --force
legatofs service install --force
legatofs service start
```

Re-run client setup against an explicit bootstrap URL:

```bash
legatofs install --bootstrap-url http://legato.lan:7824 --force
legatofs service install --force
legatofs service start
```

Stop and remove the background service without deleting client state:

```bash
legatofs service stop
legatofs service uninstall
```

Delete the state directory only if you intentionally want the client to rebuild its local cache from the server.
