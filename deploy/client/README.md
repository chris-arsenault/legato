# Legato Client Installation

The normal client install flow is:

1. Download the macOS `.pkg` or Windows `.exe` from the `v0.8` release.
2. Run the installer.
3. Run the setup helper if the platform installer does not show setup prompts.
4. Accept the defaults, or change the mount point in the setup UI.

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

For the default LAN deployment in this repository, enter:

```text
http://192.168.66.3:7824/v1/client-bundles
```

## macOS

Install the package:

```bash
sudo installer -pkg legatofs-0.8-macos.pkg -target /
```

Then run the setup helper:

```bash
legato-setup-client
```

The package installs the binaries and prepares `/Library/Application Support/Legato`. The setup helper performs LAN discovery or uses the bootstrap URL you enter, registers the client, sets ownership for the logged-in user's launchd agent, installs the agent, and starts the mount. The launchd agent runs the native `legatofs service launch --config /Library/Application Support/Legato/legatofs.toml` action, which hosts the normal mount runtime under launchd.

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
- mount point, defaulting to `L:`

Windows mounts require the WinFsp MSI from `https://winfsp.dev/rel/`. The Legato installer validates the installed runtime and fixes the WinFsp registry compatibility key used by the client before starting the background task.

The installer registers the client, installs the per-user scheduled task, and starts the client before it exits. The task is per-user because the WinFsp drive letter is hosted in that user's interactive Windows session.

Default Windows runtime paths:

- Mount point: `L:`
- Config: `C:\ProgramData\Legato\legatofs.toml`
- Logs: `C:\ProgramData\Legato\logs\`

The main client tracing file is `C:\ProgramData\Legato\logs\legatofs.log`. The Windows scheduled task runs the native `legatofs.exe service launch --config C:\ProgramData\Legato\legatofs.toml` action, which starts the normal `legatofs.exe --config ...` runtime detached from the task console. There is no generated VBS or `cmd.exe` launcher.

The Windows client writes slow-operation warnings to the log when a mount callback or client metadata/read operation takes longer than 250 ms.

## Verify

After install, verify that the mount exists:

- macOS: open `/Volumes/Legato`
- Windows: open `L:`

If you need a command-line check:

```bash
legatofs service status
legatofs doctor
```

## Upgrade

Install the newer client package over the old one. On Windows, the installer detects an existing `C:\ProgramData\Legato\legatofs.toml`, skips bootstrap/client-name/mount-point setup, preserves the local state, stops the running mount task, updates the installed binaries, reinstalls the scheduled task, and starts the mount again.

The same path also acts as repair for a broken or missing scheduled task when the generated config still exists.

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
legatofs install --bootstrap-url http://192.168.66.3:7824/v1/client-bundles --force
legatofs service install --force
legatofs service start
```

Stop and remove the background service without deleting client state:

```bash
legatofs service stop
legatofs service uninstall
```

The Windows uninstaller stops the Legato task, removes the scheduled task, and deletes `C:\ProgramData\Legato`, including generated certs, config, cache, catalog, and logs. It does not uninstall WinFsp because WinFsp is a shared system runtime that other tools may use.

Delete the state directory manually only if you are using the CLI service commands instead of the Windows uninstaller and intentionally want the client to rebuild its local cache from the server.
