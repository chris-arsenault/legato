$ErrorActionPreference = "Stop"

param(
    [Parameter(Mandatory = $true)]
    [string]$InstallerPath
)

$WorkDir = Join-Path ([System.IO.Path]::GetTempPath()) ("legato-validate-" + [System.Guid]::NewGuid().ToString("N"))
$InstallDir = Join-Path $WorkDir "install"
$BundleDir = Join-Path $WorkDir "bundle"
$StateDir = Join-Path $WorkDir "state"

try {
    New-Item -ItemType Directory -Force -Path $InstallDir, $BundleDir, $StateDir | Out-Null

    Set-Content -Path (Join-Path $BundleDir "server-ca.pem") -Value "ca"
    Set-Content -Path (Join-Path $BundleDir "client.pem") -Value "client"
    Set-Content -Path (Join-Path $BundleDir "client-key.pem") -Value "key"
    @'
{
  "client_name": "release-windows",
  "endpoint": "legato.lan:7823",
  "server_name": "legato.lan",
  "mount_point": "L:\\Legato",
  "library_root": "/srv/libraries",
  "issued_at_unix_ms": 1
}
'@ | Set-Content -Path (Join-Path $BundleDir "bundle.json")

    & $InstallerPath /VERYSILENT /SUPPRESSMSGBOXES /NORESTART /DIR="$InstallDir"

    $Legatofs = Join-Path $InstallDir "legatofs.exe"
    $RegisterClient = Join-Path $InstallDir "register-client.ps1"

    if (-not (Test-Path $Legatofs)) {
        throw "legatofs.exe was not installed"
    }
    if (-not (Test-Path $RegisterClient)) {
        throw "register-client.ps1 was not installed"
    }

    powershell.exe -ExecutionPolicy Bypass -File $RegisterClient `
        --bundle-dir $BundleDir `
        --state-dir $StateDir `
        --force

    if (-not (Test-Path (Join-Path $StateDir "legatofs.toml"))) {
        throw "legatofs.toml was not created"
    }
    if (-not (Test-Path (Join-Path $StateDir "certs\server-ca.pem"))) {
        throw "server-ca.pem was not installed"
    }
    if (-not (Test-Path (Join-Path $StateDir "certs\client.pem"))) {
        throw "client.pem was not installed"
    }
    if (-not (Test-Path (Join-Path $StateDir "certs\client-key.pem"))) {
        throw "client-key.pem was not installed"
    }
    if (-not (Test-Path (Join-Path $StateDir "extents"))) {
        throw "extents directory was not created"
    }

    $Config = Get-Content (Join-Path $StateDir "legatofs.toml") -Raw
    if ($Config -notmatch 'endpoint = "legato\.lan:7823"') {
        throw "endpoint was not hydrated from bundle metadata"
    }
    if ($Config -notmatch 'server_name = "legato\.lan"') {
        throw "server_name was not hydrated from bundle metadata"
    }

    Write-Host "Windows installer validation passed"
}
finally {
    Remove-Item -Recurse -Force $WorkDir -ErrorAction SilentlyContinue
}
