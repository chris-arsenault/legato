$ErrorActionPreference = "Stop"

$RootDir = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$OutputDir = if ($args.Length -gt 0) { $args[0] } else { Join-Path $RootDir "artifacts\windows" }
$Version = if ($env:LEGATO_VERSION) { $env:LEGATO_VERSION } else {
    (Get-Content (Join-Path $RootDir "Cargo.toml") | Select-String '^version = "(.*)"$' | Select-Object -First 1).Matches[0].Groups[1].Value
}

$StageDir = Join-Path ([System.IO.Path]::GetTempPath()) ("legato-windows-" + [System.Guid]::NewGuid().ToString("N"))
$BinaryStage = Join-Path $StageDir "input"
New-Item -ItemType Directory -Force -Path $OutputDir, $BinaryStage | Out-Null

function Get-RegistryString {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Name
    )

    try {
        return (Get-ItemProperty -Path $Path -Name $Name -ErrorAction Stop).$Name
    }
    catch {
        return $null
    }
}

function Find-WinFspInstallDir {
    $candidates = @(
        (Get-RegistryString -Path "HKLM:\SOFTWARE\WOW6432Node\WinFsp" -Name "InstallDir"),
        (Get-RegistryString -Path "HKLM:\SOFTWARE\WinFsp" -Name "InstallDir"),
        "${env:ProgramFiles(x86)}\WinFsp",
        "$env:ProgramFiles\WinFsp"
    )

    foreach ($candidate in $candidates) {
        if ([string]::IsNullOrWhiteSpace($candidate)) {
            continue
        }

        $dllPath = Join-Path $candidate "bin\winfsp-x64.dll"
        if (Test-Path $dllPath) {
            return $candidate
        }
    }

    return $null
}

try {
    Copy-Item (Join-Path $RootDir "target\release\legatofs.exe") (Join-Path $BinaryStage "legatofs.exe")
    Copy-Item (Join-Path $RootDir "deploy\client\windows\register-client.ps1") (Join-Path $BinaryStage "register-client.ps1")
    Copy-Item (Join-Path $RootDir "deploy\client\windows\ensure-winfsp.ps1") (Join-Path $BinaryStage "ensure-winfsp.ps1")
    Copy-Item (Join-Path $RootDir "deploy\client\config\certs-README.txt") (Join-Path $BinaryStage "certs-README.txt")

    $WinFspInstallDir = Find-WinFspInstallDir
    if (-not $WinFspInstallDir) {
        throw "WinFsp x64 runtime DLL was not found. Install WinFsp from https://winfsp.dev/rel/ before packaging the Windows client."
    }
    Copy-Item (Join-Path $WinFspInstallDir "bin\winfsp-x64.dll") (Join-Path $BinaryStage "winfsp-x64.dll")

    if (-not (Get-Command iscc.exe -ErrorAction SilentlyContinue)) {
        choco install innosetup --no-progress -y
    }

    $env:LEGATO_VERSION = $Version
    $env:LEGATO_SOURCE_DIR = $BinaryStage
    $env:LEGATO_OUTPUT_DIR = $OutputDir

    & iscc.exe (Join-Path $RootDir "deploy\client\windows\installer.iss")
}
finally {
    Remove-Item -Recurse -Force $StageDir -ErrorAction SilentlyContinue
}
