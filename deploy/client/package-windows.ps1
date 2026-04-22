$ErrorActionPreference = "Stop"

$RootDir = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$OutputDir = if ($args.Length -gt 0) { $args[0] } else { Join-Path $RootDir "artifacts\windows" }
$Version = if ($env:LEGATO_VERSION) { $env:LEGATO_VERSION } else {
    (Get-Content (Join-Path $RootDir "Cargo.toml") | Select-String '^version = "(.*)"$' | Select-Object -First 1).Matches[0].Groups[1].Value
}

$StageDir = Join-Path ([System.IO.Path]::GetTempPath()) ("legato-windows-" + [System.Guid]::NewGuid().ToString("N"))
$BinaryStage = Join-Path $StageDir "input"
New-Item -ItemType Directory -Force -Path $OutputDir, $BinaryStage | Out-Null

try {
    Copy-Item (Join-Path $RootDir "target\release\legatofs.exe") (Join-Path $BinaryStage "legatofs.exe")
    Copy-Item (Join-Path $RootDir "deploy\client\config\certs-README.txt") (Join-Path $BinaryStage "certs-README.txt")

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
