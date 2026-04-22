$ErrorActionPreference = "Stop"

$ProgramFilesDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Legatofs = Join-Path $ProgramFilesDir "legatofs.exe"

if (-not (Test-Path $Legatofs)) {
    throw "legatofs.exe was not found at $Legatofs"
}

& $Legatofs install @args
