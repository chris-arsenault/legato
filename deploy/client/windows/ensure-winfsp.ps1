$ErrorActionPreference = "Stop"

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

$installDir = Find-WinFspInstallDir
if (-not $installDir) {
    throw "WinFsp x64 runtime was not found. Install the WinFsp MSI from https://winfsp.dev/rel/ and rerun this installer."
}

foreach ($registryPath in @("HKLM:\SOFTWARE\WOW6432Node\WinFsp", "HKLM:\SOFTWARE\WinFsp")) {
    New-Item -Path $registryPath -Force | Out-Null
    New-ItemProperty -Path $registryPath -Name "InstallDir" -Value $installDir -PropertyType String -Force | Out-Null
}

Write-Host "WinFsp runtime detected at $installDir"
