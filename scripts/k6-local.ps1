param(
    [ValidateSet("submission", "build")]
    [string]$Mode = "submission",
    [string]$ProjectName = "rinha-rust-local",
    [string]$K6Image = $env:K6_IMAGE,
    [string]$EarlyCandidates = $env:EARLY_CANDIDATES,
    [string]$MinCandidates = $env:MIN_CANDIDATES,
    [string]$MaxCandidates = $env:MAX_CANDIDATES,
    [string]$ProfileFastPath = $env:PROFILE_FASTPATH,
    [string]$ProfileMinCount = $env:PROFILE_MIN_COUNT,
    [string]$ExactFallback = $env:EXACT_FALLBACK,
    [string]$FastPath = $env:FAST_PATH,
    [string]$Workers = $env:WORKERS,
    [switch]$KeepServices,
    [switch]$RefreshData,
    [switch]$Pull
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($K6Image)) {
    $K6Image = "grafana/k6:latest"
}

$root = Resolve-Path (Join-Path $PSScriptRoot "..")
$testDir = Join-Path $root "test"
$testData = Join-Path $testDir "test-data.json"
$resourcesDir = Join-Path $root "resources"
$references = Join-Path $resourcesDir "references.json.gz"

if ($RefreshData -or -not (Test-Path $testData)) {
    New-Item -ItemType Directory -Force -Path $testDir | Out-Null
    Invoke-WebRequest `
        -Uri "https://raw.githubusercontent.com/zanfranceschi/rinha-de-backend-2026/main/test/test-data.json" `
        -OutFile $testData
}

if ($Mode -eq "submission") {
    $composeFile = Join-Path $root "submission/docker-compose.yml"
} else {
    $composeFile = Join-Path $root "docker-compose.yml"
}

if ($Mode -eq "build" -and ($RefreshData -or -not (Test-Path $references))) {
    New-Item -ItemType Directory -Force -Path $resourcesDir | Out-Null
    Invoke-WebRequest `
        -Uri "https://raw.githubusercontent.com/zanfranceschi/rinha-de-backend-2026/main/resources/references.json.gz" `
        -OutFile $references
}

$overrideFile = $null
$apiOverrides = [ordered]@{
    "EARLY_CANDIDATES" = $EarlyCandidates
    "MIN_CANDIDATES" = $MinCandidates
    "MAX_CANDIDATES" = $MaxCandidates
    "PROFILE_FASTPATH" = $ProfileFastPath
    "PROFILE_MIN_COUNT" = $ProfileMinCount
    "EXACT_FALLBACK" = $ExactFallback
    "FAST_PATH" = $FastPath
    "WORKERS" = $Workers
}

$activeApiOverrides = @()
foreach ($item in $apiOverrides.GetEnumerator()) {
    if (-not [string]::IsNullOrWhiteSpace($item.Value)) {
        $activeApiOverrides += $item
    }
}

if ($activeApiOverrides.Count -gt 0) {
    $overrideFile = Join-Path ([System.IO.Path]::GetTempPath()) "$ProjectName.override.yml"
    $lines = @("services:")
    foreach ($service in @("api1", "api2")) {
        $lines += "  ${service}:"
        $lines += "    environment:"
        foreach ($item in $activeApiOverrides) {
            $lines += "      $($item.Key): `"$($item.Value)`""
        }
    }

    Set-Content -Path $overrideFile -Value ($lines -join [Environment]::NewLine) -Encoding ascii
}

function Compose {
    param(
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]]$ComposeArgs
    )

    $args = @("compose", "-p", $ProjectName, "-f", $composeFile)
    if ($overrideFile) {
        $args += @("-f", $overrideFile)
    }

    $args += $ComposeArgs
    & docker @args
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose failed with exit code $LASTEXITCODE"
    }
}

try {
    if ($Pull -or $Mode -eq "submission") {
        Compose "pull"
    }

    if ($Mode -eq "build") {
        & docker build -t "ghcr.io/daniloitagyba/rinha-2026-rust:latest" $root
        if ($LASTEXITCODE -ne 0) {
            throw "docker build failed with exit code $LASTEXITCODE"
        }
        Compose "up" "-d" "--remove-orphans"
    } else {
        Compose "up" "-d" "--remove-orphans"
    }

    $ready = $false
    for ($i = 0; $i -lt 90; $i++) {
        try {
            $response = Invoke-WebRequest -Uri "http://127.0.0.1:9999/ready" -UseBasicParsing -TimeoutSec 2
            if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 300) {
                $ready = $true
                break
            }
        } catch {
            Start-Sleep -Seconds 1
        }
    }

    if (-not $ready) {
        throw "backend did not become ready on http://127.0.0.1:9999/ready"
    }

    $mount = "${testDir}:/scripts"
    & docker run --rm `
        --network "${ProjectName}_default" `
        -e "BASE_URL=http://lb:9999" `
        -e "RESULTS_PATH=/scripts/results.json" `
        -e "TARGET_RATE" `
        -e "RAMP_DURATION" `
        -e "START_RATE" `
        -e "PRE_ALLOCATED_VUS" `
        -e "MAX_VUS" `
        -e "REQUEST_TIMEOUT" `
        -v $mount `
        $K6Image `
        run /scripts/test.js
    if ($LASTEXITCODE -ne 0) {
        throw "docker run k6 failed with exit code $LASTEXITCODE"
    }
} finally {
    if (-not $KeepServices) {
        try {
            Compose "down" "--remove-orphans"
        } catch {
            Write-Warning $_
        }
    }

    if ($overrideFile -and (Test-Path $overrideFile)) {
        Remove-Item -Path $overrideFile -Force
    }
}
