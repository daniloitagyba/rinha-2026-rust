param(
    [ValidateSet("submission", "build")]
    [string]$Mode = "submission",
    [ValidateSet("default", "remote-ryzen", "remote-ryzen-hard")]
    [string]$RunnerPreset = "default",
    [string]$ProjectName = "rinha-rust-local",
    [string]$K6Image = $env:K6_IMAGE,
    [string]$EarlyCandidates = $env:EARLY_CANDIDATES,
    [string]$MinCandidates = $env:MIN_CANDIDATES,
    [string]$MaxCandidates = $env:MAX_CANDIDATES,
    [string]$ProfileFastPath = $env:PROFILE_FASTPATH,
    [string]$ProfileFastPathReferenceSha256 = $env:PROFILE_FASTPATH_REFERENCE_SHA256,
    [string]$ExpectedReferencesGzipSha256 = $env:EXPECTED_REFERENCES_GZIP_SHA256,
    [string]$ProfileMinCount = $env:PROFILE_MIN_COUNT,
    [string]$ProfileLegitMinCount = $env:PROFILE_LEGIT_MIN_COUNT,
    [string]$ProfileFraudMinCount = $env:PROFILE_FRAUD_MIN_COUNT,
    [string]$ProfileDominantFastPath = $env:PROFILE_DOMINANT_FASTPATH,
    [string]$ProfileDominantMinCount = $env:PROFILE_DOMINANT_MIN_COUNT,
    [string]$ProfileDominantMaxOpposite = $env:PROFILE_DOMINANT_MAX_OPPOSITE,
    [string]$ExactFallback = $env:EXACT_FALLBACK,
    [string]$RiskySemanticGroups = $env:RISKY_SEMANTIC_GROUPS,
    [string]$RiskySemanticRadius = $env:RISKY_SEMANTIC_RADIUS,
    [string]$EarlyEdgeFallback = $env:EARLY_EDGE_FALLBACK,
    [string]$RiskyAmountMin = $env:RISKY_AMOUNT_MIN,
    [string]$RiskyAmountMax = $env:RISKY_AMOUNT_MAX,
    [string]$RiskyInstallmentsMin = $env:RISKY_INSTALLMENTS_MIN,
    [string]$RiskyInstallmentsMax = $env:RISKY_INSTALLMENTS_MAX,
    [string]$RiskyRatioMin = $env:RISKY_RATIO_MIN,
    [string]$RiskyKmHomeMin = $env:RISKY_KM_HOME_MIN,
    [string]$RiskyKmHomeMax = $env:RISKY_KM_HOME_MAX,
    [string]$RiskyTx24hMin = $env:RISKY_TX24H_MIN,
    [string]$RiskyTx24hMax = $env:RISKY_TX24H_MAX,
    [string]$RiskyMerchantAvgMin = $env:RISKY_MERCHANT_AVG_MIN,
    [string]$RiskyMerchantAvgMax = $env:RISKY_MERCHANT_AVG_MAX,
    [string]$FastPath = $env:FAST_PATH,
    [string]$FdEpollRaw = $env:FD_EPOLL_RAW,
    [string]$Workers = $env:WORKERS,
    [string]$ApiCpu = $env:API_CPU,
    [string]$ApiMemory = $env:API_MEMORY,
    [string]$ApiCpuset = $env:API_CPUSET,
    [string]$Api1Cpuset = $env:API1_CPUSET,
    [string]$Api2Cpuset = $env:API2_CPUSET,
    [string]$LbCpu = $env:LB_CPU,
    [string]$LbMemory = $env:LB_MEMORY,
    [string]$LbCpuset = $env:LB_CPUSET,
    [string]$SubmissionComposeFile = $env:SUBMISSION_COMPOSE_FILE,
    [switch]$KeepServices,
    [switch]$RefreshData,
    [switch]$Pull
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($K6Image)) {
    $K6Image = "grafana/k6:latest"
}

switch ($RunnerPreset) {
    "remote-ryzen" {
        if ([string]::IsNullOrWhiteSpace($ApiCpu)) {
            $ApiCpu = "0.300"
        }

        if ([string]::IsNullOrWhiteSpace($LbCpu)) {
            $LbCpu = "0.110"
        }
    }
    "remote-ryzen-hard" {
        if ([string]::IsNullOrWhiteSpace($ApiCpu)) {
            $ApiCpu = "0.300"
        }

        if ([string]::IsNullOrWhiteSpace($LbCpu)) {
            $LbCpu = "0.108"
        }
    }
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
    if ([string]::IsNullOrWhiteSpace($SubmissionComposeFile)) {
        $worktreeCompose = "C:\tmp\rinha-2026-rust-submission\docker-compose.yml"
        if (Test-Path $worktreeCompose) {
            $SubmissionComposeFile = $worktreeCompose
        }
    }

    if ([string]::IsNullOrWhiteSpace($SubmissionComposeFile)) {
        $composeFile = Join-Path $root "submission/docker-compose.yml"
    } else {
        $composeFile = $SubmissionComposeFile
    }
} else {
    $composeFile = Join-Path $root "docker-compose.yml"
}

$originalComposeParallelLimit = $env:COMPOSE_PARALLEL_LIMIT
if ($Mode -eq "build" -and [string]::IsNullOrWhiteSpace($env:COMPOSE_PARALLEL_LIMIT)) {
    $env:COMPOSE_PARALLEL_LIMIT = "1"
}

if ($Mode -eq "build" -and ($RefreshData -or -not (Test-Path $references))) {
    New-Item -ItemType Directory -Force -Path $resourcesDir | Out-Null
    Invoke-WebRequest `
        -Uri "https://raw.githubusercontent.com/zanfranceschi/rinha-de-backend-2026/main/resources/references.json.gz" `
        -OutFile $references
}

if (([string]::IsNullOrWhiteSpace($ProfileFastPathReferenceSha256) -or [string]::IsNullOrWhiteSpace($ExpectedReferencesGzipSha256)) -and (Test-Path $references)) {
    $referencesHash = (Get-FileHash -Algorithm SHA256 -Path $references).Hash.ToLowerInvariant()
    if ([string]::IsNullOrWhiteSpace($ProfileFastPathReferenceSha256)) {
        $ProfileFastPathReferenceSha256 = $referencesHash
    }
    if ([string]::IsNullOrWhiteSpace($ExpectedReferencesGzipSha256)) {
        $ExpectedReferencesGzipSha256 = $referencesHash
    }
}

$overrideFile = $null
$apiOverrides = [ordered]@{
    "EARLY_CANDIDATES" = $EarlyCandidates
    "MIN_CANDIDATES" = $MinCandidates
    "MAX_CANDIDATES" = $MaxCandidates
    "PROFILE_FASTPATH" = $ProfileFastPath
    "PROFILE_FASTPATH_REFERENCE_SHA256" = $ProfileFastPathReferenceSha256
    "EXPECTED_REFERENCES_GZIP_SHA256" = $ExpectedReferencesGzipSha256
    "PROFILE_MIN_COUNT" = $ProfileMinCount
    "PROFILE_LEGIT_MIN_COUNT" = $ProfileLegitMinCount
    "PROFILE_FRAUD_MIN_COUNT" = $ProfileFraudMinCount
    "PROFILE_DOMINANT_FASTPATH" = $ProfileDominantFastPath
    "PROFILE_DOMINANT_MIN_COUNT" = $ProfileDominantMinCount
    "PROFILE_DOMINANT_MAX_OPPOSITE" = $ProfileDominantMaxOpposite
    "EXACT_FALLBACK" = $ExactFallback
    "RISKY_SEMANTIC_GROUPS" = $RiskySemanticGroups
    "RISKY_SEMANTIC_RADIUS" = $RiskySemanticRadius
    "EARLY_EDGE_FALLBACK" = $EarlyEdgeFallback
    "RISKY_AMOUNT_MIN" = $RiskyAmountMin
    "RISKY_AMOUNT_MAX" = $RiskyAmountMax
    "RISKY_INSTALLMENTS_MIN" = $RiskyInstallmentsMin
    "RISKY_INSTALLMENTS_MAX" = $RiskyInstallmentsMax
    "RISKY_RATIO_MIN" = $RiskyRatioMin
    "RISKY_KM_HOME_MIN" = $RiskyKmHomeMin
    "RISKY_KM_HOME_MAX" = $RiskyKmHomeMax
    "RISKY_TX24H_MIN" = $RiskyTx24hMin
    "RISKY_TX24H_MAX" = $RiskyTx24hMax
    "RISKY_MERCHANT_AVG_MIN" = $RiskyMerchantAvgMin
    "RISKY_MERCHANT_AVG_MAX" = $RiskyMerchantAvgMax
    "FAST_PATH" = $FastPath
    "FD_EPOLL_RAW" = $FdEpollRaw
    "WORKERS" = $Workers
}

$activeApiOverrides = @()
foreach ($item in $apiOverrides.GetEnumerator()) {
    if (-not [string]::IsNullOrWhiteSpace($item.Value)) {
        $activeApiOverrides += $item
    }
}

$hasResourceOverrides =
    -not [string]::IsNullOrWhiteSpace($ApiCpu) -or
    -not [string]::IsNullOrWhiteSpace($ApiMemory) -or
    -not [string]::IsNullOrWhiteSpace($LbCpu) -or
    -not [string]::IsNullOrWhiteSpace($LbMemory)

$hasCpusetOverrides =
    -not [string]::IsNullOrWhiteSpace($ApiCpuset) -or
    -not [string]::IsNullOrWhiteSpace($Api1Cpuset) -or
    -not [string]::IsNullOrWhiteSpace($Api2Cpuset) -or
    -not [string]::IsNullOrWhiteSpace($LbCpuset)

if ($activeApiOverrides.Count -gt 0 -or $hasResourceOverrides -or $hasCpusetOverrides) {
    $overrideFile = Join-Path ([System.IO.Path]::GetTempPath()) "$ProjectName.override.yml"
    $lines = @("services:")
    if (-not [string]::IsNullOrWhiteSpace($LbCpu) -or -not [string]::IsNullOrWhiteSpace($LbMemory) -or -not [string]::IsNullOrWhiteSpace($LbCpuset)) {
        $lines += "  lb:"
        if (-not [string]::IsNullOrWhiteSpace($LbCpuset)) {
            $lines += "    cpuset: `"$LbCpuset`""
        }

        if (-not [string]::IsNullOrWhiteSpace($LbCpu) -or -not [string]::IsNullOrWhiteSpace($LbMemory)) {
            $lines += "    deploy:"
            $lines += "      resources:"
            $lines += "        limits:"
            if (-not [string]::IsNullOrWhiteSpace($LbCpu)) {
                $lines += "          cpus: `"$LbCpu`""
            }

            if (-not [string]::IsNullOrWhiteSpace($LbMemory)) {
                $lines += "          memory: `"$LbMemory`""
            }
        }
    }

    foreach ($service in @("api1", "api2")) {
        $serviceCpuset = $ApiCpuset
        if ($service -eq "api1" -and -not [string]::IsNullOrWhiteSpace($Api1Cpuset)) {
            $serviceCpuset = $Api1Cpuset
        }

        if ($service -eq "api2" -and -not [string]::IsNullOrWhiteSpace($Api2Cpuset)) {
            $serviceCpuset = $Api2Cpuset
        }

        $lines += "  ${service}:"
        if (-not [string]::IsNullOrWhiteSpace($serviceCpuset)) {
            $lines += "    cpuset: `"$serviceCpuset`""
        }

        if ($activeApiOverrides.Count -gt 0) {
            $lines += "    environment:"
            foreach ($item in $activeApiOverrides) {
                $lines += "      $($item.Key): `"$($item.Value)`""
            }
        }

        if (-not [string]::IsNullOrWhiteSpace($ApiCpu) -or -not [string]::IsNullOrWhiteSpace($ApiMemory)) {
            $lines += "    deploy:"
            $lines += "      resources:"
            $lines += "        limits:"
            if (-not [string]::IsNullOrWhiteSpace($ApiCpu)) {
                $lines += "          cpus: `"$ApiCpu`""
            }

            if (-not [string]::IsNullOrWhiteSpace($ApiMemory)) {
                $lines += "          memory: `"$ApiMemory`""
            }
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
    try {
        Compose "down" "--remove-orphans" "-v"
    } catch {
        Write-Warning $_
    }

    if ($Pull -or $Mode -eq "submission") {
        Compose "pull"
    }

    if ($Mode -eq "build") {
        Compose "up" "-d" "--build" "--remove-orphans"
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

    if ($null -eq $originalComposeParallelLimit) {
        Remove-Item Env:COMPOSE_PARALLEL_LIMIT -ErrorAction SilentlyContinue
    } else {
        $env:COMPOSE_PARALLEL_LIMIT = $originalComposeParallelLimit
    }
}
