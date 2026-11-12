$ErrorActionPreference = 'SilentlyContinue'
docker info *> $null
if ($LASTEXITCODE -ne 0) {
    Write-Output 'DOCKER : DOWN'
    Write-Output 'MINIMAL SEARCH STACK NOT READY'
    exit 1
}

$rows = docker compose ps --format json | ConvertFrom-Json
$ok = $true
foreach ($service in @('postgres', 'neo4j')) {
    $row = @($rows | Where-Object { $_.Service -eq $service })[0]
    if ($row -and $row.State -match 'running|up') {
        Write-Output ("{0} : RUNNING" -f $service.ToUpper())
    } else {
        $ok = $false
        Write-Output ("{0} : DOWN" -f $service.ToUpper())
    }
}

try {
    Invoke-RestMethod 'http://127.0.0.1:9200' -TimeoutSec 3 | Out-Null
    Write-Output 'ELASTICSEARCH : RUNNING'
} catch {
    $ok = $false
    Write-Output 'ELASTICSEARCH : DOWN'
}

Write-Output 'KAFKA/AIRFLOW : OPTIONAL (profile=pipeline)'
if ($ok) {
    Write-Output 'MINIMAL SEARCH STACK READY'
} else {
    Write-Output 'MINIMAL SEARCH STACK NOT READY'
    exit 1
}
