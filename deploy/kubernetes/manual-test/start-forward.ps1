param(
    [ValidatePattern('^[A-Za-z0-9][A-Za-z0-9._@:-]*$')]
    [string] $SshHost = 'john@192.168.1.109'
)

$ErrorActionPreference = 'Stop'
$ports = @(5173, 8081, 8082)
foreach ($port in $ports) {
    if (Get-NetTCPConnection -State Listen -LocalPort $port -ErrorAction SilentlyContinue) {
        throw "Local port $port is already in use."
    }
}
$listeners = @(& ssh -o BatchMode=yes -o StrictHostKeyChecking=yes $SshHost `
    "ss -H -ltn 'sport = :5173 or sport = :8081 or sport = :8082'")
if ($LASTEXITCODE -ne 0) { throw 'Could not inspect remote forwarding ports.' }
if ($listeners.Count -ne 0) { throw 'A required remote forwarding port is already in use.' }

$arguments = @(
    '-tt', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
    '-o', 'ExitOnForwardFailure=yes', '-o', 'ServerAliveInterval=20',
    '-o', 'ServerAliveCountMax=3',
    '-L', '127.0.0.1:5173:127.0.0.1:5173',
    '-L', '127.0.0.1:8081:127.0.0.1:8081',
    '-L', '127.0.0.1:8082:127.0.0.1:8082',
    $SshHost,
    'kubectl -n kafka3o-test-clusters port-forward deployment/kafka3o-manual 5173:5173 8081:8081 8082:8082 --address 127.0.0.1'
)
$forwardExit = $null
$cleanupError = $null
try {
    & ssh @arguments
    $forwardExit = $LASTEXITCODE
} finally {
    $closed = $false
    for ($attempt = 0; $attempt -lt 30; $attempt++) {
        $remaining = @(& ssh -o BatchMode=yes -o StrictHostKeyChecking=yes $SshHost `
            "ss -H -ltn 'sport = :5173 or sport = :8081 or sport = :8082'")
        if ($LASTEXITCODE -ne 0) {
            $cleanupError = 'Could not verify that remote forwarding ports closed.'
            break
        }
        if ($remaining.Count -eq 0) { $closed = $true; break }
        Start-Sleep -Seconds 1
    }
    if (-not $closed -and $null -eq $cleanupError) {
        $cleanupError = 'Remote forwarding ports did not close; inspect the owning processes.'
    }
}
if ($null -ne $cleanupError) { throw $cleanupError }
if ($null -ne $forwardExit -and $forwardExit -ne 0) {
    throw "The manual-test SSH forward ended with exit code $forwardExit."
}
