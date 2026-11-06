param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[A-Za-z0-9][A-Za-z0-9._@:-]*$')]
    [string] $SshHost
)

$ErrorActionPreference = 'Stop'
$id = [guid]::NewGuid().ToString('N').Substring(0, 8)
$tempDirectory = Join-Path ([System.IO.Path]::GetTempPath()) "kafka3o-persistent-check-$id"
$repo = (Resolve-Path (Join-Path $PSScriptRoot '..\..\..')).Path
$ports = @(19092, 19093)
$forwards = [System.Collections.Generic.List[System.Diagnostics.Process]]::new()
$tempFiles = [System.Collections.Generic.List[string]]::new()
$tempDirectoryCreated = $false
$savedEnvironment = @{}
$environmentNames = @(
    'KAFKA3O_TEST_PERSISTENT_BROKER',
    'KAFKA3O_KAFKA__TLS__ENABLED', 'KAFKA3O_KAFKA__TLS__CAFILE',
    'KAFKA3O_KAFKA__SASL__MECHANISM', 'KAFKA3O_KAFKA__SASL__USERNAME',
    'KAFKA3O_KAFKA__SASL__PASSWORD', 'KAFKA3O_KAFKA_SASL_PASSWORD'
)
foreach ($name in $environmentNames) {
    $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}

function Invoke-Remote([string] $Command) {
    $result = & ssh -o BatchMode=yes -o StrictHostKeyChecking=yes $SshHost $Command
    if ($LASTEXITCODE -ne 0) { throw "Remote command failed: $Command" }
    return $result
}

function Assert-PortsAvailable {
    foreach ($port in $ports) {
        if (Get-NetTCPConnection -State Listen -LocalPort $port -ErrorAction SilentlyContinue) {
            throw "Local port $port is already in use."
        }
    }
    $listeners = @(Invoke-Remote "ss -H -ltn 'sport = :19092 or sport = :19093'")
    if ($listeners.Count -ne 0) { throw 'A required remote loopback port is already in use.' }
}

function Start-Forward([string] $Broker, [int] $Port) {
    $stdout = Join-Path $tempDirectory "$Broker-forward.out"
    $stderr = Join-Path $tempDirectory "$Broker-forward.err"
    $tempFiles.Add($stdout)
    $tempFiles.Add($stderr)
    $arguments = @(
        '-tt', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
        '-o', 'ExitOnForwardFailure=yes',
        '-L', "127.0.0.1:$Port`:127.0.0.1:$Port",
        $SshHost,
        "timeout --signal=TERM --kill-after=5s 300s kubectl -n kafka3o-test-clusters port-forward pod/kafka-$Broker-0 $Port`:9095 --address 127.0.0.1"
    )
    $forward = Start-Process -FilePath 'ssh' -ArgumentList $arguments -PassThru -NoNewWindow `
        -RedirectStandardOutput $stdout -RedirectStandardError $stderr
    $forwards.Add($forward)
    for ($attempt = 0; $attempt -lt 40; $attempt++) {
        $forward.Refresh()
        if ($forward.HasExited) { throw "Broker $Broker forward exited before becoming ready." }
        $socket = [System.Net.Sockets.TcpClient]::new()
        try {
            $connection = $socket.ConnectAsync('127.0.0.1', $Port)
            if ($connection.Wait(500) -and $socket.Connected) { return }
        } catch [System.Management.Automation.MethodInvocationException] {
            $cause = $_.Exception.GetBaseException()
            if ($cause -isnot [System.Net.Sockets.SocketException] -or
                $cause.SocketErrorCode -ne [System.Net.Sockets.SocketError]::ConnectionRefused) {
                throw
            }
        } finally {
            $socket.Dispose()
        }
        Start-Sleep -Milliseconds 500
    }
    throw "Broker $Broker forward did not become ready."
}

$runError = $null
$cleanupErrors = [System.Collections.Generic.List[string]]::new()
try {
    Assert-PortsAvailable
    New-Item -ItemType Directory -Path $tempDirectory | Out-Null
    $tempDirectoryCreated = $true
    foreach ($broker in @(
        @{ Name = 'a'; Port = 19092 },
        @{ Name = 'b'; Port = 19093 }
    )) {
        $name = $broker.Name
        $port = $broker.Port
        Invoke-Remote "kubectl -n kafka3o-test-clusters wait --for=condition=Ready pod/kafka-$name-0 --timeout=120s" | Out-Host
        $certificate = Join-Path $tempDirectory "kafka-$name-ca.pem"
        $tempFiles.Add($certificate)
        $publicCertificate = @(Invoke-Remote "kubectl -n kafka3o-test-clusters exec kafka-$name-0 -c kafka -- cat /run/kafka3o-security/tls.crt")
        if ($publicCertificate.Count -eq 0) { throw "Could not retrieve broker $name's public certificate." }
        [System.IO.File]::WriteAllText($certificate, ($publicCertificate -join "`n") + "`n")
        $password = Invoke-Remote "kubectl -n kafka3o-test-clusters exec kafka-$name-0 -c kafka -- cat /run/kafka3o-security/sasl.password"
        if ($password -isnot [string] -or $password -cnotmatch '^[0-9a-f]{64}$') {
            throw "Could not retrieve broker $name's test credential."
        }

        $env:KAFKA3O_TEST_PERSISTENT_BROKER = "127.0.0.1:$port"
        $env:KAFKA3O_KAFKA__TLS__ENABLED = 'true'
        $env:KAFKA3O_KAFKA__TLS__CAFILE = $certificate
        $env:KAFKA3O_KAFKA__SASL__MECHANISM = 'SCRAM-SHA-512'
        $env:KAFKA3O_KAFKA__SASL__USERNAME = "kafka3otest-$name"
        Remove-Item -LiteralPath 'Env:\KAFKA3O_KAFKA__SASL__PASSWORD' -ErrorAction SilentlyContinue
        $env:KAFKA3O_KAFKA_SASL_PASSWORD = $password
        $password = $null

        Start-Forward $name $port
        Push-Location $repo
        try {
            & go test -count=1 -tags integration .\test\integration -run '^TestPersistentKafka_(Security|AdministrationValidation)$' -v
            if ($LASTEXITCODE -ne 0) { throw "Read-only security checks failed for broker $name." }
        } finally {
            Pop-Location
        }
    }
} catch {
    $runError = $_
} finally {
    foreach ($forward in $forwards) {
        try {
            $forward.Refresh()
            if (-not $forward.HasExited) {
                Stop-Process -Id $forward.Id
                Wait-Process -Id $forward.Id -ErrorAction SilentlyContinue
            }
        } catch {
            $cleanupErrors.Add($_.Exception.Message)
        }
    }
    foreach ($name in $environmentNames) {
        if ($null -eq $savedEnvironment[$name]) {
            Remove-Item -LiteralPath "Env:\$name" -ErrorAction SilentlyContinue
        } else {
            [Environment]::SetEnvironmentVariable($name, $savedEnvironment[$name], 'Process')
        }
    }
    if ($forwards.Count -ne 0) {
        try {
            $closed = $false
            for ($attempt = 0; $attempt -lt 30; $attempt++) {
                $listeners = @(Invoke-Remote "ss -H -ltn 'sport = :19092 or sport = :19093'")
                if ($listeners.Count -eq 0) { $closed = $true; break }
                Start-Sleep -Seconds 1
            }
            if (-not $closed) { throw 'Remote forward ports did not close.' }
        } catch {
            $cleanupErrors.Add($_.Exception.Message)
        }
    }
    if ($tempDirectoryCreated) {
        foreach ($path in $tempFiles) {
            if (Test-Path -LiteralPath $path) {
                try { Remove-Item -LiteralPath $path } catch { $cleanupErrors.Add($_.Exception.Message) }
            }
        }
        if (Test-Path -LiteralPath $tempDirectory) {
            try { Remove-Item -LiteralPath $tempDirectory } catch { $cleanupErrors.Add($_.Exception.Message) }
        }
    }
}
foreach ($message in $cleanupErrors) { Write-Warning "Cleanup: $message" }
if ($null -ne $runError) { throw $runError }
if ($cleanupErrors.Count -ne 0) { throw 'Persistent Kafka test cleanup was incomplete.' }
