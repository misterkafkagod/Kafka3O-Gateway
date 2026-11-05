param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[A-Za-z0-9][A-Za-z0-9._@:-]*$')]
    [string] $SshHost
)

$ErrorActionPreference = 'Stop'
$id = [guid]::NewGuid().ToString('N').Substring(0, 8)
$namespace = "kafka3o-sec-$id"
$tempDirectory = Join-Path ([System.IO.Path]::GetTempPath()) "kafka3o-sec-$id"
$certificate = Join-Path $tempDirectory 'broker.crt'
$stdout = Join-Path $tempDirectory 'forward.out'
$stderr = Join-Path $tempDirectory 'forward.err'
$repo = (Resolve-Path (Join-Path $PSScriptRoot '..\..\..')).Path
$manifest = Get-Content -Raw (Join-Path $PSScriptRoot 'secure-kafka-pod.yaml')
$ports = @(19092, 19095, 19096)
$tempDirectoryCreated = $false
$namespaceCreated = $false
$namespaceUid = $null
$forward = $null
$savedEnvironment = @{}
$environmentNames = @(
    'KAFKA3O_TEST_SECURE_BROKER', 'KAFKA3O_TEST_TLS_BROKER', 'KAFKA3O_TEST_SASL_BROKER',
    'KAFKA3O_TEST_SECURE_CA', 'KAFKA3O_TEST_SECURE_USERNAME', 'KAFKA3O_TEST_DISPOSABLE',
    'KAFKA3O_KAFKA__TLS__ENABLED', 'KAFKA3O_KAFKA__TLS__CAFILE',
    'KAFKA3O_KAFKA__SASL__MECHANISM', 'KAFKA3O_KAFKA__SASL__USERNAME',
    'KAFKA3O_KAFKA_SASL_PASSWORD'
)
foreach ($name in $environmentNames) {
    $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}

function Invoke-Remote([string] $Command, [string] $InputText = '') {
    if ($InputText -ne '') {
        $result = $InputText | & ssh -o BatchMode=yes -o StrictHostKeyChecking=yes $SshHost $Command
    } else {
        $result = & ssh -o BatchMode=yes -o StrictHostKeyChecking=yes $SshHost $Command
    }
    if ($LASTEXITCODE -ne 0) { throw "Remote command failed: $Command" }
    return $result
}

function Assert-PortsAvailable {
    foreach ($port in $ports) {
        if (Get-NetTCPConnection -State Listen -LocalPort $port -ErrorAction SilentlyContinue) {
            throw "Local port $port is already in use."
        }
    }
    $listeners = @(Invoke-Remote "ss -H -ltn 'sport = :19092 or sport = :19095 or sport = :19096'")
    if ($listeners.Count -ne 0) { throw 'A required remote loopback port is already in use.' }
}

function Stop-Forward {
    if ($null -ne $script:forward) {
        $script:forward.Refresh()
        if (-not $script:forward.HasExited) {
            Stop-Process -Id $script:forward.Id
            Wait-Process -Id $script:forward.Id -ErrorAction SilentlyContinue
        }
        $script:forward = $null
    }
    for ($attempt = 0; $attempt -lt 30; $attempt++) {
        $listeners = @(Invoke-Remote "ss -H -ltn 'sport = :19092 or sport = :19095 or sport = :19096'")
        if ($listeners.Count -eq 0) { return }
        Start-Sleep -Seconds 1
    }
    throw 'Remote forward ports did not close; inspect the owned forward before retrying.'
}

function Start-Forward {
    Assert-PortsAvailable
    $arguments = @(
        '-tt', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
        '-o', 'ExitOnForwardFailure=yes',
        '-L', '127.0.0.1:19092:127.0.0.1:19092',
        '-L', '127.0.0.1:19095:127.0.0.1:19095',
        '-L', '127.0.0.1:19096:127.0.0.1:19096',
        $SshHost,
        "timeout --signal=TERM --kill-after=5s 480s kubectl -n $namespace port-forward pod/kafka3o-secure 19092:9092 19095:9095 19096:9096 --address 127.0.0.1"
    )
    $script:forward = Start-Process -FilePath 'ssh' -ArgumentList $arguments -PassThru -NoNewWindow `
        -RedirectStandardOutput $stdout -RedirectStandardError $stderr
    for ($attempt = 0; $attempt -lt 40; $attempt++) {
        $script:forward.Refresh()
        if ($script:forward.HasExited) { throw 'The broker forward exited before becoming ready.' }
        $ready = $true
        foreach ($port in $ports) {
            $socket = [System.Net.Sockets.TcpClient]::new()
            try {
                $connection = $socket.ConnectAsync('127.0.0.1', $port)
                if (-not $connection.Wait(500) -or -not $socket.Connected) { $ready = $false }
            } catch {
                $ready = $false
            } finally {
                $socket.Dispose()
            }
        }
        if ($ready) { return }
        Start-Sleep -Milliseconds 500
    }
    throw 'The broker forward did not become ready.'
}

function Assert-NamespaceOwnership {
    $currentUid = Invoke-Remote "kubectl get namespace $namespace -o jsonpath='{.metadata.uid}'"
    $owned = @(Invoke-Remote "kubectl get namespaces -l kafka3o.dev/session=$id -o name")
    if ($currentUid -ne $namespaceUid -or $owned -notcontains "namespace/$namespace") {
        throw "Namespace $namespace ownership changed; refusing to delete it."
    }
}

$runError = $null
$cleanupErrors = [System.Collections.Generic.List[string]]::new()
try {
    Assert-PortsAvailable
    New-Item -ItemType Directory -Path $tempDirectory | Out-Null
    $tempDirectoryCreated = $true
    $namespaceManifest = @"
apiVersion: v1
kind: Namespace
metadata:
  name: $namespace
  labels:
    kafka3o.dev/session: $id
    kafka3o.dev/purpose: secure-integration
"@
    Invoke-Remote 'kubectl create -f -' $namespaceManifest | Out-Host
    $namespaceCreated = $true
    $namespaceUid = Invoke-Remote "kubectl get namespace $namespace -o jsonpath='{.metadata.uid}'"
    if ([string]::IsNullOrWhiteSpace($namespaceUid)) { throw 'Could not verify the new namespace UID.' }

    $tests = @(
        @{ Package = '.\test\integration'; Name = '^TestKafkaSecurity_RealBroker$' },
        @{ Package = '.\cmd\gateway'; Name = '^TestGateway_AuthenticatedKafkaWriteLifecycle$' }
    )
    foreach ($test in $tests) {
        Invoke-Remote "kubectl -n $namespace create -f -" $manifest | Out-Host
        Invoke-Remote "kubectl -n $namespace wait --for=condition=Ready pod/kafka3o-secure --timeout=200s" | Out-Host
        $publicCertificate = @(Invoke-Remote "kubectl -n $namespace exec kafka3o-secure -- cat /run/kafka3o/server.crt")
        if ($publicCertificate.Count -eq 0) { throw 'Could not retrieve the test broker certificate.' }
        [System.IO.File]::WriteAllText($certificate, ($publicCertificate -join "`n") + "`n")
        $password = Invoke-Remote "kubectl -n $namespace exec kafka3o-secure -- cat /run/kafka3o/password"
        if ([string]::IsNullOrWhiteSpace($password)) { throw 'Could not retrieve the disposable test credential.' }

        $env:KAFKA3O_TEST_SECURE_BROKER = '127.0.0.1:19092'
        $env:KAFKA3O_TEST_TLS_BROKER = '127.0.0.1:19095'
        $env:KAFKA3O_TEST_SASL_BROKER = '127.0.0.1:19096'
        $env:KAFKA3O_TEST_SECURE_CA = $certificate
        $env:KAFKA3O_TEST_SECURE_USERNAME = 'kafka3otest'
        $env:KAFKA3O_TEST_DISPOSABLE = 'true'
        $env:KAFKA3O_KAFKA__TLS__ENABLED = 'true'
        $env:KAFKA3O_KAFKA__TLS__CAFILE = $certificate
        $env:KAFKA3O_KAFKA__SASL__MECHANISM = 'SCRAM-SHA-512'
        $env:KAFKA3O_KAFKA__SASL__USERNAME = 'kafka3otest'
        $env:KAFKA3O_KAFKA_SASL_PASSWORD = $password
        $password = $null

        Start-Forward
        Push-Location $repo
        try {
            & go test -count=1 -tags integration $test.Package -run $test.Name -v
            if ($LASTEXITCODE -ne 0) { throw "Integration test failed: $($test.Name)" }
        } finally {
            Pop-Location
        }
        Stop-Forward
        Assert-NamespaceOwnership
        Invoke-Remote "kubectl -n $namespace delete pod kafka3o-secure --wait=true --timeout=120s" | Out-Host
    }
} catch {
    $runError = $_
} finally {
    if ($null -ne $forward) {
        try { Stop-Forward } catch { $cleanupErrors.Add($_.Exception.Message) }
    }
    foreach ($name in $environmentNames) {
        [Environment]::SetEnvironmentVariable($name, $savedEnvironment[$name], 'Process')
    }
    if ($namespaceCreated) {
        try {
            Assert-NamespaceOwnership
            Invoke-Remote "kubectl delete namespace $namespace --wait=true --timeout=120s" | Out-Host
        } catch {
            $cleanupErrors.Add($_.Exception.Message)
        }
    }
    if ($tempDirectoryCreated) {
        foreach ($path in @($certificate, $stdout, $stderr)) {
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
if ($cleanupErrors.Count -ne 0) { throw 'Secured Kafka test cleanup was incomplete.' }
