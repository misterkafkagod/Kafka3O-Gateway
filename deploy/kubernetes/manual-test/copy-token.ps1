param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('a', 'b')]
    [string] $Broker,

    [ValidatePattern('^[A-Za-z0-9][A-Za-z0-9._@:-]*$')]
    [string] $SshHost = 'john@192.168.1.109'
)

$ErrorActionPreference = 'Stop'
if ([Threading.Thread]::CurrentThread.GetApartmentState() -ne [Threading.ApartmentState]::STA) {
    throw 'Run this helper in an STA PowerShell session.'
}

$token = & ssh -o BatchMode=yes -o StrictHostKeyChecking=yes $SshHost `
    "kubectl -n kafka3o-test-clusters exec deployment/kafka3o-manual -c gateway-$Broker -- cat /run/gateway/token"
if ($LASTEXITCODE -ne 0 -or $token -isnot [string] -or $token -cnotmatch '^[0-9a-f]{64}$') {
    throw "Could not retrieve the in-memory token for Kafka $Broker."
}

Add-Type -AssemblyName System.Windows.Forms
$data = [System.Windows.Forms.DataObject]::new()
$data.SetData([System.Windows.Forms.DataFormats]::UnicodeText, $false, $token)
$data.SetData('ExcludeClipboardContentFromMonitorProcessing', $false, [byte[]]@(1))
try {
    [System.Windows.Forms.Clipboard]::SetDataObject($data, $true)
} finally {
    $token = $null
}
Write-Output "Copied the test token for Kafka $Broker. Paste it into that Gateway's credential field, then clear the clipboard."
