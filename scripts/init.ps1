param()
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$envPath = Join-Path $projectRoot '.env'
if (-not (Test-Path -LiteralPath $envPath)) {
    $keys = @('MYSQL_ROOT_PASSWORD','MYSQL_PASSWORD','REDIS_PASSWORD','RABBITMQ_PASSWORD','SALE_CLIENT_SECRET','SALE_PARTNER_SECRET')
    $lines = foreach ($key in $keys) {
        $bytes = New-Object byte[] 32
        [System.Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
        "$key=$([Convert]::ToHexString($bytes).ToLowerInvariant())"
    }
    [System.IO.File]::WriteAllLines($envPath, $lines, [System.Text.UTF8Encoding]::new($false))
    Write-Host 'Created local credentials in .env. Keep this file private.'
} else {
    Write-Host 'Using existing local credentials.'
}
