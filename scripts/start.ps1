param(
    [string]$JavaHome = $env:JAVA_HOME,
    [switch]$SkipBuild
)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
if (-not $JavaHome -or -not (Test-Path -LiteralPath (Join-Path $JavaHome 'bin\java.exe'))) {
    throw 'Provide a Java 25 installation with -JavaHome.'
}
$javaPath = Join-Path $JavaHome 'bin\java.exe'
$version = & $javaPath -version 2>&1 | Out-String
if ($version -notmatch 'version "25[.\"]') { throw 'This project requires Java 25.' }
$env:JAVA_HOME = $JavaHome
$env:PATH = (Join-Path $JavaHome 'bin') + [IO.Path]::PathSeparator + $env:PATH
$env:MAVEN_OPTS = '-Dfile.encoding=UTF-8'
& (Join-Path $PSScriptRoot 'init.ps1')
. (Join-Path $PSScriptRoot 'env.ps1')
Push-Location $projectRoot
try {
    docker compose up -d --wait
    if ($LASTEXITCODE -ne 0) { throw 'Dependency startup failed.' }
    if (-not $SkipBuild) {
        Push-Location (Join-Path $projectRoot 'web')
        try {
            npm.cmd ci --no-fund --no-audit
            if ($LASTEXITCODE -ne 0) { throw 'Frontend dependency installation failed.' }
            npm.cmd run build
            if ($LASTEXITCODE -ne 0) { throw 'Frontend build failed.' }
        } finally { Pop-Location }
        Push-Location (Join-Path $projectRoot 'backend')
        try {
            mvn -s ../scripts/maven-settings.xml -B -ntp -DskipTests package
            if ($LASTEXITCODE -ne 0) { throw 'Backend build failed.' }
        } finally { Pop-Location }
    }
    $env:DEMO_ENABLED = 'true'
    $env:SERVER_ADDRESS = '127.0.0.1'
    Write-Host 'Open http://127.0.0.1:18080. Press Ctrl+C to stop the application.'
    & $javaPath '-Dfile.encoding=UTF-8' -jar (Join-Path $projectRoot 'backend\target\flash-sale-service-0.1.0.jar')
    if ($LASTEXITCODE -ne 0) { throw 'Application exited with an error.' }
} finally { Pop-Location }
