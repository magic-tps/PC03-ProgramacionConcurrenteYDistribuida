# Construye las imágenes y levanta los 6 contenedores (MongoDB, Redis, coordinador y 3 nodos ML).
# Uso:  .\scripts\01_levantar.ps1            (conserva los datos de MongoDB)
#       .\scripts\01_levantar.ps1 -Limpio    (borra el volumen de MongoDB para empezar de cero)
param([switch]$Limpio)
. "$PSScriptRoot\_comun.ps1"

$csv = Join-Path $Root "data\DATASET_Denuncias Policiales - Enero 2018 a Septiembre 2026.csv"
if (-not (Test-Path $csv)) {
    $original = Join-Path (Split-Path $Root -Parent) "DATASET_Denuncias Policiales - Enero 2018 a Septiembre 2026.csv"
    if (-not (Test-Path $original)) { throw "No se encuentra el CSV en data\ ni en la carpeta del repositorio" }
    Write-Host "Copiando el CSV oficial a data\ ..." -ForegroundColor Cyan
    New-Item -ItemType Directory -Force -Path (Join-Path $Root "data") | Out-Null
    Copy-Item $original $csv
}

if ($Limpio) {
    Write-Host "0) Eliminando contenedores y volumen de MongoDB anteriores..." -ForegroundColor Cyan
    docker compose down -v
}
Write-Host "1) Construyendo e iniciando los contenedores (la primera vez descarga imágenes y módulos de Go)..." -ForegroundColor Cyan
$inicio = Get-Date
docker compose up -d --build
if ($LASTEXITCODE -ne 0) { throw "docker compose up falló (¿Docker Desktop está abierto? ¿puertos 8080/27017/6379 libres?)" }

Write-Host "2) Esperando a que los 3 nodos ML se registren en el coordinador..." -ForegroundColor Cyan
$h = Wait-Api -Condicion { param($j) $j.nodesConnected -ge 3 } -Segundos 240 -Mensaje "registro de nodos"
$seg = [math]::Round(((Get-Date) - $inicio).TotalSeconds, 1)
Write-Host ("   Listo en {0} s: {1} nodos ({2}); MongoDB {3}; Redis {4}" -f $seg, $h.Json.nodesConnected, ($h.Json.nodes -join ", "), $h.Json.storage.mongo, $h.Json.storage.redis) -ForegroundColor Green

docker compose ps
docker compose ps | Out-File -Encoding utf8 (Join-Path $Evid "01_docker_compose_ps.txt")
Write-Host "`nSwagger UI: http://localhost:8080/swagger" -ForegroundColor Yellow
