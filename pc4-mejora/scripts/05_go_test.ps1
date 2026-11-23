# Pruebas automáticas de Go con el detector de carreras, ejecutadas dentro de un
# contenedor golang:1.24 (no hace falta tener Go ni gcc instalados en Windows).
. "$PSScriptRoot\_comun.ps1"
Write-Host "go vet + go test -race (tarda 2-4 minutos: usa el dataset real)..." -ForegroundColor Cyan
$out = Join-Path $Evid "05_go_test.txt"
docker run --rm -v "${Root}:/src" -v pc4-gomod:/go/pkg/mod -w /src -e GOFLAGS=-mod=mod -e CGO_ENABLED=1 golang:1.24 sh -c "go version && go vet ./... && echo 'go vet: sin observaciones' && go test -race -count=1 -v ./... 2>&1" | Tee-Object -FilePath $out
Write-Host "`nResumen:" -ForegroundColor Cyan
Select-String -Path $out -Pattern "^(ok|FAIL|---|PASS)|DATA RACE" | ForEach-Object { $_.Line }
