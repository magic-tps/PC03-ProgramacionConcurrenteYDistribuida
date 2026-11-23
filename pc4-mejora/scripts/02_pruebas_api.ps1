# Pruebas funcionales y de latencia de la API (requisito: < 100 ms).
# Ejecutar con el stack recién levantado (01_levantar.ps1 -Limpio) para registrar también el 409 previo al entrenamiento.
. "$PSScriptRoot\_comun.ps1"

$r = [ordered]@{}
$r.fecha = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ss")
$r.entorno = Get-Entorno
Write-Host ("Entorno: {0} | {1} núcleos / {2} hilos | {3} GB RAM | {4}" -f $r.entorno.cpu, $r.entorno.nucleosFisicos, $r.entorno.hilosLogicos, $r.entorno.ramGB, $r.entorno.docker) -ForegroundColor Cyan

Write-Host "1) Estado inicial" -ForegroundColor Cyan
$h = Invoke-Api GET "/api/v1/health"
$r.health_inicial = [ordered]@{ status = $h.Status; clientMs = $h.ClientMs; body = $h.Json }

Write-Host "2) Predicción ANTES de entrenar (se espera 409, sin probabilidades inventadas)" -ForegroundColor Cyan
$p0 = Invoke-Api POST "/api/v1/predict" @{ district = "SAN JUAN DE LURIGANCHO"; franja = 2; mes = 10; anio = 2026 }
$r.predict_antes_de_entrenar = [ordered]@{ status = $p0.Status; body = $p0.Json }
Write-Host ("   HTTP {0} {1}" -f $p0.Status, $p0.Json.code)

Write-Host "3) Entrenamiento distribuido (POST /api/v1/train)" -ForegroundColor Cyan
$t = Invoke-Api POST "/api/v1/train" @{}
if ($t.Status -ne 200) { throw "El entrenamiento falló: HTTP $($t.Status) $($t.Raw)" }
$r.train = [ordered]@{ status = $t.Status; clientMs = $t.ClientMs; serverMs = $t.ServerMs; body = $t.Json }
$ev = $t.Json.evaluation
Write-Host ("   {0} nodos, {1} árboles, {2:N0} denuncias de entrenamiento, {3} ms" -f $t.Json.nodeCount, $t.Json.totalTrees, $t.Json.trainRecords, $t.Json.durationMs)
foreach ($n in $t.Json.nodes) {
    Write-Host ("   {0}: shard {1}/{2}, {3} árboles, {4:N0} denuncias, {5} goroutines, entrenamiento {6} ms" -f $n.nodeId, ($n.result.shard + 1), $n.result.shards, $n.result.trees, $n.result.trainRecords, $n.result.workers, $n.result.trainMs)
}
Write-Host ("   Evaluación 2026: exactitud {0:P2} vs línea base {1:P2} ({2:N0} denuncias)" -f $ev.accuracy, $ev.baselineAccuracy, $ev.testRecords) -ForegroundColor Green

# 20 combinaciones distintas (10 distritos x 4 franjas, mes 10/2026): todas son "frías" (no están en caché).
$distritos = @("SAN JUAN DE LURIGANCHO", "LIMA", "CALLAO", "COMAS", "SAN MARTIN DE PORRES", "ATE", "LOS OLIVOS", "VILLA MARIA DEL TRIUNFO", "SANTIAGO DE SURCO", "VILLA EL SALVADOR")
$consultas = @()
for ($i = 0; $i -lt 20; $i++) { $consultas += @{ district = $distritos[$i % 10]; franja = ($i % 4); mes = 10; anio = 2026 } }

function Invoke-Serie($nombre) {
    $filas = @()
    foreach ($c in $consultas) {
        $p = Invoke-Api POST "/api/v1/predict" $c
        $filas += [pscustomobject][ordered]@{
            district = $c.district; franja = $c.franja; status = $p.Status; fromCache = $p.Json.fromCache
            nodesUsed = $p.Json.nodesUsed; treesUsed = $p.Json.treesUsed; topClass = $p.Json.topClass
            topProbability = [math]::Round([double]$p.Json.topProbability, 4); topStreetCrime = $p.Json.topStreetCrime
            clientMs = $p.ClientMs; serverMs = $p.ServerMs; clusterMs = $p.Json.clusterMs
        }
    }
    return ,$filas
}

Write-Host "4) 20 predicciones SIN caché (consultan a los 3 nodos por TCP)" -ForegroundColor Cyan
$frias = Invoke-Serie "frias"
Write-Host "5) Las mismas 20 predicciones CON caché (Redis)" -ForegroundColor Cyan
$calidas = Invoke-Serie "calidas"
$r.predicciones_sin_cache = $frias
$r.predicciones_con_cache = $calidas
$r.resumen_latencia = [ordered]@{
    sin_cache_cliente = Get-Resumen ($frias | ForEach-Object { $_.clientMs })
    sin_cache_servidor = Get-Resumen ($frias | ForEach-Object { $_.serverMs })
    con_cache_cliente = Get-Resumen ($calidas | ForEach-Object { $_.clientMs })
    con_cache_servidor = Get-Resumen ($calidas | ForEach-Object { $_.serverMs })
    nota = "cliente = Stopwatch en PowerShell (incluye HTTP local); servidor = cabecera X-Response-Time-Ms"
}
$frias | Select-Object district, franja, fromCache, nodesUsed, treesUsed, topClass, topProbability, clientMs, serverMs | Format-Table -AutoSize
$s = $r.resumen_latencia
Write-Host ("   Sin caché: promedio {0} ms, p95 {1} ms, máximo {2} ms (cliente)" -f $s.sin_cache_cliente.promedioMs, $s.sin_cache_cliente.p95Ms, $s.sin_cache_cliente.maximoMs) -ForegroundColor Green
Write-Host ("   Con caché: promedio {0} ms, p95 {1} ms, máximo {2} ms (cliente)" -f $s.con_cache_cliente.promedioMs, $s.con_cache_cliente.p95Ms, $s.con_cache_cliente.maximoMs) -ForegroundColor Green

Write-Host "6) Ejemplo completo de predicción" -ForegroundColor Cyan
$ej = Invoke-Api POST "/api/v1/predict" @{ district = "San Juan de Lurigancho"; franja = 2; mes = 10; anio = 2026 }
$r.ejemplo_prediccion = $ej.Json
$ej.Json.probabilities | Format-Table -AutoSize

Write-Host "7) Validaciones (códigos de error)" -ForegroundColor Cyan
$casos = @(
    @{ nombre = "franja fuera de rango"; metodo = "POST"; ruta = "/api/v1/predict"; body = @{ district = "MIRAFLORES"; franja = 7; mes = 10; anio = 2026 }; esperado = 400 },
    @{ nombre = "mes fuera de rango"; metodo = "POST"; ruta = "/api/v1/predict"; body = @{ district = "MIRAFLORES"; franja = 1; mes = 13; anio = 2026 }; esperado = 400 },
    @{ nombre = "falta un campo"; metodo = "POST"; ruta = "/api/v1/predict"; body = @{ district = "MIRAFLORES"; franja = 1 }; esperado = 400 },
    @{ nombre = "distrito inexistente"; metodo = "POST"; ruta = "/api/v1/predict"; body = @{ district = "NARNIA"; franja = 1; mes = 10; anio = 2026 }; esperado = 404 },
    @{ nombre = "método no permitido"; metodo = "GET"; ruta = "/api/v1/predict"; body = $null; esperado = 405 },
    @{ nombre = "patrullaje sin parámetros"; metodo = "GET"; ruta = "/api/v1/patrullaje"; body = $null; esperado = 400 }
)
$validaciones = @()
foreach ($c in $casos) {
    $v = Invoke-Api $c.metodo $c.ruta $c.body
    $ok = ($v.Status -eq $c.esperado)
    $validaciones += [pscustomobject][ordered]@{ caso = $c.nombre; esperado = $c.esperado; obtenido = $v.Status; correcto = $ok; codigo = $v.Json.code; clientMs = $v.ClientMs }
}
$r.validaciones = $validaciones
$validaciones | Format-Table -AutoSize

Write-Host "8) Matriz de patrullaje (pregunta de impacto social)" -ForegroundColor Cyan
$m1 = Invoke-Api GET "/api/v1/patrullaje?mes=10&anio=2026&top=10"
$m2 = Invoke-Api GET "/api/v1/patrullaje?mes=10&anio=2026&top=10"
$r.patrullaje = [ordered]@{ sin_cache = [ordered]@{ status = $m1.Status; clientMs = $m1.ClientMs; serverMs = $m1.ServerMs; body = $m1.Json }
                            con_cache = [ordered]@{ status = $m2.Status; clientMs = $m2.ClientMs; serverMs = $m2.ServerMs; fromCache = $m2.Json.fromCache } }
Write-Host ("   200 predicciones en un lote: {0} ms sin caché, {1} ms con caché (cliente)" -f $m1.ClientMs, $m2.ClientMs) -ForegroundColor Green
$m1.Json.rows | Select-Object rank, district, zone, historicalStreetCrimes, priorityFranja, priorityCrime, @{n = "prob"; e = { [math]::Round($_.priorityProbability, 3) } } | Format-Table -AutoSize

Write-Host "9) Métricas y estado final" -ForegroundColor Cyan
Start-Sleep -Seconds 1
$mt = Invoke-Api GET "/api/v1/metrics"
$hf = Invoke-Api GET "/api/v1/health"
$r.metrics_final = $mt.Json
$r.health_final = $hf.Json
Write-Host ("   MongoDB: {0} | Redis: {1} claves" -f (($hf.Json.storage.collections.PSObject.Properties | ForEach-Object { "$($_.Name)=$($_.Value)" }) -join ", "), $hf.Json.storage.cacheKeys)

Save-Json $r "02_pruebas_api.json"
Write-Host "`nListo. Ahora puedes ejecutar 03_resiliencia.ps1 y 04_evidencias_bd.ps1" -ForegroundColor Yellow
