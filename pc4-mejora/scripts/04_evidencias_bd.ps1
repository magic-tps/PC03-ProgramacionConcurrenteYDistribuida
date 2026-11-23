# Evidencias de contenedores y bases de datos: docker compose ps, docker stats,
# documentos de MongoDB (mongosh) y claves de Redis (redis-cli). Ejecutar después de 02 y 03.
. "$PSScriptRoot\_comun.ps1"

Write-Host "1) Contenedores" -ForegroundColor Cyan
docker compose ps | Tee-Object -FilePath (Join-Path $Evid "04_docker_compose_ps.txt")
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}" | Tee-Object -FilePath (Join-Path $Evid "04_docker_stats.txt")

Write-Host "2) MongoDB (base pc4_denuncias)" -ForegroundColor Cyan
$js = @'
db = db.getSiblingDB('pc4_denuncias');
print('Base de datos: pc4_denuncias');
db.getCollectionNames().sort().forEach(function (c) { print('  ' + c + ': ' + db.getCollection(c).countDocuments() + ' documentos'); });
print('');
print('--- training_jobs (ultimo entrenamiento, resumido) ---');
printjson(db.training_jobs.find({}, { _id: 1, jobId: 1, version: 1, createdAt: 1, durationMs: 1, nodeCount: 1, totalTrees: 1, trainRecords: 1, 'nodes.nodeId': 1, 'nodes.result.shard': 1, 'nodes.result.trees': 1, 'nodes.result.trainRecords': 1, 'nodes.result.trainMs': 1, 'evaluation.accuracy': 1, 'evaluation.baselineAccuracy': 1, 'evaluation.testRecords': 1 }).sort({ createdAt: -1 }).limit(1).toArray());
print('');
print('--- predictions (ultimas 2) ---');
printjson(db.predictions.find({}).sort({ createdAt: -1 }).limit(2).toArray());
print('');
print('--- patrol_recommendations (ultima, primeras 2 filas) ---');
printjson(db.patrol_recommendations.find({}, { mes: 1, anio: 1, modelVersion: 1, nodesUsed: 1, treesUsed: 1, queries: 1, createdAt: 1, rows: { $slice: 2 } }).sort({ createdAt: -1 }).limit(1).toArray());
print('');
print('--- cluster_metrics (ultima instantanea, resumida) ---');
printjson(db.cluster_metrics.find({}, { timestamp: 1, requests: 1, cacheHitPct: 1, latency: 1, 'nodes.id': 1, 'nodes.stats.trees': 1 }).sort({ timestamp: -1 }).limit(1).toArray());
'@
# El script (solo ASCII) se envía por la entrada estándar al contenedor y luego se ejecuta con mongosh.
$js | docker compose exec -T mongo sh -c "cat > /tmp/pc4_consultas.js"
docker compose exec -T mongo mongosh --quiet /tmp/pc4_consultas.js | Tee-Object -FilePath (Join-Path $Evid "04_mongodb.txt")

Write-Host "3) Redis" -ForegroundColor Cyan
$salida = @()
$salida += "> DBSIZE"
$salida += (docker compose exec -T redis redis-cli DBSIZE)
$salida += "> INFO keyspace"
$salida += (docker compose exec -T redis redis-cli INFO keyspace)
$salida += "> SCAN de claves pc4:* (primeras 25)"
$claves = @(docker compose exec -T redis redis-cli --scan --pattern "pc4:*" | Sort-Object)
$salida += ($claves | Select-Object -First 25)
$pred = $claves | Where-Object { $_ -like "pc4:pred:*" } | Select-Object -First 1
if ($pred) {
    $salida += "> TTL $pred   (segundos que le quedan en caché)"
    $salida += (docker compose exec -T redis redis-cli TTL "$pred")
    $salida += "> GET $pred"
    $salida += (docker compose exec -T redis redis-cli GET "$pred")
}
$salida += "> GET pc4:model:current"
$salida += (docker compose exec -T redis redis-cli GET "pc4:model:current")
$salida | Tee-Object -FilePath (Join-Path $Evid "04_redis.txt")

Write-Host "4) Logs (últimas líneas de cada servicio)" -ForegroundColor Cyan
docker compose logs --no-color --tail 40 coordinator ml-node-1 ml-node-2 ml-node-3 | Out-File -Encoding utf8 (Join-Path $Evid "04_logs.txt")
Write-Host "  -> guardado evidencias\04_logs.txt" -ForegroundColor DarkGray
