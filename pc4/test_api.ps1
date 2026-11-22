$base = "http://127.0.0.1:8080"
$body = '{"district":"SAN JUAN DE LURIGANCHO","franja":2,"mes":11,"anio":2026}'

$health = Invoke-RestMethod -Method Get -Uri "$base/api/v1/health"
$train = Invoke-RestMethod -Method Post -Uri "$base/api/v1/train" -ContentType "application/json" -Body "{}"
$cold = Measure-Command {
    $coldResponse = Invoke-RestMethod -Method Post -Uri "$base/api/v1/predict" -ContentType "application/json" -Body $body
}
$warm = Measure-Command {
    $warmResponse = Invoke-RestMethod -Method Post -Uri "$base/api/v1/predict" -ContentType "application/json" -Body $body
}

$runs = @()
for ($i = 0; $i -lt 20; $i++) {
    $m = Measure-Command {
        Invoke-RestMethod -Method Post -Uri "$base/api/v1/predict" -ContentType "application/json" -Body $body | Out-Null
    }
    $runs += [double]$m.TotalMilliseconds
}

[PSCustomObject]@{
    Timestamp = (Get-Date).ToString("s")
    Health = $health
    Train = $train
    ColdMs = [math]::Round($cold.TotalMilliseconds, 3)
    WarmMs = [math]::Round($warm.TotalMilliseconds, 3)
    Average20Ms = [math]::Round(($runs | Measure-Object -Average).Average, 3)
    Min20Ms = [math]::Round(($runs | Measure-Object -Minimum).Minimum, 3)
    Max20Ms = [math]::Round(($runs | Measure-Object -Maximum).Maximum, 3)
    ColdResponse = $coldResponse
    WarmResponse = $warmResponse
} | ConvertTo-Json -Depth 8
