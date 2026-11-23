# Funciones comunes de los scripts de evidencias (compatibles con Windows PowerShell 5.1 y PowerShell 7).
# "Continue": en Windows PowerShell 5.1, "Stop" convierte la salida de error de docker en excepción.
$ErrorActionPreference = "Continue"
$ProgressPreference = "SilentlyContinue"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$Root = Split-Path $PSScriptRoot -Parent
$Evid = Join-Path $Root "evidencias"
$Base = "http://localhost:8080"
New-Item -ItemType Directory -Force -Path $Evid | Out-Null
Set-Location $Root

Add-Type -AssemblyName System.Net.Http
$script:Client = New-Object System.Net.Http.HttpClient
$script:Client.Timeout = [TimeSpan]::FromMinutes(5)

# Llama a la API y mide el tiempo en el cliente (Stopwatch) y en el servidor (cabecera X-Response-Time-Ms).
function Invoke-Api {
    param([string]$Method, [string]$Path, $Body = $null)
    $req = New-Object System.Net.Http.HttpRequestMessage ([System.Net.Http.HttpMethod]::new($Method)), "$Base$Path"
    if ($null -ne $Body) {
        if ($Body -is [string]) { $json = $Body } else { $json = $Body | ConvertTo-Json -Depth 10 -Compress }
        $req.Content = New-Object System.Net.Http.StringContent $json, ([System.Text.Encoding]::UTF8), "application/json"
    }
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    $resp = $script:Client.SendAsync($req).GetAwaiter().GetResult()
    $text = $resp.Content.ReadAsStringAsync().GetAwaiter().GetResult()
    $sw.Stop()
    $serverMs = $null
    $vals = $null
    if ($resp.Headers.TryGetValues("X-Response-Time-Ms", [ref]$vals)) { $serverMs = [double]($vals | Select-Object -First 1) }
    $obj = $null
    if ($text) { try { $obj = $text | ConvertFrom-Json } catch { $obj = $null } }
    [pscustomobject]@{
        Status   = [int]$resp.StatusCode
        ClientMs = [math]::Round($sw.Elapsed.TotalMilliseconds, 3)
        ServerMs = $serverMs
        Json     = $obj
        Raw      = $text
    }
}

function Get-Resumen {
    param([double[]]$Valores)
    if (-not $Valores -or $Valores.Count -eq 0) { return $null }
    $o = $Valores | Sort-Object
    $n = $o.Count
    [ordered]@{
        n           = $n
        promedioMs  = [math]::Round(($o | Measure-Object -Average).Average, 3)
        minimoMs    = [math]::Round($o[0], 3)
        p50Ms       = [math]::Round($o[[int][math]::Floor(0.50 * ($n - 1))], 3)
        p95Ms       = [math]::Round($o[[int][math]::Floor(0.95 * ($n - 1))], 3)
        maximoMs    = [math]::Round($o[$n - 1], 3)
        todosMenor100ms = ($o[$n - 1] -lt 100)
    }
}

function Get-Entorno {
    $cpu = Get-CimInstance Win32_Processor | Select-Object -First 1
    $cs = Get-CimInstance Win32_ComputerSystem
    $os = Get-CimInstance Win32_OperatingSystem
    $dockerServer = (docker version --format "{{.Server.Version}}" 2>$null)
    $dockerInfo = (docker info --format "{{.NCPU}} CPU, {{.MemTotal}} bytes, {{.OperatingSystem}}" 2>$null)
    $compose = (docker compose version --short 2>$null)
    [ordered]@{
        equipo           = $env:COMPUTERNAME
        sistemaOperativo = "$($os.Caption) $($os.Version)"
        cpu              = $cpu.Name.Trim()
        nucleosFisicos   = $cpu.NumberOfCores
        hilosLogicos     = $cpu.NumberOfLogicalProcessors
        ramGB            = [math]::Round($cs.TotalPhysicalMemory / 1GB, 1)
        docker           = "Docker Engine $dockerServer; Compose $compose"
        dockerVM         = $dockerInfo
        despliegue       = "Local: Docker Desktop en el equipo del integrante (no en la nube)"
    }
}

function Save-Json {
    param($Objeto, [string]$Archivo)
    $ruta = Join-Path $Evid $Archivo
    $Objeto | ConvertTo-Json -Depth 30 | Out-File -Encoding utf8 $ruta
    Write-Host "  -> guardado evidencias\$Archivo" -ForegroundColor DarkGray
}

function Wait-Api {
    param([scriptblock]$Condicion, [int]$Segundos = 120, [string]$Mensaje = "esperando")
    $limite = (Get-Date).AddSeconds($Segundos)
    while ((Get-Date) -lt $limite) {
        try {
            $h = Invoke-Api GET "/api/v1/health"
            if ($h.Status -eq 200 -and (& $Condicion $h.Json)) { return $h }
        } catch { }
        Write-Host "  ... $Mensaje" -ForegroundColor DarkGray
        Start-Sleep -Seconds 2
    }
    throw "Tiempo agotado: $Mensaje"
}
