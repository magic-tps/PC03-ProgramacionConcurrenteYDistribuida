# Detiene el stack. Con -Borrar también elimina el volumen de MongoDB.
param([switch]$Borrar)
. "$PSScriptRoot\_comun.ps1"
if ($Borrar) { docker compose down -v } else { docker compose down }
