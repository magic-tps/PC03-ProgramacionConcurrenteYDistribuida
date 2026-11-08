# PC03 — Programación Concurrente y Distribuida

Prototipo en Go para analizar denuncias policiales con un pipeline concurrente,
un pool de workers y un clasificador Random Forest local. Compara la ejecución
con un worker y con varios workers.

## Ejecutar

Instalar Go y abrir una terminal en esta carpeta. El programa utiliza únicamente
la biblioteca estándar y busca el CSV incluido en la raíz del proyecto.

```powershell
go run main.go
```

Para consultar las opciones o ejecutar con cuatro workers:

```powershell
go run main.go -h
go run main.go -workers=4 -v=false -reps=1
```

## Archivos

- `main.go`: implementación del programa.
- `DATASET_Denuncias Policiales - Enero 2018 a Septiembre 2026.csv`: datos de entrada.
- `1ACC0065_PC3_202620_Informe_Grupo3.docx`: informe del proyecto.
- `A_enunciado_CC65-TB2-1ACC0065_202620.pdf`: requisitos del trabajo.
- `capturas-ejecucion/`: capturas, registros y verificaciones. Consultar el
  [detalle de las nuevas evidencias](capturas-ejecucion/nuevas-2026-11-08/LEEME_CAPTURAS.md).
- `output.txt`: registro histórico de ejecución.

Las franjas horarias son simuladas porque el CSV no contiene la hora del hecho.
Los resultados describen un prototipo local; no validan su uso operativo.

## Actualizar el repositorio

Después de guardar los cambios, ejecutar desde esta carpeta:

```powershell
git status
git add .
git commit -m "Actualizar proyecto PC03"
git push origin main
```

El archivo `.gitignore` excluye temporales de Office, ejecutables, cachés y el
CSV sintético generado. Los registros y capturas existentes se conservan como evidencia.
