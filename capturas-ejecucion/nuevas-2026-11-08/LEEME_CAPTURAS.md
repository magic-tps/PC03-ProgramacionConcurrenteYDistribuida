# Nuevas capturas PC3 — 8 de noviembre de 2026

15 capturas de los registros reales de nuevas ejecuciones. Se insertaron en los espacios 3–11 del informe Word existente. Los espacios 1, 2, 12, 13 y 14 se marcaron pendientes de evidencia real de GitHub. El informe recibió después una revisión integral.

Cada PNG muestra una vista del registro guardado: no es una foto de la terminal. El contenido del programa se conserva sin alterar sus valores. Las tablas largas se dividieron para facilitar la lectura. Las franjas horarias del prototipo son simuladas.

Entorno: Windows/amd64, Go 1.26.10, 12 CPU lógicas, GOMAXPROCS=4, 4 workers, lote de 10,000 filas, 32 árboles. Las corridas normales usaron el CSV original y una repetición de benchmark.

| Captura | Evidencia | Registro / líneas |
|---|---|---|
| [03-lectura-limpieza.png](03-lectura-limpieza.png) | Lectura, limpieza y desagregación | normal.stdout.txt / 19–46 |
| [04a-sintetico-generacion.png](04a-sintetico-generacion.png) | Generación del dataset sintético de prueba | sintetico.stdout.txt / 8–15 |
| [04b-sintetico-limpieza.png](04b-sintetico-limpieza.png) | Prueba de limpieza con ruido sintético | sintetico.stdout.txt / 21–49 |
| [05a-distribucion-clases.png](05a-distribucion-clases.png) | Distribución nacional y clases del clasificador | normal.stdout.txt / 50–76 |
| [05b-distritos-zonas.png](05b-distritos-zonas.png) | Distritos y zonas urbanas | normal.stdout.txt / 77–104 |
| [05c-franjas-simuladas.png](05c-franjas-simuladas.png) | Franjas horarias imputadas (simuladas) | normal.stdout.txt / 105–127 |
| [06a-random-forest.png](06a-random-forest.png) | Random Forest: entrenamiento y evaluación | normal.stdout.txt / 128–147 |
| [06b-importancia-variables.png](06b-importancia-variables.png) | Importancia de variables | normal.stdout.txt / 148–158 |
| [06c-predicciones.png](06c-predicciones.png) | Predicciones del prototipo local | normal.stdout.txt / 159–176 |
| [07-entorno-fuente.png](07-entorno-fuente.png) | Entorno y fuente de datos | normal.stdout.txt / 3–18 |
| [08a-workers-monitor.png](08a-workers-monitor.png) | Worker Pool y monitor: inicio de la ejecución | replica.stdout.txt / 14–33 |
| [08b-workers-reparto.png](08b-workers-reparto.png) | Worker Pool: cierre y reparto de los 38 lotes | replica.stdout.txt / 51–64 |
| [09-benchmark-corrida1.png](09-benchmark-corrida1.png) | Benchmark: corrida 1 | normal.stdout.txt / 177–201 |
| [10-benchmark-corrida2.png](10-benchmark-corrida2.png) | Benchmark: corrida 2 | replica.stdout.txt / 223–247 |
| [11-detector-carreras.png](11-detector-carreras.png) | Ejecución con detector de carreras | race.stdout.txt / 177–201 |

| Verificación | Estado | Evidencia |
|---|---|---|
| Lectura del CSV original | Cumplido | 376,715 filas; 7,509,690 denuncias nacionales |
| Desagregación y clasificación | Cumplido | 1,987,556 registros; 32 árboles; accuracy 51.11% |
| Comparación secuencial/concurrente | Cumplido según comprobaciones existentes | Columna = con cuatro marcas ✔ en normal, réplica y race |
| Limpieza de datos sintéticos | Cumplido | 8,523 filas descartadas (3.03%); 2,861 reparadas (1.02%) |
| Detector de carreras | Cumplido en esta ejecución | race: salida 0 y stderr vacío; sin WARNING: DATA RACE |
| Formato y análisis estático | Cumplido | go fmt main.go y go vet main.go: salida 0 |
| GitHub, Git Flow, PR y Kanban | Evidencia parcial | El repositorio ya está inicializado y tiene un commit inicial. Queda documentar las ramas, revisiones y el tablero Kanban con evidencia real. |

La revisión integral unificó las tablas y conclusiones del informe con estas nuevas corridas. Las mediciones históricas se conservan por separado en output.txt y en las capturas JPEG originales: los tiempos no deben mezclarse. Los tiempos de la ejecución instrumentada con -race no representan rendimiento nativo.

Comandos ejecutados (Go portátil disponible temporalmente en el equipo):

```powershell
$env:GOMAXPROCS = "4"
go fmt main.go
go vet main.go
go run main.go -workers=4 -v=false -reps=1
go run main.go -workers=4 -reps=1
go run -race main.go -workers=4 -v=false -reps=1
# Sintético: ejecutar desde una carpeta temporal vacía, indicando la ruta absoluta a main.go.
```

Los archivos *.stdout.txt y *.stderr.txt contienen las salidas completas; los *.json registran comandos, fechas de Lima, códigos de salida y hashes. El CSV sintético se generó en una carpeta temporal separada del CSV original.

SHA256 de main.go (sin cambios): d56c8457cebe599efce6a1ef3b39f3c64da145e6f761352d98c159ec5782810b
SHA256 del CSV original: e67a1edcd84483df5c471f0d6b9e13c2eeaa34c680af30eb1ec86c443d02ec76

Copia de respaldo del informe previo a insertar capturas: C:\Users\magic\AppData\Local\Temp\pc3-capturas-tools\1ACC0065_PC3_202620_Informe_Grupo3_antes_capturas.docx
