# PC4 · Entregable 2 — Clúster ML, API REST y bases de datos (Grupo 3)

Versión corregida y ampliada de la PC4, en una carpeta propia (`pc4-mejora/`) para no modificar el trabajo original de `pc4/`.

| Integrante | Código |
|---|---|
| Karim Samanamud | U201816862 |
| Tomás Pastor | U201916314 |
| Juan Diego Sánchez | U202221147 |

## Qué hace

1. **Coordinador Go** (`cmd/coordinator`): API REST en `:8080` con Swagger, servidor TCP del clúster en `:9090`, MongoDB y Redis.
2. **3 nodos ML** (`cmd/node`): cada uno carga el CSV con un pipeline concurrente (lector + Worker Pool), toma su **shard estratificado** (denuncia *k* → nodo *k mod 3*) y entrena su bloque del **Random Forest distribuido** (32 árboles = 11 + 11 + 10) con goroutines.
3. **Predicción**: el coordinador consulta en paralelo a los nodos y promedia las probabilidades ponderando por número de árboles; el resultado es idéntico al de entrenar los 32 árboles en un solo proceso (probado).
4. **Impacto social**: `GET /api/v1/patrullaje` devuelve, por distrito y franja, el delito de calle más probable (matriz de patrullaje de la PC3, ahora calculada por el clúster).

Mismas reglas de datos que la PC3: 376,715 filas, 1,987,556 denuncias de Lima Metropolitana y Callao, entrenamiento 2018–2025 (1,823,335) y prueba 2026 (164,221).

## Correcciones respecto de `pc4/`

| Problema en `pc4/` | Corrección en `pc4-mejora/` |
|---|---|
| Sharding por distrito: 2 de 3 nodos predecían con **otro** distrito (ID local 0) | Sharding estratificado + catálogo global por UBIGEO: los 3 nodos conocen los 50 distritos |
| "EXTORSIÓN" con tilde no se reconocía: 50,677 denuncias y una clase perdidas | Normalización sin tildes como en la PC3 (1,987,556 denuncias) |
| `mes` y `anio` se ignoraban (fijos en 11/2026) | Se usan en la predicción y se validan |
| Distrito inexistente → 200 con probabilidades | 404 `UNKNOWN_DISTRICT` |
| Sin modelo → fórmula hash con 200 OK | 409 `MODEL_NOT_TRAINED`; si un nodo reinicia, el coordinador lo reentrena solo |
| Caché en memoria sin expiración, sin mes/año, no se invalidaba al reentrenar | Clave con versión del modelo + TTL (Redis) |
| Carrera de datos en `LastSeen` (confirmada con `-race`) | Campos protegidos con mutex; suite completa sin carreras |
| Respuestas TCP sin correlación; un timeout desconectaba al nodo | Correlación por ID; respuesta tardía descartada; el nodo sigue conectado |
| Nodos entrenaban 6 árboles en serie | Worker Pool de goroutines por nodo (11/11/10 árboles) |
| Zonas mal asignadas (Ventanilla, Lurigancho-Chosica, Pucusana… → "Lima Centro") | Tabla de zonas de la PC3 |
| Swagger sin esquemas ni códigos de error | OpenAPI 3.0.3 completo con ejemplos reales, embebido en el binario |
| Cabecera `X-Response-Time-Ms` nunca se enviaba | Se fija antes de escribir la respuesta |
| `depends_on` sin healthcheck | MongoDB/Redis/coordinador con healthchecks |
| Sin pruebas automáticas | `go test -race` (dataset, bosque, clúster, nodo, API de extremo a extremo) |

## Estructura

```
cmd/coordinator, cmd/node   ejecutables
internal/dataset            carga concurrente, limpieza, catálogo, shards
internal/forest             CART + Random Forest con Worker Pool
internal/protocol           mensajes TCP (JSON)
internal/worker             nodo ML
internal/cluster            lado coordinador del clúster (TCP, fan-out/fan-in)
internal/api                API REST, evaluación, patrullaje, métricas
internal/storage[/dbstore]  MongoDB + Redis (o memoria en pruebas)
openapi/swagger.yaml        especificación OpenAPI
postman/                    colección con pruebas automáticas (< 100 ms)
scripts/                    scripts de evidencias (PowerShell)
data/                       copia del CSV oficial (fuente: Plataforma Nacional de Datos Abiertos, SIDPOL–MININTER)
evidencias/                 resultados de los scripts y capturas de pantalla (evidencias/capturas)
docs/generar_diagramas.py   genera evidencias/arquitectura-pc4.png
informe/entrega/            informe PC4 en Word, una copia por integrante (no se sube a Git)
```

## Cómo ejecutarlo (Windows, Docker Desktop)

Requisitos: Docker Desktop abierto. Si el stack de `pc4/` está corriendo, apágalo primero (`docker compose down` en la carpeta raíz), porque usa los mismos puertos.

```powershell
cd C:\Users\jde\Documents\Antigravity\PC03-ProgramacionConcurrenteYDistribuida\pc4-mejora
Set-ExecutionPolicy -Scope Process Bypass      # permite ejecutar los scripts en esta ventana
.\scripts\01_levantar.ps1 -Limpio              # construye y levanta los 6 contenedores
.\scripts\02_pruebas_api.ps1                   # entrenamiento, 20 predicciones sin y con caché, errores, patrullaje
.\scripts\03_resiliencia.ps1                   # detiene un nodo, sigue respondiendo, se recupera solo
.\scripts\04_evidencias_bd.ps1                 # docker ps/stats, documentos de MongoDB, claves de Redis, logs
.\scripts\05_go_test.ps1                       # go vet + go test -race dentro de un contenedor golang:1.24
```

Swagger UI: http://localhost:8080/swagger · Postman: importar `postman/PC4_Grupo3.postman_collection.json`.
Para apagar: `.\scripts\99_apagar.ps1` (o `-Borrar` para eliminar también los datos de MongoDB).

Sin Docker (solo memoria, sin MongoDB/Redis):

```powershell
go run ./cmd/coordinator
go run ./cmd/node -id ml-node-1
go run ./cmd/node -id ml-node-2
go run ./cmd/node -id ml-node-3
```

## Endpoints

| Método | Ruta | Descripción |
|---|---|---|
| GET | `/api/v1/health` | Estado de nodos, modelo, MongoDB y Redis |
| POST | `/api/v1/train` | Entrena el bosque distribuido y lo evalúa con 2026 |
| POST | `/api/v1/predict` | `{"district","franja","mes","anio"}` → probabilidades de 6 delitos |
| GET | `/api/v1/patrullaje?mes=&anio=&top=` | Delito de calle más probable por distrito y franja |
| GET | `/api/v1/districts` | 50 distritos con UBIGEO, zona y volumen |
| GET | `/api/v1/metrics` | Nodos, latencias (p50/p95/p99), caché, persistencia |

La franja horaria es **imputada** (el CSV no trae la hora del hecho), igual que en la PC3; distrito, mes y año son datos reales.
