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

## Entregable 2 — Clúster, API y persistencia

La solución PC4 agrega un coordinador HTTP/TCP y tres nodos ML:

- API REST en `:8080`, documentación Swagger en `/swagger`.
- Coordinador TCP en `:9090`.
- Tres servicios `ml-node-*` que reciben tareas por canales Go y JSON sobre TCP.
- Cada nodo ML entrena 6 árboles CART con una muestra bootstrap de su partición.
- El coordinador agrega las 18 predicciones parciales como voto suave del bosque distribuido.
- MongoDB como persistencia y Redis como caché.
- Docker Compose para levantar MongoDB, Redis, API y los tres nodos.

Ejecutar el entorno distribuido:

```powershell
docker compose up --build
```

Sin Docker Desktop, la misma API puede ejecutarse localmente con memoria temporal:

```powershell
go run ./pc4/cmd/coordinator -http=:8080 -tcp=:9090
go run ./pc4/cmd/node -id=ml-node-1 -coordinator=127.0.0.1:9090
go run ./pc4/cmd/node -id=ml-node-2 -coordinator=127.0.0.1:9090
go run ./pc4/cmd/node -id=ml-node-3 -coordinator=127.0.0.1:9090
```

## Kanban

El [tablero Kanban PC03](https://github.com/users/magic-tps/projects/2) está
vinculado a este repositorio y organiza las tareas en **Por hacer**,
**En progreso** y **Terminado**. Cada tarjeta corresponde a una issue con su
descripción, evidencia o pasos pendientes.

Al comenzar una tarea, mover su tarjeta a **En progreso**. Al verificar el
resultado, moverla a **Terminado** y cerrar la issue. Los responsables se asignan
de acuerdo con el equipo. La captura del tablero para el informe sigue pendiente.

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
