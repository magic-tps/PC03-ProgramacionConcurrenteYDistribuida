# Fuente del dataset

`DATASET_Denuncias Policiales - Enero 2018 a Septiembre 2026.csv` es una copia exacta del CSV de la raíz del repositorio
(mismo archivo usado en la PC3). Origen: Sistema de Denuncias Policiales (SIDPOL) – Ministerio del Interior,
publicado en la Plataforma Nacional de Datos Abiertos del Perú (https://www.datosabiertos.gob.pe).

- 376,715 filas de datos, 8 columnas (ANIO, MES, DPTO_HECHO_NEW, PROV_HECHO, DIST_HECHO, UBIGEO_HECHO, P_MODALIDADES, cantidad)
- 7,509,690 denuncias a nivel nacional; 1,987,556 clasificables en Lima Metropolitana y Callao
- Docker Compose lo monta como volumen de solo lectura en /data/dataset.csv
