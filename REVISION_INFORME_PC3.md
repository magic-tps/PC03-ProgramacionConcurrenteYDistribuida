# Revisión integral del informe PC3

Se actualizó el documento Word existente, conservando su estructura principal, tablas y 15 capturas nuevas. Se revisaron el enunciado, la rúbrica, el modelo de informe, main.go, el CSV y los registros de ejecución. No se modificó el programa.

Cambios principales:

- Resumen y abstract ajustados al prototipo local y a sus limitaciones.
- Tablas 26–30 y figura 5 unificadas con normal, replica y race del 08/11/2026.
- Separación entre filas originales, cantidades representadas y posiciones expandidas; la expansión no crea microdatos ni observaciones independientes.
- Explicación explícita de la franja generada desde la etiqueta y de por qué condiciona la evaluación.
- Retiradas afirmaciones no medidas sobre reducción del delito, ahorro, energía y causas de cuellos de botella.
- Explicados los límites de las comprobaciones: firma parcial del bosque, alcance de -race, memoria del pipeline y restricciones del lector CSV.
- GitHub, ramas, revisiones, Kanban y participación presentados como pendientes, sin historia ni porcentajes inventados.
- Diseño de PC4 simplificado: se retiraron ejemplos de servicios y despliegue que no existen en el código local.
- Conclusiones, recomendaciones, bibliografía y anexos corregidos. Índice convertido en campo automático de Word.

| Requisito | Estado | Evidencia o pendiente |
|---|---|---|
| Carátula e identificación | Parcial | Se conservan nombres. Completar sección y códigos; confirmar datos del docente y nombre de entrega. |
| Problema, motivación y pregunta social | Documentado | Secciones d y e; utilidad esperada separada de efectos medidos. |
| Limpieza y análisis de datos | Evidenciado con límites | CSV, tablas, prueba sintética y lectura independiente. No se demuestra ausencia de todo error de datos. |
| Volumen masivo | Procesamiento expandido demostrado | 376,715 filas originales; 1,987,556 posiciones en memoria. Sustentar ante el docente esta diferencia respecto del umbral del caso. |
| Paralelización en Go | Implementada y explicada | Worker Pool, canales, WaitGroup, select, semáforo y Map-Reduce en main.go. |
| Modelo ML y diseño distribuido | Prototipo local y propuesta documentados | 32 árboles locales; clúster no implementado. Validez predictiva limitada por la franja generada desde la etiqueta. |
| Pruebas y evidencias | Disponibles | Dos corridas nativas, -race, sintético, go fmt y go vet; registros y 15 capturas. |
| Repositorio público y Git Flow | Pendiente de evidencia | Añadir enlace real, ramas, commits y revisiones existentes; capturas 1, 12 y 13. |
| Tablero Kanban en GitHub | Pendiente de evidencia | Crear o identificar tablero real y actualizar estados; capturas 2 y 14. |
| Participación del equipo | Pendiente | Completar Anexo D con datos y porcentajes validados por los integrantes. |
| Bibliografía y trazabilidad | Revisadas; un dato pendiente | Referencias a fuentes utilizadas y registros identificados. Falta fecha original de descarga del CSV. |
| Video | Pendiente de evidencia y alcance | El requisito figura en instrucciones generales y rúbrica; confirmar presentación para PC3 y añadir enlace. |

La revisión no implementa componentes de PC4 ni completa actividades externas a la documentación. La utilidad operativa del modelo no está validada.

Fuentes revisadas:

- [Documentación oficial del detector de carreras de Go](https://go.dev/doc/articles/race_detector): alcance limitado a las operaciones ejecutadas.
- [Especificación de Go](https://go.dev/ref/spec): semántica de canales y sincronización.
- [Random Forests, Breiman](https://www.stat.berkeley.edu/~breiman/randomforest2001.pdf): referencia del algoritmo.
- [Ensign et al. (2018)](https://proceedings.mlr.press/v81/ensign18a.html): retroalimentación en vigilancia predictiva.
- [Braga et al. (2019)](https://onlinelibrary.wiley.com/doi/10.1002/cl2.1046): resultados sobre lugares pequeños; no validan este prototipo distrital.
- [Ficha del dataset](https://www.datosabiertos.gob.pe/dataset/denuncias-policiales): referencia pública de procedencia; falta fecha original de descarga del equipo.

Control independiente reproducible desde la carpeta PC3:

```powershell
python capturas-ejecucion/revision-integral/verificar_csv.py
```

Respaldo previo a esta revisión: C:\Users\magic\AppData\Local\Temp\pc3-capturas-tools\1ACC0065_PC3_202620_Informe_Grupo3_antes_revision_integral.docx
SHA256 de main.go: d56c8457cebe599efce6a1ef3b39f3c64da145e6f761352d98c159ec5782810b
SHA256 del CSV: e67a1edcd84483df5c471f0d6b9e13c2eeaa34c680af30eb1ec86c443d02ec76

La fecha de estas evidencias es 08/11/2026; no se presentan como evidencia de trabajo anterior al plazo del enunciado.

Verificación final: informe de 46 páginas, 34 tablas, 21 imágenes (incluidas las 15 capturas nuevas), índice actualizado en Word y sin páginas en blanco. Los hashes del código y del CSV coinciden con los registrados en las ejecuciones. Las 15 capturas se conservan en su resolución original dentro del documento.
