import json
import os
import shutil
from PIL import Image, ImageDraw, ImageFont
from docx import Document
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.shared import Inches, Pt

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PC4 = os.path.join(ROOT, "pc4")
SOURCE = os.path.join(ROOT, "1ACC0065_PC3_202620_Informe_Grupo3.docx")
OUTPUT = os.path.join(ROOT, "1ACC0065_PC4_202620_Informe_Grupo3.docx")

evidence_path = os.path.join(PC4, "evidencias", "prueba-random-forest-docker.json")
with open(evidence_path, "r", encoding="utf-8-sig") as f:
    evidence = json.load(f)

def font(size, bold=False):
    candidates = [
        r"C:\Windows\Fonts\segoeui.ttf",
        r"C:\Windows\Fonts\arial.ttf",
    ]
    if bold:
        candidates = [r"C:\Windows\Fonts\seguisb.ttf", r"C:\Windows\Fonts\arialbd.ttf"] + candidates
    for path in candidates:
        if os.path.exists(path):
            return ImageFont.truetype(path, size)
    return ImageFont.load_default()

arch_path = os.path.join(PC4, "evidencias", "arquitectura-pc4.png")
img = Image.new("RGB", (1200, 680), "white")
draw = ImageDraw.Draw(img)
title = font(34, True)
box = font(21, True)
small = font(17)
draw.text((40, 25), "Arquitectura PC4 - Clúster de nodos ML", fill="#17365D", font=title)
boxes = [
    (40, 130, 300, 260, "Cliente / Postman", "HTTP REST\nPuerto 8080"),
    (370, 130, 660, 260, "Coordinador Go", "API + TCP\nPuerto 9090"),
    (730, 100, 1040, 180, "ML Node 1", "Goroutines + TCP"),
    (730, 220, 1040, 300, "ML Node 2", "Goroutines + TCP"),
    (730, 340, 1040, 420, "ML Node 3", "Goroutines + TCP"),
    (370, 470, 660, 590, "MongoDB + Redis", "Persistencia + caché"),
]
for x1, y1, x2, y2, heading, text in boxes:
    draw.rounded_rectangle((x1, y1, x2, y2), radius=18, fill="#EAF2F8", outline="#2F75B5", width=3)
    draw.text((x1 + 18, y1 + 18), heading, fill="#17365D", font=box)
    draw.text((x1 + 18, y1 + 70), text, fill="#333333", font=small)
draw.line([(300,195),(370,195)], fill="#2F75B5", width=5)
draw.polygon([(370,195),(350,185),(350,205)], fill="#2F75B5")
draw.line([(660,195),(730,195)], fill="#2F75B5", width=5)
draw.polygon([(730,195),(710,185),(710,205)], fill="#2F75B5")
draw.line([(660,235),(730,260)], fill="#2F75B5", width=4)
draw.line([(660,235),(730,380)], fill="#2F75B5", width=4)
draw.line([(660,260),(370,530)], fill="#70AD47", width=4)
draw.line([(515,260),(515,470)], fill="#70AD47", width=4)
img.save(arch_path)

proof_path = os.path.join(PC4, "evidencias", "prueba-api.png")
proof = Image.new("RGB", (1200, 620), "white")
pd = ImageDraw.Draw(proof)
pd.text((40, 25), "Evidencia de pruebas API PC4", fill="#17365D", font=title)
rows = [
    ("Entrenamiento distribuido", f"{evidence['Train']['nodes']} nodos; {evidence['Train']['rows']} posiciones; {evidence['Train']['durationMs']} ms"),
    ("Predicción fría", f"{evidence['ColdMs']} ms"),
    ("Predicción cálida", f"{evidence['WarmMs']} ms"),
    ("20 ejecuciones", f"Promedio {evidence['Average20Ms']} ms; mínimo {evidence['Min20Ms']} ms; máximo {evidence['Max20Ms']} ms"),
    ("Clase predicha", f"{evidence['WarmResponse']['topClass']} ({evidence['WarmResponse']['topProbability']:.4f})"),
    ("Almacenamiento", "MongoDB: connected; Redis: connected; modo persisted"),
]
y = 110
for label, value in rows:
    pd.rounded_rectangle((40, y, 1160, y + 68), radius=12, fill="#F3F6F9", outline="#D9E2F3")
    pd.text((60, y + 14), label, fill="#17365D", font=box)
    pd.text((430, y + 16), value, fill="#333333", font=small)
    y += 78
pd.text((40, 590), "Evidencia Docker real generada por pc4/test_api.ps1 el 21/11/2026.", fill="#666666", font=small)
proof.save(proof_path)

shutil.copy2(SOURCE, OUTPUT)
doc = Document(OUTPUT)

def set_font(run, size=None, bold=None):
    run.font.name = "Aptos"
    if size:
        run.font.size = Pt(size)
    if bold is not None:
        run.bold = bold

def add_heading(text, level=1):
    p = doc.add_heading(text, level=level)
    p.alignment = WD_ALIGN_PARAGRAPH.LEFT
    return p

def add_table(headers, rows):
    table = doc.add_table(rows=1, cols=len(headers))
    table.alignment = WD_TABLE_ALIGNMENT.CENTER
    table.style = "Table Grid"
    for i, header in enumerate(headers):
        cell = table.rows[0].cells[i]
        cell.text = header
        for run in cell.paragraphs[0].runs:
            set_font(run, 9, True)
    for row in rows:
        cells = table.add_row().cells
        for i, value in enumerate(row):
            cells[i].text = str(value)
            for run in cells[i].paragraphs[0].runs:
                set_font(run, 9)
    doc.add_paragraph()

doc.add_page_break()
title_p = doc.add_paragraph()
title_p.alignment = WD_ALIGN_PARAGRAPH.CENTER
r = title_p.add_run("ENTREGABLE 2 - PC4")
set_font(r, 18, True)
sub = doc.add_paragraph()
sub.alignment = WD_ALIGN_PARAGRAPH.CENTER
r = sub.add_run("Clúster distribuido, API REST, Swagger y bases de datos")
set_font(r, 14, True)

add_heading("Corrección y cierre del Entregable 1", 1)
doc.add_paragraph(
    "Se conserva el alcance validado de PC3 y se separan con claridad los resultados locales de las "
    "capacidades futuras. Se incorporan como correcciones: franja horaria identificada como simulada, "
    "precaución sobre el ranking por volumen de denuncias, ausencia de inferencias operativas de patrullaje "
    "y necesidad de evidencias reales de GitHub, Kanban y participación."
)

add_heading("Resumen PC4", 1)
doc.add_paragraph(
    "El sistema implementado incorpora un coordinador Go, tres nodos ML comunicados por TCP y una API REST "
    "HTTP. El coordinador recibe tareas, las reparte entre los nodos mediante JSON sobre sockets persistentes "
    "y consolida resultados. MongoDB se usa como almacenamiento persistente y Redis como caché. Cuando los "
    "servicios externos no están disponibles, la aplicación mantiene un respaldo en memoria para seguir "
    "siendo demostrable."
)

add_heading("1. Clúster distribuido", 1)
doc.add_picture(arch_path, width=Inches(6.6))
doc.paragraphs[-1].alignment = WD_ALIGN_PARAGRAPH.CENTER
doc.add_paragraph(
    "Topología: estrella. El cliente consulta al coordinador; el coordinador registra los nodos por TCP "
    "en el puerto 9090, distribuye el entrenamiento y agrega las predicciones. La estrella reduce el acoplamiento "
    "del cliente con los nodos y centraliza registro, resultados y persistencia."
)
doc.add_paragraph(
    "La partición del dataset es determinista por hash del distrito. Cada nodo procesa una tercera parte "
    "disjunta y construye 6 árboles CART con muestreo bootstrap. Las tareas se codifican como mensajes JSON "
    "con tipo, identificador de trabajo y datos solicitados."
)
doc.add_paragraph(
    "El bosque distribuido contiene 18 árboles: 6 en cada uno de los tres nodos. El coordinador solicita "
    "una predicción a los nodos y promedia sus probabilidades por clase."
)

add_heading("2. API y Swagger", 1)
add_table(
    ["Endpoint", "Método", "Función"],
    [
        ("GET /api/v1/health", "GET", "Estado del coordinador, nodos y almacenamiento."),
        ("POST /api/v1/train", "POST", "Distribuye el entrenamiento y devuelve filas procesadas."),
        ("POST /api/v1/predict", "POST", "Agrega probabilidades de los tres nodos."),
        ("GET /api/v1/metrics", "GET", "Métricas de nodos y persistencia."),
        ("GET /swagger", "GET", "Interfaz Swagger UI."),
    ],
)
doc.add_paragraph(
    "La API usa net/http del paquete estándar de Go. El entrenador y las predicciones se ejecutan mediante "
    "goroutines y channels para esperar las respuestas TCP sin bloquear el proceso principal. Se valida el JSON "
    "de entrada y se devuelve Cache-Control: no-store."
)
doc.add_paragraph(
    "Especificación OpenAPI: pc4/openapi/swagger.yaml. Colección Postman: pc4/postman_collection.json."
)

add_heading("3. Bases de datos", 1)
add_table(
    ["Entidad MongoDB", "Campos principales", "Uso"],
    [
        ("training_jobs", "jobId, rows, durationMs, nodes, createdAt", "Historial de trabajos distribuidos."),
        ("predictions", "district, franja, month, year, probabilities, latencyMs, createdAt", "Persistencia de predicciones."),
        ("Redis pc4:predict:*", "probabilities con TTL 30 s", "Caché colaborativa de consultas repetidas."),
    ],
)
doc.add_paragraph(
    "MongoDB se configura mediante MONGO_URI y Redis mediante REDIS_ADDR. Docker Compose incorpora mongo:7 "
    "y redis:7-alpine. Si no hay servicios externos, el almacen interno conserva temporalmente los registros "
    "de demostración."
)

add_heading("4. Evidencias de implementación y pruebas", 1)
doc.add_picture(proof_path, width=Inches(6.6))
doc.paragraphs[-1].alignment = WD_ALIGN_PARAGRAPH.CENTER
add_table(
    ["Prueba", "Resultado"],
    [
        ("Compilación Go", "go test ./... y go build completados sin errores."),
        ("Nodos registrados", "ml-node-1, ml-node-2 y ml-node-3 activos."),
        ("Entrenamiento", f"{evidence['Train']['rows']} posiciones en {evidence['Train']['durationMs']} ms."),
        ("Predicción fría", f"{evidence['ColdMs']} ms."),
        ("Predicción cálida", f"{evidence['WarmMs']} ms."),
        ("20 predicciones", f"Promedio {evidence['Average20Ms']} ms; máximo {evidence['Max20Ms']} ms."),
        ("Docker", "Docker Compose ejecutado con 6 contenedores activos: coordinador, 3 nodos, MongoDB y Redis."),
        ("MongoDB", "1 documento de predicción y 1 trabajo de entrenamiento persistidos."),
        ("Redis", "1 clave de caché registrada en el despliegue."),
    ],
)
doc.add_paragraph(
    "Los resultados de latencia se obtuvieron mediante pc4/test_api.ps1 con el coordinador y los tres nodos "
    "ejecutándose localmente. La medición confirma el objetivo de menos de 100 ms en las pruebas calientes "
    "repetidas. Debe complementarse con capturas reales de Postman importando pc4/postman_collection.json."
)

add_heading("5. Correcciones para el informe final", 1)
doc.add_paragraph(
    "Pendientes administrativos reales: enlace público final del repositorio, capturas auténticas del tablero "
    "Kanban, reporte de participación con actividad verificable y capturas visuales de Postman. La ejecución "
    "Docker Compose ya fue confirmada con Docker Desktop activo."
)

add_heading("Conclusiones", 1)
doc.add_paragraph(
    "PC4 demuestra una arquitectura distribuida funcional con API, registro TCP de tres nodos, partición "
    "determinista y persistencia diseñada según MongoDB/Redis. Las mediciones locales permanecieron por debajo "
    "de 100 ms para el promedio de predicciones repetidas."
)
doc.add_paragraph(
    "El prototipo conserva las limitaciones metodológicas de PC3: la franja horaria no proviene del dataset "
    "original y las predicciones no deben interpretarse como un sistema operativo de patrullaje."
)

add_heading("Recomendaciones", 1)
doc.add_paragraph(
    "Adjuntar capturas visuales de Postman y MongoDB/Redis, integrar "
    "JWT y desarrollar la SPA para TB2. Con microdatos horarios reales puede ampliarse el bosque distribuido "
    "y reemplazar la franja derivada por la hora observada."
)

add_heading("Anexos", 1)
doc.add_paragraph("Repositorio local: https://github.com/magic-tps/PC03-ProgramacionConcurrenteYDistribuida")
doc.add_paragraph("Código: main.go, pc4/cmd/coordinator, pc4/cmd/node, pc4/internal.")
doc.add_paragraph("Despliegue: Dockerfile y docker-compose.yml.")
doc.add_paragraph("API: pc4/openapi/swagger.yaml y pc4/postman_collection.json.")
doc.add_paragraph("Evidencias: pc4/evidencias/prueba-api.json y pc4/evidencias/prueba-api.png.")

doc.save(OUTPUT)
print(OUTPUT)
