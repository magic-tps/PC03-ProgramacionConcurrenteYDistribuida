"""Genera el diagrama de arquitectura de la PC4 (evidencias/arquitectura-pc4.png).

Uso:  python docs/generar_diagramas.py      (requiere Pillow)
Las cajas se dibujan con coordenadas fijas y el texto se mide para que nunca
se salga de su caja; las flechas se conectan a los bordes, sin cruzar cajas.
"""
import os
from PIL import Image, ImageDraw, ImageFont

S = 1.5  # escala de salida (1600x900 -> 2400x1350)
W, H = 1600, 920
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "evidencias", "arquitectura-pc4.png")


def font(size, bold=False):
    names = (["DejaVuSans-Bold.ttf", "arialbd.ttf", "segoeuib.ttf"] if bold else ["DejaVuSans.ttf", "arial.ttf", "segoeui.ttf"])
    dirs = ["/usr/share/fonts/truetype/dejavu", "C:/Windows/Fonts", "/Library/Fonts"]
    for d in dirs:
        for n in names:
            p = os.path.join(d, n)
            if os.path.exists(p):
                return ImageFont.truetype(p, int(size * S))
    return ImageFont.load_default()


img = Image.new("RGB", (int(W * S), int(H * S)), "white")
d = ImageDraw.Draw(img)


def sc(*v):
    return [int(x * S) for x in v]


def box(x1, y1, x2, y2, title, lines, fill, border, title_size=19, line_size=14, center=False):
    d.rounded_rectangle(sc(x1, y1, x2, y2), radius=int(14 * S), fill=fill, outline=border, width=int(3 * S))
    ft, fl = font(title_size, True), font(line_size)
    y = y1 + 14
    tw = d.textlength(title, font=ft) / S
    assert tw <= (x2 - x1) - 24, f"título no cabe: {title}"
    tx = x1 + ((x2 - x1) - tw) / 2 if center else x1 + 16
    d.text(sc(tx, y), title, fill="#1F2D3D", font=ft)
    y += title_size + 12
    for ln in lines:
        lw = d.textlength(ln, font=fl) / S
        assert lw <= (x2 - x1) - 24, f"línea no cabe ({lw:.0f} > {x2 - x1 - 24}): {ln}"
        lx = x1 + ((x2 - x1) - lw) / 2 if center else x1 + 16
        d.text(sc(lx, y), ln, fill="#333333", font=fl)
        y += line_size + 7
    assert y <= y2 - 6, f"texto desborda la caja: {title}"


def arrow_head(x, y, dx, dy, color, size=11):
    import math
    ang = math.atan2(dy, dx)
    p1 = (x - size * math.cos(ang - 0.45), y - size * math.sin(ang - 0.45))
    p2 = (x - size * math.cos(ang + 0.45), y - size * math.sin(ang + 0.45))
    d.polygon([tuple(sc(x, y)), tuple(sc(*p1)), tuple(sc(*p2))], fill=color)


def arrow(x1, y1, x2, y2, color, both=False, width=3, dash=False):
    if dash:
        import math
        L = math.hypot(x2 - x1, y2 - y1)
        n = int(L // 12)
        for i in range(0, n, 2):
            a, b = i / n, min((i + 1) / n, 1)
            d.line(sc(x1 + (x2 - x1) * a, y1 + (y2 - y1) * a, x1 + (x2 - x1) * b, y1 + (y2 - y1) * b), fill=color, width=int(width * S))
    else:
        d.line(sc(x1, y1, x2, y2), fill=color, width=int(width * S))
    arrow_head(x2, y2, x2 - x1, y2 - y1, color)
    if both:
        arrow_head(x1, y1, x1 - x2, y1 - y2, color)


def label(x, y, text, size=13, color="#444444", bold=False, bg=True):
    f = font(size, bold)
    w = d.textlength(text, font=f) / S
    if bg:
        d.rectangle(sc(x - 4, y - 2, x + w + 4, y + size + 4), fill="white")
    d.text(sc(x, y), text, fill=color, font=f)
    return w


# Título
d.text(sc(40, 24), "Arquitectura PC4 — Random Forest distribuido en un clúster de nodos ML", fill="#17365D", font=font(28, True))
d.text(sc(40, 64), "Caso 2 · Denuncias SIDPOL (Lima Metropolitana y Callao) · Grupo 3", fill="#555555", font=font(16))

# Frontera de Docker Compose (líneas punteadas)
bx1, by1, bx2, by2 = 395, 110, 1565, 895
dash_c = "#7A8899"
for x in range(bx1, bx2, 18):
    d.line(sc(x, by1, min(x + 10, bx2), by1), fill=dash_c, width=int(2 * S))
    d.line(sc(x, by2, min(x + 10, bx2), by2), fill=dash_c, width=int(2 * S))
for y in range(by1, by2, 18):
    d.line(sc(bx1, y, bx1, min(y + 10, by2)), fill=dash_c, width=int(2 * S))
    d.line(sc(bx2, y, bx2, min(y + 10, by2)), fill=dash_c, width=int(2 * S))
label(bx1 + 14, by1 - 10, "Docker Compose · proyecto pc4-grupo3 · 6 contenedores · red interna", size=14, color="#3A4A5C", bold=True)

# Clientes (fuera de Docker)
box(30, 360, 290, 505, "Clientes", ["Postman (pruebas)", "Swagger UI (/swagger)", "Navegador / SPA (TB2)", "scripts PowerShell"],
    "#F3F4F6", "#6B7280")

# Coordinador
cx1, cy1, cx2, cy2 = 450, 280, 880, 545
box(cx1, cy1, cx2, cy2, "Coordinador Go · API + clúster", [
    "API REST net/http — puerto 8080",
    "  /health /train /predict /patrullaje /metrics",
    "Servidor TCP del clúster — puerto 9090",
    "Fan-out / fan-in: goroutines + WaitGroup",
    "Correlación petición-respuesta por ID",
    "Latido (PING) cada 3 s · métricas",
    "Combina 32 árboles (voto suave ponderado)",
    "Cola asíncrona (canal) hacia MongoDB",
], "#E3EEFA", "#2F6FB0", title_size=18, line_size=14)

# Nodos ML
nodes = [("ml-node-1", "shard 1/3", "árboles 0-10 (11)"), ("ml-node-2", "shard 2/3", "árboles 11-21 (11)"), ("ml-node-3", "shard 3/3", "árboles 22-31 (10)")]
nx1, nx2 = 1120, 1530
ny = [160, 395, 630]
for (name, shard, trees), y in zip(nodes, ny):
    box(nx1, y, nx2, y + 150, f"{name} · {shard}", [
        f"{trees} · 607,779 denuncias",
        "Carga del CSV: lector + Worker Pool",
        "Entrena con goroutines en paralelo",
        "Responde TRAIN / PREDICT / PING",
    ], "#E5F5EC", "#2E8B57", title_size=17, line_size=14)

# Conexiones TCP coordinador <-> nodos (bidireccionales, a los bordes)
tcp_c = "#2E8B57"
starts = [cy1 + 50, (cy1 + cy2) / 2, cy2 - 50]
for (y0), y in zip(starts, ny):
    yc = y + 75
    arrow(cx2 + 4, y0, nx1 - 4, yc, tcp_c, both=True)
label(cx2 + 14, 178, "TCP · JSON · puerto 9090", size=13, color="#1F6B45", bold=True)
label(cx2 + 14, 198, "TRAIN · PREDICT · PING", size=12, color="#1F6B45")

# Clientes -> Coordinador (HTTP)
arrow(290 + 4, 432, cx1 - 4, 432, "#2F6FB0", both=True)
label(318, 398, "HTTP REST", size=13, color="#1F4E86", bold=True)
label(336, 444, ":8080", size=13, color="#1F4E86", bold=True)

# Bases de datos
mx1, my1, mx2, my2 = 420, 610, 640, 780
box(mx1, my1, mx2, my2, "MongoDB 7", ["base pc4_denuncias", "training_jobs", "predictions", "patrol_recommendations", "cluster_metrics"],
    "#FFF4E0", "#C77700", title_size=17, line_size=13)
rx1, ry1, rx2, ry2 = 690, 610, 910, 780
box(rx1, ry1, rx2, ry2, "Redis 7", ["caché con TTL 10 min", "pc4:pred:v{n}:...", "pc4:patrol:v{n}:...", "pc4:model:current", "pc4:metrics:latest"],
    "#FDECEC", "#C0392B", title_size=17, line_size=13)
arrow((mx1 + mx2) / 2, cy2 + 4, (mx1 + mx2) / 2, my1 - 4, "#C77700", both=True)
arrow((rx1 + rx2) / 2, cy2 + 4, (rx1 + rx2) / 2, ry1 - 4, "#C0392B", both=True)
label((mx1 + mx2) / 2 + 8, cy2 + 24, "persistencia", size=12, color="#8A5300")
label((rx1 + rx2) / 2 + 8, cy2 + 24, "caché", size=12, color="#8E2A20")

# Dataset (volumen de solo lectura)
dx1, dy1, dx2, dy2 = 1000, 815, 1530, 885
box(dx1, dy1, dx2, dy2, "CSV SIDPOL · 28 MB", ["volumen de solo lectura: coordinador y 3 nodos"], "#F1ECFA", "#6C4AB6", title_size=15, line_size=13, center=True)
arrow(nx1 + 205, dy1 - 4, nx1 + 205, ny[2] + 150 + 4, "#6C4AB6", dash=True, width=2)

os.makedirs(os.path.dirname(OUT), exist_ok=True)
img.save(OUT, optimize=True)
print(OUT)
