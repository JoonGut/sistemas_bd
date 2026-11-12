from __future__ import annotations

import argparse
import io
import json
import sys
from contextlib import contextmanager
from pathlib import Path

import matplotlib
matplotlib.use("Agg")

import polars as pl
from confluent_kafka import Consumer
from docx import Document
from docx.shared import Inches

RUTA_DATA = Path("data")
RUTA_VENTAS = RUTA_DATA / "ventas.csv"
RUTA_IMAGENES = RUTA_DATA / "imagenes"
RUTA_DOCS = RUTA_DATA / "docs"
RUTA_CLIENTES = RUTA_DATA / "clientes.csv"
RUTA_PRODUCTOS = RUTA_DATA / "productos.csv"

TOPIC = "ventas"

_imagenes_registradas: list[Path] = []

COLUMNAS = [
    "id_venta",
    "id_cliente",
    "id_producto",
    "cantidad",
    "metodo_pago",
    "timestamp",
]


def ruta_imagen(nombre: str) -> Path:
    RUTA_IMAGENES.mkdir(parents=True, exist_ok=True)
    return RUTA_IMAGENES / nombre


def reiniciar_imagenes() -> None:
    _imagenes_registradas.clear()


def imagenes_registradas() -> list[Path]:
    return list(_imagenes_registradas)


def guardar_figura(fig, nombre: str, dpi: int = 150) -> Path:
    ruta = ruta_imagen(nombre)
    fig.tight_layout()
    fig.savefig(ruta, dpi=dpi, bbox_inches="tight")
    _imagenes_registradas.append(ruta)
    print(f"\nGrafico guardado en {ruta}")
    return ruta


class _Tee:
    def __init__(self, *streams):
        self.streams = streams

    def write(self, data):
        for stream in self.streams:
            stream.write(data)

    def flush(self):
        for stream in self.streams:
            stream.flush()


@contextmanager
def capturar_resumen():
    reiniciar_imagenes()
    buffer = io.StringIO()
    original = sys.stdout
    sys.stdout = _Tee(original, buffer)
    try:
        yield buffer
    finally:
        sys.stdout = original


def guardar_resumen_word(
    nombre_archivo: str,
    titulo: str,
    texto: str,
    imagenes=None,
) -> Path:
    RUTA_DOCS.mkdir(parents=True, exist_ok=True)
    ruta = RUTA_DOCS / nombre_archivo

    doc = Document()
    doc.add_heading(titulo, level=0)

    for linea in texto.splitlines():
        limpia = linea.strip()
        if not limpia or set(limpia) <= {"=", "-"}:
            continue

        if limpia.startswith("EJERCICIO") or limpia.startswith("PREGUNTA"):
            doc.add_heading(limpia, level=1)
        else:
            doc.add_paragraph(linea.rstrip())

    for imagen in imagenes or []:
        ruta_imagen_actual = Path(imagen)
        if not ruta_imagen_actual.exists():
            continue
        doc.add_page_break()
        doc.add_heading(ruta_imagen_actual.stem, level=2)
        doc.add_picture(str(ruta_imagen_actual), width=Inches(6))

    doc.save(ruta)
    print(f"Word guardado en {ruta}")
    return ruta


def _ejecutar_y_guardar(
    mostrar_resumen,
    nombre_word: str | None,
    titulo_word: str,
) -> None:
    with capturar_resumen() as buffer:
        mostrar_resumen()

    if nombre_word:
        guardar_resumen_word(
            nombre_word,
            titulo_word,
            buffer.getvalue(),
            imagenes_registradas(),
        )


def cargar_ventas() -> pl.DataFrame:
    if not RUTA_VENTAS.exists():
        raise SystemExit(
            "No existe data/ventas.csv. Arranca el producer y este consumer "
            "para generar ventas, o ejecuta el script con --offline cuando "
            "ya tengas datos."
        )

    return pl.read_csv(RUTA_VENTAS)


def guardar_ventas(nuevas: list[dict]) -> int:
    if not nuevas:
        return 0

    RUTA_VENTAS.parent.mkdir(parents=True, exist_ok=True)

    df_nuevas = pl.DataFrame(nuevas).select(COLUMNAS)

    if RUTA_VENTAS.exists():
        df_total = pl.concat(
            [pl.read_csv(RUTA_VENTAS).select(COLUMNAS), df_nuevas],
            how="vertical",
        )
    else:
        df_total = df_nuevas

    df_total = (
        df_total.unique(subset=["id_venta"], keep="last")
        .sort("id_venta")
    )

    df_total.write_csv(RUTA_VENTAS)

    return df_total.height


def ejecutar_consumer(
    group_id: str,
    mostrar_resumen,
    procesar_venta=None,
    nombre_word: str | None = None,
    titulo_word: str = "Resumen de ventas",
) -> None:
    parser = argparse.ArgumentParser(
        description="Consumer de ventas con analisis final en Polars"
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="No conecta a Kafka: analiza directamente data/ventas.csv",
    )
    args = parser.parse_args()

    if args.offline:
        print("Modo offline: analizando data/ventas.csv sin Kafka.\n")
        _ejecutar_y_guardar(mostrar_resumen, nombre_word, titulo_word)
        return

    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": group_id,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TOPIC])

    pendientes: list[dict] = []

    print(f"Consumer '{group_id}' escuchando el topic '{TOPIC}'.")
    print("Pulsa Ctrl+C para detener y lanzar el analisis con Polars.\n")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print("Error de Kafka:", msg.error())
                continue

            venta = json.loads(msg.value().decode("utf-8"))
            pendientes.append(venta)

            print(
                f"Venta {venta['id_venta']} | "
                f"cliente {venta['id_cliente']} | "
                f"producto {venta['id_producto']} | "
                f"{venta['cantidad']} uds | "
                f"{venta['metodo_pago']}"
            )

            if procesar_venta is not None:
                procesar_venta(venta)

    except KeyboardInterrupt:
        print("\n\nFinalizando consumer...")

    finally:
        consumer.close()

    total = guardar_ventas(pendientes)

    if pendientes:
        print(
            f"{len(pendientes)} ventas recibidas. "
            f"{RUTA_VENTAS} contiene ahora {total} registros unicos."
        )

    _ejecutar_y_guardar(mostrar_resumen, nombre_word, titulo_word)
