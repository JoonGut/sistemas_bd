from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")

import polars as pl
from confluent_kafka import Consumer

RUTA_DATA = Path("data")
RUTA_VENTAS = RUTA_DATA / "ventas.csv"
RUTA_IMAGENES = RUTA_DATA / "imagenes"
RUTA_CLIENTES = RUTA_DATA / "clientes.csv"
RUTA_PRODUCTOS = RUTA_DATA / "productos.csv"

TOPIC = "ventas"

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


def guardar_figura(fig, nombre: str, dpi: int = 150) -> Path:
    ruta = ruta_imagen(nombre)
    fig.tight_layout()
    fig.savefig(ruta, dpi=dpi, bbox_inches="tight")
    print(f"\nGrafico guardado en {ruta}")
    return ruta


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
        mostrar_resumen()
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

    mostrar_resumen()
