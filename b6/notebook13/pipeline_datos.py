import json
import time

import polars as pl
from confluent_kafka import Consumer
from prefect import flow, get_run_logger, task

from postgres_utils import (
    cerrar_conexion,
    conectar,
    crear_tabla_desde_polars,
    ejecutar_consulta,
    insertar_dataframe_sin_duplicados
)

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "ventas_postgres"
GROUP_ID = "superdata_prefect"
TABLA = "ventas"

ESQUEMA_VENTAS = {
    "id_venta": pl.Int64,
    "fecha": pl.String,
    "id_cliente": pl.Int64,
    "id_producto": pl.Int64,
    "cantidad": pl.Int64,
    "metodo_pago": pl.String,
    "descuento": pl.Int64
}


@task(
    name="Consumir ventas de Kafka",
    retries=2,
    retry_delay_seconds=5
)
def consumir_ventas(segundos: int = 20) -> list[dict]:

    logger = get_run_logger()

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    consumer.subscribe([TOPIC])

    fin = time.monotonic() + segundos

    ventas = []

    try:

        while time.monotonic() < fin:

            msg = consumer.poll(1)

            if msg is None:
                continue

            if msg.error():
                logger.warning(f"Error Kafka: {msg.error()}")
                continue

            ventas.append(
                json.loads(msg.value().decode("utf-8"))
            )

        if ventas:
            consumer.commit(asynchronous=False)

    finally:
        consumer.close()

    logger.info(f"Ventas recibidas de Kafka: {len(ventas)}")

    return ventas


@task(name="Transformar ventas con Polars")
def transformar_ventas(ventas: list[dict]) -> pl.DataFrame:

    logger = get_run_logger()

    if not ventas:
        return pl.DataFrame(schema=ESQUEMA_VENTAS)

    df = pl.DataFrame(ventas).select(
        pl.col("id_venta").cast(pl.Int64),
        pl.col("fecha").cast(pl.String),
        pl.col("id_cliente").cast(pl.Int64),
        pl.col("id_producto").cast(pl.Int64),
        pl.col("cantidad").cast(pl.Int64),
        pl.col("metodo_pago").cast(pl.String),
        pl.col("descuento")
        .cast(pl.Float64)
        .round()
        .cast(pl.Int64)
    )

    df = (
        df.drop_nulls()
        .filter(
            (pl.col("cantidad") > 0)
            & pl.col("descuento").is_between(0, 100)
        )
        .unique(subset="id_venta", keep="last")
        .sort("id_venta")
    )

    logger.info(f"Ventas válidas tras la transformación: {df.height}")

    return df


@task(name="Guardar ventas en PostgreSQL")
def guardar_ventas(df: pl.DataFrame) -> int:

    logger = get_run_logger()

    if df.is_empty():
        logger.info("No hay ventas nuevas que guardar.")
        return 0

    conn = conectar()

    try:

        crear_tabla_desde_polars(
            conn,
            df,
            TABLA,
            "id_venta"
        )

        insertadas = insertar_dataframe_sin_duplicados(
            conn,
            df,
            TABLA,
            "id_venta"
        )

    finally:
        cerrar_conexion(conn)

    logger.info(f"Ventas insertadas en PostgreSQL: {insertadas}")

    return insertadas


@task(name="Resumen de ventas")
def resumen_ventas() -> pl.DataFrame:

    logger = get_run_logger()

    conn = conectar()

    try:

        filas = ejecutar_consulta(
            conn,
            """
            SELECT
                metodo_pago,
                COUNT(*) AS ventas,
                SUM(cantidad) AS unidades
            FROM ventas
            GROUP BY metodo_pago
            ORDER BY ventas DESC
            """
        )

        total = ejecutar_consulta(
            conn,
            "SELECT COUNT(*) FROM ventas"
        )[0][0]

    finally:
        cerrar_conexion(conn)

    resumen = pl.DataFrame(
        filas,
        schema=["metodo_pago", "ventas", "unidades"],
        orient="row"
    )

    logger.info(f"Total de ventas en PostgreSQL: {total}")
    logger.info(f"\n{resumen}")

    return resumen


@flow(name="Pipeline Ventas Superdata", log_prints=True)
def pipeline_datos(segundos: int = 20):

    ventas = consumir_ventas(segundos)
    df = transformar_ventas(ventas)
    guardar_ventas(df)
    resumen_ventas()


if __name__ == "__main__":
    pipeline_datos()
