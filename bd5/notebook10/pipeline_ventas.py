import json
import time
from pathlib import Path

import polars as pl
from confluent_kafka import Consumer
from prefect import flow, task

TOPIC = "ventas_demo"
VENTAS = Path("data/ventas.csv")
INFORME = Path("data/informe_ventas.csv")


@task
def consumir(segundos: int = 20) -> pl.DataFrame:
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "prefect_ventas",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])

    fin = time.monotonic() + segundos
    ventas = []

    try:
        while time.monotonic() < fin:
            msg = consumer.poll(1)
            if msg and not msg.error():
                ventas.append(json.loads(msg.value()))

        if ventas:
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()

    return pl.DataFrame(ventas)


@task
def guardar(ventas: pl.DataFrame) -> None:
    if ventas.is_empty():
        return

    VENTAS.parent.mkdir(parents=True, exist_ok=True)

    total = (
        pl.concat([pl.read_csv(VENTAS), ventas], how="diagonal_relaxed")
        if VENTAS.exists()
        else ventas
    )

    total.unique(subset="id_venta", keep="last").sort("id_venta").write_csv(VENTAS)


@task
def informe() -> None:
    if not VENTAS.exists():
        return

    df = pl.read_csv(VENTAS)

    resumen = (
        df.group_by("metodo_pago")
        .agg(
            pl.len().alias("ventas"),
            pl.col("cantidad").sum().alias("unidades"),
            pl.col("importe").sum().round(2).alias("ingresos"),
        )
        .sort("ingresos", descending=True)
    )

    resumen.write_csv(INFORME)
    print(resumen)
    print("Ingresos totales:", round(df["importe"].sum(), 2))


@flow(name="Ventas Supermercado")
def pipeline_ventas(segundos: int = 20):
    ventas = consumir(segundos)
    guardar(ventas)
    informe()


if __name__ == "__main__":
    pipeline_ventas()
