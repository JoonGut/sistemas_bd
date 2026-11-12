from confluent_kafka import Consumer
import json
import csv
from pathlib import Path

conf = {

    "bootstrap.servers": "localhost:9092",

    "group.id": "superdata",

    "auto.offset.reset": "earliest"

}

consumer = Consumer(conf)

consumer.subscribe(["ventas"])

RUTA_VENTAS = Path("data/ventas.csv")


COLUMNAS = [
    "id_venta",
    "id_cliente",
    "id_producto",
    "cantidad",
    "metodo_pago",
    "timestamp"
]

def guardar_venta(venta):

    archivo_existe = RUTA_VENTAS.exists()

    with open(
        RUTA_VENTAS,
        "a",
        newline="",
        encoding="utf-8"
    ) as archivo:

        writer = csv.DictWriter(
            archivo,
            fieldnames=COLUMNAS
        )

        # Si el archivo no existía,
        # escribimos primero la cabecera
        if not archivo_existe:


            writer.writeheader()

        writer.writerow(venta)


print("Esperando ventas...\n")


try:

    while True:

        msg = consumer.poll(1)

        if msg is None:
            continue

        if msg.error():

            print(msg.error())

            continue


        # Convertimos el mensaje de Kafka
        # de JSON a diccionario

        venta = json.loads(
            msg.value().decode("utf-8")
        )

        print("=" * 50)

        print("Nueva venta recibida")

        print(f"Venta: {venta['id_venta']}")

        print(f"Cliente: {venta['id_cliente']}")

        print(f"Producto: {venta['id_producto']}")

        print(f"Cantidad: {venta['cantidad']}")

        print(f"Pago: {venta['metodo_pago']}")


        # Guardamos la venta

        guardar_venta(venta)

        print("Venta guardada en ventas.csv")

        print("=" * 50)


except KeyboardInterrupt:

    print("\nFinalizando consumidor...")


finally:

    consumer.close()