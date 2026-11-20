from confluent_kafka import Producer

import polars as pl
import random
import json
import time
from datetime import datetime


# Configuración

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "ventas_postgres"

# Producer

producer = Producer({
    "bootstrap.servers": BOOTSTRAP_SERVERS
})

clientes = pl.read_csv(
    "../../data/clientes.csv"
)

productos = pl.read_csv(
    "../../data/productos.csv"
)

metodos_pago = [
    "Tarjeta",
    "Bizum",
    "Efectivo"
]

def delivery_report(err, msg):

    if err is not None:
        print(
            f"Error enviando mensaje: {err}"
        )
    else:
        print(
            f"Venta enviada a Kafka "
            f"(topic={msg.topic()}, "
            f"partición={msg.partition()})"
        )

if __name__ == "__main__":

    print("Generando ventas en tiempo real...\n")

    id_venta = max(100000, int(time.time()))

    while True:

        cliente = clientes.sample(
            n=1
        ).to_dicts()[0]

        producto = productos.sample(
            n=1
        ).to_dicts()[0]

        venta = {

            "id_venta": id_venta,

            "fecha": datetime.now().isoformat(
                timespec="seconds"
            ),

            "id_cliente": cliente["id_cliente"],

            "id_producto": producto["id_producto"],

            "cantidad": random.randint(1, 5),

            "metodo_pago": random.choice(
                metodos_pago
            ),

            "descuento": random.choice(
                [0, 5, 10, 15]
            )
        }

        print(
            f"Venta generada: {venta}"
        )

        producer.produce(
            topic=TOPIC,
            value=json.dumps(
                venta
            ).encode("utf-8"),
            callback=delivery_report
        )

        producer.poll(0)

        id_venta += 1

        time.sleep(2)