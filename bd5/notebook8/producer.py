from confluent_kafka import Producer
import polars as pl
import random
import json
import time

conf = {
    "bootstrap.servers": "localhost:9092"
}

producer = Producer(conf)

# Leemos los datasets
clientes = pl.read_csv("data/clientes.csv")
productos = pl.read_csv("data/productos.csv")

metodos_pago = [
    "Tarjeta",
    "Bizum",
    "Efectivo"
]

if __name__ == "__main__":

    print("Generando ventas en tiempo real...\n")

    id_venta = 1

    while True:

        cliente = clientes.sample(1).to_dicts()[0]
        producto = productos.sample(1).to_dicts()[0]

        venta = {

            "id_venta": id_venta,

            "id_cliente": cliente["id_cliente"],

            "id_producto": producto["id_producto"],

            "cantidad": random.randint(1,5),

            "metodo_pago": random.choice(
                metodos_pago
            ),

            "timestamp": time.time()

        }

        print(f"Venta enviada: {venta}")

        producer.produce(

            topic="ventas",

            value=json.dumps(venta).encode("utf-8")

        )

        producer.poll(0)

        id_venta += 1

        time.sleep(2)