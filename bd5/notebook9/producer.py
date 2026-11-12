
from confluent_kafka import Producer
from pathlib import Path
import polars as pl
import random
import json
import time

conf = {
    "bootstrap.servers": "localhost:9092"
}

producer = Producer(conf)

data_dir = Path("data")

clientes = pl.read_csv(data_dir / "clientes.csv")
productos = pl.read_csv(data_dir / "productos.csv")

# Leer ventas existentes
ventas_path = data_dir / "ventas.csv"

if ventas_path.exists():
    ventas = pl.read_csv(ventas_path)

    if ventas.height > 0:
        # Obtener el último ID de venta
        id_venta = int(ventas["id_venta"][-1]) + 1
    else:
        id_venta = 1
else:
    id_venta = 1


metodos_pago = [
    "Tarjeta",
    "Bizum",
    "Efectivo"
]


if __name__ == "__main__":

    print("Generando ventas en tiempo real...\n")
    print(f"Continuando desde la venta: {id_venta}\n")

    while True:

        # Seleccionamos un cliente
        cliente = clientes.sample(1).to_dicts()[0]

        # Seleccionamos un producto
        producto = productos.sample(1).to_dicts()[0]

        # Creamos la venta
        venta = {
            "id_venta": id_venta,
            "id_cliente": cliente["id_cliente"],
            "id_producto": producto["id_producto"],
            "cantidad": random.randint(1, 5),
            "metodo_pago": random.choice(metodos_pago),
            "timestamp": time.time()
        }

        print(f"Venta enviada: {venta}")

        # Enviamos la venta a Kafka
        producer.produce(
            topic="ventas",
            value=json.dumps(venta).encode("utf-8")
        )

        producer.poll(0)

        id_venta += 1

        time.sleep(2)
