from confluent_kafka import Producer
import json
import random
import time

producer = Producer({"bootstrap.servers": "localhost:9092"})
TOPIC = "ventas_demo"
PAGOS = ["Tarjeta", "Bizum", "Efectivo"]

try:
    while True:
        cantidad = random.randint(1, 5)
        precio = random.uniform(1, 20)
        descuento = random.choice([0, 0, 0, 10])

        venta = {
            "id_venta": int(time.time() * 1000),
            "id_producto": random.randint(1, 150),
            "cantidad": cantidad,
            "metodo_pago": random.choice(PAGOS),
            "importe": round(cantidad * precio * (1 - descuento / 100), 2),
        }

        producer.produce(TOPIC, json.dumps(venta).encode())
        producer.poll(0)
        print(venta)
        time.sleep(1)

except KeyboardInterrupt:
    producer.flush()
