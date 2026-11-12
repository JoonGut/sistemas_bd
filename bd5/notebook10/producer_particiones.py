from confluent_kafka import Producer
import json
import random
import time

producer = Producer({"bootstrap.servers": "localhost:9092"})
TOPIC = "ventas_demo"

def delivery_report(err, msg):
    if err:
        print(f"ERROR: {err}")
    else:
        print(
            f"Venta {msg.key().decode()} -> "
            f"partición {msg.partition()}, offset {msg.offset()}"
        )

id_venta = 1

try:
    print("Producer iniciado. Ctrl+C para detener.")

    while True:
        venta = {
            "id_venta": id_venta,
            "id_cliente": random.randint(1, 500),
            "id_producto": random.randint(1, 150),
            "cantidad": random.randint(1, 5)
        }

        clave = str(venta["id_cliente"])

        producer.produce(
            TOPIC,
            key=clave.encode("utf-8"),
            value=json.dumps(venta).encode("utf-8"),
            callback=delivery_report
        )

        producer.poll(0)
        id_venta += 1
        time.sleep(1)

except KeyboardInterrupt:
    print("\nDeteniendo producer...")
finally:
    producer.flush()
