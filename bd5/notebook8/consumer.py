from confluent_kafka import Consumer

import json

conf = {

    "bootstrap.servers":"localhost:9092",

    "group.id":"superdata",

    "auto.offset.reset":"earliest"

}

consumer = Consumer(conf)

consumer.subscribe(["ventas"])

print("Esperando ventas...\n")

try:

    while True:

        msg = consumer.poll(1)

        if msg is None:
            continue

        if msg.error():
            print(msg.error())
            continue

        venta = json.loads(
            msg.value().decode("utf-8")
        )

        print("="*50)

        print("Nueva venta recibida")

        print(f"Venta: {venta['id_venta']}")

        print(f"Cliente: {venta['id_cliente']}")

        print(f"Producto: {venta['id_producto']}")

        print(f"Cantidad: {venta['cantidad']}")

        print(f"Pago: {venta['metodo_pago']}")

        print("="*50)

except KeyboardInterrupt:

    print("Finalizando consumidor...")

finally:

    consumer.close()