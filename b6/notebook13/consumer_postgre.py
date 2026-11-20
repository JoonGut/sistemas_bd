from confluent_kafka import Consumer

import json

from postgres_utils import (
    conectar,
    insertar_venta
)

# COnfiguración

BOOTSTRAP_SERVERS = "localhost:9092"

TOPIC = "ventas_postgres"

GROUP_ID = "superdata_postgres"

# Consumer

consumer = Consumer({

    "bootstrap.servers":
        BOOTSTRAP_SERVERS,

    "group.id":
        GROUP_ID,

    "auto.offset.reset":
        "latest"
})

consumer.subscribe([TOPIC])

# Conexión con Postgre

conn = conectar()

print("Consumer iniciado.")
print(f"Topic: {TOPIC}")
print(f"Grupo: {GROUP_ID}")
print("Esperando ventas...\n")

# Recibir mensajes

try:

    while True:

        msg = consumer.poll(1)

        if msg is None:
            continue

        if msg.error():

            print(
                f"Error Kafka: {msg.error()}"
            )

            continue

        venta = json.loads(
            msg.value().decode("utf-8")
        )

        print(
            f"Nueva venta recibida: "
            f"{venta}"
        )

        try:

            insertar_venta(
                conn,
                venta
            )

            print(
                f"Venta {venta['id_venta']} "
                f"almacenada en PostgreSQL"
            )

        except Exception as e:

            conn.rollback()

            print(
                f"Error almacenando la venta: {e}"
            )

except KeyboardInterrupt:

    print(
        "\nDeteniendo consumer..."
    )

finally:

    consumer.close()

    conn.close()

    print(
        "Consumer y conexión PostgreSQL cerrados."
    )