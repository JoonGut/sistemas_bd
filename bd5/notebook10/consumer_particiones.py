from confluent_kafka import Consumer
import json
import uuid # lo utilizamos solamente para generar un identificador diferente para cada proceso consumidor

TOPIC = "ventas_demo"
GROUP_ID = "grupo_superdata" # los consumidores van a pertenecer al mismo consumer group
CONSUMER_ID = str(uuid.uuid4())[:8]

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest"
    # Esta opción dice qué hacer cuando el grupo todavía no tiene un offset guardado para una partición.
    # earliest: Empieza desde el mensaje más antiguo disponible.
    # Si creamos un Consumer Group nuevo, podrá leer los mensajes que ya existían en el topic.
})

# Definimos una función simplemente para ver por pantalla cómo kafka distribuye las particiones:
def on_assign(consumer, partitions):
    print(
        f"[{CONSUMER_ID}] Particiones asignadas: "
        f"{[p.partition for p in partitions]}"
    )

consumer.subscribe([TOPIC], on_assign=on_assign)

print(f"Consumer {CONSUMER_ID} iniciado.")
print(f"Grupo: {GROUP_ID}")
print("Ctrl+C para detener.\n")

try:
    while True:
        msg = consumer.poll(1)

        if msg is None:
            continue

        if msg.error():
            print(f"ERROR: {msg.error()}")
            continue

        venta = json.loads(msg.value().decode("utf-8"))

        print(
            f"[{CONSUMER_ID}] "
            f"Venta {venta['id_venta']} | "
            f"cliente {venta['id_cliente']} | "
            f"partición {msg.partition()} | "
            f"offset {msg.offset()}"
        )

except KeyboardInterrupt:
    print("\nDeteniendo consumer...")
finally:
    consumer.close()
