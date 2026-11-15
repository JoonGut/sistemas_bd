# Vamos a usar herramientas para administrar Kafka, no para producir o consumir mensajes.

from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "ventas_demo" # guardamos el nombre del topic en una variable

admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS}) 
# objeto administrador: objeto que se conecta a Kafka y nos permite administrarlo

metadata = admin.list_topics(timeout=10) # Consultamos los topics existentes

if TOPIC in metadata.topics:
    print(f"El topic '{TOPIC}' ya existe.") # si existe solo lo mostramos
else:
    futures = admin.create_topics([
        NewTopic(TOPIC, num_partitions=3, replication_factor=1) 
        # sino, creamos el topic con 3 particiones y un factor de replicación de 1
        # tenemos una única copia de cada partición, pero se puede aujmentar para mantener tolerancia a fallos
    ])

    # Future es un mecanismo que utiliza la librería para gestionar una operación que se realiza en Kafka 
    # y comprobar si ha terminado correctamente. No necesitamos conocer su funcionamiento interno.

    for name, future in futures.items():
        try:
            future.result() # Esperamos a que Kafka termine de crear el topic 
            # y comprobamos si la operación ha sido correcta
            print(f"Topic '{name}' creado con 3 particiones.")
        except Exception as e:
            print(f"No se pudo crear el topic: {e}")
