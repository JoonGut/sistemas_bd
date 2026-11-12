
from confluent_kafka import Consumer
import json
import csv
from collections import Counter
import matplotlib.pyplot as plt

from consumer_comun import (
    RUTA_VENTAS,
    capturar_resumen,
    guardar_figura,
    guardar_resumen_word,
    imagenes_registradas,
)

conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "superdata",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(conf)
consumer.subscribe(["ventas"])


# Crear la carpeta si no existe
RUTA_VENTAS.parent.mkdir(parents=True, exist_ok=True)

COLUMNAS = [
    "id_venta",
    "id_cliente",
    "id_producto",
    "cantidad",
    "metodo_pago",
    "timestamp"
]

# Contador de métodos de pago
contador_pagos = Counter()


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

        if not archivo_existe:
            writer.writeheader()

        writer.writerow(venta)


def mostrar_resumen():

    print("\n" + "=" * 50)
    print("           RESUMEN FINAL DE VENTAS")
    print("=" * 50)

    if not contador_pagos:
        print("No se han recibido ventas.")
        return

    print("\nUSO DE LOS METODOS DE PAGO:\n")

    for metodo, cantidad in contador_pagos.items():
        print(f"{metodo}: {cantidad} veces")



    # Método más utilizado
    maximo = max(contador_pagos.values())

    mas_utilizados = [
        metodo for metodo, cantidad in contador_pagos.items()
        if cantidad == maximo
    ]

    # Método menos utilizado
    minimo = min(contador_pagos.values())

    menos_utilizados = [
        metodo for metodo, cantidad in contador_pagos.items()
        if cantidad == minimo
    ]
    #Porcentaje de uso de cada método
    total_ventas = sum(contador_pagos.values())

    porcentajes = {
        metodo: (cantidad / total_ventas) * 100
        for metodo, cantidad in contador_pagos.items()
    }

    print("\nPORCENTAJE DE USO DE CADA METODO:")
    for metodo, porcentaje in porcentajes.items():
        print(f"{metodo}: {porcentaje:.2f}% del total de ventas")


    print("\n" + "-" * 50)

    print(f"Metodo(s) MAS utilizado(s): {', '.join(mas_utilizados)}")
    print(f"Numero de usos: {maximo}")

    print("\n" + "-" * 50)

    print(f"Metodo(s) MENOS utilizado(s): {', '.join(menos_utilizados)}")
    print(f"Numero de usos: {minimo}")
    print(f"Porcentajes de uso de cada metodo: {porcentajes}")

    print("\n" + "=" * 50)
    # ==================================
    # GRAFICO CON MATPLOTLIB
    # ==================================

    metodos = list(contador_pagos.keys())
    cantidades = list(contador_pagos.values())

    fig, ax = plt.subplots(figsize=(10, 6))

    barras = ax.bar(
        metodos,
        cantidades
    )

    # Mostrar cantidad y porcentaje encima de cada barra
    for barra, metodo in zip(barras, metodos):

        altura = barra.get_height()

        porcentaje = porcentajes[metodo]

        ax.annotate(
            f"{int(altura)} ventas\n{porcentaje:.2f}%",
            xy=(barra.get_x() + barra.get_width() / 2, altura),
            xytext=(0, 5),
            textcoords="offset points",
            ha="center"
        )

    ax.set_title("Ventas por método de pago", fontsize=16)


    ax.set_ylim(0, max(cantidades) * 1.2)

    guardar_figura(fig, "metodos_pago.png", dpi=300)

    plt.close()



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

        print("=" * 50)
        print("Nueva venta recibida")

        print(f"Venta: {venta['id_venta']}")
        print(f"Cliente: {venta['id_cliente']}")
        print(f"Producto: {venta['id_producto']}")
        print(f"Cantidad: {venta['cantidad']}")
        print(f"Pago: {venta['metodo_pago']}")

        # Guardar venta en CSV
        guardar_venta(venta)

        # Contabilizar método de pago
        contador_pagos[venta['metodo_pago']] += 1

        print("Venta guardada en ventas.csv")
        print("=" * 50)

except KeyboardInterrupt:

    print("\nFinalizando consumidor...")

finally:

    with capturar_resumen() as buffer:
        mostrar_resumen()

    guardar_resumen_word(
        "metodos_pago.docx",
        "Ejercicio 2 - Uso de los metodos de pago",
        buffer.getvalue(),
        imagenes_registradas(),
    )

    consumer.close()
