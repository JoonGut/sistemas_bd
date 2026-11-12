import matplotlib.pyplot as plt
import polars as pl

from consumer_comun import (
    RUTA_CLIENTES,
    cargar_ventas,
    ejecutar_consumer,
    guardar_figura,
)


def mostrar_resumen():

    print("\n" + "=" * 60)
    print("EJERCICIO 4 - COMPORTAMIENTO DE LOS CLIENTES")
    print("=" * 60)

    df = cargar_ventas()

    if df.height == 0:
        print("El archivo no contiene ventas todavia.")
        return

    por_cliente = (
        df.group_by("id_cliente")
        .agg(
            pl.len().alias("compras"),
            pl.col("cantidad").sum().alias("unidades"),
        )
        .sort(["compras", "unidades"], descending=[True, True])
    )

    total_clientes = por_cliente.height
    catalogo = pl.read_csv(RUTA_CLIENTES)
    total_catalogo = catalogo.height

    print(f"\n1. CLIENTES DIFERENTES QUE HAN COMPRADO: {total_clientes}")
    print(
        f"   Sobre un catalogo de {total_catalogo} clientes, "
        f"han comprado el {total_clientes / total_catalogo * 100:.2f}%."
    )

    maximo = por_cliente["compras"].max()
    mas_frecuentes = por_cliente.filter(pl.col("compras") == maximo)

    print(f"\n2. CLIENTES QUE APARECEN MAS VECES ({maximo} compras):")
    for fila in mas_frecuentes.iter_rows(named=True):
        print(
            f"   Cliente {fila['id_cliente']} -> "
            f"{fila['compras']} compras, {fila['unidades']} unidades"
        )

    top_10 = (
        por_cliente.head(10)
        .join(
            catalogo.select("id_cliente", "nombre", "apellidos"),
            on="id_cliente",
            how="left",
        )
    )

    print("\n3. TOP 10 CLIENTES CON MAS COMPRAS:")
    for posicion, fila in enumerate(top_10.iter_rows(named=True), start=1):
        nombre = fila["nombre"] or "desconocido"
        print(
            f"   {posicion:>2}. Cliente {fila['id_cliente']:<4} "
            f"({nombre}) -> {fila['compras']} compras, "
            f"{fila['unidades']} unidades"
        )

    una_compra = por_cliente.filter(pl.col("compras") == 1).height

    print(f"\n   Clientes con una sola compra: {una_compra}")

    etiquetas = [
        f"{fila['id_cliente']}\n{fila['nombre'] or ''}"
        for fila in top_10.iter_rows(named=True)
    ]
    compras = top_10["compras"].to_list()

    fig, ax = plt.subplots(figsize=(12, 6))

    barras = ax.bar(etiquetas, compras, color="#C44E52")

    for barra in barras:
        ax.annotate(
            str(int(barra.get_height())),
            xy=(barra.get_x() + barra.get_width() / 2, barra.get_height()),
            xytext=(0, 5),
            textcoords="offset points",
            ha="center",
        )

    ax.set_title("Top 10 clientes con mas compras", fontsize=16)
    ax.set_xlabel("ID y nombre del cliente")
    ax.set_ylabel("Numero de compras")
    ax.set_ylim(0, max(compras) * 1.2)
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")

    guardar_figura(fig, "top_10_clientes.png")
    plt.close(fig)

    print("\n" + "=" * 60)


if __name__ == "__main__":
    ejecutar_consumer(
        group_id="superdata-ej4",
        mostrar_resumen=mostrar_resumen,
        nombre_word="clientes.docx",
        titulo_word="Ejercicio 4 - Comportamiento de los clientes",
    )
