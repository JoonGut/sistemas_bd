import matplotlib.pyplot as plt
import polars as pl

from consumer_comun import (
    RUTA_PRODUCTOS,
    cargar_ventas,
    ejecutar_consumer,
    guardar_figura,
)


def mostrar_resumen():

    print("\n" + "=" * 60)
    print("EJERCICIO 5 - ANALISIS DE LOS PRODUCTOS VENDIDOS")
    print("=" * 60)

    df = cargar_ventas()

    if df.height == 0:
        print("El archivo no contiene ventas todavia.")
        return

    catalogo = pl.read_csv(RUTA_PRODUCTOS)

    por_producto = (
        df.group_by("id_producto")
        .agg(
            pl.len().alias("ventas"),
            pl.col("cantidad").sum().alias("unidades"),
        )
        .join(
            catalogo.select("id_producto", "nombre", "categoria"),
            on="id_producto",
            how="left",
        )
    )

    distintos = por_producto.height

    print(f"\n1. PRODUCTOS DIFERENTES VENDIDOS: {distintos}")
    print(
        f"   Sobre un catalogo de {catalogo.height} productos, "
        f"se ha vendido el {distintos / catalogo.height * 100:.2f}%."
    )

    mas_ventas = por_producto.sort("ventas", descending=True).head(10)

    print("\n2. PRODUCTOS QUE APARECEN EN MAS VENTAS:")
    for posicion, fila in enumerate(mas_ventas.iter_rows(named=True), start=1):
        print(
            f"   {posicion:>2}. {fila['nombre']:<22} "
            f"(id {fila['id_producto']}, {fila['categoria']}) -> "
            f"{fila['ventas']} ventas"
        )

    mas_vendidos = por_producto.sort("unidades", descending=True).head(10)

    print("\n3. TOP 10 PRODUCTOS MAS VENDIDOS (UNIDADES):")
    for posicion, fila in enumerate(
        mas_vendidos.iter_rows(named=True), start=1
    ):
        print(
            f"   {posicion:>2}. {fila['nombre']:<22} "
            f"(id {fila['id_producto']}, {fila['categoria']}) -> "
            f"{fila['unidades']} unidades en {fila['ventas']} ventas"
        )

    etiquetas = [
        f"{fila['nombre']} (#{fila['id_producto']})"
        for fila in mas_vendidos.iter_rows(named=True)
    ]
    unidades = mas_vendidos["unidades"].to_list()

    fig, ax = plt.subplots(figsize=(11, 7))

    barras = ax.barh(etiquetas, unidades, color="#8172B2")

    for barra in barras:
        ax.annotate(
            str(int(barra.get_width())),
            xy=(barra.get_width(), barra.get_y() + barra.get_height() / 2),
            xytext=(5, 0),
            textcoords="offset points",
            va="center",
        )

    ax.invert_yaxis()
    ax.set_title("Top 10 productos mas vendidos (unidades)", fontsize=16)
    ax.set_xlabel("Unidades vendidas")
    ax.set_ylabel("Producto")
    ax.set_xlim(0, max(unidades) * 1.15)

    guardar_figura(fig, "top_10_productos.png")
    plt.close(fig)

    print("\n" + "=" * 60)


if __name__ == "__main__":
    ejecutar_consumer(
        group_id="superdata-ej5",
        mostrar_resumen=mostrar_resumen,
        nombre_word="ej5_productos.docx",
        titulo_word="Ejercicio 5 - Analisis de los productos vendidos",
    )
