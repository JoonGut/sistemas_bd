import matplotlib.pyplot as plt
import polars as pl

from consumer_comun import cargar_ventas, ejecutar_consumer, guardar_figura


def mostrar_resumen():

    print("\n" + "=" * 60)
    print("EJERCICIO 3 - ANALISIS DE LA COLUMNA cantidad")
    print("=" * 60)

    df = cargar_ventas()

    if df.height == 0:
        print("El archivo no contiene ventas todavia.")
        return

    stats = df.select(
        pl.col("cantidad").min().alias("minima"),
        pl.col("cantidad").max().alias("maxima"),
        pl.col("cantidad").mean().alias("media"),
        pl.col("cantidad").median().alias("mediana"),
        pl.col("cantidad").sum().alias("total"),
    ).row(0, named=True)

    print(f"\n   Cantidad minima: {stats['minima']}")
    print(f"   Cantidad maxima: {stats['maxima']}")
    print(f"   Cantidad media: {stats['media']:.2f}")
    print(f"   Cantidad mediana: {stats['mediana']:.2f}")
    print(f"   Total de productos vendidos: {stats['total']}")

    distribucion = (
        df.group_by("cantidad")
        .agg(pl.len().alias("ventas"))
        .with_columns(
            (pl.col("ventas") / df.height * 100)
            .round(2)
            .alias("porcentaje")
        )
        .sort("cantidad")
    )

    print("\nDISTRIBUCION DE LAS CANTIDADES:")
    for fila in distribucion.iter_rows(named=True):
        print(
            f"   {fila['cantidad']} unidades -> "
            f"{fila['ventas']:>5} ventas "
            f"({fila['porcentaje']:>6.2f}%)"
        )

    maximo = distribucion["ventas"].max()
    modas = distribucion.filter(pl.col("ventas") == maximo)

    texto_modas = ", ".join(
        f"{fila['cantidad']} ({fila['porcentaje']:.2f}%)"
        for fila in modas.iter_rows(named=True)
    )

    pocas = distribucion.filter(pl.col("cantidad") <= 2)["porcentaje"].sum()
    muchas = distribucion.filter(pl.col("cantidad") >= 4)["porcentaje"].sum()

    print("\nCONCLUSION:")
    print(
        f"   Las cantidades mas frecuentes son: {texto_modas}, "
        f"con {maximo} ventas cada una."
    )
    print(
        f"   El {pocas:.2f}% de las ventas contiene 1 o 2 productos, "
        f"frente al {muchas:.2f}% que contiene 4 o 5."
    )

    if pocas - muchas > 5:
        print(
            "   Lo habitual es que cada venta contenga pocos productos."
        )
    elif muchas - pocas > 5:
        print(
            "   Lo habitual es que cada venta contenga varios productos."
        )
    else:
        print(
            "   La distribucion es practicamente uniforme: no hay una "
            "tendencia clara hacia compras pequenas ni grandes. Esto es "
            "tipico de un producer que genera la cantidad de forma aleatoria."
        )

    cantidades = distribucion["cantidad"].to_list()
    ventas = distribucion["ventas"].to_list()

    fig, ax = plt.subplots(figsize=(10, 6))

    barras = ax.bar(
        [str(c) for c in cantidades],
        ventas,
        color="#55A868",
    )

    for barra, fila in zip(barras, distribucion.iter_rows(named=True)):
        ax.annotate(
            f"{fila['ventas']}\n{fila['porcentaje']:.1f}%",
            xy=(barra.get_x() + barra.get_width() / 2, barra.get_height()),
            xytext=(0, 5),
            textcoords="offset points",
            ha="center",
            fontsize=9,
        )

    ax.set_title("Distribucion de la cantidad por venta", fontsize=16)
    ax.set_xlabel("Unidades por venta")
    ax.set_ylabel("Numero de ventas")
    ax.set_ylim(0, max(ventas) * 1.2)

    guardar_figura(fig, "distribucion_cantidades.png")
    plt.close(fig)

    print("\n" + "=" * 60)


if __name__ == "__main__":
    ejecutar_consumer(
        group_id="superdata-ej3",
        mostrar_resumen=mostrar_resumen,
        nombre_word="cantidades.docx",
        titulo_word="Ejercicio 3 - Analisis de la columna cantidad",
    )
