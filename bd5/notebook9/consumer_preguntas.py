import matplotlib.pyplot as plt
import polars as pl

from consumer_comun import (
    RUTA_CLIENTES,
    RUTA_PRODUCTOS,
    cargar_ventas,
    ejecutar_consumer,
    guardar_figura,
)


def cargar_datos():

    ventas = cargar_ventas()

    clientes = pl.read_csv(RUTA_CLIENTES)
    productos = pl.read_csv(RUTA_PRODUCTOS)

    completo = (
        ventas.join(
            productos.select(
                "id_producto",
                pl.col("nombre").alias("producto"),
                "categoria",
                "precio",
            ),
            on="id_producto",
            how="left",
        )
        .join(
            clientes.select(
                "id_cliente",
                "edad",
                "provincia",
                "socio",
                "tipo_cliente",
            ),
            on="id_cliente",
            how="left",
        )
        .with_columns(
            (pl.col("cantidad") * pl.col("precio")).alias("importe")
        )
    )

    return ventas, completo


def grafico_barras_horizontales(
    etiquetas,
    valores,
    titulo,
    xlabel,
    archivo,
    color,
    formato="{:,.0f}",
):

    fig, ax = plt.subplots(figsize=(11, 7))

    barras = ax.barh(etiquetas, valores, color=color)

    for barra in barras:
        ax.annotate(
            formato.format(barra.get_width()),
            xy=(barra.get_width(), barra.get_y() + barra.get_height() / 2),
            xytext=(5, 0),
            textcoords="offset points",
            va="center",
            fontsize=9,
        )

    ax.invert_yaxis()
    ax.set_title(titulo, fontsize=15)
    ax.set_xlabel(xlabel)
    ax.set_xlim(0, max(valores) * 1.18)

    guardar_figura(fig, archivo)
    plt.close(fig)


def pregunta_1(completo):

    print("\n" + "=" * 60)
    print("PREGUNTA 1 - QUE CATEGORIAS GENERAN MAS INGRESOS")
    print("=" * 60)
    print("\nQUE QUEREMOS AVERIGUAR:")
    print("   Que categorias aportan mas facturacion, no solo mas ventas.")

    por_categoria = (
        completo.group_by("categoria")
        .agg(
            pl.len().alias("ventas"),
            pl.col("cantidad").sum().alias("unidades"),
            pl.col("importe").sum().round(2).alias("importe"),
        )
        .sort("importe", descending=True)
    )

    total = por_categoria["importe"].sum()

    print("\nRESULTADO CON POLARS:")
    for posicion, fila in enumerate(
        por_categoria.iter_rows(named=True), start=1
    ):
        print(
            f"   {posicion:>2}. {fila['categoria']:<14} "
            f"{fila['importe']:>10.2f} EUR "
            f"({fila['importe'] / total * 100:>5.2f}%) | "
            f"{fila['ventas']} ventas | {fila['unidades']} unidades"
        )

    top_10 = por_categoria.head(10)
    primera = por_categoria.row(0, named=True)

    print("\nCONCLUSION:")
    print(
        f"   {primera['categoria']} es la categoria que mas factura "
        f"({primera['importe']:.2f} EUR, "
        f"{primera['importe'] / total * 100:.2f}% del total)."
    )
    print(
        "   Las categorias con mas ventas no siempre son las que mas "
        "ingresos generan, porque el precio medio de cada producto influye."
    )

    grafico_barras_horizontales(
        etiquetas=top_10["categoria"].to_list(),
        valores=top_10["importe"].to_list(),
        titulo="Top 10 categorias por ingresos",
        xlabel="Ingresos (EUR)",
        archivo="ingresos_categoria.png",
        color="#4C72B0",
        formato="{:,.0f} EUR",
    )


def pregunta_2(completo):

    print("\n" + "=" * 60)
    print("PREGUNTA 2 - QUE METODO DE PAGO TIENE MAYOR TICKET MEDIO")
    print("=" * 60)
    print("\nQUE QUEREMOS AVERIGUAR:")
    print(
        "   Si los clientes que usan un metodo de pago concreto gastan "
        "mas por compra."
    )

    por_metodo = (
        completo.group_by("metodo_pago")
        .agg(
            pl.len().alias("ventas"),
            pl.col("cantidad").mean().round(2).alias("unidades_medias"),
            pl.col("importe").mean().round(2).alias("ticket_medio"),
            pl.col("importe").sum().round(2).alias("importe_total"),
        )
        .sort("ticket_medio", descending=True)
    )

    print("\nRESULTADO CON POLARS:")
    for fila in por_metodo.iter_rows(named=True):
        print(
            f"   {fila['metodo_pago']:<10} "
            f"ticket medio {fila['ticket_medio']:>7.2f} EUR | "
            f"{fila['unidades_medias']:.2f} unidades/venta | "
            f"{fila['ventas']} ventas"
        )

    primero = por_metodo.row(0, named=True)
    ultimo = por_metodo.row(-1, named=True)

    diferencia = (
        (primero["ticket_medio"] - ultimo["ticket_medio"])
        / ultimo["ticket_medio"]
        * 100
    )

    if diferencia < 5:
        valoracion = "muy pequena"
    elif diferencia < 15:
        valoracion = "moderada"
    else:
        valoracion = "notable"

    print("\nCONCLUSION:")
    print(
        f"   {primero['metodo_pago']} presenta el ticket medio mas alto "
        f"({primero['ticket_medio']:.2f} EUR), frente a "
        f"{ultimo['metodo_pago']} ({ultimo['ticket_medio']:.2f} EUR)."
    )
    print(
        f"   La diferencia entre ambos es de un {diferencia:.2f}%, "
        f"una diferencia {valoracion}; el metodo de pago no parece el "
        "factor principal del gasto."
    )

    fig, ax = plt.subplots(figsize=(9, 6))

    barras = ax.bar(
        por_metodo["metodo_pago"].to_list(),
        por_metodo["ticket_medio"].to_list(),
        color="#55A868",
    )

    for barra in barras:
        ax.annotate(
            f"{barra.get_height():.2f} EUR",
            xy=(barra.get_x() + barra.get_width() / 2, barra.get_height()),
            xytext=(0, 5),
            textcoords="offset points",
            ha="center",
        )

    ax.set_title("Ticket medio por metodo de pago", fontsize=15)
    ax.set_xlabel("Metodo de pago")
    ax.set_ylabel("Ticket medio (EUR)")
    ax.set_ylim(0, max(por_metodo["ticket_medio"].to_list()) * 1.2)

    guardar_figura(fig, "ej7_q2_ticket_medio_pago.png")
    plt.close(fig)


def pregunta_3(completo):

    print("\n" + "=" * 60)
    print("PREGUNTA 3 - COMPRAN MAS LOS SOCIOS QUE LOS NO SOCIOS")
    print("=" * 60)
    print("\nQUE QUEREMOS AVERIGUAR:")
    print(
        "   Si pertenecer al programa de socios se traduce en mas "
        "compras o mayor gasto por cliente."
    )

    por_socio = (
        completo.group_by("socio")
        .agg(
            pl.len().alias("ventas"),
            pl.col("id_cliente").n_unique().alias("clientes"),
            pl.col("cantidad").sum().alias("unidades"),
            pl.col("importe").sum().round(2).alias("importe"),
        )
        .with_columns(
            (pl.col("ventas") / pl.col("clientes"))
            .round(2)
            .alias("ventas_por_cliente"),
            (pl.col("importe") / pl.col("clientes"))
            .round(2)
            .alias("gasto_por_cliente"),
        )
        .sort("socio")
    )

    print("\nRESULTADO CON POLARS:")
    for fila in por_socio.iter_rows(named=True):
        etiqueta = "Socio" if fila["socio"] else "No socio"
        print(
            f"   {etiqueta:<9} -> {fila['clientes']} clientes | "
            f"{fila['ventas']} ventas | "
            f"{fila['ventas_por_cliente']:.2f} ventas/cliente | "
            f"{fila['gasto_por_cliente']:.2f} EUR/cliente"
        )

    socios = por_socio.filter(pl.col("socio")).row(0, named=True)
    no_socios = por_socio.filter(~pl.col("socio")).row(0, named=True)

    diferencia = (
        socios["ventas_por_cliente"] - no_socios["ventas_por_cliente"]
    )

    print("\nCONCLUSION:")
    if diferencia >= 0:
        print(
            f"   Los socios realizan {diferencia:.2f} ventas mas por "
            "cliente que los no socios."
        )
    else:
        print(
            f"   Los no socios realizan {abs(diferencia):.2f} ventas mas "
            "por cliente que los socios."
        )

    print(
        f"   Gasto medio por cliente: socios "
        f"{socios['gasto_por_cliente']:.2f} EUR frente a "
        f"{no_socios['gasto_por_cliente']:.2f} EUR de los no socios."
    )

    etiquetas = [
        "Socio" if valor else "No socio"
        for valor in por_socio["socio"].to_list()
    ]

    fig, ax = plt.subplots(figsize=(8, 6))

    barras = ax.bar(
        etiquetas,
        por_socio["ventas_por_cliente"].to_list(),
        color="#C44E52",
    )

    for barra, fila in zip(barras, por_socio.iter_rows(named=True)):
        ax.annotate(
            f"{fila['ventas_por_cliente']:.2f} ventas\n"
            f"{fila['gasto_por_cliente']:.2f} EUR",
            xy=(barra.get_x() + barra.get_width() / 2, barra.get_height()),
            xytext=(0, 5),
            textcoords="offset points",
            ha="center",
        )

    ax.set_title("Ventas por cliente segun condicion de socio", fontsize=15)
    ax.set_xlabel("Condicion")
    ax.set_ylabel("Ventas por cliente")
    ax.set_ylim(
        0,
        max(por_socio["ventas_por_cliente"].to_list()) * 1.25,
    )

    guardar_figura(fig, "socios.png")
    plt.close(fig)


def pregunta_4(completo):

    print("\n" + "=" * 60)
    print("PREGUNTA 4 - QUE FRANJA DE EDAD COMPRA MAS")
    print("=" * 60)
    print("\nQUE QUEREMOS AVERIGUAR:")
    print(
        "   Que decadas de edad concentran mas ventas y mayor gasto "
        "por cliente."
    )

    por_edad = (
        completo.with_columns(
            ((pl.col("edad") // 10) * 10).alias("decada")
        )
        .group_by("decada")
        .agg(
            pl.len().alias("ventas"),
            pl.col("id_cliente").n_unique().alias("clientes"),
            pl.col("cantidad").sum().alias("unidades"),
            pl.col("importe").sum().round(2).alias("importe"),
        )
        .with_columns(
            (pl.col("importe") / pl.col("clientes"))
            .round(2)
            .alias("gasto_por_cliente")
        )
        .sort("decada")
    )

    print("\nRESULTADO CON POLARS:")
    for fila in por_edad.iter_rows(named=True):
        print(
            f"   {fila['decada']:>2}-{fila['decada'] + 9} anos -> "
            f"{fila['ventas']:>4} ventas | "
            f"{fila['clientes']:>3} clientes | "
            f"{fila['gasto_por_cliente']:>7.2f} EUR/cliente"
        )

    ganadora = por_edad.sort("ventas", descending=True).row(0, named=True)
    mayor_gasto = por_edad.sort(
        "gasto_por_cliente", descending=True
    ).row(0, named=True)

    print("\nCONCLUSION:")
    print(
        f"   La decada de {ganadora['decada']}-"
        f"{ganadora['decada'] + 9} anos es la que mas ventas acumula "
        f"({ganadora['ventas']} ventas)."
    )
    print(
        f"   El mayor gasto medio por cliente se da en la decada de "
        f"{mayor_gasto['decada']}-{mayor_gasto['decada'] + 9} anos "
        f"({mayor_gasto['gasto_por_cliente']:.2f} EUR/cliente)."
    )

    etiquetas = [
        f"{fila['decada']}-{fila['decada'] + 9}"
        for fila in por_edad.iter_rows(named=True)
    ]
    ventas = por_edad["ventas"].to_list()

    fig, ax = plt.subplots(figsize=(10, 6))

    barras = ax.bar(etiquetas, ventas, color="#8172B2")

    for barra in barras:
        ax.annotate(
            str(int(barra.get_height())),
            xy=(barra.get_x() + barra.get_width() / 2, barra.get_height()),
            xytext=(0, 5),
            textcoords="offset points",
            ha="center",
        )

    ax.set_title("Ventas por decada de edad", fontsize=15)
    ax.set_xlabel("Decada de edad")
    ax.set_ylabel("Numero de ventas")
    ax.set_ylim(0, max(ventas) * 1.2)

    guardar_figura(fig, "edad.png")
    plt.close(fig)


def pregunta_5(completo):

    print("\n" + "=" * 60)
    print("PREGUNTA 5 - QUE PROVINCIAS GENERAN MAS INGRESOS")
    print("=" * 60)
    print("\nQUE QUEREMOS AVERIGUAR:")
    print(
        "   Que provincias concentran la facturacion y si ello se debe "
        "al volumen de clientes o al gasto medio."
    )

    por_provincia = (
        completo.group_by("provincia")
        .agg(
            pl.len().alias("ventas"),
            pl.col("id_cliente").n_unique().alias("clientes"),
            pl.col("importe").sum().round(2).alias("importe"),
        )
        .with_columns(
            (pl.col("importe") / pl.col("clientes"))
            .round(2)
            .alias("gasto_por_cliente")
        )
        .sort("importe", descending=True)
    )

    total = por_provincia["importe"].sum()

    print("\nRESULTADO CON POLARS (TOP 10):")
    for posicion, fila in enumerate(
        por_provincia.head(10).iter_rows(named=True), start=1
    ):
        print(
            f"   {posicion:>2}. {fila['provincia']:<14} "
            f"{fila['importe']:>9.2f} EUR "
            f"({fila['importe'] / total * 100:>5.2f}%) | "
            f"{fila['clientes']} clientes | "
            f"{fila['gasto_por_cliente']:.2f} EUR/cliente"
        )

    primera = por_provincia.row(0, named=True)

    print("\nCONCLUSION:")
    print(
        f"   {primera['provincia']} lidera la facturacion con "
        f"{primera['importe']:.2f} EUR "
        f"({primera['importe'] / total * 100:.2f}% del total) "
        f"y {primera['clientes']} clientes distintos."
    )
    print(
        "   Conviene comparar el gasto medio por cliente: una provincia "
        "puede facturar mucho solo por tener mas clientes activos."
    )

    top_10 = por_provincia.head(10)

    grafico_barras_horizontales(
        etiquetas=top_10["provincia"].to_list(),
        valores=top_10["importe"].to_list(),
        titulo="Top 10 provincias por ingresos",
        xlabel="Ingresos (EUR)",
        archivo="ej7_q5_provincias.png",
        color="#CCB974",
        formato="{:,.0f} EUR",
    )


def mostrar_resumen():

    print("\n" + "=" * 60)
    print("EJERCICIO 7 - CINCO PREGUNTAS ADICIONALES SOBRE LAS VENTAS")
    print("=" * 60)

    ventas, completo = cargar_datos()

    if ventas.height == 0:
        print("El archivo no contiene ventas todavia.")
        return

    pregunta_1(completo)
    pregunta_2(completo)
    pregunta_3(completo)
    pregunta_4(completo)
    pregunta_5(completo)

    print("\n" + "=" * 60)


if __name__ == "__main__":
    ejecutar_consumer(
        group_id="superdata-ej7",
        mostrar_resumen=mostrar_resumen,
        nombre_word="ej7_preguntas.docx",
        titulo_word="Ejercicio 7 - Cinco preguntas adicionales sobre las ventas",
    )
