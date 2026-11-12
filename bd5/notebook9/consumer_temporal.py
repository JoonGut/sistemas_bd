import matplotlib.pyplot as plt
import polars as pl

from consumer_comun import cargar_ventas, ejecutar_consumer, guardar_figura


def mostrar_resumen():

    print("\n" + "=" * 60)
    print("EJERCICIO 6 - ANALISIS TEMPORAL DE LAS VENTAS")
    print("=" * 60)

    df = cargar_ventas()

    if df.height == 0:
        print("El archivo no contiene ventas todavia.")
        return

    df = df.with_columns(
        pl.from_epoch(
            pl.col("timestamp").cast(pl.Int64),
            time_unit="s",
        ).alias("fecha_hora")
    ).with_columns(
        pl.col("fecha_hora").dt.hour().alias("hora"),
        pl.col("fecha_hora").dt.date().alias("fecha"),
        pl.col("fecha_hora").dt.truncate("1h").alias("intervalo"),
    )

    rango = df.select(
        pl.col("fecha_hora").min().alias("inicio"),
        pl.col("fecha_hora").max().alias("fin"),
    ).row(0, named=True)

    print("\nTIMESTAMP CONVERTIDO A FECHA Y HORA LEGIBLE:")
    print(f"   Primera venta: {rango['inicio']}")
    print(f"   Ultima venta:  {rango['fin']}")

    print("\nPRIMERAS FILAS CON LA FECHA CONVERTIDA:")
    print(
        df.select(
            "id_venta",
            "timestamp",
            "fecha_hora",
        ).head(5)
    )

    por_hora = (
        df.group_by("hora")
        .agg(pl.len().alias("ventas"))
        .sort("hora")
    )

    print("\nVENTAS POR HORA DEL DIA:")
    for fila in por_hora.iter_rows(named=True):
        print(f"   {fila['hora']:02d}:00 -> {fila['ventas']} ventas")

    hora_punta = por_hora.sort("ventas", descending=True).row(0, named=True)

    print(
        f"\n   Hora con mas ventas: {hora_punta['hora']:02d}:00 "
        f"({hora_punta['ventas']} ventas)"
    )

    por_dia = (
        df.group_by("fecha")
        .agg(pl.len().alias("ventas"))
        .sort("fecha")
    )

    print("\nVENTAS POR DIA:")
    for fila in por_dia.iter_rows(named=True):
        print(f"   {fila['fecha']} -> {fila['ventas']} ventas")

    por_intervalo = (
        df.group_by("intervalo")
        .agg(pl.len().alias("ventas"))
        .sort("ventas", descending=True)
    )

    print(
        f"\nINTERVALOS HORARIOS CON VENTAS: {por_intervalo.height}"
    )
    print("TOP 10 INTERVALOS CON MAS VENTAS:")

    for posicion, fila in enumerate(
        por_intervalo.head(10).iter_rows(named=True), start=1
    ):
        print(
            f"   {posicion:>2}. {fila['intervalo']} -> "
            f"{fila['ventas']} ventas"
        )

    horas = por_hora["hora"].to_list()
    ventas_hora = por_hora["ventas"].to_list()

    fechas = [str(f) for f in por_dia["fecha"].to_list()]
    ventas_dia = por_dia["ventas"].to_list()

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    barras = ax1.bar([f"{h:02d}" for h in horas], ventas_hora, color="#4C72B0")

    for barra in barras:
        ax1.annotate(
            str(int(barra.get_height())),
            xy=(barra.get_x() + barra.get_width() / 2, barra.get_height()),
            xytext=(0, 4),
            textcoords="offset points",
            ha="center",
        )

    ax1.set_title("Ventas por hora del dia")
    ax1.set_xlabel("Hora")
    ax1.set_ylabel("Numero de ventas")
    ax1.set_ylim(0, max(ventas_hora) * 1.2)

    ax2.bar(fechas, ventas_dia, color="#55A868")
    ax2.set_title("Ventas por dia")
    ax2.set_xlabel("Fecha")
    ax2.set_ylabel("Numero de ventas")
    ax2.set_ylim(0, max(ventas_dia) * 1.2)
    plt.setp(ax2.get_xticklabels(), rotation=45, ha="right")

    guardar_figura(fig, "analisis_temporal.png")
    plt.close(fig)

    print("\nCONCLUSION:")
    print(
        f"   La actividad se concentra en la franja de "
        f"{hora_punta['hora']:02d}:00-{hora_punta['hora']:02d}:59, "
        f"que acumula "
        f"{hora_punta['ventas'] / df.height * 100:.2f}% de las ventas."
    )

    print("\n" + "=" * 60)


if __name__ == "__main__":
    ejecutar_consumer(
        group_id="superdata-ej6",
        mostrar_resumen=mostrar_resumen,
        nombre_word="temporal.docx",
        titulo_word="Ejercicio 6 - Analisis temporal de las ventas",
    )
