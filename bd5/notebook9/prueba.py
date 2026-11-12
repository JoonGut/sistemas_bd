def mostrar_resumen():

    print("\n" + "=" * 55)
    print("       RESUMEN DEL COMPORTAMIENTO DE CLIENTES")
    print("=" * 55)

    if not contador_clientes:
        print("No se han recibido ventas.")
        return

    # 1. Número de clientes diferentes
    total_clientes = len(contador_clientes)

    print("\n1. NUMERO DE CLIENTES DIFERENTES:")
    print(f"Total de clientes que han realizado compras: {total_clientes}")

    # 2. Clientes que aparecen más veces
    max_compras = max(contador_clientes.values())

    clientes_mas_compras = [
        (cliente, compras)
        for cliente, compras in contador_clientes.items()
        if compras == max_compras
    ]

    print("\n2. CLIENTES QUE APARECEN MAS VECES:")
    print(f"Máximo número de compras: {max_compras}")

    for cliente, compras in clientes_mas_compras:
        print(f"Cliente {cliente}: {compras} compras")

    # 3. Top 10 clientes con más compras
    top_10 = contador_clientes.most_common(10)

    print("\n3. TOP 10 CLIENTES CON MAS COMPRAS:")

    for posicion, (cliente, compras) in enumerate(top_10, start=1):
        print(f"{posicion}. Cliente {cliente}: {compras} compras")

    # 4. Clientes que solamente han comprado una vez
    clientes_una_compra = sum(
        1 for compras in contador_clientes.values()
        if compras == 1
    )

    print("\n4. CLIENTES CON UNA SOLA COMPRA:")
    print(f"Total: {clientes_una_compra}")

    # 5. Gráfico de compras por cliente (Top 10)
    clientes = [cliente for cliente, compras in top_10]
    cantidades = [compras for cliente, compras in top_10]

    fig, ax = plt.subplots(figsize=(12, 6))

    barras = ax.bar(clientes, cantidades)

    for barra in barras:
        altura = barra.get_height()

        ax.annotate(
            str(int(altura)),
            xy=(barra.get_x() + barra.get_width() / 2, altura),
            xytext=(0, 5),
            textcoords="offset points",
            ha="center"
        )

    ax.set_title("TOP 10 CLIENTES CON MAS COMPRAS")
    ax.set_xlabel("ID del cliente")
    ax.set_ylabel("Número de compras")

    ax.set_ylim(0, max(cantidades) * 1.2)

    plt.xticks(rotation=45)
    plt.tight_layout()

    Path("data/imagenes").mkdir(parents=True, exist_ok=True)

    plt.savefig(
        "data/imagenes/top_10_clientes.png",
        dpi=300,
        bbox_inches="tight"
    )

    print("\nGráfico guardado en data/imagenes/top_10_clientes.png")

    plt.close()

    print("\n" + "=" * 55)
