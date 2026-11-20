import polars as pl
import psycopg


# Configuración

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "superdata",
    "user": "postgres",
    "password": "postgres"
}


# Conexións 

def conectar():
    """
    Crea y devuelve una conexión con PostgreSQL.
    """

    return psycopg.connect(**DB_CONFIG)


# Crear tablas

def polars_to_postgres_type(dtype):

    if dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64):
        return "INTEGER"

    elif dtype in (pl.Float32, pl.Float64):
        return "DOUBLE PRECISION"

    elif dtype == pl.Boolean:
        return "BOOLEAN"

    elif dtype == pl.Date:
        return "DATE"

    elif dtype == pl.Datetime:
        return "TIMESTAMP"

    else:
        return "TEXT"


def crear_tabla_desde_polars(
    conn,
    df,
    nombre_tabla,
    clave_primaria=None
):

    columnas = []

    for nombre, tipo in df.schema.items():

        tipo_postgres = polars_to_postgres_type(tipo)

        columna = f'"{nombre}" {tipo_postgres}'

        if nombre == clave_primaria:
            columna += " PRIMARY KEY"

        columnas.append(columna)

    sql = f"""
    CREATE TABLE IF NOT EXISTS "{nombre_tabla}" (
        {", ".join(columnas)}
    );
    """

    with conn.cursor() as cursor:
        cursor.execute(sql)

    conn.commit()


# Insertar datos

def insertar_dataframe(
    conn,
    df,
    nombre_tabla
):

    columnas = df.columns

    placeholders = ", ".join(
        ["%s"] * len(columnas)
    )

    sql = f"""
    INSERT INTO "{nombre_tabla}"
    ({", ".join(columnas)})
    VALUES ({placeholders})
    """

    with conn.cursor() as cursor:

        cursor.executemany(
            sql,
            df.rows()
        )

    conn.commit()


def insertar_dataframe_sin_duplicados(
    conn,
    df,
    nombre_tabla,
    columna_clave
):

    columnas = df.columns

    columnas_sql = ", ".join(
        f'"{columna}"'
        for columna in columnas
    )

    placeholders = ", ".join(
        ["%s"] * len(columnas)
    )

    sql = f"""
    INSERT INTO "{nombre_tabla}"
    ({columnas_sql})
    VALUES ({placeholders})
    ON CONFLICT ("{columna_clave}") DO NOTHING
    """

    with conn.cursor() as cursor:

        cursor.executemany(
            sql,
            df.rows()
        )

        insertadas = cursor.rowcount

    conn.commit()

    return insertadas


# Cargar datos

def cargar_datos():

    clientes = pl.read_csv(
        "../../data/clientes.csv"
    )

    productos = pl.read_csv(
        "../../data/productos.csv"
    )

    ventas = pl.read_csv(
        "../../data/ventas.csv"
    )

    return clientes, productos, ventas


# Preparar la base de datos

def cargar_datos_en_postgresql():

    conn = conectar()

    clientes, productos, ventas = cargar_datos()

    crear_tabla_desde_polars(
        conn,
        clientes,
        "clientes",
        "id_cliente"
    )

    crear_tabla_desde_polars(
        conn,
        productos,
        "productos",
        "id_producto"
    )

    crear_tabla_desde_polars(
        conn,
        ventas,
        "ventas",
        "id_venta"
    )

    insertar_dataframe(
        conn,
        clientes,
        "clientes"
    )

    insertar_dataframe(
        conn,
        productos,
        "productos"
    )

    insertar_dataframe(
        conn,
        ventas,
        "ventas"
    )

    return conn


# Ejecutar consulta

def ejecutar_consulta(
    conn,
    sql,
    parametros=None
):

    with conn.cursor() as cursor:

        cursor.execute(
            sql,
            parametros
        )

        return cursor.fetchall()


# Cerrar la conexión

def cerrar_conexion(conn):

    conn.close()

# Para integración con kafka necesitamos esta función

def insertar_venta(conn, venta):

    sql = """
    INSERT INTO ventas (
        id_venta,
        fecha,
        id_cliente,
        id_producto,
        cantidad,
        metodo_pago,
        descuento
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    with conn.cursor() as cursor:

        cursor.execute(
            sql,
            (
                venta["id_venta"],
                venta["fecha"],
                venta["id_cliente"],
                venta["id_producto"],
                venta["cantidad"],
                venta["metodo_pago"],
                venta["descuento"]
            )
        )

    conn.commit()