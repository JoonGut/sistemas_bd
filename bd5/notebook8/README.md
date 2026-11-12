# Notebook 8 - Apache Kafka

## Instalación

Levantar Kafka

```bash
docker compose up -d
```

---

Instalar dependencias

```bash
pip install -r requirements.txt
```

---

## Abrir dos terminales

Terminal 1

```bash
python consumer.py
```

Terminal 2

```bash
python producer.py
```

---

Cada dos segundos se generará una nueva venta del supermercado.

El Producer enviará la venta mediante Kafka y el Consumer la recibirá inmediatamente.

------------------------
## Ejercicio 1

Añadir al mensaje los campos:

descuento
precio_unitario

# Ejercicio 2

Mostrar el importe total de la compra.

# Ejercicio 3

Modificar el Producer para que envíe una venta cada segundo.

# Ejercicio 4

Añadir un nuevo método de pago.