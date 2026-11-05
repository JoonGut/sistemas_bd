# Práctica: Orquestación de un Pipeline ETL con Prefect

# 1. Levantar el servidor de Prefect

Desde la carpeta del proyecto ejecutar:

```bash
docker compose up -d
```

Si todo ha ido correctamente aparecerá un contenedor llamado:

```
prefect_server
```

Puedes comprobarlo con:

```bash
docker ps
```

---

# 2. Abrir la interfaz web

Abrir el navegador en:

```
http://127.0.0.1:4200
```

Desde esta interfaz podremos visualizar la ejecución de nuestros pipelines.

---

# 3. Configurar Prefect

La primera vez debemos indicar dónde se encuentra la API del servidor.

Ejecutar en una terminal:

```bash
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
```

Comprobar la configuración:

```bash
prefect config view
```

Debe aparecer algo similar a:

```
PREFECT_API_URL='http://127.0.0.1:4200/api'
```

---

# 4. Ejecutar el notebook

Abrir:

```
notebook_7_prefect.ipynb
```

y ejecutar todas las celdas.

Al lanzar:

```python
pipeline_clientes()
```

el flujo aparecerá automáticamente en la interfaz web de Prefect.

---

# Finalizar el servidor

Cuando termines la práctica puedes detener el servidor ejecutando:

```bash
docker compose down
```

Si deseas eliminar también la base de datos creada por Prefect:

```bash
docker compose down -v
```

---
