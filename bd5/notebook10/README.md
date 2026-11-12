# Notebook 10 — Particiones y Consumer Groups en Kafka

## Objetivo

En el Notebook 8 vimos Producer → Kafka → Consumer y en el Notebook 9 utilizamos
Kafka para alimentar `ventas.csv`.

En este notebook cerramos la parte de Kafka trabajando con:

- Topics
- Particiones
- Consumer Groups
- Paralelismo
- Rebalanceo
- Escalabilidad

No se profundiza en Kafka Streams, Schema Registry, exactly-once ni administración
avanzada del clúster.

## 1. Instalar dependencias

Usa el mismo entorno virtual de los notebooks anteriores:

```bash
python -m pip install -r requirements.txt
```

En Jupyter puedes comprobar el intérprete:

```python
import sys
print(sys.executable)
```

## 2. Arrancar Kafka

```bash
docker compose up -d
```

Comprobar:

```bash
docker ps
```

## 3. Crear el topic

```bash
python crear_topic.py
```

Se crea `ventas_demo` con 3 particiones.

Usamos un topic separado para no modificar el `ventas` del Notebook 9.

## 4. Producer

En una terminal:

```bash
python producer_particiones.py
```

El producer genera ventas continuamente y muestra la partición y el offset
asignados.

## 5. Un Consumer

En otra terminal:

```bash
python consumer_particiones.py
```

Observa las particiones asignadas.

## 6. Varios Consumers

Abre una segunda y una tercera terminal y ejecuta:

```bash
python consumer_particiones.py
```

Los tres consumidores utilizan el mismo:

```text
group.id = grupo_superdata
```

Kafka repartirá las 3 particiones entre ellos.

## 7. Más consumidores que particiones

Ejecuta un cuarto consumidor.

Tenemos:

```text
3 particiones
4 consumidores
```

Uno de ellos no tendrá ninguna partición asignada.

Pregunta:

> ¿Por qué aumentar consumidores por encima del número de particiones no aumenta
> el paralelismo?

## 8. Rebalanceo

Con varios consumidores funcionando, detén uno con `Ctrl+C`.

Observa cómo cambian las particiones asignadas a los consumidores restantes.

Explica qué es un rebalanceo.

## 9. Consumer Groups diferentes

Modifica temporalmente `GROUP_ID` en una copia del consumer:

```python
GROUP_ID = "grupo_analisis"
```

Mientras otro consumidor utiliza:

```python
GROUP_ID = "grupo_superdata"
```

Observa que ambos grupos pueden recibir los mismos eventos.

Pregunta:

> ¿Por qué es útil que diferentes aplicaciones puedan consumir el mismo topic?

## 10. Actividad final

Responde:

1. ¿Qué es una partición?
2. ¿Qué es un Consumer Group?
3. ¿Qué es un offset?
4. ¿Qué es un rebalanceo?
5. ¿Por qué las particiones permiten paralelismo?
6. ¿Qué ocurre si hay más consumidores que particiones?
7. ¿Cómo podríamos escalar el procesamiento si aumenta mucho el número de ventas?

## 11. Cierre de Kafka

Con esta práctica terminamos la parte de Kafka del módulo.

Los conceptos que deben dominar son:

```text
Producer
   ↓
Topic
   ↓
Particiones
   ↓
Consumer Group
   ↓
Consumidores
```

No es necesario profundizar en administración avanzada de Kafka.

## 12. Detener Kafka

```bash
docker compose down
```
