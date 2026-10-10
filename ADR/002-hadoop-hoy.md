# ADR 002 — Hadoop en el diseño de NexoUrbano

## Contexto

El TP requiere practicar un stack on-prem heredado que sigue apareciendo en
plataformas de datos existentes. El entorno local usa la imagen oficial
`apache/hadoop:3.4.1` con un NameNode, un DataNode, un ResourceManager y un
NodeManager.

## Decisión

Se conserva Hadoop para demostrar almacenamiento distribuido compatible con
HDFS y ejecutar procesamiento batch MapReduce sobre datos operativos. También
permite integrar y consultar formatos y herramientas heredadas del ecosistema,
como Hive y HBase, relevantes al mantener plataformas ya instaladas.

El Compose local es de un solo nodo, con replicación 1: sirve para aprendizaje
y reproducibilidad del TP, no como diseño de alta disponibilidad.

## Qué no elegiríamos para un sistema nuevo

No elegiríamos MapReduce para analítica interactiva, procesamiento de baja
latencia, streaming ni transformaciones iterativas. Para esos casos preferimos
servicios administrados, motores SQL interactivos o Spark/Structured
Streaming, según el requisito. En este TP mantenemos WordCount porque permite
mostrar el modelo batch y la ejecución distribuida de forma verificable.
