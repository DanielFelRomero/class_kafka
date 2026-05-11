# 🚀 Pipeline de Machine Learning Orientado a Eventos: Batch vs Streaming

Bienvenido al repositorio práctico del laboratorio de Arquitectura de Datos y Machine Learning. En este proyecto implementamos un flujo de datos completo utilizando **Apache Airflow**, **Apache Kafka** y **Streamlit** para simular y comparar el rendimiento de modelos predictivos en escenarios de procesamiento por lotes (Batch) y en tiempo real (Streaming).

## 🎯 Objetivo del Laboratorio
El objetivo es predecir el riesgo de abandono (Churn) de clientes de una empresa de telecomunicaciones. Para ello, orquestamos un pipeline que:
1. Genera un dataset sintético y entrena dos modelos de Machine Learning.
2. **Regresión Lineal:** Evaluada en formato **Batch** (procesando todo el lote de datos a la vez).
3. **Árbol de Decisión (Decision Tree):** Evaluado en formato **Streaming** (procesando un evento a la vez, simulando tiempo real).
4. Visualiza la ingesta de predicciones en un Dashboard interactivo en vivo.

## 🛠️ Tecnologías Utilizadas
* **Orquestación:** Apache Airflow (Standalone)
* **Message Broker:** Apache Kafka (Modo KRaft, sin Zookeeper)
* **Machine Learning:** Scikit-Learn, Pandas, NumPy
* **Visualización:** Streamlit
* **Entorno:** GitHub Codespaces (Preconfigurado con Devcontainers y Java)

## 📁 Estructura del Proyecto
```text
📦 class_kafka
 ┣ 📂 .devcontainer        # Configuración del entorno (Dockerfile e instalación de Java/Dependencias)
 ┣ 📂 consumers            # Consumidores de Kafka
 ┃ ┣ 📜 consumer_lr.py     # Consume el modelo Batch y evalúa todo el dataset
 ┃ ┗ 📜 consumer_dt.py     # Consume el modelo Streaming evento por evento
 ┣ 📂 dags
 ┃ ┗ 📜 churn_ml_kafka.py  # DAG de Airflow que genera datos, entrena y produce a Kafka
 ┣ 📜 app.py               # Dashboard en tiempo real de Streamlit
 ┣ 📜 requirements.txt     # Dependencias de Python
 ┗ 📜 setup_kafka.sh       # Script para descargar e inicializar Kafka

```

---

## 🚀 Guía de Ejecución Rápida

Este repositorio está diseñado para ejecutarse de forma nativa y sin fricciones utilizando **GitHub Codespaces**.

### Fase 1: Preparación del Entorno

1. Haz clic en el botón verde **`<> Code`** -> Pestaña **`Codespaces`** -> **`Create codespace on main`**.
2. Espera a que el contenedor se construya. Gracias a la configuración en `.devcontainer`, todas las dependencias (Airflow, Kafka, Java y Python) se instalarán automáticamente.

### Fase 2: Levantar la Infraestructura (4 Terminales)

Para ejecutar este laboratorio, abre **4 terminales divididas** en Visual Studio Code y ejecuta los siguientes comandos en cada una:

#### 🖥️ Terminal 1: Core (Kafka y Airflow)

Primero limpiamos logs anteriores y levantamos el broker de Kafka. Una vez encendido, arrancamos el motor de Airflow:

```bash
rm -rf /tmp/kraft-combined-logs
chmod +x setup_kafka.sh
./setup_kafka.sh

export AIRFLOW__WEBSERVER__ENABLE_PROXY_FIX=True
python -m airflow standalone

```

*(Nota: Guarda la contraseña de `admin` que imprime Airflow en consola).*

#### 🖥️ Terminal 2: Dashboard (Streamlit)

Lanza el monitor visual interactivo:

```bash
streamlit run app.py

```

*(Haz clic en "Open in Browser" cuando VS Code te notifique que el puerto 8501 está abierto).*

#### 🖥️ Terminal 3: Consumidores (Kafka Consumers)

Pon a escuchar a los dos scripts que evaluarán los modelos. Uno se ejecuta en segundo plano (`&`) y el otro en primer plano:

```bash
python consumers/consumer_lr.py &
python consumers/consumer_dt.py

```

*(Verás el mensaje: "Escuchando modelo DT y eventos de streaming...").*

#### 🖥️ Terminal 4: Disparar el Pipeline (Trigger)

Usa la interfaz de línea de comandos de Airflow para quitar la pausa al DAG y ejecutarlo sin necesidad de entrar a la UI web:

```bash
airflow dags unpause churn_ml_kafka_pipeline
airflow dags trigger churn_ml_kafka_pipeline

```

---

## 📊 ¿Qué observar durante la ejecución?

Una vez ejecutado el trigger en la Terminal 4:

1. Mira la **Terminal 3**. Verás cómo el consumidor del Árbol de Decisión comienza a imprimir logs de predicción 1 a 1, simulando un flujo en tiempo real (un evento por segundo).
2. Abre la **pestaña de Streamlit** en tu navegador.
* La columna izquierda (Batch) se llenará de golpe mostrando los 200 registros predecidos por la Regresión Lineal.
* La columna derecha (Streaming) actuará como un monitor en vivo, actualizando la gráfica punto por punto a medida que Kafka entrega los eventos del Árbol de Decisión.



---

⚠️ **Importante:** Recuerda detener tu Codespace desde [github.com/codespaces](https://github.com/codespaces) una vez termines tu práctica para no consumir tu cuota gratuita.

```

```

## 🕵️‍♂️ Auditoría y Monitoreo de Kafka (CLI)

Puedes abrir una quinta terminal para ejecutar comandos de diagnóstico mientras el pipeline está corriendo:

* **1. Ver si el servidor (Broker) está vivo y listar tópicos:**
  ```bash
  ./kafka_2.13-3.7.0/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
  ```
2. Monitorear el "Lag" (Retraso) del Consumidor de Streaming:
Este comando muestra cuántos mensajes faltan por leer. Si el "LAG" es 0, el consumidor está al día.

 ```bash
   ./kafka_2.13-3.7.0/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group group_dt_stream
  ```
3. Espiar el tráfico en vivo (Sniffing):
Actúa como un consumidor fantasma leyendo todo lo que pasa por el tópico.

 ```bash
  ./kafka_2.13-3.7.0/bin/kafka-console-consumer.sh --topic topic_test_stream --bootstrap-server localhost:9092
 ```
