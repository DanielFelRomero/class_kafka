# .devcontainer/Dockerfile
FROM mcr.microsoft.com/devcontainers/python:3.11

# 1. Instalar Java (JRE) que es estrictamente necesario para correr Kafka
RUN apt-get update && apt-get install -y default-jre && apt-get clean

# 2. Copiar el archivo de requerimientos al contenedor temporalmente
COPY requirements.txt /tmp/pip-tmp/

# 3. Actualizar pip e instalar dependencias (Esto quedará guardado en la caché de Docker)
RUN pip install --upgrade pip \
    && pip install -r /tmp/pip-tmp/requirements.txt \
    && rm -rf /tmp/pip-tmp
