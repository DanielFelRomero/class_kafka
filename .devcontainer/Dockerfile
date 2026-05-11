# .devcontainer/Dockerfile
FROM mcr.microsoft.com/devcontainers/python:3.11

# Copiamos el archivo de requerimientos al contenedor temporalmente
COPY requirements.txt /tmp/pip-tmp/

# Actualizamos pip e instalamos las dependencias (esto se quedará en caché)
RUN pip install --upgrade pip \
    && pip install -r /tmp/pip-tmp/requirements.txt \
    && rm -rf /tmp/pip-tmp
