# Usar una imagen base ligera de Python
FROM python:3.9-slim

# Instalar dependencias del sistema necesarias
RUN apt-get update && apt-get install -y \
    build-essential \
    default-libmysqlclient-dev \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Establecer el directorio de trabajo
WORKDIR /app

# Copiar los archivos necesarios al contenedor
COPY main.py ./
COPY requirements.txt ./
COPY log_config.py ./
COPY .env ./
COPY airflow ./

# Instalar las dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Instalar Apache Airflow
RUN pip install apache-airflow

# Comando por defecto para ejecutar el script
CMD ["python", "main.py"]
