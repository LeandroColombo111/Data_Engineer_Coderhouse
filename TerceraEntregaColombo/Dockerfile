# Utilizar la imagen base oficial de Apache Airflow
FROM apache/airflow:2.1.0

# Establecer el usuario root para instalar dependencias del sistema
USER root

# Instalar dependencias del sistema
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
       default-libmysqlclient-dev \
       libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Establecer el usuario airflow para el resto del contenedor
USER airflow

# Establecer el directorio de trabajo
WORKDIR /opt/airflow

# Copiar los archivos del proyecto al directorio de trabajo
COPY . /opt/airflow

# Instalar las dependencias de Python
RUN pip install --no-cache-dir --user -r /opt/airflow/requirements.txt

# Exponer el puerto 8080 para acceder a la interfaz web de Airflow
EXPOSE 8080

# Establecer el comando para iniciar el servidor web de Airflow
ENTRYPOINT ["airflow", "webserver"]
