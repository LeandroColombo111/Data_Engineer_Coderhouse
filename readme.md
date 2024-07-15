# Proyecto de ETL con Airflow y Redshift

Este proyecto implementa un flujo de trabajo ETL (Extract, Transform, Load) utilizando Apache Airflow para orquestar las tareas y Amazon Redshift como la base de datos de destino.

## Estructura del Proyecto


### Descripción de los Directorios y Archivos

- **dags/**: Contiene los archivos del DAG de Airflow.
  - `etl_dag.py`: Define el DAG para el flujo de trabajo ETL.
- **modules/**: Contiene los módulos de Python que realizan las operaciones de extracción, transformación, carga y alertas.
  - `extraction.py`: Define la función `extract_posts()` para extraer datos desde una API.
  - `transformation.py`: Define la función `transform_and_combine()` para transformar y combinar datos.
  - `loading.py`: Define las funciones `load_initial_data()` y `load_combined_data()` para cargar datos en Amazon Redshift.
  - `alerting.py`: Define la función `send_email()` para enviar alertas por correo electrónico.
- **requirements.txt**: Lista de dependencias necesarias para el proyecto.

## Requisitos

Asegúrate de tener instaladas las siguientes dependencias. Puedes instalarlas utilizando `pip`:

```bash
pip install -r requirements.txt



Configuración
Variables de Entorno
Crea un archivo .env en la raíz del proyecto con las siguientes variables de entorno:

REDSHIFT_USER=
REDSHIFT_PASSWORD=
REDSHIFT_ENDPOINT=
REDSHIFT_PORT=
REDSHIFT_DB=
REDSHIFT_TABLE=
ALERT_EMAIL=your_email@gmail.com
RECEIVER_EMAIL=receiver_email@gmail.com
EMAIL_PASSWORD=your_email_password


Este `README.md` proporciona una guía completa sobre la configuración y ejecución del proyecto ETL con Airflow y Redshift, incluyendo la estructura del proyecto, los requisitos, y el código del DAG de Airflow(dentro de la carpeta /dags).