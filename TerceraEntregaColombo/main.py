import requests
import pandas as pd
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os
import logging
from log_config import setup_logging

# Configurar el logging
setup_logging()

# Cargar variables de entorno desde el archivo .env
load_dotenv()

REDSHIFT_USER = os.getenv('REDSHIFT_USER')
REDSHIFT_PASSWORD = os.getenv('REDSHIFT_PASSWORD')
REDSHIFT_ENDPOINT = os.getenv('REDSHIFT_ENDPOINT')
REDSHIFT_PORT = os.getenv('REDSHIFT_PORT')
REDSHIFT_DB = os.getenv('REDSHIFT_DB')
REDSHIFT_TABLE = os.getenv('REDSHIFT_TABLE')

# Verificar que las variables de entorno se han cargado correctamente
required_env_vars = {
    "REDSHIFT_USER": REDSHIFT_USER,
    "REDSHIFT_PASSWORD": REDSHIFT_PASSWORD,
    "REDSHIFT_ENDPOINT": REDSHIFT_ENDPOINT,
    "REDSHIFT_PORT": REDSHIFT_PORT,
    "REDSHIFT_DB": REDSHIFT_DB,
    "REDSHIFT_TABLE": REDSHIFT_TABLE,
}

for var, value in required_env_vars.items():
    if not value:
        logging.error(f"La variable de entorno {var} no está definida o está vacía.")
        raise ValueError(f"La variable de entorno {var} no está definida o está vacía.")

logging.info("Iniciando la extracción de datos")
def extract_data(api_url):
    try:
        response = requests.get(api_url)
        if response.status_code == 200:
            data = response.json()
            logging.info("Datos extraídos correctamente desde la API")
            return data
        else:
            logging.error(f"Error fetching data from API: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
        return None

# Función para aplanar y transformar los datos
logging.info("Iniciando la transformación de datos")
def transform_data(data):
    df = pd.json_normalize(data)
    df.drop_duplicates(inplace=True)
    df.dropna(inplace=True)
    logging.info("Datos transformados correctamente")
    return df

# Función para cargar los datos en Redshift
logging.info("Iniciando la carga de datos a Redshift")
def load_data(df, table_name, connection_string):
    try:
        conn = psycopg2.connect(connection_string)
        cursor = conn.cursor()

        create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {table} (
                userId INTEGER,
                id INTEGER,
                title VARCHAR,
                body VARCHAR,
                PRIMARY KEY (userId, id)
            )
        """).format(table=sql.Identifier(table_name))
        
        cursor.execute(create_table_query)
        conn.commit()
        logging.info(f"Tabla {table_name} creada exitosamente en Redshift")
        
        # Convertir DataFrame a lista de tuplas
        data = [tuple(row) for row in df.to_numpy()]
       
        insert_data_query = sql.SQL("""
            INSERT INTO {table} (userId, id, title, body)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (userId, id) DO UPDATE
            SET title = EXCLUDED.title,
                body = EXCLUDED.body
        """).format(table=sql.Identifier(table_name))

        psycopg2.extras.execute_values(cursor, insert_data_query, data)
        conn.commit()
        logging.info(f"Datos cargados exitosamente en la tabla {table_name} de Redshift")

    except Exception as e:
        logging.error(f"Error al cargar datos en Redshift: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def main():
    api_url = 'https://jsonplaceholder.typicode.com/posts' 
    data = extract_data(api_url)
    if data:
        df = transform_data(data)
        logging.info(df) # Queda para corroborar que los datos se transformaron correctamente
        if df is not None and not df.empty:
            connection_string = f"dbname='{REDSHIFT_DB}' user='{REDSHIFT_USER}' password='{REDSHIFT_PASSWORD}' host='{REDSHIFT_ENDPOINT}' port='{REDSHIFT_PORT}'"
            table_name = REDSHIFT_TABLE
            load_data(df, table_name, connection_string)

if __name__ == "__main__":
    main()
