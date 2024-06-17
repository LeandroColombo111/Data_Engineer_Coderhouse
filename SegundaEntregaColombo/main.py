import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import logging
from Data_Engineer_Coderhouse.SegundaEntrega.log_config import setup_logging

# Configurar el logging
setup_logging()

# Cargar variables de entorno desde el archivo .env
load_dotenv()

REDSHIFT_USER = os.getenv('REDSHIFT_USER')
REDSHIFT_PASSWORD = os.getenv('REDSHIFT_PASSWORD')
REDSHIFT_HOST = os.getenv('REDSHIFT_HOST')
REDSHIFT_PORT = os.getenv('REDSHIFT_PORT')
REDSHIFT_DB = os.getenv('REDSHIFT_DB')
REDSHIFT_TABLE = os.getenv('REDSHIFT_TABLE')

# Verificar que las variables de entorno se han cargado correctamente
required_env_vars = {
    "REDSHIFT_USER": REDSHIFT_USER,
    "REDSHIFT_PASSWORD": REDSHIFT_PASSWORD,
    "REDSHIFT_HOST": REDSHIFT_HOST,
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
        engine = create_engine(connection_string)
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logging.info("Datos cargados exitosamente en Redshift")
    except Exception as e:
        logging.error(f"Error al cargar datos en Redshift: {e}")

def main():
    api_url = 'https://jsonplaceholder.typicode.com/posts' 
    data = extract_data(api_url)
    if data:
        df = transform_data(data)
        connection_string = f'postgresql+psycopg2://{REDSHIFT_USER}:{REDSHIFT_PASSWORD}@{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DB}'
        table_name = REDSHIFT_TABLE
        load_data(df, table_name, connection_string)

if __name__ == "__main__":
    main()
