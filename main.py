import requests
import json
import psycopg2
from sqlalchemy import create_engine
import schedule
import time
from dotenv import load_dotenv
import os
import logging

# Cargar variables del archivo .env
load_dotenv()

redshift_endpoint = os.getenv("REDSHIFT_ENDPOINT")
redshift_db = os.getenv("REDSHIFT_DB")
redshift_user = os.getenv("REDSHIFT_USER")
redshift_password = os.getenv("REDSHIFT_PASSWORD")
redshift_port = os.getenv("REDSHIFT_PORT")

# Conexión a Redshift
engine = create_engine(f'redshift+redshift_connector://{redshift_user}:{redshift_password}@{redshift_endpoint}:{redshift_port}/{redshift_db}')

# Crear tabla en Redshift
create_table_query = """
CREATE TABLE IF NOT EXISTS crypto_data (
    id INT IDENTITY(1,1) PRIMARY KEY,
    cryptocurrency VARCHAR(50),
    price_usd FLOAT,
    volume_24h FLOAT,
    market_cap FLOAT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

with engine.connect() as conn:
    conn.execute(create_table_query)

logging.info("Loading Data")
def fetch_and_load_data():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 10,
        "page": 1,
        "sparkline": "false"
    }

    response = requests.get(url, params=params)
    data = response.json()

    for coin in data:
        cryptocurrency = coin['name']
        price_usd = coin['current_price']
        volume_24h = coin['total_volume']
        market_cap = coin['market_cap']

        # Insertar datos en la tabla de Redshift
        insert_query = f"""
        INSERT INTO crypto_data (cryptocurrency, price_usd, volume_24h, market_cap)
        VALUES ('{cryptocurrency}', {price_usd}, {volume_24h}, {market_cap});
        """

        with engine.connect() as conn:
            conn.execute(insert_query)

    print("Datos insertados correctamente en Redshift")

#Comentado para ejecucion unica
#schedule.every().day.at("00:00").do(fetch_and_load_data)

#while True:
#    schedule.run_pending()
#    time.sleep(1)

fetch_and_load_data()