import requests
import sqlite3
import pandas as pd
from dotenv import load_dotenv
import os
from modules.extraction import extract_posts
from modules.loading import load_initial_data, load_combined_data
from modules.transformation import transform_and_combine
from modules.alerting import send_email
from modules.log_config import setup_logging

# Configurar logging
setup_logging()

def main():
    # Extraer datos
    posts = extract_posts()

    # Simulación de datos de usuarios descargados de Kaggle
    data_frame = 'modules/data_frame.xlsx'
    users_df = pd.read_excel(data_frame)

    # Cargar datos iniciales
    load_initial_data(posts, users_df)

    # Transformar y combinar datos
    combined_data = transform_and_combine()

    # Cargar datos combinados
    load_combined_data(combined_data)

    # Mostrar los datos combinados
    for row in combined_data:
        print(row)

    # Enviar alerta por correo electrónico
    send_email("ETL Process Completed", "The ETL process has completed successfully.")

if __name__ == "__main__":
    main()
