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
    users_df = pd.DataFrame({
        'name': ['John', 'Jane', 'Doe'],
        'surname': ['Doe', 'Smith', 'Roe'],
        'email': ['john.doe@example.com', 'jane.smith@example.com', 'doe.roe@example.com'],
        'address': ['123 Main St', '456 Maple Ave', '789 Oak Dr'],
        'phone': ['555-1234', '555-5678', '555-8765'],
        'company': ['Company A', 'Company B', 'Company C'],
        'id': [1, 2, 3]
    })

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
