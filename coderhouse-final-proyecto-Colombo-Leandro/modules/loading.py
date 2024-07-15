import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os

# Cargar las variables de entorno
load_dotenv()

def load_initial_data(posts, users_df):
    conn = psycopg2.connect(
        host=os.getenv("REDSHIFT_ENDPOINT"),
        port=os.getenv("REDSHIFT_PORT"),
        dbname=os.getenv("REDSHIFT_DB"),
        user=os.getenv("REDSHIFT_USER"),
        password=os.getenv("REDSHIFT_PASSWORD")
    )
    cursor = conn.cursor()

    # Crear tabla posts
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS posts (
        userId INTEGER,
        id INTEGER PRIMARY KEY,
        title TEXT,
        body TEXT
    )
    ''')

    # Insertar datos de posts
    for post in posts:
        cursor.execute('''
        INSERT INTO posts (userId, id, title, body)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING
        ''', (post['userId'], post['id'], post['title'], post['body']))

    # Crear tabla users
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        name TEXT,
        surname TEXT,
        email TEXT,
        address TEXT,
        phone TEXT,
        company TEXT
    )
    ''')

    # Insertar datos de usuarios
    for index, row in users_df.iterrows():
        cursor.execute('''
        INSERT INTO users (id, name, surname, email, address, phone, company)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING
        ''', (row['id'], row['name'], row['surname'], row['email'], row['address'], row['phone'], row['company']))

    conn.commit()
    cursor.close()
    conn.close()

def load_combined_data(combined_data):
    conn = psycopg2.connect(
        host=os.getenv("REDSHIFT_ENDPOINT"),
        port=os.getenv("REDSHIFT_PORT"),
        dbname=os.getenv("REDSHIFT_DB"),
        user=os.getenv("REDSHIFT_USER"),
        password=os.getenv("REDSHIFT_PASSWORD")
    )
    cursor = conn.cursor()

    # Crear tabla combined_data
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS combined_data (
        post_id INTEGER PRIMARY KEY,
        title TEXT,
        body TEXT,
        name TEXT,
        surname TEXT,
        email TEXT
    )
    ''')

    # Insertar datos combinados
    for row in combined_data:
        cursor.execute('''
        INSERT INTO combined_data (post_id, title, body, name, surname, email)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (post_id) DO NOTHING
        ''', row)

    conn.commit()
    cursor.close()
    conn.close()
