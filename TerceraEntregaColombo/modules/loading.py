import sqlite3
import pandas as pd

def load_initial_data(posts, users_df):
    conn = sqlite3.connect('external_users.db')
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
        INSERT OR REPLACE INTO posts (userId, id, title, body)
        VALUES (?, ?, ?, ?)
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
    users_df.to_sql('users', conn, if_exists='replace', index=False)

    conn.commit()
    conn.close()

def load_combined_data(combined_data):
    conn = sqlite3.connect('external_users.db')
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
        INSERT OR REPLACE INTO combined_data (post_id, title, body, name, surname, email)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', row)

    conn.commit()
    conn.close()
