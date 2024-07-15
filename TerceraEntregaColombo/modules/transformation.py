import psycopg2
from dotenv import load_dotenv
import os

# Cargar las variables de entorno
load_dotenv()

def transform_and_combine():
    conn = psycopg2.connect(
        host=os.getenv("REDSHIFT_ENDPOINT"),
        port=os.getenv("REDSHIFT_PORT"),
        dbname=os.getenv("REDSHIFT_DB"),
        user=os.getenv("REDSHIFT_USER"),
        password=os.getenv("REDSHIFT_PASSWORD")
    )
    cursor = conn.cursor()

    query = '''
    SELECT posts.id, posts.title, posts.body, users.name, users.surname, users.email
    FROM posts
    JOIN users ON posts.userId = users.id
    '''

    cursor.execute(query)
    combined_data = cursor.fetchall()
    conn.close()

    return combined_data
