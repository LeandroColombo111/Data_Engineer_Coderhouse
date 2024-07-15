import sqlite3

def transform_and_combine():
    conn = sqlite3.connect('external_users.db')
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
