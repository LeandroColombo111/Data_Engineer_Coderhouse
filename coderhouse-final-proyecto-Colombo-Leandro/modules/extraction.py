import requests

def extract_posts():
    url = "https://jsonplaceholder.typicode.com/posts"
    response = requests.get(url)
    data = response.json()
    return data
