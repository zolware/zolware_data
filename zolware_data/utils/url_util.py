import requests


def url_exists(url):
    if url.startswith('http://') or url.startswith('https://'):
        return requests.head(url).status_code == 200
    else:
        return False
