import requests


def url_exists(url):
    if url.startswith('http://') or url.startswith('https://'):
        return requests.head(url).status_code == 200
    else:
        return False

def is_S3_url(url):
    if url.startswith('s3://') or url.startswith('https://s3'):
        return True
    else:
        return False