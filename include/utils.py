import time
import requests

def fetch_url_with_retries(url, retries=3, delay=2, headers=None):
    for attempt in range(retries):
        try:
            return requests.get(url, timeout=5, headers=headers)
        except requests.exceptions.ReadTimeout:
            time.sleep(delay)