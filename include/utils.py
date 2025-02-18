import time
import requests
import logging
from PIL import Image
from io import BytesIO

def fetch_url_with_retries(url, retries=5, delay=2, headers=None):
    for attempt in range(retries):
        try:
            return requests.get(url, timeout=8, headers=headers)
        except requests.exceptions.ReadTimeout:
            logging.warning(f"The get request for {url} timed out !")
            time.sleep(delay)

def get_image_from_url(url):
    response = fetch_url_with_retries(url)
    
    if response.status_code == 200:
        img = Image.open(BytesIO(response.content)).convert("RGB")  # Ouvrir l'image à partir du flux binaire
        return img
    return None
