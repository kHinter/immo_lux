import requests
import pandas as pd

def fetch_url_with_retries(url, retries=5, delay=5, headers=None):
    import time
    import logging
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=12, headers=headers)
            if response.status_code != 404:
                return response
            else:
                logging.warning(f"404 error for {url} !")
        except requests.exceptions.ReadTimeout:
            logging.warning(f"The get request for {url} timed out !")
            time.sleep(delay)

def get_image_from_url(url):
    from io import BytesIO
    from PIL import Image
    
    response = fetch_url_with_retries(url)
    
    if response.status_code == 200:
        img = Image.open(BytesIO(response.content)).convert("RGB")  # Ouvrir l'image à partir du flux binaire
        return img
    return None