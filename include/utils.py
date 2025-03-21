import requests

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

def create_data_related_folder_if_not_exists(folder_name):
    import os
    from airflow.models import Variable

    if not os.path.exists(f"{Variable.get('immo_lux_data_folder')}/{folder_name}"):
        os.makedirs(f"{Variable.get('immo_lux_data_folder')}/{folder_name}")