import time
import requests
import logging
import cv2
import numpy as np

def fetch_url_with_retries(url, retries=5, delay=2, headers=None):
    for attempt in range(retries):
        try:
            return requests.get(url, timeout=8, headers=headers)
        except requests.exceptions.ReadTimeout:
            logging.warning(f"The get request for {url} timed out !")
            time.sleep(delay)

def load_img_from_url(url):
    response = fetch_url_with_retries(url)
    
    if response.status_code == 200:
        return cv2.imdecode(np.asarray(bytearray(response.content), dtype=np.uint8), cv2.IMGREAD_UNCHANGED)
    return None
