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

def merge_dataframes(ds, ti):
    import os
    from airflow.models import Variable

    #Retrieve all df
    df_athome = pd.read_csv(
        f"{Variable.get('immo_lux_data_folder')}/enriched/athome_last3d_{ds}.csv",
        dtype={
            "Monthly_charges" : "Int64",
            "Deposit" : "Int64",
            "Floor_number" : "Int64",
            "Bedrooms" : "Int64",
            "Bathroom" : "Int64",
            "Garages" : "Int64"})

    df_immotop = pd.read_csv(f"{Variable.get('immo_lux_data_folder')}/enriched/immotop_lu_{ds}.csv")

    #Merge all df into a single one
    df = pd.concat([df_athome, df_immotop])

    merged_folder_path = f"{Variable.get('immo_lux_data_folder')}/merged"
    if not os.path.isdir(merged_folder_path):
        os.makedirs(merged_folder_path)

    df.to_csv(f"{merged_folder_path}/merged_{ds}.csv", index=False)

    ti.xcom_push(key="path", value=f"{merged_folder_path}/merged_{ds}.csv")

def get_merged_dataframe(ti):
    return pd.read_csv(ti.xcom_pull(key="path", task_ids="merge_dataframes"))