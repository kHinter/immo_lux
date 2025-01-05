import logging
import cv2
import requests
import numpy as np
import pandas as pd
import os
from datetime import date
import time

def sift_similarity(img1, img2):
    nfeatures = 500
    sift = cv2.SIFT_create(nfeatures=nfeatures)

    #Image preprocessing
    img1 = cv2.cvtColor(img1, cv2.COLOR_BGR2GRAY)
    img2 = cv2.cvtColor(img2, cv2.COLOR_BGR2GRAY)

    kp_img1, desc_img1 = sift.detectAndCompute(img1, None)
    kp_img2, desc_img2 = sift.detectAndCompute(img2, None)

    index_params = dict(algorithm=1, trees=5)  # K-D Tree (algorithm=1). Increase "trees" value to improve precision, decrease to improve speed
    search_params = dict(checks=50)  #Amount of comparison. Increase to improve precision, decrease to improve speed

    flann = cv2.FlannBasedMatcher(index_params, search_params)

    # bf = cv2.BFMatcher(cv2.NORM_L2)
    matches = flann.knnMatch(desc_img1, desc_img2, k=2)
    good_matches = [m for m, n in matches if m.distance < 0.3 * n.distance]

    if len(matches) == 0:
        return 0
    return len(good_matches) / nfeatures

def fetch_url_with_retries(url, retries=3, delay=3):
    for attempt in range(retries):
        try:
            return requests.get(url, timeout=5)
        except requests.exceptions.ReadTimeout:
            time.sleep(delay)

    
def merge_all_df_and_treat_duplicates():
    #Retrieve all df
    # today = str(date.today())
    today = "2025-01-02"
    airflow_home = os.environ["AIRFLOW_HOME"]

    df_athome = pd.read_csv(
        f"{airflow_home}/dags/data/enriched/athome_last3d_{today}.csv",
        dtype={
            "Monthly_charges" : "Int64",
            "Deposit" : "Int64",
            "Floor_number" : "Int64",
            "Bedrooms" : "Int64",
            "Bathroom" : "Int64",
            "Garages" : "Int64"})

    df_immotop = pd.read_csv(f"{airflow_home}/dags/data/enriched/immotop_lu_{today}.csv")

    #Merge all df into a single one
    df = pd.concat([df_athome, df_immotop])

    #Introduction of a new column that will be used to identify the duplicates later on
    #1 is the default value (= no other duplicates)
    df["Duplicate_rank"] = 1

    df = df.sort_values(by=["Price", "Bedrooms", "Surface"]).reset_index(drop=True)

    #Variables initialization
    adjacent_districts = {
        "Luxembourg" : {
            "Rollingergrund" : ["Muhlenbach", "Belair", "Limpertsberg", "Ville Haute"],
            "Muhlenbach" : ["Rollingergrund", "Beggen", "Limpertsberg", "Eich"],
            "Beggen" : ["Muhlenbach", "Dommeldange", "Eich"],
            "Dommeldange" : ["Beggen", "Eich", "Weimerskirch"],
            "Limpertsberg" : ["Rollingergrund", "Muhlenbach", "Eich", "Pfaffenthal", "Ville Haute"],
            "Eich" : ["Beggen", "Dommeldange", "Limpertsberg", "Muhlenbach", "Weimerskirch"],
            "Weimerskirch" : ["Dommeldange", "Eich", "Pfaffenthal", "Kirchberg"],
            "Belair" : ["Rollingergrund", "Ville Haute", "Merl", "Hollerich"],
            "Ville Haute" : ["Belair", "Rollingergrund", "Limpertsberg", "Pfaffenthal", "Grund", "Gare", "Hollerich"],
            "Pfaffenthal" : ["Ville Haute", "Limpertsberg", "Eich", "Weimerskirch", "Kirchberg", "Clausen", "Grund"],
            "Kirchberg" : ["Weimerskirch", "Pfaffenthal", "Clausen", "Neudorf-Weimershof"],
            "Neudorf-Weimershof" : ["Kirchberg", "Clausen", "Cents"],
            "Clausen" : ["Neudorf-Weimershof", "Kirchberg", "Pfaffenthal", "Grund", "Cents"],
            "Merl" : ["Belair", "Hollerich", "Cessange"],
            "Hollerich" : ["Merl", "Belair", "Ville Haute", "Gare", "Gasperich", "Cessange"],
            "Gare" : ["Hollerich", "Ville Haute", "Grund", "Bonnevoie", "Gasperich"],
            "Grund" : ["Ville Haute", "Pfaffenthal", "Clausen", "Cents", "Pulvermuhl", "Bonnevoie", "Gare"],
            "Cents" : ["Neudorf-Weimershof", "Clausen", "Grund", "Pulvermuhl", "Hamm"],
            "Cessange" : ["Merl", "Hollerich", "Gasperich"],
            "Gasperich" : ["Cessange", "Hollerich", "Gare", "Bonnevoie"],
            "Bonnevoie" : ["Gasperich", "Gare", "Grund", "Pulvermuhl", "Hamm"],
            "Pulvermuhl" : ["Bonnevoie", "Grund", "Cents", "Hamm"],
            "Hamm" : ["Bonnevoie", "Pulvermuhl", "Cents"]
        }
    }

    adjacent_cities = {
        "Luxembourg" : ["Strassen", "Bertrange", "Leudelange", "Roeser", "Hesperange", "Sandweiler", "Niederanven", "Walferdange", "Kopstal"],
        "Esch-sur-Alzette" : ["Sanem", "Mondercange", "Schifflange" "Kayl", "Rumelange"],
        "Strassen" : ["Mamer", "Kehlen", "Kopstal", "Luxembourg", "Bertrange"]
    }

    i = 0
    df_len = len(df)

    logging.info("Duplicates treatment has started")

    #Using a while in order to change the incrementation value
    while i < df_len:
        #The count of duplicated elements compared to i
        duplicates_count = 0

        #If no images to compare then skip
        if pd.isna(df.loc[i, "Photos"]):
            i += 1
            continue

        for j in range (i+1, df_len):
            surface_diff = df.loc[j, "Surface"] - df.loc[i, "Surface"]
            surface_diff_threshold = 5

            if (df.loc[i, "Price"] != df.loc[j, "Price"] 
            or (pd.notna(df.loc[i, "Bedrooms"]) and pd.notna(df.loc[j, "Bedrooms"]) and df.loc[i, "Bedrooms"] != df.loc[j, "Bedrooms"]) 
            or surface_diff > surface_diff_threshold):
                #Allow to skip series of duplicates
                if duplicates_count > 0 and (j - i) == duplicates_count + 1:
                    i = j - 1
                break
                     
            i_city = df.loc[i, "City"]
            j_city = df.loc[j, "City"]

            #Skip the current j line duplicate treatment if both districts of a same city are not adjacent
            if i_city == j_city and i_city in adjacent_districts.keys():
                i_district = df.loc[i, "District"]
                j_district = df.loc[j, "District"]

                if i_district != j_district and j_district not in adjacent_districts[i_city][i_district]:
                    continue
            
            #Skip the current j line duplicate treatment if both cities are not adjacent
            elif i_city in adjacent_cities.keys() and j_city not in adjacent_cities[i_city]:
                continue
            elif j_city in adjacent_cities.keys() and i_city not in adjacent_cities[j_city]:
                continue

            if pd.isna(df.loc[j, "Photos"]):
                continue

            logging.info(f"Comparison between accomodation line {i+2} and accomodation line {j+2}")

            i_photos_url = df.loc[i, "Photos"].split(" ")
            j_photos_url = df.loc[j, "Photos"].split(" ")

            #Initialization of variables so they are accessible in the external for loops
            metric_val = 0
            exactness_threshold = 0.07

            for i_photo_url in i_photos_url:
                for j_photo_url in j_photos_url:
                    #Get the images from url
                    i_photo_request = fetch_url_with_retries(i_photo_url)
                    i_photo = cv2.imdecode(np.asarray(bytearray(i_photo_request.content), dtype=np.uint8), -1)

                    j_photo_request = fetch_url_with_retries(j_photo_url)
                    j_photo = cv2.imdecode(np.asarray(bytearray(j_photo_request.content), dtype=np.uint8), -1)

                    metric_val = sift_similarity(i_photo, j_photo)
                    logging.info(f"\tSIFT similarity score between {i_photo_url} and {j_photo_url} = {round(metric_val, 3)}")

                    if metric_val >= exactness_threshold:
                        duplicates_count += 1
                        df.loc[j, "Duplicate_rank"] = duplicates_count + 1
                        break
                
                if metric_val >= exactness_threshold:
                    break
            
            #Allow to skip the series of adjacent duplicates
            if duplicates_count > 0 and (j - i) == duplicates_count + 1:
                i = j - 1

        i+=1
    df.to_csv(f"{airflow_home}/dags/data/deduplicated/merged_accomodations_{today}.csv", index=False)

# merge_all_df_and_treat_duplicates()