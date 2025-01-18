import logging
import cv2
import numpy as np
import pandas as pd
import os
from datetime import date
from airflow.models import Variable

#Custom modules
from . import utils

# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

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

def get_city_coordinates(city_name):
    response = utils.fetch_url_with_retries(f"https://api.opencagedata.com/geocode/v1/json?q={city_name}&countrycode=lu&key={Variable.get('opencage_api_key')}")
    data = response.json()
    if data["results"]:
        coordinates = data["results"][0]["geometry"]
        return (coordinates["lat"], coordinates["lng"])
    else:
        raise ValueError(f"City {city_name} not found")

def calculate_haversine_distance(coord1, coord2):
    R = 6371
    lat1, lon1 = coord1
    lat2, lon2 = coord2

    dlat = np.radians(lat2 - lat1)
    dlon = np.radians(lon2 - lon1)

    a = np.sin(dlat / 2) * np.sin(dlat / 2) + np.cos(np.radians(lat1)) * np.cos(np.radians(lat2)) * np.sin(dlon / 2) * np.sin(dlon / 2)
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

    return R * c
    
def merge_all_df_and_treat_duplicates():
    #Retrieve all df
    today = str(date.today())
    # today = "2025-01-10"
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

    df = df.sort_values(by=["Price", "Bedrooms", "Bathroom", "Surface"]).reset_index(drop=True)

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
        "Strassen" : ["Mamer", "Kehlen", "Kopstal", "Luxembourg", "Bertrange"],
        "Hesperange" : ["Luxembourg", "Roeser", "Weiler-la-Tour", "Contern", "Sandweiler"],
        "Differdange" : ["Pétange", "Käerjeng", "Sanem"],
        "Walferdange" : ["Kopstal", "Steinsel", "Luxembourg"],
        "Sanem" : ["Differdange", "Käerjeng", "Dippach", "Reckange-sur-Mess", "Mondercange", "Esch-sur-Alzette"]
    }



    ##Constants

    surface_diff_threshold = 5
    photos_exactness_threshold = 0.07
    #To limit the amount of photos compared and reduce the similarity comparison computation time
    max_photos_treated_per_accomodation = 11
    #Distance expressed in km
    distance_between_cities_threshold = 8

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

            if (df.loc[i, "Price"] != df.loc[j, "Price"] 
            or pd.notna(df.loc[i, "Bedrooms"]) and pd.notna(df.loc[j, "Bedrooms"]) and df.loc[i, "Bedrooms"] != df.loc[j, "Bedrooms"]
            or pd.notna(df.loc[i, "Bathroom"]) and pd.notna(df.loc[j, "Bathroom"]) and df.loc[i, "Bathroom"] != df.loc[j, "Bathroom"]
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

                if pd.notna(i_district) and pd.notna(j_district) and i_district != j_district and j_district not in adjacent_districts[i_city][i_district]:
                    continue
            
            #Skip the current j line duplicate treatment if both cities are not adjacent
            elif i_city in adjacent_cities.keys() and j_city not in adjacent_cities[i_city]:
                continue
            elif j_city in adjacent_cities.keys() and i_city not in adjacent_cities[j_city]:
                continue

            if pd.isna(df.loc[j, "Photos"]):
                continue
            
            i_city_coords = get_city_coordinates(i_city)
            j_city_coords = get_city_coordinates(j_city)
            distance_between_cities = calculate_haversine_distance(i_city_coords, j_city_coords)

            #Skip the current j line duplicate treatment if the distance between the cities is too big
            if distance_between_cities >= distance_between_cities_threshold:
                continue

            logging.info(f"Comparison between accomodation line {i+2} and accomodation line {j+2}")

            i_photos_url = df.loc[i, "Photos"].split(" ")[:max_photos_treated_per_accomodation]
            j_photos_url = df.loc[j, "Photos"].split(" ")[:max_photos_treated_per_accomodation]

            #Initialization of variables so they are accessible in the external for loops
            metric_val = 0

            j_loaded_photos = {}

            #Pre-load the j accomodation photos to save computation time
            for j_photo_url in j_photos_url:
                j_photo_request = utils.fetch_url_with_retries(j_photo_url)

                #If the requests for j failed skip to j + 1
                if j_photo_request.status_code != 200:
                    continue

                j_loaded_photos[j_photo_url] = cv2.imdecode(np.asarray(bytearray(j_photo_request.content), dtype=np.uint8), -1)
                
            for i_photo_url in i_photos_url:
                #Get the photo via HTML request
                i_photo_request = utils.fetch_url_with_retries(i_photo_url)
                    
                #If the requests for i failed skip to i + 1
                if i_photo_request.status_code != 200:
                    continue
                i_photo = cv2.imdecode(np.asarray(bytearray(i_photo_request.content), dtype=np.uint8), -1)

                for j_photo_url, j_photo in j_loaded_photos.items():
                    metric_val = sift_similarity(i_photo, j_photo)
                    logging.info(f"\tSIFT similarity score between {i_photo_url} and {j_photo_url} = {round(metric_val, 3)}")

                    if metric_val >= photos_exactness_threshold:
                        duplicates_count += 1
                        df.loc[j, "Duplicate_rank"] = duplicates_count + 1
                        break
                
                if metric_val >= photos_exactness_threshold:
                    break
            
            #Allow to skip the series of adjacent duplicates
            if duplicates_count > 0 and (j - i) == duplicates_count + 1:
                i = j - 1

        i+=1
    df.to_csv(f"{airflow_home}/dags/data/deduplicated/merged_accomodations_{today}.csv", index=False)

# merge_all_df_and_treat_duplicates()