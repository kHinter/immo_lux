import logging
import cv2
import numpy as np
import pandas as pd
from airflow.models import Variable

#Custom modules
from . import utils

# from utils import fetch_url_with_retries

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
    good_matches = [m for m, n in matches if m.distance < 0.2 * n.distance]

    if len(matches) == 0:
        return 0
    return len(good_matches) / nfeatures

def get_city_coordinates(city_name):
    response = utils.fetch_url_with_retries(f"https://api.opencagedata.com/geocode/v1/json?q={city_name}&countrycode=lu&key={Variable.get('opencage_api_key')}")
    data = response.json()

    if response.status_code != 200:
        raise RuntimeError(f"Code {response.status_code} - {data['status']['message']}")

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
    
def merge_all_df_and_treat_duplicates(ds):
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

    #Introduction of a new column that will be used to identify the duplicates later on
    #1 is the default value (= no other duplicates)
    df["Duplicate_rank"] = 1

    df = df.sort_values(by=["Price", "Bedrooms", "Bathroom", "Surface"]).reset_index(drop=True)

    #Variables initialization
    areas_adjacent_to_districts = {
        "luxembourg" : {
            "Rollingergrund" : {
                "districts" : ["Muhlenbach", "Belair", "Limpertsberg", "Ville Haute"],
                "cities" : ["strassen", "kopstal"]
            },
            "Muhlenbach" : {
                "districts" : ["Rollingergrund", "Beggen", "Limpertsberg", "Eich"],
                "cities" : ["walferdange"]
            },
            "Beggen" : {
                "districts" : ["Muhlenbach", "Dommeldange", "Eich"],
                "cities" : ["walferdange"]
            },
            "Dommeldange" : {
                "districts" : ["Beggen", "Eich", "Weimerskirch"],
                "cities" : ["walferdange", "niederanven"]
            },
            "Limpertsberg" : {
                "districts" : ["Rollingergrund", "Muhlenbach", "Eich", "Pfaffenthal", "Ville Haute"],
                "cities" : []
            },
            "Eich" : {
                "districts" : ["Beggen", "Dommeldange", "Limpertsberg", "Muhlenbach", "Weimerskirch"],
                "cities" : []
            },
            "Weimerskirch" : {
                "districts" : ["Dommeldange", "Eich", "Pfaffenthal", "Kirchberg"],
                "cities" : ["niederanven"]
            },
            "Belair" : {
                "districts" : ["Rollingergrund", "Ville Haute", "Merl", "Hollerich"],
                "cities" : ["strassen"]
            },
            "Ville Haute" : {
                "districts" : ["Belair", "Rollingergrund", "Limpertsberg", "Pfaffenthal", "Grund", "Gare", "Hollerich"],
                "cities" : []
            },
            "Pfaffenthal" : {
                "districts" : ["Ville Haute", "Limpertsberg", "Eich", "Weimerskirch", "Kirchberg", "Clausen", "Grund"],
                "cities" : []
            },
            "Kirchberg" : {
                "districts" : ["Weimerskirch", "Pfaffenthal", "Clausen", "Neudorf-Weimershof"],
                "cities" : ["niederanven"]
            },
            "Neudorf-Weimershof" : {
                "districts" : ["Kirchberg", "Clausen", "Cents"],
                "cities" : ["niederanven", "sandweiler"]
            },
            "Clausen" : {
                "districts" : ["Neudorf-Weimershof", "Kirchberg", "Pfaffenthal", "Grund", "Cents"],
                "cities" : []
            },
            "Merl" : {
                "districts" : ["Belair", "Hollerich", "Cessange"],
                "cities" : ["strassen", "bertrange"]
            },
            "Hollerich" : {
                "districts" : ["Merl", "Belair", "Ville Haute", "Gare", "Gasperich", "Cessange"],
                "cities" : []
            },
            "Gare" : {
                "districts" : ["Hollerich", "Ville Haute", "Grund", "Bonnevoie", "Gasperich"],
                "cities" : []
            },
            "Grund" : {
                "districts" : ["Ville Haute", "Pfaffenthal", "Clausen", "Cents", "Pulvermuhl", "Bonnevoie", "Gare"],
                "cities" : []
            },
            "Cents" : {
                "districts" : ["Neudorf-Weimershof", "Clausen", "Grund", "Pulvermuhl", "Hamm"],
                "cities" : ["niederanven", "sandweiler"]
            },
            "Cessange" : {
                "districts" : ["Merl", "Hollerich", "Gasperich"],
                "cities" : ["bertrange", "leudelange"]
            },
            "Gasperich" : {
                "districts" : ["Cessange", "Hollerich", "Gare", "Bonnevoie"],
                "cities" : ["leudelange", "roeser", "hesperange"]
            },
            "Bonnevoie" : {
                "districts" : ["Gasperich", "Gare", "Grund", "Pulvermuhl", "Hamm"],
                "cities" : ["hesperange"]
            },
            "Pulvermuhl" : {
                "districts" : ["Bonnevoie", "Grund", "Cents", "Hamm"],
                "cities" : []
            },
            "Hamm" : {
                "districts" : ["Bonnevoie", "Pulvermuhl", "Cents"],
                "cities" : ["hesperange", "sandweiler"]
            }
        }
    }

    adjacent_cities = {
        "luxembourg" : ["strassen", "bertrange", "leudelange", "roeser", "hesperange", "sandweiler", "niederanven", "walferdange", "kopstal"],
        "esch-sur-alzette" : ["sanem", "mondercange", "schifflange", "kayl", "rumelange"],
        "strassen" : ["mamer", "kehlen", "kopstal", "luxembourg", "bertrange"],
        "hesperange" : ["luxembourg", "roeser", "weiler-la-tour", "contern", "sandweiler"],
        "bertrange" : ["mamer", "strassen", "dippach", "leudelange", "reckange-sur-mess", "luxembourg"],
        "mamer" : ["steinfort", "koerich", "kehlen", "strassen", "bertrange", "dippach", "garnich"],
        "leudelange" : ["reckange-sur-mess", "mondercange", "bettembourg", "roeser", "bertrange", "luxembourg"],
        "differdange" : ["pétange", "käerjeng", "sanem"],
        "käerjeng" : ["pétange", "differdange", "sanem", "dippach", "garnich"],
        "niederanven" : ["steinsel", "walferdange", "luxembourg", "sandweiler", "schuttrange", "betzdorf", "junglinster"],
        "walferdange" : ["kopstal", "steinsel", "luxembourg"],
        "sanem" : ["differdange", "käerjeng", "dippach", "reckange-sur-mess", "mondercange", "esch-sur-alzette"]
    }

    ##Constants

    surface_diff_threshold = 5
    photos_exactness_threshold = 0.045
    #To limit the amount of photos compared and reduce the similarity comparison computation time
    max_photos_treated_per_accomodation = 11
    #Distance expressed in km
    distance_between_cities_threshold = 6.7

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
            or (pd.notna(df.loc[i, "Bedrooms"]) and pd.notna(df.loc[j, "Bedrooms"]) and df.loc[i, "Bedrooms"] != df.loc[j, "Bedrooms"])
            or (pd.notna(df.loc[i, "Bathroom"]) and pd.notna(df.loc[j, "Bathroom"]) and df.loc[i, "Bathroom"] != df.loc[j, "Bathroom"])
            or surface_diff > surface_diff_threshold):
                #Allow to skip series of duplicates
                if duplicates_count > 0 and (j - i) == duplicates_count + 1:
                    i = j - 1
                break

            i_url = df.loc[i, "Link"]
            j_url = df.loc[j, "Link"]

            if pd.isna(df.loc[j, "Photos"]):
                continue
            
            #Skip the current j line duplicate treatment if both street names are not the same
            if pd.notna(df.loc[i, "Street_name_validity"]) and pd.notna(df.loc[j, "Street_name_validity"]) and df.loc[i, "Street_name"] != df.loc[j, "Street_name"]:
                logging.info(f"Skipping comparison between line {i+2} ( {i_url} ) and line {j+2} ( {j_url} ) because the street names are different !")
                continue
                     
            i_city = df.loc[i, "City"].lower()
            j_city = df.loc[j, "City"].lower()

            #Skip the current j line duplicate treatment if both districts of a same city are not adjacent
            if i_city == j_city and i_city in areas_adjacent_to_districts.keys():
                i_district = df.loc[i, "District"]
                j_district = df.loc[j, "District"]

                if pd.notna(i_district) and pd.notna(j_district) and i_district != j_district and j_district not in areas_adjacent_to_districts[i_city][i_district]["districts"]:
                    continue
            elif i_city != j_city:
                i_district = df.loc[i, "District"]
                j_district = df.loc[j, "District"]

                if i_city in areas_adjacent_to_districts.keys() and pd.notna(i_district) and j_city not in areas_adjacent_to_districts[i_city][i_district]["cities"]:
                    logging.info(f"Skipping comparison between line {i+2} ( {i_url} ) and line {j+2} ( {j_url} ) because the district of {i_district} and the city of {j_city} are not adjacent !")
                    continue
                elif j_city in areas_adjacent_to_districts.keys() and pd.notna(j_district) and i_city not in areas_adjacent_to_districts[j_city][j_district]["cities"]:
                    logging.info(f"Skipping comparison between line {i+2} ( {i_url} ) and line {j+2} ( {j_url} ) because the district of {j_district} and the city of {i_city} are not adjacent !")
                    continue
                
                #To avoid code duplication
                i_city_in_adj_cities = i_city in adjacent_cities.keys()
                j_city_in_adj_cities = j_city in adjacent_cities.keys()

                #Skip the current j line duplicate treatment if both cities are not adjacent
                if i_city_in_adj_cities and j_city not in adjacent_cities[i_city]:
                    logging.info(f"Skipping comparision between line {i+2} ( {i_url} ) and line {j+2} ( {j_url} ) because the city of {i_city} and the city of {j_city} are not adjacent")
                    continue     
                elif j_city_in_adj_cities and i_city not in adjacent_cities[j_city]:
                    logging.info(f"Skipping comparision between line {i+2} ( {i_url} ) and line {j+2} ( {j_url} ) because the city of {i_city} and the city of {j_city} are not adjacent")
                    continue
                elif not i_city_in_adj_cities and not j_city_in_adj_cities:
                    i_city_coords = get_city_coordinates(i_city)
                    j_city_coords = get_city_coordinates(j_city)
                    distance_between_cities = calculate_haversine_distance(i_city_coords, j_city_coords)

                    #Skip the current j line duplicate treatment if the distance between the cities is too big
                    if distance_between_cities >= distance_between_cities_threshold:
                        logging.info(f"Skipping comparison between line {i+2} ( {i_url} ) and line {j+2} ( {j_url} ) because the distance between {i_city} and {j_city} is above {distance_between_cities_threshold} km !")
                        continue

            logging.info(f"Comparison between line {i+2} ( {i_url} ) and line {j+2} ( {j_url} )")

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

                if i_photo.size == 0:
                    logging.warning(f"The accomodation image {i_photo_url} haven't been successfully loaded !")

                for j_photo_url, j_photo in j_loaded_photos.items():
                    metric_val = sift_similarity(i_photo, j_photo)
                    logging.info(f"\tSIFT similarity score between {i_photo_url} and {j_photo_url} = {round(metric_val, 3)}")

                    if metric_val >= photos_exactness_threshold:
                        duplicates_count += 1
                        df.loc[j, "Duplicate_rank"] = duplicates_count + 1

                        logging.info(f"\tBoth accommodations have been identified and flagged as duplicates !")
                        break
                
                if metric_val >= photos_exactness_threshold:
                    break
            
            #Allow to skip the series of adjacent duplicates
            if duplicates_count > 0 and (j - i) == duplicates_count + 1:
                i = j - 1

        i+=1
    df.to_csv(f"{Variable.get('immo_lux_data_folder')}/deduplicated/merged_accomodations_{ds}.csv", index=False)

# merge_all_df_and_treat_duplicates("2025-02-05")