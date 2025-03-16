import logging
import numpy as np
import pandas as pd
import re
from airflow.models import Variable

#Custom modules
from . import utils

# from utils import fetch_url_with_retries, get_image_from_url

def get_embedding(image, model, transform):
    import torch

    #Add a batch dimension
    image = transform(image).unsqueeze(0)
    with torch.no_grad():
        #Extract features
        embedding = model(image).squeeze()
    return embedding

def init_dinoV2():
    import torchvision.transforms as transforms
    import timm
    import torch

    device = torch.device("cpu")
    model = timm.create_model("vit_small_patch14_dinov2.lvd142m", pretrained=True).to(device)
    model.eval() 

    transform = transforms.Compose([
        transforms.Resize((518, 518)),
        transforms.ToTensor(),
        transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),  # Normalisation
    ])

    return model, transform

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
    import torch.nn.functional as F

    #Introduction of a new column that will be used to identify the duplicates later on
    #1 is the default value (= no other duplicates)
    df["Duplicate_rank"] = 1

    df = df.sort_values(by=["Price", "Bedrooms", "Bathroom", "Shower_room", "Surface"]).reset_index(drop=True)

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
    photos_exactness_threshold = 0.88
    #To limit the amount of photos compared and reduce the similarity comparison computation time
    max_photos_treated_per_accomodation = 11
    #Distance expressed in km
    distance_between_cities_threshold = 6.7
    #The photo number at which I need to stop the image similarity comparison between two accomodations if the sum of the similarity score of the previous comparison is equal to zero 
    max_photo_number_threshold = 5

    athome_img_id_reg = re.compile("(?<=\/)\w+(?=\.jpg)|(?<=\/)\w+(?=\.png)", re.IGNORECASE)

    i = 0
    df_len = len(df)

    #To save the number of comparisons required before finding that two accomodations are duplicates
    comparisons_number = []
    accomodations_embeddings = {}

    dinoV2_model, dinoV2_transform = init_dinoV2()
    logging.info("Duplicates treatment has started")

    #Using a while in order to change the incrementation value
    while i < df_len:
        #The count of duplicated elements compared to i
        duplicates_count = 0

        if df.loc[i, "Duplicate_rank"] > 1:
            logging.info(f"Skipping line {i+2} ( {i_url} ) because line {i+2} has already been flagged as a duplicate during a previous comparison !")
            i += 1
            accomodations_embeddings.pop(i+2, None)
            continue

        #If no images to compare then skip
        if pd.isna(df.loc[i, "Photos"]):
            i += 1
            accomodations_embeddings.pop(i+2, None)
            continue

        for j in range (i+1, df_len):
            surface_diff_abs = abs(df.loc[j, "Surface"] - df.loc[i, "Surface"])

            if (df.loc[i, "Price"] != df.loc[j, "Price"]
            or (pd.notna(df.loc[i, "Bedrooms"]) and pd.notna(df.loc[j, "Bedrooms"]) and df.loc[i, "Bedrooms"] != df.loc[j, "Bedrooms"])
            or (pd.notna(df.loc[i, "Bathroom"]) and pd.notna(df.loc[j, "Bathroom"]) and df.loc[i, "Bathroom"] != df.loc[j, "Bathroom"])
            or (pd.notna(df.loc[i, "Shower_room"]) and pd.notna(df.loc[j, "Shower_room"]) and df.loc[i, "Shower_room"] != df.loc[j, "Shower_room"])
            or surface_diff_abs > surface_diff_threshold):
                break

            if df.loc[j, "Duplicate_rank"] > 1:
                logging.info(f"Skipping comparison between line {i+2} ( {i_url} ) and line {j+2} ( {j_url} ) because line {j+2} has already been flagged as a duplicate during a previous comparison !")
                continue

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
                    logging.info(f"Skipping comparison between line {i+2} ( {i_url} ) and line {j+2} ( {j_url} ) because the districts of {i_district} and {j_district} are not adjacent !")
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

            common_url = []
            #Find if there is an id in common between the urls of the two accomodations
            if df.loc[i, "Website"] == "athome" and df.loc[j, "Website"] == "athome":
                duplicate_found = False

                for i_photo_url in i_photos_url:
                    match = athome_img_id_reg.search(i_photo_url)
                    if match:
                        common_url.append(match.group())
                    else:
                        logging.warning(f"Cannot extract the image id from {i_photo_url} !")
                
                for j_photo_url in j_photos_url:
                    match = athome_img_id_reg.search(j_photo_url)
                    if match:
                        if match.group() in common_url:
                            duplicates_count += 1
                            df.loc[j, "Duplicate_rank"] = duplicates_count + 1

                            logging.info("\tBoth accommodations have been identified and flagged as duplicates because they have at least one image id in common !")
                            duplicate_found = True
                            break
                    else:
                        logging.warning(f"Cannot extract the image id from {j_photo_url} !")
                #Skip the current j line duplicate treatment
                if duplicate_found:
                    continue

            #Initialization of variables so they are accessible in the external for loops
            cosine_sim = 0
            attempts = 0
            similarity_score_sum = 0

            if j+2 not in accomodations_embeddings.keys():
                accomodations_embeddings[j+2] = {}
                #Pre-load the j accomodation embeddings to save computation time
                for j_photo_url in j_photos_url:
                    j_photo = utils.get_image_from_url(j_photo_url)

                    if j_photo is not None:
                        accomodations_embeddings[j+2][j_photo_url] = get_embedding(j_photo, dinoV2_model, dinoV2_transform)
                    else:
                        logging.warning(f"\tThe accomodation image {j_photo_url} haven't been successfully loaded !")
            
            for i_photo_url in i_photos_url:
                
                if i+2 not in accomodations_embeddings.keys():
                    i_photo = utils.get_image_from_url(i_photo_url)

                    if i_photo is None:
                        logging.warning(f"\tThe accomodation image {i_photo_url} haven't been successfully loaded !")
                        continue
                    i_embedding = get_embedding(i_photo, dinoV2_model, dinoV2_transform)
                else:
                    i_embedding = accomodations_embeddings[i+2][i_photo_url]

                duplicate_found = False
                for j_photo_url, j_embedding in accomodations_embeddings[j+2].items():
                    cosine_sim = F.cosine_similarity(i_embedding, j_embedding, dim=0).item()
                    logging.info(f"\tCosine similarity score between {i_photo_url} and {j_photo_url} = {round(cosine_sim, 5)}")
                    
                    similarity_score_sum += cosine_sim
                    attempts += 1

                    if cosine_sim >= photos_exactness_threshold:
                        duplicates_count += 1
                        df.loc[j, "Duplicate_rank"] = duplicates_count + 1

                        logging.info(f"\tBoth accommodations have been identified and flagged as duplicates after {attempts} comparisons !")
                        duplicate_found = True
                        comparisons_number.append(attempts)
                        break
                
                if duplicate_found:
                    break
            
            logging.info(f"\t Sum of similarity score : {similarity_score_sum}")
        i+=1
        accomodations_embeddings.pop(i+2, None)
    df.to_csv(f"{Variable.get('immo_lux_data_folder')}/deduplicated/merged_accomodations_{ds}.csv", index=False)

    #Save the metadata
    df_comparisons_number = pd.DataFrame({"Comparisons_number" : comparisons_number})
    df_comparisons_number.to_csv(f"{Variable.get('immo_lux_data_folder')}/tmp/comp_number_{ds}.csv", index=False)

# merge_all_df_and_treat_duplicates("2025-02-10")