import urllib.request
import pandas as pd
from datetime import date
import re
import os
import cv2
import numpy as np
import logging
import sys
import matplotlib.pyplot as plt

##REGEX
deposit_amount_reg = re.compile("((?<=Deposit amount: )€?\d+(?:\.\d+)?)|((?<=Montant caution: )€?\d+(?:\.\d+)?)", re.IGNORECASE)

##TRANSLATE TABLES
translate_table_price = str.maketrans("", "", "€ ,")

def get_garages_number(garage):
    garages_number = [int(s) for s in garage.split() if s.isdigit()]
    return str(sum(garages_number))

def get_floor_number(floor_number):
    if floor_number == "Ground floor":
        return 0
    elif not floor_number.isdigit():
        for part in floor_number.split():
            if part.isdigit():
                return part
    else:
        return floor_number

def clean_deposit(df):
    for i in range (len(df)):
        try:
            if df.loc[i, "Deposit"] == "Not indicated":
                if df.loc[i, "Rental guarantee"] != "Not specified" and pd.notna(df.loc[i, "Rental guarantee"]):
                    df.loc[i, "Deposit"] = df.loc[i, "Rental guarantee"].translate(translate_table_price)
                elif pd.notna(df.loc[i, "Description"]):
                    matches = deposit_amount_reg.findall(df.loc[i, "Description"])
                    #No deposit reference found in the accomodation description
                    if len(matches) == 0:
                        df.loc[i, "Deposit"] = pd.NA
                    else:
                        df.loc[i, "Deposit"] = next(element.replace("€", "") for element in matches[0] if element != "")
                else:
                    df.loc[i, "Deposit"] = pd.NA
            else:
                df.loc[i, "Deposit"] = df.loc[i, "Deposit"].translate(translate_table_price)
        except KeyError:
            pass

def get_heating_athome(row):
    if row["Has_gas_heating"] == "Oui":
        return "Gas"
    elif row["Has_electric_heating"] == "Oui":
        return "Electric"

def get_district(district):
    district_lower = district.lower()
    district_blacklist = ("new", "excellent condition", "germany", "luxemburg")

    if district_lower.startswith(("rue ", "route ")) or district_lower.endswith("floor") or district_lower in district_blacklist:
        return pd.NA
    else:
        return district.replace("Localité", "")
    
def hist_similarity(img1, img2):
    hist_img1 = cv2.calcHist([img1], [0, 1, 2], None, [256, 256, 256], [0, 256, 0, 256, 0, 256])
    hist_img1[255, 255, 255] = 0 #Ignore white pixels
    cv2.normalize(hist_img1, hist_img1, alpha=0, beta=1, norm_type=cv2.NORM_MINMAX)

    hist_img2 = cv2.calcHist([img2], [0, 1, 2], None, [256, 256, 256], [0, 256, 0, 256, 0, 256])
    hist_img2[255, 255, 255] = 0 #Ignore white pixels
    cv2.normalize(hist_img2, hist_img2, alpha=0, beta=1, norm_type=cv2.NORM_MINMAX)

    # Find the metric value
    return cv2.compareHist(hist_img1, hist_img2, cv2.HISTCMP_CORREL)

def sift_similarity(img1, img2):
    sift = cv2.SIFT_create(nfeatures=500)

    #Image preprocessing
    img1 = cv2.cvtColor(img1, cv2.COLOR_BGR2GRAY)
    img2 = cv2.cvtColor(img2, cv2.COLOR_BGR2GRAY)

    #Improve the contrasts

    kp_img1, desc_img1 = sift.detectAndCompute(img1, None)
    kp_img2, desc_img2 = sift.detectAndCompute(img2, None)

    bf = cv2.BFMatcher(cv2.NORM_L2)
    matches = bf.knnMatch(desc_img1, desc_img2, k=2)
    good_matches = [m for m, n in matches if m.distance < 0.5 * n.distance]

    if len(matches) == 0:
        return 0
    return len(good_matches) / max(len(kp_img1), len(kp_img2))
    
def treat_athome_duplicates(df):
    df = df.sort_values(by=["Price", "Surface"]).reset_index(drop=True)

    i = 0
    df_len = len(df)

    logging.info("Athome duplicates treatment")

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
            if df.loc[i, "Price"] != df.loc[j, "Price"] or surface_diff > surface_diff_threshold:
                break

            if pd.isna(df.loc[j, "Photos"]):
                continue

            logging.info(f"Comparison between accomodation line {i+2} and accomodation line {j+2}")

            i_photos_url = df.loc[i, "Photos"].split(" ")
            j_photos_url = df.loc[j, "Photos"].split(" ")

            #Initialization of variables so they are accessible in the external for loops
            metric_val = 0
            exactness_threshold = 0.90

            for i_photo_url in i_photos_url:
                for j_photo_url in j_photos_url:
                    #Get the images from url
                    i_photo_request = urllib.request.urlopen(i_photo_url)
                    i_photo = cv2.imdecode(np.asarray(bytearray(i_photo_request.read()), dtype=np.uint8), -1)

                    j_photo_request = urllib.request.urlopen(j_photo_url)
                    j_photo = cv2.imdecode(np.asarray(bytearray(j_photo_request.read()), dtype=np.uint8), -1)

                    # metric_val = hist_similarity(i_photo, j_photo)
                    # logging.info(f"\tHist similarity score between {i_photo_url} and {j_photo_url} = {round(metric_val, 2)}")
                    # print(f"Hist Similarity Score: ", )

                    #Decomment only for testing purpose
                    # ret_value = orb_similarity(i_photo, j_photo)

                    # plt.figure()
                    # plt.imshow(ret_value)
                    # plt.show()

                    metric_val = sift_similarity(i_photo, j_photo)
                    logging.info(f"\tSIFT similarity score between {i_photo_url} and {j_photo_url} = {round(metric_val, 3)}")

                    if metric_val >= exactness_threshold:
                        duplicates_count += 1
                        df.loc[j, "Duplicate_rank"] = duplicates_count + 1

                        #Allow to skip the series of adjacent duplicates
                        if (j - i) == duplicates_count:
                            i = j
                        break
                
                if metric_val >= exactness_threshold:
                    break
        i+=1

def immotop_lu_data_cleaning():
    today = str(date.today())
    airflow_home = os.environ["AIRFLOW_HOME"]
    
    df = pd.read_csv(f"{airflow_home}/dags/data/raw/immotop_lu_{today}.csv", dtype={"Bedrooms" : "Int64"})
    
    #Starting by renaming the columns to correspond with all the other files
    #Key = Feature name displayed on the website, Value = Column name on the df
    column_names = {
        "Floor" : "Floor_number",
        "Lift" : "Has_lift",
        "Bathrooms" : "Bathroom",
        "Furnished" : "Is_furnished",
        "Terrace" : "Has_terrace",
        "Balcony" : "Has_balcony",
        "Garage, car parking" : "Garages",
        "Estate agency fee" : "Agency_fees",
        "Condominium fees" : "Condominium_fees"
    }

    df.drop_duplicates(subset=["Link"], inplace=True)

    df.rename(columns=column_names, inplace=True)

    df["Surface"] = df["Surface"].apply(lambda surface: surface.replace("m²", "").replace(" ", "") if pd.notnull(surface) else surface)

    df["Agency_fees"] = df["Agency_fees"].apply(lambda fees: fees.replace("Not specified", "").translate(translate_table_price) if pd.notnull(fees) else fees)

    df["Has_lift"] = df["Has_lift"].map({"Yes" : "Oui", "No" : "Non"})
    
    df["Has_terrace"] = df["Has_terrace"].map({"Yes" : "Oui", "No" : "Non"})
    df["Has_balcony"] = df["Has_balcony"].apply(lambda has_balcony: "Oui" if has_balcony == "Yes" else has_balcony)

    df["Price"] = df["Price"].apply(lambda price: price.replace("/month", "").translate(translate_table_price))

    #Remove the % of commissions of the Agency fees
    df["Agency_fees"] = df["Agency_fees"].apply(lambda fees: pd.NA if pd.notna(fees) and "%" in fees else fees)

    df["Bathroom"] = df["Bathroom"].apply(lambda bathroom: 4 if pd.notna(bathroom) and bathroom == "3+" else bathroom)

    clean_deposit(df)

    df["Garages"] = df["Garages"].apply(lambda garage: get_garages_number(garage) if pd.notna(garage) else garage)
    df["Floor_number"] = df["Floor_number"].apply(lambda floor_number: get_floor_number(floor_number) if pd.notna(floor_number) else floor_number)
    df["District"] = df["District"].apply(lambda district: get_district(district) if pd.notna(district) else district)
    df["Is_furnished"] = df["Is_furnished"].map({"Yes" : "Oui", "No" : "Non", "Only Kitchen Furnished" : "Non", "Partially furnished" : "Oui"})
    df["Heating"] = df["Heating"].map({
        "Independent, powered by heat pump" : "Heat pump",
        "Independent, gas powered" : "Gas",
        "Independent, power supply" : "Electric",
        "Independent, powered by gas oil" : "Gasoil",
        "Independent, powered by pellets" : "Pellets"
    })
    
    df.drop(df[df.Type == "Building"].index, inplace=True)
    df.drop(columns=["Rental guarantee", "Condominium_fees"], inplace=True)
    df.dropna(subset=["Surface", "Price"], inplace=True)

    df.to_csv(f"{airflow_home}/dags/data/cleaned/immotop_lu_{today}.csv", index=False)

def athome_lu_data_cleaning():
    today = str(date.today())
    airflow_home = os.environ["AIRFLOW_HOME"]

    df = pd.read_csv(
        f"{airflow_home}/dags/data/raw/athome_last3d_2024-12-14.csv",
        dtype={
            "Monthly_charges" : "Int64",
            "Deposit" : "Int64",
            "Floor_number" : "Int64",
            "Bedrooms" : "Int64",
            "Bathroom" : "Int64",
            "Garages" : "Int64"})

    lines_before_duplicates_removal = len(df)

    #Drop duplicated rows
    df = df.drop_duplicates(subset=["Link"])

    lines_after_duplicates_removal = len(df)

    logging.info(f"{lines_before_duplicates_removal - lines_after_duplicates_removal} duplicates have been removed")

    #Introduction of a new column that will be used to identify the duplicates later on
    #1 is the default value (= no other duplicates)
    df["Duplicate_rank"] = 1
    
    treat_athome_duplicates(df)

    df["Heating"] = df.apply(get_heating_athome, axis=1)
    df.drop(columns=["Has_electric_heating", "Has_gas_heating"], inplace=True)

    df.to_csv(f"{airflow_home}/dags/data/cleaned/athome_last3d_2024-12-14.csv", index=False)

# athome_lu_data_cleaning()