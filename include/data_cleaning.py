import urllib.request
import pandas as pd
from datetime import date
import re
import os
import cv2
import urllib
import numpy as np

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
    
def remove_athome_duplicates(df):
    df.sort_values(by="Price")

    i = 0
    latest_exactness_acc_index = 0
    df_len = len(df)

    #Using a while in order to change the incrementation value
    while i < df_len:

        #If no images to compare then skip
        if pd.isna(df.loc[i, "Photos"]):
            continue

        for j in range (i+1, len(df)):
            if df.loc[i, "Price"] != df.loc[j, "Price"]:
                if latest_exactness_acc_index > i:
                    i = latest_exactness_acc_index
                break

            if pd.isna(df.loc[j, "Photos"]):
                continue

            i_photos_url = df.loc[i, "Photos"].split(" ")
            j_photos_url = df.loc[j, "Photos"].split(" ")

            for i_photo_url in i_photos_url:
                for j_photo_url in j_photos_url:
                    #Get the images from url
                    i_photo_request = urllib.request.urlopen(i_photo_url)
                    i_photo = cv2.imdecode(np.asarray(bytearray(i_photo_request.read()), dtype=np.uint8), -1)

                    j_photo_request = urllib.request.urlopen(j_photo_url)
                    j_photo = cv2.imdecode(np.asarray(bytearray(j_photo_request.read()), dtype=np.uint8), -1)

                    hist_i_photo = cv2.calcHist([i_photo], [0, 1, 2], None, [256, 256, 256], [0, 256, 0, 256, 0, 256])
                    hist_i_photo[255, 255, 255] = 0 #Ignore white pixels
                    cv2.normalize(hist_i_photo, hist_i_photo, alpha=0, beta=1, norm_type=cv2.NORM_MINMAX)

                    hist_j_photo = cv2.calcHist([j_photo], [0, 1, 2], None, [256, 256, 256], [0, 256, 0, 256, 0, 256])
                    hist_j_photo[255, 255, 255] = 0 #Ignore white pixels
                    cv2.normalize(hist_j_photo, hist_j_photo, alpha=0, beta=1, norm_type=cv2.NORM_MINMAX)

                    # Find the metric value
                    metric_val = cv2.compareHist(hist_i_photo, hist_j_photo, cv2.HISTCMP_CORREL)
                    # print(f"Similarity Score: ", round(metric_val, 2))

                    exactness_threshold = 0.90

                    if metric_val >= exactness_threshold:
                        df.loc[j, "Duplicate_Rank"] = (j + 1) - i
                        latest_exactness_acc_index = j
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
        f"{airflow_home}/dags/data/raw/athome_last3d_{today}.csv",
        dtype={
            "Monthly_charges" : "Int64",
            "Deposit" : "Int64",
            "Floor_number" : "Int64",
            "Bedrooms" : "Int64",
            "Bathroom" : "Int64",
            "Garages" : "Int64"})

    #Introduction of a new column that will be used to identify the duplicates later on
    #1 is the default value (= no other duplicates)
    df["Duplicate_rank"] = 1
    
    remove_athome_duplicates(df)

    df["Heating"] = df.apply(get_heating_athome, axis=1)
    df.drop(columns=["Has_electric_heating", "Has_gas_heating"], inplace=True)

    df.to_csv(f"{airflow_home}/dags/data/cleaned/athome_last3d_{today}.csv", index=False)

# athome_lu_data_cleaning()