import pandas as pd
import re
import logging
from unidecode import unidecode
from airflow.models import Variable

##REGEX
deposit_amount_reg = re.compile("((?<=Deposit amount: )€?\d+(?:\.\d+)?)|((?<=Montant caution: )€?\d+(?:\.\d+)?)", re.IGNORECASE)
street_number_reg = re.compile("^\d+(?:-\d+)*(?:[a-zA-Z](?= |,))?,?")

##TRANSLATE TABLES
translate_table_price = str.maketrans("", "", "€ ,")

street_abbreviations = {
    "rte" : "route",
    "bd." : "boulevard",
    "bld" : "boulevard",
    "av." : "avenue",
    "av" : "avenue"
}

#Official dataset and SoT of the Luxembourgish government for street names
df_streets_sot = pd.read_excel(f"{Variable.get('immo_lux_data_folder')}/caclr.xlsx", sheet_name="RUE")
streets_validity = {}

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
    
def get_street_name_and_number(row):
    if pd.notna(row["Address"]):
        row["Address"] = row["Address"].lstrip()
        match = street_number_reg.search(row["Address"])
        if match:
            row["Street_number"] = match.group().replace(",", "")
            row["Street_name"] = row["Address"][match.end():].split(",")[0]
        else:
            row["Street_name"] = row["Address"].split(",")[0]

            if row["Street_name"] == "":
                row["Street_name"] = pd.NA
                row["Street_number"] = pd.NA

                return row
        
        #Normalization of street names
        row["Street_name"] = unidecode(row["Street_name"].strip().lower())
        row["Street_name"] = " ".join([street_abbreviations.get(word, word) for word in row["Street_name"].split()])
    return row

#Will be used later during duplicate treatment to eliminate potential candidates for image similarity comparison
def get_street_name_validity(street_name):
    if street_name in streets_validity:
        return streets_validity[street_name]
    else:
        match = df_streets_sot.loc[df_streets_sot["NOM_MAJUSUCLE"] == street_name.upper(), "NOM_MAJUSUCLE"]
        if match.empty:
            streets_validity[street_name] = False
            return pd.NA
        else:
            streets_validity[street_name] = True
            return "Oui"

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

def immotop_lu_data_cleaning(ds):
    df = pd.read_csv(f"{Variable.get('immo_lux_data_folder')}/raw/immotop_lu_{ds}.csv", dtype={"Bedrooms" : "Int64"})

    df.dropna(subset=["Surface", "Price", "Photos"], inplace=True)
    
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
    
    #Add a comma at the end so the function get_street_name_and_number can work properly
    df["Address"] = df["Address"].apply(lambda address: address + "," if pd.notnull(address) else address)

    #Determine the street name and/or street number
    df = df.apply(get_street_name_and_number, axis=1)

    df["Steert_name_validity"] = df["Street_name"].apply(lambda street_name: get_street_name_validity(street_name) if pd.notna(street_name) else pd.NA)

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
    df["Heating"] = df["Heating"].replace({
        "Independent, powered by heat pump" : "Heat pump",
        "Independent, gas powered" : "Gas",
        "Independent, power supply" : "Electric",
        "Independent, powered by gas oil" : "Gasoil",
        "Independent, powered by pellets" : "Pellets"
    })

    #Use replace instead of map to avoid replacing values not present in the dict by NaN
    df["District"] = df["District"].replace({
        "Gasperich-Cloche d’or" : "Gasperich",
        "Bonnevoie-Verlorenkost" : "Bonnevoie"
    })
    
    df.drop(df[df.Type == "Building"].index, inplace=True)
    df.drop(columns=["Rental guarantee", "Condominium_fees"], inplace=True)

    lines_before_duplicates_removal = len(df)
    df.drop_duplicates(subset=["Link"], inplace=True)
    lines_after_duplicates_removal = len(df)

    logging.info(f"{lines_before_duplicates_removal - lines_after_duplicates_removal} duplicates have been removed")

    df.drop(columns=["Address"], inplace=True)

    df.to_csv(f"{Variable.get('immo_lux_data_folder')}/cleaned/immotop_lu_{ds}.csv", index=False)

def athome_lu_data_cleaning(ds):
    df = pd.read_csv(
        f"{Variable.get('immo_lux_data_folder')}/raw/athome_last3d_{ds}.csv",
        dtype={
            "Monthly_charges" : "Int64",
            "Deposit" : "Int64",
            "Floor_number" : "Int64",
            "Bedrooms" : "Int64",
            "Bathroom" : "Int64",
            "Garages" : "Int64",
            #Convert surface into object in order to be able to replace the "," by "."
            "Surface" : "object"})
    
    df.dropna(subset=["Surface", "Price", "Photos"], inplace=True)

    df["Heating"] = df.apply(get_heating_athome, axis=1)
    df["City"] = df["City"].apply(lambda city : city.strip())
    df.drop(columns=["Has_electric_heating", "Has_gas_heating"], inplace=True)

    df["Surface"] = df["Surface"].apply(lambda surface : surface.replace(",", "."))

    #Determine the street name and/or street number
    df = df.apply(get_street_name_and_number, axis=1)

    df["Steert_name_validity"] = df["Street_name"].apply(lambda street_name: get_street_name_validity(street_name) if pd.notna(street_name) else pd.NA)

    df["District"] = df["District"].replace({
        "Neudorf" : "Neudorf-Weimershof",
        "Weimershof" : "Neudorf-Weimershof",
        "Pulvermuehle" : "Pulvermuhl"
    })

    #Replace "Centre ville" values by NA because they are not reliable (don't always reflect the real district)
    df["District"] = df["District"].replace("Centre ville", pd.NA)

    lines_before_duplicates_removal = len(df)
    df = df.drop_duplicates(subset=["Link"])
    lines_after_duplicates_removal = len(df)
    
    logging.info(f"{lines_before_duplicates_removal - lines_after_duplicates_removal} duplicates have been removed")

    df.drop(columns=["Address"], inplace=True)

    df.to_csv(f"{Variable.get('immo_lux_data_folder')}/cleaned/athome_last3d_{ds}.csv", index=False)

# athome_lu_data_cleaning("2025-01-31")
# immotop_lu_data_cleaning("2025-01-29")