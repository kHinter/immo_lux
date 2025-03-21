import pandas as pd
import re
# from langchain_ollama import OllamaLLM
# from langchain_core.prompts import ChatPromptTemplate
from airflow.models import Variable
from . import utils

surface_trans_table = str.maketrans("", "", "sqm²(+/-")
price_trans_table = str.maketrans("", "", "€")

##REGEX
monthly_charges_reg = re.compile("(?<=Monthly charges: )€?\d+(?:\.?,?\d+)?|(?<=Charges mensuelles: )€?\d+(?:\.?,?\d+)?", re.IGNORECASE)

# deposit_in_months_reg = re.compile("(?<=Deposit) *: +\d+ +(?=month)", re.IGNORECASE)
# no_deposit_reg = re.compile("No deposit")
# deposit_reg = re.compile("(?<=Deposit amount) *: \d+(?:\.?,?\d+)? +(?=€)", re.IGNORECASE)

balcony_surface_reg = re.compile(
    "(?<=balcon de )\d+(?:\.?,?\d+)? ?m(?:2|²)|(?<=balcon de \+/- )\d+(?:\.?,?\d+)? ?m(?:2|²)|(?<=balcon de ±) *\d+(?:\.?,?\d+)? *m *(?:2|²)"
    "|(?<=balcony of) *\d+(?:\.?,?\d+)? *(?:m *(?:2|²)|sqm)|(?<=balcony of ±) *\d+(?:\.?,?\d+)? *(?:m *(?:2|²)|sqm)|(?<=balcony of \+/-) *\d+(?:\.?,?\d+)? *(?:m *(?:2|²)|sqm)"
    "|\d+(?:\.?,?\d+)? *(?:m *(?:2|²)|sqm) *(?=balcony)|(?<=balcony) *\((?:\+/-)? *\d+(?:\.?,?\d+)? *(?:m *(?:2|²)|sqm)|(?<=balcony) *\d+(?:\.?,?\d+)? *(?:m *(?:2|²)|sqm)"
    , re.IGNORECASE
)

garden_surface_reg = re.compile(
    "\d+(?:\.?,?\d+)? *(?=m2 private garden)|(?<=garden of \+/-) *\d+(?:\.?,?\d+)? *(?:m *(?:2|²)|sqm)|(?<=garden of) *\d+(?:\.?,?\d+)? *(?:m *(?:2|²)|sqm)"
    "|(?<=garden) *\((?:\+/-)? *\d+(?:\.?,?\d+)? *(?:m *(?:2|²)|sqm)|\d+(?:\.?,?\d+)? *(?:m *(?:2|²)|sqm) *(?=garden)|(?<=jardin de )\d+(?:\.?,?\d+)? ?m(?:2|²)"
    "|(?<=jardin de \+/- )\d+(?:\.?,?\d+)? *m(?:2|²)|(?<=jardin) *\((?:\+/-)? *\d+(?:\.?,?\d+)? *m *(?:2|²)"
    , re.IGNORECASE
)

terrace_surface_reg = re.compile(
    "\d+(?:\.?,?\d+)? *(?:m *(?:2|²)|sqm) *(?=terrace)|(?<=terrace of )\d+(?:\.?,?\d+)? *(?:m *(?:2|²)|sqm)|(?<=terrace of \+/-) *\d+(?:\.?,?\d+)? *(?:m *(?:2|²)|sqm)"
    "|(?<=terrace of ±) *\d+(?:\.?,?\d+)? *(?:m *(?:2|²)|sqm)|(?<=terrace) *\((?:\+/-)? *\d+(?:\.?,?\d+)? *(?:m *(?:2|²)|sqm)|(?<=terrasse de )\d+(?:\.?,?\d+)? ?m(?:2|²)"
    "|(?<=terrasse de \+/- )\d+(?:\.?,?\d+)? *m(?:2|²)|(?<=terrasse de ±)\d+(?:\.?,?\d+)? *m(?:2|²)|(?<=terrasse) *\((?:\+/-)? *\d+(?:\.?,?\d+)? *m *(?:2|²)"
    , re.IGNORECASE
)

exposition_reg = re.compile(
    "south-west(?= facing)|south-west(?=-facing)|(?<=facing )south-west|south-west (?=orientat)|(?<=facing )south-west|(?<=exposé )sud(?:-| )?ouest|(?<=exposée )sud(?:-| )?ouest|(?<=orienté )sud(?:-| )?ouest|(?<=orientée )sud(?:-| )?ouest" #South-west
    "|south-east(?= facing)|south-east(?=-facing)|(?<=facing )south-east|south-east (?=orientat)|(?<=facing )south-east|(?<=exposé )sud(?:-| )?est|(?<=exposée )sud(?:-| )?est|(?<=orienté )sud(?:-| )?est|(?<=orientée )sud(?:-| )?est" #South-east
    "|south(?=-facing)|(?<=facing )south(?!-)|south(?=ern exposure)|(?<=orienté )sud(?!-)|(?<=orientée )sud(?!-)|(?<=orienté plein )sud|(?<=orientée plein )sud|(?<=exposé )sud(?!-)|(?<=exposée )sud(?!-)|(?<=exposition )sud|(?<=exposition plein )sud|(?<=exposée plein )sud|(?<=exposé plein )sud" #South
    "|north-west(?= facing)|north-west(?=-facing)|(?<=facing )north-west|north-west (?=orientat)|(?<=facing )north-west|(?<=exposé )nord(?:-| )?ouest|(?<=exposée )nord(?:-| )?ouest|(?<=orienté )nord(?:-| )?ouest|(?<=orientée )nord(?:-| )?ouest" #North-west
    "|north-east(?= facing)|north-east(?=-facing)|(?<=facing )north-east|north-east (?=orientat)|(?<=facing )north-east|(?<=exposé )nord(?:-| )?est|(?<=exposée )nord(?:-| )?est|(?<=orienté )nord(?:-| )?est|(?<=orientée )nord(?:-| )?est" #North-east
    "|(?<!-)(?<!h)west(?=-facing)|(?<!-)(?<!h)west(?= facing)|(?<=facing )west|(?<=orientation )ouest|(?<=exposition )ouest" #West
    "|(?<!-)(?<!h)east(?=-facing)|(?<!-)(?<!h)east(?= facing)|(?<=facing )east|(?<=orientation )est|(?<=exposition )est" #East
    "|north(?=-facing)|(?<=facing )north(?!-)|north(?=ern exposure)|(?<=orienté )nord(?!-)|(?<=orientée )nord(?!-)|(?<=orienté plein )nord|(?<=orientée plein )nord|(?<=exposé )nord(?!-)|(?<=exposée )nord(?!-)|(?<=exposition )nord|(?<=exposition plein )nord|(?<=exposée plein )nord|(?<=exposé plein )nord" #North
    , re.IGNORECASE 
)

insulation_class_reg = re.compile(
    "(?<=isolation thermique: )[A-G]|(?<=thermal protection class: )[A-G]|(?<=thermal insulation: )[A-G]"
    , re.IGNORECASE
)

energy_class_reg = re.compile(
    "(?<=energy class )[A-G]|(?<=energy class: )[A-G]|(?<=classe énergétique )[A-G]|(?<=classe énergétique : )[A-G]"
    , re.IGNORECASE
)

has_cellar_reg = re.compile("(no +cellar|pas +de +cave)|(cellar|cave)", re.IGNORECASE)
has_garden_reg = re.compile("(no +garden)|(garden|jardin)", re.IGNORECASE)
has_terrace_reg = re.compile("(no +terrace)|(terrace|terrasse)", re.IGNORECASE)
is_flat_reg = re.compile("flat |shared (?:accommodation|apartment)|colocation|maison partagée", re.IGNORECASE)
has_balcony_reg = re.compile("balcony|balcon", re.IGNORECASE)
is_furnished_reg = re.compile("(non-? *meublé|unfurnished)|(meublé|furnished(?!.lu))", re.IGNORECASE)
has_lift_reg = re.compile("(without +lift|without +elevator|sans +ascenseur)|((?:with a?|and) lift|ascenseur(?! du))", re.IGNORECASE)
no_agency_fees_reg = re.compile("No agency fees", re.IGNORECASE)

###MODEL
# model = OllamaLLM(model="llama3.2:3b")

# ###PROMPTS
# deposit_prompt_template = """
#     Based on the following property description, determine me (if any) the amount of agency fees. If there is no agency fees, just answer "No fees", otherwise give me directly the amount of the agency fees without adding additional text. :

#     {description}
# """
# deposit_chain = ChatPromptTemplate.from_template(deposit_prompt_template) | model

def get_monthly_charge_from_desc(description):
    match = monthly_charges_reg.search(description)
    if match:
        return match.group(0).strip().translate(price_trans_table).replace(",", ".")
    return None

def get_has_cellar_from_desc(description):
    match = has_cellar_reg.search(description)
    if match:
        if match.group(1) is not None:
            return "Non"
        elif match.group(2) != "":
            return "Oui"
    return None

def get_has_garden_from_desc(description):
    match = has_garden_reg.search(description)
    if match:
        if match.group(1) is not None:
            return "Non"
        elif match.group(2) != "":
            return "Oui"
    return None

def get_has_terrace_from_desc(description):
    match = has_terrace_reg.search(description)
    if match:
        if match.group(1) is not None:
            return "Non"
        elif match.group(2) != "":
            return "Oui"
    return None
    
def get_is_flat_from_desc(description):
    matches = is_flat_reg.findall(description)
    if len(matches) == 0:
        return None
    return "Oui"
    
def get_has_balcony_from_desc(description):
    matches = has_balcony_reg.findall(description)
    if len(matches) == 0:
        return None
    return "Oui"

def get_has_lift_from_desc(description):
    match = has_lift_reg.search(description)
    if match:
        if match.group(1) != None:
            return "Non"
        elif match.group(2) != "":
            return "Oui"
    return None
    
def get_is_furnished_from_desc(description):
    match = is_furnished_reg.search(description)
    if match:
        if match.group(1) != None:
            return "Non"
        elif match.group(2) != "":
            return "Oui"
    return None

def get_balcony_surface_from_desc(description):
    match = balcony_surface_reg.search(description)
    if match:
        return match.group(0).strip().replace("m2", "").translate(surface_trans_table).replace(" 2", "").replace(",", ".")
    return None

def get_garden_surface_from_desc(description):
    match = garden_surface_reg.search(description)
    if match:
        return match.group(0).strip().replace("m2", "").translate(surface_trans_table).replace(" 2", "").replace(",", ".")
    return None

def get_terrace_surface_from_desc(description):
    match = terrace_surface_reg.search(description)
    if match:
        return match.group(0).strip().replace("m2", "").translate(surface_trans_table).replace(" 2", "").replace(",", ".")
    return None

def get_agency_fees_from_desc(description):
    no_agency_fees_match = no_agency_fees_reg.search(description)
    if no_agency_fees_match:
        return 0

def get_exposition_from_desc(description):
    french_to_english_trans = {
        "sud-ouest": "south-west",
        "sud-est": "south-east",
        "sud": "south",
        "nord-ouest": "north-west",
        "nord-est": "north-east",
        "ouest": "west",
        "est": "east"
    }

    match = exposition_reg.search(description)
    if match:
        exposition = match.group(0).strip().lower().replace(" ", "-")
        return french_to_english_trans.get(exposition, exposition)

    return None

def get_insulation_class_from_desc(description):
    match = insulation_class_reg.search(description)
    if match:
        return match.group(0)
    return None

def get_energy_class_from_desc(description):
    match = energy_class_reg.search(description)
    if match:
        return match.group(0)
    return None

def enrich_surface_related_columns(df):
    if "Balcony_surface" not in df.columns:
        df["Balcony_surface"] = pd.NA
    if "Garden_surface" not in df.columns:
        df["Garden_surface"] = pd.NA
    if "Terrace_surface" not in df.columns:
        df["Terrace_surface"] = pd.NA

    #Surface columns
    df.loc[df["Balcony_surface"].isna(), "Balcony_surface"] = df.loc[df["Balcony_surface"].isna(), "Description"].apply(lambda description: get_balcony_surface_from_desc(description) if pd.notna(description) else pd.NA)
    df.loc[df["Garden_surface"].isna(), "Garden_surface"] = df.loc[df["Garden_surface"].isna(), "Description"].apply(lambda description: get_garden_surface_from_desc(description) if pd.notna(description) else pd.NA)
    df.loc[df["Terrace_surface"].isna(), "Terrace_surface"] = df.loc[df["Terrace_surface"].isna(), "Description"].apply(lambda description: get_terrace_surface_from_desc(description) if pd.notna(description) else pd.NA)

    #Fill the Has_garden, Has_terrace, Has_balcony columns if their surface is not NA
    df.loc[df["Has_balcony"].isna(), "Has_balcony"] = df.loc[df["Has_balcony"].isna(), "Balcony_surface"].apply(lambda surface: "Oui" if pd.notna(surface) else pd.NA)
    df.loc[df["Has_terrace"].isna(), "Has_terrace"] = df.loc[df["Has_terrace"].isna(), "Terrace_surface"].apply(lambda surface: "Oui" if pd.notna(surface) else pd.NA)
    df.loc[df["Has_garden"].isna(), "Has_garden"] = df.loc[df["Has_garden"].isna(), "Garden_surface"].apply(lambda surface: "Oui" if pd.notna(surface) else pd.NA)

    return df

def immotop_lu_enrichment(ds):
    df = pd.read_csv(f"{Variable.get('immo_lux_data_folder')}/cleaned/immotop_lu_{ds}.csv",
    dtype={
            "Floor_number" : "Int64",
            "Bedrooms" : "Int64",
            "Bathroom" : "Int64",
            "Garages" : "Int64"})

    #Determine other attributes based on description
    df["Monthly_charges"] = df["Description"].apply(lambda description: get_monthly_charge_from_desc(description) if pd.notna(description) else pd.NA)
    df["Is_flat"] = df["Description"].apply(lambda description: get_is_flat_from_desc(description) if pd.notna(description) else pd.NA)
    df["Exposition"] = df["Description"].apply(lambda description: get_exposition_from_desc(description) if pd.notna(description) else pd.NA)
    df["Insulation_class"] = df["Description"].apply(lambda description: get_insulation_class_from_desc(description) if pd.notna(description) else pd.NA)
    df["Energy_class"] = df["Description"].apply(lambda description: get_energy_class_from_desc(description) if pd.notna(description) else pd.NA)
    df["Has_cellar"] = df["Description"].apply(lambda description: get_has_cellar_from_desc(description) if pd.notna(description) else pd.NA)
    df["Has_garden"] = df["Description"].apply(lambda description: get_has_garden_from_desc(description) if pd.notna(description) else pd.NA)

    df = enrich_surface_related_columns(df)

    df.loc[df["Has_balcony"].isna(), "Has_balcony"] = df.loc[df["Has_balcony"].isna(), "Description"].apply(lambda description: get_has_balcony_from_desc(description) if pd.notna(description) else pd.NA)
    df.loc[df["Has_terrace"].isna(), "Has_terrace"] = df.loc[df["Has_terrace"].isna(), "Description"].apply(lambda description: get_has_terrace_from_desc(description) if pd.notna(description) else pd.NA)

    df.loc[df["Has_lift"].isna(), "Has_lift"] = df.loc[df["Has_lift"].isna(), "Description"].apply(lambda description: get_has_lift_from_desc(description) if pd.notna(description) else pd.NA)
    df.loc[df["Is_furnished"].isna(), "Is_furnished"] = df.loc[df["Is_furnished"].isna(), "Description"].apply(lambda description: get_is_furnished_from_desc(description) if pd.notna(description) else pd.NA)
    df.loc[df["Agency_fees"].isna(), "Agency_fees"] = df.loc[df["Agency_fees"].isna(), "Description"].apply(lambda description: get_agency_fees_from_desc(description) if pd.notna(description) else pd.NA)

    utils.create_data_related_folder_if_not_exists("enriched")
    df.to_csv(f"{Variable.get('immo_lux_data_folder')}/enriched/immotop_lu_{ds}.csv", index=False)

def athome_lu_enrichment(ds):
    df = pd.read_csv(
        f"{Variable.get('immo_lux_data_folder')}/cleaned/athome_last3d_{ds}.csv",
        dtype={
            "Floor_number" : "Int64",
            "Bedrooms" : "Int64",
            "Bathroom" : "Int64",
            "Garages" : "Int64"})

    #Determine other attributes based on description
    df["Is_flat"] = df["Description"].apply(lambda description: get_is_flat_from_desc(description) if pd.notna(description) else pd.NA)

    df.loc[df["Exposition"].isna(), "Exposition"] = df["Description"].apply(lambda description: get_exposition_from_desc(description) if pd.notna(description) else pd.NA)
    df.loc[df["Energy_class"].isna(), "Energy_class"] = df["Description"].apply(lambda description: get_energy_class_from_desc(description) if pd.notna(description) else pd.NA)

    df.loc[df["Is_furnished"].isna(), "Is_furnished"] = df.loc[df["Is_furnished"].isna(), "Description"].apply(lambda description: get_is_furnished_from_desc(description) if pd.notna(description) else pd.NA)
    df.loc[df["Has_lift"].isna(), "Has_lift"] = df.loc[df["Has_lift"].isna(), "Description"].apply(lambda description: get_has_lift_from_desc(description) if pd.notna(description) else pd.NA)

    df = enrich_surface_related_columns(df)

    df.loc[df["Has_balcony"].isna(), "Has_balcony"] = df.loc[df["Has_balcony"].isna(), "Description"].apply(lambda description: get_has_balcony_from_desc(description) if pd.notna(description) else pd.NA)
    df.loc[df["Has_garden"].isna(), "Has_garden"] = df.loc[df["Has_garden"].isna(), "Description"].apply(lambda description: get_has_garden_from_desc(description) if pd.notna(description) else pd.NA)
    df.loc[df["Has_cellar"].isna(), "Has_cellar"] = df.loc[df["Has_cellar"].isna(), "Description"].apply(lambda description: get_has_cellar_from_desc(description) if pd.notna(description) else pd.NA)
    df.loc[df["Has_terrace"].isna(), "Has_terrace"] = df.loc[df["Has_terrace"].isna(), "Description"].apply(lambda description: get_has_terrace_from_desc(description) if pd.notna(description) else pd.NA)

    utils.create_data_related_folder_if_not_exists("enriched")
    df.to_csv(f"{Variable.get('immo_lux_data_folder')}/enriched/athome_last3d_{ds}.csv", index=False)

# immotop_lu_enrichment("2025-03-05")
# athome_lu_enrichment("2025-03-02")