import pandas as pd
from datetime import date
import re
from langchain_ollama import OllamaLLM
from langchain_core.prompts import ChatPromptTemplate
import os

surface_trans_table = str.maketrans("", "", "sqm²(+/-")

##REGEX
monthly_charges_reg = re.compile("((?<=Monthly charges: )€?\d+(?:\.?,?\d+)?)|((?<=Charges mensuelles: )€?\d+(?:\.?,?\d+)?)|((?<=Charges: )€?\d+(?:\.?,?\d+)?)", re.IGNORECASE)

# deposit_in_months_reg = re.compile("(?<=Deposit) *: +\d+ +(?=month)", re.IGNORECASE)
# no_deposit_reg = re.compile("No deposit")
# deposit_reg = re.compile("(?<=Deposit amount) *: \d+(?:\.?,?\d+)? +(?=€)", re.IGNORECASE)

balcony_surface_reg = re.compile(
    "(?<=balcon de )\d+(?:\.?,?\d+)? ?m(?:2|²)|(?<=balcon de \+/- )\d+(?:\.?,?\d+)? ?m(?:2|²)|(?<=balcon de ±) *\d+(?:\.?,?\d+)? *m *(?:2|²)"
    "|(?<=balcony of) *\d+(?:\.?,?\d+)? *(?:m *(?:2|²)|sqm)|(?<=balcony of ±) *\d+(?:\.?,?\d+)? *(?:m *(?:2|²)|sqm)|(?<=balcony of \+/-) *\d+(?:\.?,?\d+)? *(?:m *(?:2|²)|sqm)"
    "|\d+(?:\.?,?\d+)? *(?:m *(?:2|²)|sqm) *(?=balcony)|(?<=balcony) *\((?:\+/-)? *\d+(?:\.?,?\d+)? *(?:m *(?:2|²)|sqm)|(?<=balcony) *\d+(?:\.?,?\d+)? *(?:m *(?:2|²)|sqm)", re.IGNORECASE)

garden_surface_reg = re.compile(
    "\d+(?:\.?,?\d+)? *(?=m2 private garden)|(?<=garden of \+/-) *\d+(?:\.?,?\d+)? *(?:m *(?:2|²)|sqm)|(?<=garden of) *\d+(?:\.?,?\d+)? *(?:m *(?:2|²)|sqm)"
    "|(?<=garden) *\((?:\+/-)? *\d+(?:\.?,?\d+)? *(?:m *(?:2|²)|sqm)|\d+(?:\.?,?\d+)? *(?:m *(?:2|²)|sqm) *(?=garden)|(?<=jardin de )\d+(?:\.?,?\d+)? ?m(?:2|²)"
    "|(?<=jardin de \+/- )\d+(?:\.?,?\d+)? *m(?:2|²)|(?<=jardin) *\((?:\+/-)? *\d+(?:\.?,?\d+)? *m *(?:2|²)", re.IGNORECASE
)

#Terrace examples:
#+/- 25 m2 terrace
#terrace of

has_cellar_reg = re.compile("(no +cellar|pas +de +cave)|(cellar|cave)", re.IGNORECASE)
has_garden_reg = re.compile("(no +garden)|(garden|jardin)", re.IGNORECASE)
is_flat_reg = re.compile("flat |shared (?:accommodation|apartment)|colocation|maison partagée", re.IGNORECASE)
has_balcony_reg = re.compile("balcony|balcon", re.IGNORECASE)
is_furnished_reg = re.compile("(non-? *meublé|unfurnished)|(meublé|furnished(?!.lu))", re.IGNORECASE)
has_lift_reg = re.compile("(without +lift|without +elevator|sans +ascenseur)|((?:with a?|and) lift|ascenseur(?! du))", re.IGNORECASE)
no_agency_fees_reg = re.compile("No agency fees", re.IGNORECASE)

###MODEL
model = OllamaLLM(model="llama3.2:3b")

###PROMPTS
deposit_prompt_template = """
    Based on the following property description, determine me (if any) the amount of agency fees. If there is no agency fees, just answer "No fees", otherwise give me directly the amount of the agency fees without adding additional text. :

    {description}
"""
deposit_chain = ChatPromptTemplate.from_template(deposit_prompt_template) | model

def get_monthly_charge_from_desc(description):
    matches = monthly_charges_reg.findall(description)
    if len(matches) == 0:
        return None
    return next(element.replace("€", "") for element in matches[0] if element != "")

def get_has_cellar_from_desc(description):
    match = has_cellar_reg.search(description)
    if match:
        if match.group(1) != None:
            return "Non"
        elif match.group(2) != "":
            return "Oui"
    return None

def get_has_garden_from_desc(description):
    match = has_garden_reg.search(description)
    if match:
        if match.group(1) != None:
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
        return match.group(0).strip()#.replace("m2", "").translate(surface_trans_table).replace(" 2", "").replace(",", ".")
    return None

def get_agency_fees_from_desc(description):
    no_agency_fees_match = no_agency_fees_reg.search(description)
    if no_agency_fees_match:
        return 0

def immotop_lu_enrichment():
    df = pd.read_csv("/usr/local/airflow/dags/data/cleaned/immotop_lu.csv")
    #df = pd.read_csv("./dags/data/cleaned/immotop_lu.csv")

    #Determine other attributes based on description
    df["Monthly_charges"] = df["Description"].apply(lambda description: get_monthly_charge_from_desc(description) if pd.notna(description) else pd.NA)

    #Surface columns
    df["Balcony_surface"] = df["Description"].apply(lambda description: get_balcony_surface_from_desc(description) if pd.notna(description) else pd.NA)
    df["Garden_surface"] = df["Description"].apply(lambda description: get_garden_surface_from_desc(description) if pd.notna(description) else pd.NA)

    df["Has_cellar"] = df["Description"].apply(lambda description: get_has_cellar_from_desc(description) if pd.notna(description) else pd.NA)
    df["Has_garden"] = df["Description"].apply(lambda description: get_has_garden_from_desc(description) if pd.notna(description) else pd.NA)
    df.loc[df["Has_balcony"].isna(), "Has_balcony"] = df.loc[df["Has_balcony"].isna(), "Description"].apply(lambda description: get_has_balcony_from_desc(description) if pd.notna(description) else pd.NA)
    df.loc[df["Has_lift"].isna(), "Has_lift"] = df.loc[df["Has_lift"].isna(), "Description"].apply(lambda description: get_has_lift_from_desc(description) if pd.notna(description) else pd.NA)
    df.loc[df["Is_furnished"].isna(), "Is_furnished"] = df.loc[df["Is_furnished"].isna(), "Description"].apply(lambda description: get_is_furnished_from_desc(description) if pd.notna(description) else pd.NA)
    df.loc[df["Agency_fees"].isna(), "Agency_fees"] = df.loc[df["Agency_fees"].isna(), "Description"].apply(lambda description: get_agency_fees_from_desc(description) if pd.notna(description) else pd.NA)
    df["Is_flat"] = df["Description"].apply(lambda description: get_is_flat_from_desc(description) if pd.notna(description) else pd.NA)

    df.to_csv("/usr/local/airflow/dags/data/enriched/immotop_lu.csv", index=False)
    #df.to_csv("./dags/data/enriched/immotop_lu.csv", index=False)

def athome_lu_enrichment():
    df = pd.read_csv(
        "/usr/local/airflow/dags/data/cleaned/athome_last3d_" + str(date.today()) + ".csv",
        dtype={
            "Monthly_charges" : "Int64",
            "Deposit" : "Int64",
            "Floor_number" : "Int64",
            "Bedrooms" : "Int64",
            "Bathroom" : "Int64",
            "Garages" : "Int64"})

    # df = pd.read_csv(
    #     "./data/cleaned/athome_last3d_" + str(date.today()) + ".csv",
    #     dtype={
    #         "Monthly_charges" : "Int64",
    #         "Deposit" : "Int64",
    #         "Floor_number" : "Int64",
    #         "Bedrooms" : "Int64",
    #         "Bathroom" : "Int64",
    #         "Garages" : "Int64"})

    #Determine other attributes based on description
    df["Is_flat"] = df["Description"].apply(lambda description: get_is_flat_from_desc(description) if pd.notna(description) else pd.NA)
    df["Is_furnished"] = df["Description"].apply(lambda description: get_is_furnished_from_desc(description) if pd.notna(description) else pd.NA)
    df.loc[df["Has_lift"].isna(), "Has_lift"] = df.loc[df["Has_lift"].isna(), "Description"].apply(lambda description: get_has_lift_from_desc(description) if pd.notna(description) else pd.NA)
    df.loc[df["Has_balcony"].isna(), "Has_balcony"] = df.loc[df["Has_balcony"].isna(), "Description"].apply(lambda description: get_has_balcony_from_desc(description) if pd.notna(description) else pd.NA)
    df.loc[df["Balcony_surface"].isna(), "Balcony_surface"] = df.loc[df["Balcony_surface"].isna(), "Description"].apply(lambda description: get_balcony_surface_from_desc(description) if pd.notna(description) else pd.NA)
    df.loc[df["Garden_surface"].isna(), "Garden_surface"] = df.loc[df["Garden_surface"].isna(), "Description"].apply(lambda description: get_garden_surface_from_desc(description) if pd.notna(description) else pd.NA)
    #TODO complete with terrace_surface

    #Fill the Has_garden, Has_terrace, Has_balcony columns if their surface is not NA
    df.loc[df["Has_balcony"].isna(), "Has_balcony"] = df.loc[df["Has_balcony"].isna(), "Balcony_surface"].apply(lambda surface: "Oui" if pd.notna(surface) else pd.NA)
    df.loc[df["Has_terrace"].isna(), "Has_terrace"] = df.loc[df["Has_terrace"].isna(), "Terrace_surface"].apply(lambda surface: "Oui" if pd.notna(surface) else pd.NA)
    df.loc[df["Has_garden"].isna(), "Has_garden"] = df.loc[df["Has_garden"].isna(), "Garden_surface"].apply(lambda surface: "Oui" if pd.notna(surface) else pd.NA)

    df.to_csv("/usr/local/airflow/dags/data/enriched/athome_last3d_" + str(date.today()) + ".csv", index=False)
    # df.to_csv("./data/enriched/athome_last3d_" + str(date.today()) + ".csv", index=False)

immotop_lu_enrichment()