import pandas as pd
from datetime import date
from google.oauth2 import service_account
from pandas_gbq import to_gbq

#Load each dataframe
df_immotop = pd.read_csv("./data/cleaned/immotop_lu.csv")
df_athome = pd.read_csv("./data/enriched/athome_last3d_" + str(date.today()) + ".csv")

#df = pd.concat([df_athome, df_immotop])

#df.to_csv("./data/collapsed.csv")

#Initialization of Google BigQuery connection
# credentials = service_account.Credentials.from_service_account_file("lux-immo-438316-afc354278954.json")

# project_id = "lux-immo-438316"
# table_id = "immo_532.accomodations"

# to_gbq(df, table_id, project_id=project_id, if_exists="append", credentials=credentials)