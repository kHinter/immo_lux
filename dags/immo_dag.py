from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator
)

import sys
import os
from datetime import timedelta
import pandas as pd

#To find the include folder in order to realize the local imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Local includes
from include.extract_data import extract_immotop_lu_data, extract_athome_data
from include.data_cleaning import immotop_lu_data_cleaning, athome_lu_data_cleaning
from include.data_enrichment import immotop_lu_enrichment, athome_lu_enrichment
from include.reports import generate_dq_report
from include.duplicates_treatment import merge_all_df_and_treat_duplicates

def merge_dataframes(ds):
    from airflow.models import Variable

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

    df_immotop = pd.read_csv(f"{Variable.get('immo_lux_data_folder')}/enriched/immotop_lu_{ds}.csv", dtype={
            "Monthly_charges" : "Int64",
            "Deposit" : "Int64",
            "Floor_number" : "Int64",
            "Bedrooms" : "Int64",
            "Bathroom" : "Int64",
            "Garages" : "Int64"})

    #Merge all df into a single one
    df = pd.concat([df_athome, df_immotop])

    df.to_csv(f"{Variable.get('immo_lux_data_folder')}/gx_merged.csv", index=False)

def verify_all_columns_are_in_gx_dq_suite():
    #Get the columns list from merged df
    merged_df = pd.read_csv(f"{Variable.get('immo_lux_data_folder')}/gx_merged.csv")
    #List of columns that are not present in the GreatExpectations suite
    missing_columns = merged_df.columns.tolist()

    import json
    with open("gx/expectations/preload_dq_suite.json", "r") as f:
        json_suite = json.load(f)

        for expectation in json_suite["expectations"]:
            column_name = expectation["kwargs"]["column"]
            if column_name in missing_columns:
                missing_columns.remove(column_name)

    if len(missing_columns) != 0:
        raise RuntimeError(f"The following columns are not present in the GreatExpectations suite : {missing_columns}")

if Variable.get("immo_lux_data_folder", default_var=None) is None:
    airflow_home = os.environ["AIRFLOW_HOME"]
    Variable.set("immo_lux_data_folder", f"{airflow_home}/dags/data")

#Creation of the csv file consummed by the GreatExpectationsOperator
if not os.path.exists(f"{Variable.get('immo_lux_data_folder')}/gx_merged.csv"):
    with open(f"{Variable.get('immo_lux_data_folder')}/gx_merged.csv", "w") as f:
        #Write a dummy header just to avoid that the DAG parsing fails when the GreatExpectationsOperator is evaluated
        f.write("column1,column2")
        f.close()

default_args = {
    "owner" : "airflow",
    'depends_on_past' : False,
    'start_date' : days_ago(1),
    "email" : Variable.get("email"),
    "email_on_failure" : True,
    "email_on_retry" : True,
    "retries" : 1,
    "retry_delay" : timedelta(minutes=1)
}

with DAG(
    "immo_dag",
    default_args=default_args,
    description="Web scraping of several real estate listing websites",
    schedule='@daily',
    catchup=False
) as dag:
    extract_data_from_immotop_lu = PythonOperator(
        task_id = "extract_data_from_immotop_lu",
        python_callable=extract_immotop_lu_data
    )

    extract_data_from_athome_lu = PythonOperator(
            task_id = "extract_data_from_athome_lu",
            python_callable=extract_athome_data
    )

    transform_data_from_immotop_lu = PythonOperator(
        task_id = "transform_data_from_immotop_lu",
        python_callable=immotop_lu_data_cleaning
    )

    transform_data_from_athome_lu = PythonOperator(
        task_id = "transform_data_from_athome_lu",
        python_callable=athome_lu_data_cleaning
    )

    immotop_lu_data_enrichment = PythonOperator(
        task_id = "immotop_lu_data_enrichment",
        python_callable=immotop_lu_enrichment
    )

    athome_lu_data_enrichment = PythonOperator(
        task_id = "athome_lu_data_enrichment",
        python_callable=athome_lu_enrichment
    )

    merge_df = PythonOperator(
        task_id = "merge_dataframes",
        python_callable=merge_dataframes
    )

    verify_columns_are_in_gx_dq_suite = PythonOperator(
        task_id = "verify_all_columns_are_in_gx_dq_suite",
        python_callable=verify_all_columns_are_in_gx_dq_suite
    )

    treat_duplicates = PythonOperator(
        task_id = "merge_all_df_and_treat_duplicates",
        python_callable=merge_all_df_and_treat_duplicates
    )

    gen_report = PythonOperator(
        task_id = "generate_dq_report",
        python_callable=generate_dq_report,
    )

    gx_dq_validation = GreatExpectationsOperator(
        task_id = "gx_dq_validation",
        data_context_root_dir="gx",
        #Can't use a dynamic path for the dataframe (e.g : merged_2024-12-01.csv) because this Operator is evaluated during DAG parsing
        dataframe_to_validate=pd.read_csv(f"{Variable.get('immo_lux_data_folder')}/gx_merged.csv", dtype={
            "Floor_number" : "Int64",
            "Bedrooms" : "Int64",
            "Bathroom" : "Int64",
            "Garages" : "Int64",
            "Shower_room" : "Int64"}),
        execution_engine="PandasExecutionEngine",
        data_asset_name="merged_data",
        expectation_suite_name="preload_dq_suite",
        return_json_dict=True
    )
    
    extract_data_from_athome_lu >> transform_data_from_athome_lu >> athome_lu_data_enrichment
    extract_data_from_immotop_lu >> transform_data_from_immotop_lu >> immotop_lu_data_enrichment

    athome_lu_data_enrichment >> merge_df
    immotop_lu_data_enrichment >> merge_df

    merge_df >> verify_columns_are_in_gx_dq_suite >> gx_dq_validation

    gx_dq_validation >> treat_duplicates
    gx_dq_validation >> gen_report