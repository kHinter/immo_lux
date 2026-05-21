from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Variable

import sys
import os
import pendulum
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
from include.data_quality import verify_dq

def merge_dataframes(ds):

    #Retrieve all df
    df_athome = pd.read_csv(
        f"{Variable.get('immo_lux_data_folder')}/enriched/athome_{ds}.csv",
        dtype={
            "Deposit" : "Int64",
            "Floor_number" : "Int64",
            "Bedrooms" : "Int64",
            "Bathroom" : "Int64",
            "Garages" : "Int64",
            "Street_number" : "object"})

    df_immotop = pd.read_csv(f"{Variable.get('immo_lux_data_folder')}/enriched/immotop_lu_{ds}.csv", dtype={
            "Deposit" : "Int64",
            "Floor_number" : "Int64",
            "Bedrooms" : "Int64",
            "Bathroom" : "Int64",
            "Garages" : "Int64",
            "Street_number" : "object"})

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

def verify_no_data_loss_after_data_enrichment(ds, filename_prefix):
    #Retrieve all df before enrichment
    df_data_cleaning = pd.read_csv(f"{Variable.get('immo_lux_data_folder')}/cleaned/{filename_prefix}_{ds}.csv")
    df_data_enrichment = pd.read_csv(f"{Variable.get('immo_lux_data_folder')}/enriched/{filename_prefix}_{ds}.csv")

    for column in df_data_enrichment.columns:
        if column in df_data_cleaning.columns:
            non_na_values_count_data_cleaning = df_data_cleaning[column].count()
            non_na_values_count_data_enrichment = df_data_enrichment[column].count()
            if non_na_values_count_data_enrichment < non_na_values_count_data_cleaning:
                delta = non_na_values_count_data_cleaning - non_na_values_count_data_enrichment
                raise RuntimeError(f"Data loss: After data enrichment, the column {column} has {delta} fewer non-NA values compared to before.")

default_args = {
    "owner" : "airflow",
    'depends_on_past' : False,
    'start_date' : pendulum.today("UTC").add(days=-1),
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
    schedule='1 0 * * *',
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

    verify_no_data_loss_after_athome_lu_data_enrichment = PythonOperator(
        task_id = "verify_no_data_loss_after_athome_lu_data_enrichment",
        python_callable=verify_no_data_loss_after_data_enrichment,
        op_kwargs={"filename_prefix" : "athome"}
    )

    verify_no_data_loss_after_immotop_lu_data_enrichment = PythonOperator(
        task_id = "verify_no_data_loss_after_immotop_lu_data_enrichment",
        python_callable=verify_no_data_loss_after_data_enrichment,
        op_kwargs={"filename_prefix" : "immotop_lu"}
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

    gx_dq_validation = PythonOperator(
        task_id = "gx_dq_validation",
        python_callable=verify_dq,
    )
    
    extract_data_from_athome_lu >> transform_data_from_athome_lu >> athome_lu_data_enrichment >> verify_no_data_loss_after_athome_lu_data_enrichment
    extract_data_from_immotop_lu >> transform_data_from_immotop_lu >> immotop_lu_data_enrichment >> verify_no_data_loss_after_immotop_lu_data_enrichment

    verify_no_data_loss_after_athome_lu_data_enrichment >> merge_df
    verify_no_data_loss_after_immotop_lu_data_enrichment >> merge_df

    merge_df >> verify_columns_are_in_gx_dq_suite >> gx_dq_validation

    gx_dq_validation >> treat_duplicates
    gx_dq_validation >> gen_report