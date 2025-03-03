from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

import sys
import os

#To find the include folder in order to realize the local imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Local includes
from include.extract_data import extract_immotop_lu_data, extract_athome_data
from include.data_cleaning import immotop_lu_data_cleaning, athome_lu_data_cleaning
from include.data_enrichment import immotop_lu_enrichment, athome_lu_enrichment
from include.reports import generate_dq_report
from include.duplicates_treatment import merge_all_df_and_treat_duplicates

if Variable.get("immo_lux_data_folder", default_var=None) == None:
    airflow_home = os.environ["AIRFLOW_HOME"]
    Variable.set("immo_lux_data_folder", f"{airflow_home}/dags/data")

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

    treat_duplicates = PythonOperator(
        task_id = "merge_all_df_and_treat_duplicates",
        python_callable=merge_all_df_and_treat_duplicates
    )

    gen_report = PythonOperator(
        task_id = "generate_dq_report",
        python_callable=generate_dq_report,
    )
    
    extract_data_from_athome_lu >> transform_data_from_athome_lu >> athome_lu_data_enrichment
    extract_data_from_immotop_lu >> transform_data_from_immotop_lu >> immotop_lu_data_enrichment

    athome_lu_data_enrichment >> gen_report
    immotop_lu_data_enrichment >> gen_report

    athome_lu_data_enrichment >> treat_duplicates
    immotop_lu_data_enrichment >> treat_duplicates