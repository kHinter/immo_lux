from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import sys
import os

#To find the include folder in order to realize the local imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Local includes
from include.extract_data import extract_immotop_lu_data, extract_athome_data
from include.data_cleaning import immotop_lu_data_cleaning, athome_lu_data_cleaning
from include.data_enrichment import immotop_lu_enrichment, athome_lu_enrichment
from include.reports import generate_report

default_args = {
    "owner" : "airflow",
    'depends_on_past' : False,
    'start_date' : datetime(2024, 10, 3),
    "email" : "kevin.hautmann@outlook.fr",
    "email_on_failure" : True,
    "email_on_retry" : True,
    "retries" : 1,
    "retry_delay" : timedelta(minutes=2)
}

with DAG(
    "immo_dag",
    default_args=default_args,
    description="Web scraping of several real estate listing websites",
    schedule_interval="0 0 */3 * *"
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

    gen_report = PythonOperator(
        task_id = "generate_report",
        python_callable=generate_report,
    )

    extract_data_from_immotop_lu >> transform_data_from_immotop_lu >> immotop_lu_data_enrichment
    extract_data_from_athome_lu >> transform_data_from_athome_lu >> athome_lu_data_enrichment

    athome_lu_data_enrichment >> gen_report
    immotop_lu_data_enrichment >> gen_report