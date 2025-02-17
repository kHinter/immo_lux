from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

import sys
import os

#To find the include folder in order to realize the local imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Local includes
from include.extract_data import extract_immotop_lu_data, extract_athome_data, merge_athome_raw_data_parts
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
    "retry_delay" : timedelta(minutes=2)
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

    #TaskGroup to split the scraping of athome.lu in two parts so they can run parallelly
    with TaskGroup("extract_data_from_athome_lu") as extract_data_from_athome_lu:
        scraping_part1 = PythonOperator(
            task_id = "scraping_part1",
            python_callable=extract_athome_data,
            op_kwargs={
                "part_number" : 1,
                "total_parts" : 2
            }
        )

        scraping_part2 = PythonOperator(
            task_id = "scraping_part2",
            python_callable=extract_athome_data,
            op_kwargs={
                "part_number" : 2,
                "total_parts" : 2
            }
        )

        merge_athome_raw_data_parts = PythonOperator(
            task_id = "merge_athome_raw_data_parts",
            python_callable=merge_athome_raw_data_parts
        )

        scraping_part1 >> merge_athome_raw_data_parts
        scraping_part2 >> merge_athome_raw_data_parts

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

    extract_data_from_immotop_lu >> transform_data_from_immotop_lu >> immotop_lu_data_enrichment
    extract_data_from_athome_lu >> transform_data_from_athome_lu >> athome_lu_data_enrichment

    athome_lu_data_enrichment >> gen_report
    immotop_lu_data_enrichment >> gen_report

    athome_lu_data_enrichment >> treat_duplicates
    immotop_lu_data_enrichment >> treat_duplicates