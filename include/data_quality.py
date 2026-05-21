from pathlib import Path

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from airflow.models import Variable
import pandas as pd

def verify_dq(run_id, logical_date):
    GX_ROOT_DIR = Path(__file__).parent.parent / "gx"

    context = gx.get_context(context_root_dir=str(GX_ROOT_DIR))

    df = pd.read_csv(f"{Variable.get('immo_lux_data_folder')}/gx_merged.csv", dtype={
            "Floor_number" : "Int64",
            "Bedrooms" : "Int64",
            "Bathroom" : "Int64",
            "Garages" : "Int64",
            "Building_total_floors" : "Int64",
            "Construction_year" : "Int64",
            "Shower_room" : "Int64"})

    batch_request = RuntimeBatchRequest(
        datasource_name="dataframe_datasource",
        data_connector_name="default_runtime_connector",
        data_asset_name="final_df",
        runtime_parameters={"batch_data": df},
        batch_identifiers={
            "run_id": run_id,
            "logical_date": logical_date.isoformat()
        }
    )

    validator = context.get_validator(batch_request=batch_request, expectation_suite_name="preload_dq_suite")

    results = validator.validate()

    if not results.success:
        raise ValueError("Data quality validation failed. See the Great Expectations validation results for more details.")