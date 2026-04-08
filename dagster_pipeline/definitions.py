from dagster import Definitions, define_asset_job
from dagster_pipeline.assets import extract_data, transform_data, test_data

# Define Job
etl_job = define_asset_job("etl_job")

defs = Definitions(
    assets=[extract_data, transform_data, test_data],
    jobs=[etl_job]
)