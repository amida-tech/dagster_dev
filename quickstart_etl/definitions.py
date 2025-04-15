from dagster import Definitions, define_asset_job
from quickstart_etl.assets.copyADLSfiles import process_and_count_csvs
from quickstart_etl.assets.validate_csv_counts import validate_csv_counts

# Define the asset job
adls_dq_job = define_asset_job(
    name="adls_dq_job",
    selection=["process_and_count_csvs", "validate_csv_counts"],
)

defs = Definitions(
    assets=[process_and_count_csvs, validate_csv_counts],
    jobs=[adls_dq_job]
)
