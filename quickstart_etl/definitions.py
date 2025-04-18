from dagster import Definitions, define_asset_job

from quickstart_etl.assets.copyADLSfiles import process_and_count_csvs
from quickstart_etl.assets.validate_csv_counts import validate_csv_counts
from quickstart_etl.assets.convert_csv_to_parquet import convert_csv_to_parquet
from quickstart_etl.assets.load_all_recipient_data import load_all_recipient_data

recipient_data_load = define_asset_job(name="recipient_data_load")

defs = Definitions(
    assets=[
        process_and_count_csvs,
        validate_csv_counts,
        convert_csv_to_parquet,
        load_all_recipient_data,
    ],
    jobs=[recipient_data_load],
)

