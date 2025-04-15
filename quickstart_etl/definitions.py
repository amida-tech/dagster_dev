from dagster import Definitions
from quickstart_etl.assets.copyADLSfiles import process_and_count_csvs
from quickstart_etl.assets.validate_csv_counts import validate_csv_counts

defs = Definitions(
    assets=[process_and_count_csvs, validate_csv_counts],
)
