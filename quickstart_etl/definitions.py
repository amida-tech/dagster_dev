from dagster import Definitions, load_assets_from_modules
from quickstart_etl.assets import data_assets

defs = Definitions(
    assets=[copyADLSfiles, validate_csv_counts],
    schedules=[daily_refresh_schedule],
)
