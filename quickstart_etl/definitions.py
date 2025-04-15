from dagster import Definitions, load_assets_from_modules
from quickstart_etl.assets import data_assets

defs = Definitions(
    assets=load_assets_from_modules([data_assets]),
)
