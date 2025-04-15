from dagster import Definitions, load_assets_from_module
from quickstart_etl.assets import data_assets

defs = Definitions(
    assets=load_assets_from_module(data_assets),
)
