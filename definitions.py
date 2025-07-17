from dagster import Definitions
from amida_demo.assets import processed_data

defs = Definitions(
    assets=[processed_data],
)