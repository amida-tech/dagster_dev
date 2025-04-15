from dagster import Definitions

from quickstart_etl.assets.copyADLSfiles import copyADLSfiles
from quickstart_etl.assets.validate_csv_counts import validate_csv_counts

defs = Definitions(
    assets=[copyADLSfiles, validate_csv_counts],
)
