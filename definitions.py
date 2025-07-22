from dagster import Definitions, load_assets_from_modules
from utils.snowflake import snowpark_session
from utils.adls_sftp import adls_sftp_resource
from utils.adls_access_keys import adls_access_keys_resource
from orchestration_pipelines.medicaid import recipient_pipeline
from orchestration_pipelines.medicaid.recipient_pipeline import (
    recipient_sensor
)
from orchestration_pipelines.medicaid import provider_pipeline
from orchestration_pipelines.medicaid.provider_pipeline import (
    provider_sensor
)

PIPELINE_MODULES = [
    recipient_pipeline,
    provider_pipeline
]

all_assets = load_assets_from_modules(PIPELINE_MODULES)

defs = Definitions(
    assets=all_assets,
    sensors=[recipient_sensor,provider_sensor],
    resources={
        "snowflake_snowpark": snowpark_session,
        "adls_sftp": adls_sftp_resource,
        "adls_access_keys": adls_access_keys_resource
    },
)