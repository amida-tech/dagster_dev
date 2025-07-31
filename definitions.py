from dagster import Definitions, load_assets_from_modules
from utils.snowflake import snowpark_session
from utils.adls_sftp import adls_sftp_resource
from utils.adls_access_keys import adls_access_keys_resource
from orchestration_pipelines.medicaid import recipient_pipeline, provider_pipeline
from orchestration_pipelines.nppes import nppes_pipeline
from orchestration_pipelines.nucc import nucc_pipeline
from jobs import (
    all_pipelines_job
)
from schedules import (
    daily_morning_schedule
)

PIPELINE_MODULES = [
    recipient_pipeline,
    provider_pipeline,
    nppes_pipeline,
    nucc_pipeline
]

all_assets = load_assets_from_modules(PIPELINE_MODULES)

defs = Definitions(
    assets=all_assets,
    jobs=[
        all_pipelines_job
    ],
    schedules=[
        daily_morning_schedule
    ],
    resources={
        "snowflake_snowpark": snowpark_session,
        "adls_sftp": adls_sftp_resource,
        "adls_access_keys": adls_access_keys_resource
    },
)