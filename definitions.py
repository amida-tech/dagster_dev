# definitions.py
from dagster import Definitions, load_assets_from_modules
from utils.snowflake import snowpark_session
from utils.adls_sftp import adls_sftp_resource
from utils.adls_access_keys import adls_access_keys_resource

from orchestration_pipelines.medicaid import recipient_pipeline, provider_pipeline
from orchestration_pipelines.nppes import nppes_pipeline
from orchestration_pipelines.nucc import nucc_pipeline

from jobs import (
    nppes_job,
    nucc_job, 
    provider_job,
    recipient_job
)

from schedules import (
    nppes_morning_schedule,
    nucc_morning_schedule,
    provider_morning_schedule,
    recipient_morning_schedule
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
        nppes_job,
        nucc_job,
        provider_job,
        recipient_job
    ],
    schedules=[
        nppes_morning_schedule,      
        nucc_morning_schedule,       
        provider_morning_schedule,  
        recipient_morning_schedule
    ],
    resources={
        "snowflake_snowpark": snowpark_session,
        "adls_sftp": adls_sftp_resource,
        "adls_access_keys": adls_access_keys_resource
    },
)