from dagster import define_asset_job, AssetSelection

nppes_job = define_asset_job(
    name="nppes_pipeline_job",
    selection=AssetSelection.groups("nppes_file_processing"),
    description="Job to run NPPES pipeline"
)

nucc_job = define_asset_job(
    name="nucc_pipeline_job", 
    selection=AssetSelection.groups("nucc_file_processing"),
    description="Job to run NUCC pipeline"
)

provider_job = define_asset_job(
    name="provider_pipeline_job",
    selection=AssetSelection.groups("provider_file_processing"), 
    description="Job to run Medicaid Provider pipeline"
)

recipient_job = define_asset_job(
    name="recipient_pipeline_job",
    selection=AssetSelection.groups("recipient_file_processing"),
    description="Job to run Medicaid Recipient pipeline"
)
