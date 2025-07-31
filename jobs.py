from dagster import define_asset_job, AssetSelection

# Combined job to run all pipelines
all_pipelines_job = define_asset_job(
    name="daily_morning_pipelines",
    selection=AssetSelection.groups("nppes_file_processing", "nucc_processing", "provider_file_processing", "recipient_file_processing"),
    description="Job to run all pipelines (NPPES, NUCC, Medicaid Provider, Medicaid Recipient)"
)
