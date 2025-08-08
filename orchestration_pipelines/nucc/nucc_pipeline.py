from reference.NUCC.assets.bronze.nucc_reusable import create_nucc_monitor_and_downloader
from reusable_components.pipelines.nucc_pipeline import create_pipeline 
from pipeline_config import NUCC_CONFIG

nucc_monitor_components = create_nucc_monitor_and_downloader(NUCC_CONFIG)
nucc_files_monitor = nucc_monitor_components["monitor_asset"]
nucc_files_monitor_sensor = nucc_monitor_components["monitor_sensor"]

nucc_pipeline_assets = create_pipeline(NUCC_CONFIG)

start_dq_audit_run_nucc = nucc_pipeline_assets["start_dq_audit_run"]
load_dq_transactions_nucc = nucc_pipeline_assets["load_dq_transactions"]
copy_nucc_files_to_load = nucc_pipeline_assets["copy_files_to_load"]
archive_nucc_files = nucc_pipeline_assets["archive_files"]
dq_schema_check_nucc = nucc_pipeline_assets["dq_schema_check"]
load_csv_to_iceberg_nucc = nucc_pipeline_assets["load_csv_to_iceberg"]
transform_bronze_to_silver_nucc = nucc_pipeline_assets["transform_bronze_to_silver"]
transform_silver_to_gold_nucc = nucc_pipeline_assets["transform_silver_to_gold"]
execute_rules_asset_nucc = nucc_pipeline_assets["execute_rules_asset"]
cleanup_nucc_directories = nucc_pipeline_assets["cleanup_directories"] 
