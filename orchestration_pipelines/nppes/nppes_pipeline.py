from reference.NPPES.assets.bronze.nppes_reusable import create_cms_npi_monitor_and_downloader 
from reusable_components.nppes.pipeline import create_nppes_pipeline 
from pipeline_config import NPPES_CONFIG

nppes_monitor_components = create_cms_npi_monitor_and_downloader(NPPES_CONFIG)
nppes_files_monitor = nppes_monitor_components["monitor_asset"]
nppes_files_monitor_sensor = nppes_monitor_components["monitor_sensor"]

nppes_pipeline_assets = create_nppes_pipeline(NPPES_CONFIG)

start_dq_audit_run_nppes = nppes_pipeline_assets["start_dq_audit_run"]
load_dq_transactions_nppes = nppes_pipeline_assets["load_dq_transactions"]
copy_nppes_files_to_load = nppes_pipeline_assets["copy_files_to_load"]     
archive_nppes_files = nppes_pipeline_assets["archive_files"]               
dq_schema_check_nppes = nppes_pipeline_assets["dq_schema_check"]
load_csv_to_iceberg_nppes = nppes_pipeline_assets["load_csv_to_iceberg"]
execute_rules_asset_nppes = nppes_pipeline_assets["execute_rules_asset"]
