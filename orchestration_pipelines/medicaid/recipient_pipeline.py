from reusable_components.medicaid.pipeline import create_pipeline
from pipeline_config import RECIPIENT_CONFIG

# Create Recipient Pipeline
recipient_pipeline_assets = create_pipeline(RECIPIENT_CONFIG)
recipient_files_monitor_sensor = recipient_pipeline_assets["monitor_sensor"]

# Recipient Assets
recipient_monitor_asset = recipient_pipeline_assets["monitor_asset"]
recipient_monitor_sensor = recipient_pipeline_assets["monitor_sensor"]
start_dq_audit_run_recipient = recipient_pipeline_assets["start_dq_audit_run"]
copy_mftserver_recipient_files_to_srcfiles_stage = recipient_pipeline_assets["copy_files_to_stage"]
load_dq_transactions_recipient = recipient_pipeline_assets["load_dq_transactions"]
unzip_recipient_files_to_load = recipient_pipeline_assets["unzip_files_to_load"]
archive_recipient_files = recipient_pipeline_assets["archive_files"]
load_dq_control_table_recipient = recipient_pipeline_assets["load_dq_control_table"]
dq_recipient_row_count_validation = recipient_pipeline_assets["dq_row_count_validation"]
dq_schema_check_recipient = recipient_pipeline_assets["dq_schema_check"]
load_csv_to_iceberg_recipient = recipient_pipeline_assets["load_csv_to_iceberg"]
execute_rules_asset_recipient= recipient_pipeline_assets["execute_rules_asset"]
