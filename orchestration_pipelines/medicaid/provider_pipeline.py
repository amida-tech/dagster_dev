from dagster import asset, AssetExecutionContext, AssetIn, MaterializeResult
from reusable_components.etl.dq_audit import create_dq_audit_entry
from reusable_components.file_processing.archive_files import archive_files_with_result
from reusable_components.file_processing.monitor_files import create_standardized_file_monitor
from reusable_components.error_handling.standardized_alerts import with_pipeline_alerts
from reusable_components.dq.dq_transactions import load_dq_transactions_with_result
from reusable_components.dq.row_count_validator import run_dq_row_count_validation_with_result
from reusable_components.file_processing.unzip_processor import unzip_files_with_result
from reusable_components.etl.copy_sftp_to_adls import copy_files_sftp_to_adls
from reusable_components.dq.control_table import load_dq_control_table_with_result
from reusable_components.etl.adls_csv_to_snowflake_iceberg import load_csv_to_iceberg_with_result
from reusable_components.dq.dq_schema_validator import validate_all_file_schemas_with_result

PROVIDER_CONFIG = {
    "pipeline_name": "MEDICAID_PROVIDER",
    "subject_area": "PROVIDER", 
    "program_name": "MEDICAID",
    "asset_name": "provider_files_monitor",
    "sftp_source_path": "/prod/mmis/provider",
    "file_criteria": {
        "prefix": {"pattern": ["P_MMIS"], "count": 11},
        "suffix": {"pattern": None, "count": 0},
        "contains": {"pattern": None, "count": 0},
        "not_contains": {"pattern": None, "count": 0},
        "regex": {"pattern": None, "count": 0},
        "extension":{"pattern": None, "count": 0}
    },
    "downstream_assets": [
        "start_dq_audit_run_provider",
        "copy_mftserver_provider_files_to_srcfiles_stage", 
        "load_dq_transactions_provider",
        "unzip_provider_files_to_load_provider",
        "archive_provider_files_provider",
        "load_dq_control_table_provider",
        "dq_provider_row_count_validation",
        "dq_schema_check_provider",
        "load_csv_to_iceberg_provider" 
    ],
    "stage_container": "srcfiles",
    "stage_directory": "medicaid/provider/stage",
    "load_directory": "medicaid/provider/load", 
    "archive_directory": "medicaid/provider/archive",
    "control_file": "P_MMIS_PROVIDER_CONTROL_FILE.csv",
    "snowflake_db": "ANALYTYXONE_DEV",
    "snowflake_schema": "BRONZE",
    "snowflake_stage": "PARQUET_STAGE",
    "group_name": "provider_file_processing",
    "alert_config": {
        "program_name": "Medicaid Provider Data Processing",
        "send_success_alerts": True
    }
}

# Sensor and file monitor asset
provider_asset, provider_sensor = create_standardized_file_monitor(PROVIDER_CONFIG)

# DQ Audit Asset
@asset(
    name="start_dq_audit_run_provider",
    description="Create DQ_Audit entry for provider pipeline",
    required_resource_keys={"snowflake_snowpark"},
    ins={"monitor_result": AssetIn("provider_files_monitor")},
    group_name=PROVIDER_CONFIG["group_name"]
)
@with_pipeline_alerts(
    pipeline_name=PROVIDER_CONFIG["pipeline_name"],
    alert_config=PROVIDER_CONFIG["alert_config"]
)
def start_dq_audit_run_provider(context: AssetExecutionContext, monitor_result) -> MaterializeResult:  
    return create_dq_audit_entry(
        context=context,
        session=context.resources.snowflake_snowpark,
        monitor_result=monitor_result,
        program_name=PROVIDER_CONFIG["program_name"],
        subject_area=PROVIDER_CONFIG["subject_area"],
        pipeline_name=PROVIDER_CONFIG["pipeline_name"],
        prefix=PROVIDER_CONFIG["file_criteria"]["prefix"]["pattern"],
        suffix=PROVIDER_CONFIG["file_criteria"]["suffix"]["pattern"],
        contains=PROVIDER_CONFIG["file_criteria"]["contains"]["pattern"],
        not_contains=PROVIDER_CONFIG["file_criteria"]["not_contains"]["pattern"],
        regex=PROVIDER_CONFIG["file_criteria"]["regex"]["pattern"],
        extension=PROVIDER_CONFIG["file_criteria"]["extension"]["pattern"]
    )

# Copy SFTP to ADLS Asset
@asset(
    name="copy_mftserver_provider_files_to_srcfiles_stage",
    description="Copy provider files from SFTP to ADLS staging",
    required_resource_keys={"adls_sftp", "adls_access_keys"},
    ins={"file_monitor_result": AssetIn("provider_files_monitor")},
    group_name=PROVIDER_CONFIG["group_name"]
)
@with_pipeline_alerts(
    pipeline_name=PROVIDER_CONFIG["pipeline_name"],
    alert_config=PROVIDER_CONFIG["alert_config"]
)
def copy_provider_files_to_stage_provider(context: AssetExecutionContext, file_monitor_result) -> MaterializeResult:
   return copy_files_sftp_to_adls(
        context=context,
        sftp_client=context.resources.adls_sftp,
        adls_client=context.resources.adls_access_keys,
        file_monitor_result=file_monitor_result,
        source_path=PROVIDER_CONFIG["sftp_source_path"],
        destination_container=PROVIDER_CONFIG["stage_container"],
        destination_path=PROVIDER_CONFIG["stage_directory"],
        pipeline_name=PROVIDER_CONFIG["pipeline_name"],
        prefix=PROVIDER_CONFIG["file_criteria"]["prefix"]["pattern"],
        suffix=PROVIDER_CONFIG["file_criteria"]["suffix"]["pattern"],
        contains=PROVIDER_CONFIG["file_criteria"]["contains"]["pattern"],
        not_contains=PROVIDER_CONFIG["file_criteria"]["not_contains"]["pattern"],
        regex=PROVIDER_CONFIG["file_criteria"]["regex"]["pattern"],
        extension=PROVIDER_CONFIG["file_criteria"]["extension"]["pattern"]
    )

# DQ Transactions Asset
@asset(
    name="load_dq_transactions_provider",
    description="Load DQ transactions for provider files",
    required_resource_keys={"snowflake_snowpark", "adls_access_keys"},
    ins={
        "copy_result": AssetIn("copy_mftserver_provider_files_to_srcfiles_stage"),
        "audit_batch_id": AssetIn("start_dq_audit_run_provider")
    },
    group_name=PROVIDER_CONFIG["group_name"]
)
@with_pipeline_alerts(
    pipeline_name=PROVIDER_CONFIG["pipeline_name"],
    alert_config=PROVIDER_CONFIG["alert_config"]
)
def load_file_transaction_metadata(context: AssetExecutionContext, copy_result, audit_batch_id: int) -> MaterializeResult:
    return load_dq_transactions_with_result(
        context=context,
        snowpark_session=context.resources.snowflake_snowpark,
        adls_client=context.resources.adls_access_keys,
        copy_result=copy_result,
        audit_batch_id=audit_batch_id,
        container_name=PROVIDER_CONFIG["stage_container"],
        directory_path=PROVIDER_CONFIG["stage_directory"],
        program_name=PROVIDER_CONFIG["program_name"],
        subject_area=PROVIDER_CONFIG["subject_area"],
        pipeline_name=PROVIDER_CONFIG["pipeline_name"],
        prefix=PROVIDER_CONFIG["file_criteria"]["prefix"]["pattern"],
        suffix=PROVIDER_CONFIG["file_criteria"]["suffix"]["pattern"],
        contains=PROVIDER_CONFIG["file_criteria"]["contains"]["pattern"],
        not_contains=PROVIDER_CONFIG["file_criteria"]["not_contains"]["pattern"],
        regex=PROVIDER_CONFIG["file_criteria"]["regex"]["pattern"],
        extension=PROVIDER_CONFIG["file_criteria"]["extension"]["pattern"]
    )

# Copy files to Load directory Asset
@asset(
    name="unzip_provider_files_to_load_provider",
    description="Unzip provider files to load directory",
    required_resource_keys={"adls_access_keys"},
    ins={"copy_result": AssetIn("copy_mftserver_provider_files_to_srcfiles_stage")},
    group_name=PROVIDER_CONFIG["group_name"]
)
@with_pipeline_alerts(
    pipeline_name=PROVIDER_CONFIG["pipeline_name"],
    alert_config=PROVIDER_CONFIG["alert_config"]
)
def unzip_provider_files_to_load(context: AssetExecutionContext, copy_result) -> MaterializeResult: 
    return unzip_files_with_result(
        context=context,
        adls_client=context.resources.adls_access_keys,
        copy_result=copy_result,
        container_name=PROVIDER_CONFIG["stage_container"],
        stage_directory=PROVIDER_CONFIG["stage_directory"],
        load_directory=PROVIDER_CONFIG["load_directory"],
        pipeline_name=PROVIDER_CONFIG["pipeline_name"],
        prefix=PROVIDER_CONFIG["file_criteria"]["prefix"]["pattern"],
        suffix=PROVIDER_CONFIG["file_criteria"]["suffix"]["pattern"],
        contains=PROVIDER_CONFIG["file_criteria"]["contains"]["pattern"],
        not_contains=PROVIDER_CONFIG["file_criteria"]["not_contains"]["pattern"],
        regex=PROVIDER_CONFIG["file_criteria"]["regex"]["pattern"],
        extension=PROVIDER_CONFIG["file_criteria"]["extension"]["pattern"]
    )

# Copy files to Archive Directory Asset
@asset(
    name="archive_provider_files_provider",
    description="Archive ZIP files from stage to archive directory",
    required_resource_keys={"adls_access_keys"},
    ins={"copy_result": AssetIn("copy_mftserver_provider_files_to_srcfiles_stage")},
    group_name=PROVIDER_CONFIG["group_name"]
)
@with_pipeline_alerts(
    pipeline_name=PROVIDER_CONFIG["pipeline_name"],
    alert_config=PROVIDER_CONFIG["alert_config"]
)
def archive_provider_files(context: AssetExecutionContext, copy_result) -> MaterializeResult:  
    return archive_files_with_result(
        context=context,
        adls_client=context.resources.adls_access_keys,
        copy_result=copy_result,
        stage_container=PROVIDER_CONFIG["stage_container"],
        stage_directory=PROVIDER_CONFIG["stage_directory"],
        archive_directory=PROVIDER_CONFIG["archive_directory"],
        pipeline_name=PROVIDER_CONFIG["pipeline_name"],
        prefix=PROVIDER_CONFIG["file_criteria"]["prefix"]["pattern"],
        suffix=PROVIDER_CONFIG["file_criteria"]["suffix"]["pattern"],
        contains=PROVIDER_CONFIG["file_criteria"]["contains"]["pattern"],
        not_contains=PROVIDER_CONFIG["file_criteria"]["not_contains"]["pattern"],
        regex=PROVIDER_CONFIG["file_criteria"]["regex"]["pattern"],
        extension=PROVIDER_CONFIG["file_criteria"]["extension"]["pattern"]
    )

# DQ Control Asset
@asset(
    name="load_dq_control_table_provider",
    description="Load control table data for provider pipeline",
    required_resource_keys={"adls_access_keys", "snowflake_snowpark"},
    ins={
        "audit_batch_id": AssetIn("start_dq_audit_run_provider"),
        "unzip_result": AssetIn("unzip_provider_files_to_load_provider")
    },
    group_name=PROVIDER_CONFIG["group_name"]
)
@with_pipeline_alerts(
    pipeline_name=PROVIDER_CONFIG["pipeline_name"],
    alert_config=PROVIDER_CONFIG["alert_config"]
)
def load_dq_control_table(context: AssetExecutionContext, audit_batch_id: int, unzip_result) -> MaterializeResult: 
    return load_dq_control_table_with_result(
        context=context,
        snowpark_session=context.resources.snowflake_snowpark,
        unzip_result=unzip_result,
        audit_batch_id=audit_batch_id,
        adls_container=PROVIDER_CONFIG["stage_container"],
        folder_path=PROVIDER_CONFIG["load_directory"],
        control_file=PROVIDER_CONFIG["control_file"],
        program_name=PROVIDER_CONFIG["program_name"],
        subject_area=PROVIDER_CONFIG["subject_area"],
        pipeline_name=PROVIDER_CONFIG["pipeline_name"]
    )

# Row Count Asset
@asset(
    name="dq_provider_row_count_validation",
    description="Validate row counts for provider files",
    required_resource_keys={"adls_access_keys", "snowflake_snowpark"},
    ins={
        "audit_batch_id": AssetIn("start_dq_audit_run_provider"),
        "control_table_result": AssetIn("load_dq_control_table_provider")
    },
    group_name=PROVIDER_CONFIG["group_name"]
)
@with_pipeline_alerts(
    pipeline_name=PROVIDER_CONFIG["pipeline_name"],
    alert_config=PROVIDER_CONFIG["alert_config"]
)
def dq_provider_row_count_validation(context: AssetExecutionContext, audit_batch_id: int, control_table_result) -> MaterializeResult:
    return run_dq_row_count_validation_with_result(
        context=context,
        session=context.resources.snowflake_snowpark,
        adls2=context.resources.adls_access_keys,
        control_table_result=control_table_result,
        audit_batch_id=audit_batch_id,
        container=PROVIDER_CONFIG["stage_container"],
        folder_path=PROVIDER_CONFIG["load_directory"],
        program_name=PROVIDER_CONFIG["program_name"],
        subject_area=PROVIDER_CONFIG["subject_area"],
        pipeline_name=PROVIDER_CONFIG["pipeline_name"],
        prefix=PROVIDER_CONFIG["file_criteria"]["prefix"]["pattern"],
        suffix=PROVIDER_CONFIG["file_criteria"]["suffix"]["pattern"],
        contains=PROVIDER_CONFIG["file_criteria"]["contains"]["pattern"],
        not_contains=["CONTROL"],
        regex=PROVIDER_CONFIG["file_criteria"]["regex"]["pattern"],
        extension=[".csv"]
    )

# Schema check Asset
@asset(
    name="dq_schema_check_provider",
    description="Validate file schemas for provider pipeline",
    required_resource_keys={"adls_access_keys", "snowflake_snowpark"},
    ins={"dq_result": AssetIn("dq_provider_row_count_validation")},
    group_name=PROVIDER_CONFIG["group_name"]
)
@with_pipeline_alerts(
    pipeline_name=PROVIDER_CONFIG["pipeline_name"],
    alert_config=PROVIDER_CONFIG["alert_config"]
)
def dq_schema_check(context: AssetExecutionContext, dq_result) -> MaterializeResult:    
    return validate_all_file_schemas_with_result(
        adls_client=context.resources.adls_access_keys,
        container=PROVIDER_CONFIG["stage_container"],
        folder_path=PROVIDER_CONFIG["load_directory"],
        session=context.resources.snowflake_snowpark,
        context=context,
        dq_result=dq_result,
        pipeline_name=PROVIDER_CONFIG["pipeline_name"],
        prefix=PROVIDER_CONFIG["file_criteria"]["prefix"]["pattern"],
        suffix=PROVIDER_CONFIG["file_criteria"]["suffix"]["pattern"],
        contains=PROVIDER_CONFIG["file_criteria"]["contains"]["pattern"],
        not_contains=["CONTROL"],
        regex=PROVIDER_CONFIG["file_criteria"]["regex"]["pattern"],
        extension=[".csv"]
    )

# CSV to Iceberg Asset
@asset(
    name="load_csv_to_iceberg_provider",
    description="Load CSV files to Iceberg tables for provider pipeline",
    required_resource_keys={"adls_access_keys", "snowflake_snowpark"},
    ins={
        "audit_batch_id": AssetIn("start_dq_audit_run_provider"),
        "schema_check_result": AssetIn("dq_schema_check_provider")
    },
    group_name=PROVIDER_CONFIG["group_name"]
)
@with_pipeline_alerts(
    pipeline_name=PROVIDER_CONFIG["pipeline_name"],
    alert_config=PROVIDER_CONFIG["alert_config"]
)
def load_csv_to_iceberg(context: AssetExecutionContext, audit_batch_id: int, schema_check_result) -> MaterializeResult:
    return load_csv_to_iceberg_with_result(
        context=context,
        adls_client=context.resources.adls_access_keys,
        snowpark_session=context.resources.snowflake_snowpark,
        audit_batch_id=audit_batch_id,
        schema_check_result=schema_check_result,
        config=PROVIDER_CONFIG,
        prefix=PROVIDER_CONFIG["file_criteria"]["prefix"]["pattern"],
        suffix=PROVIDER_CONFIG["file_criteria"]["suffix"]["pattern"],
        contains=PROVIDER_CONFIG["file_criteria"]["contains"]["pattern"],
        not_contains=["CONTROL"],  
        regex=PROVIDER_CONFIG["file_criteria"]["regex"]["pattern"],
        extension=[".csv"]
    )