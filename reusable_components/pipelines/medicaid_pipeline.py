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
from reusable_components.dq.dq_procedure import dq_rules_procedure
from reusable_components.etl.cleanup_files import cleanup_pipeline_directories_with_result


def create_pipeline(config: dict):
    
    subject_area = config["subject_area"].lower()
    
    # Create file monitor asset and sensor
    monitor_asset, monitor_sensor = create_standardized_file_monitor(config)
    
    # DQ Audit Asset
    @asset(
        name=f"start_dq_audit_run_{subject_area}",
        description=f"Create DQ_Audit entry for {subject_area} pipeline",
        required_resource_keys={"snowflake_snowpark"},
        ins={"monitor_result": AssetIn(config["asset_name"])},
        group_name=config["group_name"]
    )
    @with_pipeline_alerts(
        pipeline_name=config["pipeline_name"],
        alert_config=config["alert_config"]
    )
    def start_dq_audit_run(context: AssetExecutionContext, monitor_result) -> MaterializeResult:  
        return create_dq_audit_entry(
            context=context,
            session=context.resources.snowflake_snowpark,
            monitor_result=monitor_result,
            program_name=config["program_name"],
            subject_area=config["subject_area"],
            pipeline_name=config["pipeline_name"],
            prefix=config["file_criteria"]["prefix"]["pattern"],
            suffix=config["file_criteria"]["suffix"]["pattern"],
            contains=config["file_criteria"]["contains"]["pattern"],
            not_contains=config["file_criteria"]["not_contains"]["pattern"],
            regex=config["file_criteria"]["regex"]["pattern"],
            extension=config["file_criteria"]["extension"]["pattern"]
        )

    # Copy SFTP to ADLS Asset
    @asset(
        name=f"copy_mftserver_{subject_area}_files_to_srcfiles_stage",
        description=f"Copy {subject_area} files from SFTP to ADLS staging",
        required_resource_keys={"adls_sftp", "adls_access_keys"},
        ins={"file_monitor_result": AssetIn(config["asset_name"])},
        group_name=config["group_name"]
    )
    @with_pipeline_alerts(
        pipeline_name=config["pipeline_name"],
        alert_config=config["alert_config"]
    )
    def copy_files_to_stage(context: AssetExecutionContext, file_monitor_result) -> MaterializeResult:
       return copy_files_sftp_to_adls(
            context=context,
            sftp_client=context.resources.adls_sftp,
            adls_client=context.resources.adls_access_keys,
            file_monitor_result=file_monitor_result,
            source_path=config["sftp_source_path"],
            destination_container=config["stage_container"],
            destination_path=config["stage_directory"],
            pipeline_name=config["pipeline_name"],
            prefix=config["file_criteria"]["prefix"]["pattern"],
            suffix=config["file_criteria"]["suffix"]["pattern"],
            contains=config["file_criteria"]["contains"]["pattern"],
            not_contains=config["file_criteria"]["not_contains"]["pattern"],
            regex=config["file_criteria"]["regex"]["pattern"],
            extension=config["file_criteria"]["extension"]["pattern"]
        )

    # DQ Transactions Asset
    @asset(
        name=f"load_dq_transactions_{subject_area}",
        description=f"Load DQ transactions for {subject_area} files",
        required_resource_keys={"snowflake_snowpark", "adls_access_keys"},
        ins={
            "copy_result": AssetIn(f"copy_mftserver_{subject_area}_files_to_srcfiles_stage"),
            "audit_batch_id": AssetIn(f"start_dq_audit_run_{subject_area}")
        },
        group_name=config["group_name"]
    )
    @with_pipeline_alerts(
        pipeline_name=config["pipeline_name"],
        alert_config=config["alert_config"]
    )
    def load_dq_transactions(context: AssetExecutionContext, copy_result, audit_batch_id: int) -> MaterializeResult:
        return load_dq_transactions_with_result(
            context=context,
            snowpark_session=context.resources.snowflake_snowpark,
            adls_client=context.resources.adls_access_keys,
            copy_result=copy_result,
            audit_batch_id=audit_batch_id,
            container_name=config["stage_container"],
            directory_path=config["stage_directory"],
            program_name=config["program_name"],
            subject_area=config["subject_area"],
            pipeline_name=config["pipeline_name"],
            prefix=config["file_criteria"]["prefix"]["pattern"],
            suffix=config["file_criteria"]["suffix"]["pattern"],
            contains=config["file_criteria"]["contains"]["pattern"],
            not_contains=config["file_criteria"]["not_contains"]["pattern"],
            regex=config["file_criteria"]["regex"]["pattern"],
            extension=config["file_criteria"]["extension"]["pattern"]
        )

    # Unzip files Asset
    @asset(
        name=f"unzip_{subject_area}_files_to_load",
        description=f"Unzip {subject_area} files to load directory",
        required_resource_keys={"adls_access_keys"},
        ins={"copy_result": AssetIn(f"copy_mftserver_{subject_area}_files_to_srcfiles_stage")},
        group_name=config["group_name"]
    )
    @with_pipeline_alerts(
        pipeline_name=config["pipeline_name"],
        alert_config=config["alert_config"]
    )
    def unzip_files_to_load(context: AssetExecutionContext, copy_result) -> MaterializeResult: 
        return unzip_files_with_result(
            context=context,
            adls_client=context.resources.adls_access_keys,
            copy_result=copy_result,
            container_name=config["stage_container"],
            stage_directory=config["stage_directory"],
            load_directory=config["load_directory"],
            pipeline_name=config["pipeline_name"],
            prefix=config["file_criteria"]["prefix"]["pattern"],
            suffix=config["file_criteria"]["suffix"]["pattern"],
            contains=config["file_criteria"]["contains"]["pattern"],
            not_contains=config["file_criteria"]["not_contains"]["pattern"],
            regex=config["file_criteria"]["regex"]["pattern"],
            extension=config["file_criteria"]["extension"]["pattern"]
        )

    # Archive files Asset
    @asset(
        name=f"archive_{subject_area}_files",
        description=f"Archive ZIP files from stage to archive directory for {subject_area}",
        required_resource_keys={"adls_access_keys"},
        ins={"copy_result": AssetIn(f"copy_mftserver_{subject_area}_files_to_srcfiles_stage")},
        group_name=config["group_name"]
    )
    @with_pipeline_alerts(
        pipeline_name=config["pipeline_name"],
        alert_config=config["alert_config"]
    )
    def archive_files(context: AssetExecutionContext, copy_result) -> MaterializeResult:  
        return archive_files_with_result(
            context=context,
            adls_client=context.resources.adls_access_keys,
            copy_result=copy_result,
            stage_container=config["stage_container"],
            stage_directory=config["stage_directory"],
            archive_directory=config["archive_directory"],
            pipeline_name=config["pipeline_name"],
            prefix=config["file_criteria"]["prefix"]["pattern"],
            suffix=config["file_criteria"]["suffix"]["pattern"],
            contains=config["file_criteria"]["contains"]["pattern"],
            not_contains=config["file_criteria"]["not_contains"]["pattern"],
            regex=config["file_criteria"]["regex"]["pattern"],
            extension=config["file_criteria"]["extension"]["pattern"]
        )

    # DQ Control Asset
    @asset(
        name=f"load_dq_control_table_{subject_area}",
        description=f"Load control table data for {subject_area} pipeline",
        required_resource_keys={"adls_access_keys", "snowflake_snowpark"},
        ins={
            "audit_batch_id": AssetIn(f"start_dq_audit_run_{subject_area}"),
            "unzip_result": AssetIn(f"unzip_{subject_area}_files_to_load")
        },
        group_name=config["group_name"]
    )
    @with_pipeline_alerts(
        pipeline_name=config["pipeline_name"],
        alert_config=config["alert_config"]
    )
    def load_dq_control_table(context: AssetExecutionContext, audit_batch_id: int, unzip_result) -> MaterializeResult: 
        return load_dq_control_table_with_result(
            context=context,
            snowpark_session=context.resources.snowflake_snowpark,
            unzip_result=unzip_result,
            audit_batch_id=audit_batch_id,
            adls_container=config["stage_container"],
            folder_path=config["load_directory"],
            control_file=config["control_file"],
            program_name=config["program_name"],
            subject_area=config["subject_area"],
            pipeline_name=config["pipeline_name"]
        )

    # Row Count Validation Asset
    @asset(
        name=f"dq_{subject_area}_row_count_validation",
        description=f"Validate row counts for {subject_area} files",
        required_resource_keys={"adls_access_keys", "snowflake_snowpark"},
        ins={
            "audit_batch_id": AssetIn(f"start_dq_audit_run_{subject_area}"),
            "control_table_result": AssetIn(f"load_dq_control_table_{subject_area}")
        },
        group_name=config["group_name"]
    )
    @with_pipeline_alerts(
        pipeline_name=config["pipeline_name"],
        alert_config=config["alert_config"]
    )
    def dq_row_count_validation(context: AssetExecutionContext, audit_batch_id: int, control_table_result) -> MaterializeResult:
        return run_dq_row_count_validation_with_result(
            context=context,
            session=context.resources.snowflake_snowpark,
            adls2=context.resources.adls_access_keys,
            control_table_result=control_table_result,
            audit_batch_id=audit_batch_id,
            container=config["stage_container"],
            folder_path=config["load_directory"],
            program_name=config["program_name"],
            subject_area=config["subject_area"],
            pipeline_name=config["pipeline_name"]
        )

    # Schema Validation Asset
    @asset(
        name=f"dq_schema_check_{subject_area}",
        description=f"Validate file schemas for {subject_area} pipeline",
        required_resource_keys={"adls_access_keys", "snowflake_snowpark"},
        ins={"dq_result": AssetIn(f"dq_{subject_area}_row_count_validation")},
        group_name=config["group_name"]
    )
    @with_pipeline_alerts(
        pipeline_name=config["pipeline_name"],
        alert_config=config["alert_config"]
    )
    def dq_schema_check(context: AssetExecutionContext, dq_result) -> MaterializeResult:    
        return validate_all_file_schemas_with_result(
            adls_client=context.resources.adls_access_keys,
            container=config["stage_container"],
            folder_path=config["load_directory"],
            session=context.resources.snowflake_snowpark,
            context=context,
            dq_result=dq_result,
            pipeline_name=config["pipeline_name"],
            prefix=config["file_criteria"]["prefix"]["pattern"],
            suffix=config["file_criteria"]["suffix"]["pattern"],
            contains=config["file_criteria"]["contains"]["pattern"],
            not_contains=["CONTROL"],
            regex=config["file_criteria"]["regex"]["pattern"],
            extension=[".csv"]
        )

    # CSV to Iceberg Asset
    @asset(
        name=f"load_csv_to_iceberg_{subject_area}",
        description=f"Load CSV files to Iceberg tables for {subject_area} pipeline",
        required_resource_keys={"adls_access_keys", "snowflake_snowpark"},
        ins={
            "audit_batch_id": AssetIn(f"start_dq_audit_run_{subject_area}"),
            "schema_check_result": AssetIn(f"dq_schema_check_{subject_area}")
        },
        group_name=config["group_name"]
    )
    @with_pipeline_alerts(
        pipeline_name=config["pipeline_name"],
        alert_config=config["alert_config"]
    )
    def load_csv_to_iceberg(context: AssetExecutionContext, audit_batch_id: int, schema_check_result) -> MaterializeResult:
        return load_csv_to_iceberg_with_result(
            context=context,
            adls_client=context.resources.adls_access_keys,
            snowpark_session=context.resources.snowflake_snowpark,
            audit_batch_id=audit_batch_id,
            schema_check_result=schema_check_result,
            config=config,
            prefix=config["file_criteria"]["prefix"]["pattern"],
            suffix=config["file_criteria"]["suffix"]["pattern"],
            contains=config["file_criteria"]["contains"]["pattern"],
            not_contains=["CONTROL"], 
            regex=config["file_criteria"]["regex"]["pattern"],
            extension=[".csv"] 
        )
    
    # Rules Asset
    @asset(
        name=f"execute_rules_asset_{subject_area}",
        required_resource_keys={"snowflake_snowpark", "adls_access_keys"},
        ins={
            "audit_batch_id": AssetIn(f"start_dq_audit_run_{subject_area}"),
            "iceberg_result": AssetIn(f"load_csv_to_iceberg_{subject_area}")
        },
        group_name=config["group_name"]
    )
    @with_pipeline_alerts(
        pipeline_name=config["pipeline_name"],
        alert_config=config["alert_config"]
    )
    def execute_rules_asset(context: AssetExecutionContext, audit_batch_id: int, iceberg_result):
        dq_output = dq_rules_procedure(
            context=context,
            audit_batch_id=audit_batch_id,
            session=context.resources.snowflake_snowpark,
            rule_group=config.get("RULE_GROUP"),
            rule_id=config.get("RULE_ID"),
            refresh_summary=config.get("REFRESH_SUMMARY"),
            pipeline_name=config.get("pipeline_name"),
            alert_config=config.get("alert_config")
        )
        
        return dq_output

    # Cleanup Asset
    @asset(
    name=f"cleanup_{subject_area}_directories",
    description=f"Clean up {subject_area} pipeline directories",
    required_resource_keys={"adls_access_keys"},
    ins={
        "copy_result": AssetIn(f"copy_mftserver_{subject_area}_files_to_srcfiles_stage"), 
        "unzip_result": AssetIn(f"unzip_{subject_area}_files_to_load"),
        "transactions_result": AssetIn(f"load_dq_transactions_{subject_area}"),
        "archive_result": AssetIn(f"archive_{subject_area}_files"),
        "iceberg_result": AssetIn(f"load_csv_to_iceberg_{subject_area}"),
        "dq_result": AssetIn(f"execute_rules_asset_{subject_area}")
    },
    group_name=config["group_name"]
    )
    @with_pipeline_alerts(
        pipeline_name=config["pipeline_name"],
        alert_config=config["alert_config"]
    )
    def cleanup_directories(context: AssetExecutionContext, **upstream_results) -> MaterializeResult:
        return cleanup_pipeline_directories_with_result(
            context=context,
            adls_client=context.resources.adls_access_keys,
            config=config
        )

    return {
        "monitor_asset": monitor_asset,
        "monitor_sensor": monitor_sensor,
        "start_dq_audit_run": start_dq_audit_run,
        "copy_files_to_stage": copy_files_to_stage,
        "load_dq_transactions": load_dq_transactions,
        "unzip_files_to_load": unzip_files_to_load,
        "archive_files": archive_files,
        "load_dq_control_table": load_dq_control_table,
        "dq_row_count_validation": dq_row_count_validation,
        "dq_schema_check": dq_schema_check,
        "load_csv_to_iceberg": load_csv_to_iceberg,
        "execute_rules_asset": execute_rules_asset,
        "cleanup_directories": cleanup_directories
    }