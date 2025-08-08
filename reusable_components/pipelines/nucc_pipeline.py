from dagster import asset, AssetExecutionContext, AssetIn, MaterializeResult
from reusable_components.etl.dq_audit import create_dq_audit_entry
from reusable_components.error_handling.standardized_alerts import with_pipeline_alerts
from reusable_components.dq.dq_transactions import load_dq_transactions_with_result
from reusable_components.dq.dq_schema_validator import validate_all_file_schemas_with_result
from reusable_components.dq.dq_procedure import dq_rules_procedure
from reusable_components.etl.cleanup_files import cleanup_pipeline_directories_with_result
from reusable_components.etl.load_csv_to_iceberg_with_mapping import load_csv_to_iceberg_with_mapping
from reusable_components.etl.nucc_bronze_silver import transform_bronze_to_silver_nucc
from reusable_components.etl.nucc_silver_gold import transform_silver_to_gold_nucc
from reusable_components.file_processing.archive_files import archive_files_with_result
from reusable_components.file_processing.unzip_processor import unzip_files_with_result


def create_pipeline(config: dict):
    subject_area = config["subject_area"].lower()
    
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
    
    # DQ Transactions Asset
    @asset(
        name=f"load_dq_transactions_{subject_area}",
        description=f"Load DQ transactions for {subject_area} files",
        required_resource_keys={"snowflake_snowpark", "adls_access_keys"},
        ins={
            "monitor_result": AssetIn(config["asset_name"]),
            "audit_batch_id": AssetIn(f"start_dq_audit_run_{subject_area}")
        },
        group_name=config["group_name"]
    )
    @with_pipeline_alerts(
        pipeline_name=config["pipeline_name"],
        alert_config=config["alert_config"]
    )
    def load_dq_transactions(context: AssetExecutionContext, monitor_result, audit_batch_id: int) -> MaterializeResult:
        return load_dq_transactions_with_result(
            context=context,
            snowpark_session=context.resources.snowflake_snowpark,
            adls_client=context.resources.adls_access_keys,
            copy_result= None,
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
    
    # Copy CSV files to load
    @asset(
    name=f"copy_{subject_area}_files_to_load",
    description=f"Copy {subject_area} CSV files to load directory with timestamp removal",
    required_resource_keys={"adls_access_keys"},
    ins={"monitor_result": AssetIn(config["asset_name"])},
    group_name=config["group_name"]
    )
    @with_pipeline_alerts(
        pipeline_name=config["pipeline_name"],
        alert_config=config["alert_config"]
    )
    def copy_files_to_load(context: AssetExecutionContext, monitor_result) -> MaterializeResult:
        return unzip_files_with_result(
            context=context,
            adls_client=context.resources.adls_access_keys,
            monitor_result=monitor_result,
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

    # Archive CSV files
    @asset(
    name=f"archive_{subject_area}_files",
    description=f"Archive {subject_area} CSV files from stage to archive",
    required_resource_keys={"adls_access_keys"},
    ins={"monitor_result": AssetIn(config["asset_name"])},
    group_name=config["group_name"]
    )
    @with_pipeline_alerts(
        pipeline_name=config["pipeline_name"],
        alert_config=config["alert_config"]
    )
    def archive_files(context: AssetExecutionContext, monitor_result) -> MaterializeResult:
        return archive_files_with_result(
            context=context,
            adls_client=context.resources.adls_access_keys,
            monitor_result=monitor_result,
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

    # Schema Validation Asset
    @asset(
        name=f"dq_schema_check_{subject_area}",
        description=f"Validate file schemas for {subject_area} pipeline",
        required_resource_keys={"adls_access_keys", "snowflake_snowpark"},
        ins={"copy_result": AssetIn(f"copy_{subject_area}_files_to_load")},
        group_name=config["group_name"]
    )
    @with_pipeline_alerts(
        pipeline_name=config["pipeline_name"],
        alert_config=config["alert_config"]
    )
    def dq_schema_check(context: AssetExecutionContext, copy_result) -> MaterializeResult:
        return validate_all_file_schemas_with_result(
            adls_client=context.resources.adls_access_keys,
            container=config["stage_container"],
            folder_path=config["load_directory"],
            session=context.resources.snowflake_snowpark,
            context=context,
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
        return load_csv_to_iceberg_with_mapping(
            context=context,
            adls_client=context.resources.adls_access_keys,
            snowpark_session=context.resources.snowflake_snowpark,
            program_name=config["program_name"],
            subject_area=config["subject_area"],
            batch_id=audit_batch_id,
            container=config["stage_container"],
            folder=config["load_directory"],
            file_name=config["file_name"],
            mapping_file_path=config["column_mapping_file_path"],
            prefix=config["file_criteria"]["prefix"]["pattern"],
            suffix=config["file_criteria"]["suffix"]["pattern"],
            contains=config["file_criteria"]["contains"]["pattern"],
            not_contains=["CONTROL"], 
            regex=config["file_criteria"]["regex"]["pattern"],
            extension=[".csv"] 
        )
    
    #Bronze to silver
    @asset(
    name=f"transform_bronze_to_silver_{subject_area}",
    description=f"Transform bronze {subject_area} data to silver table",
    required_resource_keys={"snowflake_snowpark"},
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
    def transform_bronze_to_silver(context: AssetExecutionContext, audit_batch_id: int, iceberg_result) -> MaterializeResult:
        return transform_bronze_to_silver_nucc(
            context=context,
            snowpark_session=context.resources.snowflake_snowpark,
            audit_batch_id=audit_batch_id,
            config=config
    )

   #Silver to gold
    @asset(
        name=f"transform_silver_to_gold_{subject_area}",
        description="Transform silver NUCC data to gold using MD5 hash comparison",
        required_resource_keys={"snowflake_snowpark"},
        ins={
            "audit_batch_id": AssetIn(f"start_dq_audit_run_{subject_area}"),
            "silver_result": AssetIn(f"transform_bronze_to_silver_{subject_area}")
        },
        group_name=config["group_name"]
    )
    @with_pipeline_alerts(
        pipeline_name=config["pipeline_name"],
        alert_config=config["alert_config"]
    )
    def silver_to_gold_asset(context: AssetExecutionContext, audit_batch_id: int, silver_result) -> MaterializeResult:
        return transform_silver_to_gold_nucc(
            context=context,
            snowpark_session=context.resources.snowflake_snowpark,
            audit_batch_id=audit_batch_id,
            config=config
        )
    # Rules Assset
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
        "copy_result": AssetIn(f"copy_{subject_area}_files_to_load"),
        "archive_result": AssetIn(f"archive_{subject_area}_files"),
        "transactions_result": AssetIn(f"load_dq_transactions_{subject_area}"),
        "iceberg_result": AssetIn(f"load_csv_to_iceberg_{subject_area}"),
        "dq_result": AssetIn(f"execute_rules_asset_{subject_area}"),
        "bronze_to_silver": AssetIn(f"transform_bronze_to_silver_{subject_area}"),
        "silver_to_gold": AssetIn(f"transform_silver_to_gold_{subject_area}")
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
        "start_dq_audit_run": start_dq_audit_run,
        "load_dq_transactions": load_dq_transactions,
        "copy_files_to_load": copy_files_to_load,
        "archive_files": archive_files,
        "dq_schema_check": dq_schema_check,
        "load_csv_to_iceberg": load_csv_to_iceberg,
        "transform_bronze_to_silver":transform_bronze_to_silver,
        "transform_silver_to_gold":silver_to_gold_asset,
        "execute_rules_asset": execute_rules_asset,
        "cleanup_directories": cleanup_directories
    }
