from typing import List, Optional, Union, Dict, Any
import re
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue
from azure.storage.filedatalake import DataLakeServiceClient

def load_csv_to_iceberg_with_result(
    context: AssetExecutionContext,
    adls_client: DataLakeServiceClient,
    snowpark_session,
    audit_batch_id: int,
    schema_check_result: Dict[str, Any],
    config: Dict[str, Any],
    # NEW: Optional override parameters
    prefix: Optional[Union[str, List[str]]] = None,
    suffix: Optional[Union[str, List[str]]] = None,
    contains: Optional[Union[str, List[str]]] = None,
    not_contains: Optional[Union[str, List[str]]] = None,
    regex: Optional[str] = None,
    extension: Optional[Union[str, List[str]]] = None
) -> MaterializeResult:
    """
    Standardized CSV to Iceberg loading component with schema validation check.
    
    Args:
        context: Dagster execution context
        adls_client: Azure Data Lake client
        snowpark_session: Snowflake Snowpark session
        audit_batch_id: Batch ID for auditing
        schema_check_result: Result from schema validation
        config: Pipeline configuration dictionary
        prefix: Override prefix criteria (optional)
        suffix: Override suffix criteria (optional)
        contains: Override contains criteria (optional)
        not_contains: Override not_contains criteria (optional)
        regex: Override regex criteria (optional)
        extension: Override extension criteria (optional)
        
    Returns:
        MaterializeResult with loading status and metadata
    """
    
    # Extract configuration values
    pipeline_name = config["pipeline_name"]
    stage_container = config["stage_container"]
    load_directory = config["load_directory"]
    snowflake_db = config["snowflake_db"]
    snowflake_schema = config["snowflake_schema"]
    snowflake_stage = config["snowflake_stage"]
    program_name = config["program_name"]
    subject_area = config["subject_area"]
    file_criteria = config["file_criteria"]
    
    # Use override parameters if provided, otherwise use config values
    final_prefix = prefix if prefix is not None else file_criteria["prefix"]["pattern"]
    final_suffix = suffix if suffix is not None else file_criteria["suffix"]["pattern"]
    final_contains = contains if contains is not None else file_criteria["contains"]["pattern"]
    final_not_contains = not_contains if not_contains is not None else file_criteria["not_contains"]["pattern"]
    final_regex = regex if regex is not None else file_criteria["regex"]["pattern"]
    final_extension = extension if extension is not None else [".csv"]
    
    # Check if schema validation was successful
    schema_status = schema_check_result.get("status", "unknown")
    validation_passed = schema_check_result.get("validation_passed", False)
    
    if schema_status != "completed" or not validation_passed:
        context.log.info(f"❌ Skipping {pipeline_name} CSV to Iceberg load - schema validation failed: {schema_status}")
        
        return MaterializeResult(
            value={
                "status": "skipped", 
                "reason": f"Schema validation failed: {schema_status}",
                "pipeline_name": pipeline_name
            },
            metadata={
                "status": MetadataValue.text("⏭️ SKIPPED"),
                "reason": MetadataValue.text("Schema validation failed"),
                "pipeline_name": MetadataValue.text(pipeline_name),
                "batch_id": MetadataValue.int(audit_batch_id)
            }
        )
    
    context.log.info(f"🔄 {pipeline_name} loading CSV to Iceberg (after successful schema validation):")
    context.log.info(f"   Container: {stage_container}")
    context.log.info(f"   Folder: {load_directory}")
    context.log.info(f"   Database: {snowflake_db}")
    context.log.info(f"   Schema: {snowflake_schema}")
    context.log.info(f"   Stage: {snowflake_stage}")

    try:
        # Log the criteria being used for CSV loading
        context.log.info(f"🔍 {pipeline_name} CSV Loading Criteria (with overrides):")
        context.log.info(f"   - prefix: {final_prefix}")
        context.log.info(f"   - suffix: {final_suffix}")
        context.log.info(f"   - contains: {final_contains}")
        context.log.info(f"   - not_contains: {final_not_contains}")
        context.log.info(f"   - regex: {final_regex}")
        context.log.info(f"   - extension: {final_extension}")
        
        # Use the CSV loading function with final criteria
        result = copy_all_csv_to_iceberg(
            context=context,
            adls_client=adls_client,
            container=stage_container,
            folder=load_directory,
            prefix=final_prefix,
            suffix=final_suffix,
            contains=final_contains,
            not_contains=final_not_contains,
            regex=final_regex,
            extension=final_extension,
            db=snowflake_db,
            schema=snowflake_schema,
            stage_name=snowflake_stage,
            program_name=program_name,
            subject_area=subject_area,
            batch_id=audit_batch_id,
        )
        
        # Ensure result is a list and count tables loaded
        tables_loaded = result if isinstance(result, list) else []
        table_count = len(tables_loaded)
        
        context.log.info(f"✅ {pipeline_name} CSV to Iceberg load completed:")
        context.log.info(f"   Tables loaded: {table_count}")
        if tables_loaded:
            context.log.info(f"   Loaded tables: {', '.join(tables_loaded)}")
        
        return MaterializeResult(
            value={
                "status": "completed",
                "tables_loaded": tables_loaded,
                "table_count": table_count,
                "pipeline_name": pipeline_name,
                "batch_id": audit_batch_id,
                "validated_before_load": True,
                "file_criteria_used": {
                    "prefix": final_prefix,
                    "suffix": final_suffix,
                    "contains": final_contains,
                    "not_contains": final_not_contains,
                    "regex": final_regex,
                    "extension": final_extension
                }
            },
            metadata={
                "status": MetadataValue.text("✅ SUCCESS"),
                "tables_loaded": MetadataValue.int(table_count),
                "pipeline_name": MetadataValue.text(pipeline_name),
                "batch_id": MetadataValue.int(audit_batch_id),
                "validated_before_load": MetadataValue.bool(True),
                "loaded_tables": MetadataValue.text(", ".join(tables_loaded) if tables_loaded else "None"),
                "criteria_overrides": MetadataValue.text(f"prefix={final_prefix}, extension={final_extension}, not_contains={final_not_contains}")
            }
        )
        
    except Exception as e:
        context.log.error(f"❌ {pipeline_name} CSV to Iceberg load failed: {str(e)}")
        
        return MaterializeResult(
            value={
                "status": "failed", 
                "error": str(e), 
                "pipeline_name": pipeline_name,
                "table_count": 0
            },
            metadata={
                "status": MetadataValue.text("❌ FAILED"),
                "error": MetadataValue.text(str(e)[:200] + "..." if len(str(e)) > 200 else str(e)),
                "pipeline_name": MetadataValue.text(pipeline_name),
                "batch_id": MetadataValue.int(audit_batch_id),
                "tables_loaded": MetadataValue.int(0)
            }
        )


def copy_all_csv_to_iceberg(
    context: AssetExecutionContext,
    adls_client: DataLakeServiceClient,
    container: str,
    folder: str,
    db: str,
    schema: str,
    stage_name: str,
    program_name: str,
    subject_area: str,
    batch_id: int,
    prefix: Optional[Union[str, List[str]]] = None,
    suffix: Optional[Union[str, List[str]]] = None,
    contains: Optional[Union[str, List[str]]] = None,
    not_contains: Optional[Union[str, List[str]]] = None,
    regex: Optional[str] = None,
    extension: Optional[Union[str, List[str]]] = None
) -> List[str]:
    """
    Core CSV to Iceberg loading logic.
    
    1) List all CSV files in ADLS under container/folder that match the criteria
    2) For each file, strip off ".csv" to get table_name
    3) Issue COPY INTO db.schema.table_name FROM @stage_name/<path>
       using MATCH_BY_COLUMN_NAME='CASE_SENSITIVE'
    
    Returns the list of tables that were loaded.
    """
    log = context.log
    
    def file_matches_criteria(filename: str) -> bool:
        """Check if file matches all specified criteria"""
        
        # Check prefix criteria
        if prefix:
            if isinstance(prefix, list):
                if not any(filename.startswith(p) for p in prefix):
                    return False
            else:
                if not filename.startswith(prefix):
                    return False
        
        # Check suffix criteria
        if suffix:
            if isinstance(suffix, list):
                if not any(filename.endswith(s) for s in suffix):
                    return False
            else:
                if not filename.endswith(suffix):
                    return False
        
        # Check contains criteria
        if contains:
            if isinstance(contains, list):
                if not any(c in filename for c in contains):
                    return False
            else:
                if contains not in filename:
                    return False
        
        # Check not_contains criteria
        if not_contains:
            if isinstance(not_contains, list):
                if any(nc in filename for nc in not_contains):
                    return False
            else:
                if not_contains in filename:
                    return False
        
        # Check regex criteria
        if regex:
            if not re.search(regex, filename):
                return False
        
        # Check extension criteria
        if extension:
            if isinstance(extension, list):
                if not any(filename.lower().endswith(ext.lower()) for ext in extension):
                    return False
            else:
                if not filename.lower().endswith(extension.lower()):
                    return False
        
        return True
    
    # Use the client directly
    file_system = adls_client.get_file_system_client(container)
    log.info(f"🔍 Listing files in {container}/{folder} with criteria")
    
    # Collect all matching CSV files
    matching_files = []
    all_files_count = 0
    csv_files_count = 0
    
    for path in file_system.get_paths(path=folder):
        if path.is_directory:
            continue
            
        filename = path.name.split('/')[-1]
        all_files_count += 1
        
        # Must be CSV file first
        if not filename.lower().endswith('.csv'):
            continue
            
        csv_files_count += 1
        log.info(f"   📄 Found CSV file: {filename}")
        
        # Apply filtering criteria
        if file_matches_criteria(filename):
            matching_files.append(path.name)
            log.info(f"      ✅ Matches criteria: {filename}")
        else:
            log.info(f"      ❌ Doesn't match criteria: {filename}")
    
    log.info(f"📋 File Discovery Summary:")
    log.info(f"   Total files found: {all_files_count}")
    log.info(f"   CSV files found: {csv_files_count}")
    log.info(f"   CSV files matching criteria: {len(matching_files)}")
    
    if not matching_files:
        log.warning(f"⚠️ No CSV files found matching criteria in {container}/{folder}")
        log.info(f"   Criteria used:")
        log.info(f"     - prefix: {prefix}")
        log.info(f"     - suffix: {suffix}")
        log.info(f"     - contains: {contains}")
        log.info(f"     - not_contains: {not_contains}")
        log.info(f"     - regex: {regex}")
        log.info(f"     - extension: {extension}")
        return []
    
    loaded_tables: List[str] = []
    
    for full_path in matching_files:
        filename = full_path.rsplit("/", 1)[-1]
        
        # Remove .csv extension to get table name
        if filename.lower().endswith('.csv'):
            table_name = filename[:-4]  # Remove .csv
        else:
            table_name = filename
            
        full_table = f"{db}.{schema}.{table_name}"
        stage_path = f"@{db}.{schema}.{stage_name}/{full_path}"
        
        log.info(f"➡️ Copying {filename} → Iceberg table {full_table}")
        
        try:
            # Execute COPY INTO command
            copy_sql = f"""
                COPY INTO {full_table}
                FROM {stage_path}
                FILE_FORMAT = ANALYTYXONE_DEV.BRONZE.bronze_csv
                MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE'
                FORCE = TRUE
            """
            log.info(f"   SQL: {copy_sql}")
            context.resources.snowflake_snowpark.sql(copy_sql).collect()
            
            # Back-fill metadata on new rows
            log.info(f"📝 Back-filling metadata on new rows in {full_table}")
            metadata_sql = f"""
                UPDATE {full_table}
                SET
                    "META_PROGRAM_NAME" = '{program_name}',
                    "META_SUBJECT_AREA" = '{subject_area}',
                    "META_BATCH_ID" = {batch_id},
                    "META_ROW_ID" = DEFAULT,
                    "META_DATE_INSERT" = CURRENT_TIMESTAMP()
                WHERE "META_BATCH_ID" IS NULL
            """
            context.resources.snowflake_snowpark.sql(metadata_sql).collect()
            
            loaded_tables.append(table_name)
            log.info(f"✅ Successfully loaded table: {table_name}")
            
        except Exception as e:
            log.error(f"❌ Failed to load {filename} to {full_table}: {str(e)}")
            # Log the full error details for debugging
            import traceback
            log.error(f"   Full error: {traceback.format_exc()}")
            raise
    
    log.info(f"✅ Completed COPY for {len(loaded_tables)} tables: {loaded_tables}")
    return loaded_tables