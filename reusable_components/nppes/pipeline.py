from dagster import asset, AssetExecutionContext, AssetIn, MaterializeResult, MetadataValue
from reusable_components.etl.dq_audit import create_dq_audit_entry
from reusable_components.error_handling.standardized_alerts import with_pipeline_alerts
from reusable_components.dq.dq_transactions import load_dq_transactions_with_result
from reusable_components.etl.adls_csv_to_snowflake_iceberg import load_csv_to_iceberg_with_result
from reusable_components.dq.dq_schema_validator import validate_all_file_schemas_with_result
from reusable_components.dq.dq_procedure import dq_rules_procedure
import os
import re
import time

def create_nppes_pipeline(config: dict):
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
        if monitor_result.get("status") == "success" and monitor_result.get("file_ready"):
            converted_monitor_result = {
                "complete": True,
                "found_total": len(monitor_result.get("extracted_files", [])),
                "expected_total": 1
            }
        else:
            converted_monitor_result = {
                "complete": False,
                "found_total": 0,
                "expected_total": 1
            }
        
        context.log.info(f"📊 Converting monitor result for DQ audit:")
        context.log.info(f"   Original: {monitor_result}")
        context.log.info(f"   Converted: {converted_monitor_result}")
        
        return create_dq_audit_entry(
            context=context,
            session=context.resources.snowflake_snowpark,
            monitor_result=converted_monitor_result,
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
        mock_copy_result = {"status": "completed", "copy_count": 1, "pipeline_name": config["pipeline_name"]}
        
        return load_dq_transactions_with_result(
            context=context,
            snowpark_session=context.resources.snowflake_snowpark,
            adls_client=context.resources.adls_access_keys,
            copy_result=mock_copy_result,
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
        """Copy CSV files from stage to load directory with timestamp removal"""
        if monitor_result.get("status") != "success":
            return MaterializeResult(
                value={"status": "skipped", "files_copied": 0},
                metadata={"status": MetadataValue.text("SKIPPED - Monitor failed")}
            )
        
        try:
            fs_client = context.resources.adls_access_keys.get_file_system_client(config["stage_container"])
            files_copied = 0
            processed_files = []
            
            for path in fs_client.get_paths(path=config["stage_directory"]):
                if path.is_directory:
                    continue
                
                filename = os.path.basename(path.name)
                if (any(filename.startswith(p) for p in config["file_criteria"]["prefix"]["pattern"]) and
                    filename.endswith(".csv")):
                    
                    context.log.info(f"Processing: {filename}")
                    
                    file_client = fs_client.get_file_client(path.name)
                    file_data = file_client.download_file().readall()
                    
                    clean_name = remove_timestamp(filename)
                    context.log.info(f"Cleaned name: {clean_name}")
                    
                    load_path = f"{config['load_directory']}/{clean_name}"
                    success = upload_large_file_to_adls(
                        context=context,
                        adls_client=context.resources.adls_access_keys,
                        file_content=file_data,
                        file_name=clean_name,
                        config=config,
                        target_directory=config['load_directory']
                    )
                    
                    if success:
                        files_copied += 1
                        processed_files.append(clean_name)
                        context.log.info(f"Copied: {clean_name}")
                    else:
                        context.log.error(f"Failed to copy: {clean_name}")
            
            if files_copied > 0:
                return MaterializeResult(
                    value={
                        "status": "completed",
                        "files_processed": files_copied,
                        "csv_files_copied": files_copied,
                        "processed_files": processed_files
                    },
                    metadata={
                        "status": MetadataValue.text("SUCCESS"),
                        "files_processed": MetadataValue.int(files_copied)
                    }
                )
            else:
                return MaterializeResult(
                    value={"status": "failed", "error": "No files were successfully copied"},
                    metadata={"status": MetadataValue.text("FAILED - No files copied")}
                )
                
        except Exception as e:
            context.log.error(f"Copy failed: {str(e)}")
            return MaterializeResult(
                value={"status": "failed", "error": str(e)},
                metadata={"status": MetadataValue.text("FAILED")}
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
        """Archive CSV files from stage to archive directory"""
        if monitor_result.get("status") != "success":
            return MaterializeResult(
                value={"status": "skipped", "files_archived": 0},
                metadata={"status": MetadataValue.text("SKIPPED - Monitor failed")}
            )
        
        try:
            fs_client = context.resources.adls_access_keys.get_file_system_client(config["stage_container"])
            files_archived = 0
            archived_files = []
            
            for path in fs_client.get_paths(path=config["stage_directory"]):
                if path.is_directory:
                    continue
                
                filename = os.path.basename(path.name)
                if (any(filename.startswith(p) for p in config["file_criteria"]["prefix"]["pattern"]) and
                    filename.endswith(".csv")):
                    
                    context.log.info(f"Archiving: {filename}")
                    
                    stage_client = fs_client.get_file_client(path.name)
                    file_data = stage_client.download_file().readall()
                    
                    archive_path = f"{config['archive_directory']}/{filename}"
                    success = upload_large_file_to_adls(
                        context=context,
                        adls_client=context.resources.adls_access_keys,
                        file_content=file_data,
                        file_name=filename,
                        config=config,
                        target_directory=config['archive_directory']
                    )
                    
                    if success:
                        files_archived += 1
                        archived_files.append(filename)
                        context.log.info(f"Archived: {filename}")
                    else:
                        context.log.error(f"Failed to archive: {filename}")
            
            if files_archived > 0:
                return MaterializeResult(
                    value={
                        "status": "completed",
                        "files_archived": files_archived,
                        "archived_files": archived_files
                    },
                    metadata={
                        "status": MetadataValue.text("SUCCESS"),
                        "files_archived": MetadataValue.int(files_archived)
                    }
                )
            else:
                return MaterializeResult(
                    value={"status": "failed", "error": "No files were successfully archived"},
                    metadata={"status": MetadataValue.text("FAILED - No files archived")}
                )
                
        except Exception as e:
            context.log.error(f"Archive failed: {str(e)}")
            return MaterializeResult(
                value={"status": "failed", "error": str(e)},
                metadata={"status": MetadataValue.text("FAILED")}
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
        mock_dq_result = {
        "status": "completed",  
        "pipeline_name": config["pipeline_name"]
        }
        return validate_all_file_schemas_with_result(
            adls_client=context.resources.adls_access_keys,
            container=config["stage_container"],
            folder_path=config["load_directory"],
            session=context.resources.snowflake_snowpark,
            context=context,
            dq_result=mock_dq_result,
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
        required_resource_keys={"snowflake_snowpark"},
        ins={
            "audit_batch_id": AssetIn(f"start_dq_audit_run_{subject_area}"),
            "iceberg_result": AssetIn(f"load_csv_to_iceberg_{subject_area}")},
        group_name=config["group_name"]
    )
    @with_pipeline_alerts(
        pipeline_name=config["pipeline_name"],
        alert_config=config["alert_config"]
    )
    def execute_rules_asset(context: AssetExecutionContext, audit_batch_id: int, iceberg_result):
        """
        Invokes the reusable dq_rules_procedure and returns:
        - procedure_output: the JSON dict from the proc
        - dq_run_log: list of log‑row dicts for this batch
        """
        dq_result = dq_rules_procedure(
            context=context,
            audit_batch_id=audit_batch_id,
            session=context.resources.snowflake_snowpark,
            rule_group=config.get("RULE_GROUP"),      
            rule_id=config.get("RULE_ID"),           
            refresh_summary=config.get("REFRESH_SUMMARY")
        )
        return dq_result
    
    return {
        "start_dq_audit_run": start_dq_audit_run,
        "load_dq_transactions": load_dq_transactions,
        "copy_files_to_load": copy_files_to_load,
        "archive_files": archive_files,
        "dq_schema_check": dq_schema_check,
        "load_csv_to_iceberg": load_csv_to_iceberg,
        "execute_rules_asset": execute_rules_asset
    }


def remove_timestamp(filename):
    """Remove timestamp from filename - keep everything before the timestamp"""
    name, ext = os.path.splitext(filename)
    
    patterns = [
        r'_\d{4}_\d{2}_\d{2}T\d{6}Z?.*',
        r'_\d{8}T\d{6}Z?.*',
        r'_\d{4}-\d{2}-\d{2}.*',
        r'_\d{8}.*',
        r'_\d{14}.*',
    ]
    
    for pattern in patterns:
        name = re.sub(pattern, '', name)
    
    name = name.rstrip('_-')
    return f"{name}{ext}"


def upload_large_file_to_adls(context, adls_client, file_content, file_name, config, target_directory):
    """
    Upload large file to ADLS with improved error handling and retry logic
    """
    
    try:
        fs_client = adls_client.get_file_system_client(config["stage_container"])
        file_path = f"{target_directory}/{file_name}"
        file_client = fs_client.get_file_client(file_path)
        
        file_size_mb = len(file_content) / (1024 * 1024)
        context.log.info(f"Uploading {file_name} ({file_size_mb:.1f} MB) to {file_path}")
        
        if file_size_mb <= 50:  
            context.log.info("Trying simple upload...")
            max_simple_retries = 3
            
            for simple_attempt in range(max_simple_retries):
                try:
                    file_client.upload_data(file_content, overwrite=True)
                    context.log.info("Simple upload succeeded")
                    return True
                except Exception as simple_error:
                    context.log.warning(f"Simple upload attempt {simple_attempt + 1} failed: {str(simple_error)[:100]}")
                    if simple_attempt < max_simple_retries - 1:
                        time.sleep(2 ** simple_attempt)  
    
        context.log.info("Using chunked upload...")
        chunk_size = 8 * 1024 * 1024  
        max_chunk_retries = 5
     
        try:
            file_client.create_file()
        except Exception as create_error:
            context.log.warning(f"Create file warning: {str(create_error)[:100]}")
            
        total_uploaded = 0
        
        while total_uploaded < len(file_content):
            chunk_end = min(total_uploaded + chunk_size, len(file_content))
            chunk_data = file_content[total_uploaded:chunk_end]
            
            chunk_uploaded = False
            for chunk_attempt in range(max_chunk_retries):
                try:
                    file_client.append_data(chunk_data, offset=total_uploaded)
                    chunk_uploaded = True
                    break
                    
                except Exception as chunk_error:
                    error_str = str(chunk_error)
                    context.log.warning(f"Chunk upload attempt {chunk_attempt + 1}/{max_chunk_retries} failed at offset {total_uploaded}: {error_str[:100]}")
                
                    if "ssl" in error_str.lower() or "eof" in error_str.lower() or "connection" in error_str.lower():
                        wait_time = min(30, 2 ** chunk_attempt)  
                        context.log.info(f"SSL/Connection error detected. Waiting {wait_time} seconds before retry...")
                        time.sleep(wait_time)
                    else:
                        time.sleep(1)
                    
                    if chunk_attempt == max_chunk_retries - 1:
                        context.log.error(f"Chunk upload failed permanently at offset {total_uploaded}")
                        return False
            
            if not chunk_uploaded:
                return False
                
            total_uploaded = chunk_end
            progress = (total_uploaded / len(file_content)) * 100
            context.log.info(f"Upload progress: {progress:.1f}% ({total_uploaded / (1024*1024):.1f} MB / {file_size_mb:.1f} MB)")
        
        flush_attempts = 3
        for flush_attempt in range(flush_attempts):
            try:
                file_client.flush_data(len(file_content))
                context.log.info("File flushed successfully")
                return True
            except Exception as flush_error:
                context.log.warning(f"Flush attempt {flush_attempt + 1} failed: {str(flush_error)[:100]}")
                if flush_attempt < flush_attempts - 1:
                    time.sleep(2)
                else:
                    context.log.error("Flush failed permanently")
                    return False
        
        return False
        
    except Exception as upload_error:
        context.log.error(f"Upload function failed: {str(upload_error)}")
        return False