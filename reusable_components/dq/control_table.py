from dagster import AssetExecutionContext, MaterializeResult, MetadataValue
from snowflake.snowpark import Session
from typing import Union


def load_dq_control_table_with_result(
    context: AssetExecutionContext,
    snowpark_session: Session,
    unzip_result: Union[dict, str],
    audit_batch_id: int,
    adls_container: str,
    folder_path: str,
    control_file: str,
    program_name: str,
    subject_area: str,
    pipeline_name: str
) -> MaterializeResult:
    """
    Complete control table logic: validation + control data loading + MaterializeResult.
    Returns MaterializeResult ready for the asset to return.
    """
    
    # Extract unzip status
    unzip_status = unzip_result.get("status", "unknown") if isinstance(unzip_result, dict) else "completed"
    
    if unzip_status in ["skipped", "failed"]:
        context.log.info(f"❌ Skipping {pipeline_name} control table load - unzip failed")
        
        return MaterializeResult(
            value={"status": "skipped", "reason": f"Unzip operation failed: {unzip_status}", "pipeline_name": pipeline_name},
            metadata={
                "status": MetadataValue.text("⏭️ SKIPPED"),
                "reason": MetadataValue.text("Previous step failed"),
                "pipeline_name": MetadataValue.text(pipeline_name),
                "batch_id": MetadataValue.int(audit_batch_id)
            }
        )
    
    try:
        context.log.info(f"📋 {pipeline_name} loading control table data for batch {audit_batch_id}")
        context.log.info(f"   Control file: {control_file}")
        context.log.info(f"   Source path: {folder_path}")
        
        # Use internal component to do the actual work
        _load_dq_control_data_internal(
            context=context,
            snowpark_session=snowpark_session,
            adls_container=adls_container,
            folder_path=folder_path,
            control_file=control_file,
            program_name=program_name,
            subject_area=subject_area,
            batch_id=audit_batch_id,
        )
        
        context.log.info(f"✅ {pipeline_name} control table data loaded successfully")
        
        # Return successful MaterializeResult
        return MaterializeResult(
            value={
                "status": "completed",
                "batch_id": audit_batch_id,
                "control_file_loaded": control_file,
                "pipeline_name": pipeline_name,
                "folder_path": folder_path
            },
            metadata={
                "status": MetadataValue.text("✅ SUCCESS"),
                "batch_id": MetadataValue.int(audit_batch_id),
                "pipeline_name": MetadataValue.text(pipeline_name),
                "control_file": MetadataValue.text(control_file),
                "program_name": MetadataValue.text(program_name),
                "subject_area": MetadataValue.text(subject_area)
            }
        )
        
    except Exception as e:
        context.log.error(f"❌ {pipeline_name} control table load failed: {str(e)}")
        
        # Return failed MaterializeResult
        return MaterializeResult(
            value={"status": "failed", "error": str(e), "pipeline_name": pipeline_name},
            metadata={
                "status": MetadataValue.text("❌ FAILED"),
                "error": MetadataValue.text(str(e)[:200] + "..." if len(str(e)) > 200 else str(e)),
                "pipeline_name": MetadataValue.text(pipeline_name),
                "batch_id": MetadataValue.int(audit_batch_id),
                "control_file": MetadataValue.text(control_file)
            }
        )
        raise


def _load_dq_control_data_internal(
    context: AssetExecutionContext,
    snowpark_session: Session,
    adls_container: str,
    folder_path: str,
    control_file: str,
    program_name: str,
    subject_area: str,
    batch_id: int,
) -> None:
    """
    Internal function that does the actual control table loading work.
    1. COPY INTO the two data columns (TABLE_NAME, ROW_COUNT) from the ADLS CSV.
    2. UPDATE the remaining metadata columns for any rows where META_BATCH_ID is NULL.
    """
    # Fully qualified target table
    full_table = "ANALYTYXONE_DEV.DATALOOM.DQ_CONTROL_TABLE"
    
    # Build the ADLS path string that your external stage is configured to point at
    adls_path = f"{folder_path}/{control_file}"
    stage_path = f"@ANALYTYXONE_DEV.DATALOOM.CSV_STAGE/{adls_path}"
    
    context.log.info(f"📋 Control Table Load Details:")
    context.log.info(f"   Target table: {full_table}")
    context.log.info(f"   Stage path: {stage_path}")
    context.log.info(f"   Program: {program_name}")
    context.log.info(f"   Subject area: {subject_area}")
    context.log.info(f"   Batch ID: {batch_id}")
    
    # 1) Load just the two columns from the CSV
    copy_sql = f"""
        COPY INTO {full_table}
        FROM '{stage_path}'
        FILE_FORMAT = ANALYTYXONE_DEV.DATALOOM.dataloom_csv
        MATCH_BY_COLUMN_NAME = 'case_sensitive'
        FORCE = TRUE
    """
    
    context.log.info(f"📋 Copying control file into {full_table}...")
    snowpark_session.sql(copy_sql).collect()
    
    # 2) Update the metadata columns for new rows
    update_sql = f"""
        UPDATE {full_table}
        SET
            "META_PROGRAM_NAME" = '{program_name}',
            "META_SUBJECT_AREA" = '{subject_area}',
            "META_BATCH_ID" = {batch_id},
            "META_DATE_INSERT" = CURRENT_TIMESTAMP()
        WHERE "META_BATCH_ID" IS NULL
    """
    
    context.log.info(f"📋 Updating metadata columns in {full_table}...")
    snowpark_session.sql(update_sql).collect()
    
    context.log.info(f"✅ Control table load completed successfully")


# Legacy function for backward compatibility
def load_dq_control_data(
    context: AssetExecutionContext,
    snowpark_session: Session,
    adls_container: str,
    folder_path: str,
    control_file: str,
    program_name: str,
    subject_area: str,
    batch_id: int,
) -> None:
    """
    Legacy function - calls the internal implementation.
    Kept for backward compatibility with existing code.
    """
    return _load_dq_control_data_internal(
        context=context,
        snowpark_session=snowpark_session,
        adls_container=adls_container,
        folder_path=folder_path,
        control_file=control_file,
        program_name=program_name,
        subject_area=subject_area,
        batch_id=batch_id,
    )