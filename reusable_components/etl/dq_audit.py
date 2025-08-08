from snowflake.snowpark import Session
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue
from datetime import datetime, timezone
from typing import Optional, Union, List, Dict, Any

DQ_AUDIT_TABLE = "ANALYTYXONE_DEV.DATALOOM.DQ_AUDIT"
UPDATED_BY = "ETL"

def standardize_monitor_result(monitor_result: Dict[str, Any]) -> Dict[str, Any]:

    if all(key in monitor_result for key in ["complete", "found_total", "expected_total"]):
        return monitor_result
    
    # NPPES format - convert it
    elif "status" in monitor_result and "file_ready" in monitor_result:
        if monitor_result.get("status") == "success" and monitor_result.get("file_ready"):
            return {
                "complete": True,
                "found_total": len(monitor_result.get("extracted_files", [])),
                "expected_total": 1
            }
        else:
            return {
                "complete": False,
                "found_total": 0,
                "expected_total": 1
            }
    
    # Fallback - return as-is
    else:
        return monitor_result

def create_dq_audit_entry(
    context: AssetExecutionContext,
    session: Session,
    monitor_result: dict,
    program_name: str,
    subject_area: str,
    pipeline_name: str,
    prefix: Optional[Union[str, List[str]]] = None,
    suffix: Optional[Union[str, List[str]]] = None,
    contains: Optional[Union[str, List[str]]] = None,
    not_contains: Optional[Union[str, List[str]]] = None,
    regex: Optional[str] = None,
    extension: Optional[Union[str, List[str]]] = None
) -> MaterializeResult:
    """
    Complete DQ audit asset logic: validation + audit entry creation + MaterializeResult.
    Returns MaterializeResult ready for the asset to return.
    """
    
    # AUTO-CONVERT monitor result format
    standardized_result = standardize_monitor_result(monitor_result)
    
    # Extract monitoring results (now guaranteed to be in standard format)
    files_complete = standardized_result.get("complete", False)
    files_found = standardized_result.get("found_total", 0)
    expected_count = standardized_result.get("expected_total", 0)
    
    context.log.info(f"🔍 {pipeline_name} DQ Audit Check:")
    context.log.info(f"   Files complete: {files_complete}")
    context.log.info(f"   Files found: {files_found}/{expected_count}")
    
    # Check if monitoring requirements are met
    if not files_complete or files_found != expected_count:
        context.log.info("❌ Skipping DQ Audit - file monitoring requirements not met")
        
        return MaterializeResult(
            metadata={
                "status": MetadataValue.text("❌ FAILED"),
                "reason": MetadataValue.text("File monitoring requirements not met"),
                "files_found": MetadataValue.int(files_found),
                "expected_files": MetadataValue.int(expected_count),
                "pipeline_name": MetadataValue.text(pipeline_name)
            }
        )
    
    if prefix:
        if isinstance(prefix, list):
            context.log.info(f"   Processing files with prefixes: {prefix}")
        else:
            context.log.info(f"   Processing files with prefix: {prefix}")
    
    try:
        # Calculate next batch ID
        context.log.info(f"📊 Creating DQ audit entry for {program_name}/{subject_area}")
        
        id_query = session.sql(f"""
            SELECT MAX(TRY_TO_NUMBER(ID_BATCH)) AS max_id
            FROM {DQ_AUDIT_TABLE}
            WHERE PROGRAM_NAME = '{program_name}'
              AND SUBJECT_AREA = '{subject_area}'
              AND CODE_LOAD_STATUS IN ('COMPLETED','FAILED','IN_PROGRESS')
        """)
        
        row = id_query.collect()[0]
        max_id = int(row["MAX_ID"]) if row["MAX_ID"] is not None else 0
        id_batch = max_id + 1 if max_id >= 1 else 1
        
        # Generate timestamps
        current_time = datetime.now(timezone.utc)
        code_load_status = "IN_PROGRESS"
        
        # Insert audit record
        session.sql(f"""
            INSERT INTO {DQ_AUDIT_TABLE} (
                ID_BATCH, PROGRAM_NAME, SUBJECT_AREA, CODE_LOAD_STATUS,
                DATE_BATCH_LOAD, UPDATED_BY
            ) VALUES (
                '{id_batch}', '{program_name}', '{subject_area}', '{code_load_status}',
                '{current_time.isoformat()}', '{UPDATED_BY}'
            )
        """).collect()
        
        context.log.info(f"✅ {pipeline_name} DQ audit entry created (ID: {id_batch})")
        
        # Return successful MaterializeResult
        return MaterializeResult(
            value=id_batch,
            metadata={
                "status": MetadataValue.text("✅ SUCCESS"),
                "batch_id": MetadataValue.int(id_batch),
                "pipeline_name": MetadataValue.text(pipeline_name),
                "files_processed": MetadataValue.int(files_found),
                "file_criteria_used": MetadataValue.text(f"prefix={prefix}, suffix={suffix}, contains={contains}")
            }
        )
        
    except Exception as e:
        context.log.error(f"❌ {pipeline_name} DQ audit entry failed: {str(e)}")
        
        # Return failed MaterializeResult
        return MaterializeResult(
            metadata={
                "status": MetadataValue.text("❌ ERROR"),
                "error": MetadataValue.text(str(e)[:200] + "..." if len(str(e)) > 200 else str(e)),
                "pipeline_name": MetadataValue.text(pipeline_name),
                "files_found": MetadataValue.int(files_found)
            }
        )
        raise