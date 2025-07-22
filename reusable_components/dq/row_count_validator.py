from io import BytesIO
import pandas as pd
import re
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue
from dagster_azure.adls2 import ADLS2Resource
from snowflake.snowpark import Session
from azure.core.exceptions import ResourceNotFoundError
from snowflake.snowpark.functions import col
from typing import Union, Optional, List


def run_dq_row_count_validation_with_result(
    context: AssetExecutionContext,
    session: Session,
    adls2: ADLS2Resource,
    control_table_result: Union[dict, str],
    audit_batch_id: int,
    container: str,
    folder_path: str,
    program_name: str,
    subject_area: str,
    pipeline_name: str,
    # NEW: Optional file filtering parameters
    prefix: Optional[Union[str, List[str]]] = None,
    suffix: Optional[Union[str, List[str]]] = None,
    contains: Optional[Union[str, List[str]]] = None,
    not_contains: Optional[Union[str, List[str]]] = None,
    regex: Optional[str] = None,
    extension: Optional[Union[str, List[str]]] = None
) -> MaterializeResult:
    """
    Complete row count validation logic with file filtering support.
    
    NEW: Now supports filtering which files to validate based on criteria:
    - prefix: File must start with these patterns
    - suffix: File must end with these patterns  
    - contains: File must contain these patterns
    - not_contains: File must NOT contain these patterns
    - regex: File must match this regex pattern
    - extension: File must have these extensions
    
    Returns MaterializeResult ready for the asset to return.
    """
    
    control_status = control_table_result.get("status", "unknown")
    
    if control_status != "completed":
        context.log.info(f"❌ Skipping {pipeline_name} DQ validation - control table load failed: {control_status}")
        
        return MaterializeResult(
            value={"status": "skipped", "reason": f"Control table load failed: {control_status}", "pipeline_name": pipeline_name},
            metadata={
                "status": MetadataValue.text("⏭️ SKIPPED"),
                "reason": MetadataValue.text("Previous step failed"),
                "pipeline_name": MetadataValue.text(pipeline_name),
                "batch_id": MetadataValue.int(audit_batch_id)
            }
        )
    
    try:
        context.log.info(f"🔍 {pipeline_name} starting DQ validation for batch {audit_batch_id}")
        
        # Log filtering criteria if provided
        if any([prefix, suffix, contains, not_contains, regex, extension]):
            context.log.info(f"🔍 {pipeline_name} File Filtering Criteria:")
            if prefix:
                context.log.info(f"   - prefix: {prefix}")
            if suffix:
                context.log.info(f"   - suffix: {suffix}")
            if contains:
                context.log.info(f"   - contains: {contains}")
            if not_contains:
                context.log.info(f"   - not_contains: {not_contains}")
            if regex:
                context.log.info(f"   - regex: {regex}")
            if extension:
                context.log.info(f"   - extension: {extension}")
        
        # Use internal component to do the actual validation work
        validation_result = _run_dq_row_count_validation_internal(
            context=context,
            session=session,
            adls2=adls2,
            container=container,
            folder_path=folder_path,
            program_name=program_name,
            subject_area=subject_area,
            batch_id=audit_batch_id,
            # Pass filtering criteria
            prefix=prefix,
            suffix=suffix,
            contains=contains,
            not_contains=not_contains,
            regex=regex,
            extension=extension
        )
        
        files_validated = validation_result.get("files_validated", 0)
        files_filtered_out = validation_result.get("files_filtered_out", 0)
        validation_details = validation_result.get("validation_details", [])
        
        context.log.info(f"✅ {pipeline_name} validation completed:")
        context.log.info(f"   Files validated: {files_validated}")
        if files_filtered_out > 0:
            context.log.info(f"   Files filtered out: {files_filtered_out}")
        
        # Return successful MaterializeResult
        return MaterializeResult(
            value={
                "status": "completed",
                "batch_id": audit_batch_id,
                "validation_passed": True,
                "pipeline_name": pipeline_name,
                "files_validated": files_validated,
                "files_filtered_out": files_filtered_out,
                "validation_details": validation_details,
                "filtering_applied": any([prefix, suffix, contains, not_contains, regex, extension])
            },
            metadata={
                "status": MetadataValue.text("✅ SUCCESS"),
                "batch_id": MetadataValue.int(audit_batch_id),
                "pipeline_name": MetadataValue.text(pipeline_name),
                "validation_result": MetadataValue.text("All files passed validation"),
                "files_validated": MetadataValue.int(files_validated),
                "files_filtered_out": MetadataValue.int(files_filtered_out),
                "program_name": MetadataValue.text(program_name),
                "subject_area": MetadataValue.text(subject_area),
                "filtering_criteria": MetadataValue.text(
                    f"prefix={prefix}, not_contains={not_contains}, extension={extension}" 
                    if any([prefix, suffix, contains, not_contains, regex, extension])
                    else "No filtering applied"
                )
            }
        )
        
    except Exception as e:
        context.log.error(f"❌ {pipeline_name} DQ validation failed: {str(e)}")
        
        # Return failed MaterializeResult
        return MaterializeResult(
            value={"status": "failed", "error": str(e), "validation_passed": False, "pipeline_name": pipeline_name},
            metadata={
                "status": MetadataValue.text("❌ FAILED"),
                "error": MetadataValue.text(str(e)[:300] + "..." if len(str(e)) > 300 else str(e)),
                "pipeline_name": MetadataValue.text(pipeline_name),
                "batch_id": MetadataValue.int(audit_batch_id),
                "program_name": MetadataValue.text(program_name),
                "subject_area": MetadataValue.text(subject_area)
            }
        )


def _run_dq_row_count_validation_internal(
    context: AssetExecutionContext,
    session: Session,
    adls2: ADLS2Resource,
    container: str,
    folder_path: str,
    program_name: str,
    subject_area: str,
    batch_id: int,
    prefix: Optional[Union[str, List[str]]] = None,
    suffix: Optional[Union[str, List[str]]] = None,
    contains: Optional[Union[str, List[str]]] = None,
    not_contains: Optional[Union[str, List[str]]] = None,
    regex: Optional[str] = None,
    extension: Optional[Union[str, List[str]]] = None
) -> dict:
    """
     1) Load TABLE_NAME/ROW_COUNT from Snowflake (fully qualified).
    2) For each entry, pull <TABLE_NAME>.csv from ADLS and compare its row count.
    3) Raise if any files are missing or counts don’t match.
    """
    
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
    
    # Load just the two needed columns in one go
    control_df = (
        session
        .table(f"ANALYTYXONE_DEV.DATALOOM.DQ_CONTROL_TABLE")
        .filter(
            (col("META_BATCH_ID")      == batch_id) &
            (col("META_PROGRAM_NAME")  == program_name) &
            (col("META_SUBJECT_AREA")  == subject_area)
        )
        .select("TABLE_NAME", "ROW_COUNT")
        .to_pandas()
    )
    
    if control_df.empty:
        context.log.warning(f"⚠️ No control records found for batch {batch_id}, program {program_name}, subject {subject_area}")
        return {
            "files_validated": 0,
            "files_filtered_out": 0,
            "validation_details": [],
            "control_records_found": 0
        }
    
    context.log.info(f"📊 Found {len(control_df)} control records")
    
    # Apply filtering to determine which files to validate
    files_to_validate = []
    files_filtered_out = 0
    
    for table_name, expected in control_df.itertuples(index=False, name=None):
        filename = f"{table_name}.csv"
        
        if file_matches_criteria(filename):
            files_to_validate.append((table_name, int(expected)))
            context.log.info(f"   ✅ Will validate: {filename}")
        else:
            files_filtered_out += 1
            context.log.info(f"   🚫 Filtered out: {filename} (doesn't match criteria)")
    
    context.log.info(f"📋 Filtering Results:")
    context.log.info(f"   Total control records: {len(control_df)}")
    context.log.info(f"   Files to validate: {len(files_to_validate)}")
    context.log.info(f"   Files filtered out: {files_filtered_out}")
    
    if not files_to_validate:
        context.log.warning(f"⚠️ No files match the filtering criteria")
        return {
            "files_validated": 0,
            "files_filtered_out": files_filtered_out,
            "validation_details": [],
            "control_records_found": len(control_df)
        }
    
    fs = adls2.get_file_system_client(container)
    errors = []
    validation_details = []
    files_validated = 0
    
    for table_name, expected in files_to_validate:
        expected = int(expected)
        path = f"{folder_path}/{table_name}.csv"
        
        try:
            context.log.info(f"🔍 Validating: {table_name}.csv")
            
            blob = fs.get_file_client(path)
            data = blob.download_file().readall()
            actual = len(pd.read_csv(BytesIO(data), low_memory=False))
            
            context.log.info(f"   📊 {table_name}.csv → expected {expected}, got {actual}")
            
            if actual != expected:
                error_msg = f"{table_name}.csv: {actual} rows (expected {expected})"
                errors.append(error_msg)
                validation_details.append({
                    "table_name": table_name,
                    "expected_rows": expected,
                    "actual_rows": actual,
                    "status": "FAILED",
                    "error": f"Row count mismatch: expected {expected}, got {actual}"
                })
                context.log.error(f"   ❌ {error_msg}")
            else:
                files_validated += 1
                validation_details.append({
                    "table_name": table_name,
                    "expected_rows": expected,
                    "actual_rows": actual,
                    "status": "PASSED"
                })
                context.log.info(f"   ✅ {table_name}.csv: Row count matches ({actual} rows)")
                
        except ResourceNotFoundError:
            error_msg = f"{table_name}.csv: file not found"
            errors.append(error_msg)
            validation_details.append({
                "table_name": table_name,
                "expected_rows": expected,
                "actual_rows": None,
                "status": "FAILED",
                "error": "File not found"
            })
            context.log.error(f"   ❌ {error_msg}")
            
        except Exception as e:
            error_msg = f"{table_name}.csv: validation error - {str(e)}"
            errors.append(error_msg)
            validation_details.append({
                "table_name": table_name,
                "expected_rows": expected,
                "actual_rows": None,
                "status": "FAILED",
                "error": str(e)
            })
            context.log.error(f"   ❌ {error_msg}")
    
    # Summary logging
    context.log.info(f"📊 Validation Summary:")
    context.log.info(f"   Files checked: {len(files_to_validate)}")
    context.log.info(f"   Files passed: {files_validated}")
    context.log.info(f"   Files failed: {len(errors)}")
    context.log.info(f"   Files filtered out: {files_filtered_out}")
    
    if errors:
        context.log.error(f"❌ Validation failures:")
        for error in errors:
            context.log.error(f"   • {error}")
        
        raise RuntimeError("DQ row-count validation failed:\n" + "\n".join(errors))
    
    return {
        "files_validated": files_validated,
        "files_filtered_out": files_filtered_out,
        "validation_details": validation_details,
        "control_records_found": len(control_df)
    }


# Legacy function for backward compatibility
def run_dq_row_count_validation(
    context: AssetExecutionContext,
    session: Session,
    adls2: ADLS2Resource,
    container: str,
    folder_path: str,
    program_name: str,
    subject_area: str,
    batch_id: int,
) -> None:
    """
    Legacy function - calls the internal implementation without filtering.
    Kept for backward compatibility with existing code.
    """
    _run_dq_row_count_validation_internal(
        context=context,
        session=session,
        adls2=adls2,
        container=container,
        folder_path=folder_path,
        program_name=program_name,
        subject_area=subject_area,
        batch_id=batch_id,
    )