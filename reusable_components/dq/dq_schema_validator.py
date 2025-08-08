import os
from io import StringIO
import pandas as pd
from typing import Optional, List, Union
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.filedatalake import DataLakeServiceClient
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue
from snowflake.snowpark import Session

# hard-code your DQ_OBJECTS table here
DQ_TABLE = "ANALYTYXONE_DEV.DATALOOM.DQ_OBJECTS"


def validate_all_file_schemas_with_result(
    adls_client: DataLakeServiceClient,
    container: str,
    folder_path: str,
    session: Session,
    context: AssetExecutionContext,
    pipeline_name: str,
    dq_result: Optional[Union[dict, str]] = None,
    prefix: Optional[Union[str, List[str]]] = None,
    suffix: Optional[Union[str, List[str]]] = None,
    contains: Optional[Union[str, List[str]]] = None,
    not_contains: Optional[Union[str, List[str]]] = None,
    regex: Optional[str] = None,
    extension: Optional[Union[str, List[str]]] = None
) -> MaterializeResult:
    """
    Complete schema validation logic: validation + schema checking + MaterializeResult.
    Returns MaterializeResult ready for the asset to return.
    """
    
    if dq_result is not None:
        dq_status = dq_result.get("status", "unknown")
        
        if dq_status != "completed":
            context.log.info(f"❌ Skipping {pipeline_name} schema validation - DQ validation failed: {dq_status}")
            
            return MaterializeResult(
                value={
                    "status": "skipped",
                    "reason": f"DQ validation failed: {dq_status}",
                    "validation_passed": False,
                    "pipeline_name": pipeline_name
                },
                metadata={
                    "status": MetadataValue.text("⏭️ SKIPPED"),
                    "reason": MetadataValue.text("Previous step failed"),
                    "pipeline_name": MetadataValue.text(pipeline_name)
                }
            )
    else:
        context.log.info(f"🔍 {pipeline_name} starting schema validation")
    
    try:
        # Use internal component to do the actual validation work
        validation_result = _validate_all_file_schemas_internal(
            adls_client=adls_client,
            container=container,
            folder_path=folder_path,
            session=session,
            context=context,
            prefix=prefix,
            suffix=suffix,
            contains=contains,
            not_contains=not_contains,
            regex=regex,
            extension=extension
        )
        
        files_validated = validation_result.get("files_validated", 0)
        files_excluded = validation_result.get("files_excluded", 0)
        validation_details = validation_result.get("validation_details", [])
        
        context.log.info(f"✅ {pipeline_name} schema validation completed successfully")
        context.log.info(f"   Files validated: {files_validated}")
        context.log.info(f"   Files excluded: {files_excluded}")
        
        # Return successful MaterializeResult
        return MaterializeResult(
            value={
                "status": "completed",
                "validation_passed": True,
                "files_validated": files_validated,
                "files_excluded": files_excluded,
                "pipeline_name": pipeline_name,
                "validation_details": validation_details,
                "exclusion_criteria": not_contains
            },
            metadata={
                "status": MetadataValue.text("✅ SUCCESS"),
                "pipeline_name": MetadataValue.text(pipeline_name),
                "validation_result": MetadataValue.text("Schema validation passed"),
                "files_validated": MetadataValue.int(files_validated),
                "files_excluded": MetadataValue.int(files_excluded),
                "excluded_pattern": MetadataValue.text(str(not_contains) if not_contains else "None"),
                "file_criteria": MetadataValue.text(f"prefix={prefix}, extension={extension}")
            }
        )
        
    except Exception as e:
        context.log.error(f"❌ {pipeline_name} schema validation failed: {str(e)}")
        
        # Return failed MaterializeResult
        return MaterializeResult(
            value={
                "status": "failed",
                "error": str(e),
                "validation_passed": False,
                "pipeline_name": pipeline_name
            },
            metadata={
                "status": MetadataValue.text("❌ FAILED"),
                "error": MetadataValue.text(str(e)[:500] + "..." if len(str(e)) > 500 else str(e)),
                "pipeline_name": MetadataValue.text(pipeline_name),
                "validation_result": MetadataValue.text("Schema validation failed")
            }
        )
        # Re-raise the exception to ensure Dagster marks this as failed
        raise


def _validate_all_file_schemas_internal(
    adls_client: DataLakeServiceClient,
    container: str,
    folder_path: str,
    session: Session,
    context: AssetExecutionContext,
    prefix: Optional[Union[str, List[str]]] = None,
    suffix: Optional[Union[str, List[str]]] = None,
    contains: Optional[Union[str, List[str]]] = None,
    not_contains: Optional[Union[str, List[str]]] = None,
    regex: Optional[str] = None,
    extension: Optional[Union[str, List[str]]] = None
) -> dict:
    """
    Internal function that does the actual schema validation work.
    1) Lists every CSV under container/folder_path in ADLS with optional filtering.
    2) For each, derives object_name (filename sans .csv), pulls
       expected FIELD_NAMEs from DQ_OBJECTS.
    3) Downloads the CSV header, compares exact list.
    4) Raises Exception on any mismatch.
    """
    import re
    
    fs = adls_client.get_file_system_client(container)
    
    # Helper function to check if file matches all criteria
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
                if not any(filename.endswith(ext) for ext in extension):
                    return False
            else:
                if not filename.endswith(extension):
                    return False
        
        return True
    
    # List all files and apply filtering
    files_to_validate = []
    files_excluded = 0
    
    for path in fs.get_paths(path=folder_path):
        if path.is_directory:
            continue
            
        filename = os.path.basename(path.name)
        
        # Must be CSV file
        if not filename.lower().endswith(".csv"):
            continue
        
        # Apply filtering criteria
        if not file_matches_criteria(filename):
            context.log.info(f"🔍 Skipping {filename} (doesn't match criteria)")
            files_excluded += 1
            continue
            
        files_to_validate.append(path)
    
    context.log.info(f"🔍 Found {len(files_to_validate)} CSV files to validate")
    context.log.info(f"🔍 Excluded {files_excluded} files based on criteria")
    
    validation_errors = []
    validated_count = 0
    validation_details = []
    
    # Validate each file
    for path in files_to_validate:
        filename = os.path.basename(path.name)
        object_name = os.path.splitext(filename)[0]
        
        try:
            context.log.info(f"🔍 Validating schema for: {filename}")
            
            # Pull expected fields from Snowflake
            query = f"""
                SELECT FIELD_NAME
                FROM {DQ_TABLE}
                WHERE OBJECT_NAME = '{object_name}'
                ORDER BY ID_OBJECT
            """
            rows = session.sql(query).collect()
            expected = [r["FIELD_NAME"] for r in rows]
            
            if not expected:
                error_msg = f"No schema definition found in DQ_OBJECTS for object '{object_name}'"
                context.log.error(f"❌ {error_msg}")
                validation_errors.append(error_msg)
                validation_details.append({
                    "filename": filename,
                    "object_name": object_name,
                    "status": "FAILED",
                    "error": "No schema definition found",
                    "expected_columns": 0,
                    "actual_columns": None
                })
                continue
            
            # Download only the header of the CSV
            file_client = fs.get_file_client(path.name)
            try:
                raw = file_client.download_file().readall().decode("utf-8")
            except ResourceNotFoundError:
                error_msg = f"File not found in ADLS: {path.name}"
                context.log.error(f"❌ {error_msg}")
                validation_errors.append(error_msg)
                validation_details.append({
                    "filename": filename,
                    "object_name": object_name,
                    "status": "FAILED",
                    "error": "File not found",
                    "expected_columns": len(expected),
                    "actual_columns": None
                })
                continue
            
            # Read CSV header
            try:
                actual = pd.read_csv(StringIO(raw), nrows=0).columns.tolist()
            except Exception as e:
                error_msg = f"Failed to read CSV header for {filename}: {str(e)}"
                context.log.error(f"❌ {error_msg}")
                validation_errors.append(error_msg)
                validation_details.append({
                    "filename": filename,
                    "object_name": object_name,
                    "status": "FAILED",
                    "error": f"Failed to read CSV header: {str(e)}",
                    "expected_columns": len(expected),
                    "actual_columns": None
                })
                continue
            
            # Compare EXACTLY
            if actual != expected:
                missing = [c for c in expected if c not in actual]
                extra = [c for c in actual if c not in expected]
                error_msg = (
                    f"Schema mismatch for '{object_name}':\n"
                    f"  Expected: {expected}\n"
                    f"  Actual: {actual}\n"
                    f"  Missing columns: {missing}\n"
                    f"  Extra columns: {extra}"
                )
                context.log.error(f"❌ {error_msg}")
                validation_errors.append(error_msg)
                validation_details.append({
                    "filename": filename,
                    "object_name": object_name,
                    "status": "FAILED",
                    "error": "Schema mismatch",
                    "expected_columns": len(expected),
                    "actual_columns": len(actual),
                    "missing_columns": missing,
                    "extra_columns": extra
                })
            else:
                context.log.info(f"✅ Schema OK for '{object_name}' ({len(actual)} columns)")
                validated_count += 1
                validation_details.append({
                    "filename": filename,
                    "object_name": object_name,
                    "status": "PASSED",
                    "expected_columns": len(expected),
                    "actual_columns": len(actual)
                })
                
        except Exception as e:
            error_msg = f"Unexpected error validating {filename}: {str(e)}"
            context.log.error(f"❌ {error_msg}")
            validation_errors.append(error_msg)
            validation_details.append({
                "filename": filename,
                "object_name": object_name,
                "status": "FAILED",
                "error": f"Unexpected error: {str(e)}",
                "expected_columns": None,
                "actual_columns": None
            })
    
    # Summary logging
    context.log.info(f"📊 Schema Validation Summary:")
    context.log.info(f"   Files validated successfully: {validated_count}")
    context.log.info(f"   Files with errors: {len(validation_errors)}")
    context.log.info(f"   Files excluded by criteria: {files_excluded}")
    
    # If there were any validation errors, raise an exception to fail the asset
    if validation_errors:
        context.log.error(f"❌ Schema validation failed for {len(validation_errors)} files")
        
        # Create a comprehensive error message
        error_summary = f"Schema validation failed for {len(validation_errors)} files:\n\n"
        for i, error in enumerate(validation_errors, 1):
            error_summary += f"{i}. {error}\n\n"
        
        # Truncate if too long for better readability
        if len(error_summary) > 2000:
            error_summary = error_summary[:2000] + "...\n(truncated for readability)"
        
        # This will cause the asset to fail
        raise Exception(error_summary)
    
    context.log.info(f"✅ All {validated_count} files passed schema validation!")
    
    return {
        "files_validated": validated_count,
        "files_excluded": files_excluded,
        "validation_details": validation_details,
        "total_files_checked": len(files_to_validate)
    }


# Legacy function for backward compatibility
def validate_all_file_schemas(
    adls_client: DataLakeServiceClient,
    container: str,
    folder_path: str,
    session: Session,
    context: AssetExecutionContext,
    prefix: Optional[Union[str, List[str]]] = None,
    suffix: Optional[Union[str, List[str]]] = None,
    contains: Optional[Union[str, List[str]]] = None,
    not_contains: Optional[Union[str, List[str]]] = None,
    regex: Optional[str] = None,
    extension: Optional[Union[str, List[str]]] = None
) -> None:
    """
    Legacy function - calls the internal implementation.
    Kept for backward compatibility with existing code.
    """
    _validate_all_file_schemas_internal(
        adls_client=adls_client,
        container=container,
        folder_path=folder_path,
        session=session,
        context=context,
        prefix=prefix,
        suffix=suffix,
        contains=contains,
        not_contains=not_contains,
        regex=regex,
        extension=extension
    )