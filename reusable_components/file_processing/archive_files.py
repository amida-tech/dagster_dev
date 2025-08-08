import os
from typing import List, Optional, Union
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue
from azure.storage.filedatalake import DataLakeServiceClient


def archive_files_with_result(
    context: AssetExecutionContext,
    adls_client: DataLakeServiceClient,
    stage_container: str,
    stage_directory: str,
    archive_directory: str,
    pipeline_name: str,
    copy_result: Optional[dict] = None,     
    monitor_result: Optional[dict] = None,  
    prefix: Optional[Union[str, List[str]]] = None,
    suffix: Optional[Union[str, List[str]]] = None,
    contains: Optional[Union[str, List[str]]] = None,
    not_contains: Optional[Union[str, List[str]]] = None,
    regex: Optional[str] = None,
    extension: Optional[Union[str, List[str]]] = None
) -> MaterializeResult:
    """
    Complete archive logic: validation + file archiving + MaterializeResult.
    - If copy_result provided: validates copy operation first (Medicaid flow - archives ZIP files)
    - If monitor_result provided: validates monitor status first (NPPES flow - archives CSV files)
    Returns MaterializeResult ready for the asset to return.
    """
    
    if copy_result is not None:
        copy_status = copy_result.get("status", "unknown")
        
        context.log.info(f"📦 {pipeline_name} Copy Validation for Archive:")
        context.log.info(f"   Copy status: {copy_status}")
        
        if copy_status != "completed":
            context.log.info(f"❌ Skipping {pipeline_name} archive - copy status: {copy_status}")
            
            return MaterializeResult(
                value={"status": "skipped", "files_archived": 0, "pipeline_name": pipeline_name},
                metadata={
                    "status": MetadataValue.text("⏭️ SKIPPED"),
                    "reason": MetadataValue.text(f"Copy status: {copy_status}"),
                    "pipeline_name": MetadataValue.text(pipeline_name)
                }
            )
    
    elif monitor_result is not None:
        monitor_status = monitor_result.get("status", "unknown")
        file_ready = monitor_result.get("file_ready", False)
        
        context.log.info(f"📦 {pipeline_name} Monitor Validation for Archive:")
        context.log.info(f"   Monitor status: {monitor_status}, File ready: {file_ready}")
        
        if monitor_status != "success" or not file_ready:
            context.log.info(f"❌ Skipping {pipeline_name} archive - files not ready")
            
            return MaterializeResult(
                value={"status": "skipped", "files_archived": 0, "pipeline_name": pipeline_name},
                metadata={
                    "status": MetadataValue.text("⏭️ SKIPPED"),
                    "reason": MetadataValue.text("Files not ready for archiving"),
                    "pipeline_name": MetadataValue.text(pipeline_name)
                }
            )
    
    else:
        context.log.info(f"📦 {pipeline_name} - No upstream validation required for archive")
    
    try:
        result = _archive_files_internal(
            context=context,
            adls_client=adls_client,
            stage_container=stage_container,
            stage_directory=stage_directory,
            archive_directory=archive_directory,
            pipeline_name=pipeline_name,
            prefix=prefix,
            suffix=suffix,
            contains=contains,
            not_contains=not_contains,
            regex=regex,
            extension=extension
        )
        
        archived_count = result.get("files_archived", 0)
        archived_files = result.get("archived_files", [])
        
        context.log.info(f"🎉 {pipeline_name} archived {archived_count} files")
        
        # Return successful MaterializeResult
        return MaterializeResult(
            value={
                "status": "completed",
                "files_archived": archived_count,
                "archived_files": archived_files,
                "pipeline_name": pipeline_name,
                "archive_criteria": {
                    "extension": extension,
                    "prefix": prefix,
                    "suffix": suffix,
                    "contains": contains,
                    "not_contains": not_contains
                }
            },
            metadata={
                "status": MetadataValue.text("✅ SUCCESS"),
                "files_archived": MetadataValue.int(archived_count),
                "pipeline_name": MetadataValue.text(pipeline_name),
                "archive_directory": MetadataValue.text(archive_directory),
                "archived_files": MetadataValue.text(", ".join(archived_files) if archived_files else "None"),
                "file_criteria_used": MetadataValue.text(f"prefix={prefix}, extension={extension}")
            }
        )
        
    except Exception as e:
        context.log.error(f"❌ {pipeline_name} archive operation failed: {str(e)}")
        
        # Return failed MaterializeResult
        return MaterializeResult(
            value={
                "status": "failed",
                "error": str(e),
                "pipeline_name": pipeline_name,
                "files_archived": 0
            },
            metadata={
                "status": MetadataValue.text("❌ ERROR"),
                "error": MetadataValue.text(str(e)[:200] + "..." if len(str(e)) > 200 else str(e)),
                "pipeline_name": MetadataValue.text(pipeline_name),
                "files_archived": MetadataValue.int(0)
            }
        )
        raise


def _file_matches_archive_criteria(filename: str,
                                  prefix: Optional[Union[str, List[str]]] = None,
                                  suffix: Optional[Union[str, List[str]]] = None,
                                  contains: Optional[Union[str, List[str]]] = None,
                                  not_contains: Optional[Union[str, List[str]]] = None,
                                  regex: Optional[str] = None,
                                  extension: Optional[Union[str, List[str]]] = None) -> bool:
    """Check if file matches all specified archive criteria"""
    import re
    
    # Check extension criteria
    if extension:
        if isinstance(extension, list):
            if not any(filename.lower().endswith(ext.lower()) for ext in extension):
                return False
        else:
            if not filename.lower().endswith(extension.lower()):
                return False
    
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
    
    return True


def _archive_files_internal(
    context: AssetExecutionContext,
    adls_client: DataLakeServiceClient,
    stage_container: str,
    stage_directory: str,
    archive_directory: str,
    pipeline_name: str,
    prefix: Optional[Union[str, List[str]]] = None,
    suffix: Optional[Union[str, List[str]]] = None,
    contains: Optional[Union[str, List[str]]] = None,
    not_contains: Optional[Union[str, List[str]]] = None,
    regex: Optional[str] = None,
    extension: Optional[Union[str, List[str]]] = None
) -> dict:
    """
    Internal function that does the actual file archiving work.
    Separated from the main function to keep the MaterializeResult logic clean.
    """
    
    # Get ADLS client
    filesystem_client = adls_client.get_file_system_client(stage_container)
    
    context.log.info(f"📦 {pipeline_name} Archive Operation:")
    context.log.info(f"   Source: {stage_container}/{stage_directory}")
    context.log.info(f"   Destination: {stage_container}/{archive_directory}")
    context.log.info(f"   Criteria: prefix={prefix}, extension={extension}")
    
    # Get files from stage using configuration with proper filtering
    stage_files = []
    
    try:
        for path in filesystem_client.get_paths(path=stage_directory):
            if not path.is_directory:
                filename = os.path.basename(path.name)
                
                context.log.info(f"🔍 Checking file: {filename}")
                
                # Apply file matching criteria
                if _file_matches_archive_criteria(
                    filename,
                    prefix=prefix,
                    suffix=suffix,
                    contains=contains,
                    not_contains=not_contains,
                    regex=regex,
                    extension=extension
                ):
                    stage_files.append(path.name)
                    context.log.info(f"   ✅ Will archive: {filename}")
                else:
                    context.log.info(f"   ❌ Skipping: {filename} (doesn't match criteria)")
    
    except Exception as e:
        context.log.error(f"❌ Error listing files in {stage_directory}: {str(e)}")
        raise
    
    context.log.info(f"📦 {pipeline_name} found {len(stage_files)} files to archive")
    
    archived_count = 0
    archived_files = []
    failed_files = []
    
    # Copy each file to archive
    for file_path in stage_files:
        filename = os.path.basename(file_path)
        archive_path = f"{archive_directory}/{filename}"
        
        try:
            context.log.info(f"📦 Archiving: {filename}")
            
            # Read from stage
            stage_client = filesystem_client.get_file_client(file_path)
            file_data = stage_client.download_file().readall()
            
            # Write to archive
            archive_client = filesystem_client.get_file_client(archive_path)
            archive_client.create_file()
            archive_client.append_data(file_data, offset=0, length=len(file_data))
            archive_client.flush_data(len(file_data))
            
            archived_count += 1
            archived_files.append(filename)
            context.log.info(f"✅ {pipeline_name} archived: {filename}")
            
        except Exception as e:
            context.log.error(f"❌ Failed to archive {filename}: {str(e)}")
            failed_files.append(filename)
            # Continue with other files instead of failing completely
            continue
    
    # Log summary
    context.log.info(f"📦 Archive Summary:")
    context.log.info(f"   Files found: {len(stage_files)}")
    context.log.info(f"   Files archived: {archived_count}")
    context.log.info(f"   Files failed: {len(failed_files)}")
    
    if failed_files:
        context.log.warning(f"   Failed files: {failed_files}")
    
    return {
        "files_archived": archived_count,
        "archived_files": archived_files,
        "failed_files": failed_files,
        "total_files_found": len(stage_files)
    }