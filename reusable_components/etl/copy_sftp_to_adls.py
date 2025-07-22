import re
from typing import List, Optional, Union
from paramiko import SFTPClient
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import AzureError
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue


def copy_files_sftp_to_adls(
    context: AssetExecutionContext,
    sftp_client: SFTPClient,
    adls_client: DataLakeServiceClient,
    file_monitor_result: dict,
    source_path: str,
    destination_container: str,
    destination_path: str,
    pipeline_name: str,
    prefix: Optional[Union[str, List[str]]] = None,
    suffix: Optional[Union[str, List[str]]] = None,
    contains: Optional[Union[str, List[str]]] = None,
    not_contains: Optional[Union[str, List[str]]] = None,
    regex: Optional[str] = None,
    extension: Optional[Union[str, List[str]]] = None,
    file_formats: Optional[List[str]] = None,
) -> MaterializeResult:
    """
    Complete SFTP to ADLS copy logic: validation + copy + MaterializeResult.
    Returns MaterializeResult ready for the asset to return.
    """
    
    # Extract monitoring results
    files_complete = file_monitor_result.get("complete", False)
    alert_was_sent = file_monitor_result.get("alert_sent", False)
    expected_count = file_monitor_result.get("expected_total", 0)
    found_count = file_monitor_result.get("found_total", 0)
    
    context.log.info(f"📁 {pipeline_name} File Copy Check:")
    context.log.info(f"   Complete: {files_complete}, Alert sent: {alert_was_sent}")
    context.log.info(f"   Files: {found_count}/{expected_count}")
    
    # Check if monitoring requirements are met
    if not files_complete or not alert_was_sent or found_count != expected_count:
        context.log.info("❌ Skipping copy - conditions not met")
        
        return MaterializeResult(
            value={
                "status": "failed", 
                "reason": "File monitoring conditions not met",
                "pipeline_name": pipeline_name,
                "error": f"Copy conditions not met: complete={files_complete}, alert={alert_was_sent}, count={found_count}/{expected_count}"
            },
            metadata={
                "status": MetadataValue.text("❌ FAILED"),
                "reason": MetadataValue.text("File monitoring conditions not met"),
                "pipeline_name": MetadataValue.text(pipeline_name)
            }
        )
    
    # Log file criteria
    context.log.info(f"🔍 {pipeline_name} Copy Criteria:")
    context.log.info(f"   - prefix: {prefix}")
    context.log.info(f"   - suffix: {suffix}")
    context.log.info(f"   - contains: {contains}")
    context.log.info(f"   - not_contains: {not_contains}")
    context.log.info(f"   - regex: {regex}")
    context.log.info(f"   - extension: {extension}")
    
    try:
        # Perform the actual copy
        copied = _copy_sftp_to_adls_internal(
            context=context,
            sftp_client=sftp_client,
            adls_client=adls_client,
            source_path=source_path,
            destination_container=destination_container,
            destination_path=destination_path,
            prefix=prefix,
            suffix=suffix,
            contains=contains,
            not_contains=not_contains,
            regex=regex,
            extension=extension,
            file_formats=file_formats
        )
        
        context.log.info(f"✅ {pipeline_name} copied {len(copied)} files to staging")
        
        # Validate copy count
        if len(copied) != expected_count:
            context.log.warning(f"⚠️ Copy count mismatch: copied {len(copied)}, expected {expected_count}")
            context.log.info("This might indicate files were already present or filtering criteria changed")
        
        # Return successful MaterializeResult
        return MaterializeResult(
            value={
                "status": "completed",
                "copied_files": copied,
                "copy_count": len(copied),
                "expected_count": expected_count,
                "pipeline_name": pipeline_name
            },
            metadata={
                "status": MetadataValue.text("✅ SUCCESS"),
                "files_copied": MetadataValue.int(len(copied)),
                "expected_files": MetadataValue.int(expected_count),
                "copy_efficiency": MetadataValue.text(f"{len(copied)}/{expected_count} files"),
                "pipeline_name": MetadataValue.text(pipeline_name),
                "destination": MetadataValue.text(f"{destination_container}/{destination_path}")
            }
        )
        
    except Exception as e:
        context.log.error(f"❌ {pipeline_name} copy operation failed: {str(e)}")
        
        # Return failed MaterializeResult
        return MaterializeResult(
            value={
                "status": "failed",
                "error": str(e),
                "pipeline_name": pipeline_name,
                "copy_count": 0
            },
            metadata={
                "status": MetadataValue.text("❌ ERROR"),
                "error": MetadataValue.text(str(e)[:200] + "..." if len(str(e)) > 200 else str(e)),
                "pipeline_name": MetadataValue.text(pipeline_name),
                "files_copied": MetadataValue.int(0)
            }
        )
        raise


def _copy_sftp_to_adls_internal(
    context: AssetExecutionContext,
    sftp_client: SFTPClient,
    adls_client: DataLakeServiceClient,
    source_path: str,
    destination_container: str,
    destination_path: str,
    prefix: Optional[Union[str, List[str]]] = None,
    suffix: Optional[Union[str, List[str]]] = None,
    contains: Optional[Union[str, List[str]]] = None,
    not_contains: Optional[Union[str, List[str]]] = None,
    regex: Optional[str] = None,
    extension: Optional[Union[str, List[str]]] = None,
    file_formats: Optional[List[str]] = None,
) -> List[str]:
    """
    Internal function that does the actual SFTP to ADLS copy work.
    Separated from the main function to keep the MaterializeResult logic clean.
    """
    dest_fs_client = adls_client.get_file_system_client(destination_container)
    
    # List files using the existing SFTP session
    try:
        files_list = sftp_client.listdir_attr(source_path)
    except Exception as e:
        context.log.error(f"Failed to list SFTP directory {source_path}: {e}")
        raise
    
    copied_files: List[str] = []
    
    for entry in files_list:
        fname = entry.filename
        full_sftp_path = f"{source_path}/{fname}"
        
        # Apply all filters inline
        should_copy = True
        
        # Check prefix criteria
        if prefix and should_copy:
            if isinstance(prefix, list):
                should_copy = any(fname.startswith(p) for p in prefix)
            else:
                should_copy = fname.startswith(prefix)
        
        # Check suffix criteria
        if suffix and should_copy:
            if isinstance(suffix, list):
                should_copy = any(fname.endswith(s) for s in suffix)
            else:
                should_copy = fname.endswith(suffix)
        
        # Check contains criteria
        if contains and should_copy:
            if isinstance(contains, list):
                should_copy = any(c in fname for c in contains)
            else:
                should_copy = contains in fname
        
        # Check not_contains criteria
        if not_contains and should_copy:
            if isinstance(not_contains, list):
                should_copy = not any(nc in fname for nc in not_contains)
            else:
                should_copy = not_contains not in fname
        
        # Check regex criteria
        if regex and should_copy:
            should_copy = bool(re.search(regex, fname))
        
        # Check extension criteria
        if extension and should_copy:
            if isinstance(extension, list):
                should_copy = any(fname.endswith(ext) for ext in extension)
            else:
                should_copy = fname.endswith(extension)
        
        # Filter by file_formats (legacy parameter)
        if file_formats and should_copy:
            should_copy = any(fname.lower().endswith(ext.lower()) for ext in file_formats)
        
        if not should_copy:
            continue
        
        # Prepare Destination ADLS path
        adls_path = f"{destination_path}/{fname}"
        dest_file_client = dest_fs_client.get_file_client(adls_path)
        
        if dest_file_client.exists():
            raise Exception(f"Destination file '{destination_container}/{adls_path}' already exists.")
        
        # Copy file
        try:
            with sftp_client.open(full_sftp_path, 'rb') as f:
                data = f.read()
            dest_file_client.upload_data(data, overwrite=True)
            context.log.info(f"✅ Copied '{full_sftp_path}' -> '{destination_container}/{adls_path}'")
            copied_files.append(f"{destination_container}/{adls_path}")
        except Exception as e:
            context.log.error(f"❌ Failed to copy {fname}: {e}")
            raise
    
    return copied_files