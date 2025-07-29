import re
import time
from typing import List, Optional, Union
from paramiko import SFTPClient
from azure.storage.filedatalake import DataLakeServiceClient
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
    
    try:
        # Configure SFTP for better reliability
        sftp_client.get_channel().settimeout(600)  # 10 minutes timeout
        sftp_client.get_channel().transport.set_keepalive(30)  # Keep connection
        
        # Get file system client
        dest_fs_client = adls_client.get_file_system_client(destination_container)
        
        # List files
        files_list = sftp_client.listdir_attr(source_path)
        
        # Filter and copy files
        copied_files = []
        total_start_time = time.time()
        
        for entry in files_list:
            fname = entry.filename
            file_size = entry.st_size if hasattr(entry, 'st_size') else 0
            size_mb = file_size / (1024 * 1024)
            
            # Apply filters
            if not _should_copy_file(fname, prefix, suffix, contains, not_contains, regex, extension, file_formats):
                continue
            
            full_sftp_path = f"{source_path}/{fname}"
            adls_path = f"{destination_path}/{fname}"
            
            # Check if file already exists
            dest_file_client = dest_fs_client.get_file_client(adls_path)
            if dest_file_client.exists():
                context.log.warning(f"⚠️ File already exists: {fname}")
                continue
            
            # Copy file with progress logging
            context.log.info(f"🚀 Starting: {fname} ({size_mb:.1f} MB)")
            file_start_time = time.time()
            
            try:
                # Simple copy with progress updates
                with sftp_client.open(full_sftp_path, 'rb') as sftp_file:
                    if size_mb < 10:  # Small files - copy at once
                        data = sftp_file.read()
                        dest_file_client.upload_data(data, overwrite=True)
                    else:  # Large files - copy in chunks
                        _copy_large_file_with_progress(
                            context, sftp_file, dest_file_client, fname, file_size, size_mb
                        )
                
                file_end_time = time.time()
                file_time = file_end_time - file_start_time
                speed = size_mb / file_time if file_time > 0 else 0
                
                context.log.info(f"✅ Completed: {fname} ({size_mb:.1f} MB in {file_time:.1f}s - {speed:.1f} MB/s)")
                copied_files.append(f"{destination_container}/{adls_path}")
                
            except Exception as e:
                context.log.error(f"❌ Failed to copy {fname}: {str(e)}")
                raise
        
        total_end_time = time.time()
        total_time = total_end_time - total_start_time
        total_size = sum(entry.st_size for entry in files_list 
                        if hasattr(entry, 'st_size') and 
                        _should_copy_file(entry.filename, prefix, suffix, contains, not_contains, regex, extension, file_formats)) / (1024 * 1024)
        avg_speed = total_size / total_time if total_time > 0 else 0
        
        context.log.info(f"🎉 {pipeline_name} Copy Complete!")
        context.log.info(f"   📁 Files: {len(copied_files)}")
        context.log.info(f"   📦 Size: {total_size:.1f} MB")
        context.log.info(f"   ⏱️ Time: {total_time:.1f} seconds")
        context.log.info(f"   🚀 Speed: {avg_speed:.1f} MB/s")
        
        return MaterializeResult(
            value={
                "status": "completed",
                "copied_files": copied_files,
                "copy_count": len(copied_files),
                "expected_count": expected_count,
                "pipeline_name": pipeline_name,
                "total_size_mb": total_size,
                "total_time_seconds": total_time,
                "average_speed_mbps": avg_speed
            },
            metadata={
                "status": MetadataValue.text("✅ SUCCESS"),
                "files_copied": MetadataValue.int(len(copied_files)),
                "total_size_mb": MetadataValue.float(total_size),
                "total_time_seconds": MetadataValue.float(total_time),
                "average_speed_mbps": MetadataValue.float(avg_speed),
                "pipeline_name": MetadataValue.text(pipeline_name)
            }
        )
        
    except Exception as e:
        context.log.error(f"❌ {pipeline_name} copy failed: {str(e)}")
        return MaterializeResult(
            value={
                "status": "failed",
                "error": str(e),
                "pipeline_name": pipeline_name,
                "copy_count": 0
            },
            metadata={
                "status": MetadataValue.text("❌ ERROR"),
                "error": MetadataValue.text(str(e)[:200]),
                "pipeline_name": MetadataValue.text(pipeline_name)
            }
        )


def _copy_large_file_with_progress(context, sftp_file, dest_file_client, fname, file_size, size_mb):
    """Copy large file in chunks with progress updates."""
    chunk_size = 8 * 1024 * 1024  # 8MB chunks
    total_copied = 0
    last_log_time = time.time()
    
    # Create destination file
    dest_file_client.create_file()
    
    while total_copied < file_size:
        chunk = sftp_file.read(chunk_size)
        if not chunk:
            break
        
        dest_file_client.append_data(chunk, offset=total_copied)
        total_copied += len(chunk)
        
        # Log progress every 15 seconds
        current_time = time.time()
        if current_time - last_log_time > 15:
            progress = (total_copied / file_size) * 100
            copied_mb = total_copied / (1024 * 1024)
            context.log.info(f"   📊 {fname}: {progress:.0f}% ({copied_mb:.1f}/{size_mb:.1f} MB)")
            last_log_time = current_time
    
    # Flush the file
    dest_file_client.flush_data(total_copied)


def _should_copy_file(fname, prefix, suffix, contains, not_contains, regex, extension, file_formats):
    """Simple file filtering logic."""
    # Check prefix
    if prefix:
        prefixes = prefix if isinstance(prefix, list) else [prefix]
        if not any(fname.startswith(p) for p in prefixes):
            return False
    
    # Check suffix
    if suffix:
        suffixes = suffix if isinstance(suffix, list) else [suffix]
        if not any(fname.endswith(s) for s in suffixes):
            return False
    
    # Check contains
    if contains:
        contains_list = contains if isinstance(contains, list) else [contains]
        if not any(c in fname for c in contains_list):
            return False
    
    # Check not_contains
    if not_contains:
        not_contains_list = not_contains if isinstance(not_contains, list) else [not_contains]
        if any(nc in fname for nc in not_contains_list):
            return False
    
    # Check regex
    if regex and not re.search(regex, fname):
        return False
    
    # Check extension
    if extension:
        extensions = extension if isinstance(extension, list) else [extension]
        if not any(fname.endswith(ext) for ext in extensions):
            return False
    
    # Check file_formats (legacy)
    if file_formats:
        if not any(fname.lower().endswith(ext.lower()) for ext in file_formats):
            return False
    
    return True