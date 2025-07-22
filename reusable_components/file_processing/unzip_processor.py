import os
import io
import time
from typing import Dict, Optional, Union, List
import zipfile
import re
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue
from datetime import datetime
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ServiceRequestError, HttpResponseError

def unzip_files_with_result(
    context: AssetExecutionContext,
    adls_client: DataLakeServiceClient,
    copy_result: dict,
    container_name: str,
    stage_directory: str,
    load_directory: str,
    pipeline_name: str,
    prefix: Optional[Union[str, List[str]]] = None,
    suffix: Optional[Union[str, List[str]]] = None,
    contains: Optional[Union[str, List[str]]] = None,
    not_contains: Optional[Union[str, List[str]]] = None,
    regex: Optional[str] = None,
    extension: Optional[Union[str, List[str]]] = None
) -> MaterializeResult:
    """
    Complete unzip/copy logic: handles both ZIP files and CSV files.
    - ZIP files: extracts and cleans filenames
    - CSV files: copies and cleans filenames
    Returns MaterializeResult ready for the asset to return.
    """
    
    copy_status = copy_result.get("status", "unknown")
    copy_count = copy_result.get("copy_count", 0)
    
    context.log.info(f"📂 {pipeline_name} File Processing Check:")
    context.log.info(f"   Copy status: {copy_status}, Files: {copy_count}")
    
    # Check if copy was successful
    if copy_status != "completed":
        context.log.info(f"❌ Skipping {pipeline_name} file processing - copy not completed")
        
        return MaterializeResult(
            value={"status": "skipped", "reason": "Copy operation failed", "pipeline_name": pipeline_name},
            metadata={
                "status": MetadataValue.text("⏭️ SKIPPED"),
                "reason": MetadataValue.text("Copy operation failed"),
                "pipeline_name": MetadataValue.text(pipeline_name)
            }
        )
    
    context.log.info(f"✅ {pipeline_name} starting file processing: {copy_count} files to process")
    
    # Use retry logic for the entire operation
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            result = _process_files_internal(
                context=context,
                adls_client=adls_client,
                container_name=container_name,
                stage_directory=stage_directory,
                load_directory=load_directory,
                prefix=prefix,
                suffix=suffix,
                contains=contains,
                not_contains=not_contains,
                regex=regex,
                extension=extension
            )
            
            # Success - extract counts and create successful result
            files_processed = result.get("files_processed", 0)
            csv_files_copied = result.get("csv_files_copied", 0)
            zip_files_extracted = result.get("zip_files_extracted", 0)
            
            context.log.info(f"✅ {pipeline_name} file processing complete:")
            context.log.info(f"   Total files processed: {files_processed}")
            context.log.info(f"   CSV files copied: {csv_files_copied}")
            context.log.info(f"   ZIP files extracted: {zip_files_extracted}")
            
            return MaterializeResult(
                value={**result, "pipeline_name": pipeline_name},
                metadata={
                    "status": MetadataValue.text("✅ SUCCESS"),
                    "files_processed": MetadataValue.int(files_processed),
                    "csv_files_copied": MetadataValue.int(csv_files_copied),
                    "zip_files_extracted": MetadataValue.int(zip_files_extracted),
                    "pipeline_name": MetadataValue.text(pipeline_name),
                    "retry_attempts": MetadataValue.int(retry_count)
                }
            )
            
        except Exception as e:
            retry_count += 1
            error_msg = str(e)
            
            # Check for retryable network errors
            if ("EOF occurred in violation of protocol" in error_msg or
                "ssl.c:" in error_msg or
                "Connection" in error_msg or
                "timeout" in error_msg.lower()):
                
                if retry_count < max_retries:
                    context.log.warning(f"⚠️ {pipeline_name} network error on attempt {retry_count}/{max_retries}, retrying...")
                    time.sleep(5 * retry_count)  # Exponential backoff
                    continue
                else:
                    context.log.error(f"❌ {pipeline_name} network error persisted after {max_retries} attempts")
            else:
                # Non-retryable error
                context.log.error(f"❌ {pipeline_name} non-retryable error: {error_msg}")
                break
    
    # If we reach here, either max retries exceeded or non-retryable error
    context.log.error(f"❌ {pipeline_name} file processing failed: {str(e)}")
    
    return MaterializeResult(
        value={"status": "failed", "error": str(e), "pipeline_name": pipeline_name},
        metadata={
            "status": MetadataValue.text("❌ FAILED"),
            "error_message": MetadataValue.text(str(e)[:200] + "..." if len(str(e)) > 200 else str(e)),
            "pipeline_name": MetadataValue.text(pipeline_name),
            "retry_attempts": MetadataValue.int(retry_count)
        }
    )

def _remove_timestamp_from_filename(filename):
    """Remove only timestamp/date numbers from filenames, preserving meaningful identifiers like TB, TBL"""
    name, ext = os.path.splitext(filename)
    
    # Specific approach: find TB/TBL and preserve everything before the timestamp part
    # Pattern: look for _TB or _TBL followed by timestamp patterns
    
    # First, try to find TB/TBL patterns and preserve up to that point
    tb_patterns = [
        (r'(.+_TB)_\d{4}_\d{2}_\d{2}T\d{6}Z?.*', r'\1'),      # Keep everything up to _TB
        (r'(.+_TBL)_\d{4}_\d{2}_\d{2}T\d{6}Z?.*', r'\1'),     # Keep everything up to _TBL
        (r'(.+_TB)_\d{4}_\d{2}_\d{2}.*', r'\1'),              # Keep everything up to _TB (date format)
        (r'(.+_TBL)_\d{4}_\d{2}_\d{2}.*', r'\1'),             # Keep everything up to _TBL (date format)
        (r'(.+_TB)_\d{8}T\d{6}Z?.*', r'\1'),                  # Keep everything up to _TB (compact)
        (r'(.+_TBL)_\d{8}T\d{6}Z?.*', r'\1'),                 # Keep everything up to _TBL (compact)
    ]
    
    clean_name = name
    
    # Try TB/TBL specific patterns first
    for pattern, replacement in tb_patterns:
        if re.match(pattern, clean_name):
            clean_name = re.sub(pattern, replacement, clean_name)
            return f"{clean_name}{ext}"
    
    # If no TB/TBL patterns matched, use general timestamp removal patterns
    general_patterns = [
        r'_\d{4}_\d{2}_\d{2}T\d{6}Z?.*',        # _2025_07_18T094424Z...
        r'_\d{4}_\d{2}_\d{2}_\d{4,6}.*',        # _2025_07_08_143000...
        r'_\d{4}-\d{2}-\d{2}T\d{6}Z?.*',        # _2024-07-08T143000Z...
        r'_\d{4}-\d{2}-\d{2}_\d{4,6}.*',        # _2024-07-08_1430...
        r'_\d{8}T\d{6}Z?.*',                     # _20240708T143000Z...
        r'_\d{8}_\d{4,6}.*',                     # _20240708_143000...
        r'_\d{14}.*',                            # _20240708143000...
        r'_\d{12}.*',                            # _202407081430...
        r'_\d{10}.*',                            # _1234567890...
        r'T\d{6}Z?.*',                           # T094424Z...
        r'_\d{4}_\d{2}_\d{2}.*',                 # _2025_07_18...
        r'_\d{8}.*',                             # _20250718...
    ]
    
    for pattern in general_patterns:
        clean_name = re.sub(pattern, '', clean_name)
    
    # Clean up any trailing underscores or dashes
    clean_name = clean_name.rstrip('_-')
    
    # Handle edge case where name becomes empty
    if not clean_name:
        clean_name = 'file'
    
    return f"{clean_name}{ext}"

def _file_matches_criteria(filename: str,
                          prefix: Union[str, List[str], tuple] = None,
                          suffix: Union[str, List[str], tuple] = None,
                          contains: Union[str, List[str], tuple] = None,
                          not_contains: Union[str, List[str], tuple] = None,
                          regex: str = None,
                          extension: Union[str, List[str], tuple] = None) -> bool:
    """Check if a file matches all the specified criteria with proper list/tuple handling."""
    
    # Check prefix - handle both string and list/tuple
    if prefix is not None:
        if isinstance(prefix, (list, tuple)):
            if not any(filename.startswith(p) for p in prefix):
                return False
        else:
            if not filename.startswith(prefix):
                return False
    
    # Check suffix - handle both string and list/tuple
    if suffix is not None:
        if isinstance(suffix, (list, tuple)):
            if not any(filename.endswith(s) for s in suffix):
                return False
        else:
            if not filename.endswith(suffix):
                return False
    
    # Check contains - handle both string and list/tuple
    if contains is not None:
        if isinstance(contains, (list, tuple)):
            if not any(c in filename for c in contains):
                return False
        else:
            if contains not in filename:
                return False
    
    # Check not_contains - handle both string and list/tuple
    if not_contains is not None:
        if isinstance(not_contains, (list, tuple)):
            if any(nc in filename for nc in not_contains):
                return False
        else:
            if not_contains in filename:
                return False
    
    # Check regex
    if regex and not re.search(regex, filename):
        return False
    
    # Check extension - handle both string and list/tuple
    if extension is not None:
        if isinstance(extension, (list, tuple)):
            if not any(filename.lower().endswith(e.lower()) for e in extension):
                return False
        else:
            if not filename.lower().endswith(extension.lower()):
                return False
    
    return True


def _upload_file_with_retry(load_client, file_content: bytes, context: AssetExecutionContext,
                           filename: str, max_retries: int = 3, retry_delay: int = 5) -> bool:
    """Upload file content with retry logic for network issues."""
    
    for attempt in range(max_retries + 1):
        try:
            # Create file first
            load_client.create_file()
            
            # Upload in chunks if file is large (>10MB)
            content_length = len(file_content)
            chunk_size = 10 * 1024 * 1024  # 10MB chunks
            
            if content_length > chunk_size:
                context.log.info(f"   📤 Uploading large file {filename} ({content_length:,} bytes) in chunks...")
                
                # Upload in chunks
                offset = 0
                while offset < content_length:
                    chunk_end = min(offset + chunk_size, content_length)
                    chunk = file_content[offset:chunk_end]
                    load_client.append_data(chunk, offset, len(chunk))
                    offset = chunk_end
                
                # Flush all data
                load_client.flush_data(content_length)
            else:
                # Small file - upload all at once
                load_client.append_data(file_content, 0, content_length)
                load_client.flush_data(content_length)
            
            return True
            
        except (ServiceRequestError, HttpResponseError, Exception) as e:
            if attempt < max_retries:
                context.log.warning(f"   ⚠️ Upload attempt {attempt + 1} failed for {filename}: {str(e)}")
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)  # Max 60 seconds
                
                # Try to clean up failed file before retry
                try:
                    load_client.delete_file()
                except:
                    pass  # Ignore cleanup errors
            else:
                context.log.error(f"   ❌ Failed to upload {filename} after {max_retries + 1} attempts: {str(e)}")
                return False
    
    return False


def _process_files_internal(
    context: AssetExecutionContext,
    adls_client,
    container_name: str,
    stage_directory: str,
    load_directory: str,
    prefix: Union[str, List[str], tuple] = None,
    suffix: Union[str, List[str], tuple] = None,
    contains: Union[str, List[str], tuple] = None,
    not_contains: Union[str, List[str], tuple] = None,
    regex: str = None,
    extension: Union[str, List[str], tuple] = None
) -> Dict:
    """
    Internal function that processes both ZIP and CSV files:
    - ZIP files: extracts contents and cleans filenames
    - CSV files: copies directly with filename cleaning
    """
    
    context.log.info(f"🗂️ File processing operation (handles ZIP and CSV):")
    context.log.info(f"   Container: {container_name}")
    context.log.info(f"   Source: {stage_directory}")
    context.log.info(f"   Destination: {load_directory}")
    context.log.info(f"   Filters: prefix={prefix}, not_contains={not_contains}")
    
    fs_client = adls_client.get_file_system_client(container_name)
    
    # List all files in stage directory
    context.log.info(f"📂 Scanning {stage_directory} for files...")
    try:
        all_paths = list(fs_client.get_paths(path=stage_directory))
        context.log.info(f"📂 Found {len(all_paths)} total items in directory")
    except Exception as e:
        context.log.error(f"❌ Failed to list directory {stage_directory}: {e}")
        raise
    
    # Separate ZIP and CSV files that match criteria
    zip_files = []
    csv_files = []
    
    for path in all_paths:
        if path.is_directory:
            continue
        
        # Get just the filename from the full path
        filename = os.path.basename(path.name)
        
        # Apply file matching criteria
        if _file_matches_criteria(
            filename,
            prefix=prefix,
            suffix=suffix,
            contains=contains,
            not_contains=not_contains,
            regex=regex,
            extension=extension
        ):
            if filename.lower().endswith('.zip'):
                zip_files.append(path.name)
                context.log.info(f"✅ ZIP Match: {filename}")
            elif filename.lower().endswith('.csv'):
                csv_files.append(path.name)
                context.log.info(f"✅ CSV Match: {filename}")
            else:
                context.log.info(f"📄 Other file type: {filename}")
    
    context.log.info(f"📁 File Summary:")
    context.log.info(f"   ZIP files to extract: {len(zip_files)}")
    context.log.info(f"   CSV files to copy: {len(csv_files)}")
    
    # Process results
    total_files_processed = 0
    csv_files_copied = 0
    zip_files_extracted = 0
    total_failed = 0
    processed_files = []
    failed_files = []
    
    # Process CSV files (copy with filename cleaning)
    if csv_files:
        context.log.info(f"📋 Processing {len(csv_files)} CSV files...")
        
        for csv_path in csv_files:
            filename = os.path.basename(csv_path)
            context.log.info(f"📄 Processing CSV: {filename}")
            
            try:
                # Download CSV file
                file_client = fs_client.get_file_client(csv_path)
                csv_data = file_client.download_file().readall()
                
                # Clean filename
                clean_name = _remove_timestamp_from_filename(filename)
                context.log.info(f"   Original: {filename}")
                context.log.info(f"   Cleaned: {clean_name}")
                
                # Save to load directory
                load_path = f"{load_directory}/{clean_name}"
                load_client = fs_client.get_file_client(load_path)
                
                # Upload with retry logic
                if _upload_file_with_retry(load_client, csv_data, context, clean_name):
                    csv_files_copied += 1
                    total_files_processed += 1
                    processed_files.append({
                        "original_name": filename,
                        "cleaned_name": clean_name,
                        "size_bytes": len(csv_data),
                        "file_type": "CSV",
                        "source": "direct_copy"
                    })
                    context.log.info(f"   ✅ Copied: {clean_name}")
                else:
                    total_failed += 1
                    failed_files.append(filename)
                    context.log.error(f"   ❌ Failed to copy: {filename}")
                    
            except Exception as e:
                context.log.error(f"   ❌ Error processing CSV {filename}: {str(e)}")
                total_failed += 1
                failed_files.append(filename)
    
    # Process ZIP files (extract with filename cleaning)
    if zip_files:
        context.log.info(f"📦 Processing {len(zip_files)} ZIP files...")
        
        for zip_path in zip_files:
            filename = os.path.basename(zip_path)
            context.log.info(f"🗂️ Processing ZIP: {filename}")
            
            try:
                # Download ZIP file
                file_client = fs_client.get_file_client(zip_path)
                zip_data = file_client.download_file().readall()
                
                # Extract contents
                with zipfile.ZipFile(io.BytesIO(zip_data), 'r') as zip_ref:
                    file_list = zip_ref.namelist()
                    context.log.info(f"   📦 Contains {len(file_list)} files")
                    
                    for file_in_zip in file_list:
                        if file_in_zip.endswith('/'):
                            continue  # Skip directories
                        
                        try:
                            file_content = zip_ref.read(file_in_zip)
                            
                            # Clean filename
                            clean_name = _remove_timestamp_from_filename(file_in_zip)
                            
                            # Save to load directory
                            load_path = f"{load_directory}/{clean_name}"
                            load_client = fs_client.get_file_client(load_path)
                            
                            if _upload_file_with_retry(load_client, file_content, context, clean_name):
                                zip_files_extracted += 1
                                total_files_processed += 1
                                processed_files.append({
                                    "original_name": file_in_zip,
                                    "cleaned_name": clean_name,
                                    "size_bytes": len(file_content),
                                    "file_type": "ZIP_EXTRACTED",
                                    "source_zip": filename
                                })
                                context.log.info(f"   ✅ Extracted: {clean_name}")
                            else:
                                total_failed += 1
                                failed_files.append(f"{filename}:{clean_name}")
                                
                        except Exception as e:
                            context.log.error(f"   ❌ Error extracting {file_in_zip}: {str(e)}")
                            total_failed += 1
                            failed_files.append(f"{filename}:{file_in_zip}")
                            
            except Exception as e:
                context.log.error(f"   ❌ Error processing ZIP {filename}: {str(e)}")
                total_failed += 1
                failed_files.append(filename)
    
    # Final summary
    context.log.info(f"🎉 File processing completed!")
    context.log.info(f"   Total files processed: {total_files_processed}")
    context.log.info(f"   CSV files copied: {csv_files_copied}")
    context.log.info(f"   ZIP files extracted: {zip_files_extracted}")
    if total_failed > 0:
        context.log.warning(f"   Files failed: {total_failed}")
    
    return {
        "status": "completed",
        "files_processed": total_files_processed,
        "csv_files_copied": csv_files_copied,
        "zip_files_extracted": zip_files_extracted,
        "files_failed": total_failed,
        "processed_files": processed_files,
        "failed_files": failed_files,
        "source_csv_files": [os.path.basename(f) for f in csv_files],
        "source_zip_files": [os.path.basename(f) for f in zip_files]
    }