import requests
import re
import time
import zipfile
import io
from datetime import datetime
from bs4 import BeautifulSoup
from dagster import DagsterEventType, EventRecordsFilter
from dagster import asset, sensor, AssetExecutionContext, SensorEvaluationContext, MaterializeResult, MetadataValue, RunRequest, SkipReason, AssetKey


def create_cms_npi_monitor_and_downloader(config: dict):
    
    subject_area = config["subject_area"].lower()
    asset_name = config["asset_name"]
    
    @asset(
        name=asset_name,
        description=f"Monitor CMS NPI Files and download npidata_pfile for {subject_area}",
        required_resource_keys={"adls_access_keys"},
        group_name=config["group_name"]
    )
    def cms_npi_monitor_and_download(context: AssetExecutionContext) -> MaterializeResult:
        """Main asset that checks, downloads, and extracts NPI files"""
        
        pipeline_name = config["pipeline_name"]
        current_date = datetime.now()
        
        context.log.info(f"🔍 {pipeline_name} - Starting CMS NPI check for {current_date.strftime('%B %Y')}")
        
        try:
            target_month = current_date.strftime('%B').lower() 
            target_year = current_date.year
            
            context.log.info(f"Looking for {target_month} {target_year} NPI file")
            
            # Try to find and download file
            max_retries = 3
            retry_count = 0
            
            while retry_count < max_retries:
                context.log.info(f"🔄 Attempt {retry_count + 1} of {max_retries}")
                
                # Check CMS website
                npi_file = find_cms_npi_file_for_month_v2_priority(context, target_month, target_year)
                
                if npi_file:
                    # Found file, try to download and extract
                    result = download_and_extract_npi_file(
                        context=context,
                        file_info=npi_file,
                        adls_client=context.resources.adls_access_keys,
                        config=config
                    )
                    
                    if result["success"]:
                        context.log.info(f"🎉 Successfully processed {npi_file['name']}")
                        return MaterializeResult(
                            value={
                                "status": "success",
                                "filename": npi_file['name'],
                                "extracted_files": result["extracted_files"],
                                "target_directory": result["target_directory"],
                                "pipeline_name": pipeline_name,
                                "file_ready": True,
                                "target_month": target_month,
                                "target_year": target_year
                            },
                            metadata={
                                "status": MetadataValue.text("🎉 SUCCESS"),
                                "filename": MetadataValue.text(npi_file['name']),
                                "extracted_count": MetadataValue.int(len(result["extracted_files"])),
                                "target_directory": MetadataValue.text(result["target_directory"]),
                                "pipeline_name": MetadataValue.text(pipeline_name),
                                "target_month": MetadataValue.text(target_month)
                            }
                        )
                    else:
                        context.log.warning(f"Download failed: {result['error']}")
                else:
                    context.log.warning(f"No {target_month} {target_year} file found on attempt {retry_count + 1}")
            
                retry_count += 1
                if retry_count < max_retries:
                    context.log.info("Will retry on next scheduled run...")
                
            # All retries exhausted
            return MaterializeResult(
                value={
                    "status": "not_found",
                    "reason": f"No {target_month} {target_year} file found after {max_retries} attempts",
                    "pipeline_name": pipeline_name,
                    "target_month": target_month,
                    "target_year": target_year,
                    "file_ready": False
                },
                metadata={
                    "status": MetadataValue.text("FILE NOT FOUND"),
                    "attempts": MetadataValue.int(max_retries),
                    "target_month": MetadataValue.text(target_month),
                    "pipeline_name": MetadataValue.text(pipeline_name)
                }
            )
            
        except Exception as e:
            context.log.error(f"{pipeline_name} failed: {str(e)}")
            return MaterializeResult(
                value={
                    "status": "error",
                    "error": str(e),
                    "pipeline_name": pipeline_name,
                    "file_ready": False
                },
                metadata={
                    "status": MetadataValue.text("ERROR"),
                    "error": MetadataValue.text(str(e)[:200]),
                    "pipeline_name": MetadataValue.text(pipeline_name)
                }
            )
    
    @sensor(
        name=f"cms_npi_{subject_area}_sensor",
        asset_selection=[asset_name],
        description=f"Sensor to trigger {subject_area} pipeline when npidata_pfile is ready"
    )
    def cms_npi_sensor(context: SensorEvaluationContext):
        """Sensor that triggers pipeline when npidata_pfile is extracted and ready"""
        
        try:
            asset_key = AssetKey(asset_name)
            
            try:
                latest_event = context.instance.get_latest_materialization_event(asset_key)
                if not latest_event:
                    return SkipReason("No materializations found")
                
                materialization = latest_event.dagster_event.event_specific_data.materialization
                
            except AttributeError:
                try:
                    records = context.instance.get_event_records(
                        EventRecordsFilter(
                            event_type=DagsterEventType.ASSET_MATERIALIZATION,
                            asset_key=asset_key
                        ),
                        limit=1
                    )
                    if not records:
                        return SkipReason("No materializations found")
                    
                    materialization = records[0].event_log_entry.dagster_event.event_specific_data.materialization
                    
                except (AttributeError, ImportError):
                    context.log.info("Using fallback method - triggering based on monitoring period")
                    
                    current_date = datetime.now()
                    
                    if not (13 <= current_date.day <= 17):
                        return SkipReason(f"Not monitoring period (day {current_date.day})")
                    
                    run_key = f"cms_npi_fallback_{current_date.strftime('%Y%m%d')}"
                    
                    context.log.info(f"Triggering NPI pipeline (fallback method)")
                    
                    return RunRequest(
                        run_key=run_key,
                        tags={
                            "source": "cms_npi_fallback",
                            "date": current_date.strftime('%Y-%m-%d'),
                            "pipeline_type": "npi_data"
                        }
                    )
            
            data = {}
            if materialization and materialization.metadata:
                for key, value in materialization.metadata.items():
                    if hasattr(value, 'value'):
                        data[key] = value.value
                    elif hasattr(value, 'text'):
                        data[key] = value.text
                    else:
                        data[key] = str(value)
            
            status = data.get('status', '')
            extracted_count = data.get('extracted_count', 0)
            
            context.log.info(f"Sensor check - Status: {status}, Extracted: {extracted_count}")
            
            if 'SUCCESS' in status and extracted_count > 0:
                filename = data.get('filename', 'unknown')
                target_month = data.get('target_month', datetime.now().strftime('%B').lower())
                
                context.log.info(f"npidata_pfile ready for processing: {filename}")
                
                return RunRequest(
                    run_key=f"cms_npi_{target_month}_{datetime.now().strftime('%Y%m%d')}",
                    tags={
                        "source": "cms_npi",
                        "filename": filename,
                        "month": target_month,
                        "pipeline_type": "npi_data"
                    }
                )
            
            return SkipReason(f"File not ready. Status: {status}, Extracted: {extracted_count}")
            
        except Exception as e:
            context.log.error(f"Sensor error: {str(e)}")
            return SkipReason(f"Sensor error: {str(e)}")
    
    return {
        "monitor_asset": cms_npi_monitor_and_download,
        "monitor_sensor": cms_npi_sensor
    }


def find_cms_npi_file_for_month_v2_priority(context, target_month, target_year):
    
    try:
        url = "https://download.cms.gov/nppes/NPI_Files.html"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        context.log.info(f"Looking for {target_month} {target_year} files")
        
        candidates = []
        
        for link in soup.find_all('a', href=True):
            href = link.get('href')
            if not href or not href.endswith('.zip'):
                continue
            
            filename = href.split('/')[-1]
            filename_lower = filename.lower()
            
            if (target_month in filename_lower and str(target_year) in filename and 
                'dissemination' in filename_lower and
                'weekly' not in filename_lower):

                if href.startswith('/'):
                    download_url = f"https://download.cms.gov{href}"
                elif href.startswith('http'):
                    download_url = href
                else:
                    download_url = f"https://download.cms.gov/nppes/{href}"
                
                is_v2 = ('v.2' in filename_lower or 
                        'v2' in filename_lower or 
                        '_v2' in filename_lower or
                        'version2' in filename_lower or
                        'version_2' in filename_lower)
                
                candidates.append({
                    'name': filename,
                    'download_url': download_url,
                    'month': target_month,
                    'year': target_year,
                    'is_v2': is_v2,
                    'priority': 1 if is_v2 else 2 
                })
                
                context.log.info(f"Found candidate: {filename} {'(v2 ⭐)' if is_v2 else '(v1)'}")
        
        if candidates:
            candidates.sort(key=lambda x: (x['priority'], x['name']))
            
            selected = candidates[0]
            version_label = "v2" if selected['is_v2'] else "v1"
            context.log.info(f"SELECTED: {selected['name']} ({version_label})")
            
            return selected
        
        context.log.warning(f"No {target_month} {target_year} files found")
        return None
        
    except Exception as e:
        context.log.error(f"Error searching CMS website: {e}")
        return None


def download_and_extract_npi_file(context, file_info, adls_client, config):
    """Download ZIP file and extract ONLY the main npidata_pfile (not fileheader)"""
    
    try:
        filename = file_info['name']
        download_url = file_info['download_url']
        
        context.log.info(f"Downloading: {filename}")
        
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        
        # Download with retry logic
        max_download_retries = 3
        for download_attempt in range(max_download_retries):
            try:
                response = requests.get(download_url, headers=headers, stream=True, timeout=600)
                response.raise_for_status()
                
                # Read ZIP content into memory
                zip_content = io.BytesIO()
                for chunk in response.iter_content(chunk_size=8*1024*1024):
                    if chunk:
                        zip_content.write(chunk)
                
                zip_content.seek(0)
                context.log.info(f"Downloaded {len(zip_content.getvalue()) / (1024*1024):.1f} MB")
                break
                
            except Exception as download_error:
                if download_attempt == max_download_retries - 1:
                    raise download_error
                context.log.warning(f"Download attempt {download_attempt + 1} failed: {str(download_error)[:100]}")
                time.sleep(5)
        
        # Extract ONLY the main npidata_pfile
        context.log.info("Extracting main npidata_pfile...")
        
        extracted_files = []
        target_directory = config["stage_directory"]  
        
        with zipfile.ZipFile(zip_content, 'r') as zip_file:
            # First, identify all files in the ZIP
            all_files = zip_file.namelist()
            context.log.info(f"ZIP contains {len(all_files)} files: {all_files}")
            
            # Find the main data file
            main_data_files = []
            
            for file_name in all_files:
                # More specific pattern to get ONLY the main data file
                if (re.match(r'^npidata_pfile_\d{8}-\d{8}\.csv$', file_name, re.IGNORECASE) or
                    (file_name.startswith('npidata_pfile_') and 
                     file_name.endswith('.csv') and 
                     'fileheader' not in file_name.lower() and
                     'header' not in file_name.lower())):
                    
                    main_data_files.append(file_name)
                    context.log.info(f"Found main data file: {file_name}")
                else:
                    context.log.info(f"Skipping: {file_name}")
            
            # Process only the main data file
            if not main_data_files:
                context.log.error("No main npidata_pfile found in ZIP")
                return {
                    "success": False,
                    "error": "No main npidata_pfile found in ZIP"
                }
            
            # Process the main file
            for file_name in main_data_files:
                context.log.info(f"Processing main file: {file_name}")
                
                # Extract file content
                file_content = zip_file.read(file_name)
                file_size_mb = len(file_content) / (1024 * 1024)
                context.log.info(f"File size: {file_size_mb:.1f} MB")
                
                # Upload to ADLS
                success = upload_large_file_to_adls(
                    context=context,
                    adls_client=adls_client,
                    file_content=file_content,
                    file_name=file_name,
                    config=config,
                    target_directory=target_directory
                )
                
                if success:
                    extracted_files.append(file_name)
                    context.log.info(f"Successfully uploaded: {file_name}")
                    # Only process one main file
                    break
                else:
                    context.log.error(f"Failed to upload: {file_name}")
                    return {
                        "success": False,
                        "error": f"Upload failed for {file_name}"
                    }
        
        if not extracted_files:
            return {
                "success": False,
                "error": "No files were successfully processed"
            }
        
        context.log.info(f"Successfully processed {len(extracted_files)} file(s): {extracted_files}")
        
        return {
            "success": True,
            "extracted_files": extracted_files,
            "target_directory": f"{config['stage_container']}/{target_directory}",
            "zip_filename": filename
        }
        
    except Exception as e:
        context.log.error(f"Download/extract failed: {e}")
        return {
            "success": False,
            "error": str(e)
        }


def upload_large_file_to_adls(context, adls_client, file_content, file_name, config, target_directory):
    """Upload large file to ADLS with improved error handling and retry logic"""
    
    try:
        fs_client = adls_client.get_file_system_client(config["stage_container"])
        file_path = f"{target_directory}/{file_name}"
        file_client = fs_client.get_file_client(file_path)
        
        file_size_mb = len(file_content) / (1024 * 1024)
        context.log.info(f"Uploading {file_name} ({file_size_mb:.1f} MB) to {file_path}")
        
        # Try simple upload first for smaller files
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
    
        # Chunked upload for large files
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
            
            # Upload chunk with retry and exponential backoff
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
        
        # Flush the file with retry
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