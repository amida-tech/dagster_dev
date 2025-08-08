import requests
import re
import time
import zipfile
import io
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from dagster import asset, sensor, AssetExecutionContext, SensorEvaluationContext, MaterializeResult, MetadataValue, RunRequest, SkipReason, AssetKey


def create_cms_npi_monitor_and_downloader(config: dict):
    
    @asset(
        name=config["asset_name"],
        description=f"Monitor and download CMS NPI files for {config['subject_area']}",
        required_resource_keys={"adls_access_keys"},
        group_name=config["group_name"]
    )
    def cms_npi_monitor_and_download(context: AssetExecutionContext) -> MaterializeResult:
        """Main asset - checks, downloads, and extracts NPI files"""
        
        current_day = datetime.now().day
        is_monthly = 13 <= current_day <= 17
        file_type = "monthly" if is_monthly else "weekly"
        
        context.log.info(f"Looking for {file_type} NPI files (day {current_day})")
        
        try:
            # Find available file
            npi_file = find_latest_npi_file(context, file_type)
            if not npi_file:
                return create_result("not_found", f"No {file_type} file found", False)
            
            # Check if already processed
            if is_already_processed(context, npi_file, config):
                context.log.info(f"File {npi_file['name']} already processed")
                return create_result("already_processed", npi_file['name'], False)
            
            # Download and extract
            context.log.info(f"Downloading {npi_file['name']}")
            result = download_and_extract(context, npi_file, config)
            
            if result["success"]:
                context.log.info(f"Successfully processed {npi_file['name']}")
                return create_result("success", npi_file['name'], True, result["extracted_files"])
            else:
                return create_result("error", result["error"], False)
                
        except Exception as e:
            context.log.error(f"Pipeline failed: {e}")
            return create_result("error", str(e), False)
    
    @sensor(
        name=f"cms_npi_{config['subject_area']}_sensor",
        asset_selection=[config["asset_name"]],
        description=f"Trigger pipeline when new NPI file is ready"
    )
    def cms_npi_sensor(context: SensorEvaluationContext):
        """Sensor that triggers when new file is processed"""
        
        try:
            # Get latest asset run
            asset_key = AssetKey(config["asset_name"])
            latest_event = context.instance.get_latest_materialization_event(asset_key)
            
            if not latest_event:
                return SkipReason("No runs found")
            
            # Extract metadata
            materialization = latest_event.dagster_event.event_specific_data.materialization
            metadata = {}
            
            if materialization and materialization.metadata:
                for key, value in materialization.metadata.items():
                    if hasattr(value, 'text'):
                        metadata[key] = value.text
                    elif hasattr(value, 'value'):
                        metadata[key] = value.value
            
            status = metadata.get('status', '')
            file_ready = metadata.get('file_ready', False)
            filename = metadata.get('filename', 'unknown')
            
            context.log.info(f"Sensor check: {status}, Ready: {file_ready}")
            
            # Only trigger on successful new file
            if status == "SUCCESS" and file_ready:
                context.log.info(f"Triggering pipeline for {filename}")
                return RunRequest(
                    run_key=f"npi_{datetime.now().strftime('%Y%m%d_%H%M')}",
                    tags={"filename": filename, "source": "cms_npi"}
                )
            
            return SkipReason(f"Not ready: {status}")
            
        except Exception as e:
            return SkipReason(f"Sensor error: {e}")
    
    return {
        "monitor_asset": cms_npi_monitor_and_download,
        "monitor_sensor": cms_npi_sensor
    }


def find_latest_npi_file(context, file_type):
    """Dynamically find the latest monthly or weekly files on CMS website"""
    
    url = "https://download.cms.gov/nppes/NPI_Files.html"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        current_year = datetime.now().year
        current_month = datetime.now().month
        
        candidates = []
        
        for link in soup.find_all('a', href=True):
            href = link.get('href')
            if not href or not href.endswith('.zip'):
                continue
                
            filename = href.split('/')[-1]
            filename_lower = filename.lower()
            
            # Skip if not V2 (we only want V2 files)
            is_v2 = any(v in filename_lower for v in ['v.2', 'v2', '_v2', 'version2'])
            if not is_v2:
                continue
            
            # Skip if not dissemination file
            if 'dissemination' not in filename_lower:
                continue
            
            # Determine file type and validate
            is_weekly_file = 'weekly' in filename_lower
            is_monthly_file = not is_weekly_file and any(month_name in filename_lower 
                            for month_name in ['january', 'february', 'march', 'april', 'may', 'june',
                                             'july', 'august', 'september', 'october', 'november', 'december'])
            
            # Only process files that match what we're looking for
            if file_type == "weekly" and not is_weekly_file:
                continue
            if file_type == "monthly" and not is_monthly_file:
                continue
            
            # Extract date information for ranking
            file_date_score = extract_file_date_score(filename, file_type, current_year, current_month)
            
            if file_date_score is None:
                context.log.debug(f"Could not extract date from: {filename}")
                continue
                
            # Build full URL
            if href.startswith('/'):
                download_url = f"https://download.cms.gov{href}"
            elif href.startswith('http'):
                download_url = href
            else:
                download_url = f"https://download.cms.gov/nppes/{href}"
            
            candidates.append({
                'name': filename,
                'download_url': download_url,
                'file_type': file_type,
                'date_score': file_date_score,
                'parsed_info': parse_filename_info(filename)
            })
            
            context.log.info(f"Found candidate: {filename} (score: {file_date_score})")
        
        if candidates:
            # Sort by date score DESC to get the LATEST file first
            candidates.sort(key=lambda x: x['date_score'], reverse=True)
            selected = candidates[0]
            context.log.info(f"Selected LATEST: {selected['name']} (score: {selected['date_score']})")
            context.log.info(f"File info: {selected['parsed_info']}")
            return selected
        
        context.log.warning(f"No {file_type} V2 files found for current timeframe")
        return None
        
    except Exception as e:
        context.log.error(f"Error finding files: {e}")
        return None


def extract_file_date_score(filename, file_type, current_year, current_month):
    """Extract a comparable date score from filename for ranking (higher = more recent)"""
    
    try:
        if file_type == "weekly":
            # Look for MMDDYY_MMDDYY pattern and use end date
            date_match = re.search(r'(\d{6})_(\d{6})', filename)
            if date_match:
                end_date_mmddyy = date_match.group(2)
                
                # Parse MMDDYY
                mm = int(end_date_mmddyy[0:2])
                dd = int(end_date_mmddyy[2:4])
                yy = int(end_date_mmddyy[4:6])
                
                # Convert to 4-digit year (assuming 20xx)
                yyyy = 2000 + yy
                
                # Create a score: YYYYMMDD format for easy comparison
                return yyyy * 10000 + mm * 100 + dd
            
            # Fallback: look for any date pattern in weekly files
            all_dates = re.findall(r'\d{6}', filename)
            if all_dates:
                # Take the last (typically end) date
                last_date = all_dates[-1]
                mm = int(last_date[0:2])
                dd = int(last_date[2:4]) 
                yy = int(last_date[4:6])
                yyyy = 2000 + yy
                return yyyy * 10000 + mm * 100 + dd
                
        elif file_type == "monthly":
            # Extract year from filename
            year_match = re.search(r'20\d{2}', filename)
            if not year_match:
                return None
                
            year = int(year_match.group())
            
            # Find month name
            filename_lower = filename.lower()
            months = ['january', 'february', 'march', 'april', 'may', 'june',
                     'july', 'august', 'september', 'october', 'november', 'december']
            
            for i, month in enumerate(months):
                if month in filename_lower:
                    month_num = i + 1
                    # Score format: YYYYMM00 (add 00 for day to make it comparable)
                    return year * 10000 + month_num * 100
            
        return None
        
    except Exception as e:
        return None


def parse_filename_info(filename):
    """Parse filename to extract readable information"""
    
    try:
        info = {}
        
        # Extract date ranges for weekly files
        date_match = re.search(r'(\d{6})_(\d{6})', filename)
        if date_match:
            start_mmddyy = date_match.group(1)
            end_mmddyy = date_match.group(2)
            
            info['start_date'] = f"{start_mmddyy[0:2]}/{start_mmddyy[2:4]}/20{start_mmddyy[4:6]}"
            info['end_date'] = f"{end_mmddyy[0:2]}/{end_mmddyy[2:4]}/20{end_mmddyy[4:6]}"
            info['type'] = 'weekly'
        else:
            # Try to extract month/year for monthly files
            year_match = re.search(r'20\d{2}', filename)
            if year_match:
                info['year'] = year_match.group()
            
            filename_lower = filename.lower()
            months = ['january', 'february', 'march', 'april', 'may', 'june',
                     'july', 'august', 'september', 'october', 'november', 'december']
            
            for month in months:
                if month in filename_lower:
                    info['month'] = month.title()
                    info['type'] = 'monthly'
                    break
        
        return info
        
    except Exception:
        return {}


def is_already_processed(context, file_info, config):
    """Check if file already exists in stage directory using standardized naming"""
    
    try:
        adls_client = context.resources.adls_access_keys
        fs_client = adls_client.get_file_system_client(config["stage_container"])
        directory_client = fs_client.get_directory_client(config['stage_directory'])
        
        # Generate the expected CSV filename that would be created
        expected_csv_name = generate_csv_filename(file_info['name'])
        context.log.info(f"Looking for existing file: {expected_csv_name}")
        
        if not expected_csv_name:
            context.log.warning(f"Could not generate CSV name for {file_info['name']}")
            return False
        
        # Check if this specific file exists
        try:
            for file_item in directory_client.get_paths():
                existing_filename = file_item.name.split('/')[-1]
                
                if existing_filename == expected_csv_name:
                    context.log.info(f"Found exact match: {existing_filename}")
                    return True
                    
                # Also check for similar files (in case of minor naming variations)
                if (existing_filename.endswith('.csv') and 
                    'npidata_pfile' in existing_filename and 
                    files_represent_same_data(file_info['name'], existing_filename)):
                    context.log.info(f"Found similar file representing same data: {existing_filename}")
                    return True
                    
        except Exception as e:
            context.log.info(f"No existing files found or directory empty: {e}")
        
        return False
        
    except Exception as e:
        context.log.warning(f"Could not check existing files: {e}")
        return False


def generate_csv_filename(zip_filename):
    """Generate standardized CSV filename from ZIP filename"""
    
    try:
        # For weekly files: MMDDYY_MMDDYY pattern
        date_match = re.search(r'(\d{6})_(\d{6})', zip_filename)
        if date_match:
            start_mmddyy = date_match.group(1)
            end_mmddyy = date_match.group(2)
            
            # Convert to YYYYMMDD format
            start_yyyy = f"20{start_mmddyy[4:6]}"
            start_mm = start_mmddyy[0:2]
            start_dd = start_mmddyy[2:4]
            
            end_yyyy = f"20{end_mmddyy[4:6]}"
            end_mm = end_mmddyy[0:2]
            end_dd = end_mmddyy[2:4]
            
            start_date = f"{start_yyyy}{start_mm}{start_dd}"
            end_date = f"{end_yyyy}{end_mm}{end_dd}"
            
            return f"npidata_pfile_{start_date}-{end_date}.csv"
        
        # For monthly files: use month and year
        year_match = re.search(r'20\d{2}', zip_filename)
        if year_match:
            year = year_match.group()
            
            filename_lower = zip_filename.lower()
            months = {
                'january': '01', 'february': '02', 'march': '03', 'april': '04',
                'may': '05', 'june': '06', 'july': '07', 'august': '08',
                'september': '09', 'october': '10', 'november': '11', 'december': '12'
            }
            
            for month_name, month_num in months.items():
                if month_name in filename_lower:
                    return f"npidata_pfile_{year}{month_num}_monthly.csv"
        
        # Fallback: use original name pattern
        return f"npidata_pfile_{int(datetime.now().timestamp())}.csv"
        
    except Exception:
        return None


def files_represent_same_data(zip_filename, csv_filename):
    """Check if ZIP and CSV files represent the same data period"""
    
    try:
        # Extract date ranges from both files
        zip_dates = re.search(r'(\d{6})_(\d{6})', zip_filename)
        csv_dates = re.search(r'(\d{8})-(\d{8})', csv_filename)
        
        if zip_dates and csv_dates:
            # Compare the date ranges
            zip_start = zip_dates.group(1)
            zip_end = zip_dates.group(2)
            
            # Convert ZIP dates (MMDDYY) to YYYYMMDD
            zip_start_converted = f"20{zip_start[4:6]}{zip_start[0:2]}{zip_start[2:4]}"
            zip_end_converted = f"20{zip_end[4:6]}{zip_end[0:2]}{zip_end[2:4]}"
            
            csv_start = csv_dates.group(1)
            csv_end = csv_dates.group(2)
            
            return (zip_start_converted == csv_start and zip_end_converted == csv_end)
        
        return False
        
    except Exception:
        return False


def download_and_extract(context, file_info, config):
    """Download ZIP and extract main CSV file with standardized naming"""
    
    try:
        # Download ZIP file
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        response = requests.get(file_info['download_url'], headers=headers, stream=True, timeout=600)
        response.raise_for_status()
        
        # Read into memory
        zip_content = io.BytesIO()
        for chunk in response.iter_content(chunk_size=16*1024*1024): 
            if chunk:
                zip_content.write(chunk)
        
        zip_content.seek(0)
        context.log.info(f"Downloaded {len(zip_content.getvalue()) / (1024*1024):.1f} MB")
        
        # Extract main CSV file
        with zipfile.ZipFile(zip_content, 'r') as zip_file:
            all_files = zip_file.namelist()
            
            # Find main data file 
            main_file = None
            for file_name in all_files:
                if (file_name.startswith('npidata_pfile_') and 
                    file_name.endswith('.csv') and 
                    'header' not in file_name.lower()):
                    main_file = file_name
                    break
            
            if not main_file:
                return {"success": False, "error": "No main CSV file found"}
            
            context.log.info(f"Extracting: {main_file}")
            
            # Generate standardized filename
            new_filename = generate_csv_filename(file_info['name'])
            if not new_filename:
                new_filename = main_file
                
            context.log.info(f"Saving as: {new_filename}")
            
            # Extract and upload
            file_content = zip_file.read(main_file)
            success = upload_to_adls(context, file_content, new_filename, config)
            
            if success:
                return {
                    "success": True,
                    "extracted_files": [new_filename]
                }
            else:
                return {"success": False, "error": "Upload failed"}
                
    except Exception as e:
        return {"success": False, "error": str(e)}


def upload_to_adls(context, file_content, file_name, config):
    """Fast upload to ADLS with optimized settings"""
    
    try:
        adls_client = context.resources.adls_access_keys
        fs_client = adls_client.get_file_system_client(config["stage_container"])
        file_path = f"{config['stage_directory']}/{file_name}"
        file_client = fs_client.get_file_client(file_path)
        
        file_size_mb = len(file_content) / (1024 * 1024)
        context.log.info(f"Uploading {file_name} ({file_size_mb:.1f} MB)")
        
        # For smaller files, use simple upload
        if file_size_mb <= 100:
            file_client.upload_data(file_content, overwrite=True)
            context.log.info("✅ Upload complete")
            return True
        
        # For large files, use chunked upload with larger chunks
        context.log.info("Using chunked upload for large file...")
        chunk_size = 32 * 1024 * 1024  
        
        file_client.create_file()
        total_uploaded = 0
        
        while total_uploaded < len(file_content):
            chunk_end = min(total_uploaded + chunk_size, len(file_content))
            chunk_data = file_content[total_uploaded:chunk_end]
            
            # Upload chunk with simple retry
            for attempt in range(3):
                try:
                    file_client.append_data(chunk_data, offset=total_uploaded)
                    break
                except Exception as e:
                    if attempt == 2:
                        raise e
                    time.sleep(2)
            
            total_uploaded = chunk_end
            progress = (total_uploaded / len(file_content)) * 100
            context.log.info(f"Progress: {progress:.0f}%")
        
        # Flush file
        file_client.flush_data(len(file_content))
        context.log.info("Upload complete")
        return True
        
    except Exception as e:
        context.log.error(f"Upload failed: {e}")
        return False


def create_result(status, message, file_ready, extracted_files=None):
    """Helper to create consistent MaterializeResult"""
    
    status_icons = {
        "success": "SUCCESS",
        "already_processed": "ALREADY PROCESSED", 
        "not_found": "NOT FOUND",
        "error": "ERROR"
    }
    
    return MaterializeResult(
        value={
            "status": status,
            "message": message,
            "file_ready": file_ready,
            "extracted_files": extracted_files or []
        },
        metadata={
            "status": MetadataValue.text(status_icons.get(status, status)),
            "message": MetadataValue.text(str(message)[:200]),
            "file_ready": MetadataValue.bool(file_ready),
            "extracted_count": MetadataValue.int(len(extracted_files) if extracted_files else 0)
        }
    )