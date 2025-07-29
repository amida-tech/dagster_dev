import requests
import re
import time
import zipfile
import io
from datetime import datetime
from bs4 import BeautifulSoup
from dagster import (
    asset, sensor, AssetExecutionContext, SensorEvaluationContext, 
    MaterializeResult, MetadataValue, RunRequest, SkipReason, AssetKey
)


def create_nucc_monitor_and_downloader(config: dict):
    """
    Creates a NUCC Provider Taxonomy file monitor that:
    1. Checks for current version files twice yearly (Jan 1 & July 1)
    2. Downloads and extracts CSV files
    3. Places them in the specified directory for processing
    4. Retries for 3 days if file not found
    """
    
    subject_area = config["subject_area"].lower()
    asset_name = config["asset_name"]
    
    @sensor(
        name=f"{subject_area}_sensor",
        description=f"Triggers NUCC monitoring during release periods"
    )
    def nucc_sensor(context: SensorEvaluationContext):
        """Sensor triggers - monitors Jan 1-7 and July 1-7"""
        
        current_date = datetime.now()
        day, month = current_date.day, current_date.month
        
        if not ((month == 1 and 1 <= day <= 7) or (month == 7 and 1 <= day <= 7)):
            return SkipReason(f"Not monitoring period (day {day}, month {month})")
        
        run_key = f"nucc_taxonomy_{current_date.strftime('%Y%m%d')}"
        
        context.log.info(f"🚀 Triggering NUCC check for {current_date.strftime('%B %Y')}")
        
        return RunRequest(
            run_key=run_key,
            tags={
                "version": get_target_version(current_date),
                "pipeline_type": "taxonomy_data"
            }
        )
    
    @asset(
        name=asset_name,
        description=f"Download NUCC files for {subject_area}",
        required_resource_keys={"adls_access_keys"},
        group_name=config["group_name"]
    )
    def nucc_monitor_and_download(context: AssetExecutionContext) -> MaterializeResult:
        """Download and process NUCC files"""
        
        current_date = datetime.now()
        target_version = get_target_version(current_date)
        
        context.log.info(f"🔍 Looking for NUCC version {target_version}")
        
        try:
            nucc_file = find_nucc_file(context, target_version)
            if not nucc_file:
                return MaterializeResult(
                    value={"status": "not_found", "file_ready": False},
                    metadata={"status": MetadataValue.text("FILE NOT FOUND")}
                )
            
            result = download_and_process_nucc(context, nucc_file, context.resources.adls_access_keys, config)
            
            if result["success"]:
                context.log.info(f"Success: {nucc_file['name']}")
                return MaterializeResult(
                    value={
                        "status": "success",
                        "filename": nucc_file['name'],
                        "extracted_files": result["extracted_files"],
                        "file_ready": True
                    },
                    metadata={
                        "status": MetadataValue.text("SUCCESS"),
                        "filename": MetadataValue.text(nucc_file['name']),
                        "extracted_count": MetadataValue.int(len(result["extracted_files"]))
                    }
                )
            else:
                return MaterializeResult(
                    value={"status": "error", "error": result['error'], "file_ready": False},
                    metadata={"status": MetadataValue.text("ERROR")}
                )
                
        except Exception as e:
            context.log.error(f"Failed: {str(e)}")
            return MaterializeResult(
                value={"status": "error", "error": str(e), "file_ready": False},
                metadata={"status": MetadataValue.text("ERROR")}
            )
    
    return {
        "monitor_sensor": nucc_sensor,
        "monitor_asset": nucc_monitor_and_download
    }


def get_target_version(current_date):
    """Get target version: X.1 for Jan, X.2 for July"""
    year = current_date.year
    month = current_date.month
    return f"{year}.{'2' if month >= 7 else '1'}"


def find_nucc_file(context, target_version):
    """Find current NUCC file"""
    
    try:
        response = requests.get(
            "https://www.nucc.org/index.php/code-sets-mainmenu-41/provider-taxonomy-mainmenu-40/csv-mainmenu-57",
            headers={'User-Agent': 'Mozilla/5.0'}, 
            timeout=30
        )
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        patterns = [
            lambda text, href: target_version.replace('.', '_') in text.lower(),
            lambda text, href: 'current' in text.lower() and 'version' in text.lower(),
            lambda text, href: re.search(r'nucc.*taxonomy.*csv', href.lower())
        ]
        
        for pattern in patterns:
            for link in soup.find_all('a', href=True):
                href = link.get('href', '')
                if not (href.endswith('.csv') or 'csv' in href.lower()):
                    continue
                
                text = link.get_text(strip=True)
                
                if pattern(text, href):
                    download_url = build_nucc_url(href)
                    filename = href.split('/')[-1] if '/' in href else href
                    
                    context.log.info(f"Found: {filename}")
                    return {
                        'name': filename,
                        'download_url': download_url
                    }
        
        return None
        
    except Exception as e:
        context.log.error(f"Search failed: {e}")
        return None


def download_and_process_nucc(context, file_info, adls_client, config):
    """Download and process file"""
    
    try:
        context.log.info(f"Downloading: {file_info['name']}")
        response = requests.get(
            file_info['download_url'], 
            headers={'User-Agent': 'Mozilla/5.0'}, 
            stream=True, 
            timeout=600
        )
        response.raise_for_status()
        
        content = b''.join(chunk for chunk in response.iter_content(chunk_size=8*1024*1024) if chunk)
        
        if file_info['name'].lower().endswith('.csv'):
            processed_files = {file_info['name']: content}
        else:
            with zipfile.ZipFile(io.BytesIO(content), 'r') as zip_file:
                csv_files = [f for f in zip_file.namelist() if f.lower().endswith('.csv')]
                if not csv_files:
                    return {"success": False, "error": "No CSV files in ZIP"}
                
                processed_files = {}
                for csv_file in csv_files:
                    processed_files[csv_file] = zip_file.read(csv_file)
        
        uploaded_files = []
        for filename, file_content in processed_files.items():
            success = upload_to_adls(context, adls_client, file_content, filename, config)
            if success:
                uploaded_files.append(filename)
            else:
                return {"success": False, "error": f"Upload failed for {filename}"}
        
        return {"success": True, "extracted_files": uploaded_files}
        
    except Exception as e:
        return {"success": False, "error": str(e)}


def upload_to_adls(context, adls_client, file_content, file_name, config):
    """Upload to ADLS with chunking for large files"""
    
    try:
        fs_client = adls_client.get_file_system_client(config["stage_container"])
        file_path = f"{config['stage_directory']}/{file_name}"
        file_client = fs_client.get_file_client(file_path)
        
        file_size_mb = len(file_content) / (1024 * 1024)
        context.log.info(f"Uploading {file_name} ({file_size_mb:.1f} MB)")
        
        if file_size_mb <= 50:
            try:
                file_client.upload_data(file_content, overwrite=True)
                return True
            except:
                pass  
        
        file_client.create_file()
        chunk_size = 8 * 1024 * 1024
        total_uploaded = 0
        
        while total_uploaded < len(file_content):
            chunk_end = min(total_uploaded + chunk_size, len(file_content))
            chunk_data = file_content[total_uploaded:chunk_end]
            
            for attempt in range(3):
                try:
                    file_client.append_data(chunk_data, offset=total_uploaded)
                    break
                except Exception as e:
                    if attempt == 2:
                        return False
                    if 'ssl' in str(e).lower() or 'connection' in str(e).lower():
                        time.sleep(min(30, 2 ** attempt))
                    else:
                        time.sleep(2)
            
            total_uploaded = chunk_end
        
        file_client.flush_data(len(file_content))
        return True
        
    except Exception as e:
        context.log.error(f"Upload failed: {e}")
        return False


def build_nucc_url(href):
    """Build full NUCC URL"""
    if href.startswith('http'):
        return href
    elif href.startswith('/'):
        return f"https://www.nucc.org{href}"
    else:
        return f"https://www.nucc.org/index.php/code-sets-mainmenu-41/provider-taxonomy-mainmenu-40/csv-mainmenu-57/{href}"