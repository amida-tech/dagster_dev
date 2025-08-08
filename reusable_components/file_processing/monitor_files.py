import os
import json
import re
from datetime import datetime
import paramiko
import io
from typing import Dict, List, Any, Optional, Tuple

from dagster import (
    asset, AssetExecutionContext, sensor, DefaultSensorStatus, 
    AssetSelection, RunRequest, SkipReason, AssetKey, 
    MaterializeResult, MetadataValue
)
from reusable_components.error_handling.standardized_alerts import send_pipeline_alert, with_pipeline_alerts


def create_standardized_file_monitor(pipeline_config: Dict[str, Any]) -> Tuple[callable, callable]:
    """
    Creates a standardized file monitor that works for any pipeline
    
    Args:
        pipeline_config: Configuration dictionary containing:
            - pipeline_name: Name of the pipeline (e.g., "MEDICAID_RECIPIENT")
            - asset_name: Name for the monitoring asset (e.g., "recipient_files_monitor")
            - sftp_source_path: SFTP directory to monitor
            - file_criteria: File matching criteria dictionary
            - downstream_assets: List of asset names to trigger when complete
            - group_name: Dagster group name
            - alert_config: Optional alert configuration overrides
    
    Returns:
        Tuple of (monitor_asset_function, monitor_sensor_function)
    """
    
    # Extract configuration
    pipeline_name = pipeline_config["pipeline_name"]
    asset_name = pipeline_config["asset_name"]
    directory_path = pipeline_config["sftp_source_path"]
    file_criteria = pipeline_config["file_criteria"]
    downstream_assets = pipeline_config.get("downstream_assets", [])
    group_name = pipeline_config.get("group_name", f"{pipeline_name.lower()}_processing")
    alert_config = pipeline_config.get("alert_config", {})
    
    # Calculate total expected files
    total_expected = calculate_expected_file_count(file_criteria)
    
    # ==========================================
    # PART 1: CREATE THE MONITORING ASSET
    # ==========================================
    @asset(
        name=asset_name,
        required_resource_keys={"adls_sftp", "adls_access_keys"},
        description=f"Monitor {directory_path} for {pipeline_name} files (expected: {total_expected})",
        group_name=group_name
    )
    @with_pipeline_alerts(
        pipeline_name=pipeline_name,
        alert_config=alert_config
    )
    def monitor_asset(context: AssetExecutionContext) -> MaterializeResult:
        """
        Standardized file monitoring asset that works for any pipeline
        """
        
        context.log.info(f"🔍 {pipeline_name} File Monitor Starting")
        context.log.info(f"   Directory: {directory_path}")
        context.log.info(f"   Expected files: {total_expected}")
        
        _log_file_criteria(context, file_criteria)
        
        # Set up memory file for this specific pipeline
        memory_file = f"/tmp/{asset_name}_memory.json"
        
        # Connect to SFTP
        sftp = context.resources.adls_sftp
        
        try:
            # Get all files from SFTP directory
            all_files = sftp.listdir(directory_path)
            context.log.info(f"   Total files in directory: {len(all_files)}")
            
            # Match files against criteria
            matched_files = _match_files_against_criteria(all_files, file_criteria, context)
            
            # Calculate totals
            total_found = _calculate_total_found(matched_files)
            
            # Load previous state
            old_memory = _load_memory_file(memory_file)
            old_file_count = old_memory.get('total_count', 0)
            alert_already_sent = old_memory.get('alert_sent', False)
            
            # Check if file count changed (reset alert status if so)
            if total_found != old_file_count:
                context.log.info(f"🔄 File count changed from {old_file_count} to {total_found} - resetting alert status")
                alert_already_sent = False
            
            # Check if we have complete set
            have_complete_set = _check_complete_set(matched_files, file_criteria, context)
            should_send_alert = False
            monitoring_status = "WAITING"
            
            if have_complete_set and not alert_already_sent:
                # SUCCESS! Complete set found
                context.log.info("🚨 SUCCESS! Complete file set detected! 🚨")
                context.log.info(f"   Found {total_found}/{total_expected} files")
                monitoring_status = "COMPLETE"
                
                _log_matched_files(context, matched_files)
                
                # Send success alert
                should_send_alert = _send_file_complete_alert(
                    context, total_found, directory_path, pipeline_name, alert_config
                )
                
                if should_send_alert and downstream_assets:
                    context.log.info(f"🚀 Ready to trigger pipeline: {downstream_assets}")
                    
            elif have_complete_set and alert_already_sent:
                # Files still complete, alert already sent
                context.log.info(f"✅ Complete set maintained ({total_found} files, alert already sent)")
                monitoring_status = "COMPLETE_NOTIFIED"
                
            else:
                # Still waiting or issues
                context.log.info(f"📁 Progress: {total_found}/{total_expected} files")
                if total_found < total_expected:
                    context.log.info("⏳ Waiting for complete set...")
                    monitoring_status = "INCOMPLETE"
                elif total_found > total_expected:
                    context.log.info("⚠️ Excess files detected")
                    monitoring_status = "EXCESS_FILES"
                else:
                    context.log.info("❌ File criteria not fully met")
                    monitoring_status = "CRITERIA_NOT_MET"
            
            # Save current state
            _save_memory_file(memory_file, matched_files, alert_already_sent or should_send_alert, total_found)
            
            # Prepare result
            result_value = {
                "pipeline_name": pipeline_name,
                "directory": directory_path,
                "criteria": file_criteria,
                "expected_total": total_expected,
                "found_total": total_found,
                "matched_files": matched_files,
                "complete": have_complete_set,
                "alert_sent": should_send_alert or alert_already_sent,
                "timestamp": datetime.now().isoformat(),
                "downstream_assets": downstream_assets,
                "status": monitoring_status,
                "all_found_files": _get_all_found_files(matched_files)
            }
            
            # Create comprehensive metadata
            metadata = _create_monitoring_metadata(
                monitoring_status, have_complete_set, should_send_alert or alert_already_sent,
                total_found, total_expected, directory_path, downstream_assets,
                file_criteria, matched_files, context
            )
            
            context.log.info(f"✅ {pipeline_name} monitoring completed: {monitoring_status}")
            
            return MaterializeResult(value=result_value, metadata=metadata)
            
        except Exception as e:
            context.log.error(f"❌ {pipeline_name} file monitoring failed: {str(e)}")
            
            # Return error result
            error_result = {
                "pipeline_name": pipeline_name,
                "directory": directory_path,
                "status": "ERROR",
                "error": str(e),
                "expected_total": total_expected,
                "found_total": 0,
                "complete": False,
                "alert_sent": False,
                "timestamp": datetime.now().isoformat()
            }
            
            return MaterializeResult(
                value=error_result,
                metadata={
                    "monitoring_status": MetadataValue.text("❌ ERROR"),
                    "pipeline_name": MetadataValue.text(pipeline_name),
                    "error_message": MetadataValue.text(str(e)[:200] + "..." if len(str(e)) > 200 else str(e)),
                    "summary": MetadataValue.text(f"❌ {pipeline_name} monitoring failed")
                }
            )
            
        finally:
            try:
                sftp.close()
            except:
                pass
    
    # ==========================================
    # PART 2: CREATE THE SENSOR
    # ==========================================
    @sensor(
        asset_selection=AssetSelection.keys(*[AssetKey(asset_name)] + [AssetKey(asset) for asset in downstream_assets]),
        minimum_interval_seconds=30,
        default_status=DefaultSensorStatus.RUNNING,
        name=f"{asset_name}_sensor"
    )
    def monitor_sensor(context):
        """
        Standardized sensor that works for any pipeline
        """
        
        # Set up memory files
        sensor_memory_file = f"/tmp/{asset_name}_sensor_simple.json"
        pipeline_complete_file = f"/tmp/{asset_name}_pipeline_complete.json"
        
        # Check if pipeline already completed
        if _pipeline_already_completed(pipeline_complete_file):
            return SkipReason(f"{pipeline_name} pipeline already completed. Monitoring stopped.")
        
        try:
            # Get current file count
            current_file_count = get_current_file_count_with_criteria(context, directory_path, file_criteria)
            if current_file_count is None:
                return SkipReason(f"{pipeline_name}: Could not connect to SFTP")
            
            # Check alert status
            alert_already_sent = _check_alert_status(asset_name)
            
            # Load previous count
            previous_count = _load_previous_count(sensor_memory_file)
            
            # Save current count
            _save_current_count(sensor_memory_file, current_file_count)
            
            # Log status
            _log_sensor_status(context, pipeline_name, previous_count, current_file_count, total_expected, alert_already_sent)
            
            # Decide what to do
            has_complete_set = current_file_count == total_expected
            needs_alert = has_complete_set and not alert_already_sent
            
            if current_file_count != previous_count:
                # File count changed
                return _handle_file_count_change(
                    context, needs_alert, current_file_count, total_expected,
                    pipeline_complete_file, asset_name, downstream_assets, pipeline_name
                )
            
            elif needs_alert:
                # Count same but need alert
                return _handle_missed_alert(
                    context, current_file_count, total_expected,
                    pipeline_complete_file, asset_name, downstream_assets, pipeline_name
                )
            
            else:
                # Nothing to do
                if has_complete_set and alert_already_sent:
                    return SkipReason(f"{pipeline_name}: Complete set present, alert sent ({current_file_count} files)")
                else:
                    return SkipReason(f"{pipeline_name}: No change ({current_file_count} files)")
                    
        except Exception as e:
            context.log.error(f"{pipeline_name} sensor error: {str(e)}")
            return RunRequest(asset_selection=[AssetKey(asset_name)])
    
    return monitor_asset, monitor_sensor


# ==========================================
# HELPER FUNCTIONS
# ==========================================

def calculate_expected_file_count(file_criteria: Dict[str, Any]) -> int:
    """Calculate total expected files from criteria"""
    total_expected = 0
    for criteria_type, criteria_config in file_criteria.items():
        if criteria_type in ["extension", "not_contains"]:
            continue  # These are filters, not counters
        count = criteria_config.get("count", 0)
        if count > 0:
            total_expected += count
    return total_expected


def _calculate_total_found(matched_files: Dict[str, List[str]]) -> int:
    """Calculate total files found (excluding filter-only criteria)"""
    return sum(len(files) for criteria_type, files in matched_files.items() 
               if criteria_type not in ["extension", "not_contains"])


def _get_all_found_files(matched_files: Dict[str, List[str]]) -> List[str]:
    """Get unique list of all found files"""
    all_files = []
    for criteria_type, files in matched_files.items():
        if files and criteria_type not in ["extension", "not_contains"]:
            all_files.extend(files)
    return list(set(all_files))


def _create_monitoring_metadata(monitoring_status, have_complete_set, alert_sent, 
                               total_found, total_expected, directory_path, downstream_assets,
                               file_criteria, matched_files, context):
    """Create comprehensive metadata for monitoring result"""
    
    # Get file breakdown
    criteria_breakdown = {}
    all_found_files = _get_all_found_files(matched_files)
    
    for criteria_type, files in matched_files.items():
        if files:
            criteria_breakdown[criteria_type] = len(files)
    
    return {
        # Status Information
        "monitoring_status": MetadataValue.text(f"🔍 {monitoring_status}"),
        "files_complete": MetadataValue.bool(have_complete_set),
        "alert_sent": MetadataValue.bool(alert_sent),
        
        # File Counts
        "files_found": MetadataValue.int(total_found),
        "files_expected": MetadataValue.int(total_expected),
        "completion_ratio": MetadataValue.text(f"{total_found}/{total_expected} files"),
        
        # Directory Information
        "source_directory": MetadataValue.text(directory_path),
        "downstream_assets": MetadataValue.text(", ".join(downstream_assets) if downstream_assets else "None"),
        
        # File Criteria Summary
        "monitoring_criteria": MetadataValue.text(_format_criteria_summary(file_criteria)),
        
        # Criteria Breakdown
        **{f"criteria_{k}": MetadataValue.text(f"{v} files") 
           for k, v in criteria_breakdown.items()},
        
        # File Lists (truncated for display)
        "found_files": MetadataValue.text(
            ", ".join(all_found_files[:10]) + 
            (f"... (+{len(all_found_files)-10} more)" if len(all_found_files) > 10 else "")
            if all_found_files else "None"
        ),
        
        # Timing
        "last_check": MetadataValue.text(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        "run_id": MetadataValue.text(context.run_id),
        
        # Status Summary
        "summary": MetadataValue.text(
            f"✅ READY: All {total_found} files found" if have_complete_set and alert_sent
            else f"✅ COMPLETE: Files ready" if have_complete_set
            else f"⏳ WAITING: {total_found}/{total_expected} files" if total_found < total_expected
            else f"⚠️ EXCESS: Too many files ({total_found})"
        )
    }


def _send_file_complete_alert(context, total_found, directory_path, pipeline_name, alert_config):
    """Send alert when files are complete"""
    
    success = send_pipeline_alert(
        context=context,
        pipeline_name=pipeline_name,
        trigger_type="info",
        message=f"🎉 {pipeline_name} COMPLETE! All {total_found} files ready in {directory_path}",
        alert_config=alert_config
    )
    
    if success:
        context.log.info("✅ File complete alert sent successfully!")
        return True
    else:
        context.log.error("❌ Failed to send file complete alert")
        return False


def _format_criteria_summary(file_criteria):
    """Format file criteria into readable summary"""
    criteria_parts = []
    for criteria_type, criteria_config in file_criteria.items():
        pattern = criteria_config.get("pattern", "")
        count = criteria_config.get("count", 0)
        if pattern and count > 0:
            criteria_parts.append(f"{criteria_type}='{pattern}'({count})")
    return ", ".join(criteria_parts)


def _log_file_criteria(context, file_criteria):
    """Log file criteria in readable format"""
    context.log.info("   File Criteria:")
    for criteria_type, criteria_config in file_criteria.items():
        patterns = criteria_config.get("pattern", "")
        count = criteria_config.get("count", 0)
        
        if isinstance(patterns, list):
            pattern_str = f"{patterns}"
        else:
            pattern_str = f"'{patterns}'"
        
        if criteria_type == "not_contains":
            context.log.info(f"     • {criteria_type}: {pattern_str} (exclude files containing these)")
        else:
            context.log.info(f"     • {criteria_type}: {pattern_str} (expect {count} files)")


def _match_files_against_criteria(all_files, file_criteria, context):
    """Match files against criteria with proper filtering"""
    matched_files = {}
    
    # Apply extension filter first
    if "extension" in file_criteria:
        extension_patterns = file_criteria["extension"].get("pattern", "")
        if extension_patterns:
            if isinstance(extension_patterns, str):
                extension_patterns = [extension_patterns]
            
            filtered_files = []
            for f in all_files:
                for ext in extension_patterns:
                    if f.endswith(ext):
                        filtered_files.append(f)
                        break
            
            all_files = filtered_files
            context.log.info(f"     📁 Extension filter {extension_patterns}: {len(all_files)} files")
    
    # Apply not_contains filter
    if "not_contains" in file_criteria:
        not_contains_patterns = file_criteria["not_contains"].get("pattern", "")
        if not_contains_patterns:
            if isinstance(not_contains_patterns, str):
                not_contains_patterns = [not_contains_patterns]
            elif not_contains_patterns is None:
                not_contains_patterns = []
            
            filtered_files = []
            for f in all_files:
                should_exclude = False
                for pattern in not_contains_patterns:
                    if pattern in f:
                        should_exclude = True
                        context.log.info(f"     🚫 Excluding: {f} (contains '{pattern}')")
                        break
                
                if not should_exclude:
                    filtered_files.append(f)
            
            all_files = filtered_files
            context.log.info(f"     📁 Not_contains filter: {len(all_files)} files remain")
    
    # Apply counting criteria
    for criteria_type, criteria_config in file_criteria.items():
        if criteria_type in ["extension", "not_contains"]:
            continue
            
        patterns = criteria_config.get("pattern", "")
        expected_count = criteria_config.get("count", 0)
        
        if expected_count == 0:
            continue
        
        if isinstance(patterns, str):
            patterns = [patterns] if patterns else []
        elif patterns is None:
            patterns = []
        
        matches = []
        for file in all_files:
            for pattern in patterns:
                if _file_matches_criteria(file, criteria_type, pattern):
                    matches.append(file)
                    break
        
        matched_files[criteria_type] = matches
        context.log.info(f"     {criteria_type} {patterns}: {len(matches)}/{expected_count} files")
    
    # Add filter results for reference
    if "extension" in file_criteria:
        matched_files["extension"] = all_files
    
    return matched_files


def _file_matches_criteria(filename, criteria_type, pattern):
    """Check if file matches specific criteria"""
    if criteria_type == "prefix":
        if isinstance(pattern, list):
            return any(filename.startswith(p) for p in pattern)
        return filename.startswith(pattern)
    
    elif criteria_type == "suffix":
        if isinstance(pattern, list):
            return any(filename.endswith(s) for s in pattern)
        return filename.endswith(pattern)
    
    elif criteria_type == "regex":
        return bool(re.search(pattern, filename))
    
    elif criteria_type == "extension":
        if isinstance(pattern, list):
            return any(filename.endswith(e) for e in pattern)
        return filename.endswith(pattern)
    
    elif criteria_type == "contains":
        if isinstance(pattern, list):
            return any(c in filename for c in pattern)
        return pattern in filename
    
    elif criteria_type == "not_contains":
        if isinstance(pattern, list):
            return not any(nc in filename for nc in pattern)
        return pattern not in filename
    
    return False


def _check_complete_set(matched_files, file_criteria, context):
    """Check if we have complete file set"""
    for criteria_type, criteria_config in file_criteria.items():
        expected_count = criteria_config.get("count", 0)
        
        if criteria_type in ["extension", "not_contains"]:
            if criteria_type == "extension":
                extension_files = matched_files.get("extension", [])
                expected_extension_count = criteria_config.get("count", 0)
                
                if expected_extension_count > 0 and len(extension_files) < expected_extension_count:
                    context.log.info(f"     ❌ {criteria_type}: {len(extension_files)}/{expected_extension_count}")
                    return False
            continue
        
        if expected_count == 0:
            continue
            
        actual_count = len(matched_files.get(criteria_type, []))
        
        if actual_count != expected_count:
            context.log.info(f"     ❌ {criteria_type}: {actual_count}/{expected_count}")
            return False
        else:
            context.log.info(f"     ✅ {criteria_type}: {actual_count}/{expected_count}")
    
    return True


def _log_matched_files(context, matched_files):
    """Log all matched files by criteria"""
    for criteria_type, files in matched_files.items():
        if files and criteria_type not in ["extension", "not_contains"]:
            context.log.info(f"     {criteria_type.upper()} files:")
            for file in files:
                context.log.info(f"       ✅ {file}")


# Memory and sensor helper functions (keeping original logic)
def _load_memory_file(memory_file):
    """Load previous memory state"""
    if os.path.exists(memory_file):
        try:
            with open(memory_file, 'r') as f:
                return json.load(f)
        except:
            return {}
    return {}


def _save_memory_file(memory_file, matched_files, alert_sent, total_count):
    """Save current state to memory"""
    new_memory = {
        'matched_files': matched_files,
        'alert_sent': alert_sent,
        'total_count': total_count,
        'last_check': datetime.now().isoformat()
    }
    with open(memory_file, 'w') as f:
        json.dump(new_memory, f)


def get_current_file_count_with_criteria(context, directory_path, file_criteria):
    """Quick SFTP connection to get current file count"""
    try:
        host = os.getenv("SFTP_HOST")
        user = os.getenv("SFTP_USERNAME")
        pem_body = os.getenv("ADLSSFTP_PEM_CONTENT")
        
        if not all([host, user, pem_body]):
            context.log.error("Missing SFTP connection info")
            return None
        
        key_stream = io.StringIO(pem_body)
        key = paramiko.RSAKey.from_private_key(key_stream)
        key_stream.close()
        
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=host, port=22, username=user, pkey=key, 
                      look_for_keys=False, allow_agent=False, timeout=10)
        
        sftp = client.open_sftp()
        all_files = sftp.listdir(directory_path)
        
        # Apply same filtering logic
        matched_files = _match_files_against_criteria(all_files, file_criteria, context)
        total_count = _calculate_total_found(matched_files)
        
        sftp.close()
        client.close()
        
        return total_count
        
    except Exception as e:
        context.log.error(f"Error connecting to SFTP: {e}")
        return None


def _pipeline_already_completed(pipeline_complete_file):
    """Check if pipeline already completed"""
    if os.path.exists(pipeline_complete_file):
        try:
            with open(pipeline_complete_file, 'r') as f:
                complete_data = json.load(f)
                return complete_data.get('completion_time') is not None
        except:
            pass
    return False


def _check_alert_status(asset_name):
    """Check if alert was already sent"""
    asset_memory_file = f"/tmp/{asset_name}_memory.json"
    if os.path.exists(asset_memory_file):
        try:
            with open(asset_memory_file, 'r') as f:
                asset_memory = json.load(f)
                return asset_memory.get('alert_sent', False)
        except:
            pass
    return False


def _load_previous_count(sensor_memory_file):
    """Load previous file count"""
    if os.path.exists(sensor_memory_file):
        try:
            with open(sensor_memory_file, 'r') as f:
                sensor_memory = json.load(f)
                return sensor_memory.get('file_count', 0)
        except:
            pass
    return 0


def _save_current_count(sensor_memory_file, current_file_count):
    """Save current file count"""
    with open(sensor_memory_file, 'w') as f:
        json.dump({'file_count': current_file_count, 'last_check': datetime.now().isoformat()}, f)


def _log_sensor_status(context, pipeline_name, previous_count, current_file_count, expected_count, alert_already_sent):
    """Log sensor status"""
    context.log.info(f"📊 {pipeline_name} Sensor Status:")
    context.log.info(f"   Previous: {previous_count}, Current: {current_file_count}, Expected: {expected_count}")
    context.log.info(f"   Alert sent: {alert_already_sent}, Changed: {current_file_count != previous_count}")


def _handle_file_count_change(context, needs_alert, current_file_count, expected_count, 
                             pipeline_complete_file, asset_name, downstream_assets, pipeline_name):
    """Handle file count changes"""
    context.log.info(f"🔍 {pipeline_name} file count changed")
    
    if needs_alert:
        context.log.info(f"🎉 {pipeline_name} COMPLETE SET! Triggering pipeline")
        _mark_pipeline_complete(pipeline_complete_file, current_file_count, expected_count, pipeline_name)
        return _trigger_all_assets(context, asset_name, downstream_assets, pipeline_name)
    else:
        context.log.info(f"{pipeline_name} progress update - running monitor")
        return RunRequest(asset_selection=[AssetKey(asset_name)])


def _handle_missed_alert(context, current_file_count, expected_count, 
                        pipeline_complete_file, asset_name, downstream_assets, pipeline_name):
    """Handle missed alert scenario"""
    context.log.info(f"🚨 {pipeline_name} COMPLETE SET DETECTED! Triggering pipeline")
    _mark_pipeline_complete(pipeline_complete_file, current_file_count, expected_count, pipeline_name)
    return _trigger_all_assets(context, asset_name, downstream_assets, pipeline_name)


def _mark_pipeline_complete(pipeline_complete_file, current_file_count, expected_count, pipeline_name):
    """Mark pipeline as complete"""
    with open(pipeline_complete_file, 'w') as f:
        json.dump({
            'pipeline_name': pipeline_name,
            'completion_time': datetime.now().isoformat(),
            'files_processed': current_file_count,
            'expected_count': expected_count
        }, f)


def _trigger_all_assets(context, asset_name, downstream_assets, pipeline_name):
    """Trigger all pipeline assets"""
    asset_selection = [AssetKey(asset_name)] + [AssetKey(asset) for asset in downstream_assets]
    
    context.log.info(f"🚀 {pipeline_name} Pipeline Assets:")
    for key in asset_selection:
        context.log.info(f"   - {key.path[-1]}")
    
    context.log.info("🛑 Pipeline triggered - sensor will stop on next run")
    return RunRequest(asset_selection=asset_selection)