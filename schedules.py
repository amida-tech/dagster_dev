# schedules.py
from dagster import schedule, ScheduleEvaluationContext, RunRequest, SkipReason, DefaultScheduleStatus
from datetime import time
from pipeline_config import RECIPIENT_CONFIG, PROVIDER_CONFIG, NPPES_CONFIG, NUCC_CONFIG
from reusable_components.file_processing.monitor_files import get_current_file_count_with_criteria, calculate_expected_file_count

# NPPES Pipeline Schedule
@schedule(
    job_name="nppes_pipeline_job",
    cron_schedule="0 8 * * *",  # Every day at 4:00 AM
    default_status=DefaultScheduleStatus.RUNNING,
    description="Runs NPPES pipeline every morning at 4 AM, but only if files are found"
)
def nppes_morning_schedule(context: ScheduleEvaluationContext):
    """Check for NPPES files before running the pipeline"""
    current_time = context.scheduled_execution_time
    context.log.info(f"🔍 NPPES: Checking for files at scheduled time: {current_time}")
    
    try:
        file_count = get_current_file_count_with_criteria(
            context,
            NPPES_CONFIG["sftp_source_path"],
            NPPES_CONFIG["file_criteria"]
        )
        
        if file_count is None:
            context.log.error("NPPES: Could not connect to SFTP")
            return SkipReason("NPPES: SFTP connection failed")
        
        expected_count = calculate_expected_file_count(NPPES_CONFIG["file_criteria"])
        context.log.info(f"NPPES: Found {file_count} files, expected {expected_count}")
        
        if file_count >= expected_count:
            context.log.info("✅ NPPES: Files found. Triggering pipeline run.")
            return RunRequest(
                run_key=f"nppes_morning_run_{current_time.strftime('%Y%m%d_%H%M')}",
                tags={"source": "nppes_morning_schedule", "pipeline": "NPPES"}
            )
        else:
            context.log.info("❌ NPPES: No files found. Skipping pipeline run.")
            return SkipReason("NPPES: No files found at scheduled time")
            
    except Exception as e:
        context.log.error(f"NPPES: Error checking files: {e}")
        return SkipReason(f"NPPES: Error checking files: {e}")

# NUCC Pipeline Schedule
@schedule(
    job_name="nucc_pipeline_job",
    cron_schedule="0 4 * * *",  # Every day at 4:00 AM
    default_status=DefaultScheduleStatus.RUNNING,
    description="Runs NUCC pipeline every morning at 4 AM, but only if files are found"
)
def nucc_morning_schedule(context: ScheduleEvaluationContext):
    """Check for NUCC files before running the pipeline"""
    current_time = context.scheduled_execution_time
    context.log.info(f"🔍 NUCC: Checking for files at scheduled time: {current_time}")
    
    try:
        file_count = get_current_file_count_with_criteria(
            context,
            NUCC_CONFIG["sftp_source_path"],
            NUCC_CONFIG["file_criteria"]
        )
        
        if file_count is None:
            context.log.error("NUCC: Could not connect to SFTP")
            return SkipReason("NUCC: SFTP connection failed")
        
        expected_count = calculate_expected_file_count(NUCC_CONFIG["file_criteria"])
        context.log.info(f"NUCC: Found {file_count} files, expected {expected_count}")
        
        if file_count >= expected_count:
            context.log.info("✅ NUCC: Files found. Triggering pipeline run.")
            return RunRequest(
                run_key=f"nucc_morning_run_{current_time.strftime('%Y%m%d_%H%M')}",
                tags={"source": "nucc_morning_schedule", "pipeline": "NUCC"}
            )
        else:
            context.log.info("❌ NUCC: No files found. Skipping pipeline run.")
            return SkipReason("NUCC: No files found at scheduled time")
            
    except Exception as e:
        context.log.error(f"NUCC: Error checking files: {e}")
        return SkipReason(f"NUCC: Error checking files: {e}")

# Provider Pipeline Schedule
@schedule(
    job_name="provider_pipeline_job",
    cron_schedule="0 4 * * *",  # Every day at 4:00 AM
    default_status=DefaultScheduleStatus.RUNNING,
    description="Runs Medicaid Provider pipeline every morning at 4 AM, but only if files are found"
)
def provider_morning_schedule(context: ScheduleEvaluationContext):
    """Check for Provider files before running the pipeline"""
    current_time = context.scheduled_execution_time
    context.log.info(f"🔍 PROVIDER: Checking for files at scheduled time: {current_time}")
    
    try:
        file_count = get_current_file_count_with_criteria(
            context,
            PROVIDER_CONFIG["sftp_source_path"],
            PROVIDER_CONFIG["file_criteria"]
        )
        
        if file_count is None:
            context.log.error("PROVIDER: Could not connect to SFTP")
            return SkipReason("PROVIDER: SFTP connection failed")
        
        expected_count = calculate_expected_file_count(PROVIDER_CONFIG["file_criteria"])
        context.log.info(f"PROVIDER: Found {file_count} files, expected {expected_count}")
        
        if file_count >= expected_count:
            context.log.info("✅ PROVIDER: Files found. Triggering pipeline run.")
            return RunRequest(
                run_key=f"provider_morning_run_{current_time.strftime('%Y%m%d_%H%M')}",
                tags={"source": "provider_morning_schedule", "pipeline": "PROVIDER"}
            )
        else:
            context.log.info("❌ PROVIDER: No files found. Skipping pipeline run.")
            return SkipReason("PROVIDER: No files found at scheduled time")
            
    except Exception as e:
        context.log.error(f"PROVIDER: Error checking files: {e}")
        return SkipReason(f"PROVIDER: Error checking files: {e}")

# Recipient Pipeline Schedule
@schedule(
    job_name="recipient_pipeline_job",
    cron_schedule="0 4 * * *", 
    default_status=DefaultScheduleStatus.RUNNING,
    description="Runs Medicaid Recipient pipeline every morning at 4 AM, but only if files are found"
)
def recipient_morning_schedule(context: ScheduleEvaluationContext):
    """Check for Recipient files before running the pipeline"""
    current_time = context.scheduled_execution_time
    context.log.info(f"🔍 RECIPIENT: Checking for files at scheduled time: {current_time}")
    
    try:
        file_count = get_current_file_count_with_criteria(
            context,
            RECIPIENT_CONFIG["sftp_source_path"],
            RECIPIENT_CONFIG["file_criteria"]
        )
        
        if file_count is None:
            context.log.error("RECIPIENT: Could not connect to SFTP")
            return SkipReason("RECIPIENT: SFTP connection failed")
        
        expected_count = calculate_expected_file_count(RECIPIENT_CONFIG["file_criteria"])
        context.log.info(f"RECIPIENT: Found {file_count} files, expected {expected_count}")
        
        if file_count >= expected_count:
            context.log.info("✅ RECIPIENT: Files found. Triggering pipeline run.")
            return RunRequest(
                run_key=f"recipient_morning_run_{current_time.strftime('%Y%m%d_%H%M')}",
                tags={"source": "recipient_morning_schedule", "pipeline": "RECIPIENT"}
            )
        else:
            context.log.info("❌ RECIPIENT: No files found. Skipping pipeline run.")
            return SkipReason("RECIPIENT: No files found at scheduled time")
            
    except Exception as e:
        context.log.error(f"RECIPIENT: Error checking files: {e}")
        return SkipReason(f"RECIPIENT: Error checking files: {e}")

