from dagster import schedule, ScheduleEvaluationContext, RunRequest, SkipReason, DefaultScheduleStatus
from datetime import time
from pipeline_config import RECIPIENT_CONFIG, PROVIDER_CONFIG, NPPES_CONFIG, NUCC_CONFIG
from reusable_components.file_processing.monitor_files import _get_current_file_count_with_criteria, _calculate_expected_file_count

@schedule(
    job_name="daily_morning_pipelines",
    cron_schedule="0 17 * * *",  # Every day at 4:00 AM
    default_status=DefaultScheduleStatus.RUNNING,
    description="Runs all pipelines (NPPES, NUCC, Medicaid Provider, Medicaid Recipient) every morning at 4 AM, but only if files are found"
)
def daily_morning_schedule(context: ScheduleEvaluationContext):
    """
    Custom evaluation function that checks for files before running the pipeline.
    Only triggers the job if files are found, otherwise skips.
    """
    current_time = context.scheduled_execution_time
    context.log.info(f"🔍 Checking for files at scheduled time: {current_time}")
    
    # Define pipeline configurations to check
    pipeline_configs = [
        ("NPPES", NPPES_CONFIG),
        ("NUCC", NUCC_CONFIG), 
        ("Medicaid Provider", PROVIDER_CONFIG),
        ("Medicaid Recipient", RECIPIENT_CONFIG)
    ]
    
    files_found = False
    found_pipelines = []
    
    # Check each pipeline for files
    for pipeline_name, config in pipeline_configs:
        try:
            context.log.info(f"Checking {pipeline_name} pipeline...")
            
            # Use existing monitor function to check files
            file_count = _get_current_file_count_with_criteria(
                context, 
                config["sftp_source_path"], 
                config["file_criteria"]
            )
            
            if file_count is None:
                context.log.error(f"Could not connect to SFTP for {pipeline_name}")
                continue
            
            expected_count = _calculate_expected_file_count(config["file_criteria"])
            context.log.info(f"{pipeline_name}: Found {file_count} files, expected {expected_count}")
            
            if file_count >= expected_count:
                files_found = True
                found_pipelines.append(pipeline_name)
                context.log.info(f"✅ Files found for {pipeline_name}")
            else:
                context.log.info(f"❌ No files found for {pipeline_name}")
                
        except Exception as e:
            context.log.error(f"Error checking {pipeline_name} pipeline: {e}")
    
    if files_found:
        context.log.info(f"✅ Files found for {len(found_pipelines)} pipeline(s): {', '.join(found_pipelines)}. Triggering pipeline run.")
        return RunRequest(
            run_key=f"daily_morning_run_{current_time.strftime('%Y%m%d_%H%M')}",
            tags={"source": "daily_morning_schedule", "pipelines_with_files": str(found_pipelines)}
        )
    else:
        context.log.info("❌ No files found for any pipeline. Skipping pipeline run.")
        return SkipReason("No files found at scheduled time")