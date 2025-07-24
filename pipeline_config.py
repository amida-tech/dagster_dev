# Base configuration
BASE_CONFIG = {
    "snowflake_db": "ANALYTYXONE_DEV",
    "snowflake_schema": "BRONZE", 
    "snowflake_stage": "PARQUET_STAGE",
    "stage_container": "srcfiles",
    "program_name": "MEDICAID"
}

# Recipient Pipeline Configuration
RECIPIENT_CONFIG = {
    **BASE_CONFIG,
    "pipeline_name": "MEDICAID_RECIPIENT",
    "subject_area": "RECIPIENT", 
    "asset_name": "recipient_files_monitor",
    "sftp_source_path": "/prod/mmis/recipient",
    "file_criteria": {
        "prefix": {"pattern": ["R_MMIS"], "count": 9},
        "suffix": {"pattern": None, "count": 0},
        "contains": {"pattern": None, "count": 0},
        "not_contains": {"pattern": None, "count": 0},
        "regex": {"pattern": None, "count": 0},
        "extension": {"pattern": None, "count": 0}
    },
    "RULE_GROUP": "RECIP_001",
    "RULE_ID": "RL_G_RECIPIENT_ELIG_ENROLL_008",
    "REFRESH_SUMMARY": None,
    "downstream_assets": [
        "start_dq_audit_run_recipient",
        "copy_mftserver_recipient_files_to_srcfiles_stage", 
        "load_dq_transactions_recipient",
        "unzip_recipient_files_to_load",
        "archive_recipient_files",
        "load_dq_control_table_recipient",
        "dq_recipient_row_count_validation",
        "dq_schema_check_recipient",
        "load_csv_to_iceberg_recipient"  
    ],
    "stage_directory": "medicaid/recipient/stage",
    "load_directory": "medicaid/recipient/load", 
    "archive_directory": "medicaid/recipient/archive",
    "control_file": "R_MMIS_RECIPIENT_CONTROL_FILE.csv",
    "group_name": "recipient_file_processing",
    "alert_config": {
        "program_name": "Medicaid Recipient Data Processing",
        "send_success_alerts": True
    }
}

# Provider Pipeline Configuration  
PROVIDER_CONFIG = {
    **BASE_CONFIG,
    "pipeline_name": "MEDICAID_PROVIDER",
    "subject_area": "PROVIDER",
    "asset_name": "provider_files_monitor", 
    "sftp_source_path": "/prod/mmis/provider",
    "file_criteria": {
        "prefix": {"pattern": ["P_MMIS"], "count": 11},
        "suffix": {"pattern": None, "count": 0},
        "contains": {"pattern": None, "count": 0},
        "not_contains": {"pattern": None, "count": 0},
        "regex": {"pattern": None, "count": 0},
        "extension": {"pattern": None, "count": 0}
    },
    "downstream_assets": [
        "start_dq_audit_run_provider",
        "copy_mftserver_provider_files_to_srcfiles_stage",
        "load_dq_transactions_provider",
        "unzip_provider_files_to_load",
        "archive_provider_files",
        "load_dq_control_table_provider",
        "dq_provider_row_count_validation",
        "dq_schema_check_provider",
        "load_csv_to_iceberg_provider"
    ],
    "stage_directory": "medicaid/provider/stage",
    "load_directory": "medicaid/provider/load",
    "archive_directory": "medicaid/provider/archive", 
    "control_file": "P_MMIS_PROVIDER_CONTROL_FILE.csv",
    "group_name": "provider_file_processing",
    "alert_config": {
        "program_name": "Medicaid Provider Data Processing",
        "send_success_alerts": True
    }
}
