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
        "load_csv_to_iceberg_recipient",
        "execute_rules_asset_recipient",
        "cleanup_recipient_directories"  
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
        "load_csv_to_iceberg_provider",
        "execute_rules_asset_provider",
        "cleanup_provider_directories"
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

# NPPES Pipeline Configuration  
NPPES_CONFIG = {
    **BASE_CONFIG,
    "pipeline_name": "NPPES_PIPELINE",
    "subject_area": "NPPES",
    "asset_name": "nppes_files_monitor",
    "source_type": "nppes",                    
    "sftp_source_path": "/prod/mmis/nppes",
    "file_criteria": {
        "prefix": {"pattern": ["npidata_pfile"], "count": 1},
        "suffix": {"pattern": None, "count": 0},
        "contains": {"pattern": None, "count": 0},
        "not_contains": {"pattern": None, "count": 0},
        "regex": {"pattern": None, "count": 0},
        "extension": {"pattern": [".csv"], "count": 1}
    },
    "file_name": "",
    "column_mapping_file_name": "npi_column_mapping.sql",
    "column_mapping_file_path": "./medicaid/npi/sql/bronze/npi_column_mapping.sql",
    "sql_files_path": "./medicaid/npi/sql/silver/",
    "gold_db": "ANALYTYXONE_DEV",
    "gold_schema": "GOLD",
    "silver_db": "ANALYTYXONE_DEV",
    "silver_schema": "SILVER",  
    "RULE_GROUP": "RECIP_001",
    "RULE_ID": None,
    "REFRESH_SUMMARY": None,
    "downstream_assets": [
        "start_dq_audit_run_nppes",
        "load_dq_transactions_nppes", 
        "unzip_nppes_files_to_load",
        "archive_nppes_files",
        "dq_schema_check_nppes",
        "load_csv_to_iceberg_nppes",
        "transform_bronze_to_silver_nppes",
        "transform_silver_to_gold_nppes"
        "execute_rules_asset_nppes",
        "cleanup_nppes_directories"
    ],
    "stage_directory": "nppes/stage",
    "load_directory": "nppes/load",  
    "archive_directory": "nppes/archive",
    "control_file": "", 
    "group_name": "nppes_file_processing",
    "alert_config": {
        "program_name": "NPPES Processing",
        "send_success_alerts": True
    }
}

# NUCC Pipeline Configuration
NUCC_CONFIG = {
    **BASE_CONFIG,
    "pipeline_name": "NUCC_PIPELINE",
    "subject_area": "NUCC",
    "asset_name": "nucc_files_monitor",
    "source_type": "nucc",
    "sftp_source_path": "/prod/mmis/nucc",
    "file_criteria": {
        "prefix": {"pattern": ["nucc_taxonomy"], "count": 1},
        "suffix": {"pattern": None, "count": 0},
        "contains": {"pattern": None, "count": 0},
        "not_contains": {"pattern": None, "count": 0},
        "regex": {"pattern": None, "count": 0},
        "extension": {"pattern": [".csv"], "count": 1}
    },
    "file_name": "",
    "column_mapping_file_name": "nucc_column_mapping.sql",
    "column_mapping_file_path": "./medicaid/nucc/sql/bronze/nucc_column_mapping.sql",
    "sql_files_path": "./medicaid/nucc/sql/silver/",
    "bronze_table": "NUCC_TAXONOMY",
    "silver_table": "NUCC_TAXONOMY",
    "gold_table": "NUCC_TAXONOMY",
    "gold_db": "ANALYTYXONE_DEV",
    "gold_schema": "GOLD",
    "silver_db": "ANALYTYXONE_DEV",
    "silver_schema": "SILVER",  
    "RULE_GROUP": "RECIP_001",
    "RULE_ID": None,
    "REFRESH_SUMMARY": None,
    "downstream_assets": [
        "start_dq_audit_run_nucc",
        "load_dq_transactions_nucc", 
        "copy_nucc_files_to_load",
        "archive_nucc_files",
        "dq_schema_check_nucc",
        "load_csv_to_iceberg_nucc",
        "transform_bronze_to_silver_nppes",
        "transform_silver_to_gold_nppes",
        "execute_rules_asset_nucc",
        "cleanup_nucc_directories"
    ],
    "stage_directory": "nucc/stage",
    "load_directory": "nucc/load",  
    "archive_directory": "nucc/archive",
    "control_file": "", 
    "group_name": "nucc_file_processing",
    "alert_config": {
        "program_name": "NUCC Data Processing",
        "send_success_alerts": True
    }
}