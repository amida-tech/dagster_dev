from typing import List, Optional
from dagster import AssetExecutionContext
from azure.storage.filedatalake import DataLakeServiceClient

def copy_all_parquets_to_iceberg(
    context: AssetExecutionContext,
    adls_client: DataLakeServiceClient,
    container: str,
    folder: str,
    db: str,
    schema: str,
    stage_name: str,
    program_name: str,
    subject_area: str,
    batch_id: int,
    prefix: Optional[str] = None,
    suffix: Optional[str] = None,
    contains: Optional[str] = None,
    not_contains: Optional[str] = None,
    regex: Optional[str] = None,
    extension: Optional[str] = None
) -> List[str]:
    """
    1) List all .parquet files in ADLS under container/folder whose
    basename starts with prefix.
    2) For each file, strip off ".parquet" to get table_name.
    3) Issue COPY INTO db.schema.table_name FROM @stage_name/<path>
    using MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE'.
    Returns the list of tables that were loaded.
    """
    log = context.log
    # Use the client directly
    file_system = adls_client.get_file_system_client(container)
    log.info(f"🔍 Listing files in {container}/{folder} with prefix '{prefix}'")
    
    paths = []
    for path in file_system.get_paths(path=folder):
        filename = path.name.split('/')[-1] 
        # Apply filtering
        if prefix and not filename.startswith(prefix):
            continue
        if not filename.lower().endswith(".parquet"):
            continue
        paths.append(path.name)
    
    loaded_tables: List[str] = []
    for full_path in paths:
        filename = full_path.rsplit("/", 1)[-1]
        table_name = filename[:-8] 
        full_table = f"{db}.{schema}.{table_name}"
        stage_path = f"@{db}.{schema}.{stage_name}/{full_path}"
        log.info(f"➡️ Copying {filename} → Iceberg table {full_table}")
        context.resources.snowflake_snowpark.sql(f"""
            COPY INTO {full_table}
            FROM {stage_path}
            FILE_FORMAT = (TYPE = 'PARQUET')
            MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE'
            FORCE = TRUE
        """).collect()
        log.info(f"📝 Back-filling metadata on new rows in {full_table}")
        context.resources.snowflake_snowpark.sql(f"""
            UPDATE {full_table}
            SET
              "META_PROGRAM_NAME"  = '{program_name}',
              "META_SUBJECT_AREA"  = '{subject_area}',
              "META_BATCH_ID"      = {batch_id},
              "META_DATE_INSERT"   = CURRENT_TIMESTAMP()
            WHERE "META_BATCH_ID" IS NULL
        """).collect()

        loaded_tables.append(table_name)

    log.info(f"✅ Completed COPY for tables: {loaded_tables}")
    return loaded_tables

