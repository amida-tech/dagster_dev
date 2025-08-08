from typing import Dict, Any
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue
import os

def transform_bronze_to_silver_nucc(
    context: AssetExecutionContext,
    snowpark_session,
    audit_batch_id: int,
    config: Dict[str, Any]
) -> MaterializeResult:
    bronze_db = config.get("bronze_db", "ANALYTYXONE_DEV")
    bronze_schema = config.get("bronze_schema", "BRONZE") 
    silver_db = config.get("silver_db", "ANALYTYXONE_DEV")
    silver_schema = config.get("silver_schema", "SILVER")
    bronze_table = config.get("bronze_table", "NUCC_TAXONOMY")
    silver_table = config.get("silver_table", "NUCC_TAXONOMY")
    program_name = config.get("program_name", "NUCC")
    subject_area = config.get("subject_area", "NUCC")
    sql_files_path = config.get("sql_files_path", "./nucc/sql/silver/")
    sql_file = "nucc_taxonomy.sql"
    
    try:
        bronze_count = snowpark_session.sql(
            f"SELECT COUNT(*) FROM {bronze_db}.{bronze_schema}.{bronze_table} WHERE META_BATCH_ID = '{audit_batch_id}'"
        ).collect()[0][0]
        
        if bronze_count == 0:
            context.log.warning(f"No records found in bronze for batch_id {audit_batch_id}")
            return MaterializeResult(
                value={"status": "no_data", "batch_id": audit_batch_id},
                metadata={"status": MetadataValue.text("NO_DATA")}
            )
        
        sql_file_path = os.path.join(sql_files_path, sql_file)
        
        try:
            with open(sql_file_path, 'r') as f:
                sql_content = f.read()
        except FileNotFoundError:
            context.log.error(f"SQL file not found: {sql_file_path}")
            raise
        
        formatted_sql = sql_content.format(
            bronze_db=bronze_db,
            bronze_schema=bronze_schema,
            silver_db=silver_db,
            silver_schema=silver_schema,
            bronze_table=bronze_table,
            silver_table=silver_table,
            audit_batch_id=audit_batch_id,
            program_name=program_name,
            subject_area=subject_area
        )
        
        context.log.info(f"Transforming {silver_table}...")
        snowpark_session.sql(formatted_sql).collect()
        count_sql = f"SELECT COUNT(*) FROM {silver_db}.{silver_schema}.{silver_table} WHERE META_BATCH_ID = '{audit_batch_id}'"
        silver_count = snowpark_session.sql(count_sql).collect()[0][0]
        context.log.info(f"Transformation completed: {silver_count} records in {silver_table} for batch_id {audit_batch_id}")
        
        return MaterializeResult(
            value={
                "status": "completed",
                "batch_id": audit_batch_id,
                "table_transformed": silver_table,
                "silver_count": silver_count,
                "bronze_source_count": bronze_count
            },
            metadata={
                "status": MetadataValue.text("SUCCESS"),
                "batch_id": MetadataValue.int(audit_batch_id),
                "silver_records": MetadataValue.int(silver_count),
                "bronze_source_count": MetadataValue.int(bronze_count)
            }
        )
        
    except Exception as e:
        context.log.error(f"Transformation failed for batch_id {audit_batch_id}: {str(e)}")
        try:
            cleanup_sql = f"DELETE FROM {silver_db}.{silver_schema}.{silver_table} WHERE META_BATCH_ID = '{audit_batch_id}'"
            snowpark_session.sql(cleanup_sql).collect()
        except:
            pass
        
        return MaterializeResult(
            value={"status": "failed", "batch_id": audit_batch_id, "error": str(e)},
            metadata={"status": MetadataValue.text("FAILED")}
        )