from typing import Dict, Any
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue
import os

def generate_other_ids_sql(bronze_db, bronze_schema, silver_db, silver_schema, bronze_table, 
                          audit_batch_id, program_name, subject_area, max_identifiers=50):
    """Generate dynamic SQL for OTHER_PROVIDER_IDENTIFIER unpivot operation."""
    
    # Delete SQL
    delete_sql = f"DELETE FROM {silver_db}.{silver_schema}.PROVIDER_NPI_OTHER_IDS WHERE META_BATCH_ID = '{audit_batch_id}';"
    
    # Insert header
    insert_header = f"""
INSERT INTO {silver_db}.{silver_schema}.PROVIDER_NPI_OTHER_IDS (
    META_PROGRAM_NAME, META_SUBJECT_AREA, META_BATCH_ID, META_DATE_INSERT, META_DATE_UPDATE, META_UPD_BATCH_ID,
    NPI, OTHER_PROVIDER_IDENTIFIER_SEQ_ID, OTHER_PROVIDER_IDENTIFIER, OTHER_PROVIDER_IDENTIFIER_TYPE_CODE,
    OTHER_PROVIDER_IDENTIFIER_STATE, OTHER_PROVIDER_IDENTIFIER_ISSUER
)"""
    
    # Generate UNION ALL blocks dynamically
    union_blocks = []
    for i in range(1, max_identifiers + 1):
        union_block = f"""
SELECT '{program_name}', '{subject_area}', '{audit_batch_id}', CURRENT_TIMESTAMP(), NULL, 'NULL',
       NPI, -- PRIMARY KEY component 1
       '{i}', -- PRIMARY KEY component 2: Sequence ID from column name
       OTHER_PROVIDER_IDENTIFIER_{i}, OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_{i},
       OTHER_PROVIDER_IDENTIFIER_STATE_{i}, OTHER_PROVIDER_IDENTIFIER_ISSUER_{i}
FROM {bronze_db}.{bronze_schema}.{bronze_table}
WHERE META_BATCH_ID = '{audit_batch_id}' AND NPI IS NOT NULL AND OTHER_PROVIDER_IDENTIFIER_{i} IS NOT NULL"""
        union_blocks.append(union_block)
    
    # Combine all parts
    complete_sql = delete_sql + insert_header + "\nUNION ALL\n".join(union_blocks) + ";"
    
    return complete_sql

def transform_bronze_to_silver_npi(
    context: AssetExecutionContext,
    snowpark_session,
    audit_batch_id: int,
    config: Dict[str, Any]
) -> MaterializeResult:
    """Transform bronze NPI data into 5 silver tables using SQL files and dynamic generation."""
    
    # Get config values
    bronze_db = config.get("bronze_db", "ANALYTYXONE_DEV")
    bronze_schema = config.get("bronze_schema", "BRONZE")
    silver_db = config.get("silver_db", "ANALYTYXONE_DEV")
    silver_schema = config.get("silver_schema", "SILVER")
    bronze_table = config.get("bronze_table", "NPIDATA_PFILE")
    program_name = config.get("program_name", "NPPES")
    subject_area = config.get("subject_area", "PROVIDER")
    sql_files_path = config.get("sql_files_path", "./sql/bronze_to_silver/")
    
    # SQL files to execute
    sql_files = [
        "provider_npi.sql",
        "provider_npi_address.sql", 
        "provider_npi_taxonomy.sql",
        "provider_npi_xref.sql"
    ]
    
    # Corresponding table names
    table_names = [
        "PROVIDER_NPI",
        "PROVIDER_NPI_ADDRESS",
        "PROVIDER_NPI_TAXONOMY", 
        "PROVIDER_NPI_XREF"
    ]
    
    transformed_tables = []
    transformation_stats = {}
    
    try:
        # Check bronze data exists for this batch
        bronze_count = snowpark_session.sql(
            f"SELECT COUNT(*) FROM {bronze_db}.{bronze_schema}.{bronze_table} WHERE META_BATCH_ID = '{audit_batch_id}'"
        ).collect()[0][0]
        
        if bronze_count == 0:
            context.log.warning(f"No records found in bronze for batch_id {audit_batch_id}")
            return MaterializeResult(
                value={"status": "no_data", "batch_id": audit_batch_id},
                metadata={"status": MetadataValue.text("NO_DATA")}
            )
            
        context.log.info(f"Processing {bronze_count} records from bronze for batch_id {audit_batch_id}")
        
        # Execute each SQL file
        for i, sql_file in enumerate(sql_files):
            table_name = table_names[i]
            context.log.info(f"Transforming {table_name}...")
            
            # Read SQL file
            sql_file_path = os.path.join(sql_files_path, sql_file)
            
            try:
                with open(sql_file_path, 'r') as f:
                    sql_content = f.read()
            except FileNotFoundError:
                context.log.error(f"SQL file not found: {sql_file_path}")
                raise
            
            # Replace placeholders in SQL
            formatted_sql = sql_content.format(
                bronze_db=bronze_db,
                bronze_schema=bronze_schema,
                silver_db=silver_db,
                silver_schema=silver_schema,
                bronze_table=bronze_table,
                audit_batch_id=audit_batch_id,
                program_name=program_name,
                subject_area=subject_area
            )
            
            # Execute SQL
            snowpark_session.sql(formatted_sql).collect()
            
            # Get count for this table
            count_sql = f"SELECT COUNT(*) FROM {silver_db}.{silver_schema}.{table_name} WHERE META_BATCH_ID = '{audit_batch_id}'"
            table_count = snowpark_session.sql(count_sql).collect()[0][0]
            
            transformed_tables.append(table_name)
            transformation_stats[table_name] = table_count
            
            context.log.info(f"Completed {table_name}: {table_count} records")
        
        # Handle PROVIDER_NPI_OTHER_IDS with dynamic SQL generation
        context.log.info("Transforming PROVIDER_NPI_OTHER_IDS (dynamic generation)...")
        
        # Generate dynamic SQL for OTHER_IDS (separate DELETE and INSERT)
        delete_sql = f"DELETE FROM {silver_db}.{silver_schema}.PROVIDER_NPI_OTHER_IDS WHERE META_BATCH_ID = '{audit_batch_id}'"
        
        # Execute DELETE first
        snowpark_session.sql(delete_sql).collect()
        
        # Generate INSERT SQL only
        insert_header = f"""
INSERT INTO {silver_db}.{silver_schema}.PROVIDER_NPI_OTHER_IDS (
    META_PROGRAM_NAME, META_SUBJECT_AREA, META_BATCH_ID, META_DATE_INSERT, META_DATE_UPDATE, META_UPD_BATCH_ID,
    NPI, OTHER_PROVIDER_IDENTIFIER_SEQ_ID, OTHER_PROVIDER_IDENTIFIER, OTHER_PROVIDER_IDENTIFIER_TYPE_CODE,
    OTHER_PROVIDER_IDENTIFIER_STATE, OTHER_PROVIDER_IDENTIFIER_ISSUER
)"""
        
        # Generate UNION ALL blocks
        union_blocks = []
        for i in range(1, 51):  # 1 to 50
            union_block = f"""
SELECT '{program_name}', '{subject_area}', '{audit_batch_id}', CURRENT_TIMESTAMP(), NULL, NULL,
       NPI, -- PRIMARY KEY component 1
       '{i}', -- PRIMARY KEY component 2: Sequence ID from column name
       OTHER_PROVIDER_IDENTIFIER_{i}, OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_{i},
       OTHER_PROVIDER_IDENTIFIER_STATE_{i}, OTHER_PROVIDER_IDENTIFIER_ISSUER_{i}
FROM {bronze_db}.{bronze_schema}.{bronze_table}
WHERE META_BATCH_ID = '{audit_batch_id}' AND NPI IS NOT NULL AND OTHER_PROVIDER_IDENTIFIER_{i} IS NOT NULL"""
            union_blocks.append(union_block)
        
        # Combine INSERT with UNION blocks
        insert_sql = insert_header + "\nUNION ALL\n".join(union_blocks)
        
        # Execute INSERT
        snowpark_session.sql(insert_sql).collect()
        
        # Get count for OTHER_IDS table
        other_ids_count = snowpark_session.sql(
            f"SELECT COUNT(*) FROM {silver_db}.{silver_schema}.PROVIDER_NPI_OTHER_IDS WHERE META_BATCH_ID = '{audit_batch_id}'"
        ).collect()[0][0]
        
        transformed_tables.append("PROVIDER_NPI_OTHER_IDS")
        transformation_stats["PROVIDER_NPI_OTHER_IDS"] = other_ids_count
        
        context.log.info(f"Completed PROVIDER_NPI_OTHER_IDS: {other_ids_count} records")
        
        # Summary
        total_records = sum(transformation_stats.values())
        context.log.info(f"Transformation completed: {total_records} total records across {len(transformed_tables)} tables for batch_id {audit_batch_id}")
        
        return MaterializeResult(
            value={
                "status": "completed",
                "batch_id": audit_batch_id,
                "tables_transformed": transformed_tables,
                "transformation_stats": transformation_stats,
                "total_records": total_records,
                "bronze_source_count": bronze_count
            },
            metadata={
                "status": MetadataValue.text("SUCCESS"),
                "batch_id": MetadataValue.int(audit_batch_id),
                "total_records": MetadataValue.int(total_records),
                "tables_transformed": MetadataValue.int(len(transformed_tables)),
                "bronze_source_count": MetadataValue.int(bronze_count)
            }
        )
        
    except Exception as e:
        context.log.error(f"Transformation failed for batch_id {audit_batch_id}: {str(e)}")
        
        # Cleanup on failure - remove records for this batch_id
        cleanup_tables = table_names + ["PROVIDER_NPI_OTHER_IDS"]
        for table_name in cleanup_tables:
            try:
                cleanup_sql = f"DELETE FROM {silver_db}.{silver_schema}.{table_name} WHERE META_BATCH_ID = '{audit_batch_id}'"
                snowpark_session.sql(cleanup_sql).collect()
            except:
                pass
        
        return MaterializeResult(
            value={"status": "failed", "batch_id": audit_batch_id, "error": str(e)},
            metadata={"status": MetadataValue.text("FAILED")}
        )