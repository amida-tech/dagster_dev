# # reusable_components/etl/adls_to_snowflake_parquet.py
import os
from typing import List
from dagster_azure.adls2 import ADLS2Resource
from snowflake.snowpark import Session
from dagster import AssetExecutionContext

def copy_adls_parquet_to_snowflake(
    context: AssetExecutionContext,
    session: Session,
    adls2: ADLS2Resource,
    source_container: str,
    source_folder: str,
    target_db: str,
    target_schema: str,
    file_format_name: str,
    prefix_filter: str,
    stage_name: str,
    truncate_before_load: bool,
) -> List[str]:
    """
    Copy Parquet files from ADLS to Snowflake tables.
    Creates new tables or optionally truncates existing ones before loading data.
    
    Args:
        truncate_before_load: If True, truncates existing tables before loading.
                             If False, appends data to existing tables.
    """
    
    context.log.info(f"🔍 Scanning for Parquet files in {source_container}/{source_folder}")
    
    try:
        # Get file system client
        fs_client = adls2.adls2_client.get_file_system_client(source_container)
        paths = fs_client.get_paths(path=source_folder)
        
        # Filter files based on prefix and extension
        parquet_files = []
        for path in paths:
            if path.is_directory:
                continue
                
            filename = os.path.basename(path.name)

            if path.name.count('/') > 2:  
                continue

            if not filename.lower().endswith('.parquet'):
                continue
                
            if prefix_filter and not filename.startswith(prefix_filter):
                continue
                
            parquet_files.append(path)
        
        context.log.info(f"📊 Found {len(parquet_files)} Parquet files to process")
        
        if not parquet_files:
            context.log.warning("⚠️ No Parquet files found matching criteria")
            return []
        
        # Set database and schema context
        session.use_database(target_db)
        session.use_schema(target_schema)
        
        loaded_tables: List[str] = []
        
        for path in parquet_files:
            filename = os.path.basename(path.name)
            table_name = os.path.splitext(filename)[0]
            full_table = f"{target_db}.{target_schema}.{table_name}"
            
            context.log.info(f"📥 Processing file: {filename} -> {table_name}")
            
            try:
                # Check if table exists
                table_exists_sql = f"""
                    SELECT COUNT(*) as table_count 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = '{target_schema}' 
                    AND TABLE_NAME = '{table_name}'
                """
                
                table_exists = session.sql(table_exists_sql).collect()[0]['TABLE_COUNT'] > 0
                
                # Construct the stage path
                stage_path = f"@{target_db}.{target_schema}.{stage_name}/{path.name}"
                context.log.info(f"📂 Using stage path: {stage_path}")
                
                if table_exists:
                    # Table exists - handle based on truncate_before_load parameter
                    if truncate_before_load:
                        context.log.info(f"🧹 Table {table_name} exists, truncating and loading fresh data")
                        session.sql(f"TRUNCATE TABLE {full_table}").collect()
                    else:
                        context.log.info(f"📝 Table {table_name} exists, appending new data")
                else:
                    # Table doesn't exist - create it
                    context.log.info(f"🏗️ Creating table {table_name} using INFER_SCHEMA")
                    
                    # Create table with inferred schema from Parquet file
                    create_table_sql = f"""
                        CREATE TABLE {full_table}
                        USING TEMPLATE (
                            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                            FROM TABLE(
                                INFER_SCHEMA(
                                    LOCATION => '{stage_path}',
                                    FILE_FORMAT => '{target_schema}.{file_format_name}'
                                )
                            )
                        )
                    """
                    
                    session.sql(create_table_sql).collect()
                    context.log.info(f"✅ Table {table_name} created with inferred Parquet schema")
                
                # Load data using COPY command (this runs for both existing and new tables)
                copy_sql = f"""
                    COPY INTO {full_table}
                    FROM {stage_path}
                    FILE_FORMAT = (FORMAT_NAME = '{target_schema}.{file_format_name}')
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                    ON_ERROR = 'CONTINUE'
                    FORCE = TRUE
                """
                
                context.log.info(f"🔄 Executing COPY command for {table_name}")
                copy_result = session.sql(copy_sql).collect()
                
                # Log copy results for debugging
                if copy_result:
                    for result_row in copy_result:
                        context.log.info(f"📊 Copy result: {result_row}")
                
                # Get row count to verify load
                count_result = session.sql(f"SELECT COUNT(*) as row_count FROM {full_table}").collect()
                row_count = count_result[0]['ROW_COUNT']
                
                if row_count > 0:
                    context.log.info(f"✅ Successfully loaded {row_count} rows into {table_name}")
                    loaded_tables.append(full_table)
                else:
                    context.log.warning(f"⚠️ No rows loaded into {table_name}")
                    
                    # Additional debugging for zero rows
                    context.log.info(f"🔍 Debugging: Checking stage file existence")
                    try:
                        list_result = session.sql(f"LIST {stage_path}").collect()
                        context.log.info(f"📋 Stage file check: {list_result}")
                    except Exception as list_error:
                        context.log.warning(f"⚠️ Could not list stage file: {list_error}")
                
            except Exception as file_error:
                context.log.error(f"❌ Failed to load {filename}: {str(file_error)}")
                # Continue processing other files instead of failing the entire job
                continue
        
        context.log.info(f"🎉 Successfully processed {len(loaded_tables)} out of {len(parquet_files)} Parquet files")
        return loaded_tables
        
    except Exception as e:
        context.log.error(f"❌ Error in copy_adls_parquet_to_snowflake: {str(e)}")
        raise

# from dagster import get_dagster_logger

# def copy_adls_parquet_to_snowflake(
#     session: Session,
#     adls2: ADLS2Resource,
#     source_container: str,
#     source_folder: str,
#     target_db: str,
#     target_schema: str,
#     file_format_name: str,
#     stage_name: str = "PM_SA_PARQUET_STAGE",
# ) -> List[str]:
    
#     fs_client = adls2.adls2_client.get_file_system_client(source_container)
#     paths = fs_client.get_paths(path=source_folder)

#     loaded_tables: List[str] = []
    
#     for path in paths:
#         if path.is_directory:
#             continue

#         filename = os.path.basename(path.name)
#         table_name = os.path.splitext(filename)[0]
#         full_table = f"{target_db}.{target_schema}.{table_name}"
#         format_fqn = f"{target_schema}.{file_format_name}"

#         session.use_database(target_db)
#         session.use_schema(target_schema)

#         # For Parquet files, we need to create/recreate the table with proper schema
#         # First, infer the schema from the Parquet file
#         session.sql(f"DROP TABLE IF EXISTS {full_table}").collect()
        
#         # Create table with inferred schema from Parquet file
#         session.sql(f"""
#             CREATE TABLE {full_table}
#             USING TEMPLATE (
#                 SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
#                 FROM TABLE(
#                     INFER_SCHEMA(
#                         LOCATION => '@{target_db}.{target_schema}.{stage_name}/{path.name}',
#                         FILE_FORMAT => '{format_fqn}'
#                     )
#                 )
#             )
#         """).collect()
        
#         logger.info(f"✅ Created table {table_name} with inferred schema")

#         copy_sql = f"""
#             COPY INTO {full_table}
#             FROM @{target_db}.{target_schema}.{stage_name}/{path.name}
#             FILE_FORMAT = (FORMAT_NAME = '{format_fqn}')
#             MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
#             ON_ERROR = 'CONTINUE'
#         """
        
#         result = session.sql(copy_sql).collect()
#         count_result = session.sql(f"SELECT COUNT(*) as row_count FROM {full_table}").collect()
#         logger.info(f"✅ Loaded {count_result[0]['ROW_COUNT']} rows into {table_name}")
        
#         loaded_tables.append(full_table)

#     return loaded_tables