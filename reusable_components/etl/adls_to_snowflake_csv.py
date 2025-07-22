import os
from typing import List
from dagster_azure.adls2 import ADLS2Resource
from snowflake.snowpark import Session
from dagster import AssetExecutionContext


def copy_adls_csv_to_snowflake(
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
    
    context.log.info(f"🔍 Scanning for CSV files in {source_container}/{source_folder}")
    
    try:
        # Get file system client
        fs_client = adls2.adls2_client.get_file_system_client(source_container)
        paths = fs_client.get_paths(path=source_folder)
        
        # Filter files based on prefix if provided
        csv_files = []
        for path in paths:
            if path.is_directory:
                continue
                
            filename = os.path.basename(path.name)
            if not filename.lower().endswith('.csv'):
                continue
                
            if prefix_filter and not filename.startswith(prefix_filter):
                continue
                
            csv_files.append(path)
        
        context.log.info(f"📊 Found {len(csv_files)} CSV files to process")
        
        if not csv_files:
            context.log.warning("⚠️ No CSV files found matching criteria")
            return []
        
        # Set database and schema context
        session.use_database(target_db)
        session.use_schema(target_schema)
        
        loaded_tables: List[str] = []
        
        for path in csv_files:
            filename = os.path.basename(path.name)
            table_name = os.path.splitext(filename)[0]
            full_table = f"{target_db}.{target_schema}.{table_name}"
            
            context.log.info(f"📥 Processing file: {filename} -> {table_name}")
            
            try:
                # Construct the stage path
                stage_path = f"@{target_db}.{target_schema}.{stage_name}/{source_folder}/{filename}"
                context.log.info(f"📂 Using stage path: {stage_path}")
                
                # Check if table exists
                table_exists_sql = f"""
                    SELECT COUNT(*) as table_count 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = '{target_schema}' 
                    AND TABLE_NAME = '{table_name}'
                """
                
                table_exists = session.sql(table_exists_sql).collect()[0]['TABLE_COUNT'] > 0
                
                if not table_exists or not truncate_before_load:
                    context.log.info(f"🏗️ Creating table {table_name} using INFER_SCHEMA")
                    
                    # Use INFER_SCHEMA to create table with proper structure
                    create_table_sql = f"""
                        CREATE OR REPLACE TABLE {full_table}
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
                    context.log.info(f"✅ Table {table_name} created with inferred schema")
        
                
                elif truncate_before_load:
                    # Truncate existing table
                    context.log.info(f"🧹 Truncating existing table {table_name}")
                    session.sql(f"TRUNCATE TABLE {full_table}").collect()
                
                # Load data using COPY command
                copy_sql = f"""
                    COPY INTO {full_table}
                    FROM {stage_path}
                    FILE_FORMAT = (FORMAT_NAME = '{target_schema}.{file_format_name}')
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
        
        context.log.info(f"🎉 Successfully processed {len(loaded_tables)} out of {len(csv_files)} files")
        return loaded_tables
        
    except Exception as e:
        context.log.error(f"❌ Error in copy_adls_csv_to_snowflake: {str(e)}")
        raise

# import os
# from typing import List
# from dagster_azure.adls2 import ADLS2Resource
# from snowflake.snowpark import Session
# from dagster import get_dagster_logger
# logger = get_dagster_logger()


# def copy_adls_csv_to_snowflake(
#     session: Session,
#     adls2: ADLS2Resource,
#     source_container: str,
#     source_folder: str,
#     target_db: str,
#     target_schema: str,
#     file_format_name: str,
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

#         # # Create table if it doesn't exist
#         # session.sql(f"""
#         #     CREATE TABLE IF NOT EXISTS {full_table} (
#         #         data VARIANT
#         #     )
#         # """).collect()

#         # Clear existing data before loading
#         session.sql(f"TRUNCATE TABLE {full_table}").collect()

#         copy_sql = f"""
#             COPY INTO {full_table}
#             FROM @{target_db}.{target_schema}.PM_SA_CSV_STAGE/{path.name}
#             FILE_FORMAT = (FORMAT_NAME = '{format_fqn}')
#             ON_ERROR = 'CONTINUE'
#         """
        
#         result = session.sql(copy_sql).collect()
#         count_result = session.sql(f"SELECT COUNT(*) as row_count FROM {full_table}").collect()
#         print(f"✅ Loaded {count_result[0]['ROW_COUNT']} rows into {table_name}")
#         logger.info(f"✅ Loaded {count_result[0]['ROW_COUNT']} rows into {table_name}")
        
#         loaded_tables.append(full_table)

#     return loaded_tables


# # BEST: For Production:
# def copy_adls_csv_to_snowflake_production(
#     session: Session,
#     adls2: ADLS2Resource,
#     source_container: str,
#     source_folder: str,
#     target_db: str,
#     target_schema: str,
#     file_format_name: str = "PM_CSV_FORMAT",
# ) -> List[str]:
    
#     fs_client = adls2.adls2_client.get_file_system_client(source_container)
#     paths = fs_client.get_paths(path=source_folder)
#     loaded_tables: List[str] = []
    
#     for path in paths:
#         if path.is_directory:
#             continue
            
#         filename = os.path.basename(path.name)
#         table_name = os.path.splitext(filename)[0]
#         staging_table = f"{target_db}.{target_schema}.{table_name}_STAGING"
#         final_table = f"{target_db}.{target_schema}.{table_name}"
#         format_fqn = f"{target_schema}.{file_format_name}"
        
#         session.use_database(target_db)
#         session.use_schema(target_schema)

#         # 1. Create staging table (always clean)
#         session.sql(f"CREATE OR REPLACE TABLE {staging_table} LIKE {final_table}").collect()
        
#         # 2. Load into staging
#         copy_sql = f"""
#             COPY INTO {staging_table}
#             FROM @{target_db}.{target_schema}.PM_SA_CSV_STAGE/{path.name}
#             FILE_FORMAT = (FORMAT_NAME = '{format_fqn}')
#             ON_ERROR = 'CONTINUE'
#             FORCE = TRUE
#         """
        
#         result = session.sql(copy_sql).collect()
        
#         # 3. Swap tables atomically
#         session.sql(f"ALTER TABLE {final_table} SWAP WITH {staging_table}").collect()
        
#         # 4. Drop staging table
#         session.sql(f"DROP TABLE {staging_table}").collect()
        
#         count_result = session.sql(f"SELECT COUNT(*) as row_count FROM {final_table}").collect()
#         print(f"✅ Loaded {count_result[0]['ROW_COUNT']} rows into {table_name}")
        
#         loaded_tables.append(final_table)
        
#     return loaded_tables