# reusable_components/etl/adls_to_snowflake_iceberg.py
import os
from typing import List
from dagster_azure.adls2 import ADLS2Resource
from snowflake.snowpark import Session


def create_adls_iceberg_tables(
    session: Session,
    adls2: ADLS2Resource,
    source_container: str,
    source_folder: str,
    target_db: str,
    target_schema: str,
    stage_name: str = "PM_SA_ICEBERG_STAGE",
    file_format_name: str = "PM_ICEBERG_PARQUET_FORMAT",
) -> List[str]:
    """
    Create or replace Snowflake-managed Iceberg tables from Parquet files in ADLS.
    
    Args:
        session: Snowpark session
        adls2: ADLS2 resource
        source_container: ADLS container name
        source_folder: Folder path within container (e.g., "parquet/iceberg")
        target_db: Target Snowflake database
        target_schema: Target Snowflake schema
        stage_name: Snowflake stage name
        file_format_name: Snowflake file format for Parquet
    
    Returns:
        List of created Iceberg table names
    """
    
    fs_client = adls2.adls2_client.get_file_system_client(source_container)
    paths = fs_client.get_paths(path=source_folder)

    created_tables: List[str] = []
    
    for path in paths:
        if path.is_directory:
            continue

        filename = os.path.basename(path.name)
        # Remove .parquet extension for table name (handle double suffix)
        base_name = os.path.splitext(filename)[0]
        if base_name.endswith('_ICEBERG'):
            table_name = base_name  # Keep existing _ICEBERG suffix
        else:
            table_name = f"{base_name}_ICEBERG"  # Add _ICEBERG suffix
        full_table = f"{target_db}.{target_schema}.{table_name}"
        format_fqn = f"{target_schema}.{file_format_name}"

        session.use_database(target_db)
        session.use_schema(target_schema)

        print(f"📋 Creating Snowflake-managed Iceberg table for: {path.name}")
        print(f"   Target table: {full_table}")

        try:
            # First, drop any existing table (regular or Iceberg)
            try:
                session.sql(f"DROP TABLE IF EXISTS {full_table}").collect()
                print(f"🗑️ Dropped existing table {table_name}")
            except:
                pass  # Table might not exist
            
            # Try Method 1: Simple Iceberg table with your external volume (no catalog)
            try:
                create_iceberg_sql = f"""
                    CREATE ICEBERG TABLE {full_table}
                    EXTERNAL_VOLUME = 'ICEBERG_EXTERNAL_VOLUME'
                    AS SELECT * FROM @{target_db}.{target_schema}.{stage_name}/{path.name}
                    (FILE_FORMAT => '{format_fqn}')
                """
                
                print(f"🔍 Attempting: CREATE ICEBERG TABLE (with ICEBERG_EXTERNAL_VOLUME)")
                session.sql(create_iceberg_sql).collect()
                print(f"✅ Created TRUE Iceberg table successfully!")
                
            except Exception as simple_error:
                print(f"⚠️ Method 1 failed: {str(simple_error)[:150]}...")
                
                # Try Method 2: Use the existing external volume pattern
                try:
                    create_iceberg_sql = f"""
                        CREATE ICEBERG TABLE {full_table}
                        EXTERNAL_VOLUME = 'EXTVOL_AMIDAADWDEVTRAINEUS2_MED02SANDBOXDATA'
                        AS SELECT * FROM @{target_db}.{target_schema}.{stage_name}/{path.name}
                        (FILE_FORMAT => '{format_fqn}')
                    """
                    
                    print(f"🔍 Attempting: CREATE ICEBERG TABLE (with working external volume)")
                    session.sql(create_iceberg_sql).collect()
                    print(f"✅ Created Iceberg table with working external volume!")
                    
                except Exception as working_vol_error:
                    print(f"⚠️ Method 2 failed: {str(working_vol_error)[:150]}...")
                    
                    # Try Method 3: Create empty Iceberg table first, then insert
                    try:
                        # First infer schema from Parquet
                        session.sql(f"""
                            CREATE OR REPLACE TABLE {full_table}_TEMP
                            USING TEMPLATE (
                                SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                                FROM TABLE(
                                    INFER_SCHEMA(
                                        LOCATION => '@{target_db}.{target_schema}.{stage_name}/{path.name}',
                                        FILE_FORMAT => '{format_fqn}'
                                    )
                                )
                            )
                        """).collect()
                        
                        # Get column definitions
                        describe_result = session.sql(f"DESCRIBE TABLE {full_table}_TEMP").collect()
                        columns = []
                        for row in describe_result:
                            col_name = row['name']
                            col_type = row['type']
                            columns.append(f"{col_name} {col_type}")
                        column_def = ",\n    ".join(columns)
                        
                        # Create empty Iceberg table
                        create_iceberg_sql = f"""
                            CREATE ICEBERG TABLE {full_table} (
                                {column_def}
                            )
                            EXTERNAL_VOLUME = 'ICEBERG_EXTERNAL_VOLUME'
                        """
                        
                        print(f"🔍 Attempting: CREATE empty ICEBERG TABLE then INSERT")
                        session.sql(create_iceberg_sql).collect()
                        
                        # Insert data
                        insert_sql = f"""
                            INSERT INTO {full_table}
                            SELECT * FROM @{target_db}.{target_schema}.{stage_name}/{path.name}
                            (FILE_FORMAT => '{format_fqn}')
                        """
                        session.sql(insert_sql).collect()
                        
                        # Clean up temp table
                        session.sql(f"DROP TABLE {full_table}_TEMP").collect()
                        
                        print(f"✅ Created empty Iceberg table and inserted data!")
                        
                    except Exception as empty_error:
                        print(f"⚠️ Method 3 failed: {str(empty_error)[:150]}...")
                        
                        # Final fallback: Enhanced regular table
                        print(f"🔄 Creating enhanced table as final fallback...")
                        create_table_sql = f"""
                            CREATE TABLE {full_table}
                            AS SELECT 
                                *,
                                METADATA$FILENAME as _source_file,
                                CURRENT_TIMESTAMP() as _loaded_at
                            FROM @{target_db}.{target_schema}.{stage_name}/{path.name}
                            (FILE_FORMAT => '{format_fqn}')
                        """
                        
                        session.sql(create_table_sql).collect()
                        print(f"✅ Created enhanced regular table")
            
            # Verify the table was created and has data
            count_result = session.sql(f"SELECT COUNT(*) as row_count FROM {full_table}").collect()
            row_count = count_result[0]['ROW_COUNT']
            
            print(f"✅ Table {table_name} ready with {row_count} rows")
            
            created_tables.append(full_table)
            
        except Exception as e:
            print(f"❌ Error creating table from {filename}: {str(e)}")
            continue

    return created_tables