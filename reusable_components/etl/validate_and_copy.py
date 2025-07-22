# import os
# import io
# import csv
# from typing import Dict, List

# from dagster import OpExecutionContext
# from dagster_azure.adls2 import ADLS2Resource
# from snowflake.snowpark import Session
# from dagster import get_dagster_logger

# logger = get_dagster_logger()

# def _read_table_metadata(
#     adls2: ADLS2Resource,
#     metadata_container: str,
#     metadata_path: str,
# ) -> Dict[str, List[str]]:
#     fs = adls2.adls2_client.get_file_system_client(metadata_container)
#     file_client = fs.get_file_client(metadata_path)
#     download = file_client.download_file().readall()
#     text = download.decode("utf-8").splitlines()
#     reader = csv.DictReader(text)
#     meta: Dict[str, List[str]] = {}
#     for row in reader:
#         cols = [c.strip() for c in row["COLUMNS"].split(",") if c.strip()]
#         meta[row["FILE_NAME"].strip()] = cols
#     return meta

# def _read_header(
#     adls2: ADLS2Resource,
#     source_container: str,
#     source_folder: str,
#     filename: str,
# ) -> List[str]:
#     fs = adls2.adls2_client.get_file_system_client(source_container)
#     file_client = fs.get_file_client(f"{source_folder}/{filename}")
#     # read first 4KB (should include header)
#     raw = file_client.download_file(offset=0, length=4096).readall()
#     header = raw.decode("utf-8").splitlines()[0]
#     return [c.strip() for c in header.split(",")]

# def validate_and_copy(
#     context: OpExecutionContext,
#     session: Session,
#     adls2: ADLS2Resource,
#     *,
#     metadata_container: str,
#     metadata_path: str,
#     source_container: str,
#     source_folder: str,
#     prefix: str,
#     target_db: str,
#     target_schema: str,
#     file_format_name: str,
# ) -> Dict[str, List[str]]:
#     passed: List[str] = []
#     failed: List[str] = []

#     # 1) read metadata CSV
#     metadata = _read_table_metadata(adls2, metadata_container, metadata_path)
#     context.log.info(f"Loaded metadata for {len(metadata)} files")

#     # 2) list and filter source files
#     fs = adls2.adls2_client.get_file_system_client(source_container)
#     paths = fs.get_paths(path=source_folder)
#     candidates = [
#         os.path.basename(p.name)
#         for p in paths
#         if not p.is_directory and os.path.basename(p.name).startswith(prefix)
#     ]
#     context.log.info(f"Found {len(candidates)} files starting with {prefix}")

#     # 3) prep snowflake
#     session.use_database(target_db)
#     session.use_schema(target_schema)
#     stage_fqn = f"{target_db}.{target_schema}.PM_SA_CSV_STAGE"
#     fmt_fqn   = f"{target_schema}.{file_format_name}"

#     # 4) validate & load
#     for fname in candidates:
#         expected = metadata.get(fname)
#         if not expected:
#             context.log.error(f"No metadata entry for {fname}; skipping")
#             failed.append(fname)
#             continue

#         actual = _read_header(adls2, source_container, source_folder, fname)
#         if actual != expected:
#             context.log.error(
#                 f"Column mismatch for {fname}\n"
#                 f"  expected: {expected}\n"
#                 f"  actual:   {actual}"
#             )
#             failed.append(fname)
#             continue

#         # ensure table exists
#         table = os.path.splitext(fname)[0]
#         full_table = f"{target_db}.{target_schema}.{table}"
#         session.sql(f"""
#             CREATE TABLE IF NOT EXISTS {full_table} (
#               data VARIANT
#             )
#         """).collect()

#         # truncate and copy
#         session.sql(f"TRUNCATE TABLE {full_table}").collect()
#         copy_sql = (
#             f"COPY INTO {full_table} "
#             f"FROM @{stage_fqn}/{source_folder}/{fname} "
#             f"FILE_FORMAT=(FORMAT_NAME='{fmt_fqn}') "
#             "ON_ERROR='CONTINUE'"
#         )
#         session.sql(copy_sql).collect()
#         context.log.info(f"✅ Loaded {fname} into {full_table}")
#         passed.append(fname)

#     return {"passed_files": passed, "failed_files": failed}

import os
import csv
from typing import Dict, List
from dagster import AssetExecutionContext, get_dagster_logger
from dagster_azure.adls2 import ADLS2Resource
from snowflake.snowpark import Session

logger = get_dagster_logger()

def load_metadata_from_csv(
    adls2: ADLS2Resource,
    metadata_container: str,
    metadata_path: str,
) -> Dict[str, List[str]]:
    """
    Load TABLE_METADATA.csv and return mapping of file names to expected columns.
    
    Args:
        adls2: ADLS2 resource
        metadata_container: Container containing metadata file
        metadata_path: Path to TABLE_METADATA.csv file
        
    Returns:
        Dictionary mapping file names to list of expected column names
    """
    try:
        # Read the metadata file from ADLS
        fs = adls2.adls2_client.get_file_system_client(metadata_container)
        file_client = fs.get_file_client(metadata_path)
        download = file_client.download_file().readall()
        text = download.decode("utf-8").strip()
        
        # Debug logging
        logger.info(f"📋 Raw metadata file content (first 300 chars): {text[:300]}")
        
        lines = text.splitlines()
        if not lines:
            raise ValueError("Metadata file is empty")
        
        # Parse CSV content
        reader = csv.DictReader(lines)
        metadata_mapping: Dict[str, List[str]] = {}
        
        # Debug: Show available columns
        logger.info(f"📋 Available columns in metadata file: {reader.fieldnames}")
        
        if not reader.fieldnames:
            raise ValueError("No columns found in metadata file")
        
        # Find the correct column names (handle variations)
        file_name_column = None
        columns_column = None
        
        for field in reader.fieldnames:
            field_upper = field.strip().upper()
            if field_upper in ['FILE_NAME', 'FILENAME', 'FILE']:
                file_name_column = field
            elif field_upper in ['COLUMNS', 'COLUMN', 'COLS']:
                columns_column = field
        
        if not file_name_column:
            raise ValueError(f"File name column not found. Available columns: {reader.fieldnames}")
        if not columns_column:
            raise ValueError(f"Columns column not found. Available columns: {reader.fieldnames}")
            
        logger.info(f"📋 Using columns: file_name='{file_name_column}', columns='{columns_column}'")
        
        # Read metadata rows
        for row_num, row in enumerate(reader, start=2):
            try:
                file_name = row[file_name_column].strip()
                columns_str = row[columns_column].strip()
                
                if not file_name:
                    logger.warning(f"⚠️ Empty file name in row {row_num}, skipping")
                    continue
                    
                if not columns_str:
                    logger.warning(f"⚠️ Empty columns for file {file_name} in row {row_num}, skipping")
                    continue
                
                # Parse and clean column names
                columns = [col.strip() for col in columns_str.split(",") if col.strip()]
                metadata_mapping[file_name] = columns
                
                logger.info(f"📋 Loaded metadata for {file_name}: {len(columns)} columns")
                
            except Exception as e:
                logger.error(f"❌ Error processing row {row_num}: {str(e)}")
                continue
            
        logger.info(f"📋 Successfully loaded metadata for {len(metadata_mapping)} files")
        return metadata_mapping
        
    except Exception as e:
        logger.error(f"❌ Failed to load metadata from {metadata_container}/{metadata_path}: {str(e)}")
        raise

def extract_csv_column_headers(
    adls2: ADLS2Resource,
    source_container: str,
    source_folder: str,
    filename: str,
) -> List[str]:
    """
    Extract column headers from a CSV file in ADLS.
    
    Args:
        adls2: ADLS2 resource
        source_container: Container containing the CSV file
        source_folder: Folder containing the CSV file
        filename: Name of the CSV file
        
    Returns:
        List of column names from the header row
    """
    try:
        fs = adls2.adls2_client.get_file_system_client(source_container)
        file_path = f"{source_folder}/{filename}"
        file_client = fs.get_file_client(file_path)
        
        # Read enough data to get the header line
        raw_data = file_client.download_file(offset=0, length=8192).readall()
        text = raw_data.decode("utf-8")
        
        # Get first line (header)
        lines = text.splitlines()
        if not lines:
            raise ValueError(f"File {filename} appears to be empty")
            
        header_line = lines[0].strip()
        
        # Parse header and clean column names
        columns = []
        for col in header_line.split(","):
            # Remove quotes and whitespace
            cleaned_col = col.strip().strip('"').strip("'").strip()
            if cleaned_col:  # Only add non-empty columns
                columns.append(cleaned_col)
        
        logger.info(f"📄 Extracted {len(columns)} columns from {filename}")
        return columns
        
    except Exception as e:
        logger.error(f"❌ Failed to extract headers from {source_container}/{source_folder}/{filename}: {str(e)}")
        raise

def create_snowflake_table_if_not_exists(
    session: Session,
    table_name: str,
    target_db: str,
    target_schema: str,
    columns: List[str],
) -> bool:
    """
    Create a Snowflake table with the specified column schema if it doesn't already exist.
    
    Args:
        session: Snowflake session
        table_name: Name of the table to create
        target_db: Target database
        target_schema: Target schema
        columns: List of column names for the table
        
    Returns:
        True if table was created, False if it already existed
    """
    try:
        full_table_name = f"{target_db}.{target_schema}.{table_name}"
        
        # Check if table already exists
        check_table_sql = f"""
            SELECT COUNT(*) as table_count
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{target_schema.upper()}' 
            AND TABLE_NAME = '{table_name.upper()}'
            AND TABLE_CATALOG = '{target_db.upper()}'
        """
        
        result = session.sql(check_table_sql).collect()
        table_exists = result[0]['TABLE_COUNT'] > 0
        
        if table_exists:
            logger.info(f"📊 Table {full_table_name} already exists, skipping creation")
            return False
        
        # Create table if it doesn't exist
        columns_definitions = []
        for col in columns:
            # Ensure column name is properly quoted
            column_def = f'"{col.upper()}" VARCHAR(16777216)'
            columns_definitions.append(column_def)
        
        columns_sql = ", ".join(columns_definitions)
        
        create_table_sql = f"""
            CREATE TABLE {full_table_name} (
                {columns_sql}
            )
        """
        
        session.sql(create_table_sql).collect()
        logger.info(f"📊 Created new table {full_table_name} with {len(columns)} columns")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to create table {table_name}: {str(e)}")
        raise

def get_files_with_prefix(
    adls2: ADLS2Resource,
    container: str,
    folder: str,
    prefix: str,
) -> List[str]:
    """
    Get list of files in ADLS folder that start with the given prefix.
    
    Args:
        adls2: ADLS2 resource
        container: Container name
        folder: Folder path
        prefix: File name prefix to filter by
        
    Returns:
        List of file names matching the prefix
    """
    try:
        fs = adls2.adls2_client.get_file_system_client(container)
        paths = fs.get_paths(path=folder)
        
        matching_files = []
        for path in paths:
            if not path.is_directory:
                filename = os.path.basename(path.name)
                if filename.startswith(prefix):
                    matching_files.append(filename)
        
        logger.info(f"📁 Found {len(matching_files)} files with prefix '{prefix}' in {container}/{folder}")
        return matching_files
        
    except Exception as e:
        logger.error(f"❌ Failed to list files in {container}/{folder}: {str(e)}")
        raise

def validate_csv_files_and_load_to_snowflake(
    context: AssetExecutionContext,
    session: Session,
    adls2: ADLS2Resource,
    *,
    metadata_container: str,
    metadata_path: str,
    source_container: str,
    source_folder: str,
    prefix_filter: str,
    target_db: str,
    target_schema: str,
    file_format_name: str,
    stage_name: str,
) -> Dict[str, List[str]]:
    """
    Validate CSV files against metadata schema and load valid files to Snowflake.
    
    Args:
        context: Dagster execution context
        session: Snowflake session
        adls2: ADLS2 resource
        metadata_container: Container containing TABLE_METADATA.csv
        metadata_path: Path to TABLE_METADATA.csv
        source_container: Container containing source CSV files
        source_folder: Folder containing source CSV files
        prefix_filter: Prefix filter for file names (e.g., "CLAIMS_")
        target_db: Target Snowflake database
        target_schema: Target Snowflake schema
        file_format_name: Snowflake file format name
        stage_name: Snowflake stage name
        
    Returns:
        Dictionary containing lists of passed and failed files
    """
    validated_files: List[str] = []
    failed_files: List[str] = []
    
    try:
        # Step 1: Load metadata from CSV
        context.log.info(f"📋 Loading metadata from {metadata_container}/{metadata_path}")
        file_metadata = load_metadata_from_csv(adls2, metadata_container, metadata_path)
        context.log.info(f"📋 Loaded metadata for {len(file_metadata)} files")
        
        # Step 2: Get list of files to process
        context.log.info(f"📁 Getting files from {source_container}/{source_folder} with prefix '{prefix_filter}'")
        candidate_files = get_files_with_prefix(adls2, source_container, source_folder, prefix_filter)
        context.log.info(f"📁 Found {len(candidate_files)} files to process: {candidate_files}")
        
        # Step 3: Setup Snowflake environment
        context.log.info(f"❄️ Setting up Snowflake environment: {target_db}.{target_schema}")
        session.use_database(target_db)
        session.use_schema(target_schema)
        
        # Construct fully qualified names
        stage_full_name = f"{target_db}.{target_schema}.{stage_name}"
        format_full_name = f"{target_schema}.{file_format_name}"
        
        # Step 4: Process each file
        for filename in candidate_files:
            context.log.info(f"🔍 Processing file: {filename}")
            
            # Check if metadata exists for this file
            if filename not in file_metadata:
                context.log.error(f"❌ No metadata found for file: {filename}")
                failed_files.append(filename)
                continue
            
            expected_columns = file_metadata[filename]
            context.log.info(f"📋 Expected columns for {filename}: {expected_columns}")
            
            # Extract actual columns from the file
            try:
                actual_columns = extract_csv_column_headers(adls2, source_container, source_folder, filename)
                context.log.info(f"📄 Actual columns in {filename}: {actual_columns}")
            except Exception as e:
                context.log.error(f"❌ Failed to read columns from {filename}: {str(e)}")
                failed_files.append(filename)
                continue
            
            # Validate column match
            if actual_columns != expected_columns:
                context.log.error(
                    f"❌ Column validation failed for {filename}\n"
                    f"   Expected ({len(expected_columns)}): {expected_columns}\n"
                    f"   Actual ({len(actual_columns)}): {actual_columns}"
                )
                failed_files.append(filename)
                continue
            
            # Create Snowflake table and load data
            try:
                table_name = os.path.splitext(filename)[0]
                
                # Create table only if it doesn't exist
                table_created = create_snowflake_table_if_not_exists(session, table_name, target_db, target_schema, expected_columns)
                
                # Load data using Snowflake COPY command
                full_table_name = f"{target_db}.{target_schema}.{table_name}"
                
                # Option 1: Truncate table before loading (replace data)
                session.sql(f"TRUNCATE TABLE {full_table_name}").collect()
                context.log.info(f"🗑️ Truncated existing data in {full_table_name}")
                
                # Option 2: Append data to existing table (comment out truncate above)
                copy_command = f"""
                    COPY INTO {full_table_name}
                    FROM @{stage_full_name}/{source_folder}/{filename}
                    FILE_FORMAT=(FORMAT_NAME='{format_full_name}')
                    ON_ERROR='CONTINUE'
                """
                
                copy_result = session.sql(copy_command).collect()
                
                if table_created:
                    context.log.info(f"✅ Created table and loaded {filename} into {full_table_name}")
                else:
                    context.log.info(f"✅ Loaded {filename} into existing table {full_table_name}")
                    
                validated_files.append(filename)
                
            except Exception as e:
                context.log.error(f"❌ Failed to load {filename} into Snowflake: {str(e)}")
                failed_files.append(filename)
                continue
        
        # Step 5: Final summary
        context.log.info(
            f"🎯 Processing complete: "
            f"{len(validated_files)} files validated and loaded, "
            f"{len(failed_files)} files failed"
        )
        
        if validated_files:
            context.log.info(f"✅ Successfully processed: {validated_files}")
        
        if failed_files:
            context.log.warning(f"⚠️ Failed to process: {failed_files}")
        
        return {
            "passed_files": validated_files,
            "failed_files": failed_files
        }
        
    except Exception as e:
        context.log.error(f"❌ Critical error in validation and loading process: {str(e)}")
        raise