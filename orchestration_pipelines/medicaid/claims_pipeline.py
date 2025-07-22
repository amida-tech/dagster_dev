
from typing import List, Dict
from dagster import asset, AssetExecutionContext

# Below asset performs the copy from one ADLS container to another
from reusable_components.etl.copy_adls_to_adls import copy_adls_to_adls
from reusable_components.error_handling.alert import with_alerts  

@asset(
    name="copy_claims_files_adls_to_adls",
    description="Copy all files matching a given prefix from one ADLS folder to another.",
    required_resource_keys={"adls2"},
    group_name="medicaid_claims_pipeline", 
)
@with_alerts() 
def copy_claims_files_adls_to_adls(context: AssetExecutionContext) -> List[str]:

    # Source, destination and Prefix values
    SOURCE_CONTAINER = "dagstersourcedata"
    SOURCE_FOLDER    = "claims_files"
    DEST_CONTAINER   = "dagsterdestinationdata"
    DEST_FOLDER      = "csvfiles"
    PREFIX_FILTER    = "CLAIMS_"

    # Inputs to the reusable component
    file_transfer = copy_adls_to_adls(
        context=context,
        adls2_resource=context.resources.adls2,
        source_container=SOURCE_CONTAINER,
        source_folder=SOURCE_FOLDER,
        dest_container=DEST_CONTAINER,
        dest_folder=DEST_FOLDER,
        prefix_filter=PREFIX_FILTER,
    )

    context.log.info(f"✅ Copied {len(file_transfer)} files from {SOURCE_CONTAINER}/{SOURCE_FOLDER} to {DEST_CONTAINER}/{DEST_FOLDER}: {file_transfer}")
    return file_transfer


# Below is the asset that converts CSV files to parquet format and loads it in the destination location
from reusable_components.etl.csv_to_parquet_adls import convert_csv_to_parquet_adls

@asset(
    name="adls_csv_to_parquet",
    description="Convert CSVs under a folder in one ADLS container to Parquet in another folder",
    required_resource_keys={"adls2"},
    deps=[copy_claims_files_adls_to_adls],
    group_name="medicaid_claims_pipeline", 
)
@with_alerts() 
def adls_csv_to_parquet(context: AssetExecutionContext) -> list[str]:

    context.log.info("▶️ Asset adls_csv_to_parquet starting")

    # Give the source, destination and file prefix format
    SOURCE_CONTAINER = "dagsterdestinationdata"
    SOURCE_FOLDER    = "csvfiles"            
    DEST_CONTAINER   = "dagsterparquetdata"
    DEST_FOLDER      = "parquet"
    PREFIX_FILTER      ="CLAIMS_"             

    context.log.info(
        f"📁 Converting files from {SOURCE_CONTAINER}/{SOURCE_FOLDER} to "
        f"{DEST_CONTAINER}/{DEST_FOLDER} with filter '{PREFIX_FILTER}'"
    )

    # Call the reusable component by giving the values
    file_conversion = convert_csv_to_parquet_adls(
        context=context,
        adls2=context.resources.adls2,
        source_container=SOURCE_CONTAINER,
        dest_container=DEST_CONTAINER,
        source_folder=SOURCE_FOLDER,
        dest_folder=DEST_FOLDER,
        prefix_filter=PREFIX_FILTER,
    )

    context.log.info(f"✅ Asset complete: {len(file_conversion)} files written to '{DEST_CONTAINER}/{DEST_FOLDER}': {file_conversion}")
    return file_conversion

# ADLS to Snowflake CSV
from reusable_components.etl.adls_to_snowflake_csv import copy_adls_csv_to_snowflake

@asset(
    name="load_adls_csv_to_snowflake",
    description="Copy all CSVs from dagsterdestinationdata/csvfiles into ADW_DEV.SANDBOX",
    required_resource_keys={"adls2", "snowpark"},
    deps=[adls_csv_to_parquet], 
    group_name="medicaid_claims_pipeline",
)
@with_alerts() 
def load_adls_csv_to_snowflake(context: AssetExecutionContext) -> List[str]:
    """
    Load CSV files from ADLS to Snowflake tables.
    
    Returns:
        List of fully qualified table names that were loaded
    """
    # Input the Source and Destination
    SOURCE_CONTAINER = "dagsterdestinationdata"
    SOURCE_FOLDER = "csvfiles"
    TARGET_DB = "ADW_DEV"
    TARGET_SCHEMA = "SANDBOX"
    FILE_FORMAT_NAME = "PM_CSV_FORMAT"
    PREFIX_FILTER = "CLAIMS_"
    STAGE_NAME="PM_SA_CSV_STAGE"


    context.log.info(
        f"📁 Loading CSV files from {SOURCE_CONTAINER}/{SOURCE_FOLDER} to "
        f"{TARGET_DB}.{TARGET_SCHEMA} using format '{FILE_FORMAT_NAME}'"
    )

    try:
        # Call the reusable component
        loaded_tables = copy_adls_csv_to_snowflake(
            context=context,  # Pass context for better logging
            session=context.resources.snowpark,
            adls2=context.resources.adls2,
            source_container=SOURCE_CONTAINER,
            source_folder=SOURCE_FOLDER,
            target_db=TARGET_DB,
            target_schema=TARGET_SCHEMA,
            file_format_name=FILE_FORMAT_NAME,
            prefix_filter=PREFIX_FILTER,
            stage_name=STAGE_NAME,
            # Pradeep: I will change it according to our Usecase
            truncate_before_load=True,
        )
        
        context.log.info(
            f"✅ Asset complete: {len(loaded_tables)} tables loaded to "
            f"{TARGET_DB}.{TARGET_SCHEMA}: {loaded_tables}"
        )
        return loaded_tables
    
    except Exception as e:
        context.log.error(f"❌ Failed to load CSV files to Snowflake: {str(e)}")
        raise

# ADLS to Snowflake COPY of PARQUET Files
from reusable_components.etl.adls_to_snowflake_parquet import copy_adls_parquet_to_snowflake

@asset(
    name="load_adls_parquet_to_snowflake",
    description="Copy all Parquet files from dagsterparquetdata/parquet/data into ADW_DEV.SANDBOX",
    required_resource_keys={"adls2", "snowpark"},
    deps=[load_adls_csv_to_snowflake],  # Proper dependency chain
    group_name="medicaid_claims_pipeline",
)
@with_alerts() 
def load_adls_parquet_to_snowflake(context):
    SOURCE_CONTAINER = "dagsterparquetdata"
    SOURCE_FOLDER = "parquet/snowflake"
    TARGET_DB = "ADW_DEV"
    TARGET_SCHEMA = "SANDBOX"
    FILE_FORMAT_NAME = "PM_PARQUET_FORMAT"
    PREFIX_FILTER = "CLAIMS_"
    STAGE_NAME = "PM_SA_PARQUET_STAGE"

    load_copy_info = copy_adls_parquet_to_snowflake(
        context=context,
        session=context.resources.snowpark,
        adls2=context.resources.adls2,
        source_container=SOURCE_CONTAINER,
        source_folder=SOURCE_FOLDER,
        target_db=TARGET_DB,
        target_schema=TARGET_SCHEMA,
        file_format_name=FILE_FORMAT_NAME,
        prefix_filter=PREFIX_FILTER,
        stage_name=STAGE_NAME,
        truncate_before_load=True, # True = replace data, False = append data
    )

    context.log.info(f"✅ Loaded {len(load_copy_info)} Parquet tables: {load_copy_info}")
    return load_copy_info

# Loading ADLS Parquet file to Snowflake ICEBERG Table
from reusable_components.etl.copy_stage_parquet_to_iceberg import copy_stage_parquet_to_iceberg

@asset(
    name="load_all_claims_iceberg",
    description="Copy every CLAIMS_*.parquet from ADLS to Snowflake Iceberg table.",
    required_resource_keys={"snowpark"},
    deps=[load_adls_parquet_to_snowflake],
    group_name="medicaid_claims_pipeline",
)
@with_alerts() 
def load_all_claims_iceberg(context: AssetExecutionContext) -> List[str]:
    
    SOURCE_CONTAINER   = "dagsterdestinationdata"         
    SOURCE_FOLDER      = "parquet/iceberg"               
    PREFIX_FILTER      = "CLAIMS_"                      
    STAGE_NAME         = "PM_SA_PARQUET_STAGE"              
    TARGET_DB          = "ADW_DEV"
    TARGET_SCHEMA      = "SANDBOX"

    # full stage path is <stage>/<SOURCE_FOLDER>
    stage_path = f"{TARGET_DB}.{TARGET_SCHEMA}.{STAGE_NAME}/{SOURCE_FOLDER}"

    context.log.info(
        f"▶️ Listing @{stage_path} for files starting with {PREFIX_FILTER}"
    )

    tables = copy_stage_parquet_to_iceberg(
        context=context,
        session=context.resources.snowpark,
        stage_name=stage_path,
        target_db=TARGET_DB,
        target_schema=TARGET_SCHEMA,
        prefix_filter=PREFIX_FILTER,
    )

    context.log.info(f"✅ Completed COPY INTO for: {tables}")
    return tables

# Checking the columns of a file from the metadata table which is TABLE_METADATA.csv
from reusable_components.etl.validate_and_copy import validate_csv_files_and_load_to_snowflake

@asset(
    name="validate_and_load_claims",
    description="Validate CLAIMS_*.csv files against TABLE_METADATA.csv schema and load valid files into Snowflake",
    required_resource_keys={"adls2", "snowpark"},
    deps=[load_all_claims_iceberg],
    group_name="medicaid_claims_pipeline",
)
@with_alerts() 
def validate_and_load_claims(context: AssetExecutionContext) -> Dict[str, List[str]]:
    """
    Validate CSV files against metadata schema and load valid files to Snowflake.
    
    Returns:
        Dictionary containing lists of passed and failed files
    """
    context.log.info("▶️ Asset validate_and_load_claims starting")
    
    # Metadata file configuration
    METADATA_CONTAINER = "dagsterdestinationdata"
    METADATA_PATH = "validate/TABLE_METADATA.csv"
    
    # Source files configuration
    SOURCE_CONTAINER = "dagsterdestinationdata"
    SOURCE_FOLDER = "validate"
    PREFIX_FILTER = "CLAIMS_"
    
    # Snowflake destination configuration
    TARGET_DB = "ADW_DEV"
    TARGET_SCHEMA = "SANDBOX"
    FILE_FORMAT_NAME = "PM_CSV_FORMAT"
    STAGE_NAME = "PM_SA_CSV_STAGE"
    
    context.log.info(
        f"📁 Validating files from {SOURCE_CONTAINER}/{SOURCE_FOLDER} "
        f"against metadata in {METADATA_CONTAINER}/{METADATA_PATH} "
        f"with filter '{PREFIX_FILTER}'"
    )
    
    try:
        # Call the reusable component
        result = validate_csv_files_and_load_to_snowflake(
            context=context,
            session=context.resources.snowpark,
            adls2=context.resources.adls2,
            metadata_container=METADATA_CONTAINER,
            metadata_path=METADATA_PATH,
            source_container=SOURCE_CONTAINER,
            source_folder=SOURCE_FOLDER,
            prefix_filter=PREFIX_FILTER,
            target_db=TARGET_DB,
            target_schema=TARGET_SCHEMA,
            file_format_name=FILE_FORMAT_NAME,
            stage_name=STAGE_NAME,
        )
        
        context.log.info(
            f"✅ Asset complete: {len(result['passed_files'])} files passed validation and loaded, "
            f"{len(result['failed_files'])} files failed validation: {result['failed_files']}"
        )
        
        if result['failed_files']:
            context.log.warning(
                f"⚠️ {len(result['failed_files'])} files failed validation: {result['failed_files']}"
            )
        
        return result
        
    except Exception as e:
        context.log.error(f"❌ Failed to validate and load claims files: {str(e)}")
        raise