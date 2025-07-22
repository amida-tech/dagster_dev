from dagster import asset, Config, get_dagster_logger    
import os                                                
from pathlib import Path
from reusable_components.error_handling.alert import with_alerts  

class SQLTransferConfig(Config):
    source_table: str      
    target_table: str      
    sql_file_path: str     


@asset(
    name="github_sql_transfer", 
    required_resource_keys={"snowpark"},
    group_name="medicaid_snowflake"
)
@with_alerts()                                                           
def github_sql_transfer(context, config: SQLTransferConfig):
    """
    Simple asset that reads SQL from local file and executes in Snowflake using Snowpark
    Uses repository-relative paths that work in both local and cloud
    """
    logger = get_dagster_logger()          
    session = context.resources.snowpark   
    
    # STEP 1: READ THE SQL FILE
    # Get the directory where Python file is located
    current_file_dir = Path(__file__).parent
    
    # Navigate to repository root
    repo_root = current_file_dir.parent.parent
    
    sql_file_path = repo_root / config.sql_file_path
    
    logger.info(f"Repository root: {repo_root}")
    logger.info(f"Reading SQL from: {sql_file_path}")
    logger.info(f"File exists: {sql_file_path.exists()}")
    
    # Check if file exists
    if not sql_file_path.exists():
        # Try alternative path structure in case the repo structure is different
        alternative_path = repo_root / "medicare" / "claims" / "sql" / "bronze" / "transform_table.sql"
        logger.info(f"Trying alternative path: {alternative_path}")
        
        if alternative_path.exists():
            sql_file_path = alternative_path
        else:
            raise FileNotFoundError(f"SQL file not found at: {sql_file_path} or {alternative_path}")
    
    # Read the entire SQL file
    with open(sql_file_path, 'r') as file:
        sql_code = file.read()
    
    logger.info("Successfully read SQL from repository")
    logger.info(f"SQL content (first 200 chars): {sql_code[:200]}...") 

    
    # STEP 2: REPLACE TABLE NAMES IN THE SQL
    logger.info(f"Replacing placeholders - Source: {config.source_table}, Target: {config.target_table}")
    
    sql_code = sql_code.replace("{{source_table}}", config.source_table)
    sql_code = sql_code.replace("{{target_table}}", config.target_table)

    logger.info(f"Processed SQL (first 200 chars): {sql_code[:200]}...")  
    
    # STEP 3: RUN THE SQL IN SNOWFLAKE
    logger.info("Executing SQL in Snowflake using Snowpark Session")
    try:
        logger.info("Executing SQL using session.sql()...")
        result = session.sql(sql_code).collect()         
        logger.info(f"SQL execution completed successfully")
        logger.info(f"Query executed, result count: {len(result) if result else 0}")
        context.log.info(f"Successfully executed SQL. Processed {len(result) if result else 0} rows.")
        return f"Successfully executed SQL. Processed {len(result) if result else 0} rows."
    except Exception as e:
        logger.error(f"Error with session.sql(): {str(e)}")
        logger.info("Trying to execute SQL statements individually...")
        sql_statements = [stmt.strip() for stmt in sql_code.split(';') if stmt.strip()]
        results = []
        for i, statement in enumerate(sql_statements):
            logger.info(f"Executing statement {i+1}/{len(sql_statements)}: {statement[:100]}...")
            try:
                result = session.sql(statement).collect()
                results.append(result)
                logger.info(f"Statement {i+1} completed successfully")
            except Exception as stmt_error:
                logger.error(f"Error in statement {i+1}: {str(stmt_error)}")
                raise stmt_error  
        total_rows = sum(len(result) if result else 0 for result in results)
        logger.info(f"All SQL statements completed. Total rows processed: {total_rows}")
        context.log.info(f"Successfully executed {len(sql_statements)} SQL statements. Total rows: {total_rows}.")
        return f"Successfully executed {len(sql_statements)} SQL statements. Total rows: {total_rows}."
