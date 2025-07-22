from typing import List
from snowflake.snowpark import Session
from dagster import AssetExecutionContext

def copy_stage_parquet_to_iceberg(
    context: AssetExecutionContext,
    session: Session,
    stage_name: str,
    target_db: str,
    target_schema: str,
    prefix_filter: str,
) -> List[str]:
    """
    1) LIST all files in @stage_name under prefix_filter
    2) COPY each into the matching Iceberg table <TARGET_DB>.<TARGET_SCHEMA>.<basename>_ICEBERG
       using the given file_format_name.
    Returns the list of tables that were loaded.
    """
    # 1) grab everything under prefix_filter
    list_sql = f"LIST @{stage_name}/{prefix_filter};"
    rows = session.sql(list_sql).collect()

    loaded_tables: List[str] = []
    for row in rows:
        full_path = row.name  # e.g. azure://…/dagsterparquetdata/parquet/iceberg/CLAIMS_FOO.parquet
        context.log.info(
        f"▶️ Loading @{full_path} into SNOWFLAKE ICEBERG TABLE"
    )

        # derive table from filename (strip path + .parquet, )
        filename = full_path.rsplit("/", 1)[-1]

        base = filename[:-8]  # remove ".parquet"
        iceberg_table = f"{target_db}.{target_schema}.{base}"

        # 2) COPY INTO that Iceberg table
        session.sql(f"""
            COPY INTO {iceberg_table}
              FROM @{stage_name}
              FILE_FORMAT          = ( TYPE = 'PARQUET' )
              MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE'
              ON_ERROR             = 'CONTINUE'
        """).collect()

        loaded_tables.append(iceberg_table)

    return loaded_tables
