import os
from dagster import asset
from snowflake.snowpark import Session
from quickstart_etl.assets.convert_csv_to_parquet import convert_csv_to_parquet

@asset(deps=[convert_csv_to_parquet])
def load_all_recipient_data():
    session = Session.builder.configs({
        "account": "vba67968.east-us-2.azure",
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
         "role": "ACCOUNTADMIN",
        "warehouse": "COMPUTE_WH",
        "database": "ADW_DEV",
        "schema": "RAW"
    }).create()

    tables = [
        "b_addr_tb", "b_auth_rep_tb", "b_detail_tb", "b_elig_spn_tb",
        "b_enrl_tb", "b_lang_tb", "b_race_tb", "b_xref_tb"
    ]

    for table in tables:
        sql = f"""
        COPY INTO RAW.{table.upper()} FROM (
            SELECT
                'RECIPIENT_DAGSTER' AS ETL_DATA_SOURCE,
                1 AS ETL_BATCH_ID,
                CURRENT_TIMESTAMP AS DATE_INSERT_ROW,
                *
            FROM @extstage_recipient_dagster/{table}.parquet (FILE_FORMAT => parquet_format)
        )
        FILE_FORMAT = parquet_format
        LOAD_MODE = FULL_INGEST;
        """
        try:
            session.sql(sql).collect()
            print(f"✅ Loaded: {table.upper()}")
        except Exception as e:
            print(f"❌ Failed to load {table.upper()}: {e}")

    session.close()

