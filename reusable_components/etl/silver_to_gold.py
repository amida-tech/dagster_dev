from typing import Dict, Any, List
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue
from table_config import (
    BUSINESS_COLUMNS, 
    PRIMARY_KEY_COLUMNS, 
    TABLES_TO_PROCESS, 
    METADATA_COLUMNS, 
    DEFAULT_METADATA_VALUES, 
    METADATA_UPDATE_EXPRESSIONS
)

def get_business_columns(table_name: str) -> List[str]:
    return BUSINESS_COLUMNS.get(table_name, [])


def get_primary_key_columns(table_name: str) -> List[str]:
    return PRIMARY_KEY_COLUMNS.get(table_name, [])


def transform_silver_to_gold(
    context: AssetExecutionContext,
    snowpark_session,
    audit_batch_id: int,
    config: Dict[str, Any]
) -> MaterializeResult:
    """Transform silver data to gold using Snowflake MERGE for upsert CDC."""
    silver_db = config.get("silver_db", "ANALYTYXONE_DEV")
    silver_schema = config.get("silver_schema", "SILVER")
    gold_db = config.get("gold_db", "ANALYTYXONE_DEV")
    gold_schema = config.get("gold_schema", "GOLD")
    program_name = config.get("program_name", "NPPES")
    subject_area = config.get("subject_area", "PROVIDER")

    tables = TABLES_TO_PROCESS
    stats: Dict[str, Dict[str, int]] = {}

    try:
        context.log.info(f"Starting Silver to Gold transformation for batch {audit_batch_id}")

        for tbl in tables:
            business_cols = get_business_columns(tbl)
            pk_cols = get_primary_key_columns(tbl)
            if not business_cols or not pk_cols:
                context.log.warning(f"Skipping {tbl}: no column metadata")
                continue

            # Build column lists
            insert_cols = METADATA_COLUMNS + pk_cols + business_cols
            insert_src = [val.format(program_name=program_name, subject_area=subject_area, audit_batch_id=str(audit_batch_id)) for val in DEFAULT_METADATA_VALUES] + [f"s.{c}" for c in pk_cols + business_cols]
            update_set = [f"g.{c} = s.{c}" for c in business_cols] + METADATA_UPDATE_EXPRESSIONS
            # hash_expr = "MD5(CONCAT({}))".format(', '.join([f"COALESCE(s.{c}, '')" for c in business_cols]))
            join_cond = " AND ".join([f"g.{c} = s.{c}" for c in pk_cols])

            merge_sql = f"""
            MERGE INTO {gold_db}.{gold_schema}.{tbl} AS g
            USING (
                SELECT *, MD5(CONCAT({', '.join([f"COALESCE({c}, '')" for c in business_cols])})) AS SRC_HASH
                FROM {silver_db}.{silver_schema}.{tbl}
                WHERE META_BATCH_ID = {audit_batch_id}
            ) AS s
            ON {join_cond}
            WHEN NOT MATCHED THEN
              INSERT ({', '.join(insert_cols)})
              VALUES ({', '.join(insert_src)})
            WHEN MATCHED AND (
              MD5(CONCAT({', '.join([f"COALESCE(g.{c}, '')" for c in business_cols])})) <> s.SRC_HASH
            ) THEN
              UPDATE SET {', '.join(update_set)};
            """

            res = snowpark_session.sql(merge_sql).collect()
            # Collect stats
            silver_count = snowpark_session.sql(
                f"SELECT COUNT(*) FROM {silver_db}.{silver_schema}.{tbl} WHERE META_BATCH_ID = {audit_batch_id}"
            ).collect()[0][0]
            gold_count = snowpark_session.sql(
                f"SELECT COUNT(*) FROM {gold_db}.{gold_schema}.{tbl}"
            ).collect()[0][0]
            stats[tbl] = {"silver": silver_count, "gold": gold_count}
            context.log.info(f"{tbl} processed: {silver_count} silver rows, now {gold_count} in gold")

        return MaterializeResult(
            value={"status": "completed", "batch_id": audit_batch_id, "stats": stats},
            metadata={
                "batch_id": MetadataValue.int(audit_batch_id),
                "tables_processed": MetadataValue.int(len(stats))
            }
        )

    except Exception as e:
        context.log.error(f"Transformation failed: {e}")
        return MaterializeResult(
            value={"status": "failed", "batch_id": audit_batch_id, "error": str(e)},
            metadata={"status": MetadataValue.text("FAILED")}
        )