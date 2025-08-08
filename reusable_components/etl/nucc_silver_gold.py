from typing import Dict, Any
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue

def transform_silver_to_gold_nucc(
    context: AssetExecutionContext,
    snowpark_session,
    audit_batch_id: int,
    config: Dict[str, Any]
) -> MaterializeResult:
    silver_db = config.get("silver_db", "ANALYTYXONE_DEV")
    silver_schema = config.get("silver_schema", "SILVER")
    gold_db = config.get("gold_db", "ANALYTYXONE_DEV")
    gold_schema = config.get("gold_schema", "GOLD")
    silver_table = config.get("silver_table", "NUCC_TAXONOMY")
    gold_table = config.get("gold_table", "NUCC_TAXONOMY")
    program_name = config.get("program_name", "NUCC")
    subject_area = config.get("subject_area", "NUCC")
  
    try:
        context.log.info(f"Starting Silver to Gold transformation for NUCC batch {audit_batch_id}")
        silver_count = snowpark_session.sql(
            f"SELECT COUNT(*) FROM {silver_db}.{silver_schema}.{silver_table} WHERE META_BATCH_ID = '{audit_batch_id}'"
        ).collect()[0][0]
        
        if silver_count == 0:
            context.log.warning(f"No records found in silver for batch_id {audit_batch_id}")
            return MaterializeResult(
                value={"status": "no_data", "batch_id": audit_batch_id},
                metadata={"status": MetadataValue.text("NO_DATA")}
            )
        merge_sql = f"""
        MERGE INTO {gold_db}.{gold_schema}.{gold_table} AS g
        USING (
            SELECT *,
                   MD5(CONCAT(
                       COALESCE(GROUPING, ''), '|',
                       COALESCE(CLASSIFICATION, ''), '|',
                       COALESCE(SPECIALIZATION, ''), '|',
                       COALESCE(DEFINITION, ''), '|',
                       COALESCE(NOTES, ''), '|',
                       COALESCE(DISPLAY_NAME, ''), '|',
                       COALESCE(SECTION, '')
                   )) AS SRC_HASH
            FROM {silver_db}.{silver_schema}.{silver_table}
            WHERE META_BATCH_ID = {audit_batch_id}
        ) AS s
        ON g.CODE = s.CODE
        WHEN NOT MATCHED THEN
          INSERT (
              META_PROGRAM_NAME, META_SUBJECT_AREA, META_BATCH_ID, 
              META_DATE_INSERT, META_DATE_UPDATE, META_UPD_BATCH_ID,
              CODE, GROUPING, CLASSIFICATION, SPECIALIZATION, 
              DEFINITION, NOTES, DISPLAY_NAME, SECTION
          )
          VALUES (
              '{program_name}', '{subject_area}', {audit_batch_id},
              CURRENT_TIMESTAMP(), NULL, NULL,
              s.CODE, s.GROUPING, s.CLASSIFICATION, s.SPECIALIZATION,
              s.DEFINITION, s.NOTES, s.DISPLAY_NAME, s.SECTION
          )
        WHEN MATCHED AND (
          MD5(CONCAT(
              COALESCE(g.GROUPING, ''), '|',
              COALESCE(g.CLASSIFICATION, ''), '|',
              COALESCE(g.SPECIALIZATION, ''), '|',
              COALESCE(g.DEFINITION, ''), '|',
              COALESCE(g.NOTES, ''), '|',
              COALESCE(g.DISPLAY_NAME, ''), '|',
              COALESCE(g.SECTION, '')
          )) <> s.SRC_HASH
        ) THEN
          UPDATE SET 
              g.GROUPING = s.GROUPING,
              g.CLASSIFICATION = s.CLASSIFICATION,
              g.SPECIALIZATION = s.SPECIALIZATION,
              g.DEFINITION = s.DEFINITION,
              g.NOTES = s.NOTES,
              g.DISPLAY_NAME = s.DISPLAY_NAME,
              g.SECTION = s.SECTION,
              g.META_DATE_UPDATE = CURRENT_TIMESTAMP(),
              g.META_UPD_BATCH_ID = s.META_BATCH_ID;
        """
        snowpark_session.sql(merge_sql).collect()
        gold_count = snowpark_session.sql(
            f"SELECT COUNT(*) FROM {gold_db}.{gold_schema}.{gold_table}"
        ).collect()[0][0]
        gold_batch_count = snowpark_session.sql(
            f"SELECT COUNT(*) FROM {gold_db}.{gold_schema}.{gold_table} WHERE META_BATCH_ID = {audit_batch_id}"
        ).collect()[0][0]
        
        context.log.info(f"Transformation completed: {silver_count} silver rows processed, {gold_batch_count} records for this batch, {gold_count} total in gold")
        
        return MaterializeResult(
            value={
                "status": "completed",
                "batch_id": audit_batch_id,
                "table_transformed": gold_table,
                "silver_count": silver_count,
                "gold_total_count": gold_count,
                "gold_batch_count": gold_batch_count
            },
            metadata={
                "status": MetadataValue.text("SUCCESS"),
                "batch_id": MetadataValue.int(audit_batch_id),
                "silver_records": MetadataValue.int(silver_count),
                "gold_total_records": MetadataValue.int(gold_count),
                "gold_batch_records": MetadataValue.int(gold_batch_count)
            }
        )
        
    except Exception as e:
        context.log.error(f"Silver to Gold transformation failed for batch_id {audit_batch_id}: {str(e)}")
        
        return MaterializeResult(
            value={"status": "failed", "batch_id": audit_batch_id, "error": str(e)},
            metadata={"status": MetadataValue.text("FAILED")}
        )