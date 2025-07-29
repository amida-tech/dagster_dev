import json
from typing import Any, Dict, List, Optional

from dagster import AssetExecutionContext, Failure
from snowflake.snowpark import Session


def dq_rules_procedure(
    context: AssetExecutionContext,
    session: Session,
    rule_group: str,
    audit_batch_id: str,
    rule_id: Optional[str] = None,
    refresh_summary: Optional[str] = None,
) -> Dict[str, Any]:
    """
    1) Logs inputs
    2) Builds & logs the CALL SQL (NULL for missing optional args)
    3) Executes & logs row count
    4) Parses & logs JSON
    5) Applies status_code logic with proper log levels and raises on critical
    6) Fetches the DQ_RUN_LOG rows for this batch and returns everything
    """

    audit_sql = f"""
        INSERT INTO ANALYTYXONE_DEV.DATALOOM.DQ_AUDIT
        VALUES ('{audit_batch_id}', 'DQ', 'DQ', 'IN_PROGRESS', CURRENT_DATE, CURRENT_TIMESTAMP, NULL, NULL)
    """
    try:
        context.log.info(f"[AUDIT] Inserting audit record: {audit_sql.strip()}")
        session.sql(audit_sql).collect()
        context.log.info("[AUDIT] Audit record inserted")
    except Exception as e:
        context.log.error("[AUDIT ERROR] Failed to insert audit record", exc_info=True)
        raise Failure(f"Failed to insert audit record for batch_id={audit_batch_id}: {e}")
    
    # 1) inputs
    context.log.info(
        f"[START] dq_rules_procedure – "
        f"rule_group={rule_group}, rule_id={rule_id}, refresh_summary={refresh_summary}"
    )

    # 2) build CALL SQL
    sql_args = [
        f"'{rule_group}'",
        rule_id and f"'{rule_id}'" or "NULL",
        refresh_summary and f"'{refresh_summary}'" or "NULL",
    ]
    call_sql = (
        "CALL ANALYTYXONE_DEV.DATALOOM.EXECUTE_RULES_AND_LOG_METRICS("
        + ", ".join(sql_args)
        + ");"
    )
    context.log.info(f"[SQL] {call_sql}")

    # 3) execute
    try:
        rows = session.sql(call_sql).collect()
        context.log.info(f"[SQL EXECUTED] returned {len(rows)} row(s)")
    except Exception as e:
        context.log.error("[SQL ERROR] procedure failed", exc_info=True)
        raise Failure(f"Stored‑procedure call failed: {e}")

    if not rows:
        context.log.error("[NO OUTPUT] procedure returned zero rows")
        raise Failure("No output from EXECUTE_RULES_AND_LOG_METRICS")

    raw = rows[0][0]
    context.log.debug(f"[RAW OUTPUT] {raw!r}")

    # 4) parse JSON
    context.log.info("[PARSING] attempting to parse JSON")
    try:
        output = json.loads(raw)
        context.log.info("[PARSE SUCCESS] JSON parsed into dict")
    except Exception as e:
        context.log.error("[PARSE ERROR] invalid JSON", exc_info=True)
        raise Failure(f"Failed to parse JSON output: {e}")

    # 5) status_code logic
    status_code = output.get("status_code")
    status_msg = output.get("status_message")
    context.log.info(f"[STATUS] code={status_code}, message={status_msg!r}")

    if status_code in (2, 3):
        context.log.warning(f"[CRITICAL] {status_msg}")
    elif status_code == 1:
        context.log.warning(f"[WARNING] {status_msg}")
    else:
        context.log.info(f"[SUCCESS] {status_msg}")

    # 6) fetch detailed run log
    batch = output.get("batch_info")
    batch_id = batch.get("batch_id")
    batch_date = batch.get("batch_date")
    if not batch_id or not batch_date:
        context.log.error("[MISSING BATCH INFO]")
        raise Failure("Batch info not returned by procedure")

    select_sql = f"""
        SELECT *
          FROM ANALYTYXONE_DEV.DATALOOM.DQ_RUN_LOG
         WHERE ID_BATCH      = '{batch_id}'
           AND DATE_METRIC   = '{batch_date}'
           AND RULE_GROUP    = '{rule_group}'
           AND CODE_LOG_TYPE = 'RESULT';
    """
    context.log.info(f"[FETCH LOG] {select_sql.strip()}")
    run_log_rows = session.sql(select_sql).collect()
    if not run_log_rows:
        raise Failure("No DQ_RUN_LOG row found")
    
    context.log.info(f"[LOG ROWS] fetched {len(run_log_rows)} rows")

    run_log = run_log_rows[0].as_dict()

    # Pretty‑print the run log
    context.log.info("=== DQ_RUN_LOG ===")
    for k, v in run_log.items():
        context.log.info(f"{k:25} : {v!r}")

    # Severity logic
    if run_log["N_FAILED_SYNTAX_COUNT"] > 0:
        raise Failure(f"Stopped: {run_log['N_FAILED_SYNTAX_COUNT']} syntax error(s)")

    if run_log["N_FAILED_SEVERITY_0"] > 0:
        raise Failure(f"Stopped: {run_log['N_FAILED_SEVERITY_0']} SEV_0 failure(s)")

    if run_log["N_FAILED_SEVERITY_1"] > 0:
        context.log.warning(
            f"Notify on‑call: {run_log['N_FAILED_SEVERITY_1']} SEV_1 failure(s)"
        )

    if run_log["N_FAILED_SEVERITY_2"] > 0:
        context.log.info(
            f"{run_log['N_FAILED_SEVERITY_2']} SEV_2 failure(s) present"
        )

    if run_log["N_FAILED_SEVERITY_3"] > 0:
        context.log.info(
            f"{run_log['N_FAILED_SEVERITY_3']} SEV_3 failure(s) present"
        )


    return {
        "procedure_output": output,
        "dq_run_log": run_log,
    }