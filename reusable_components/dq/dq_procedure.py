import json
from typing import Any, Dict, List, Optional

from dagster import AssetExecutionContext, Failure
from snowflake.snowpark import Session
from reusable_components.error_handling.standardized_alerts import send_pipeline_alert


def dq_rules_procedure(
    context: AssetExecutionContext,
    session: Session,
    rule_group: str,
    audit_batch_id: str,
    rule_id: Optional[str] = None,
    refresh_summary: Optional[str] = None,
    pipeline_name: Optional[str] = None,
    alert_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    1) Logs inputs
    2) Builds & logs the CALL SQL (NULL for missing optional args)
    3) Executes & logs row count
    4) Parses & logs JSON
    5) Applies status_code logic with proper log levels and raises on critical
    6) Fetches the DQ_RUN_LOG rows for this batch and returns everything
    7) Sends Logic Apps alerts based on severity levels:
       - SEV 0: Fail asset + send high severity alert to oncall
       - SEV 1: Continue + send alert to oncall + regular alert
       - SEV 2/3: Continue + send regular alert only
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

    # Enhanced severity logic with Logic Apps alerts
    pipeline_name = pipeline_name or "DQ_PROCEDURE"
    
    # Check syntax errors first
    if run_log["N_FAILED_SYNTAX_COUNT"] > 0:
        syntax_error_msg = f"DQ Procedure failed: {run_log['N_FAILED_SYNTAX_COUNT']} syntax error(s)"
        context.log.error(f"[SYNTAX ERROR] {syntax_error_msg}")
        
        # Send high severity alert to oncall for syntax errors
        send_pipeline_alert(
            context=context,
            pipeline_name=pipeline_name,
            trigger_type="error",
            message=syntax_error_msg,
            error_details=f"Syntax errors detected in DQ procedure. Rule group: {rule_group}, Batch ID: {audit_batch_id}",
            alert_config=alert_config,
            additional_metadata={
                "severity": "HIGH",
                "alert_type": "oncall",
                "rule_group": rule_group,
                "batch_id": audit_batch_id,
                "syntax_errors": run_log["N_FAILED_SYNTAX_COUNT"]
            }
        )
        
        raise Failure(syntax_error_msg)

    # Check SEV 0 failures (asset fails + oncall alert + regular alert)
    if run_log["N_FAILED_SEVERITY_0"] > 0:
        sev0_error_msg = f"DQ Procedure failed: {run_log['N_FAILED_SEVERITY_0']} SEV_0 failure(s)"
        context.log.error(f"[SEV_0 FAILURE] {sev0_error_msg}")
        
        # Send high severity alert to oncall for SEV 0
        send_pipeline_alert(
            context=context,
            pipeline_name=pipeline_name,
            trigger_type="error",
            message=sev0_error_msg,
            error_details=f"Critical SEV_0 failures detected. Rule group: {rule_group}, Batch ID: {audit_batch_id}",
            alert_config=alert_config,
            additional_metadata={
                "severity": "HIGH",
                "alert_type": "oncall",
                "rule_group": rule_group,
                "batch_id": audit_batch_id,
                "sev0_failures": run_log["N_FAILED_SEVERITY_0"]
            }
        )
        
        # Also send regular alert for SEV 0
        send_pipeline_alert(
            context=context,
            pipeline_name=pipeline_name,
            trigger_type="error",
            message=sev0_error_msg,
            error_details=f"Critical SEV_0 failures detected. Rule group: {rule_group}, Batch ID: {audit_batch_id}",
            alert_config=alert_config,
            additional_metadata={
                "severity": "HIGH",
                "alert_type": "regular",
                "rule_group": rule_group,
                "batch_id": audit_batch_id,
                "sev0_failures": run_log["N_FAILED_SEVERITY_0"]
            }
        )
        
        raise Failure(sev0_error_msg)

    # Check SEV 1 failures (asset continues + oncall alert + regular alert)
    if run_log["N_FAILED_SEVERITY_1"] > 0:
        sev1_warning_msg = f"DQ Procedure warning: {run_log['N_FAILED_SEVERITY_1']} SEV_1 failure(s)"
        context.log.warning(f"[SEV_1 WARNING] {sev1_warning_msg}")
        
        # Send alert to oncall for SEV 1
        send_pipeline_alert(
            context=context,
            pipeline_name=pipeline_name,
            trigger_type="error",
            message=sev1_warning_msg,
            error_details=f"SEV_1 failures detected but pipeline continues. Rule group: {rule_group}, Batch ID: {audit_batch_id}",
            alert_config=alert_config,
            additional_metadata={
                "severity": "MEDIUM",
                "alert_type": "oncall",
                "rule_group": rule_group,
                "batch_id": audit_batch_id,
                "sev1_failures": run_log["N_FAILED_SEVERITY_1"]
            }
        )
        
        # Also send regular alert for SEV 1
        send_pipeline_alert(
            context=context,
            pipeline_name=pipeline_name,
            trigger_type="log",
            message=sev1_warning_msg,
            error_details=f"SEV_1 failures detected. Rule group: {rule_group}, Batch ID: {audit_batch_id}",
            alert_config=alert_config,
            additional_metadata={
                "severity": "MEDIUM",
                "alert_type": "regular",
                "rule_group": rule_group,
                "batch_id": audit_batch_id,
                "sev1_failures": run_log["N_FAILED_SEVERITY_1"]
            }
        )

    # Check SEV 2 failures (asset continues + regular alert only)
    if run_log["N_FAILED_SEVERITY_2"] > 0:
        sev2_info_msg = f"DQ Procedure info: {run_log['N_FAILED_SEVERITY_2']} SEV_2 failure(s)"
        context.log.info(f"[SEV_2 INFO] {sev2_info_msg}")
        
        # Send regular alert for SEV 2
        send_pipeline_alert(
            context=context,
            pipeline_name=pipeline_name,
            trigger_type="log",
            message=sev2_info_msg,
            error_details=f"SEV_2 failures detected. Rule group: {rule_group}, Batch ID: {audit_batch_id}",
            alert_config=alert_config,
            additional_metadata={
                "severity": "LOW",
                "alert_type": "regular",
                "rule_group": rule_group,
                "batch_id": audit_batch_id,
                "sev2_failures": run_log["N_FAILED_SEVERITY_2"]
            }
        )

    # Check SEV 3 failures (asset continues + regular alert only)
    if run_log["N_FAILED_SEVERITY_3"] > 0:
        sev3_info_msg = f"DQ Procedure info: {run_log['N_FAILED_SEVERITY_3']} SEV_3 failure(s)"
        context.log.info(f"[SEV_3 INFO] {sev3_info_msg}")
        
        # Send regular alert for SEV 3
        send_pipeline_alert(
            context=context,
            pipeline_name=pipeline_name,
            trigger_type="log",
            message=sev3_info_msg,
            error_details=f"SEV_3 failures detected. Rule group: {rule_group}, Batch ID: {audit_batch_id}",
            alert_config=alert_config,
            additional_metadata={
                "severity": "LOW",
                "alert_type": "regular",
                "rule_group": rule_group,
                "batch_id": audit_batch_id,
                "sev3_failures": run_log["N_FAILED_SEVERITY_3"]
            }
        )

    # Send success alert if no critical failures
    if (run_log["N_FAILED_SEVERITY_0"] == 0 and 
        run_log["N_FAILED_SYNTAX_COUNT"] == 0):
        
        success_msg = f"DQ Procedure completed successfully. Rule group: {rule_group}"
        context.log.info(f"[SUCCESS] {success_msg}")
        
        # Send success alert
        send_pipeline_alert(
            context=context,
            pipeline_name=pipeline_name,
            trigger_type="info",
            message=success_msg,
            alert_config=alert_config,
            additional_metadata={
                "severity": "INFO",
                "alert_type": "regular",
                "rule_group": rule_group,
                "batch_id": audit_batch_id,
                "total_failures": (
                    run_log["N_FAILED_SEVERITY_1"] + 
                    run_log["N_FAILED_SEVERITY_2"] + 
                    run_log["N_FAILED_SEVERITY_3"]
                )
            }
        )

    return {
        "procedure_output": output,
        "dq_run_log": run_log,
    }