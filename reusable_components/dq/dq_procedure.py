import json
from typing import Any, Dict, Optional
from dagster import AssetExecutionContext

from snowflake.snowpark import Session
from dagster import Failure


def dq_rules_procedure(
    context: AssetExecutionContext,
    session: Session,
    rule_group: str,
    rule_id: Optional[str] = None,
    refresh_summary: Optional[str] = None,
) -> Dict[str, Any]:
    """
    1) Logs inputs
    2) Builds & logs the CALL SQL
    3) Executes & logs row count
    4) Parses & logs JSON
    5) Applies your status_code logic with logs at every branch
    6) Raises Failure() on critical codes
    """
    # 1) log inputs
    context.log.info(
        f"[START] dq_rules_procedure – rule_group={rule_group}, "
        f"rule_id={rule_id}, refresh_summary={refresh_summary}"
    )

    # 2) build SQL
    args = [rule_group]
    if rule_id is not None:
        args.append(rule_id)
    if refresh_summary is not None:
        args.append(refresh_summary)

    context.log.info(f"[SQL BUILD] args list → {args}")
    sql_args = ", ".join(f"'{arg}'" for arg in args)
    sql = (
        "CALL ANALYTYXONE_DEV.DATALOOM.EXECUTE_RULES_AND_LOG_METRICS("
        f"{sql_args}"
        ")"
    )
    context.log.info(f"[SQL] {sql}")

    # 3) execute
    try:
        rows = session.sql(sql).collect()
        context.log.info(f"[SQL EXECUTED] returned {len(rows)} row(s)")
    except Exception as e:
        context.log.error("[SQL ERROR] failed to execute procedure", exc_info=True)
        raise Failure(f"Stored‑procedure call failed: {e}")

    if not rows:
        context.log.error("[NO OUTPUT] procedure returned zero rows")
        raise Failure("No output from EXECUTE_RULES_AND_LOG_METRICS")

    raw = rows[0][0]
    context.log.debug(f"[RAW OUTPUT] {raw}")

    # 4) parse JSON
    context.log.info("[PARSING] attempting to parse JSON")
    try:
        output = json.loads(raw)
        context.log.info("[PARSE SUCCESS] JSON parsed into dict")
    except Exception as e:
        context.log.error("[PARSE ERROR] invalid JSON", exc_info=True)
        raise Failure(f"Failed to parse JSON output: {e}")

    context.log.debug(f"[PARSED OUTPUT] {output}")

    # 5) status_code logic
    statuscode = output.get("status_code")
    summary = output.get("status_message", f"Unknown status {statuscode}")
    syntax_errors = output.get("execution_summary", {}).get("syntax_errors")

    context.log.info(f"[STATUS CHECK] status_code={statuscode}, syntax_errors={syntax_errors}")

    # syntax‑error
    if syntax_errors > 0:
        msg = "CRITICAL: Syntax errors detected in rules"
        context.log.error(f"[CRITICAL] {msg}")
        raise Failure(msg)

    # Critical Status codes
    if statuscode in (2, 3):
        context.log.error(f"[CRITICAL] status_code={statuscode}: {summary}")
        raise Failure(summary)

    # warning
    if statuscode == 1:
        context.log.warning(f"[WARNING] {summary}")
    # success
    elif statuscode == 0:
        context.log.info(f"[SUCCESS] {summary}")
    else:
        context.log.info(f"[UNKNOWN CODE] {summary}")

    return output