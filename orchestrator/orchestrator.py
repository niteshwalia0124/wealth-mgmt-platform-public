"""
Orchestrator — pulls APPROVED calls from call_queue and executes them.

For each APPROVED entry:
  1. Fetch investor + SIP + distributor from BigQuery
  2. Build script_variables dict
  3. Call voice/call_engine.make_call() directly (no HTTP)
  4. Write call_event to BigQuery
  5. Update call_queue status → IN_PROGRESS (then COMPLETED on outcome)

Designed to run in a single-threaded loop (one call at a time for PoC).
For production, wrap in asyncio with concurrency limit.
"""

import logging
import os
import uuid
from datetime import datetime, timezone

from google.cloud import bigquery

from voice.call_engine import make_call

log = logging.getLogger("orchestrator")

BQ_PROJECT  = os.getenv("GCP_PROJECT",  "your-project")
BQ_DATASET  = os.getenv("BQ_DATASET",   "sbi_mf_poc")
DEMO_MOBILE = os.getenv("DEMO_MOBILE",  "")  # when set, all calls route here (trial Twilio)
_TABLE      = f"{BQ_PROJECT}.{BQ_DATASET}"


def _client() -> bigquery.Client:
    return bigquery.Client(project=BQ_PROJECT)


def run_batch(max_calls: int = 10) -> list[dict]:
    """
    Process up to max_calls APPROVED entries.
    Returns list of call records.
    """
    bq = _client()
    approved = _fetch_approved(bq, limit=max_calls)
    results = []

    for row in approved:
        log.info(
            "Processing queue_id=%s investor=%s trigger=%s",
            row["queue_id"], row["investor_id"], row["trigger_type"],
        )
        _set_in_progress(bq, row["queue_id"])

        try:
            result = _execute_call(bq, row)
        except Exception as exc:
            log.exception("Call failed for queue_id=%s: %s", row["queue_id"], exc)
            result = {
                "queue_id": row["queue_id"],
                "status":   "error",
                "error":    str(exc),
            }

        _write_call_event(bq, row, result)
        results.append(result)

    return results


def _fetch_approved(bq: bigquery.Client, limit: int) -> list[dict]:
    query = f"""
        SELECT
            q.queue_id,
            q.sip_id,
            q.investor_id,
            q.arn_code,
            q.trigger_type,
            q.priority,
            i.full_name         AS investor_name,
            i.mobile            AS investor_mobile,
            i.preferred_language,
            s.fund_name,
            s.monthly_amount_inr,
            s.expiry_date,
            s.status            AS sip_status,
            d.name              AS distributor_name,
            d.arn_code          AS distributor_arn
        FROM `{_TABLE}.call_queue` q
        JOIN `{_TABLE}.investors`    i ON i.investor_id = q.investor_id
        JOIN `{_TABLE}.sip_mandates` s ON s.sip_id      = q.sip_id
        JOIN `{_TABLE}.distributors` d ON d.arn_code    = q.arn_code
        WHERE q.status = 'APPROVED'
        ORDER BY q.priority, q.created_at
        LIMIT @limit
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("limit", "INT64", limit)]
    )
    return [dict(row) for row in bq.query(query, job_config=job_config).result()]


def _execute_call(bq: bigquery.Client, row: dict) -> dict:
    trigger  = row["trigger_type"]
    inv_name = row["investor_name"]
    dist     = row["distributor_name"]
    lang     = row.get("preferred_language") or "hi-IN"
    expiry   = row.get("expiry_date")
    expiry_str = expiry.strftime("%-d %B %Y") if expiry else "jald hi"

    sv_map: dict[str, dict] = {
        "sip_renewal": {
            "fund_name":      row.get("fund_name", ""),
            "monthly_amount": str(int(row.get("monthly_amount_inr") or 0)),
            "expiry_date":    expiry_str,
        },
        "fund_maturity": {
            "fund_name":    row.get("fund_name", ""),
            "maturity_date": expiry_str,
        },
        "sip_debit_failure": {
            "fund_name": row.get("fund_name", ""),
            "month":     datetime.now(timezone.utc).strftime("%B %Y"),
        },
        "sip_paused": {
            "fund_name":   row.get("fund_name", ""),
            "pause_since": expiry_str,
        },
    }

    script_variables = sv_map.get(trigger, {})

    dial_to = DEMO_MOBILE if DEMO_MOBILE else row["investor_mobile"]

    return make_call(
        investor_id=row["investor_id"],
        mobile=dial_to,
        investor_name=inv_name,
        call_type=trigger,
        script_variables=script_variables,
        distributor_name=dist,
        distributor_arn=row["arn_code"],
        language=lang,
        queue_id=row["queue_id"],
    )


def _set_in_progress(bq: bigquery.Client, queue_id: str) -> None:
    bq.query(
        f"""
        UPDATE `{_TABLE}.call_queue`
        SET status = 'IN_PROGRESS', updated_at = CURRENT_TIMESTAMP()
        WHERE queue_id = @qid
        """,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("qid", "STRING", queue_id)]
        ),
    ).result()


def _write_call_event(bq: bigquery.Client, row: dict, result: dict) -> None:
    call_id = result.get("call_id", f"ERR-{uuid.uuid4().hex[:8].upper()}")
    initiated_at = result.get("initiated_at", datetime.utcnow().isoformat())[:19]
    outcome = result.get("outcome") or "NULL"
    outcome_sql = f"'{outcome}'" if outcome != "NULL" else "NULL"
    sid = (result.get("twilio_call_sid") or "").replace("'", "\\'")
    tref = (result.get("transcript") or "").replace("'", "\\'")
    status = (result.get("status") or "error").replace("'", "\\'")

    sql = f"""
        INSERT INTO `{_TABLE}.call_events`
            (call_id, queue_id, investor_id, arn_code, trigger_type,
             twilio_call_sid, status, outcome, initiated_at, transcript_ref)
        VALUES (
            '{call_id}', '{row["queue_id"]}', '{row["investor_id"]}',
            '{row["arn_code"]}', '{row["trigger_type"]}',
            '{sid}', '{status}', {outcome_sql},
            TIMESTAMP '{initiated_at}', '{tref}'
        )
    """
    try:
        bq.query(sql).result()
    except Exception as exc:
        log.error("Failed to write call_event for call_id=%s: %s", call_id, exc)
