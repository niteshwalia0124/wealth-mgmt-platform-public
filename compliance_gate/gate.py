"""
Compliance Gate — checks every PENDING call before it goes APPROVED.

Checks (all must pass):
  1. Distributor calling_window (e.g. 09:00–18:00 IST)
  2. Investor consent (consent_given = TRUE in BQ)
  3. DND blocklist (mocked in PoC — real TRAI NDNC in production)
  4. Distributor opt-in for this trigger_type (distributor_settings table)
  5. Dedup — no completed call for this (investor_id, trigger_type) in past 24h

Any failed check → status = BLOCKED with a block_reason.
Passed → status = APPROVED.

Run after trigger_engine.run() or call gate() per queue_id.
"""

import logging
import os
from datetime import datetime, timezone, timedelta

from google.cloud import bigquery

log = logging.getLogger("compliance_gate")

BQ_PROJECT = os.getenv("GCP_PROJECT", "your-project")
BQ_DATASET = os.getenv("BQ_DATASET",  "sbi_mf_poc")
_TABLE     = f"{BQ_PROJECT}.{BQ_DATASET}"

# PoC DND blocklist — in production query TRAI NDNC API
_DND_BLOCKLIST: set[str] = set()


def _client() -> bigquery.Client:
    return bigquery.Client(project=BQ_PROJECT)


def run_all() -> dict:
    """Process all PENDING entries in call_queue."""
    bq = _client()
    pending = _fetch_pending(bq)
    counts = {"approved": 0, "blocked": 0}

    for row in pending:
        block_reason = _check(bq, row)
        status = "BLOCKED" if block_reason else "APPROVED"
        _update_status(bq, row["queue_id"], status, block_reason or "")
        if block_reason:
            counts["blocked"] += 1
            log.info("BLOCKED queue_id=%s reason=%s", row["queue_id"], block_reason)
        else:
            counts["approved"] += 1
            log.info("APPROVED queue_id=%s investor=%s", row["queue_id"], row["investor_id"])

    return counts


def _fetch_pending(bq: bigquery.Client) -> list[dict]:
    query = f"""
        SELECT
            q.queue_id,
            q.sip_id,
            q.investor_id,
            q.arn_code,
            q.trigger_type,
            i.mobile,
            i.consent_given,
            i.preferred_language,
            ds.calling_window_start,
            ds.calling_window_end,
            ds.sip_expiry_calls_enabled,
            ds.fund_maturity_calls_enabled,
            ds.debit_failure_calls_enabled,
            ds.sip_paused_calls_enabled
        FROM `{_TABLE}.call_queue` q
        JOIN `{_TABLE}.investors` i ON i.investor_id = q.investor_id
        LEFT JOIN `{_TABLE}.distributor_settings` ds ON ds.arn_code = q.arn_code
        WHERE q.status = 'PENDING'
        ORDER BY
            CASE q.priority WHEN 'P0' THEN 0 WHEN 'P1' THEN 1 ELSE 2 END,
            q.created_at
    """
    return [dict(row) for row in bq.query(query).result()]


def _check(bq: bigquery.Client, row: dict) -> str:
    """Return block_reason string or empty string if approved."""

    # 1. Consent
    if not row.get("consent_given"):
        return "investor_no_consent"

    # 2. DND blocklist
    mobile = row.get("mobile", "")
    if mobile in _DND_BLOCKLIST:
        return "dnd_registered"

    # 3. Calling window (IST = UTC+5:30)
    ist_now = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
    window_start = row.get("calling_window_start") or "09:00"
    window_end   = row.get("calling_window_end")   or "18:00"
    now_str = ist_now.strftime("%H:%M")
    if not (window_start <= now_str <= window_end):
        return f"outside_calling_window_{window_start}_{window_end}"

    # 4. Distributor opt-in for this trigger type
    trigger = row.get("trigger_type", "")
    opt_in_map = {
        "sip_renewal":       row.get("sip_expiry_calls_enabled",    True),
        "sip_debit_failure": row.get("debit_failure_calls_enabled", True),
        "sip_paused":        row.get("sip_paused_calls_enabled",    True),
        "fund_maturity":     row.get("fund_maturity_calls_enabled", True),
    }
    if not opt_in_map.get(trigger, True):
        return f"distributor_opted_out_{trigger}"

    # 5. Dedup — completed call within last 24h for same investor+trigger
    if _recent_completed(bq, row["investor_id"], trigger):
        return "already_called_in_24h"

    return ""


def _recent_completed(bq: bigquery.Client, investor_id: str, trigger_type: str) -> bool:
    query = f"""
        SELECT COUNT(1) AS cnt
        FROM `{_TABLE}.call_events` ce
        JOIN `{_TABLE}.call_queue` q ON q.queue_id = ce.queue_id
        WHERE q.investor_id = @investor_id
          AND q.trigger_type = @trigger_type
          AND ce.completed_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
          AND ce.outcome NOT IN ('no_answer', 'no_meaningful_conversation')
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("investor_id",  "STRING", investor_id),
            bigquery.ScalarQueryParameter("trigger_type", "STRING", trigger_type),
        ]
    )
    row = next(bq.query(query, job_config=job_config).result())
    return row.cnt > 0


def _update_status(bq: bigquery.Client, queue_id: str, status: str, block_reason: str) -> None:
    query = f"""
        UPDATE `{_TABLE}.call_queue`
        SET status = @status,
            block_reason = @block_reason,
            updated_at = CURRENT_TIMESTAMP()
        WHERE queue_id = @queue_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("status",       "STRING", status),
            bigquery.ScalarQueryParameter("block_reason", "STRING", block_reason),
            bigquery.ScalarQueryParameter("queue_id",     "STRING", queue_id),
        ]
    )
    bq.query(query, job_config=job_config).result()


def add_to_dnd(mobile: str) -> None:
    """Runtime DND addition (PoC only — no BigQuery write)."""
    _DND_BLOCKLIST.add(mobile)


if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO)
    result = run_all()
    print(json.dumps(result, indent=2))
