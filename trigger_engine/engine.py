"""
Trigger Engine — scans BigQuery for actionable SIP events and writes call_queue.

Runs daily as a cron job (or called directly from run_pipeline.py).

Priority:
  P0: SIP expires in ≤ 7 days   OR debit_failed
  P1: SIP expires in 8–21 days  OR paused
  P2: SIP expires in 22–45 days OR fund maturing in ≤ 30 days

Trigger types:
  sip_renewal         — active SIP approaching expiry
  sip_debit_failure   — NACH debit failed
  sip_paused          — SIP paused, check if investor wants to resume
  fund_maturity       — fixed-term fund maturing soon

Idempotent: a (sip_id, trigger_type) pair already PENDING/IN_PROGRESS is skipped.
"""

import logging
import os
import uuid
from datetime import datetime, timezone

from google.cloud import bigquery

log = logging.getLogger("trigger_engine")

BQ_PROJECT = os.getenv("GCP_PROJECT", "your-project")
BQ_DATASET = os.getenv("BQ_DATASET",  "sbi_mf_poc")
_TABLE     = f"{BQ_PROJECT}.{BQ_DATASET}"


def _client() -> bigquery.Client:
    return bigquery.Client(project=BQ_PROJECT)


def run(dry_run: bool = False) -> dict:
    """
    Scan BigQuery and enqueue all actionable calls.
    Returns counts per trigger type.
    """
    bq = _client()
    today = datetime.now(timezone.utc).date()
    counts: dict[str, int] = {
        "sip_renewal":       0,
        "sip_debit_failure": 0,
        "sip_paused":        0,
        "fund_maturity":     0,
        "skipped":           0,
    }

    existing = _existing_pending(bq)

    rows = _fetch_triggers(bq)
    inserts = []
    for row in rows:
        key = (row["sip_id"], row["trigger_type"])
        if key in existing:
            counts["skipped"] += 1
            continue

        days_to_expiry = (row.get("expiry_date", today) - today).days if row.get("expiry_date") else 999

        priority = _priority(row["trigger_type"], days_to_expiry)

        inserts.append({
            "queue_id":     str(uuid.uuid4()),
            "sip_id":       row["sip_id"],
            "folio_no":     row.get("folio_no", ""),
            "investor_id":  row["investor_id"],
            "arn_code":     row["arn_code"],
            "trigger_type": row["trigger_type"],
            "priority":     priority,
            "status":       "PENDING",
            "created_at":   datetime.now(timezone.utc).isoformat(),
        })
        counts[row["trigger_type"]] += 1

    if inserts and not dry_run:
        _dml_insert(bq, inserts)
        log.info("Inserted %d rows into call_queue", len(inserts))
    elif inserts and dry_run:
        log.info("DRY RUN — would insert %d rows", len(inserts))

    return counts


def _dml_insert(bq: bigquery.Client, rows: list[dict]):
    """Use DML INSERT so rows are immediately available for UPDATE (avoids streaming buffer lock)."""
    values = []
    for r in rows:
        folio = r.get("folio_no") or ""
        values.append(
            f"('{r['queue_id']}', '{r['sip_id']}', '{folio}', '{r['investor_id']}', "
            f"'{r['arn_code']}', '{r['trigger_type']}', '{r['priority']}', "
            f"'PENDING', TIMESTAMP '{r['created_at'][:19]}')"
        )
    sql = f"""
        INSERT INTO `{_TABLE}.call_queue`
            (queue_id, sip_id, folio_no, investor_id, arn_code, trigger_type, priority, status, created_at)
        VALUES
        {', '.join(values)}
    """
    bq.query(sql).result()


def _existing_pending(bq: bigquery.Client) -> set:
    query = f"""
        SELECT sip_id, trigger_type
        FROM `{_TABLE}.call_queue`
        WHERE status IN ('PENDING', 'APPROVED', 'IN_PROGRESS')
    """
    return {(row.sip_id, row.trigger_type) for row in bq.query(query).result()}


def _fetch_triggers(bq: bigquery.Client) -> list[dict]:
    query = f"""
    -- SIP renewal (expires within 45 days)
    SELECT
        s.sip_id,
        s.folio_no,
        s.arn_code,
        i.investor_id,
        'sip_renewal' AS trigger_type,
        s.expiry_date
    FROM `{_TABLE}.sip_mandates` s
    JOIN `{_TABLE}.investors` i USING (folio_no)
    WHERE s.status = 'active'
      AND s.expiry_date BETWEEN CURRENT_DATE() AND DATE_ADD(CURRENT_DATE(), INTERVAL 45 DAY)

    UNION ALL

    -- NACH debit failure
    SELECT
        s.sip_id,
        s.folio_no,
        s.arn_code,
        i.investor_id,
        'sip_debit_failure' AS trigger_type,
        NULL AS expiry_date
    FROM `{_TABLE}.sip_mandates` s
    JOIN `{_TABLE}.investors` i USING (folio_no)
    WHERE s.status = 'debit_failed'

    UNION ALL

    -- SIP paused
    SELECT
        s.sip_id,
        s.folio_no,
        s.arn_code,
        i.investor_id,
        'sip_paused' AS trigger_type,
        NULL AS expiry_date
    FROM `{_TABLE}.sip_mandates` s
    JOIN `{_TABLE}.investors` i USING (folio_no)
    WHERE s.status = 'paused'
    """
    return [dict(row) for row in bq.query(query).result()]


def _priority(trigger_type: str, days_to_expiry: int) -> str:
    if trigger_type in ("sip_debit_failure",):
        return "P0"
    if trigger_type == "sip_renewal":
        if days_to_expiry <= 7:
            return "P0"
        if days_to_expiry <= 21:
            return "P1"
        return "P2"
    if trigger_type == "sip_paused":
        return "P1"
    return "P2"


if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO)
    result = run(dry_run=False)
    print(json.dumps(result, indent=2))
