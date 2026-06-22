"""
Outcome Processor — reads transcripts from GCS, classifies outcome with Gemini Flash,
writes result to call_events in BigQuery.

Runs after calls complete (poll GCS transcript bucket for new .txt files).

Outcome classes:
  renewal_intent             — investor said yes / agreed to renew
  callback_requested         — asked Priya to arrange a callback
  not_interested             — explicitly said no, not now
  wrong_number               — wrong person answered
  no_answer                  — call rang, nobody picked up
  query_raised               — had questions/concerns, needs distributor follow-up
  no_meaningful_conversation — picked up but didn't engage (busy, dropped early)
"""

import json
import logging
import os
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()

from google.cloud import bigquery, storage

log = logging.getLogger("outcome_processor")

BQ_PROJECT        = os.getenv("GCP_PROJECT",       "your-project")
BQ_DATASET        = os.getenv("BQ_DATASET",        "sbi_mf_poc")
TRANSCRIPT_BUCKET = os.getenv("TRANSCRIPT_BUCKET", "sbi-mf-call-transcripts")
GOOGLE_API_KEY    = os.getenv("GOOGLE_API_KEY",    "")
_TABLE            = f"{BQ_PROJECT}.{BQ_DATASET}"

VALID_OUTCOMES = {
    "renewal_intent",
    "callback_requested",
    "not_interested",
    "wrong_number",
    "no_answer",
    "query_raised",
    "no_meaningful_conversation",
}

_CLASSIFY_PROMPT = """You are an outcome classifier for SBI Mutual Fund investor calls.

Classify the call transcript below into EXACTLY ONE of these outcomes:
- renewal_intent: investor agreed to renew or expressed clear positive intent
- callback_requested: investor asked for someone to call back, or asked to talk to the advisor
- not_interested: investor clearly declined ("no", "nahi chahiye", etc.)
- wrong_number: the person who answered is not the intended investor
- no_answer: transcript is empty or contains only ringing/voicemail
- query_raised: investor had questions or concerns that need distributor follow-up
- no_meaningful_conversation: call connected but investor hung up early or didn't engage

TRANSCRIPT:
{transcript}

Respond ONLY with a JSON object: {{"outcome": "<one of the above>", "confidence": 0.0-1.0, "summary": "<one sentence>"}}
"""


def process_all() -> dict:
    """
    Scan GCS transcript bucket for unprocessed transcripts and classify them.
    Returns counts per outcome class.
    """
    gcs = storage.Client()
    bq  = bigquery.Client(project=BQ_PROJECT)

    processed = _already_classified(bq)
    counts: dict[str, int] = {}

    bucket = gcs.bucket(TRANSCRIPT_BUCKET)
    for blob in bucket.list_blobs():
        if not blob.name.endswith(".json"):
            continue

        call_id = blob.name.split("/")[-1].replace(".json", "")
        if call_id in processed:
            continue

        raw = blob.download_as_text()
        if not raw.strip():
            log.warning("Empty transcript for call_id=%s", call_id)
            continue

        # Broker saves JSON: list of {speaker, text, ts} — flatten to readable text
        try:
            segments = json.loads(raw)
            transcript_text = "\n".join(
                f"{s.get('speaker','?').upper()}: {s.get('text','')}"
                for s in segments if s.get("text")
            )
        except (json.JSONDecodeError, TypeError):
            transcript_text = raw  # fallback: treat as plain text

        if not transcript_text.strip():
            log.warning("No speech content in transcript for call_id=%s", call_id)
            continue

        outcome_data = _classify(transcript_text)
        outcome = outcome_data.get("outcome", "no_meaningful_conversation")
        if outcome not in VALID_OUTCOMES:
            outcome = "no_meaningful_conversation"

        _write_outcome(bq, call_id, outcome, transcript_text, outcome_data.get("summary", ""))
        counts[outcome] = counts.get(outcome, 0) + 1
        log.info(
            "Classified call_id=%s → %s (confidence=%.2f)",
            call_id, outcome, outcome_data.get("confidence", 0),
        )

    return counts


def classify_single(call_id: str, transcript: str) -> dict:
    """Classify a single transcript and write outcome to BigQuery."""
    bq = bigquery.Client(project=BQ_PROJECT)
    outcome_data = _classify(transcript)
    outcome = outcome_data.get("outcome", "no_meaningful_conversation")
    if outcome not in VALID_OUTCOMES:
        outcome = "no_meaningful_conversation"
    _write_outcome(bq, call_id, outcome, transcript, outcome_data.get("summary", ""))
    return outcome_data


def _classify(transcript: str) -> dict:
    """Call Gemini Flash to classify the transcript."""
    try:
        from google import genai
        if GOOGLE_API_KEY:
            client = genai.Client(api_key=GOOGLE_API_KEY)
        else:
            client = genai.Client(
                vertexai=True,
                project=BQ_PROJECT,
                location="global",
            )
        response = client.models.generate_content(
            model=os.getenv("GEMINI_MODEL", "gemini-3.1-pro-preview"),
            contents=_CLASSIFY_PROMPT.format(transcript=transcript[:4000]),
        )
        text = response.text.strip()
        # Strip markdown code fences if present
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        return json.loads(text)
    except Exception as exc:
        log.error("Gemini classification failed: %s", exc)
        return {"outcome": "no_meaningful_conversation", "confidence": 0.0, "summary": str(exc)}


def _already_classified(bq: bigquery.Client) -> set[str]:
    query = f"""
        SELECT call_id
        FROM `{_TABLE}.call_events`
        WHERE outcome IS NOT NULL
    """
    return {row.call_id for row in bq.query(query).result()}


def _write_outcome(
    bq: bigquery.Client,
    call_id: str,
    outcome: str,
    transcript: str,
    summary: str,
) -> None:
    query = f"""
        UPDATE `{_TABLE}.call_events`
        SET
            outcome       = @outcome,
            transcript_ref = @transcript_ref,
            notes         = @notes,
            completed_at  = COALESCE(completed_at, CURRENT_TIMESTAMP())
        WHERE call_id = @call_id
    """
    bq.query(
        query,
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("call_id",       "STRING", call_id),
            bigquery.ScalarQueryParameter("outcome",       "STRING", outcome),
            bigquery.ScalarQueryParameter("transcript_ref","STRING", f"gs://{TRANSCRIPT_BUCKET}/transcripts/{call_id}.txt"),
            bigquery.ScalarQueryParameter("notes",         "STRING", summary),
        ]),
    ).result()

    # Also update call_queue to COMPLETED
    bq.query(
        f"""
        UPDATE `{_TABLE}.call_queue`
        SET status = 'COMPLETED', updated_at = CURRENT_TIMESTAMP()
        WHERE queue_id IN (
            SELECT queue_id FROM `{_TABLE}.call_events` WHERE call_id = @call_id
        )
        """,
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("call_id", "STRING", call_id),
        ]),
    ).result()


if __name__ == "__main__":
    import json as _json
    logging.basicConfig(level=logging.INFO)
    result = process_all()
    print(_json.dumps(result, indent=2))
