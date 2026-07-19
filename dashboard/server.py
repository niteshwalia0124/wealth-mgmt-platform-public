"""
Distributor Dashboard — FastAPI server for distributor-scoped call visibility.
"""

import asyncio
import logging
import os
import uuid
from datetime import datetime

import websockets
from fastapi import FastAPI, Query, Request, WebSocket
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from google.auth import default as gauth_default
from google.auth.transport.requests import Request as GAuthRequest
from google.cloud import bigquery

log = logging.getLogger("dashboard")

BQ_PROJECT = os.getenv("GCP_PROJECT", "your-project")
BQ_DATASET = os.getenv("BQ_DATASET",  "sbi_mf_poc")
_TABLE     = f"{BQ_PROJECT}.{BQ_DATASET}"

app = FastAPI(title="SBI MF Outbound — Distributor Dashboard")
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))

_static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.isdir(_static_dir):
    app.mount("/static", StaticFiles(directory=_static_dir), name="static")


def _bq() -> bigquery.Client:
    return bigquery.Client(project=BQ_PROJECT)


def _run_query(bq: bigquery.Client, query: str, params: list) -> list[dict]:
    try:
        job = bq.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params))
        return [dict(row) for row in job.result()]
    except Exception as exc:
        log.error("BQ query error: %s", exc)
        return []


# ── Pages ─────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "service": "sbi-mf-dashboard"}


@app.get("/", response_class=HTMLResponse)
async def index(request: Request, arn: str = ""):
    return templates.TemplateResponse(request, "index.html", {"arn": arn})


@app.get("/advisor", response_class=HTMLResponse)
async def advisor_dashboard():
    path = os.path.join(os.path.dirname(__file__), "templates", "advisor_dashboard.html")
    return FileResponse(path, media_type="text/html")


@app.get("/priya", response_class=HTMLResponse)
async def priya_persona():
    path = os.path.join(os.path.dirname(__file__), "templates", "priya_persona.html")
    return FileResponse(path, media_type="text/html")


# ── Data APIs ─────────────────────────────────────────────────────────────────

@app.get("/api/arns")
def api_arns():
    bq = _bq()
    rows = _run_query(bq, f"SELECT arn_code, name FROM `{_TABLE}.distributors` ORDER BY name", [])
    return {"arns": rows}


@app.get("/api/investors")
def api_investors(arn: str = Query("")):
    if not arn:
        return JSONResponse({"error": "arn parameter required"}, status_code=400)
    bq = _bq()
    query = f"""
        SELECT
            i.investor_id, i.full_name, i.mobile, i.preferred_language, i.city, i.state,
            s.sip_id, s.fund_name, s.amc_name, s.monthly_amount_inr,
            CAST(s.expiry_date AS STRING)     AS expiry_date,
            CAST(s.next_debit_date AS STRING) AS next_debit_date,
            s.status AS sip_status, s.frequency, s.folio_no
        FROM `{_TABLE}.investors` i
        JOIN `{_TABLE}.sip_mandates` s ON s.investor_id = i.investor_id
        WHERE i.arn_code = @arn
        ORDER BY i.full_name, s.expiry_date
    """
    rows = _run_query(bq, query, [bigquery.ScalarQueryParameter("arn", "STRING", arn)])
    investors: dict = {}
    for r in rows:
        iid = r["investor_id"]
        if iid not in investors:
            investors[iid] = {
                "investor_id": r["investor_id"],
                "full_name": r["full_name"],
                "mobile": r["mobile"],
                "preferred_language": r.get("preferred_language", "hi-IN"),
                "city": r.get("city", ""),
                "state": r.get("state", ""),
                "sips": [],
            }
        investors[iid]["sips"].append({
            "sip_id": r["sip_id"],
            "fund_name": r["fund_name"],
            "amc_name": r.get("amc_name", ""),
            "monthly_amount_inr": float(r["monthly_amount_inr"]) if r.get("monthly_amount_inr") else 0,
            "expiry_date": r.get("expiry_date"),
            "next_debit_date": r.get("next_debit_date"),
            "sip_status": r.get("sip_status", "active"),
            "frequency": r.get("frequency", "monthly"),
            "folio_no": r.get("folio_no", ""),
        })
    return {"arn": arn, "investors": list(investors.values())}


@app.get("/api/calls")
def api_calls(arn: str = Query(""), limit: int = Query(50)):
    if not arn:
        return JSONResponse({"error": "arn parameter required"}, status_code=400)
    bq = _bq()
    query = f"""
        SELECT
            ce.call_id, ce.trigger_type, ce.status, ce.outcome, ce.twilio_call_sid,
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M IST', TIMESTAMP_ADD(ce.initiated_at, INTERVAL 330 MINUTE)) AS initiated_ist,
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M IST', TIMESTAMP_ADD(ce.completed_at, INTERVAL 330 MINUTE)) AS completed_ist,
            i.full_name AS investor_name, i.preferred_language, ce.notes, ce.transcript_ref
        FROM `{_TABLE}.call_events` ce
        LEFT JOIN `{_TABLE}.investors` i ON i.investor_id = ce.investor_id
        WHERE ce.arn_code = @arn
        ORDER BY ce.initiated_at DESC
        LIMIT @limit
    """
    rows = _run_query(bq, query, [
        bigquery.ScalarQueryParameter("arn",   "STRING", arn),
        bigquery.ScalarQueryParameter("limit", "INT64",  limit),
    ])
    return {"arn": arn, "calls": rows, "total": len(rows)}


@app.get("/api/outcomes")
def api_outcomes(arn: str = Query("")):
    if not arn:
        return JSONResponse({"error": "arn parameter required"}, status_code=400)
    bq = _bq()
    query = f"""
        SELECT COALESCE(outcome, 'pending') AS outcome, COUNT(*) AS count
        FROM `{_TABLE}.call_events` WHERE arn_code = @arn
        GROUP BY outcome ORDER BY count DESC
    """
    rows = _run_query(bq, query, [bigquery.ScalarQueryParameter("arn", "STRING", arn)])
    return {"arn": arn, "outcomes": rows}


@app.get("/api/queue")
def api_queue(arn: str = Query("")):
    if not arn:
        return JSONResponse({"error": "arn parameter required"}, status_code=400)
    bq = _bq()
    query = f"""
        SELECT
            q.queue_id, q.trigger_type, q.priority, q.status, q.block_reason,
            i.full_name AS investor_name, s.fund_name,
            CAST(s.expiry_date AS STRING) AS expiry_date
        FROM `{_TABLE}.call_queue` q
        LEFT JOIN `{_TABLE}.investors`    i ON i.investor_id = q.investor_id
        LEFT JOIN `{_TABLE}.sip_mandates` s ON s.sip_id      = q.sip_id
        WHERE q.arn_code = @arn AND q.status IN ('PENDING','APPROVED','IN_PROGRESS','BLOCKED')
        ORDER BY q.priority, q.created_at
    """
    rows = _run_query(bq, query, [bigquery.ScalarQueryParameter("arn", "STRING", arn)])
    return {"arn": arn, "queue": rows}


@app.get("/api/callbacks")
def api_callbacks(arn: str = Query("")):
    if not arn:
        return JSONResponse({"error": "arn parameter required"}, status_code=400)
    bq = _bq()
    query = f"""
        SELECT i.full_name AS investor_name, i.mobile, ce.call_id, ce.notes,
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M IST', TIMESTAMP_ADD(ce.completed_at, INTERVAL 330 MINUTE)) AS called_at_ist
        FROM `{_TABLE}.call_events` ce
        JOIN `{_TABLE}.investors` i ON i.investor_id = ce.investor_id
        WHERE ce.arn_code = @arn AND ce.outcome = 'callback_requested'
        ORDER BY ce.completed_at DESC
    """
    rows = _run_query(bq, query, [bigquery.ScalarQueryParameter("arn", "STRING", arn)])
    return {"arn": arn, "callbacks": rows}


# ── Action APIs ───────────────────────────────────────────────────────────────

@app.post("/api/call")
async def api_trigger_call(request: Request):
    """Trigger a single AI call for a specific investor + SIP."""
    body         = await request.json()
    investor_id  = body.get("investor_id", "")
    sip_id       = body.get("sip_id", "")
    arn_code     = body.get("arn_code", "")
    trigger_type = body.get("trigger_type", "sip_renewal")
    demo_mobile_ui = body.get("demo_mobile", "")

    bq = _bq()
    query = f"""
        SELECT
            i.investor_id, i.full_name, i.mobile, i.preferred_language,
            s.sip_id, s.fund_name, s.monthly_amount_inr,
            CAST(s.expiry_date AS STRING) AS expiry_date,
            d.name AS distributor_name, d.arn_code
        FROM `{_TABLE}.investors` i
        JOIN `{_TABLE}.sip_mandates` s ON s.sip_id = @sip_id
        JOIN `{_TABLE}.distributors` d ON d.arn_code = @arn
        WHERE i.investor_id = @investor_id LIMIT 1
    """
    rows = _run_query(bq, query, [
        bigquery.ScalarQueryParameter("investor_id", "STRING", investor_id),
        bigquery.ScalarQueryParameter("sip_id",      "STRING", sip_id),
        bigquery.ScalarQueryParameter("arn",         "STRING", arn_code),
    ])
    if not rows:
        return JSONResponse({"error": "Investor or SIP not found"}, status_code=404)

    row        = rows[0]
    expiry_str = str(row.get("expiry_date") or "jald hi")
    sv_map = {
        "sip_renewal":       {"fund_name": row.get("fund_name",""), "monthly_amount": str(int(row.get("monthly_amount_inr") or 0)), "expiry_date": expiry_str},
        "sip_debit_failure": {"fund_name": row.get("fund_name",""), "month": datetime.utcnow().strftime("%B %Y")},
        "sip_paused":        {"fund_name": row.get("fund_name",""), "pause_since": expiry_str},
        "fund_maturity":     {"fund_name": row.get("fund_name",""), "maturity_date": expiry_str},
    }

    demo_mobile = os.getenv("DEMO_MOBILE", "")
    dial_to     = demo_mobile_ui or demo_mobile or row["mobile"]
    queue_id    = f"MANUAL-{uuid.uuid4().hex[:8].upper()}"

    try:
        import sys as _sys
        _sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from voice.call_engine import make_call
        result = make_call(
            investor_id=row["investor_id"],
            mobile=dial_to,
            investor_name=row["full_name"],
            call_type=trigger_type,
            script_variables=sv_map.get(trigger_type, {}),
            distributor_name=row["distributor_name"],
            distributor_arn=row["arn_code"],
            language=row.get("preferred_language", "hi-IN"),
            queue_id=queue_id,
        )
    except Exception as exc:
        log.error("make_call failed: %s", exc)
        return JSONResponse({"error": str(exc)}, status_code=500)

    call_id      = result.get("call_id", f"ERR-{uuid.uuid4().hex[:8].upper()}")
    initiated_at = result.get("initiated_at", datetime.utcnow().isoformat())[:19]
    sid          = (result.get("twilio_call_sid") or "").replace("'", "\\'")
    status       = (result.get("status") or "error").replace("'", "\\'")

    sql = f"""
        INSERT INTO `{_TABLE}.call_events`
            (call_id, queue_id, investor_id, arn_code, trigger_type,
             twilio_call_sid, status, initiated_at, transcript_ref)
        VALUES (
            '{call_id}','{queue_id}','{row["investor_id"]}','{row["arn_code"]}',
            '{trigger_type}','{sid}','{status}',TIMESTAMP '{initiated_at}',''
        )
    """
    try:
        bq.query(sql).result()
    except Exception as exc:
        log.warning("Could not write call_event: %s", exc)

    resp: dict = {"call_id": call_id, "status": status, "twilio_sid": result.get("twilio_call_sid", "")}
    if status == "error":
        resp["error"] = result.get("error", "Call failed — check broker logs")
    return resp


@app.get("/api/transcript/{call_id}")
def api_transcript(call_id: str):
    """Fetch call transcript JSON from GCS."""
    try:
        from google.cloud import storage
        import json as _json
        bucket_name = os.getenv("TRANSCRIPT_BUCKET", "sbi-mf-call-transcripts")
        gcs    = storage.Client()
        bucket = gcs.bucket(bucket_name)
        blob   = bucket.blob(f"{call_id}.json")
        if not blob.exists():
            return JSONResponse({"error": "Transcript not ready yet"}, status_code=404)
        segments = _json.loads(blob.download_as_text())
        return {"call_id": call_id, "segments": segments, "gcs_path": f"gs://{bucket_name}/{call_id}.json"}
    except Exception as exc:
        log.error("Transcript fetch: %s", exc)
        return JSONResponse({"error": str(exc)}, status_code=500)


@app.get("/api/call-status/{call_id}")
def api_call_status(call_id: str):
    """Lightweight single-call status check — used by the 10s poll loop."""
    bq = _bq()
    rows = _run_query(bq, f"""
        SELECT call_id, status, outcome, notes,
            FORMAT_TIMESTAMP('%H:%M IST', TIMESTAMP_ADD(initiated_at,  INTERVAL 330 MINUTE)) AS started_ist,
            FORMAT_TIMESTAMP('%H:%M IST', TIMESTAMP_ADD(completed_at,  INTERVAL 330 MINUTE)) AS ended_ist,
            TIMESTAMP_DIFF(COALESCE(completed_at, CURRENT_TIMESTAMP()), initiated_at, SECOND) AS duration_sec
        FROM `{_TABLE}.call_events`
        WHERE call_id = @call_id LIMIT 1
    """, [bigquery.ScalarQueryParameter("call_id", "STRING", call_id)])

    if not rows:
        return {"call_id": call_id, "status": "initiated", "has_transcript": False}

    result = dict(rows[0])
    try:
        from google.cloud import storage
        bucket_name = os.getenv("TRANSCRIPT_BUCKET", "sbi-mf-call-transcripts")
        blob = storage.Client().bucket(bucket_name).blob(f"{call_id}.json")
        result["has_transcript"] = blob.exists()
    except Exception:
        result["has_transcript"] = False
    return result


@app.post("/api/refresh")
def api_refresh():
    """Trigger outcome processor — classify unprocessed GCS transcripts."""
    try:
        import sys as _sys
        _sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from outcome_processor.processor import process_all
        counts = process_all()
        return {"status": "ok", "processed": counts}
    except Exception as exc:
        log.error("Outcome processor error: %s", exc)
        return JSONResponse({"status": "error", "detail": str(exc)}, status_code=500)


# ── WhatsApp Message ──────────────────────────────────────────────────────────

_WA_TEMPLATES = {
    "sip_renewal": (
        "🏦 *Cymbal Wealth Management*\n\n"
        "Namaste *{investor_name}* ji!\n\n"
        "Your advisor *{distributor_name}* has shared an important update:\n\n"
        "📊 *Fund:* {fund_name}\n"
        "💰 *SIP Amount:* ₹{monthly_amount}/month\n"
        "📅 *Expiry Date:* {expiry_date}\n\n"
        "⚠️ Your SIP mandate is expiring soon. Please contact your advisor to renew it and continue your investment journey.\n\n"
        "_Sent on behalf of {distributor_name} via Cymbal Wealth Management_"
    ),
    "sip_debit_failure": (
        "🏦 *Cymbal Wealth Management*\n\n"
        "Namaste *{investor_name}* ji!\n\n"
        "Your advisor *{distributor_name}* wanted to inform you:\n\n"
        "📊 *Fund:* {fund_name}\n"
        "⚠️ *Status:* SIP debit could not be processed this month\n\n"
        "Please ensure sufficient balance in your registered bank account and contact your advisor for assistance.\n\n"
        "_Sent on behalf of {distributor_name} via Cymbal Wealth Management_"
    ),
    "sip_paused": (
        "🏦 *Cymbal Wealth Management*\n\n"
        "Namaste *{investor_name}* ji!\n\n"
        "A reminder from your advisor *{distributor_name}*:\n\n"
        "📊 *Fund:* {fund_name}\n"
        "⏸️ *Status:* Your SIP is currently paused\n\n"
        "Please contact your advisor if you'd like to resume your SIP and continue building your wealth.\n\n"
        "_Sent on behalf of {distributor_name} via Cymbal Wealth Management_"
    ),
    "fund_maturity": (
        "🏦 *Cymbal Wealth Management*\n\n"
        "Namaste *{investor_name}* ji!\n\n"
        "Important update from your advisor *{distributor_name}*:\n\n"
        "📊 *Fund:* {fund_name}\n"
        "📅 *Maturity Date:* {expiry_date}\n\n"
        "Your fund is approaching its maturity date. Please speak with your advisor to plan your next steps.\n\n"
        "_Sent on behalf of {distributor_name} via Cymbal Wealth Management_"
    ),
}


@app.post("/api/whatsapp")
async def api_whatsapp(request: Request):
    """Send a WhatsApp text message to an investor via Twilio."""
    body        = await request.json()
    investor_id = body.get("investor_id", "")
    sip_id      = body.get("sip_id", "")
    arn_code    = body.get("arn_code", "")
    trigger     = body.get("trigger_type", "sip_renewal")
    demo_mobile_ui = body.get("demo_mobile", "")

    bq = _bq()
    rows = _run_query(bq, f"""
        SELECT
            i.investor_id, i.full_name, i.mobile,
            s.fund_name, s.monthly_amount_inr,
            CAST(s.expiry_date AS STRING) AS expiry_date,
            d.name AS distributor_name
        FROM `{_TABLE}.investors` i
        JOIN `{_TABLE}.sip_mandates` s ON s.sip_id = @sip_id
        JOIN `{_TABLE}.distributors` d ON d.arn_code = @arn
        WHERE i.investor_id = @investor_id LIMIT 1
    """, [
        bigquery.ScalarQueryParameter("investor_id", "STRING", investor_id),
        bigquery.ScalarQueryParameter("sip_id",      "STRING", sip_id),
        bigquery.ScalarQueryParameter("arn",         "STRING", arn_code),
    ])
    if not rows:
        return JSONResponse({"error": "Investor or SIP not found"}, status_code=404)

    row = rows[0]
    template = _WA_TEMPLATES.get(trigger, _WA_TEMPLATES["sip_renewal"])
    message  = template.format(
        investor_name    = row["full_name"],
        distributor_name = row["distributor_name"],
        fund_name        = row.get("fund_name", "your fund"),
        monthly_amount   = int(row.get("monthly_amount_inr") or 0),
        expiry_date      = row.get("expiry_date") or "soon",
    )

    twilio_sid   = os.getenv("TWILIO_ACCOUNT_SID", "")
    twilio_token = os.getenv("TWILIO_AUTH_TOKEN", "")
    twilio_from  = os.getenv("TWILIO_WHATSAPP_FROM", os.getenv("TWILIO_FROM_NUMBER", ""))
    demo_mobile  = os.getenv("DEMO_MOBILE", "")

    if not twilio_sid:
        return JSONResponse({
            "status": "simulated",
            "message": message,
            "note": "Set TWILIO_ACCOUNT_SID to send real WhatsApp messages",
        })

    to_mobile = demo_mobile_ui or demo_mobile or row["mobile"]
    # Ensure whatsapp: prefix
    wa_to   = f"whatsapp:{to_mobile}"   if not to_mobile.startswith("whatsapp:")   else to_mobile
    wa_from = f"whatsapp:{twilio_from}" if not twilio_from.startswith("whatsapp:") else twilio_from

    try:
        import httpx
        r = httpx.post(
            f"https://api.twilio.com/2010-04-01/Accounts/{twilio_sid}/Messages.json",
            auth=(twilio_sid, twilio_token),
            data={"From": wa_from, "To": wa_to, "Body": message},
            timeout=15,
        )
        r.raise_for_status()
        sid = r.json().get("sid", "")
        log.info("WhatsApp sent to %s: %s", wa_to, sid)
        return {"status": "sent", "message_sid": sid, "to": wa_to}
    except Exception as exc:
        log.error("WhatsApp send failed: %s", exc)
        return JSONResponse({"error": str(exc)}, status_code=500)


# ── Video Call ─────────────────────────────────────────────────────────────────

@app.post("/api/video-call")
async def api_video_call(request: Request):
    """
    Initiate a Priya avatar video call for an investor.
    Returns a URL the distributor opens in a new tab.
    """
    body        = await request.json()
    investor_id = body.get("investor_id", "")
    sip_id      = body.get("sip_id", "")
    arn_code    = body.get("arn_code", "")
    trigger     = body.get("trigger_type", "sip_renewal")
    avatar_name = body.get("avatar_name", "Kira")   # built-in: Kira, Vera, Piper, Carmen …

    bq = _bq()
    rows = _run_query(bq, f"""
        SELECT
            i.investor_id, i.full_name, i.mobile, i.preferred_language,
            s.sip_id, s.fund_name, s.monthly_amount_inr,
            CAST(s.expiry_date AS STRING) AS expiry_date,
            d.name AS distributor_name, d.arn_code
        FROM `{_TABLE}.investors` i
        JOIN `{_TABLE}.sip_mandates` s ON s.sip_id = @sip_id
        JOIN `{_TABLE}.distributors` d ON d.arn_code = @arn
        WHERE i.investor_id = @investor_id LIMIT 1
    """, [
        bigquery.ScalarQueryParameter("investor_id", "STRING", investor_id),
        bigquery.ScalarQueryParameter("sip_id",      "STRING", sip_id),
        bigquery.ScalarQueryParameter("arn",         "STRING", arn_code),
    ])
    if not rows:
        return JSONResponse({"error": "Investor or SIP not found"}, status_code=404)

    row      = rows[0]
    language = row.get("preferred_language") or "en-IN"
    call_id  = f"VCALL-{uuid.uuid4().hex[:8].upper()}"

    # Build the same system instruction as a voice call but tuned for video
    try:
        import sys as _sys
        _sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from voice.call_engine import build_system_instruction, build_language_instruction, SUPPORTED_LANGUAGES, AGENDAS
        lang_info   = SUPPORTED_LANGUAGES.get(language, SUPPORTED_LANGUAGES["en-IN"])
        expiry_str  = str(row.get("expiry_date") or "soon")
        agenda_vars = {
            "sip_renewal":       {"fund_name": row.get("fund_name",""), "monthly_amount": str(int(row.get("monthly_amount_inr") or 0)), "expiry_date": expiry_str},
            "sip_debit_failure": {"fund_name": row.get("fund_name",""), "month": datetime.utcnow().strftime("%B %Y")},
            "sip_paused":        {"fund_name": row.get("fund_name",""), "pause_since": expiry_str},
            "fund_maturity":     {"fund_name": row.get("fund_name",""), "maturity_date": expiry_str},
        }
        sv = agenda_vars.get(trigger, {})
        sv["distributor_name"] = row["distributor_name"]
        agenda = AGENDAS.get(trigger, "").format(**sv)
        system_instruction = build_system_instruction(
            agenda=agenda,
            investor_name=row["full_name"],
            distributor_name=row["distributor_name"],
            language_instruction=build_language_instruction(language),
            greeting=lang_info["greeting"],
            closing=lang_info["closing"],
        )
    except Exception as exc:
        log.warning("Could not build system instruction: %s — using default", exc)
        system_instruction = ""

    # Send context to broker /video/{call_id}/prepare
    broker_url = os.getenv("LIVEAPI_BROKER_URL", "http://localhost:8010")
    try:
        import httpx
        r = httpx.post(f"{broker_url}/video/{call_id}/prepare", json={
            "system_instruction": system_instruction,
            "client_name":        row["full_name"],
            "rm_name":            row["distributor_name"],
            "language":           language,
            "avatar_name":        avatar_name,
        }, timeout=20)
        r.raise_for_status()
    except Exception as exc:
        return JSONResponse({"error": f"Broker prepare failed: {exc}"}, status_code=502)

    # Build the public WebSocket URL for the broker
    broker_ws = broker_url.replace("http://", "ws://").replace("https://", "wss://")

    video_url = (
        f"/video-call"
        f"?call_id={call_id}"
        f"&broker={broker_ws}"
        f"&investor={row['full_name']}"
        f"&fund={row.get('fund_name','')}"
    )
    return {"call_id": call_id, "video_url": video_url}


_CLIENT_PORTFOLIOS = {
    "amit_patel": {
        "name": "Amit Patel", "salutation": "Mr. Patel",
        "loc": "Mumbai", "age": 52, "risk": "Aggressive", "aum": "₹38.4 Cr", "nw": "₹45.2 Cr",
        "portfolio": {
            "ytdReturn": 9.2, "benchmarkReturn": 15.8, "healthScore": 58,
            "sectors": [
                {"name": "Technology", "pct": 38, "model": 20},
                {"name": "Financials", "pct": 12, "model": 28},
                {"name": "Alt Investments", "pct": 20, "model": 12},
                {"name": "Healthcare", "pct": 8, "model": 18},
                {"name": "Consumer", "pct": 14, "model": 16},
                {"name": "Cash & Equiv", "pct": 8, "model": 6},
            ],
            "holdings": [
                {"name": "Infosys", "pct": 11.2}, {"name": "TCS", "pct": 9.8},
                {"name": "HCL Technologies", "pct": 8.4}, {"name": "Kotak PE Fund (AIF)", "pct": 14.2},
                {"name": "Apple Inc (US Equity)", "pct": 6.8},
            ],
            "riskScore": 7.8,
            "aiInsight": "Portfolio trailing model by 6.6pp YTD (9.2% vs 15.8%) — technology at 38% vs model 20% while financials (held at 12% vs model 28%) and healthcare (8% vs 18%) have rallied 32% and 24% respectively this year. Rotating ₹8–10 Cr into HDFC Bank, SBI, Cipla, and Sun Pharma would close the performance gap and reduce tech concentration risk.",
            "talkingPoints": [
                "Gap: Your portfolio +9.2% vs Model +15.8% YTD → ₹2.5 Cr uncaptured returns in 2026",
                "Tech at 38% vs model 20% → holding 18pp too much in a sector that has gone flat",
                "Financials only 12% vs model 28% → missed the 32% rally in HDFC Bank and SBI",
                "Healthcare only 8% vs model 18% → missed 24% rally in Cipla and Sun Pharma this year",
                "Rebalance: rotate ₹8–10 Cr from Tech into Banking and Healthcare to close the gap",
            ],
        },
    },
    "priya_kapoor": {
        "name": "Priya Kapoor", "salutation": "Ms. Kapoor",
        "loc": "Bengaluru", "age": 44, "risk": "Balanced", "aum": "₹21.3 Cr", "nw": "₹28.7 Cr",
        "portfolio": {
            "ytdReturn": 12.8, "benchmarkReturn": 11.8, "healthScore": 74,
            "sectors": [
                {"name": "Technology", "pct": 22, "model": 20},
                {"name": "Financials", "pct": 24, "model": 25},
                {"name": "Healthcare", "pct": 16, "model": 15},
                {"name": "Consumer Disc", "pct": 18, "model": 18},
                {"name": "Industrials", "pct": 12, "model": 14},
                {"name": "Debt & Cash", "pct": 8, "model": 8},
            ],
            "holdings": [
                {"name": "HDFC Large Cap Fund (Maturing)", "pct": 14.2},
                {"name": "SBI Balanced Advantage (Maturing)", "pct": 11.8},
                {"name": "ICICI Pru Technology (Maturing)", "pct": 8.2},
                {"name": "HDFC Bank", "pct": 9.4}, {"name": "Mirae Asset Large Cap", "pct": 7.6},
            ],
            "riskScore": 5.2,
            "aiInsight": "Three goal-based SIPs (₹48,000/month combined) complete their 5-year tenure by July 31 — HDFC Large Cap underperforming category by 3.2%, ICICI Tech theme faded. Recommend switching to Parag Parikh Flexi Cap for global diversification, HDFC Manufacturing Fund for PLI-theme returns (+28% category), and Mirae ELSS to add ₹96,000 annual 80C deduction on the same monthly SIP amount.",
            "talkingPoints": [
                "3 SIPs (₹48,000/month) mature Jul 31 — HDFC Large Cap, SBI Balanced Adv, ICICI Tech",
                "HDFC Large Cap: 10.4% vs category 13.6% → −3.2pp gap over 18 months",
                "Switch to Parag Parikh Flexi Cap (16.2%) and HDFC Manufacturing Fund (18.4%)",
                "Mirae ELSS: same ₹8,000/month SIP + ₹96,000 annual 80C tax deduction added",
            ],
        },
    },
    "vikram_nair": {
        "name": "Vikram Nair", "salutation": "Mr. Nair",
        "loc": "Chennai", "age": 48, "risk": "Growth", "aum": "₹26.8 Cr", "nw": "₹31.6 Cr",
        "portfolio": {
            "ytdReturn": 22.3, "benchmarkReturn": 11.8, "healthScore": 84,
            "sectors": [
                {"name": "Technology", "pct": 28, "model": 25},
                {"name": "Consumer Disc", "pct": 22, "model": 18},
                {"name": "Financials", "pct": 20, "model": 25},
                {"name": "Industrials", "pct": 14, "model": 12},
                {"name": "Healthcare", "pct": 10, "model": 14},
                {"name": "Materials", "pct": 6, "model": 6},
            ],
            "holdings": [
                {"name": "Mirae Asset Mid Cap Fund", "pct": 11.2}, {"name": "Axis Growth Opp Fund", "pct": 9.8},
                {"name": "HDFC Mid-Cap Opp Fund", "pct": 8.6}, {"name": "Reliance Industries", "pct": 7.4},
                {"name": "Bajaj Finance", "pct": 6.1},
            ],
            "riskScore": 7.1,
            "aiInsight": "Mid-cap concentration at 52% has driven exceptional +22.3% YTD returns outperforming benchmark by 10.5pp, but 12 SIP mandates totalling ₹8.4 Cr/month expire within 30 days. Mandate disruption at current NAV levels would break rupee-cost averaging during peak momentum — renewal is the single highest-priority action this week.",
            "talkingPoints": [
                "Portfolio up +22.3% YTD — outperforming benchmark by 10.5pp, outstanding performance",
                "12 SIP mandates expire in 30 days — ₹8.4 Cr/month at risk of disruption",
                "Mid-cap at 52% has driven returns but increases volatility risk",
                "Recommend mandate renewal immediately + consider partial large-cap allocation for stability",
            ],
        },
    },
    "rajiv_singhania": {
        "name": "Rajiv Singhania", "salutation": "Mr. Singhania",
        "loc": "NRI · Dubai", "age": 61, "risk": "HNW Global", "aum": "₹58.7 Cr", "nw": "₹62.1 Cr",
        "portfolio": {
            "ytdReturn": 14.9, "benchmarkReturn": 11.8, "healthScore": 78,
            "sectors": [
                {"name": "Alt Investments (AIF)", "pct": 38, "model": 30},
                {"name": "Technology (US)", "pct": 18, "model": 15},
                {"name": "Financials", "pct": 16, "model": 20},
                {"name": "Healthcare", "pct": 12, "model": 15},
                {"name": "Consumer", "pct": 10, "model": 12},
                {"name": "Cash & Bonds", "pct": 6, "model": 8},
            ],
            "holdings": [
                {"name": "ICICI Pru AIF Cat III", "pct": 18.6}, {"name": "Apple Inc (US Equity)", "pct": 8.4},
                {"name": "Microsoft Corp (US)", "pct": 6.2}, {"name": "HDFC Balanced Advantage", "pct": 7.1},
                {"name": "Kotak Emerging Equity", "pct": 5.8},
            ],
            "riskScore": 6.4,
            "aiInsight": "AIF Cat III exposure at 38% exceeds model target of 30%, creating illiquidity risk for a global portfolio that requires FEMA repatriation flexibility. DTAA Form 67 deadline Jul 31 represents a ₹42 L tax saving on US equity LTCG under India-UAE treaty — immediate filing action required before the Section 90 claim window closes.",
            "talkingPoints": [
                "Portfolio up +14.9% YTD — 3.1pp above benchmark, solid performance",
                "AIF Cat III at 38% vs model 30% → illiquidity risk with FEMA repatriation upcoming",
                "DTAA Form 67 deadline Jul 31 — ₹42 L treaty benefit (10% vs 20% LTCG) at stake",
                "FEMA repatriation plan needed: ₹9 Cr Dubai property proceeds, USD 1M/year limit",
            ],
        },
    },
    "sunita_mehrotra": {
        "name": "Sunita Mehrotra", "salutation": "Ms. Mehrotra",
        "loc": "Pune", "age": 58, "risk": "Conservative", "aum": "₹15.2 Cr", "nw": "₹18.4 Cr",
        "portfolio": {
            "ytdReturn": 6.8, "benchmarkReturn": 11.8, "healthScore": 71,
            "sectors": [
                {"name": "Bonds & Debt", "pct": 52, "model": 35},
                {"name": "Financials", "pct": 18, "model": 25},
                {"name": "Healthcare", "pct": 10, "model": 15},
                {"name": "Consumer Staples", "pct": 8, "model": 12},
                {"name": "Technology", "pct": 6, "model": 8},
                {"name": "Cash & FDs", "pct": 6, "model": 5},
            ],
            "holdings": [
                {"name": "HDFC FD (Maturing Jul 15)", "pct": 16.4}, {"name": "SBI Corporate Bond Fund", "pct": 11.2},
                {"name": "ICICI Pru Bluechip Fund", "pct": 8.6}, {"name": "Kotak Banking & PSU Fund", "pct": 7.4},
                {"name": "HDFC Short Term Debt Fund", "pct": 6.8},
            ],
            "riskScore": 3.2,
            "aiInsight": "Debt over-concentration at 52% exceeds conservative model target of 35%, and the ₹2.5 Cr HDFC FD matures July 15 at a reduced 6.5% rate versus the locked 7.2%. Switching to HDFC Floating Rate Fund before maturity captures 7.8% post-tax yield with indexation — an additional ₹14.2 L over 3 years with equivalent AAA credit quality.",
            "talkingPoints": [
                "Portfolio up +6.8% YTD — below benchmark but aligned to conservative mandate",
                "Debt at 52% vs model 35% — over-concentrated, reducing equity growth potential",
                "₹2.5 Cr HDFC FD matures Jul 15 — new rate 6.5% vs current 7.2%, a step down",
                "Recommend HDFC Floating Rate Fund: 7.8% post-tax, AAA-rated, T+2 liquidity vs FD",
                "Over 3 years with indexation: ₹14.2 L more than renewing the FD",
            ],
        },
    },
}


@app.get("/api/wealth/client/{client_id}/portfolio")
async def get_client_portfolio(client_id: str):
    data = _CLIENT_PORTFOLIOS.get(client_id)
    if not data:
        return JSONResponse({"error": "Client not found"}, status_code=404)
    return JSONResponse(data)


@app.get("/video-call", response_class=HTMLResponse)
async def video_call_page(request: Request):
    """Serve the Rohan avatar video call page."""
    return templates.TemplateResponse(request, "video_call.html", {})


@app.websocket("/ws-proxy")
async def ws_proxy_endpoint(websocket: WebSocket):
    """Proxy browser WebSocket to Vertex AI Live API with server-side auth."""
    target = websocket.query_params.get("target")
    if not target or "LlmBidiService/BidiGenerateContent" not in target:
        await websocket.close(code=1008)
        return

    try:
        credentials, project = gauth_default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        credentials.refresh(GAuthRequest())
        token = credentials.token
    except Exception as exc:
        log.error("WS proxy auth error: %s", exc)
        await websocket.close(code=1011)
        return

    location = os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")
    if location == "global":
        location = "us-central1"
    gcp_project = project or os.getenv("GOOGLE_CLOUD_PROJECT", "butterfly-987")

    upstream_url = (
        f"wss://{location}-aiplatform.googleapis.com"
        f"//ws/google.cloud.aiplatform.v1beta1.LlmBidiService/BidiGenerateContent"
    )

    print(f"[WS-PROXY] wslib={websockets.__version__} token_len={len(token)} project={gcp_project} location={location}", flush=True)
    print(f"[WS-PROXY] upstream_url={upstream_url}", flush=True)

    await websocket.accept()

    try:
        async with websockets.connect(
            upstream_url,
            extra_headers={
                "Authorization": f"Bearer {token}",
                "X-Goog-User-Project": gcp_project,
            },
        ) as upstream:
            print("[WS-PROXY] upstream connected OK", flush=True)

            async def client_to_upstream():
                import json as _json
                try:
                    while True:
                        data = await websocket.receive()
                        if "bytes" in data and data["bytes"] is not None:
                            await upstream.send(data["bytes"])
                        elif "text" in data and data["text"] is not None:
                            text = data["text"]
                            try:
                                msg = _json.loads(text)
                                if "setup" in msg and "model" in msg["setup"]:
                                    import re as _re
                                    short_model = _re.sub(
                                        r'^projects/[^/]+/locations/[^/]+/', '',
                                        msg["setup"]["model"]
                                    )
                                    fq = f"projects/{gcp_project}/locations/{location}/{short_model}"
                                    msg["setup"]["model"] = fq
                                    text = _json.dumps(msg)
                                    print(f"[WS-PROXY] setup rewritten → {fq}", flush=True)
                                else:
                                    print(f"[WS-PROXY] client msg keys={list(msg.keys())}", flush=True)
                            except Exception as je:
                                print(f"[WS-PROXY] json parse err: {je}", flush=True)
                            await upstream.send(text)
                except Exception as e:
                    print(f"[WS-PROXY] client_to_upstream ended: {e}", flush=True)

            async def upstream_to_client():
                try:
                    async for msg in upstream:
                        if isinstance(msg, bytes):
                            await websocket.send_bytes(msg)
                        else:
                            print(f"[WS-PROXY] upstream→client text: {str(msg)[:200]}", flush=True)
                            await websocket.send_text(msg)
                except Exception as e:
                    print(f"[WS-PROXY] upstream_to_client ended: {e}", flush=True)
                try:
                    print(f"[WS-PROXY] upstream close code={getattr(upstream,'close_code','?')} reason={getattr(upstream,'close_reason','?')}", flush=True)
                except Exception:
                    pass

            tasks = [
                asyncio.create_task(client_to_upstream()),
                asyncio.create_task(upstream_to_client()),
            ]
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for t in pending:
                t.cancel()
            for t in done:
                if t.exception():
                    print(f"[WS-PROXY] task exception: {t.exception()}", flush=True)
    except Exception as exc:
        log.error("WS proxy error: %s", exc)
    finally:
        try:
            await websocket.close()
        except Exception:
            pass


# ── Wealth Management Voice Call ──────────────────────────────────────────────

BROKER_URL       = os.getenv("LIVEAPI_BROKER_URL",  "https://wealth-mgmt-broker-1058427839055.us-central1.run.app")  # shared broker, unchanged
TWILIO_SID       = os.getenv("TWILIO_ACCOUNT_SID",  "")
TWILIO_TOKEN     = os.getenv("TWILIO_AUTH_TOKEN",    "")
TWILIO_FROM      = os.getenv("TWILIO_FROM_NUMBER",   "")
DEMO_MOBILE      = os.getenv("DEMO_MOBILE",          "")
WM_TRANSCRIPT_BKT = os.getenv("TRANSCRIPT_BUCKET",  "wealth-mgmt-call-transcripts")

# In-memory stores
_WM_CALL_SIDS: dict[str, dict] = {}       # call_id → {twilio_sid, client_name, client_id}
_WM_CALL_ACTIONS: dict[str, list] = {}    # call_id → extracted action items
_WM_CRM_ACTIONS: list = []                # global Salesforce-stub task queue
_WM_VOICE_NOTES: dict[str, dict] = {}     # note_id → {script, audio_bytes, client_key, status, ...}

_TWILIO_TERMINAL = {"completed", "failed", "busy", "no-answer", "canceled"}

_WEALTH_INSTRUCTIONS: dict[str, str] = {
    "amit_patel": """You are Priya, a professional AI voice assistant for Cymbal Wealth Management.
You are calling Amit Patel on behalf of his Relationship Manager, Rohan Sharma.

OPENING: "Good afternoon Mr. Patel. I am Priya calling from Cymbal Wealth Management on behalf of your advisor Rohan Sharma. This call is being recorded for quality and compliance purposes. Do you have a moment to speak?"
If NO: "Of course, no problem. Have a good day." — end immediately.

YOUR BRIEF: Amit's portfolio is up 9.2% this year but the model portfolio for his risk profile is up 15.8% — a gap of 6.6 percentage points. On his ₹38.4 Crore corpus, that gap represents approximately ₹2.5 Crore in returns he has not captured this year. The root cause: his technology allocation stands at 38% of his portfolio, against a model target of 20%. Meanwhile financials (which he holds at only 12% vs model 28%) have returned 32% this year, and healthcare (held at 8% vs model 18%) has returned 24%.

KEY TALKING POINTS (present one at a time, listen before moving to the next):
1. The performance gap: "Your portfolio is up 9.2% this year. The model portfolio for your risk profile is up 15.8%. That 6.6 percentage point difference works out to roughly ₹2.5 Crore in returns you have not captured."
2. The cause: "The main reason is technology. You hold 38% in tech — Infosys, TCS, HCL — but the model says 20%. Tech has been flat while financials and healthcare have rallied strongly."
3. What was missed: "HDFC Bank is up 34% this year. SBI is up 28%. Cipla and Sun Pharma are both up 24%. You are significantly underweight all of them."
4. The action: "Rohan recommends rotating ₹8 to 10 Crore from tech into banking and pharma. Your tech weight comes down to around 22%, financials move up to 26%, and the gap to the model closes substantially."

RULES:
- Present talking points one at a time — say it, then pause and listen
- Never recommend specific funds — say "Rohan will walk you through the specific names"
- Never collect account numbers, OTPs, or PAN
- Never process or promise any transaction
- If asked anything outside this brief: "Rohan will personally guide you on that"
- End: "Thank you Mr. Patel. Rohan will send you the detailed rebalancing proposal within 24 hours. Goodbye."
""",

    "priya_kapoor": """You are Priya, a professional AI voice assistant for Cymbal Wealth Management.
You are calling Priya Kapoor on behalf of her Relationship Manager, Rohan Sharma.

OPENING: "Good morning Ms. Kapoor. I am Priya calling from Cymbal Wealth Management on behalf of your advisor Rohan Sharma. This call is being recorded for quality purposes. Do you have a few minutes to speak?"
If NO: "Of course, no problem. Rohan will send you a message with the details. Have a good day." — end immediately.

YOUR BRIEF: Priya has three goal-based SIPs totalling ₹48,000 per month that are completing their 5-year tenure on July 31st — just weeks away. The three SIPs are: HDFC Large Cap Fund at ₹25,000 per month, SBI Balanced Advantage Fund at ₹15,000 per month, and ICICI Prudential Technology Fund at ₹8,000 per month. All three have been underperforming their category peers. Without guidance she will auto-renew into the same underperforming funds. Rohan has identified better alternatives.

KEY TALKING POINTS (present one at a time, listen before moving to the next):
1. The maturity alert: "Ms. Kapoor, your three goal-based SIPs — HDFC Large Cap, SBI Balanced Advantage, and ICICI Technology — complete their 5-year tenure on July 31st. At maturity you have a window to reinvest into better options rather than auto-renewing."
2. The fund quality gap: "The HDFC Large Cap Fund has underperformed its category average by 3.2 percentage points over the past 18 months. The ICICI Technology SIP was well-timed when the theme was active, but technology as a theme has cooled and that fund is now 4.8 percentage points behind its category."
3. The recommended switches: "Rohan recommends switching HDFC Large Cap to Parag Parikh Flexi Cap, which gives you global diversification and has consistently outperformed. For SBI Balanced Advantage he suggests HDFC Manufacturing Fund, which is riding the government's industrial push and is up 28 percent in its category this year."
4. The tax benefit: "For the ICICI Tech SIP, Rohan recommends switching to Mirae Asset ELSS. Same ₹8,000 per month — but you gain a Section 80C deduction of ₹96,000 per year. On your tax slab that saves roughly ₹28,000 in tax annually while building your corpus in a stronger fund."

RULES:
- Present talking points one at a time — say it, then pause and listen
- Never confirm or execute a switch — say "Rohan will send you the switch instruction form for your approval"
- Never collect account numbers, folio numbers, or OTP
- Never process any transaction
- Deadline framing only: "The July 31st maturity creates a natural decision window"
- If asked anything outside this brief: "Rohan will personally guide you on that"
- End: "Thank you Ms. Kapoor. Rohan will send you the fund comparison and switch instructions on WhatsApp today. Goodbye."
""",

    "vikram_nair": """You are Priya, a bilingual AI voice assistant for Cymbal Wealth Management.
You are calling Vikram Nair on behalf of his Relationship Manager, Rohan Sharma.

LANGUAGE: Start in English. If Vikram speaks Tamil or prefers Tamil, switch immediately and continue in Tamil.

OPENING: "Hello Mr. Nair. I am Priya calling from Cymbal Wealth Management on behalf of your advisor Rohan Sharma. This call is recorded for quality purposes. Are you available for a quick 2-minute call?"
If NO: "No problem, I will send a WhatsApp message instead. Goodbye."

YOUR BRIEF: Vikram has 12 SIP mandates expiring within the next 30 days. Total monthly SIP flow at risk: ₹8.4 Crore. 4 are e-NACH mandates and 8 are physical NACH mandates. Each failed NACH results in a penalty of ₹500 to ₹2,000 and disrupts his mid-cap rupee cost averaging.

WHAT TO SAY:
"Mr. Nair, I am calling about an important update regarding your SIP investments. 12 of your SIP mandates are expiring in the next 30 days, including your largest position — the ₹1.2 Crore monthly SIP in the Mirae Asset Mid Cap fund. Rohan is sending renewal links to your WhatsApp right now. The 4 e-NACH mandates can be renewed in about 3 minutes via NetBanking. The 8 physical NACH mandates have pre-filled forms you can sign digitally with your Aadhaar OTP. Is there anything you would like me to clarify?"

Handle questions simply. If asked about specific mandates or amounts: "Rohan's message has the complete list with each mandate, amount, and renewal link."

CLOSING: "Thank you Mr. Nair. Please check your WhatsApp from Rohan for the renewal links. Have a good day!"

RULES:
- Never commit to any transaction or process any instruction
- Never collect bank details or OTP
- Keep it brief — the goal is to alert and direct to WhatsApp
""",

    "rajiv_singhania": """You are Priya, a professional AI voice assistant for Cymbal Wealth Management.
You are calling Rajiv Singhania on behalf of his Relationship Manager, Rohan Sharma.

OPENING: "Good evening Mr. Singhania. I am Priya calling from Cymbal Wealth Management on behalf of your advisor Rohan Sharma. This call is being recorded for compliance purposes. Do you have a few minutes for an important tax planning matter?"
If NO: "Of course. Rohan will reach out to schedule a call at your convenience. Goodbye."

YOUR BRIEF: Critical DTAA deadline approaching. Rajiv holds US equity positions with ₹4.2 Crore in long-term capital gains. Under the India-UAE Double Taxation Avoidance Agreement, Section 90, he is eligible for a 10% treaty rate instead of the standard 20% — a saving of ₹42 Lakhs. He must file Form 67 by July 31st. He is also planning to repatriate ₹9 Crore from a Dubai property sale.

WHAT TO SAY:
"Mr. Singhania, I am calling about an important deadline — July 31st — for your DTAA Form 67 filing. Under the India-UAE treaty, your US equity gains of ₹4.2 Crore qualify for a 10% long-term capital gains rate instead of the standard 20%. This means a potential tax saving of ₹42 Lakhs. Rohan has prepared the Form 67 draft and would like to walk you through it on a video call. Are you available for a 30-minute video call this week?"

If asked about FEMA repatriation: "Rohan has also modelled a 5-year repatriation structure for the Dubai property proceeds. He will cover both topics in the video call."

CLOSING: "Rohan will send you a calendar invite for the video call and the Form 67 draft via email. Thank you Mr. Singhania. Goodbye."

RULES:
- Speak formally, precisely — this is an HNW NRI client
- Never give specific tax advice or file anything on his behalf
- Never collect account numbers or document IDs
""",

    "sunita_mehrotra": """You are Priya, a warm and professional AI voice assistant for Cymbal Wealth Management.
You are calling Sunita Mehrotra on behalf of her Relationship Manager, Rohan Sharma.

OPENING: "Good morning Mrs. Mehrotra. I am Priya calling from Cymbal Wealth Management on behalf of your advisor Rohan Sharma. Hope you are doing well. This is a quick call about your HDFC Fixed Deposit that is maturing soon. Do you have a moment?"
If NO: "Of course. I will ask Rohan to send you the details by message. Take care, goodbye."

YOUR BRIEF: Sunita's ₹2.5 Crore HDFC Fixed Deposit matures on July 15th. The renewal rate is 6.5%, down from her locked rate of 7.2%. Post-RBI rate cuts, a short-duration debt mutual fund now yields approximately 7.8% post-tax on a 1-year rolling basis. With indexation on a 3-year hold, the effective yield reaches 8.1%. Rohan has prepared a personalised comparison.

WHAT TO SAY:
"Mrs. Mehrotra, your ₹2.5 Crore HDFC Fixed Deposit is maturing on July 15th. The renewal rate is now 6.5%, which is lower than your current locked rate. Rohan has prepared a comparison showing that a short-duration debt mutual fund could offer around 7.8% post-tax — that is about 1.3 percentage points more per year. On ₹2.5 Crore over 3 years, the difference is approximately ₹14 Lakhs after tax. The fund Rohan suggests holds 97% AAA-rated instruments — very similar safety to your FD, but with better returns and T+2 liquidity. Would you like Rohan to send you the detailed comparison before July 15th?"

If she expresses concern about safety: "I completely understand. The funds Rohan is suggesting are the highest quality — same credit rating as your FD. But the final decision is entirely yours after reviewing his document."

CLOSING: "Rohan will WhatsApp you the personalised comparison today. Thank you Mrs. Mehrotra. Have a lovely day!"

RULES:
- Warm, reassuring tone — she is conservative
- Never push or create urgency beyond the maturity date
- Never promise specific returns — use approximate language
- Never collect bank details
""",
}


# Short voice note scripts — 30-45 seconds when spoken at natural pace
_WM_VOICE_NOTE_SCRIPTS: dict[str, str] = {
    "amit_patel": (
        "Good afternoon, Mister Patel. This is Priya from Cymbal Wealth Management, "
        "calling on behalf of your advisor Rohan Sharma. "
        "Rohan has completed a review of your portfolio against the model portfolio for your risk profile. "
        "Your portfolio is up nine point two percent this year — but the model is up fifteen point eight percent. "
        "That six point six percentage point gap works out to roughly two point five crore rupees in returns not yet captured. "
        "The main reason is your technology allocation at thirty eight percent, while the model says twenty percent. "
        "Financials and healthcare have both rallied strongly this year and you are underweight both. "
        "Rohan has a rebalancing proposal ready — rotating eight to ten crore from tech into banking and pharma. "
        "Please check your WhatsApp for his detailed note. Thank you, and have a great day."
    ),
    "priya_kapoor": (
        "Good morning, Ms. Kapoor. This is Priya from Cymbal Wealth Management "
        "calling on behalf of your advisor Rohan Sharma. "
        "This is a quick alert about your three SIP investments — HDFC Large Cap, SBI Balanced Advantage, and ICICI Technology. "
        "All three are completing their five year tenure on July thirty first — just a few weeks away. "
        "Rohan has reviewed these funds and found they have been underperforming their category peers. "
        "He has identified better alternatives — including one that adds a ninety six thousand rupee annual tax deduction under Section Eighty C. "
        "Please check your WhatsApp for Rohan's fund switch recommendations before the July thirty first deadline. "
        "Thank you for your time."
    ),
    "vikram_nair": (
        "Hello, Mister Nair. This is Priya from Cymbal Wealth Management with an important update from Rohan Sharma. "
        "Twelve of your SIP mandates, totalling eight point four crore rupees per month, "
        "are expiring in the next thirty days. "
        "Renewal links for all twelve mandates have been sent to this WhatsApp number. "
        "The four e-NACH mandates can be renewed in just three minutes via net banking. "
        "Please act at the earliest to avoid any disruption to your SIP investments. "
        "Thank you, and have a good day."
    ),
    "rajiv_singhania": (
        "Good evening, Mister Singhania. This is Priya from Cymbal Wealth Management "
        "with a time-sensitive reminder from Rohan Sharma. "
        "Your DTAA Form Sixty Seven filing deadline is July thirty first. "
        "Your US equity gains of four point two crore rupees qualify for the ten percent treaty rate "
        "under the India-UAE agreement, instead of the standard twenty percent — "
        "a potential saving of forty two lakhs in tax. "
        "Rohan has the filing draft ready and requests a thirty-minute video call this week. "
        "Please respond to his calendar invite. Thank you."
    ),
    "sunita_mehrotra": (
        "Good morning, Mrs. Mehrotra. This is Priya from Cymbal Wealth Management "
        "calling on behalf of Rohan Sharma. "
        "Your two point five crore HDFC Fixed Deposit matures on July fifteenth. "
        "The renewal rate has dropped to six point five percent. "
        "Rohan has identified a short-duration debt mutual fund option offering approximately "
        "seven point eight percent post-tax — that is roughly fourteen lakhs more over three years, "
        "with ninety seven percent AAA-rated instruments, similar in safety to your FD. "
        "He is sending you the personalised comparison on WhatsApp today. "
        "Thank you, and take care."
    ),
}

DASHBOARD_URL = os.getenv("DASHBOARD_URL", "https://wealth-mgmt-demo2-dashboard-1058427839055.us-central1.run.app")
TWILIO_WHATSAPP_FROM = os.getenv("TWILIO_WHATSAPP_FROM", "whatsapp:+14155238886")


def _pcm_to_ogg(pcm_bytes: bytes, sample_rate: int = 24000) -> bytes:
    """Convert raw PCM (16-bit mono) to OGG/Opus bytes via pydub+ffmpeg.
    WhatsApp requires OGG/Opus (or MP3/AAC) — WAV is not supported (Twilio error 63015).
    """
    from pydub import AudioSegment
    import io as _io
    seg = AudioSegment(data=pcm_bytes, sample_width=2, frame_rate=sample_rate, channels=1)
    buf = _io.BytesIO()
    seg.export(buf, format="ogg", codec="libopus")
    return buf.getvalue()


def _upload_voice_note_gcs(note_id: str, ogg_bytes: bytes) -> None:
    """Upload OGG audio to GCS so it's accessible from any Cloud Run instance."""
    try:
        from google.cloud import storage as _gcs
        client = _gcs.Client()
        bucket = client.bucket(WM_TRANSCRIPT_BKT)
        blob = bucket.blob(f"voice-notes/{note_id}.ogg")
        blob.upload_from_string(ogg_bytes, content_type="audio/ogg")
        log.info("Voice note %s OGG uploaded to GCS (%d bytes)", note_id, len(ogg_bytes))
    except Exception as exc:
        log.warning("GCS upload for voice note %s failed: %s", note_id, exc)


def _write_voice_note_history(client_key: str, note_id: str, script: str, status: str, twilio_sid: str) -> None:
    """Persist voice note record to GCS so it survives cold starts."""
    if not client_key:
        return
    try:
        import json as _json
        from google.cloud import storage
        record = {
            "note_id": note_id,
            "client_key": client_key,
            "script": script,
            "status": status,
            "twilio_sid": twilio_sid,
        }
        storage.Client().bucket(WM_TRANSCRIPT_BKT).blob(
            f"_history/{client_key}_voicenote.json"
        ).upload_from_string(_json.dumps(record), content_type="application/json")
        log.info("Voice note history written for %s / %s", client_key, note_id)
    except Exception as e:
        log.warning("Voice note history GCS write failed for %s: %s", client_key, e)


@app.post("/api/wealth/call/initiate")
async def wealth_call_initiate(request: Request):
    """Initiate a real Gemini Voice Call for an HNW wealth management client."""
    body       = await request.json()
    client_id  = body.get("client_id", "")
    client_name = body.get("client_name", "")
    mobile     = body.get("mobile", "")
    channel    = body.get("channel", "voice")
    demo_mobile_ui = body.get("demo_mobile", "")

    if channel != "voice":
        return JSONResponse({"error": "Only voice channel supported for real calls"}, status_code=400)

    if client_id not in _WEALTH_INSTRUCTIONS:
        return JSONResponse({"error": f"Unknown client_id: {client_id}"}, status_code=400)

    call_id    = f"WM-{uuid.uuid4().hex[:8].upper()}"
    system_instr = _WEALTH_INSTRUCTIONS[client_id]
    dial_to    = demo_mobile_ui or DEMO_MOBILE or mobile
    language   = "ta-IN" if client_id == "vikram_nair" else "en-IN"

    # Step 1: Prepare broker with client context and system instruction
    try:
        import httpx as _httpx
        r = _httpx.post(f"{BROKER_URL}/calls/{call_id}/prepare", json={
            "system_instruction": system_instr,
            "client_name":        client_name,
            "rm_name":            "Rohan Sharma",
            "language":           language,
        }, timeout=15)
        r.raise_for_status()
    except Exception as exc:
        log.error("Broker /prepare failed for %s: %s", call_id, exc)
        return JSONResponse({"error": f"Broker prepare failed: {exc}"}, status_code=502)

    # Step 2: Place Twilio outbound call pointing at broker WebSocket
    if not TWILIO_SID:
        return JSONResponse({
            "call_id": call_id,
            "status": "simulated",
            "note": "Set TWILIO_ACCOUNT_SID env var to place real calls. Broker prepared.",
            "dial_to": dial_to,
        })

    try:
        import httpx as _httpx
        r = _httpx.post(
            f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Calls.json",
            auth=(TWILIO_SID, TWILIO_TOKEN),
            data={
                "To":                   dial_to,
                "From":                 TWILIO_FROM,
                "Url":                  f"{BROKER_URL}/twilio/voice/{call_id}",
                "Method":               "POST",
                "StatusCallback":       f"{BROKER_URL}/twilio/status/{call_id}",
                "StatusCallbackMethod": "POST",
            },
            timeout=10,
        )
        r.raise_for_status()
        twilio_sid = r.json().get("sid", "")
    except Exception as exc:
        log.error("Twilio call failed for %s: %s", call_id, exc)
        return JSONResponse({"error": f"Twilio dial failed: {exc}"}, status_code=502)

    _WM_CALL_SIDS[call_id] = {"twilio_sid": twilio_sid, "client_name": client_name, "client_id": client_id}
    log.info("Wealth call initiated: %s → %s (Twilio %s)", call_id, dial_to, twilio_sid)
    return {
        "call_id":    call_id,
        "status":     "initiated",
        "twilio_sid": twilio_sid,
        "dial_to":    dial_to[-4:],  # last 4 digits only (security)
        "gcs_path":   f"gs://{WM_TRANSCRIPT_BKT}/{call_id}.json",
    }


def _write_client_history(client_key: str, call_id: str, gcs_path: str, actions: list) -> None:
    """Persist per-client latest-call record to GCS so it survives cold starts."""
    if not client_key:
        return
    try:
        import json as _json
        from google.cloud import storage
        record = {"call_id": call_id, "client_key": client_key, "gcs_path": gcs_path, "actions": actions}
        storage.Client().bucket(WM_TRANSCRIPT_BKT).blob(
            f"_history/{client_key}.json"
        ).upload_from_string(_json.dumps(record), content_type="application/json")
        log.info("History written to GCS for %s / %s", client_key, call_id)
    except Exception as e:
        log.warning("History GCS write failed for %s: %s", client_key, e)


@app.get("/api/wealth/call/transcript/{call_id}")
async def wealth_call_transcript(call_id: str):
    """Proxy to broker transcript. Checks Twilio status when broker returns 404."""
    import httpx as _httpx
    gcs_path_str = f"gs://{WM_TRANSCRIPT_BKT}/{call_id}.json"

    def _gcs_check():
        try:
            from google.cloud import storage
            import json as _json
            blob = storage.Client().bucket(WM_TRANSCRIPT_BKT).blob(f"{call_id}.json")
            if blob.exists():
                return True, _json.loads(blob.download_as_text())
        except Exception:
            pass
        return False, []

    def _twilio_status(twilio_sid: str) -> str:
        """Returns Twilio call status string, or 'unknown' on error."""
        try:
            r = _httpx.get(
                f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Calls/{twilio_sid}.json",
                auth=(TWILIO_SID, TWILIO_TOKEN), timeout=4,
            )
            return r.json().get("status", "unknown")
        except Exception:
            return "unknown"

    try:
        r = _httpx.get(f"{BROKER_URL}/calls/{call_id}/transcript", timeout=5)
    except Exception as exc:
        log.warning("Transcript poll network error for %s: %s", call_id, exc)
        return {"call_id": call_id, "segments": [], "live": True, "call_status": "ringing"}

    gcs_found, gcs_segs = _gcs_check()

    if r.status_code == 404:
        if gcs_found:
            meta = _WM_CALL_SIDS.get(call_id, {})
            client_key = meta.get("client_id", "")
            if client_key and call_id not in _WM_CALL_ACTIONS:
                # Write a basic history stub (no actions yet); actions endpoint will overwrite
                import threading
                threading.Thread(
                    target=_write_client_history,
                    args=(client_key, call_id, gcs_path_str, []),
                    daemon=True,
                ).start()
            return {"call_id": call_id, "segments": gcs_segs, "live": False, "gcs_path": gcs_path_str}

        # Broker has no record — check Twilio to distinguish ringing vs failed
        meta = _WM_CALL_SIDS.get(call_id, {})
        twilio_sid = meta.get("twilio_sid", "")
        t_status = _twilio_status(twilio_sid) if twilio_sid else "unknown"
        log.info("Transcript 404 for %s → Twilio status=%s", call_id, t_status)

        if t_status in _TWILIO_TERMINAL:
            return {"call_id": call_id, "segments": [], "live": False,
                    "call_status": t_status,
                    "error": f"Call ended: {t_status.replace('-', ' ')}"}
        # Still ringing / in-progress
        return {"call_id": call_id, "segments": [], "live": True, "call_status": t_status}

    segments = r.json() if isinstance(r.json(), list) else []

    if gcs_found:
        return {"call_id": call_id, "segments": gcs_segs, "live": False, "gcs_path": gcs_path_str}

    return {"call_id": call_id, "segments": segments, "live": True, "call_status": "in-progress"}


_ACTION_PROMPT = """You are an AI assistant extracting client action items from a wealth management call transcript.

Client: {client_name}
RM: {rm_name}

TRANSCRIPT:
{transcript}

Extract ALL action items — meetings to schedule, documents to send, calls to arrange, follow-up tasks.
Include items the client explicitly requested AND items the RM committed to doing.

Return a JSON array only. Each object must have exactly these keys:
- "title": short imperative (max 8 words)
- "description": what was said verbatim or close paraphrase (max 30 words)
- "priority": "HIGH", "MEDIUM", or "LOW"
- "due": one of "Today", "Tomorrow", "This week", "Within 30 days"
- "assignee": always "Rohan Sharma"
- "category": one of "Meeting", "Document", "Follow-up", "Compliance", "Investment"

Return ONLY the JSON array with no markdown wrapping."""


@app.post("/api/wealth/call/{call_id}/actions")
async def wealth_call_actions(call_id: str):
    """Extract action items from transcript via Gemini, sync to CRM stub."""
    import json as _json

    if call_id in _WM_CALL_ACTIONS:
        return {"call_id": call_id, "actions": _WM_CALL_ACTIONS[call_id], "crm_sync": "cached"}

    # --- Load transcript (GCS first, broker fallback) ---
    segments: list = []
    try:
        from google.cloud import storage
        blob = storage.Client().bucket(WM_TRANSCRIPT_BKT).blob(f"{call_id}.json")
        if blob.exists():
            segments = _json.loads(blob.download_as_text())
    except Exception as e:
        log.warning("GCS read for actions %s: %s", call_id, e)

    if not segments:
        try:
            import httpx as _httpx
            r = _httpx.get(f"{BROKER_URL}/calls/{call_id}/transcript", timeout=5)
            if r.status_code == 200 and isinstance(r.json(), list):
                segments = r.json()
        except Exception as e:
            log.warning("Broker transcript for actions %s: %s", call_id, e)

    if not segments:
        return JSONResponse({"error": "No transcript available yet"}, status_code=404)

    # --- Build flat transcript text ---
    transcript_text = "\n".join(
        f"{str(s.get('speaker','?')).upper()}: {s.get('text','')}"
        for s in segments
    )

    meta = _WM_CALL_SIDS.get(call_id, {})
    client_name = meta.get("client_name") or "Client"

    # --- Gemini extraction ---
    actions_list: list = []
    try:
        from google import genai
        from google.genai.types import GenerateContentConfig
        ai_client = genai.Client(vertexai=True, project=BQ_PROJECT, location="global")
        resp = ai_client.models.generate_content(
            model="gemini-3.5-flash",
            contents=_ACTION_PROMPT.format(
                client_name=client_name,
                rm_name="Rohan Sharma",
                transcript=transcript_text,
            ),
            config=GenerateContentConfig(response_mime_type="application/json"),
        )
        parsed = _json.loads(resp.text.strip())
        actions_list = parsed if isinstance(parsed, list) else []
    except Exception as exc:
        log.error("Gemini action extraction for %s: %s", call_id, exc)
        actions_list = [{
            "title": "Review call transcript",
            "description": "AI extraction unavailable — please review transcript manually",
            "priority": "HIGH",
            "due": "Today",
            "assignee": "Rohan Sharma",
            "category": "Follow-up",
        }]

    # --- Assign CRM IDs and push to stub ---
    for act in actions_list:
        crm_id = f"SF-{uuid.uuid4().hex[:6].upper()}"
        act["crm_id"] = crm_id
        _WM_CRM_ACTIONS.append({
            "crm_id":      crm_id,
            "call_id":     call_id,
            "title":       act.get("title", ""),
            "priority":    act.get("priority", "MEDIUM"),
            "due":         act.get("due", "Today"),
            "assignee":    act.get("assignee", "Rohan Sharma"),
            "category":    act.get("category", "Follow-up"),
            "description": act.get("description", ""),
            "status":      "open",
        })

    _WM_CALL_ACTIONS[call_id] = actions_list
    log.info("Actions extracted for %s: %d items synced to CRM", call_id, len(actions_list))

    # Persist full history record to GCS (overwrites the stub written by transcript endpoint)
    client_key = meta.get("client_id", "")
    import threading
    threading.Thread(
        target=_write_client_history,
        args=(client_key, call_id, f"gs://{WM_TRANSCRIPT_BKT}/{call_id}.json", actions_list),
        daemon=True,
    ).start()

    return {
        "call_id":  call_id,
        "actions":  actions_list,
        "crm_sync": "https://cymbal.my.salesforce.com/tasks/",
    }


@app.post("/api/wealth/whatsapp/voice-note/generate")
async def wealth_whatsapp_vn_generate(request: Request):
    """Step 1 — TTS only. Returns note_id + audio_url for in-browser preview. Does NOT send via WhatsApp."""
    import threading
    body = await request.json()
    client_key  = body.get("client_key", "")
    client_name = body.get("client_name", client_key.replace("_", " ").title())

    if client_key not in _WM_VOICE_NOTE_SCRIPTS:
        return JSONResponse({"error": f"No voice script for {client_key}"}, status_code=404)

    script  = _WM_VOICE_NOTE_SCRIPTS[client_key]
    note_id = f"VN-{uuid.uuid4().hex[:8].upper()}"

    audio_bytes: bytes | None = None
    try:
        from google import genai as _genai
        from google.genai.types import GenerateContentConfig, SpeechConfig, VoiceConfig, PrebuiltVoiceConfig
        tts_client = _genai.Client(vertexai=True, project=BQ_PROJECT, location="us-central1")
        tts_resp = tts_client.models.generate_content(
            model="gemini-3.1-flash-tts-preview",
            contents=script,
            config=GenerateContentConfig(
                response_modalities=["AUDIO"],
                speech_config=SpeechConfig(
                    language_code="en-IN",
                    voice_config=VoiceConfig(
                        prebuilt_voice_config=PrebuiltVoiceConfig(voice_name="Autonoe")
                    )
                ),
            ),
        )
        pcm_bytes   = tts_resp.candidates[0].content.parts[0].inline_data.data
        audio_bytes = _pcm_to_ogg(pcm_bytes, sample_rate=24000)
        log.info("TTS preview note %s: %d bytes OGG", note_id, len(audio_bytes))
    except Exception as exc:
        log.warning("TTS failed for note %s: %s", note_id, exc)

    if audio_bytes:
        _upload_voice_note_gcs(note_id, audio_bytes)

    _WM_VOICE_NOTES[note_id] = {
        "note_id":     note_id,
        "client_key":  client_key,
        "client_name": client_name,
        "script":      script,
        "status":      "preview",
    }

    scheme    = request.headers.get("x-forwarded-proto", "https")
    host      = request.headers.get("host", DASHBOARD_URL.replace("https://", ""))
    audio_url = f"{scheme}://{host}/api/wealth/voice-note/{note_id}/audio" if audio_bytes else None

    return {
        "note_id":     note_id,
        "script":      script,
        "has_audio":   audio_bytes is not None,
        "audio_url":   audio_url,
        "client_name": client_name,
    }


@app.post("/api/wealth/whatsapp/voice-note/send")
async def wealth_whatsapp_vn_send(request: Request):
    """Step 2 — Send a previously generated voice note via Twilio WhatsApp."""
    import threading
    body    = await request.json()
    note_id = body.get("note_id", "")
    demo_mobile_ui = body.get("demo_mobile", "")

    note = _WM_VOICE_NOTES.get(note_id)
    if not note:
        return JSONResponse({"error": "Note not found — regenerate first"}, status_code=404)

    client_key  = note.get("client_key", body.get("client_key", ""))
    client_name = note.get("client_name", "")
    script      = note.get("script", "")

    scheme    = request.headers.get("x-forwarded-proto", "https")
    host      = request.headers.get("host", DASHBOARD_URL.replace("https://", ""))
    audio_url = f"{scheme}://{host}/api/wealth/voice-note/{note_id}/audio"

    twilio_sid_val = ""
    wa_status = "simulated"

    if TWILIO_SID:
        to_mobile = demo_mobile_ui or DEMO_MOBILE or "+919999999999"
        wa_to  = f"whatsapp:{to_mobile}"  if not to_mobile.startswith("whatsapp:")  else to_mobile
        wa_from = TWILIO_WHATSAPP_FROM if TWILIO_WHATSAPP_FROM.startswith("whatsapp:") else f"whatsapp:{TWILIO_WHATSAPP_FROM}"
        try:
            import httpx as _httpx
            r = _httpx.post(
                f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json",
                auth=(TWILIO_SID, TWILIO_TOKEN),
                data={
                    "From":     wa_from,
                    "To":       wa_to,
                    "MediaUrl": audio_url,
                    "Body":     f"Voice note from your Cymbal Wealth advisor (Rohan Sharma) regarding {client_name}",
                },
                timeout=15,
            )
            r.raise_for_status()
            twilio_sid_val = r.json().get("sid", "")
            wa_status = "sent"
            log.info("WhatsApp voice note sent: %s → %s (Twilio %s)", note_id, wa_to, twilio_sid_val)
        except Exception as exc:
            log.error("Twilio send failed for note %s: %s", note_id, exc)
            wa_status = "error"

    _WM_VOICE_NOTES[note_id]["status"]     = wa_status
    _WM_VOICE_NOTES[note_id]["twilio_sid"] = twilio_sid_val

    threading.Thread(
        target=_write_voice_note_history,
        args=(client_key, note_id, script, wa_status, twilio_sid_val),
        daemon=True,
    ).start()

    return {
        "note_id":     note_id,
        "status":      wa_status,
        "twilio_sid":  twilio_sid_val,
        "script":      script,
        "client_name": client_name,
    }


@app.post("/api/wealth/whatsapp/voice-note")
async def wealth_whatsapp_voice_note(request: Request):
    """Generate a Gemini TTS voice note and deliver via Twilio WhatsApp."""
    import json as _json
    import threading
    from fastapi.responses import Response as _FRsp

    body = await request.json()
    client_key = body.get("client_key", "")
    client_name = body.get("client_name", client_key.replace("_", " ").title())
    demo_mobile_ui = body.get("demo_mobile", "")

    if client_key not in _WM_VOICE_NOTE_SCRIPTS:
        return JSONResponse({"error": f"No voice note script for client: {client_key}"}, status_code=404)

    script = _WM_VOICE_NOTE_SCRIPTS[client_key]
    note_id = f"VN-{uuid.uuid4().hex[:8].upper()}"

    # --- TTS via Gemini 3.1 Flash TTS ---
    audio_bytes: bytes | None = None
    try:
        from google import genai as _genai
        from google.genai.types import GenerateContentConfig, SpeechConfig, VoiceConfig, PrebuiltVoiceConfig
        tts_client = _genai.Client(vertexai=True, project=BQ_PROJECT, location="us-central1")
        tts_resp = tts_client.models.generate_content(
            model="gemini-3.1-flash-tts-preview",
            contents=script,
            config=GenerateContentConfig(
                response_modalities=["AUDIO"],
                speech_config=SpeechConfig(
                    language_code="en-IN",
                    voice_config=VoiceConfig(
                        prebuilt_voice_config=PrebuiltVoiceConfig(voice_name="Autonoe")
                    )
                ),
            ),
        )
        pcm_bytes = tts_resp.candidates[0].content.parts[0].inline_data.data
        audio_bytes = _pcm_to_ogg(pcm_bytes, sample_rate=24000)
        log.info("TTS generated for note %s: %d bytes PCM → OGG", note_id, len(audio_bytes))
    except Exception as exc:
        log.warning("TTS failed for note %s: %s — sending text fallback", note_id, exc)

    # Upload OGG to GCS so the audio URL works across any Cloud Run instance
    if audio_bytes:
        _upload_voice_note_gcs(note_id, audio_bytes)

    # Store note metadata (no audio bytes in memory — GCS is source of truth)
    _WM_VOICE_NOTES[note_id] = {
        "note_id": note_id,
        "client_key": client_key,
        "client_name": client_name,
        "script": script,
        "status": "generated",
    }

    # --- Send via Twilio WhatsApp ---
    twilio_sid_val = ""
    wa_status = "simulated"
    scheme = request.headers.get("x-forwarded-proto", "https")
    host = request.headers.get("host", DASHBOARD_URL.replace("https://", ""))
    base_url = f"{scheme}://{host}"
    audio_url = f"{base_url}/api/wealth/voice-note/{note_id}/audio"

    if TWILIO_SID and audio_bytes:
        to_mobile = demo_mobile_ui or DEMO_MOBILE or "+919999999999"
        wa_to = f"whatsapp:{to_mobile}" if not to_mobile.startswith("whatsapp:") else to_mobile
        wa_from = TWILIO_WHATSAPP_FROM if TWILIO_WHATSAPP_FROM.startswith("whatsapp:") else f"whatsapp:{TWILIO_WHATSAPP_FROM}"
        try:
            import httpx as _httpx
            r = _httpx.post(
                f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json",
                auth=(TWILIO_SID, TWILIO_TOKEN),
                data={
                    "From":     wa_from,
                    "To":       wa_to,
                    "MediaUrl": audio_url,
                    "Body":     f"Voice note from your Cymbal Wealth advisor (Rohan Sharma) regarding {client_name}",
                },
                timeout=15,
            )
            r.raise_for_status()
            twilio_sid_val = r.json().get("sid", "")
            wa_status = "sent"
            log.info("WhatsApp voice note sent: %s → %s (Twilio %s)", note_id, wa_to, twilio_sid_val)
        except Exception as exc:
            log.error("Twilio WhatsApp voice note failed: %s", exc)
            wa_status = "error"
    elif TWILIO_SID and not audio_bytes:
        # TTS failed — send script as text WhatsApp message fallback
        to_mobile = demo_mobile_ui or DEMO_MOBILE or "+919999999999"
        wa_to = f"whatsapp:{to_mobile}" if not to_mobile.startswith("whatsapp:") else to_mobile
        wa_from = TWILIO_WHATSAPP_FROM if TWILIO_WHATSAPP_FROM.startswith("whatsapp:") else f"whatsapp:{TWILIO_WHATSAPP_FROM}"
        try:
            import httpx as _httpx
            r = _httpx.post(
                f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json",
                auth=(TWILIO_SID, TWILIO_TOKEN),
                data={"From": wa_from, "To": wa_to, "Body": f"[Voice Note Script]\n\n{script}"},
                timeout=15,
            )
            r.raise_for_status()
            twilio_sid_val = r.json().get("sid", "")
            wa_status = "sent_text_fallback"
        except Exception as exc:
            log.error("WhatsApp text fallback failed: %s", exc)
            wa_status = "error"

    _WM_VOICE_NOTES[note_id]["status"] = wa_status
    _WM_VOICE_NOTES[note_id]["twilio_sid"] = twilio_sid_val
    _WM_VOICE_NOTES[note_id]["audio_url"] = audio_url if audio_bytes else None

    # Persist history
    threading.Thread(
        target=_write_voice_note_history,
        args=(client_key, note_id, script, wa_status, twilio_sid_val),
        daemon=True,
    ).start()

    return {
        "note_id":    note_id,
        "status":     wa_status,
        "has_audio":  audio_bytes is not None,
        "audio_url":  audio_url if audio_bytes else None,
        "twilio_sid": twilio_sid_val,
        "script":     script,
        "client_name": client_name,
    }


@app.api_route("/api/wealth/voice-note/{note_id}/audio", methods=["GET", "HEAD"])
async def wealth_voice_note_audio(note_id: str, request: Request):
    """Serve OGG/Opus audio for a generated voice note.
    Reads from GCS so any Cloud Run instance can serve it (fixes Twilio error 63019).
    Supports HEAD so Twilio can validate the URL before fetching (avoids silent drops).
    """
    from fastapi.responses import Response as _FRsp
    from google.cloud import storage as _gcs
    try:
        gcs_client = _gcs.Client()
        blob = gcs_client.bucket(WM_TRANSCRIPT_BKT).blob(f"voice-notes/{note_id}.ogg")
        if not blob.exists():
            return JSONResponse({"error": "Audio not found"}, status_code=404)
        if request.method == "HEAD":
            blob.reload()
            return _FRsp(status_code=200, headers={
                "Content-Type": "audio/ogg",
                "Content-Length": str(blob.size),
                "Accept-Ranges": "bytes",
            })
        audio = blob.download_as_bytes()
        return _FRsp(content=audio, media_type="audio/ogg", headers={"Content-Type": "audio/ogg"})
    except Exception as exc:
        log.error("Voice note audio serve error for %s: %s", note_id, exc)
        return JSONResponse({"error": "Audio unavailable"}, status_code=404)


@app.post("/api/wealth/crm/actions")
async def wealth_crm_push(request: Request):
    """Salesforce stub — accept external action pushes (used by tests/integrations)."""
    body = await request.json()
    call_id = body.get("call_id", "")
    incoming = body.get("actions", [])
    ids = []
    for act in incoming:
        crm_id = f"SF-{uuid.uuid4().hex[:6].upper()}"
        ids.append(crm_id)
        _WM_CRM_ACTIONS.append({
            "crm_id":      crm_id,
            "call_id":     call_id,
            "title":       act.get("title", ""),
            "priority":    act.get("priority", "MEDIUM"),
            "due":         act.get("due", "Today"),
            "assignee":    act.get("assignee", "Rohan Sharma"),
            "category":    act.get("category", "Follow-up"),
            "description": act.get("description", ""),
            "status":      "open",
        })
    return {
        "status":     "synced",
        "action_ids": ids,
        "crm_url":    "https://cymbal.my.salesforce.com/tasks/",
        "total":      len(_WM_CRM_ACTIONS),
    }


@app.get("/api/wealth/crm/actions")
def wealth_crm_list():
    """Return RM's full CRM task queue."""
    return {"actions": _WM_CRM_ACTIONS, "total": len(_WM_CRM_ACTIONS)}


@app.get("/api/wealth/client/{client_key}/history")
async def wealth_client_history(client_key: str):
    """Return the most recent completed call for a client. GCS-first, RAM fallback."""
    import json as _json
    from google.cloud import storage

    gcs_client = storage.Client()

    # ── 1. GCS history file (survives cold starts) ────────────────────────────
    try:
        hist_blob = gcs_client.bucket(WM_TRANSCRIPT_BKT).blob(f"_history/{client_key}.json")
        if hist_blob.exists():
            record = _json.loads(hist_blob.download_as_text())
            call_id  = record["call_id"]
            gcs_path = record.get("gcs_path") or f"gs://{WM_TRANSCRIPT_BKT}/{call_id}.json"
            actions  = record.get("actions") or _WM_CALL_ACTIONS.get(call_id, [])

            # Load transcript
            segments = []
            try:
                tx_blob = gcs_client.bucket(WM_TRANSCRIPT_BKT).blob(f"{call_id}.json")
                if tx_blob.exists():
                    segments = _json.loads(tx_blob.download_as_text())
            except Exception:
                pass

            # Load voice note history (separate file)
            voice_note = None
            try:
                vn_blob = gcs_client.bucket(WM_TRANSCRIPT_BKT).blob(f"_history/{client_key}_voicenote.json")
                if vn_blob.exists():
                    voice_note = _json.loads(vn_blob.download_as_text())
            except Exception:
                pass

            return {
                "found": True,
                "call_id": call_id,
                "gcs_path": gcs_path,
                "segments": segments,
                "actions": actions,
                "completed": True,
                "voice_note": voice_note,
                "source": "gcs",
            }
    except Exception as e:
        log.warning("History GCS read for %s: %s", client_key, e)

    # ── 2. In-memory fallback (same-instance, non-cold-start) ─────────────────
    matching = [
        (cid, meta) for cid, meta in _WM_CALL_SIDS.items()
        if meta.get("client_id") == client_key
    ]
    if not matching:
        return {"found": False}

    call_id, _ = matching[-1]
    gcs_path = f"gs://{WM_TRANSCRIPT_BKT}/{call_id}.json"
    segments, gcs_found = [], False
    try:
        blob = gcs_client.bucket(WM_TRANSCRIPT_BKT).blob(f"{call_id}.json")
        if blob.exists():
            segments = _json.loads(blob.download_as_text())
            gcs_found = True
    except Exception:
        pass

    if not segments:
        try:
            import httpx as _httpx
            r = _httpx.get(f"{BROKER_URL}/calls/{call_id}/transcript", timeout=4)
            if r.status_code == 200 and isinstance(r.json(), list):
                segments = r.json()
        except Exception:
            pass

    actions = _WM_CALL_ACTIONS.get(call_id, [])
    # Check in-memory voice note for this client
    vn_ram = next(
        (v for v in _WM_VOICE_NOTES.values() if v.get("client_key") == client_key),
        None,
    )
    voice_note_ram = {
        "note_id": vn_ram["note_id"],
        "script": vn_ram["script"],
        "status": vn_ram.get("status"),
        "twilio_sid": vn_ram.get("twilio_sid", ""),
    } if vn_ram else None
    return {
        "found": True,
        "call_id": call_id,
        "gcs_path": gcs_path if gcs_found else None,
        "segments": segments,
        "actions": actions,
        "completed": gcs_found or bool(actions),
        "voice_note": voice_note_ram,
        "source": "ram",
    }


# ─── GEMINI ENTERPRISE HUB (Agentspace) ──────────────────────────────────────
_AGENTSPACE_PROJECT  = "butterfly-987"
_AGENTSPACE_LOCATION = "global"
_AGENTSPACE_ENGINE   = "agentspace_1751710232953"
_AGENTSPACE_BASE     = (
    f"https://discoveryengine.googleapis.com/v1alpha"
    f"/projects/{_AGENTSPACE_PROJECT}/locations/{_AGENTSPACE_LOCATION}"
    f"/collections/default_collection/engines/{_AGENTSPACE_ENGINE}"
)

# session_id → Agentspace session resource name
_AS_SESSIONS: dict[str, str] = {}


async def _as_token() -> str:
    """Return a short-lived Bearer token via ADC (Application Default Credentials)."""
    import google.auth
    import google.auth.transport.requests as _tr
    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    req = _tr.Request()
    creds.refresh(req)
    return creds.token


@app.post("/api/agentspace/chat")
async def agentspace_chat(request: Request):
    import httpx, json as _json

    body = await request.json()
    query: str = body.get("query", "").strip()
    session_id: str | None = body.get("session_id")
    ctx: dict | None = body.get("client_context")

    if not query:
        return JSONResponse({"error": "Empty query"}, status_code=400)

    # Inject client context into the query when available
    if ctx:
        preamble = (
            f"[Context: Wealth RM Rohan Sharma is advising client {ctx.get('name','')} "
            f"(AUM: {ctx.get('aum','')}, Alert: {ctx.get('alert','')})] "
        )
        full_query = preamble + query
    else:
        full_query = query

    try:
        token = await _as_token()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        async with httpx.AsyncClient(timeout=30) as hc:
            # ── 1. Create session on first turn ─────────────────────────────────
            if not session_id or session_id not in _AS_SESSIONS:
                sess_resp = await hc.post(
                    f"{_AGENTSPACE_BASE}/sessions",
                    headers=headers,
                    json={"userPseudoId": session_id or "rohan-sharma-demo"},
                )
                if sess_resp.status_code not in (200, 201):
                    return JSONResponse({"error": f"Session create failed: {sess_resp.text}"}, status_code=502)
                sess_name = sess_resp.json().get("name", "")
                new_sid = sess_name.split("/sessions/")[-1]
                _AS_SESSIONS[new_sid] = sess_name
                session_id = new_sid
            else:
                sess_name = _AS_SESSIONS[session_id]

            # ── 2. Send query ────────────────────────────────────────────────────
            q_resp = await hc.post(
                f"{_AGENTSPACE_BASE}/sessions/{session_id}/queries:answer",
                headers=headers,
                json={
                    "query": {"text": full_query},
                    "session": sess_name,
                    "queryUnderstandingSpec": {"queryRephraserSpec": {"disable": False}},
                    "answerGenerationSpec": {
                        "modelSpec": {"modelVersion": "stable"},
                        "groundingSpec": {"includeGroundingSupports": True},
                    },
                },
            )

        if q_resp.status_code != 200:
            return JSONResponse({"error": f"Query failed: {q_resp.text}"}, status_code=502)

        resp_body = q_resp.json()
        answer_obj = resp_body.get("answer", {})
        answer_text = answer_obj.get("answer", "") or answer_obj.get("answerText", "")

        # Extract citation titles as references
        refs: list[str] = []
        for support in answer_obj.get("groundingSupports", []):
            for ref in support.get("groundingChunkIndices", []):
                chunks = answer_obj.get("groundingChunks", [])
                if ref < len(chunks):
                    title = chunks[ref].get("retrievedContext", {}).get("title", "")
                    if title and title not in refs:
                        refs.append(title)

        return JSONResponse({
            "answer": answer_text or "I found relevant information but couldn't generate a concise answer. Please try rephrasing.",
            "session_id": session_id,
            "references": refs[:4],
        })

    except Exception as exc:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(exc)}, status_code=500)


# ── Video Meet Link ───────────────────────────────────────────────────────────

@app.post("/api/wealth/video-meet/send")
async def wealth_video_meet_send(request: Request):
    """
    Generate a Jitsi Meet room URL and send it to the client via WhatsApp.
    Returns the same meet_url so the RM can open it on their side.
    """
    import uuid as _uuid
    body        = await request.json()
    client_key  = body.get("client_key", "")
    client_name = body.get("client_name", client_key.replace("_", " ").title())
    mobile      = body.get("mobile", "")
    demo_mobile_ui = body.get("demo_mobile", "")

    room_id  = f"CymbalWealth-{_uuid.uuid4().hex[:8].upper()}"
    meet_url = f"https://meet.jit.si/{room_id}"

    message = (
        f"🎥 *Cymbal Wealth · Video Meeting*\n\n"
        f"Namaste *{client_name}* ji!\n\n"
        f"Your advisor Rohan Sharma has invited you to a quick video meeting.\n\n"
        f"👉 *Join here:* {meet_url}\n\n"
        f"No app download needed — opens directly in your browser.\n"
        f"_Sent via Cymbal Wealth Management_"
    )

    twilio_sid_val = ""
    wa_status = "simulated"

    if TWILIO_SID:
        dial_to = demo_mobile_ui or DEMO_MOBILE or mobile
        wa_to   = f"whatsapp:{dial_to}"   if not dial_to.startswith("whatsapp:")   else dial_to
        wa_from = TWILIO_WHATSAPP_FROM if TWILIO_WHATSAPP_FROM.startswith("whatsapp:") else f"whatsapp:{TWILIO_WHATSAPP_FROM}"
        try:
            import httpx as _httpx
            r = _httpx.post(
                f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json",
                auth=(TWILIO_SID, TWILIO_TOKEN),
                data={"From": wa_from, "To": wa_to, "Body": message},
                timeout=15,
            )
            r.raise_for_status()
            twilio_sid_val = r.json().get("sid", "")
            wa_status = "sent"
            log.info("Video meet link sent to %s: %s (Twilio %s)", wa_to, meet_url, twilio_sid_val)
        except Exception as exc:
            log.error("Twilio WhatsApp send failed for video meet: %s", exc)
            return JSONResponse({"error": str(exc)}, status_code=500)

    return {
        "status":      wa_status,
        "meet_url":    meet_url,
        "twilio_sid":  twilio_sid_val,
        "client_name": client_name,
        "note": "Twilio credentials not set — simulated" if wa_status == "simulated" else "",
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("DASHBOARD_PORT", 8020))
    uvicorn.run("dashboard.server:app", host="0.0.0.0", port=port, reload=True)
