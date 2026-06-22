"""
SBI MF Voice MCP — exposes call_engine functions as MCP tools.

Core logic lives in voice/call_engine.py so the orchestrator can import
functions directly without going through HTTP/MCP.

Run locally:  python voice/sbi_mf_voice_mcp.py
"""

import io
import logging
import os
import uuid
from datetime import datetime

import httpx
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

from voice.call_engine import (
    SUPPORTED_LANGUAGES,
    AGENDAS,
    make_call,
    get_call,
    get_recent_calls_by_arn,
    record_outcome,
    resolve_language,
)

log = logging.getLogger("sbi_mf_voice_mcp")

BQ_PROJECT           = os.getenv("GCP_PROJECT",         "your-project")
BQ_DATASET           = os.getenv("BQ_DATASET",          "sbi_mf_poc")
GCS_BUCKET           = os.getenv("GCS_BUCKET",          "sbi-mf-voice-notes")
TWILIO_ACCOUNT_SID   = os.getenv("TWILIO_ACCOUNT_SID",  "")
TWILIO_AUTH_TOKEN    = os.getenv("TWILIO_AUTH_TOKEN",   "")
TWILIO_WHATSAPP_FROM = os.getenv("TWILIO_WHATSAPP_FROM","whatsapp:+14155238886")

_no_dns_rebinding = TransportSecuritySettings(enable_dns_rebinding_protection=False)
mcp = FastMCP("sbi-mf-voice-mcp", transport_security=_no_dns_rebinding)

_pending_scripts: dict[str, dict] = {}
_latest_note_by_mobile: dict[str, str] = {}


@mcp.tool()
def list_supported_languages() -> dict:
    """List all Indian languages supported for SBI MF investor calls."""
    return {
        "supported_languages": [
            {"code": code, "name": info["name"]}
            for code, info in SUPPORTED_LANGUAGES.items()
        ],
    }


@mcp.tool()
def initiate_voice_call(
    investor_id: str,
    mobile: str,
    investor_name: str,
    call_type: str,
    script_variables: dict,
    distributor_name: str = "your advisor",
    distributor_arn: str = "",
    language: str = "hi-IN",
) -> dict:
    """
    Initiate an outbound SBI MF voice call to an investor.

    call_type: sip_renewal | fund_maturity | sip_debit_failure | sip_paused

    script_variables for sip_renewal: fund_name, monthly_amount, expiry_date
    script_variables for fund_maturity: fund_name, maturity_date
    script_variables for sip_debit_failure: fund_name, month
    script_variables for sip_paused: fund_name, pause_since
    """
    return make_call(
        investor_id=investor_id,
        mobile=mobile,
        investor_name=investor_name,
        call_type=call_type,
        script_variables=script_variables,
        distributor_name=distributor_name,
        distributor_arn=distributor_arn,
        language=language,
    )


@mcp.tool()
def get_call_status(call_id: str) -> dict:
    """Get status and transcript of a voice call."""
    return get_call(call_id)


@mcp.tool()
def get_recent_calls(distributor_arn: str = "", limit: int = 10) -> dict:
    """Get recent outbound calls, optionally filtered by distributor ARN."""
    calls = get_recent_calls_by_arn(distributor_arn, limit)
    return {"calls": calls, "total": len(calls)}


@mcp.tool()
def record_call_outcome(call_id: str, outcome: str, transcript: str = "") -> dict:
    """
    Record the outcome of a completed call.
    outcome: renewal_intent | callback_requested | not_interested |
             wrong_number | no_answer | query_raised | no_meaningful_conversation
    """
    return record_outcome(call_id, outcome, transcript)


# ── WhatsApp Voice Note tools ─────────────────────────────────────────────────

_VOICE_NOTE_TEMPLATES: dict[str, str] = {
    "sip_renewal": (
        "{greeting} {investor_name} ji! SBI Mutual Fund ki taraf se. "
        "Aapka {fund_name} SIP {expiry_date} expire ho raha hai. "
        "Renewal ke liye apne advisor {distributor_name} se ya SBI MF app se sampark karein. "
        "{closing}!"
    ),
    "fund_maturity": (
        "{greeting} {investor_name} ji! SBI Mutual Fund reminder — "
        "aapka {fund_name} {maturity_date} ko mature ho raha hai. "
        "Apne advisor {distributor_name} se guidance lein. {closing}!"
    ),
    "sip_debit_failure": (
        "{greeting} {investor_name} ji! SBI Mutual Fund — "
        "aapka {fund_name} SIP debit process nahi hua. "
        "Please bank balance check karein aur apne advisor {distributor_name} se sampark karein. "
        "{closing}!"
    ),
}


@mcp.tool()
def build_voice_note_script(
    note_type: str,
    investor_name: str,
    language: str = "hi-IN",
    script_variables: dict | None = None,
) -> dict:
    """Generate WhatsApp voice note script from template."""
    if note_type not in _VOICE_NOTE_TEMPLATES:
        return {"error": f"Unknown note_type '{note_type}'. Valid: {list(_VOICE_NOTE_TEMPLATES.keys())}"}

    resolved_lang = resolve_language(language)
    lang_info = SUPPORTED_LANGUAGES[resolved_lang]
    all_vars = {
        "investor_name": investor_name,
        "greeting":      lang_info["greeting"],
        "closing":       lang_info["closing"],
        **(script_variables or {}),
    }

    try:
        text = _VOICE_NOTE_TEMPLATES[note_type].format(**all_vars)
    except KeyError as exc:
        return {"error": f"Missing script variable: {exc}"}

    note_id = f"NOTE-{uuid.uuid4().hex[:8].upper()}"
    _pending_scripts[note_id] = {
        "message_text":  text,
        "note_type":     note_type,
        "language":      resolved_lang,
        "investor_name": investor_name,
    }
    _latest_note_by_mobile[investor_name.lower()] = note_id

    return {
        "note_id":       note_id,
        "note_type":     note_type,
        "language":      resolved_lang,
        "language_name": lang_info["name"],
        "message_text":  text,
        "instruction": (
            f"Show this script for approval. "
            f"When approved, pass note_id='{note_id}' to send_whatsapp_voice_note()."
        ),
    }


@mcp.tool()
def send_whatsapp_voice_note(
    mobile: str,
    investor_name: str,
    note_id: str = "",
    message_text: str = "",
    language: str = "hi-IN",
    investor_id: str = "",
    distributor_arn: str = "",
) -> dict:
    """Synthesize and deliver a WhatsApp voice note to an investor."""
    resolved_note_id = note_id
    if not resolved_note_id or resolved_note_id not in _pending_scripts:
        resolved_note_id = _latest_note_by_mobile.get(investor_name.lower(), "")

    if resolved_note_id and resolved_note_id in _pending_scripts:
        stored = _pending_scripts.pop(resolved_note_id)
        _latest_note_by_mobile.pop(investor_name.lower(), None)
        message_text = stored["message_text"]
        language = stored.get("language", language)
    elif not message_text:
        return {"error": "No stored script found. Call build_voice_note_script() first."}

    resolved_lang = resolve_language(language)
    lang_info = SUPPORTED_LANGUAGES[resolved_lang]
    mobile = mobile.strip().replace(" ", "")
    if not mobile.startswith("+"):
        mobile = f"+91{mobile}" if len(mobile) == 10 else f"+{mobile}"

    wa_note_id = f"WA-{uuid.uuid4().hex[:8].upper()}"
    audio_filename = f"{wa_note_id}.ogg"

    base = {
        "note_id":         wa_note_id,
        "investor_id":     investor_id,
        "distributor_arn": distributor_arn,
        "mobile":          mobile,
        "language":        resolved_lang,
        "language_name":   lang_info["name"],
        "message_preview": message_text[:150],
        "initiated_at":    datetime.utcnow().isoformat(),
    }

    if not TWILIO_ACCOUNT_SID:
        base["status"] = "simulated"
        base["note"] = f"PoC mode — {lang_info['name']} voice note to {investor_name} staged."
        return base

    try:
        audio_bytes = _tts_to_ogg(message_text, resolved_lang)
        public_url = _upload_audio_to_gcs(audio_bytes, audio_filename)
        twilio_sid = _send_twilio_whatsapp(mobile, public_url, lang_info["name"])
    except Exception as exc:
        return {**base, "status": "error", "error": str(exc)}

    base.update({
        "status":             "sent",
        "twilio_message_sid": twilio_sid,
        "audio_url":          public_url,
    })
    return base


def _tts_to_ogg(text: str, language_code: str) -> bytes:
    from google import genai
    from google.genai import types
    from pydub import AudioSegment

    client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))
    response = client.models.generate_content(
        model="gemini-3.1-flash-tts-preview",
        contents=text,
        config=types.GenerateContentConfig(
            response_modalities=["AUDIO"],
            speech_config=types.SpeechConfig(
                voice_config=types.VoiceConfig(
                    prebuilt_voice_config=types.PrebuiltVoiceConfig(voice_name="Autonoe")
                )
            ),
        ),
    )
    pcm_bytes = response.candidates[0].content.parts[0].inline_data.data
    audio = AudioSegment.from_raw(io.BytesIO(pcm_bytes), sample_width=2, frame_rate=24000, channels=1)
    ogg_buf = io.BytesIO()
    audio.export(ogg_buf, format="ogg", codec="libopus")
    return ogg_buf.getvalue()


def _upload_audio_to_gcs(audio_bytes: bytes, filename: str) -> str:
    from google.cloud import storage
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(f"voice-notes/{filename}")
    blob.upload_from_string(audio_bytes, content_type="audio/ogg")
    return blob.public_url


def _send_twilio_whatsapp(to_number: str, media_url: str, lang_name: str) -> str:
    resp = httpx.post(
        f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Messages.json",
        auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN),
        data={
            "From":     TWILIO_WHATSAPP_FROM,
            "To":       f"whatsapp:{to_number}",
            "MediaUrl": media_url,
            "Body":     f"Voice note from SBI Mutual Fund ({lang_name})",
        },
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json().get("sid", "")


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8005))
    uvicorn.run(mcp.streamable_http_app(), host="0.0.0.0", port=port)
