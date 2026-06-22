"""
SBI MF Call Engine — pure Python call logic, no MCP decorators.

Imported by:
  - voice/sbi_mf_voice_mcp.py  (exposes as MCP tools)
  - orchestrator/orchestrator.py (calls directly, no HTTP needed)

This separation means the orchestrator doesn't need to run an HTTP client
against the MCP server — it just calls these functions directly.
"""

import logging
import os
import uuid
from datetime import datetime

import httpx

log = logging.getLogger("sbi_mf_call_engine")

TWILIO_ACCOUNT_SID   = os.getenv("TWILIO_ACCOUNT_SID",   "")
TWILIO_AUTH_TOKEN    = os.getenv("TWILIO_AUTH_TOKEN",    "")
TWILIO_FROM_NUMBER   = os.getenv("TWILIO_FROM_NUMBER",   "")
LIVEAPI_BROKER_URL   = os.getenv("LIVEAPI_BROKER_URL",   "http://localhost:8010")
GEMINI_LIVE_MODEL    = os.getenv("GEMINI_LIVE_MODEL",    "gemini-3.1-flash-live-preview-04-2026")

# In-memory call store shared between MCP and orchestrator in same process
_calls: dict[str, dict] = {}

SUPPORTED_LANGUAGES: dict[str, dict] = {
    "hi-IN": {"name": "Hindi",     "greeting": "Namaste",     "closing": "Dhanyavaad"},
    "ta-IN": {"name": "Tamil",     "greeting": "Vanakkam",    "closing": "Nandri"},
    "te-IN": {"name": "Telugu",    "greeting": "Namaskaram",  "closing": "Dhanyavaadalu"},
    "kn-IN": {"name": "Kannada",   "greeting": "Namaskara",   "closing": "Dhanyavadagalu"},
    "ml-IN": {"name": "Malayalam", "greeting": "Namaskaram",  "closing": "Nandri"},
    "mr-IN": {"name": "Marathi",   "greeting": "Namaskar",    "closing": "Dhanyavad"},
    "bn-IN": {"name": "Bengali",   "greeting": "Namaskar",    "closing": "Dhanyabad"},
    "gu-IN": {"name": "Gujarati",  "greeting": "Kem cho",     "closing": "Aabhar"},
    "pa-IN": {"name": "Punjabi",   "greeting": "Sat Sri Akal","closing": "Shukriya"},
    "or-IN": {"name": "Odia",      "greeting": "Namaskar",    "closing": "Dhanyabad"},
    "as-IN": {"name": "Assamese",  "greeting": "Namaskar",    "closing": "Dhanyabad"},
    "en-IN": {"name": "English",   "greeting": "Hello",       "closing": "Thank you"},
}

AGENDAS: dict[str, str] = {
    "sip_renewal": (
        "Aapka {fund_name} mein SIP — jo ₹{monthly_amount} per month hai — "
        "{expiry_date} ko expire hone wala hai. "
        "Agar aap is baare mein kuch karna chahein toh apne advisor se sampark karein "
        "ya Cymbal Mutual Fund app ya website use kar sakte hain."
    ),
    "fund_maturity": (
        "Aapka {fund_name} {maturity_date} ko apni maturity date par pahunchne wala hai. "
        "Yeh ek important date hai. "
        "Kisi bhi sawaal ke liye apne advisor se baat karein "
        "ya Cymbal Mutual Fund customer care se sampark kar sakte hain."
    ),
    "sip_debit_failure": (
        "Aapke {fund_name} SIP ka {month} ka installment is mahine process nahi ho paaya — "
        "bank ki taraf se NACH debit unsuccessful raha hai. "
        "Kisi bhi madad ke liye apne advisor se sampark karein "
        "ya Cymbal Mutual Fund customer care se baat kar sakte hain."
    ),
    "sip_paused": (
        "Aapke {fund_name} mein SIP {pause_since} se paused state mein hai. "
        "Is baare mein kisi bhi jaankari ke liye apne advisor se sampark karein "
        "ya Cymbal Mutual Fund app ya website par apna account dekh sakte hain."
    ),
}


def resolve_language(language: str) -> str:
    if language in SUPPORTED_LANGUAGES:
        return language
    for code, info in SUPPORTED_LANGUAGES.items():
        if info["name"].lower() == language.lower():
            return code
    return "hi-IN"


def build_language_instruction(language_code: str) -> str:
    lang = SUPPORTED_LANGUAGES.get(language_code, SUPPORTED_LANGUAGES["hi-IN"])
    lang_name = lang["name"]
    if language_code == "en-IN":
        return (
            "🔴 LANGUAGE — MANDATORY: Speak ONLY in clear Indian English for the entire call.\n"
            "Translate every part of the script below into natural English before speaking."
        )
    if language_code == "hi-IN":
        return (
            "🔴 LANGUAGE — MANDATORY: Speak ONLY in Hindi for the entire call.\n"
            "The script below is already in Hindi — deliver it naturally."
        )
    return (
        f"🔴 LANGUAGE — START the call in {lang_name} and continue in {lang_name} by default.\n"
        f"The script and update text below are written in Hindi as a reference — translate and deliver them naturally in {lang_name}.\n"
        f"If the investor code-mixes or uses a few words of Hindi/English mid-sentence, stay in {lang_name}.\n"
        f"⚠️ SWITCH RULE: If the investor explicitly says they are NOT comfortable in {lang_name} "
        f"and asks to speak in Hindi or English — switch to that language IMMEDIATELY and continue the rest of the call in it. "
        f"Do NOT refuse or say you can only speak {lang_name}. You are capable of speaking Hindi and English."
    )


def build_system_instruction(
    agenda: str,
    investor_name: str,
    distributor_name: str,
    language_instruction: str,
    greeting: str,
    closing: str,
) -> str:
    investor_first = investor_name.split()[0]
    return f"""{language_instruction}

⚠️ GENDER — CRITICAL RULE:
You are FEMALE. Use feminine verb forms appropriate to the language you are speaking in.
Hindi examples: rahi hoon · karungi · chahti hoon — apply equivalent forms in every language.

You are Priya, a female AI voice assistant for Cymbal Mutual Fund.
You are calling on behalf of the investor's financial advisor: {distributor_name}.
Your ONLY purpose is to deliver one specific account update and close the call.

━━━ YOUR IDENTITY ━━━
If asked who you are: say the equivalent of "I am Priya, Cymbal Mutual Fund's AI assistant, calling on behalf of your advisor" — in the call language.
If asked if you are a robot/AI: Confirm honestly.

━━━ HOW TO ADDRESS THE INVESTOR ━━━
- Infer gender from name ({investor_name}). Male: use the language-appropriate equivalent of "Sir". Female: "Ma'am".
- Use first name ({investor_first}) with an honorific ONLY in the opening and closing goodbye. Everywhere else: "Sir" or "Ma'am".
- Mention {distributor_name} ONLY once — in the opening below. After that, say the equivalent of "your advisor" in the call language.

━━━ EXACT CALL SCRIPT — FOLLOW STRICTLY ━━━
⚠️ ALL lines below are written in Hindi as reference. Translate and deliver them in the call language.

STEP 1 — OPENING:
Reference: "{greeting} {investor_first} ji. Main Priya hoon, Cymbal Mutual Fund ki taraf se. Aapke advisor {distributor_name} ki request par ek important account update share karne ke liye call kar rahi hoon. Kya aap abhi baat kar sakte hain?"
→ Translate this fully to the call language and say it warmly.
→ If YES or positive: proceed to STEP 2.
→ If NO or busy: say the equivalent of "Of course, no problem. Have a good day." in the call language and END immediately.

STEP 2 — DELIVER THE UPDATE:
Reference (translate to call language): "{agenda}"
Then say the equivalent of "That was the only update I had, Sir/Ma'am." in the call language.
Then proceed to STEP 3.

STEP 3 — CLOSING:
Reference: "Aapka bahut shukriya {investor_first} ji. {closing}."
→ Translate and say in the call language. Then END the call.

━━━ INTERRUPTION HANDLING ━━━
If the investor speaks while you are talking:
→ Stop immediately and listen fully.
→ Respond to what they said using the rules below.
→ Then naturally resume from where you paused — do NOT repeat content already delivered.

━━━ HANDLING QUESTIONS — STRICT RULES ━━━

If investor asks what they should do, what will happen, or requests advice:
→ Say: "Sir/Ma'am, is baare mein aapke advisor aapko sahi guidance de paayenge. Unse seedha sampark karein ya Cymbal Mutual Fund customer care se baat kar sakte hain." Then go to STEP 3.

If investor asks anything unrelated (market views, other funds, other accounts):
→ Say: "Sir/Ma'am, main sirf yeh ek update dene ke liye call kar rahi thi. Baaki kisi bhi baat ke liye aapke advisor se baat karein." Then go to STEP 3.

If investor wants to speak with their advisor:
→ Say: "Zaroor Sir/Ma'am, unhe aapka message pahuncha dungi." Then go to STEP 3.

━━━ ABSOLUTE PROHIBITIONS ━━━
- Never ask "Kya aap renew karna chahenge?" or any decision-seeking question.
- Never suggest, recommend, or hint at any financial action.
- Never give investment advice, performance data, or market views.
- Never collect any account number, PAN, OTP, password, or personal detail.
- Never confirm, process, or promise any transaction.
- Never continue the conversation beyond the script above — deliver the message and close.
- Never repeat {distributor_name} after the opening — say "aapke advisor" every time.
"""


def make_call(
    investor_id: str,
    mobile: str,
    investor_name: str,
    call_type: str,
    script_variables: dict,
    distributor_name: str = "your advisor",
    distributor_arn: str = "",
    language: str = "hi-IN",
    queue_id: str = "",
) -> dict:
    """
    Core function — build script, prepare broker, place Twilio call.
    Returns a call record dict.
    """
    if call_type not in AGENDAS:
        return {"error": f"Unknown call_type '{call_type}'. Valid: {list(AGENDAS.keys())}"}

    resolved_lang = resolve_language(language)
    lang_info = SUPPORTED_LANGUAGES[resolved_lang]
    call_id = f"SBIMF-{uuid.uuid4().hex[:8].upper()}"

    sv = dict(script_variables)
    if call_type == "sip_renewal":
        sv.setdefault("fund_name",      "aapki mutual fund")
        sv.setdefault("monthly_amount", "")
        sv.setdefault("expiry_date",    "jald hi")

    agenda_vars = {"distributor_name": distributor_name, **sv}
    try:
        agenda_filled = AGENDAS[call_type].format(**agenda_vars)
    except KeyError as exc:
        agenda_filled = AGENDAS[call_type]
        log.warning("Missing script variable %s for call=%s", exc, call_id)

    system_instruction = build_system_instruction(
        agenda=agenda_filled,
        investor_name=investor_name,
        distributor_name=distributor_name,
        language_instruction=build_language_instruction(resolved_lang),
        greeting=lang_info["greeting"],
        closing=lang_info["closing"],
    )

    record = {
        "call_id":         call_id,
        "queue_id":        queue_id,
        "investor_id":     investor_id,
        "distributor_arn": distributor_arn,
        "mobile":          mobile,
        "call_type":       call_type,
        "language":        resolved_lang,
        "language_name":   lang_info["name"],
        "model":           GEMINI_LIVE_MODEL,
        "initiated_at":    datetime.utcnow().isoformat(),
        "outcome":         None,
        "transcript":      None,
    }

    if not TWILIO_ACCOUNT_SID:
        record["status"] = "simulated"
        record["note"] = (
            f"PoC simulation — {lang_info['name']} call to {investor_name} staged. "
            "Set TWILIO_ACCOUNT_SID to place real calls."
        )
        _calls[call_id] = record
        log.info("SIMULATED call %s → %s (%s)", call_id, investor_name, lang_info["name"])
        return record

    try:
        _prepare_bridge(call_id, resolved_lang, investor_name, distributor_name, system_instruction, sv)
    except Exception as exc:
        record.update({"status": "error", "error": f"Bridge /prepare failed: {exc}"})
        return record

    try:
        twilio_sid = _place_twilio_call(call_id, mobile)
    except Exception as exc:
        record.update({"status": "error", "error": f"Twilio dial failed: {exc}"})
        return record

    record.update({
        "status":          "initiated",
        "twilio_call_sid": twilio_sid,
        "IMPORTANT": "Call is now LIVE. Transcript saved to GCS when call ends.",
    })
    _calls[call_id] = record
    log.info("LIVE call %s → %s %s (%s)", call_id, investor_name, mobile, lang_info["name"])
    return record


def get_call(call_id: str) -> dict:
    return _calls.get(call_id, {"error": f"Call {call_id} not found"})


def get_recent_calls_by_arn(distributor_arn: str = "", limit: int = 20) -> list[dict]:
    calls = list(_calls.values())
    if distributor_arn:
        calls = [c for c in calls if c.get("distributor_arn") == distributor_arn]
    calls.sort(key=lambda x: x.get("initiated_at", ""), reverse=True)
    return calls[:limit]


def record_outcome(call_id: str, outcome: str, transcript: str = "") -> dict:
    if call_id not in _calls:
        return {"error": f"Call {call_id} not found"}
    _calls[call_id].update({
        "status":       "completed",
        "outcome":      outcome,
        "transcript":   transcript,
        "completed_at": datetime.utcnow().isoformat(),
    })
    follow_up = {
        "renewal_intent":           "Create renewal task for distributor",
        "callback_requested":       "Add to distributor callback queue",
        "not_interested":           "Mark — do not call for 90 days",
        "wrong_number":             "Flag investor record for mobile update",
        "no_answer":                "Retry in 24h (max 3 attempts)",
        "query_raised":             "Flag for distributor follow-up",
        "no_meaningful_conversation": "Retry in 24h",
    }.get(outcome, "Review manually")
    return {"call_id": call_id, "outcome": outcome, "follow_up": follow_up}


def _prepare_bridge(call_id, language, investor_name, distributor_name, system_instruction, sv):
    payload = {
        "language":           language,
        "client_name":        investor_name,
        "rm_name":            distributor_name,
        "system_instruction": system_instruction,
        "products":           sv.get("products", []),
    }
    resp = httpx.post(f"{LIVEAPI_BROKER_URL}/calls/{call_id}/prepare", json=payload, timeout=20)
    resp.raise_for_status()
    log.info("Bridge prepared: call=%s lang=%s", call_id, language)


def _place_twilio_call(call_id: str, mobile: str) -> str:
    resp = httpx.post(
        f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Calls.json",
        auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN),
        data={
            "To":                   mobile,
            "From":                 TWILIO_FROM_NUMBER,
            "Url":                  f"{LIVEAPI_BROKER_URL}/twilio/voice/{call_id}",
            "Method":               "POST",
            "StatusCallback":       f"{LIVEAPI_BROKER_URL}/twilio/status/{call_id}",
            "StatusCallbackMethod": "POST",
        },
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json().get("sid", "")
