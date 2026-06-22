# Cymbal MF Outbound AI Calling Platform — Technical Documentation

## Overview

An end-to-end outbound voice AI system that calls mutual fund investors on behalf of their financial distributors, delivers account updates (SIP renewal, debit failure, SIP paused, fund maturity), and records the outcome. The AI agent — named **Priya** — speaks in the investor's preferred language (Hindi, Gujarati, Kannada, Marathi, Punjabi, and 8 other Indian languages).

**Not built on ADK.** The system calls the Gemini Live WebSocket API directly via the `google-genai` SDK. There is no ADK agent graph, tool registry, or orchestration loop. Priya's "intelligence" lives entirely in a dynamically generated system instruction sent to a Gemini Live session per call.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                         Google Cloud (butterfly-987)                 │
│                                                                      │
│  ┌─────────────────┐   ┌──────────────────────┐   ┌──────────────┐  │
│  │  BigQuery        │   │  Cloud Run           │   │  Cloud Run   │  │
│  │  sbi_mf_poc      │   │  sbi-mf-dashboard    │   │  sbi-mf-     │  │
│  │                  │   │  (FastAPI :8080)      │   │  broker      │  │
│  │  • distributors  │◄──│                      │   │  (FastAPI    │  │
│  │  • investors     │   │  • Distributor UI     │   │   :8010)     │  │
│  │  • sip_mandates  │   │  • /api/call          │──►│             │  │
│  │  • call_queue    │   │  • /api/call-status   │   │  Twilio ↔   │  │
│  │  • call_events   │   │  • /api/transcript    │   │  Gemini Live │  │
│  │  • distributor_  │   │  • /api/refresh       │   │  WebSocket   │  │
│  │    settings      │   │                      │   │  bridge      │  │
│  └─────────────────┘   └──────────────────────┘   └──────────────┘  │
│                                   │                       │          │
│  ┌─────────────────┐              │                       │          │
│  │  GCS Buckets    │◄─────────────┼───────────────────────┘          │
│  │                 │              │                                   │
│  │  sbi-mf-call-   │   ┌──────────▼──────────┐                      │
│  │  transcripts    │   │  Outcome Processor   │                      │
│  │  (call .json)   │   │  (Gemini Flash text) │                      │
│  └─────────────────┘   └─────────────────────┘                      │
└──────────────────────────────────────────────────────────────────────┘
                                    │
              ┌─────────────────────┼──────────────────────┐
              │                     │                       │
     ┌────────▼────────┐  ┌─────────▼──────────┐  ┌───────▼───────┐
     │  Twilio          │  │  Gemini Live API    │  │  Investor     │
     │  (PSTN gateway)  │  │  (gemini-3.1-flash- │  │  Mobile Phone │
     │                  │  │   live-preview)     │  │               │
     └──────────────────┘  └─────────────────────┘  └───────────────┘
```

---

## Data Flow — Single Call

```
1. Distributor clicks "AI Call" in dashboard
         │
2. Dashboard POST /api/call
   → Fetch investor + SIP data from BigQuery
   → call_engine.make_call()
         │
3. call_engine builds system instruction
   (language rule + gender rule + agenda + script steps)
         │
4. POST /calls/{call_id}/prepare → broker
   (stores system instruction in CALL_CONTEXT dict)
         │
5. Twilio API creates outbound call → investor's phone
   TwiML webhook points to broker: /twilio/voice/{call_id}
         │
6. Investor picks up → Twilio opens Media Stream WebSocket
   to broker: /twilio/{call_id}
         │
7. Broker opens Gemini Live session with system instruction
   Sends initial turn: "(Call connected. Please begin your opening greeting now.)"
         │
8. AUDIO BRIDGE runs two concurrent async tasks:
   ┌──────────────────────────────────────────────────────┐
   │  _twilio_to_gemini:                                  │
   │  μ-law 8kHz → PCM 16kHz → Gemini Live realtime_input│
   │  (gated until greeting_done event fires)             │
   │                                                      │
   │  _gemini_to_twilio:                                  │
   │  PCM 24kHz → μ-law 8kHz → Twilio media event        │
   │  (sets greeting_done on first turn_complete)         │
   │  Handles barge-in via Twilio "clear" event           │
   │  Accumulates transcript per turn_complete            │
   └──────────────────────────────────────────────────────┘
         │
9. Twilio disconnects → asyncio.wait(FIRST_COMPLETED) fires
   Tasks cancelled → transcript flushed → saved to GCS
         │
10. Twilio status callback → broker → POST /api/refresh → dashboard
    Outcome processor reads GCS transcript → Gemini Flash classifies
    → writes outcome + notes to BigQuery call_events
         │
11. Dashboard 10s poll → /api/call-status/{call_id}
    → BigQuery outcome + GCS blob.exists() for has_transcript
    → UI card updates with outcome badge + transcript button
```

---

## Services

### 1. LiveAPI Broker (`voice/liveapi_broker.py`)

**Deployed on:** Cloud Run `sbi-mf-broker` (min-instances: 1)  
**Port:** 8010  
**Runtime:** FastAPI + uvicorn

The broker is the real-time audio core. It bridges Twilio's telephony audio to Gemini Live's streaming API.

#### Key endpoints

| Method | Path | Purpose |
|--------|------|---------|
| POST | `/calls/{call_id}/prepare` | Store system instruction before call is placed |
| GET/POST | `/twilio/voice/{call_id}` | Twilio voice webhook — returns TwiML to open Media Stream |
| WS | `/twilio/{call_id}` | Twilio Media Stream WebSocket — the live audio bridge |
| POST | `/twilio/status/{call_id}` | Twilio call status callback — triggers outcome processor |
| GET | `/calls/{call_id}/transcript` | Retrieve in-progress or completed transcript |
| GET | `/health` | Health check |

#### Audio pipeline

```
Twilio → broker:    μ-law 8kHz  →  audioop.ulaw2lin + ratecv(8000→16000)  →  PCM 16kHz
Gemini → broker:    PCM 24kHz   →  audioop.ratecv(24000→8000) + lin2ulaw  →  μ-law 8kHz  →  Twilio
```

#### Concurrency pattern

```python
t1 = asyncio.create_task(_twilio_to_gemini(...))
t2 = asyncio.create_task(_gemini_to_twilio(...))
try:
    await asyncio.wait([t1, t2], return_when=asyncio.FIRST_COMPLETED)
finally:
    for task in (t1, t2):
        task.cancel()
    await asyncio.gather(t1, t2, return_exceptions=True)
    _save_transcript(call_id)
```

`asyncio.wait(FIRST_COMPLETED)` ensures transcript saving runs as soon as either direction exits (Twilio hang-up OR Gemini session end). `asyncio.gather` was previously used but caused the transcript to never save — `_gemini_to_twilio` would block forever waiting for the next Gemini turn after Twilio disconnected.

#### Greeting gate

Twilio picks up ambient noise immediately. Without gating, early noise triggers Gemini's VAD and cuts the greeting. The broker sends a synthetic user turn `"(Call connected. Begin greeting now.)"` and blocks all real Twilio audio via `greeting_done` asyncio.Event until `turn_complete` fires from the first Gemini response.

#### Barge-in

When Gemini's `server_content.interrupted = True`, the broker sends `{"event": "clear", "streamSid": ...}` to Twilio to flush its audio buffer, preventing the investor from hearing a half-finished sentence.

#### Transcript storage

- In-memory accumulation per turn (`TRANSCRIPTS[call_id]`)
- Flushed on each `turn_complete` signal (not on individual chunk `finished` — that's unreliable in Gemini Live streaming)
- Saved to local file (ephemeral) + GCS bucket `sbi-mf-call-transcripts/{call_id}.json` on call end
- Format: `[{"speaker": "client"|"rm", "text": "...", "ts": 1718600000.0}]`

#### Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `GEMINI_LIVE_MODEL` | `gemini-3.1-flash-live-preview-04-2026` | Gemini Live model ID |
| `GCP_PROJECT` | `butterfly-987` | GCP project |
| `GCP_LOCATION` | `global` | Vertex AI location (mapped to `us-central1` for WebSocket) |
| `GOOGLE_API_KEY` | — | If set, uses AI Studio key instead of Vertex AI |
| `TRANSCRIPT_BUCKET` | `sbi-mf-call-transcripts` | GCS bucket for call transcripts |
| `DASHBOARD_URL` | — | Dashboard URL for outcome refresh trigger |
| `TWILIO_AUTH_TOKEN` | — | For Twilio signature validation |

---

### 2. Dashboard (`dashboard/server.py` + `dashboard/templates/index.html`)

**Deployed on:** Cloud Run `sbi-mf-dashboard`  
**Port:** 8080  
**URL:** `https://sbi-mf-dashboard-1058427839055.us-central1.run.app`

The distributor-facing web portal. Distributors select their ARN, view investor portfolios, launch individual AI calls, and monitor call activity in real time.

#### Backend endpoints

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/arns` | List all distributor ARNs from BigQuery |
| GET | `/api/investors?arn=` | Investor portfolio with SIP details |
| GET | `/api/calls?arn=&limit=` | Historical call activity from BigQuery |
| GET | `/api/outcomes?arn=` | Outcome counts for KPI sidebar |
| POST | `/api/call` | Trigger a single AI call |
| GET | `/api/call-status/{call_id}` | Lightweight poll: status + outcome + has_transcript |
| GET | `/api/transcript/{call_id}` | Fetch transcript JSON from GCS |
| POST | `/api/refresh` | Run outcome processor (classify unprocessed transcripts) |

#### UI layout

```
┌──────────────────┬──────────────────────────────────────────────────┐
│  SIDEBAR         │  TOPBAR: ARN label + 10s countdown bar           │
│                  ├──────────────────────────────────────────────────┤
│  • ARN selector  │  INVESTOR PORTFOLIO                               │
│  • KPI cards     │  Table: Investor | Fund | ₹/mo | Expiry | Status │
│    (calls/renewal│  Sorted by urgency (debit_failed → expiring soon) │
│    /callback/    │  Inline "AI Call" expand → trigger type + launch  │
│    investors)    ├──────────────────────────────────────────────────┤
│  • IST clock     │  CALL ACTIVITY  [All][10m][30m][1h] filter       │
│                  │  Cards: newest first, auto-sorted                 │
│                  │  10s poll: /api/call-status per active call_id   │
│                  │  Status: Initiating → Live → outcome badge       │
│                  │  Transcript: inline chat bubbles on click        │
└──────────────────┴──────────────────────────────────────────────────┘
```

#### Call card lifecycle

```
Initiated → (10s poll) → in-progress [🔴 Live] → completed
                                                      ↓
                                               outcome badge + transcript button appear
                                               (has_transcript = GCS blob exists)
```

---

### 3. Call Engine (`voice/call_engine.py`)

Pure Python library — no HTTP server. Imported by the dashboard to build system instructions and place calls.

#### `build_language_instruction(language_code)`

Generates the top-level language rule injected at the very start of the system prompt. The rule varies by language:

- **English (`en-IN`):** Hard MANDATORY — speak only English.
- **Hindi (`hi-IN`):** Hard MANDATORY — script is already in Hindi, deliver naturally.
- **Regional languages:** Soft default — start in the language, code-mixing is fine, but switch to Hindi **or** English immediately if the investor explicitly asks.

The regional language rule explicitly states `"Do NOT refuse or say you can only speak {lang_name}. You are capable of speaking Hindi and English."` — this prevents the model from claiming it cannot switch, which was the root cause of the Kavitha Reddy call failure (model refused to switch from Kannada to English).

#### `build_system_instruction(...)`

Assembles the full Gemini Live system prompt in this order:

1. **Language rule** (🔴 at top — model reads top-down, early rules override later script text)
2. Gender rule (female verb forms)
3. Identity block (Priya, Cymbal MF, on behalf of distributor)
4. Addressing rules (honorifics, first name only in opening/closing, distributor name only once)
5. Exact call script — STEP 1 (opening) → STEP 2 (agenda delivery) → STEP 3 (closing)
6. Interruption handling
7. Question deflection rules
8. Absolute prohibitions

The script steps are written in Hindi as reference and each step explicitly instructs the model to translate into the call language.

#### Supported languages

| Code | Language | Greeting | Closing |
|------|----------|----------|---------|
| `hi-IN` | Hindi | Namaste | Dhanyavaad |
| `gu-IN` | Gujarati | Kem cho | Aabhar |
| `mr-IN` | Marathi | Namaskar | Dhanyavad |
| `pa-IN` | Punjabi | Sat Sri Akal | Shukriya |
| `kn-IN` | Kannada | Namaskara | Dhanyavadagalu |
| `ta-IN` | Tamil | Vanakkam | Nandri |
| `te-IN` | Telugu | Namaskaram | Dhanyavaadalu |
| `ml-IN` | Malayalam | Namaskaram | Nandri |
| `bn-IN` | Bengali | Namaskar | Dhanyabad |
| `en-IN` | English | Hello | Thank you |

#### Call types (agendas)

| Type | Trigger | Key variables |
|------|---------|--------------|
| `sip_renewal` | SIP expiring ≤ 45 days | `fund_name`, `monthly_amount`, `expiry_date` |
| `sip_debit_failure` | NACH debit failed | `fund_name`, `month` |
| `sip_paused` | SIP in paused state | `fund_name`, `pause_since` |
| `fund_maturity` | Fixed-term fund maturing | `fund_name`, `maturity_date` |

---

### 4. Outcome Processor (`outcome_processor/processor.py`)

**Triggered by:** Broker → Twilio status callback → POST `/api/refresh`  
**Model:** `gemini-3.1-pro-preview` (text, `location="global"`)

Reads completed transcripts from GCS, classifies them with Gemini text generation, writes outcome to BigQuery.

#### Outcome classes

| Outcome | Meaning |
|---------|---------|
| `renewal_intent` | Investor expressed positive intent |
| `callback_requested` | Investor asked for someone to call back |
| `not_interested` | Investor clearly declined |
| `wrong_number` | Wrong person answered |
| `no_answer` | Call rang, nobody picked up |
| `query_raised` | Investor had questions — distributor follow-up needed |
| `no_meaningful_conversation` | Connected but investor didn't engage |

#### Note on model location

The Gemini Live voice model (`gemini-3.1-flash-live-preview`) requires the broker to connect to `us-central1` (WebSocket endpoint is regional). The text classification model (`gemini-3.1-pro-preview`) is published at `location="global"` — using `us-central1` for the text model returns 404.

---

### 5. Trigger Engine (`trigger_engine/engine.py`)

Scans BigQuery daily for actionable SIP events and writes rows to `call_queue`.

**Priority logic:**

| Condition | Priority |
|-----------|----------|
| Debit failed | P0 |
| SIP expiring ≤ 7 days | P0 |
| SIP expiring 8–21 days | P1 |
| SIP paused | P1 |
| SIP expiring 22–45 days | P2 |
| Fund maturity ≤ 30 days | P2 |

Idempotent: skips any `(sip_id, trigger_type)` pair already in `PENDING`, `APPROVED`, or `IN_PROGRESS` state.

---

### 6. Compliance Gate (`compliance_gate/gate.py`)

Runs after trigger engine. Moves each `PENDING` entry to `APPROVED` or `BLOCKED`.

**Checks (all must pass):**

1. **Investor consent** — `consent_given = TRUE` in `investors` table
2. **DND blocklist** — mobile not in TRAI NDNC list (mocked in PoC)
3. **Calling window** — current IST time within distributor's `calling_window_start`–`calling_window_end` (default 09:00–18:00)
4. **Distributor opt-in** — distributor has enabled this trigger type in `distributor_settings`
5. **Dedup** — no completed call for this `(investor_id, trigger_type)` in the past 24 hours

Blocked entries carry a `block_reason` string (e.g., `outside_calling_window_09:00_18:00`, `already_called_in_24h`).

---

## BigQuery Schema (`sbi_mf_poc` dataset)

### `distributors`
| Column | Type | Notes |
|--------|------|-------|
| `arn_code` | STRING | Primary key (e.g., ARN-12345) |
| `name` | STRING | Distributor display name |

### `distributor_settings`
| Column | Type | Notes |
|--------|------|-------|
| `arn_code` | STRING | FK → distributors |
| `calling_window_start` | STRING | "09:00" (IST) |
| `calling_window_end` | STRING | "18:00" (IST) |
| `sip_expiry_calls_enabled` | BOOL | |
| `debit_failure_calls_enabled` | BOOL | |
| `sip_paused_calls_enabled` | BOOL | |
| `fund_maturity_calls_enabled` | BOOL | |

### `investors`
| Column | Type | Notes |
|--------|------|-------|
| `investor_id` | STRING | Primary key |
| `full_name` | STRING | |
| `mobile` | STRING | E.164 format |
| `preferred_language` | STRING | BCP-47 (e.g., `gu-IN`) |
| `arn_code` | STRING | FK → distributors |
| `folio_no` | STRING | Mutual fund folio |
| `consent_given` | BOOL | DPDP consent flag |
| `city`, `state` | STRING | |

### `sip_mandates`
| Column | Type | Notes |
|--------|------|-------|
| `sip_id` | STRING | Primary key |
| `folio_no` | STRING | FK → investors |
| `arn_code` | STRING | FK → distributors |
| `fund_name` | STRING | |
| `amc_name` | STRING | |
| `monthly_amount_inr` | FLOAT | |
| `expiry_date` | DATE | |
| `next_debit_date` | DATE | |
| `status` | STRING | `active`, `paused`, `debit_failed`, `expired` |
| `frequency` | STRING | `monthly`, `quarterly` |

### `call_queue`
| Column | Type | Notes |
|--------|------|-------|
| `queue_id` | STRING | UUID primary key |
| `sip_id` | STRING | FK → sip_mandates |
| `investor_id` | STRING | |
| `arn_code` | STRING | |
| `trigger_type` | STRING | `sip_renewal`, `sip_debit_failure`, etc. |
| `priority` | STRING | P0 / P1 / P2 |
| `status` | STRING | `PENDING`, `APPROVED`, `BLOCKED`, `IN_PROGRESS`, `COMPLETED` |
| `block_reason` | STRING | Populated when BLOCKED |
| `created_at`, `updated_at` | TIMESTAMP | |

### `call_events`
| Column | Type | Notes |
|--------|------|-------|
| `call_id` | STRING | `SBIMF-{8 hex chars}` |
| `queue_id` | STRING | FK → call_queue |
| `investor_id` | STRING | |
| `arn_code` | STRING | |
| `trigger_type` | STRING | |
| `twilio_call_sid` | STRING | Twilio call SID |
| `status` | STRING | `initiated`, `completed`, `error` |
| `outcome` | STRING | Outcome class (set by processor) |
| `notes` | STRING | One-line AI summary |
| `transcript_ref` | STRING | GCS path |
| `initiated_at` | TIMESTAMP | |
| `completed_at` | TIMESTAMP | |

---

## GCS Buckets

| Bucket | Purpose | Path format |
|--------|---------|-------------|
| `sbi-mf-call-transcripts` | Call transcripts | `{call_id}.json` |
| `sbi-mf-voice-notes` | Voice notes (future) | — |

Transcript JSON format:
```json
[
  {"speaker": "rm", "text": "Namaste Amit ji...", "ts": 1718600123.4},
  {"speaker": "client", "text": "Haan, bol dijiye.", "ts": 1718600135.1}
]
```
`speaker` is `"rm"` (Priya) or `"client"` (investor).

---

## Deployment

Two independent Cloud Run services, each with its own Dockerfile and Cloud Build config.

### Broker

```bash
gcloud builds submit --config cloudbuild-broker.yaml .
gcloud run deploy sbi-mf-broker \
  --image gcr.io/butterfly-987/sbi-mf-broker \
  --region us-central1 --min-instances 1
```

`min-instances 1` is required — cold starts (~7s) cause `_prepare_bridge` to time out if the broker isn't already warm.

### Dashboard

```bash
gcloud builds submit --config cloudbuild-dashboard.yaml .
gcloud run deploy sbi-mf-dashboard \
  --image gcr.io/butterfly-987/sbi-mf-dashboard \
  --region us-central1
```

---

## Key Design Decisions

### Why not ADK?

ADK suits multi-agent systems where agents hand off tasks via tool calls. Here, the "agent" is a single Gemini Live session constrained to a 3-step script. The complexity is in real-time audio plumbing (μ-law ↔ PCM, barge-in, greeting gate), not agent orchestration. Direct Gemini Live API gives full control over audio encoding, VAD sensitivity, and session lifecycle.

### Language instruction placement

The language rule is placed **first** in the system prompt (before the identity block and call script). Gemini reads top-down — a late language rule can be overridden by the `"Namaste... Main Priya hoon"` Hindi script text that follows it.

### Script in Hindi with translation instruction

All four call agendas are authored in Hindi. Each STEP tells the model: `"Reference (translate to call language): ..."`. This avoids maintaining 11 language variants of each script while ensuring natural-sounding translation into the target language.

### asyncio.wait vs asyncio.gather

`asyncio.gather([t1, t2])` waits for both tasks. After Twilio sends `"stop"`, `_twilio_to_gemini` exits, but `_gemini_to_twilio` blocks indefinitely on `live.receive()` waiting for the next Gemini turn. `asyncio.wait(FIRST_COMPLETED)` returns as soon as one exits, then cancels the other — ensuring the `finally` block and `_save_transcript` always run.

### Twilio signature validation

Twilio signs its webhook POSTs with HMAC-SHA1. Cloud Run terminates TLS before the request reaches the container, so `request.url.scheme` is `http` inside the container even though Twilio signed against the `https` URL. The broker reconstructs the public HTTPS URL using `x-forwarded-proto` and `host` headers before validation.

---

## Compliance & Regulatory Context (PoC)

| Regulation | Handling |
|-----------|---------|
| **SEBI** | Agent never solicits transactions, never gives investment advice, never recommends specific funds |
| **TRAI NDNC** | Compliance gate checks DND blocklist (mocked in PoC — production queries TRAI API) |
| **DPDP Act** | `consent_given` flag per investor — blocked if not set |
| **Calling hours** | Configurable per distributor (default 09:00–18:00 IST) |
| **Identity disclosure** | Agent confirms it is AI when asked |
| **Data collection** | Agent never collects PAN, OTP, account numbers, or any personal data |

---

## Python Dependencies

```
google-cloud-bigquery>=3.25.0
google-cloud-storage>=2.17.0
google-genai>=1.0.0          # Gemini Live + text (NOT google-generativeai)
fastapi>=0.115.0
uvicorn>=0.30.0
httpx>=0.27.0
twilio>=9.0.0
python-multipart>=0.0.9      # Required for Twilio form POST parsing
websockets>=13.0
python-dotenv>=1.0.0
```

> **Note:** `google-genai` (new SDK) and `google-generativeai` (old SDK) are separate packages. The system uses `from google import genai` which requires `google-genai>=1.0.0`. Installing the old package instead causes `ImportError: cannot import name 'genai' from 'google'`.
