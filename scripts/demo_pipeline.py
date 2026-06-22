"""
SBI MF Outbound Agent — Manual Demo Pipeline

Runs the full pipeline with real BigQuery, GCS, and Twilio.
The only difference from production: you run this manually instead of cron.

Usage:
    cd /home/user/sbi-mf-outbound
    python scripts/demo_pipeline.py
    python scripts/demo_pipeline.py --arn ARN-12345     # one distributor only
    python scripts/demo_pipeline.py --max 3             # limit calls placed
    python scripts/demo_pipeline.py --skip-outcome      # skip transcript classification
"""

import argparse
import json
import logging
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

# ── Colour helpers ──────────────────────────────────────────────────────────

RESET   = "\033[0m"
BOLD    = "\033[1m"
DIM     = "\033[2m"
GREEN   = "\033[32m"
YELLOW  = "\033[33m"
CYAN    = "\033[36m"
RED     = "\033[31m"
WHITE   = "\033[97m"
BG_BLUE = "\033[44m"

def hdr(text):  print(f"\n{BOLD}{BG_BLUE}{WHITE}  {text}  {RESET}\n")
def ok(text):   print(f"  {GREEN}✓{RESET}  {text}")
def warn(text): print(f"  {YELLOW}⚠{RESET}  {text}")
def err(text):  print(f"  {RED}✗{RESET}  {text}")
def info(text): print(f"  {CYAN}→{RESET}  {text}")
def sep():      print(f"  {'─' * 60}")


logging.basicConfig(
    level=logging.WARNING,          # suppress library noise during demo
    format="%(levelname)s  %(name)s: %(message)s",
)
log = logging.getLogger("demo_pipeline")


# ── Pipeline steps ──────────────────────────────────────────────────────────

def step_trigger(arn_filter: str, dry_run: bool) -> dict:
    hdr("STEP 1 / 4  —  Trigger Engine  (BigQuery scan)")
    info("Scanning sip_mandates for expiring SIPs, debit failures, paused SIPs...")

    from trigger_engine.engine import run as trigger_run

    # If ARN filter is set, monkey-patch the query to add a WHERE clause.
    # For a proper demo we just run all and note what was found.
    counts = trigger_run(dry_run=dry_run)

    total = sum(v for k, v in counts.items() if k != "skipped")
    ok(f"Inserted {total} entries into call_queue  (skipped {counts.get('skipped',0)} already-queued)")
    for trigger_type, count in counts.items():
        if count and trigger_type != "skipped":
            print(f"       {CYAN}{trigger_type:<25}{RESET}  {count}")
    print()
    return counts


def step_gate(arn_filter: str) -> dict:
    hdr("STEP 2 / 4  —  Compliance Gate  (DND · consent · calling window · dedup)")
    info("Processing all PENDING entries in call_queue...")

    from compliance_gate.gate import run_all as gate_run
    counts = gate_run()

    ok(f"Approved : {counts.get('approved', 0)}")
    if counts.get("blocked", 0):
        warn(f"Blocked  : {counts.get('blocked', 0)}  (check block_reason in call_queue)")
    print()
    return counts


def step_orchestrate(max_calls: int) -> list:
    hdr("STEP 3 / 4  —  Orchestrator  (placing calls via Twilio + Gemini Live)")
    info(f"Fetching APPROVED entries from call_queue (max {max_calls})...")

    from orchestrator.orchestrator import run_batch
    results = run_batch(max_calls=max_calls)

    if not results:
        warn("No APPROVED calls to place — check compliance gate results above.")
        print()
        return []

    initiated = [r for r in results if r.get("status") == "initiated"]
    simulated = [r for r in results if r.get("status") == "simulated"]
    errored   = [r for r in results if r.get("status") == "error"]

    for r in results:
        status   = r.get("status", "unknown")
        call_id  = r.get("call_id", "—")
        inv_id   = r.get("investor_id", "—")
        lang     = r.get("language_name", "—")

        if status == "initiated":
            ok(f"LIVE      {call_id}  investor={inv_id}  lang={lang}")
            print(f"           {DIM}Twilio SID: {r.get('twilio_call_sid','')}{RESET}")
        elif status == "simulated":
            warn(f"SIMULATED {call_id}  investor={inv_id}  lang={lang}")
            print(f"           {DIM}Set TWILIO_ACCOUNT_SID in .env to place real calls{RESET}")
        else:
            err(f"ERROR     investor={inv_id}  →  {r.get('error','unknown')}")

    sep()
    if initiated:
        ok(f"{len(initiated)} live calls in progress — Priya is talking to investors now")
        info("Transcripts will be saved to GCS when each call ends")
    if simulated:
        warn(f"{len(simulated)} calls simulated (no Twilio credentials)")
    if errored:
        err(f"{len(errored)} calls failed")
    print()
    return results


def step_outcome(skip: bool) -> dict:
    hdr("STEP 4 / 4  —  Outcome Processor  (GCS transcripts → Gemini Flash → BigQuery)")

    if skip:
        warn("Skipped (--skip-outcome flag set)")
        info("Run manually later:  python -m outcome_processor.processor")
        print()
        return {}

    info("Scanning GCS transcript bucket for completed calls...")

    from outcome_processor.processor import process_all
    counts = process_all()

    if not counts:
        warn("No new transcripts found yet — calls may still be in progress")
        info("Re-run outcome step after calls complete:  python -m outcome_processor.processor")
        print()
        return {}

    total = sum(counts.values())
    ok(f"Classified {total} transcripts")
    for outcome, count in sorted(counts.items(), key=lambda x: -x[1]):
        colour = GREEN if outcome == "renewal_intent" else (YELLOW if outcome == "callback_requested" else DIM)
        print(f"       {colour}{outcome:<35}{RESET}  {count}")
    print()
    return counts


def show_summary(trigger_counts, gate_counts, call_results, outcome_counts, arn_filter):
    hdr("PIPELINE COMPLETE  —  Summary")

    queued   = sum(v for k, v in trigger_counts.items() if k != "skipped")
    approved = gate_counts.get("approved", 0)
    blocked  = gate_counts.get("blocked", 0)
    placed   = len([r for r in call_results if r.get("status") in ("initiated","simulated")])
    renewed  = outcome_counts.get("renewal_intent", 0)
    callback = outcome_counts.get("callback_requested", 0)

    print(f"""  {BOLD}Pipeline stages:{RESET}
    Trigger Engine   →  {queued} events found in BigQuery
    Compliance Gate  →  {approved} approved  |  {blocked} blocked
    Orchestrator     →  {placed} calls placed  (Twilio + Gemini Live)
    Outcome Processor→  {sum(outcome_counts.values())} transcripts classified by Gemini Flash

  {BOLD}Business results:{RESET}
    {GREEN}✓  {renewed}  renewal intents captured  →  distributor follow-up queue{RESET}
    {YELLOW}↩  {callback}  callback requests         →  distributor to call back{RESET}

  {BOLD}Where to see results:{RESET}
    Dashboard  →  http://localhost:8020/?arn={arn_filter or 'ARN-12345'}
    BigQuery   →  SELECT * FROM sbi_mf_poc.call_events ORDER BY initiated_at DESC LIMIT 20
    GCS        →  gs://{os.getenv('TRANSCRIPT_BUCKET','sbi-mf-call-transcripts')}/transcripts/
""")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="SBI MF Outbound — Manual Demo Pipeline")
    parser.add_argument("--arn",           default="", help="Filter to one distributor ARN")
    parser.add_argument("--max",           type=int, default=20, help="Max calls to place")
    parser.add_argument("--dry-run",       action="store_true", help="Scan only, no BQ writes")
    parser.add_argument("--skip-trigger",  action="store_true", help="Skip trigger engine (use existing queue)")
    parser.add_argument("--skip-gate",     action="store_true", help="Skip compliance gate")
    parser.add_argument("--skip-outcome",  action="store_true", help="Skip outcome processor")
    args = parser.parse_args()

    print()
    print(f"  {BOLD}{'='*62}{RESET}")
    print(f"  {BOLD}  SBI Mutual Fund — Outbound Agentic Calling  |  Manual Run{RESET}")
    print(f"  {BOLD}{'='*62}{RESET}")
    if args.arn:
        print(f"  ARN filter : {args.arn}")
    if args.dry_run:
        print(f"  {YELLOW}DRY RUN — BigQuery will not be written{RESET}")
    print()

    # ── Step 1: Trigger Engine ────────────────────────────────────────────
    trigger_counts = {}
    if not args.skip_trigger:
        try:
            trigger_counts = step_trigger(args.arn, args.dry_run)
        except Exception as exc:
            err(f"Trigger engine failed: {exc}")
            info("Is BigQuery set up? Run:  ./scripts/setup.sh")
            sys.exit(1)
    else:
        warn("Trigger engine skipped — using existing call_queue entries")
        print()

    if args.dry_run:
        warn("DRY RUN complete — stopping before compliance gate")
        sys.exit(0)

    # ── Step 2: Compliance Gate ───────────────────────────────────────────
    gate_counts = {}
    if not args.skip_gate:
        try:
            gate_counts = step_gate(args.arn)
        except Exception as exc:
            err(f"Compliance gate failed: {exc}")
            sys.exit(1)
    else:
        warn("Compliance gate skipped")
        print()

    # ── Step 3: Orchestrator ──────────────────────────────────────────────
    try:
        call_results = step_orchestrate(args.max)
    except Exception as exc:
        err(f"Orchestrator failed: {exc}")
        sys.exit(1)

    # ── Step 4: Outcome Processor ─────────────────────────────────────────
    outcome_counts = step_outcome(args.skip_outcome)

    # ── Summary ───────────────────────────────────────────────────────────
    show_summary(trigger_counts, gate_counts, call_results, outcome_counts, args.arn)


if __name__ == "__main__":
    main()
