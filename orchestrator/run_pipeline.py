"""
Full pipeline runner — trigger → gate → orchestrate.

Usage:
  python -m orchestrator.run_pipeline              # full run
  python -m orchestrator.run_pipeline --dry-run    # scan only, no BQ writes
  python -m orchestrator.run_pipeline --max 5      # process max 5 calls

Typical cron schedule: run once at 09:00 IST daily.
"""

import argparse
import json
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)

log = logging.getLogger("run_pipeline")


def main() -> None:
    parser = argparse.ArgumentParser(description="SBI MF Outbound Pipeline")
    parser.add_argument("--dry-run", action="store_true", help="Scan only, no writes")
    parser.add_argument("--max",     type=int, default=50, help="Max calls to place this run")
    parser.add_argument("--skip-trigger", action="store_true", help="Skip trigger engine")
    parser.add_argument("--skip-gate",    action="store_true", help="Skip compliance gate")
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("  SBI MF Outbound Pipeline")
    print(f"  dry_run={args.dry_run}  max_calls={args.max}")
    print("=" * 60 + "\n")

    # ── Layer 1: Trigger Engine ──────────────────────────────────────
    if not args.skip_trigger:
        log.info("▶ Step 1/3 — Trigger Engine")
        from trigger_engine.engine import run as trigger_run
        trigger_counts = trigger_run(dry_run=args.dry_run)
        log.info("Trigger counts: %s", json.dumps(trigger_counts))
    else:
        log.info("↩ Skipped trigger engine")

    if args.dry_run:
        log.info("DRY RUN — stopping before compliance gate")
        sys.exit(0)

    # ── Layer 2: Compliance Gate ─────────────────────────────────────
    if not args.skip_gate:
        log.info("▶ Step 2/3 — Compliance Gate")
        from compliance_gate.gate import run_all as gate_run
        gate_counts = gate_run()
        log.info("Gate counts: %s", json.dumps(gate_counts))
    else:
        log.info("↩ Skipped compliance gate")

    # ── Layer 3: Orchestrator ────────────────────────────────────────
    log.info("▶ Step 3/3 — Orchestrator (max %d calls)", args.max)
    from orchestrator.orchestrator import run_batch
    results = run_batch(max_calls=args.max)

    initiated = sum(1 for r in results if r.get("status") == "initiated")
    simulated = sum(1 for r in results if r.get("status") == "simulated")
    errored   = sum(1 for r in results if r.get("status") == "error")

    print("\n" + "=" * 60)
    print(f"  Pipeline complete — {len(results)} calls processed")
    print(f"  Initiated: {initiated}  Simulated: {simulated}  Errors: {errored}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
