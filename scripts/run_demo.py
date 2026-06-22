"""
Demo runner — tests all 4 SBI MF call types directly via call_engine.

Uses call_engine.make_call() directly — no HTTP, no MCP server needed.
In PoC mode (no TWILIO_ACCOUNT_SID), returns simulated records.
Set TWILIO_ACCOUNT_SID + DEMO_MOBILE to place a real call.

Usage:
    cd /home/user/sbi-mf-outbound
    python scripts/run_demo.py
    python scripts/run_demo.py --type sip_renewal
    python scripts/run_demo.py --type sip_debit_failure --mobile +919876543210
"""

import argparse
import json
import logging
import os
import sys

# Allow importing from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(levelname)-8s %(name)s: %(message)s")

from voice.call_engine import make_call

DEMO_MOBILE = os.getenv("DEMO_MOBILE", "+919999999999")

SCENARIOS = {
    "sip_renewal": {
        "investor_id":      "INV-001",
        "mobile":           DEMO_MOBILE,
        "investor_name":    "Rajesh Sharma",
        "call_type":        "sip_renewal",
        "distributor_name": "Sharma Wealth Advisors",
        "distributor_arn":  "ARN-12345",
        "language":         "hi-IN",
        "script_variables": {
            "fund_name":      "SBI Bluechip Fund",
            "monthly_amount": "5000",
            "expiry_date":    "22 June 2026",
        },
    },
    "fund_maturity": {
        "investor_id":      "INV-002",
        "mobile":           DEMO_MOBILE,
        "investor_name":    "Priya Nair",
        "call_type":        "fund_maturity",
        "distributor_name": "Sharma Wealth Advisors",
        "distributor_arn":  "ARN-12345",
        "language":         "ml-IN",
        "script_variables": {
            "fund_name":    "SBI Fixed Maturity Plan",
            "maturity_date":"30 June 2026",
        },
    },
    "sip_debit_failure": {
        "investor_id":      "INV-007",
        "mobile":           DEMO_MOBILE,
        "investor_name":    "Arjun Mehta",
        "call_type":        "sip_debit_failure",
        "distributor_name": "Gupta Financial Services",
        "distributor_arn":  "ARN-67890",
        "language":         "hi-IN",
        "script_variables": {
            "fund_name": "SBI Small Cap Fund",
            "month":     "June 2026",
        },
    },
    "sip_paused": {
        "investor_id":      "INV-008",
        "mobile":           DEMO_MOBILE,
        "investor_name":    "Sunita Rao",
        "call_type":        "sip_paused",
        "distributor_name": "SBI Bank — Bengaluru Branch",
        "distributor_arn":  "ARN-11111",
        "language":         "kn-IN",
        "script_variables": {
            "fund_name":   "SBI Equity Hybrid Fund",
            "pause_since": "March 2026",
        },
    },
}


def run_scenario(name: str, mobile_override: str = "") -> dict:
    sc = dict(SCENARIOS[name])
    if mobile_override:
        sc["mobile"] = mobile_override

    print(f"\n{'='*60}")
    print(f"  Scenario: {name}")
    print(f"  Investor: {sc['investor_name']}  Language: {sc['language']}")
    print(f"  Distributor: {sc['distributor_name']}")
    print(f"  Mobile: {sc['mobile']}")
    print("=" * 60)

    result = make_call(**sc)
    print(json.dumps(result, indent=2, default=str))
    return result


def main():
    parser = argparse.ArgumentParser(description="SBI MF Demo Runner")
    parser.add_argument("--type",   choices=list(SCENARIOS.keys()),
                        help="Run a specific scenario (default: all)")
    parser.add_argument("--mobile", default="", help="Override mobile number")
    args = parser.parse_args()

    if args.type:
        run_scenario(args.type, args.mobile)
    else:
        print("\nRunning all 4 SBI MF call scenarios...\n")
        results = {}
        for name in SCENARIOS:
            results[name] = run_scenario(name, args.mobile)

        print(f"\n{'='*60}")
        print("  DEMO SUMMARY")
        print("=" * 60)
        for name, r in results.items():
            status  = r.get("status",  "unknown")
            call_id = r.get("call_id", "—")
            print(f"  {name:<25} {status:<15} {call_id}")
        print()


if __name__ == "__main__":
    main()
