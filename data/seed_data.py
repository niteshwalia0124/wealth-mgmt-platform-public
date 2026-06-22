"""
Seed BigQuery with SBI MF PoC demo data.

3 distributors, 10 investors, 8 SIP mandates at various expiry stages.
Run: python data/seed_data.py
"""

import os
from dotenv import load_dotenv
load_dotenv()

from google.cloud import bigquery
from datetime import date, timedelta

BQ_PROJECT = os.getenv("GCP_PROJECT", "butterfly-987")
BQ_DATASET = os.getenv("BQ_DATASET", "sbi_mf_poc")

client = bigquery.Client(project=BQ_PROJECT)

TODAY = date.today()


def _val(v) -> str:
    """Format a Python value as a BigQuery SQL literal."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    # string / date — escape single quotes
    return "'" + str(v).replace("'", "\\'") + "'"


def insert(table: str, rows: list[dict]):
    """Use DML INSERT so rows are immediately available for UPDATE (no streaming buffer lock)."""
    if not rows:
        return
    ref = f"`{BQ_PROJECT}.{BQ_DATASET}.{table}`"
    cols = ", ".join(rows[0].keys())
    values = []
    for r in rows:
        row_vals = ", ".join(_val(v) for v in r.values())
        values.append(f"({row_vals})")
    sql = f"INSERT INTO {ref} ({cols}) VALUES {', '.join(values)}"
    job = client.query(sql)
    job.result()
    print(f"  Inserted {len(rows)} rows into {table}")


def seed():
    print("Seeding distributors...")
    insert("distributors", [
        {
            "arn_code": "ARN-12345",
            "name": "Sharma Wealth Advisors",
            "type": "IFA",
            "mobile": "+919876500001",
            "email": "sharma@swadvisors.in",
            "city": "Mumbai",
            "state": "Maharashtra",
            "active": True,
        },
        {
            "arn_code": "ARN-67890",
            "name": "Gupta Financial Services",
            "type": "IFA",
            "mobile": "+919876500002",
            "email": "gupta@guptafinancial.in",
            "city": "Delhi",
            "state": "Delhi",
            "active": True,
        },
        {
            "arn_code": "ARN-11111",
            "name": "SBI Bank — Bengaluru Branch",
            "type": "Bank",
            "mobile": "+919876500003",
            "email": "sbi.blr@sbi.co.in",
            "city": "Bengaluru",
            "state": "Karnataka",
            "active": True,
        },
    ])

    print("Seeding distributor_settings...")
    insert("distributor_settings", [
        {"arn_code": "ARN-12345", "auto_call_enabled": True,
         "calling_window_start": "09:00", "calling_window_end": "22:00",
         "max_calls_per_day": 50, "sip_expiry_calls_enabled": True,
         "fund_maturity_calls_enabled": True, "debit_failure_calls_enabled": True,
         "sip_paused_calls_enabled": True},
        {"arn_code": "ARN-67890", "auto_call_enabled": True,
         "calling_window_start": "10:00", "calling_window_end": "22:00",
         "max_calls_per_day": 30, "sip_expiry_calls_enabled": True,
         "fund_maturity_calls_enabled": False, "debit_failure_calls_enabled": True,
         "sip_paused_calls_enabled": True},
        {"arn_code": "ARN-11111", "auto_call_enabled": True,
         "calling_window_start": "09:00", "calling_window_end": "22:00",
         "max_calls_per_day": 100, "sip_expiry_calls_enabled": True,
         "fund_maturity_calls_enabled": True, "debit_failure_calls_enabled": True,
         "sip_paused_calls_enabled": True},
    ])

    print("Seeding investors...")
    investors = [
        # ARN-12345 (Mumbai/Maharashtra — Hindi)
        {"investor_id": "INV-001", "folio_no": "10234567",
         "full_name": "Rajesh Kumar Sharma", "pan": "ABCPS1234A",
         "mobile": "+919876501001", "email": "rajesh.sharma@email.com",
         "state": "Maharashtra", "city": "Mumbai",
         "preferred_language": "hi-IN", "arn_code": "ARN-12345", "consent_given": True},
        {"investor_id": "INV-002", "folio_no": "10234568",
         "full_name": "Priya Mehta", "pan": "BCQPM5678B",
         "mobile": "+919876501002", "email": "priya.mehta@email.com",
         "state": "Maharashtra", "city": "Pune",
         "preferred_language": "mr-IN", "arn_code": "ARN-12345", "consent_given": True},
        {"investor_id": "INV-003", "folio_no": "10234569",
         "full_name": "Amit Joshi", "pan": "CDRPJ9012C",
         "mobile": "+919876501003", "email": "amit.joshi@email.com",
         "state": "Gujarat", "city": "Ahmedabad",
         "preferred_language": "gu-IN", "arn_code": "ARN-12345", "consent_given": True},
        # ARN-67890 (Delhi — Hindi)
        {"investor_id": "INV-004", "folio_no": "20345678",
         "full_name": "Sunita Agarwal", "pan": "DESPA3456D",
         "mobile": "+919876501004", "email": "sunita.agarwal@email.com",
         "state": "Delhi", "city": "New Delhi",
         "preferred_language": "hi-IN", "arn_code": "ARN-67890", "consent_given": True},
        {"investor_id": "INV-005", "folio_no": "20345679",
         "full_name": "Vikram Singh", "pan": "EFTVS7890E",
         "mobile": "+919876501005", "email": "vikram.singh@email.com",
         "state": "Punjab", "city": "Chandigarh",
         "preferred_language": "pa-IN", "arn_code": "ARN-67890", "consent_given": True},
        # ARN-11111 (Bengaluru — Kannada)
        {"investor_id": "INV-006", "folio_no": "30456789",
         "full_name": "Kavitha Reddy", "pan": "FGUKR2345F",
         "mobile": "+919876501006", "email": "kavitha.reddy@email.com",
         "state": "Karnataka", "city": "Bengaluru",
         "preferred_language": "kn-IN", "arn_code": "ARN-11111", "consent_given": True},
        {"investor_id": "INV-007", "folio_no": "30456790",
         "full_name": "Ravi Krishnamurthy", "pan": "GHVRK5678G",
         "mobile": "+919876501007", "email": "ravi.k@email.com",
         "state": "Tamil Nadu", "city": "Chennai",
         "preferred_language": "ta-IN", "arn_code": "ARN-11111", "consent_given": True},
        {"investor_id": "INV-008", "folio_no": "30456791",
         "full_name": "Lakshmi Nair", "pan": "HIWLN8901H",
         "mobile": "+919876501008", "email": "lakshmi.nair@email.com",
         "state": "Kerala", "city": "Kochi",
         "preferred_language": "ml-IN", "arn_code": "ARN-11111", "consent_given": True},
    ]
    insert("investors", investors)

    print("Seeding sip_mandates...")
    sips = [
        # P0 — expiring in 7 days (urgent)
        {"sip_id": "SIP-001", "investor_id": "INV-001", "folio_no": "10234567",
         "arn_code": "ARN-12345",
         "fund_name": "SBI Bluechip Fund", "amc_name": "SBI Mutual Fund",
         "monthly_amount_inr": 5000.0,
         "start_date": str(TODAY - timedelta(days=700)),
         "expiry_date": str(TODAY + timedelta(days=7)),
         "next_debit_date": str(TODAY + timedelta(days=7)),
         "status": "active", "frequency": "monthly"},

        {"sip_id": "SIP-002", "investor_id": "INV-004", "folio_no": "20345678",
         "arn_code": "ARN-67890",
         "fund_name": "SBI Small Cap Fund", "amc_name": "SBI Mutual Fund",
         "monthly_amount_inr": 10000.0,
         "start_date": str(TODAY - timedelta(days=365)),
         "expiry_date": str(TODAY + timedelta(days=5)),
         "next_debit_date": str(TODAY + timedelta(days=5)),
         "status": "active", "frequency": "monthly"},

        # P1 — expiring in 14 days
        {"sip_id": "SIP-003", "investor_id": "INV-002", "folio_no": "10234568",
         "arn_code": "ARN-12345",
         "fund_name": "SBI Flexi Cap Fund", "amc_name": "SBI Mutual Fund",
         "monthly_amount_inr": 3000.0,
         "start_date": str(TODAY - timedelta(days=500)),
         "expiry_date": str(TODAY + timedelta(days=14)),
         "next_debit_date": str(TODAY + timedelta(days=14)),
         "status": "active", "frequency": "monthly"},

        {"sip_id": "SIP-004", "investor_id": "INV-006", "folio_no": "30456789",
         "arn_code": "ARN-11111",
         "fund_name": "SBI Equity Hybrid Fund", "amc_name": "SBI Mutual Fund",
         "monthly_amount_inr": 8000.0,
         "start_date": str(TODAY - timedelta(days=450)),
         "expiry_date": str(TODAY + timedelta(days=12)),
         "next_debit_date": str(TODAY + timedelta(days=12)),
         "status": "active", "frequency": "monthly"},

        # P2 — expiring in 30 days
        {"sip_id": "SIP-005", "investor_id": "INV-003", "folio_no": "10234569",
         "arn_code": "ARN-12345",
         "fund_name": "SBI Magnum Midcap Fund", "amc_name": "SBI Mutual Fund",
         "monthly_amount_inr": 15000.0,
         "start_date": str(TODAY - timedelta(days=600)),
         "expiry_date": str(TODAY + timedelta(days=28)),
         "next_debit_date": str(TODAY + timedelta(days=28)),
         "status": "active", "frequency": "monthly"},

        {"sip_id": "SIP-006", "investor_id": "INV-007", "folio_no": "30456790",
         "arn_code": "ARN-11111",
         "fund_name": "SBI Contra Fund", "amc_name": "SBI Mutual Fund",
         "monthly_amount_inr": 7500.0,
         "start_date": str(TODAY - timedelta(days=300)),
         "expiry_date": str(TODAY + timedelta(days=30)),
         "next_debit_date": str(TODAY + timedelta(days=30)),
         "status": "active", "frequency": "monthly"},

        # Debit failure case
        {"sip_id": "SIP-007", "investor_id": "INV-005", "folio_no": "20345679",
         "arn_code": "ARN-67890",
         "fund_name": "SBI Long Term Equity Fund", "amc_name": "SBI Mutual Fund",
         "monthly_amount_inr": 2000.0,
         "start_date": str(TODAY - timedelta(days=200)),
         "expiry_date": str(TODAY + timedelta(days=180)),
         "next_debit_date": str(TODAY - timedelta(days=2)),
         "status": "debit_failed", "frequency": "monthly"},

        # Paused SIP
        {"sip_id": "SIP-008", "investor_id": "INV-008", "folio_no": "30456791",
         "arn_code": "ARN-11111",
         "fund_name": "SBI Banking & PSU Fund", "amc_name": "SBI Mutual Fund",
         "monthly_amount_inr": 5000.0,
         "start_date": str(TODAY - timedelta(days=400)),
         "expiry_date": str(TODAY + timedelta(days=200)),
         "next_debit_date": None,
         "status": "paused", "frequency": "monthly"},
    ]
    insert("sip_mandates", sips)

    print("\nSeed complete.")
    print(f"  3 distributors, {len(investors)} investors, {len(sips)} SIP mandates")
    print(f"  P0 (≤7 days): SIP-001, SIP-002")
    print(f"  P1 (≤14 days): SIP-003, SIP-004")
    print(f"  P2 (≤30 days): SIP-005, SIP-006")
    print(f"  Debit failure: SIP-007")
    print(f"  Paused: SIP-008")


if __name__ == "__main__":
    seed()
