-- SBI MF Outbound Agent — BigQuery Schema
-- Dataset: sbi_mf_poc
--
-- Run:
--   bq mk --dataset $GCP_PROJECT:sbi_mf_poc
--   bq query --use_legacy_sql=false < data/schema.sql

CREATE TABLE IF NOT EXISTS `sbi_mf_poc.distributors` (
  arn_code        STRING NOT NULL,
  name            STRING,
  type            STRING,           -- IFA | Bank | NBFC
  mobile          STRING,
  email           STRING,
  city            STRING,
  state           STRING,
  active          BOOL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS `sbi_mf_poc.distributor_settings` (
  arn_code                    STRING NOT NULL,
  auto_call_enabled           BOOL DEFAULT TRUE,
  calling_window_start        STRING DEFAULT '09:00',
  calling_window_end          STRING DEFAULT '19:00',
  max_calls_per_day           INT64 DEFAULT 50,
  sip_expiry_calls_enabled    BOOL DEFAULT TRUE,
  fund_maturity_calls_enabled BOOL DEFAULT TRUE,
  debit_failure_calls_enabled BOOL DEFAULT TRUE,
  sip_paused_calls_enabled    BOOL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS `sbi_mf_poc.investors` (
  investor_id        STRING NOT NULL,
  folio_no           STRING,
  full_name          STRING,
  pan                STRING,
  mobile             STRING,
  email              STRING,
  state              STRING,
  city               STRING,
  preferred_language STRING,        -- BCP-47: hi-IN | ta-IN | te-IN | kn-IN | ml-IN | mr-IN | bn-IN | gu-IN | pa-IN | en-IN
  arn_code           STRING,        -- links investor to distributor
  consent_given      BOOL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS `sbi_mf_poc.sip_mandates` (
  sip_id             STRING NOT NULL,
  investor_id        STRING,
  folio_no           STRING,
  arn_code           STRING,
  fund_name          STRING,
  amc_name           STRING DEFAULT 'SBI Mutual Fund',
  monthly_amount_inr FLOAT64,
  start_date         DATE,
  expiry_date        DATE,
  next_debit_date    DATE,
  status             STRING,        -- active | paused | expired | cancelled | debit_failed
  frequency          STRING DEFAULT 'monthly'
);

CREATE TABLE IF NOT EXISTS `sbi_mf_poc.call_queue` (
  queue_id        STRING NOT NULL,
  sip_id          STRING,
  investor_id     STRING,
  folio_no        STRING,
  arn_code        STRING,
  trigger_type    STRING,           -- sip_renewal | fund_maturity | sip_debit_failure | sip_paused
  priority        STRING,           -- P0 | P1 | P2
  status          STRING,           -- PENDING | APPROVED | IN_PROGRESS | COMPLETED | BLOCKED
  block_reason    STRING,
  created_at      TIMESTAMP,
  updated_at      TIMESTAMP,
  scheduled_for   DATE
);

CREATE TABLE IF NOT EXISTS `sbi_mf_poc.call_events` (
  call_id         STRING NOT NULL,
  queue_id        STRING,
  sip_id          STRING,
  investor_id     STRING,
  folio_no        STRING,
  arn_code        STRING,
  trigger_type    STRING,
  language        STRING,
  status          STRING,           -- initiated | completed | failed | simulated
  outcome         STRING,           -- renewal_intent | callback_requested | not_interested | wrong_number | no_answer | query_raised | no_meaningful_conversation
  transcript_ref  STRING,           -- GCS path
  twilio_call_sid STRING,
  notes           STRING,           -- one-line summary from outcome_processor
  initiated_at    TIMESTAMP,
  completed_at    TIMESTAMP
);
