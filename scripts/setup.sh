#!/bin/bash
# One-time GCP setup for SBI MF Outbound PoC.
# Creates BigQuery dataset, tables, and GCS buckets.

set -e

if [ -f .env ]; then export $(grep -v '^#' .env | xargs); fi

PROJECT=${GCP_PROJECT:-"your-project"}
DATASET=${BQ_DATASET:-"sbi_mf_poc"}
REGION=${GCP_REGION:-"us-central1"}
VOICE_BUCKET=${GCS_BUCKET:-"sbi-mf-voice-notes"}
TRANSCRIPT_BUCKET=${TRANSCRIPT_BUCKET:-"sbi-mf-call-transcripts"}

echo "================================================"
echo "  SBI MF Outbound Agent — GCP Setup"
echo "  Project: $PROJECT  |  Region: $REGION"
echo "================================================"

echo ""
echo "[1/4] Creating BigQuery dataset: $DATASET"
bq mk --dataset --location=$REGION "$PROJECT:$DATASET" 2>/dev/null || echo "  Dataset already exists"

echo "[2/4] Creating BigQuery tables..."
bq query --use_legacy_sql=false --project_id="$PROJECT" < data/schema.sql
echo "  Tables created."

echo "[3/4] Creating GCS buckets..."
gsutil mb -p "$PROJECT" -l "$REGION" "gs://$VOICE_BUCKET"      2>/dev/null || echo "  $VOICE_BUCKET already exists"
gsutil mb -p "$PROJECT" -l "$REGION" "gs://$TRANSCRIPT_BUCKET" 2>/dev/null || echo "  $TRANSCRIPT_BUCKET already exists"

# Make voice notes bucket public (for WhatsApp media URL)
gsutil iam ch allUsers:objectViewer "gs://$VOICE_BUCKET" 2>/dev/null || true

echo "[4/4] Seeding demo data..."
python data/seed_data.py

echo ""
echo "================================================"
echo "  Setup complete!"
echo ""
echo "  Next: Fill in .env with your credentials"
echo "        then run: ./scripts/start_local.sh"
echo "================================================"
