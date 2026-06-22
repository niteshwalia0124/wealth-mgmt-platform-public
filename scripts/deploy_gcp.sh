#!/bin/bash
# SBI MF Outbound Agent — Full GCP Deployment
# Deploys: BigQuery, GCS, LiveAPI Broker (Cloud Run), Dashboard (Cloud Run)
# Run from project root: ./scripts/deploy_gcp.sh

set -euo pipefail

# ── Load .env ────────────────────────────────────────────────────────────────
if [ -f .env ]; then
  export $(grep -v '^#' .env | grep -v '^$' | xargs)
fi

PROJECT=${GCP_PROJECT:-"butterfly-987"}
REGION=${GCP_REGION:-"us-central1"}
DATASET=${BQ_DATASET:-"sbi_mf_poc"}
VOICE_BUCKET=${GCS_BUCKET:-"sbi-mf-voice-notes"}
TX_BUCKET=${TRANSCRIPT_BUCKET:-"sbi-mf-call-transcripts"}

BROKER_SERVICE="sbi-mf-broker"
DASHBOARD_SERVICE="sbi-mf-dashboard"

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║     SBI MF Outbound Agent — GCP Deployment               ║"
echo "║     Project : $PROJECT"
echo "║     Region  : $REGION"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

gcloud config set project "$PROJECT" --quiet

# ── Step 1: Enable APIs ───────────────────────────────────────────────────────
echo "▶ Step 1/6 — Enabling GCP APIs..."
gcloud services enable \
  bigquery.googleapis.com \
  storage.googleapis.com \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  aiplatform.googleapis.com \
  --project="$PROJECT" --quiet
echo "  ✓ APIs enabled"
echo ""

# ── Step 2: BigQuery dataset + tables ────────────────────────────────────────
echo "▶ Step 2/6 — Creating BigQuery dataset and tables..."
bq mk --dataset \
  --location="$REGION" \
  --project="$PROJECT" \
  "${PROJECT}:${DATASET}" 2>/dev/null && echo "  ✓ Dataset $DATASET created" \
  || echo "  ✓ Dataset $DATASET already exists"

bq query \
  --use_legacy_sql=false \
  --project_id="$PROJECT" \
  --location="$REGION" \
  < data/schema.sql
echo "  ✓ Tables created in $DATASET"
echo ""

# ── Step 3: GCS buckets ───────────────────────────────────────────────────────
echo "▶ Step 3/6 — Creating GCS buckets..."
gsutil mb -p "$PROJECT" -l "$REGION" "gs://$VOICE_BUCKET"      2>/dev/null \
  && echo "  ✓ gs://$VOICE_BUCKET created" \
  || echo "  ✓ gs://$VOICE_BUCKET already exists"

gsutil mb -p "$PROJECT" -l "$REGION" "gs://$TX_BUCKET"         2>/dev/null \
  && echo "  ✓ gs://$TX_BUCKET created" \
  || echo "  ✓ gs://$TX_BUCKET already exists"

# Voice notes bucket must be public for WhatsApp media URLs
gsutil iam ch allUsers:objectViewer "gs://$VOICE_BUCKET" 2>/dev/null || true
echo "  ✓ Voice notes bucket set to public"
echo ""

# ── Step 4: Seed data ─────────────────────────────────────────────────────────
echo "▶ Step 4/6 — Seeding demo data into BigQuery..."
python data/seed_data.py
echo ""

# ── Step 5: Deploy LiveAPI Broker to Cloud Run ────────────────────────────────
echo "▶ Step 5/6 — Building and deploying LiveAPI Broker to Cloud Run..."
echo "  (This takes 2–3 minutes for Cloud Build...)"

gcloud run deploy "$BROKER_SERVICE" \
  --source . \
  --dockerfile Dockerfile.broker \
  --region "$REGION" \
  --project "$PROJECT" \
  --allow-unauthenticated \
  --min-instances 1 \
  --max-instances 1 \
  --memory 1Gi \
  --cpu 1 \
  --timeout 3600 \
  --set-env-vars "GCP_PROJECT=${PROJECT},GCP_LOCATION=${GCP_LOCATION:-global},GEMINI_LIVE_MODEL=${GEMINI_LIVE_MODEL:-gemini-3.1-flash-live-preview-04-2026},GCS_BUCKET=${VOICE_BUCKET},TRANSCRIPT_BUCKET=${TX_BUCKET},TWILIO_ACCOUNT_SID=${TWILIO_ACCOUNT_SID},TWILIO_AUTH_TOKEN=${TWILIO_AUTH_TOKEN},TWILIO_FROM_NUMBER=${TWILIO_FROM_NUMBER}" \
  --quiet

BROKER_URL=$(gcloud run services describe "$BROKER_SERVICE" \
  --region "$REGION" --project "$PROJECT" \
  --format "value(status.url)")

echo "  ✓ Broker deployed: $BROKER_URL"
echo ""

# Update LIVEAPI_BROKER_URL in .env
if grep -q "^LIVEAPI_BROKER_URL=" .env; then
  sed -i "s|^LIVEAPI_BROKER_URL=.*|LIVEAPI_BROKER_URL=${BROKER_URL}|" .env
else
  echo "LIVEAPI_BROKER_URL=${BROKER_URL}" >> .env
fi
echo "  ✓ LIVEAPI_BROKER_URL updated in .env → $BROKER_URL"
echo ""

# ── Step 6: Deploy Dashboard to Cloud Run ────────────────────────────────────
echo "▶ Step 6/6 — Building and deploying Dashboard to Cloud Run..."

gcloud run deploy "$DASHBOARD_SERVICE" \
  --source . \
  --dockerfile Dockerfile.dashboard \
  --region "$REGION" \
  --project "$PROJECT" \
  --allow-unauthenticated \
  --min-instances 0 \
  --max-instances 3 \
  --memory 512Mi \
  --cpu 1 \
  --set-env-vars "GCP_PROJECT=${PROJECT},BQ_DATASET=${DATASET}" \
  --quiet

DASHBOARD_URL=$(gcloud run services describe "$DASHBOARD_SERVICE" \
  --region "$REGION" --project "$PROJECT" \
  --format "value(status.url)")

echo "  ✓ Dashboard deployed: $DASHBOARD_URL"
echo ""

# ── Summary ───────────────────────────────────────────────────────────────────
echo "╔══════════════════════════════════════════════════════════╗"
echo "║     Deployment Complete!                                  ║"
echo "╠══════════════════════════════════════════════════════════╣"
echo "║  BigQuery  : $PROJECT.$DATASET"
echo "║  GCS       : gs://$VOICE_BUCKET"
echo "║  Broker    : $BROKER_URL"
echo "║  Dashboard : $DASHBOARD_URL"
echo "╠══════════════════════════════════════════════════════════╣"
echo "║  Run demo pipeline:"
echo "║  python scripts/demo_pipeline.py"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""
