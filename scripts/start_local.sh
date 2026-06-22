#!/bin/bash
# Start the SBI MF Outbound Agent services locally.
# Run this in one terminal, then run_demo.py in another.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

echo "============================================"
echo "  SBI MF Outbound Agent — Local Services"
echo "============================================"
echo ""

# Check required env vars
if [ -z "$GOOGLE_API_KEY" ] && [ -z "$GCP_PROJECT" ]; then
  echo "ERROR: Set GOOGLE_API_KEY or GCP_PROJECT in .env"
  exit 1
fi

if [ -z "$TWILIO_ACCOUNT_SID" ]; then
  echo "WARNING: TWILIO_ACCOUNT_SID not set — calls will be simulated."
  echo "         Set it in .env to place real calls."
  echo ""
fi

# Start SBI MF Voice MCP (port 8005)
echo "Starting SBI MF Voice MCP on :8005..."
python -m uvicorn voice.sbi_mf_voice_mcp:mcp.streamable_http_app \
  --host 0.0.0.0 --port 8005 --reload \
  --app-dir "$PROJECT_ROOT" &
VOICE_MCP_PID=$!

sleep 2

# Start LiveAPI Broker (port 8010)
echo "Starting LiveAPI Broker on :8010..."
python -m uvicorn voice.liveapi_broker:app \
  --host 0.0.0.0 --port 8010 --reload \
  --app-dir "$PROJECT_ROOT" &
BROKER_PID=$!

sleep 2

# Start Distributor Dashboard (port 8020)
echo "Starting Distributor Dashboard on :8020..."
python -m uvicorn dashboard.server:app \
  --host 0.0.0.0 --port 8020 --reload \
  --app-dir "$PROJECT_ROOT" &
DASHBOARD_PID=$!

echo ""
echo "Services running:"
echo "  Voice MCP       → http://localhost:8005"
echo "  LiveAPI Broker  → http://localhost:8010"
echo "  Dashboard       → http://localhost:8020/?arn=ARN-12345"
echo ""
echo "Health checks:"
echo "  curl http://localhost:8010/health"
echo "  curl http://localhost:8020/health"
echo ""
echo "Run a demo call (no MCP server needed):"
echo "  python scripts/run_demo.py --type sip_renewal"
echo "  python scripts/run_demo.py --type sip_renewal --mobile +91XXXXXXXXXX"
echo ""
echo "Run the full pipeline:"
echo "  python -m orchestrator.run_pipeline --dry-run"
echo "  python -m orchestrator.run_pipeline --max 5"
echo ""
echo "Press Ctrl+C to stop all services."
echo "============================================"

# Cleanup on exit
trap "echo 'Stopping services...'; kill $VOICE_MCP_PID $BROKER_PID $DASHBOARD_PID 2>/dev/null; exit 0" INT TERM

wait
