#!/bin/bash
# Firecrawl health check + auto-recovery
# Detects both hard failures AND queue saturation (high 408 rate)
# Run via cron every 15 min

FIRECRAWL_IP="34.122.195.243"
FIRECRAWL_URL="http://${FIRECRAWL_IP}:3002"
SSH_KEY="$HOME/.ssh/google_compute_engine"
SSH_USER="nadyyym"
LOG="/tmp/firecrawl_health.log"
RESEARCH_LOG="/tmp/research_full_run.log"

echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Checking Firecrawl health..." >> "$LOG"

# ── Check 1: connectivity via map endpoint ────────────────────────────────────
RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" --connect-timeout 8 --max-time 15 \
  -X POST "${FIRECRAWL_URL}/v1/map" \
  -H "Content-Type: application/json" \
  -d '{"url":"https://example.com"}' 2>/dev/null)

# ── Check 2: 408 saturation — last 50 scrape responses ───────────────────────
SATURATED=0
if [ -f "$RESEARCH_LOG" ]; then
  RECENT=$(tail -50 "$RESEARCH_LOG")
  COUNT_408=$(echo "$RECENT" | grep -c "408 Request Timeout" 2>/dev/null); COUNT_408=${COUNT_408:-0}
  COUNT_200=$(echo "$RECENT" | grep -c "HTTP/1.1 200 OK" 2>/dev/null); COUNT_200=${COUNT_200:-0}
  # Saturated if >30 408s in last 50 lines AND fewer than 5 successes
  if [ "$COUNT_408" -gt 30 ] && [ "$COUNT_200" -lt 5 ]; then
    SATURATED=1
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] ⚠️ Queue saturated (408s: $COUNT_408, 200s: $COUNT_200)" >> "$LOG"
  fi
fi

# ── Healthy: connectivity OK and not saturated ────────────────────────────────
if [ "$RESPONSE" = "200" ] && [ "$SATURATED" -eq 0 ]; then
  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] ✅ Healthy (HTTP $RESPONSE)" >> "$LOG"
  exit 0
fi

# ── Recovery needed ───────────────────────────────────────────────────────────
if [ "$RESPONSE" != "200" ]; then
  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] ❌ Unhealthy (HTTP $RESPONSE) — restarting..." >> "$LOG"
else
  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] ❌ Saturated — restarting to clear queue..." >> "$LOG"
fi

# SSH in and restart docker compose
ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=10 -o IdentitiesOnly=yes \
  "${SSH_USER}@${FIRECRAWL_IP}" \
  "cd /opt/firecrawl && sudo docker compose restart" >> "$LOG" 2>&1

sleep 35

# Re-check
RESPONSE2=$(curl -s -o /dev/null -w "%{http_code}" --connect-timeout 8 --max-time 15 \
  -X POST "${FIRECRAWL_URL}/v1/map" \
  -H "Content-Type: application/json" \
  -d '{"url":"https://example.com"}' 2>/dev/null)

if [ "$RESPONSE2" = "200" ]; then
  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] ✅ Recovery successful" >> "$LOG"
  exit 0
else
  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] ❌ Recovery failed (HTTP $RESPONSE2)" >> "$LOG"
  exit 1
fi
