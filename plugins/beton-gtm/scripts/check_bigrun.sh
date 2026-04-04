#!/bin/bash
# check_bigrun.sh — health check + auto-restart for the big research run
# Exits 0 = healthy, 1 = was dead (restarted), 2 = complete (no prefiltered left)

LOG=/tmp/research_bigrun_21mar.log
SCRIPT_DIR="$(dirname "$(realpath "$0")")"
PIDFILE=/tmp/run_research_bigrun.pid

# Match ONLY python3 processes (not the bash nohup wrapper)
ALL_PIDS=$(pgrep -f "python3 run_research.py" 2>/dev/null)

# Kill duplicates — keep only the newest python3 process
if [ -n "$ALL_PIDS" ]; then
    PID_COUNT=$(echo "$ALL_PIDS" | wc -l)
    if [ "$PID_COUNT" -gt 1 ]; then
        echo "WARNING: $PID_COUNT duplicate run_research processes found — killing all but newest"
        NEWEST=$(echo "$ALL_PIDS" | sort -n | tail -1)
        echo "$ALL_PIDS" | grep -v "^$NEWEST$" | xargs kill 2>/dev/null
        sleep 2
        echo "$NEWEST" > "$PIDFILE"
    fi
fi

# Read tracked PID from lockfile
TRACKED_PID=""
if [ -f "$PIDFILE" ]; then
    TRACKED_PID=$(cat "$PIDFILE")
fi

# Verify tracked PID is still alive (python3 process only)
PIDS=""
if [ -n "$TRACKED_PID" ] && kill -0 "$TRACKED_PID" 2>/dev/null; then
    PIDS="$TRACKED_PID"
else
    # Fallback: find by name
    PIDS=$(pgrep -f "python3 run_research.py" 2>/dev/null | sort -n | tail -1)
fi

if [ -n "$PIDS" ]; then
    # Process is alive — check if it made progress in last 10 min
    LAST_MODIFIED=$(stat -c %Y "$LOG" 2>/dev/null || echo 0)
    NOW=$(date +%s)
    AGE=$((NOW - LAST_MODIFIED))

    if [ "$AGE" -lt 600 ]; then
        # Active — report last few meaningful log lines
        echo "$PIDS" > "$PIDFILE"
        echo "HEALTHY (PID: $PIDS, log updated ${AGE}s ago)"
        grep -v "HTTP Request" "$LOG" | tail -3
        exit 0
    else
        echo "STALLED (log not updated in ${AGE}s) — killing and restarting"
        kill $PIDS 2>/dev/null
        rm -f "$PIDFILE"
        sleep 3
    fi
else
    # Check if we're actually done (no prefiltered companies left)
    PREFILTERED=$(python3 - <<'PYEOF'
import json, urllib.request, re, os
with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "run_prefilter.py")) as f:
    c = f.read()
m = re.search(r'SERVICE_KEY\s*=\s*\(([^)]+)\)', c, re.DOTALL)
key = "".join(re.findall(r'"([^"]*)"', m.group(1)))
base = re.search(r'SUPABASE_BASE\s*=\s*"([^"]+)"', c).group(1)
req = urllib.request.Request(
    f"{base}/rest/v1/companies?research_status=eq.prefiltered&select=id&limit=1",
    headers={"apikey": key, "Authorization": f"Bearer {key}", "Accept": "application/json", "Prefer": "count=exact"}
)
with urllib.request.urlopen(req) as r:
    cr = r.headers.get("content-range", "0/0")
    print(cr.split("/")[-1])
PYEOF
)
    if [ "$PREFILTERED" = "0" ]; then
        echo "COMPLETE — no prefiltered companies remaining"
        exit 2
    fi
    echo "DEAD (was processing ~$PREFILTERED prefiltered remaining) — restarting"
fi

# Restart
cd "$SCRIPT_DIR"
nohup python3 run_research.py --limit=50000 --concurrency=12 >> "$LOG" 2>&1 &
NEW_PID=$!
echo "$NEW_PID" > "$PIDFILE"
echo "Restarted — new PID: $NEW_PID"
exit 1
