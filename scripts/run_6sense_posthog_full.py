#!/usr/bin/env python3
"""
Full pipeline for 6sense × PostHog intersection companies.
Correct sequence: research → segment/score → sales_org → value_hypotheses

Each stage feeds the next:
  - research:   prefiltered/classified → classified
  - segment:    classified → scored
  - sales_org:  scored (all, no filter gap)
  - hypotheses: scored (all, no filter gap)

Usage:
    python3 run_6sense_posthog_full.py [--dry-run]
    python3 run_6sense_posthog_full.py --from=segment   # resume from segment step
    python3 run_6sense_posthog_full.py --from=sales_org
    python3 run_6sense_posthog_full.py --from=hypotheses
"""
import json
import logging
import os
import sys
import time
import subprocess
import urllib.request
import urllib.parse

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPTS_DIR)

# ── Config ─────────────────────────────────────────────────────────────────────
cfg_path = os.path.abspath(os.path.join(SCRIPTS_DIR, "..", "config.local.json"))
with open(cfg_path) as f:
    cfg = json.load(f)
SB  = cfg["supabaseUrl"]
KEY = cfg["supabaseKey"]
H   = {"apikey": KEY, "Authorization": f"Bearer {KEY}"}

DRY_RUN  = "--dry-run" in sys.argv
FROM_ARG = next((a.split("=")[1] for a in sys.argv if a.startswith("--from=")), None)
STAGES   = ["research", "segment", "sales_org", "hypotheses"]
START_IDX = STAGES.index(FROM_ARG) if FROM_ARG in STAGES else 0

def sb_get(path, params=None):
    url = f"{SB}/rest/v1/{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers=H)
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())

def sb_patch_bulk(ids, data):
    hdrs = dict(H); hdrs["Content-Type"] = "application/json"; hdrs["Prefer"] = "return=minimal"
    for i in range(0, len(ids), 200):
        batch = ids[i:i+200]
        ids_str = "(" + ",".join(f'"{x}"' for x in batch) + ")"
        req = urllib.request.Request(
            f"{SB}/rest/v1/companies?id=in.{ids_str}",
            data=json.dumps(data).encode(), headers=hdrs, method="PATCH")
        urllib.request.urlopen(req)
        time.sleep(0.05)

def get_all(table, params):
    rows, offset = [], 0
    while True:
        p = dict(params); p.update({"limit": 1000, "offset": offset})
        batch = sb_get(table, p)
        if not batch or not isinstance(batch, list): break
        rows.extend(batch); offset += 1000
        if len(batch) < 1000: break
    return rows

def run_script(script_name, extra_args=None):
    cmd = [sys.executable, os.path.join(SCRIPTS_DIR, script_name)] + (extra_args or [])
    if DRY_RUN:
        cmd.append("--dry-run")
    logger.info(f"  Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=os.path.join(SCRIPTS_DIR, ".."))
    if result.returncode not in (0, None):
        logger.warning(f"  {script_name} exited with code {result.returncode}")
    return result.returncode

def target_ids():
    cos = get_all("companies", {"source": "eq.6sense", "has_posthog": "eq.true", "select": "id"})
    return [c["id"] for c in cos]

def count_with_hyp(ids):
    s = set()
    for i in range(0, len(ids), 200):
        batch = ids[i:i+200]
        ids_str = "(" + ",".join(f'"{x}"' for x in batch) + ")"
        rows = sb_get("company_hypotheses", {"company_id": f"in.{ids_str}", "select": "company_id"})
        for r in rows: s.add(r["company_id"])
    return len(s)

def count_with_so(ids):
    s = set()
    for i in range(0, len(ids), 200):
        batch = ids[i:i+200]
        ids_str = "(" + ",".join(f'"{x}"' for x in batch) + ")"
        rows = sb_get("company_sales_org", {"company_id": f"in.{ids_str}", "select": "company_id"})
        for r in rows: s.add(r["company_id"])
    return len(s)

# ── Fetch targets ──────────────────────────────────────────────────────────────
logger.info("=== 6sense × PostHog Full Pipeline ===")
companies = get_all("companies", {
    "source": "eq.6sense", "has_posthog": "eq.true",
    "select": "id,domain,name,research_status,research_raw"
})
logger.info(f"Target companies: {len(companies)}")
by_status = {}
for c in companies:
    s = c.get("research_status") or "raw"
    by_status.setdefault(s, []).append(c)
for s, lst in sorted(by_status.items(), key=lambda x: -len(x[1])):
    has_raw = sum(1 for c in lst if c.get("research_raw"))
    logger.info(f"  {s}: {len(lst)} (with scrape data: {has_raw})")

all_ids = [c["id"] for c in companies]

# ══════════════════════════════════════════════════════════════════════════════
# STAGE 1: RESEARCH
# Processes: prefiltered, classified (and any skip/raw bumped to prefiltered)
# Output: all become classified
# ══════════════════════════════════════════════════════════════════════════════
if START_IDX <= 0:
    logger.info("\n[1/4] RESEARCH")
    needs_research = [c for c in companies if not c.get("research_raw")]
    logger.info(f"  Companies without scrape data: {len(needs_research)}")

    if needs_research:
        # Bump skip/raw to prefiltered so run_research.py picks them up
        to_bump = [c["id"] for c in needs_research if c.get("research_status") in ("skip", "raw", None)]
        if to_bump:
            logger.info(f"  Bumping {len(to_bump)} skip/raw → prefiltered")
            if not DRY_RUN:
                sb_patch_bulk(to_bump, {"research_status": "prefiltered"})
            time.sleep(1)

        run_script("run_research.py", [
            f"--limit={len(needs_research) + 10}",
            "--source=6sense",
            "--statuses=prefiltered,classified",
        ])

        # Wait and verify
        time.sleep(3)
        recheck = get_all("companies", {
            "source": "eq.6sense", "has_posthog": "eq.true",
            "select": "id,research_status,research_raw"
        })
        still_missing = [c for c in recheck if not c.get("research_raw")]
        logger.info(f"  Still without research_raw after run: {len(still_missing)}")
    else:
        logger.info("  All have scrape data — skipping")

# ══════════════════════════════════════════════════════════════════════════════
# STAGE 2: SEGMENT / SCORE
# Processes: classified companies → scored
# ══════════════════════════════════════════════════════════════════════════════
if START_IDX <= 1:
    logger.info("\n[2/4] SEGMENT / SCORE")
    # Reload after research
    companies = get_all("companies", {
        "source": "eq.6sense", "has_posthog": "eq.true",
        "select": "id,domain,research_status"
    })
    needs_scoring = [c for c in companies if c.get("research_status") == "classified"]
    logger.info(f"  Classified (need scoring): {len(needs_scoring)}")

    if needs_scoring:
        run_script("run_segment.py", [f"--limit={len(needs_scoring) + 10}"])
        time.sleep(3)
        recheck = get_all("companies", {
            "source": "eq.6sense", "has_posthog": "eq.true",
            "select": "id,research_status"
        })
        still_classified = sum(1 for c in recheck if c.get("research_status") == "classified")
        scored_now = sum(1 for c in recheck if c.get("research_status") == "scored")
        logger.info(f"  After segment: scored={scored_now}, still classified={still_classified}")
    else:
        logger.info("  No classified companies — skipping")

# ══════════════════════════════════════════════════════════════════════════════
# STAGE 3: SALES ORG
# Processes: all scored companies missing sales_org
# ══════════════════════════════════════════════════════════════════════════════
if START_IDX <= 2:
    logger.info("\n[3/4] SALES ORG")
    # Reload
    companies = get_all("companies", {
        "source": "eq.6sense", "has_posthog": "eq.true",
        "select": "id,research_status"
    })
    all_ids = [c["id"] for c in companies]
    scored = [c for c in companies if c.get("research_status") == "scored"]
    so_existing = count_with_so(all_ids)
    needs_so = len(scored) - so_existing
    logger.info(f"  Scored: {len(scored)} | Have sales org: {so_existing} | Need: {max(0,needs_so)}")

    if needs_so > 0:
        run_script("run_sales_org.py", [f"--limit={len(scored) + 10}"])
        time.sleep(3)
        so_after = count_with_so(all_ids)
        logger.info(f"  Sales org after run: {so_after}/{len(all_ids)}")
    else:
        logger.info("  All scored companies have sales org — skipping")

# ══════════════════════════════════════════════════════════════════════════════
# STAGE 4: VALUE HYPOTHESES
# Processes: all scored companies missing hypotheses
# ══════════════════════════════════════════════════════════════════════════════
if START_IDX <= 3:
    logger.info("\n[4/4] VALUE HYPOTHESES")
    companies = get_all("companies", {
        "source": "eq.6sense", "has_posthog": "eq.true",
        "select": "id,research_status"
    })
    all_ids = [c["id"] for c in companies]
    scored = [c for c in companies if c.get("research_status") == "scored"]
    hyp_existing = count_with_hyp(all_ids)
    needs_hyp = len(scored) - hyp_existing
    logger.info(f"  Scored: {len(scored)} | Have hypotheses: {hyp_existing} | Need: {max(0,needs_hyp)}")

    if needs_hyp > 0:
        run_script("run_value_hypothesis.py", [f"--limit={len(scored) + 10}"])
        time.sleep(3)
        hyp_after = count_with_hyp(all_ids)
        logger.info(f"  Hypotheses after run: {hyp_after}/{len(all_ids)}")
    else:
        logger.info("  All scored companies have hypotheses — skipping")

# ══════════════════════════════════════════════════════════════════════════════
# FINAL REPORT
# ══════════════════════════════════════════════════════════════════════════════
logger.info("\n" + "="*60)
logger.info("PIPELINE COMPLETE — Final state")
logger.info("="*60)
final = get_all("companies", {
    "source": "eq.6sense", "has_posthog": "eq.true",
    "select": "id,domain,name,research_status"
})
all_ids_final = [c["id"] for c in final]
status_counts = {}
for c in final:
    s = c.get("research_status") or "raw"
    status_counts[s] = status_counts.get(s, 0) + 1
for s, n in sorted(status_counts.items(), key=lambda x: -x[1]):
    logger.info(f"  {s}: {n}")
logger.info(f"  With hypotheses: {count_with_hyp(all_ids_final)}/{len(final)}")
logger.info(f"  With sales org:  {count_with_so(all_ids_final)}/{len(final)}")
logger.info("="*60)
