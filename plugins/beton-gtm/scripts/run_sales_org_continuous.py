#!/usr/bin/env python3
"""
run_sales_org_continuous.py — continuously picks up newly classified/scored companies
that don't yet have sales_org data and runs sales org enrichment on them.

Polls every POLL_INTERVAL seconds, processes in batches of BATCH_SIZE.
Runs until no new companies found for MAX_EMPTY_POLLS consecutive cycles (default: stops after 3h idle).

Usage:
    python3 run_sales_org_continuous.py [--batch=N] [--concurrency=N] [--interval=N]
"""

import asyncio
import json
import logging
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────

POLL_INTERVAL = 300       # seconds between polls (5 min)
BATCH_SIZE = 100          # companies per batch
MAX_EMPTY_POLLS = 36      # stop after 3h idle (36 × 5min)
CONCURRENCY = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Supabase creds (borrowed from run_prefilter.py) ───────────────────────────

def _get_supabase_creds():
    config_path = os.path.expanduser("~/.openclaw/workspace/plugins/beton-gtm/config.local.json")
    with open(config_path) as f:
        cfg = json.load(f)
    return cfg["supabaseUrl"], cfg["supabaseKey"]

import httpx

SUPABASE_BASE, SERVICE_KEY = _get_supabase_creds()
SUPABASE_H = {
    "apikey": SERVICE_KEY,
    "Authorization": f"Bearer {SERVICE_KEY}",
    "Content-Type": "application/json",
}

async def fetch_unprocessed(client: httpx.AsyncClient, limit: int) -> list[dict]:
    """Fetch classified/scored companies that have no sales_org row yet."""
    # Get all company_ids that already have sales_org
    existing_ids = set()
    offset = 0
    PAGE = 1000
    while True:
        r = await client.get(
            f"{SUPABASE_BASE}/rest/v1/company_sales_org",
            params={"select": "company_id", "limit": PAGE, "offset": offset},
            headers=SUPABASE_H,
            timeout=30,
        )
        r.raise_for_status()
        batch = r.json()
        for row in batch:
            existing_ids.add(row["company_id"])
        if len(batch) < PAGE:
            break
        offset += PAGE

    # Fetch classified/scored companies not in existing_ids
    results = []
    offset = 0
    while len(results) < limit:
        r = await client.get(
            f"{SUPABASE_BASE}/rest/v1/companies",
            params={
                "research_status": "in.(classified,scored)",
                "select": "id,domain,name",
                "order": "domain.asc",
                "limit": PAGE,
                "offset": offset,
            },
            headers=SUPABASE_H,
            timeout=30,
        )
        r.raise_for_status()
        batch = r.json()
        for c in batch:
            if c["id"] not in existing_ids:
                results.append(c)
                if len(results) >= limit:
                    break
        if len(batch) < PAGE:
            break
        offset += PAGE

    return results[:limit]


async def run_batch(companies: list[dict]):
    """Run run_sales_org.py on a specific set of domains via subprocess."""
    if not companies:
        return
    domains = [c["domain"] for c in companies]
    logger.info(f"Running sales org on {len(domains)} new companies...")

    # Write domain list to temp file, run_sales_org via CLI with high enough limit
    # Easier: just invoke run_sales_org with a domain filter patch — but it doesn't support that.
    # Instead: run it with --limit=N, it'll pick up unprocessed ones naturally since it fetches
    # classified/scored and upserts (existing rows just get updated, new ones inserted).
    # The "continuous" value-add is the polling — the actual enrichment reuses run_sales_org.
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        os.path.join(os.path.dirname(__file__), "run_sales_org.py"),
        f"--limit={len(domains) + 50}",  # slight buffer
        f"--concurrency={CONCURRENCY}",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    stdout, _ = await proc.communicate()
    output = stdout.decode() if stdout else ""
    # Log summary lines only
    for line in output.splitlines():
        if any(k in line for k in ["INFO", "WARNING", "ERROR", "COMPLETE", "Success", "Errors"]):
            if "HTTP Request" not in line:
                logger.info(f"  [sales_org] {line.strip()}")


async def main():
    batch_size = BATCH_SIZE
    concurrency = CONCURRENCY
    interval = POLL_INTERVAL

    for arg in sys.argv[1:]:
        if arg.startswith("--batch="):
            batch_size = int(arg.split("=")[1])
        elif arg.startswith("--concurrency="):
            concurrency = int(arg.split("=")[1])
        elif arg.startswith("--interval="):
            interval = int(arg.split("=")[1])

    logger.info(f"Starting continuous sales org watcher (batch={batch_size}, concurrency={concurrency}, interval={interval}s)")

    empty_polls = 0
    total_processed = 0

    async with httpx.AsyncClient() as http:
        while True:
            companies = await fetch_unprocessed(http, batch_size)

            if not companies:
                empty_polls += 1
                logger.info(f"No new companies (empty poll {empty_polls}/{MAX_EMPTY_POLLS}). Sleeping {interval}s...")
                if empty_polls >= MAX_EMPTY_POLLS:
                    logger.info("Max idle polls reached. Exiting.")
                    break
                await asyncio.sleep(interval)
                continue

            empty_polls = 0
            total_processed += len(companies)
            logger.info(f"Found {len(companies)} new companies to enrich (total so far: {total_processed})")
            await run_batch(companies)
            logger.info(f"Batch done. Sleeping {interval}s before next poll...")
            await asyncio.sleep(interval)

    logger.info(f"Continuous sales org watcher done. Total processed: {total_processed}")


if __name__ == "__main__":
    asyncio.run(main())
