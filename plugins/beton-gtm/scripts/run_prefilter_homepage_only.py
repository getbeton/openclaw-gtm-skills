#!/usr/bin/env python3
"""
GTM Prefilter — Homepage-only pass.

Checks each domain:
  1. Homepage loads (not timeout, not HTTP error)
  2. Not parked / for-sale / empty

No LinkedIn. No employee count. Just: is this a real, live company site?

Updates Supabase: research_status → 'prefiltered' or 'skip'

Usage:
  python3 run_prefilter_homepage_only.py           # process timed-out domains from last run
  python3 run_prefilter_homepage_only.py --fresh   # re-fetch all raw domains
"""

import os
import httpx
import re
import time
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────
def _load_supabase_creds():
    import json as _j, os as _os
    cfg = _os.path.expanduser("~/.openclaw/workspace/plugins/beton-gtm/config.local.json")
    with open(cfg) as _f:
        d = _j.load(_f)
    return d["supabaseUrl"], d["supabaseKey"]
_SUPA_URL, SERVICE_KEY = _load_supabase_creds()
SUPABASE_BASE = _SUPA_URL
SUPABASE_H = {
    "apikey": SERVICE_KEY,
    "Authorization": f"Bearer {SERVICE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal",
}

def _load_firecrawl_base():
    import json as _j, os as _os
    try:
        with open(_os.path.expanduser("~/.openclaw/workspace/integrations/firecrawl.json")) as _f:
            return _j.load(_f)["base_url"].rstrip("/")
    except Exception:
        return "http://34.122.195.243:3002"
FIRECRAWL_BASE = _load_firecrawl_base()
FIRECRAWL_TIMEOUT = 45  # seconds — generous for slow sites

MAX_WORKERS = 8  # parallel scrapes — each still writes to Supabase immediately after completion
BATCH_SIZE = 500  # how many raw domains to fetch per Supabase page (internal pagination only)

PARKED_SIGNALS = [
    "this domain is for sale",
    "buy this domain",
    "sedo",
    "godaddy placeholder",
    "domain parking",
    "parked domain",
    "hugedomains",
    "dan.com",
    "undeveloped.com",
    "afternic",
    "namecheap parking",
    "domain for sale",
    "inquire about this domain",
    "this domain may be for sale",
    "register this domain",
    "get this domain",
    "domain is available",
    "is for sale",
]

PREV_RESULTS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "prefilter_results.json")

# ── Domain helpers ─────────────────────────────────────────────────────────────

uuid_re = re.compile(r"^[0-9a-f\-]{36}", re.I)
VENDOR_SUBDOMAINS = (
    "lovableproject.com", "createdusercontent.com", "arena.site",
    "aweber.com", "vuetifyjs.com",
)


def is_clean(domain):
    if uuid_re.match(domain): return False
    if domain.startswith("_"): return False
    if any(domain.endswith(v) for v in VENDOR_SUBDOMAINS): return False
    parts = domain.split(".")
    if len(parts) < 2: return False
    return True


def get_timeout_domains():
    """Load the timed-out domains from the previous run's results file."""
    with open(PREV_RESULTS) as f:
        prev = json.load(f)
    timed_out = [
        d for d, reason in prev["skip_reasons"].items()
        if "timeout" in reason
    ]
    return timed_out


def fetch_company_ids(domains):
    """Look up company IDs for a list of domains."""
    # Supabase in() filter
    domain_list = ",".join(f'"{d}"' for d in domains)
    r = httpx.get(
        f"{SUPABASE_BASE}/rest/v1/companies"
        f"?select=id,domain&domain=in.({domain_list})&limit=500",
        headers=SUPABASE_H,
        timeout=20,
    )
    r.raise_for_status()
    return r.json()  # [{id, domain}, ...]


def get_fresh_raw_domains(limit=500):
    r = httpx.get(
        f"{SUPABASE_BASE}/rest/v1/companies"
        f"?select=id,domain&research_status=eq.raw&order=domain.asc&limit={limit}",
        headers=SUPABASE_H, timeout=20,
    )
    r.raise_for_status()
    companies = r.json()
    return [c for c in companies if is_clean(c["domain"])]


# ── Firecrawl ─────────────────────────────────────────────────────────────────

def firecrawl_scrape(url, attempt=1):
    try:
        r = httpx.post(
            f"{FIRECRAWL_BASE}/v1/scrape",
            json={"url": url, "formats": ["markdown"]},
            timeout=FIRECRAWL_TIMEOUT,
        )
        if r.status_code == 200:
            md = r.json().get("data", {}).get("markdown", "") or ""
            return {"success": True, "markdown": md}
        return {"success": False, "markdown": "", "error": f"HTTP {r.status_code}"}
    except httpx.TimeoutException:
        if attempt == 1:
            time.sleep(3)
            return firecrawl_scrape(url, attempt=2)
        return {"success": False, "markdown": "", "error": "timeout"}
    except Exception as e:
        return {"success": False, "markdown": "", "error": str(e)}


def is_parked(md):
    low = md.lower()
    return any(sig in low for sig in PARKED_SIGNALS)


def is_empty(md):
    return len(md.strip()) < 100


# ── Supabase updates ──────────────────────────────────────────────────────────

def set_status(company_id, status):
    r = httpx.patch(
        f"{SUPABASE_BASE}/rest/v1/companies?id=eq.{company_id}",
        headers=SUPABASE_H,
        json={"research_status": status},
        timeout=10,
    )
    r.raise_for_status()


# ── Per-domain logic ──────────────────────────────────────────────────────────

def process(company):
    domain = company["domain"]
    cid = company["id"]

    # Try https first, fall back to http
    res = firecrawl_scrape(f"https://{domain}")
    if not res["success"]:
        res = firecrawl_scrape(f"http://{domain}")

    if not res["success"]:
        reason = f"homepage_unreachable:{res.get('error', 'unknown')}"
        set_status(cid, "skip")
        return {"domain": domain, "status": "skip", "reason": reason}

    md = res["markdown"]

    if is_empty(md):
        set_status(cid, "skip")
        return {"domain": domain, "status": "skip", "reason": "homepage_empty"}

    if is_parked(md):
        set_status(cid, "skip")
        return {"domain": domain, "status": "skip", "reason": "parked_page"}

    set_status(cid, "prefiltered")
    return {"domain": domain, "status": "prefiltered", "reason": None}


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    """
    Continuous loop mode: fetch raw domains in pages, process ONE at a time,
    write to Supabase immediately after each. No data loss on crash.
    Runs until no raw companies remain.
    """
    total_passed = 0
    total_skipped = 0
    total_processed = 0
    run_start = datetime.utcnow()

    print("Mode: continuous loop — processing all raw domains one at a time")
    print("Each company is written to Supabase immediately. Safe to interrupt anytime.\n")

    batch_num = 0
    while True:
        batch_num += 1
        companies = get_fresh_raw_domains(limit=BATCH_SIZE)

        if not companies:
            print(f"\n✅ All raw domains processed! Total: {total_processed} "
                  f"(passed={total_passed}, skipped={total_skipped})")
            elapsed = (datetime.utcnow() - run_start).total_seconds()
            print(f"Total elapsed: {elapsed/3600:.1f}h")
            break

        print(f"\n── Batch {batch_num}: fetched {len(companies)} raw domains (workers={MAX_WORKERS}) ──")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {pool.submit(process, c): c for c in companies}
            for future in as_completed(futures):
                total_processed += 1
                company = futures[future]
                try:
                    r = future.result()
                    domain, status, reason = r["domain"], r["status"], r["reason"]
                    if status == "prefiltered":
                        total_passed += 1
                        print(f"  [{total_processed}] ✅  {domain}", flush=True)
                    else:
                        total_skipped += 1
                        print(f"  [{total_processed}] ❌  {domain}  ({reason})", flush=True)
                except Exception as e:
                    print(f"  [{total_processed}] ERROR  {company['domain']}: {e}", flush=True)

        # Brief pause between batches
        time.sleep(1)


if __name__ == "__main__":
    main()
