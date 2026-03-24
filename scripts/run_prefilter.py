#!/usr/bin/env python3
"""
GTM Prefilter — Skill 1 of the Beton GTM pipeline.

Fast homepage-only check: is the domain alive and not parked?
Uses direct httpx (no Firecrawl, no proxy) — just HTTP GET with short timeout.
LinkedIn enrichment is a separate downstream skill.

Updates Supabase: research_status → 'prefiltered' or 'skip'
"""

import httpx
import re
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────
SERVICE_KEY = (
    "YOUR_SUPABASE_SERVICE_KEY"
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImFteWd0d29xdWpsdWVwaWJjbmZzIiwicm9sZSI6"
    "InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3Mzg0OTIxMywiZXhwIjoyMDg5NDI1MjEzfQ."
    "YOUR_SUPABASE_KEY_SIG"
)
SUPABASE_BASE = "YOUR_SUPABASE_URL"
SUPABASE_H = {
    "apikey": SERVICE_KEY,
    "Authorization": f"Bearer {SERVICE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal",
}

MAX_WORKERS = 20        # high concurrency — lightweight requests
HTTP_TIMEOUT = 8        # seconds — fast fail
MIN_CONTENT_LEN = 200   # bytes — anything shorter is an error/redirect loop
LOGS_DIR = ".//logs/prefilter"

PARKED_SIGNALS = [
    "this domain is for sale",
    "buy this domain",
    "sedo.com",
    "godaddy",
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
    "domain has expired",
    "this web page is parked",
    "namecheap.com/logo",
    "domain is available",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
    "Accept-Language": "en-US,en;q=0.9",
}

# ── Domain helpers ────────────────────────────────────────────────────────────

uuid_re = re.compile(r"^[0-9a-f\-]{36}", re.I)
VENDOR_SUBDOMAINS = (
    "lovableproject.com", "createdusercontent.com", "arena.site",
    "aweber.com", "vuetifyjs.com",
)

MULTI_PART_TLDS = (
    ".co.uk", ".com.br", ".com.au", ".co.jp", ".co.nz",
    ".co.za", ".com.mx", ".com.ar",
)

def is_clean(domain: str) -> bool:
    if uuid_re.match(domain): return False
    if domain.startswith("_"): return False
    if any(domain.endswith(v) for v in VENDOR_SUBDOMAINS): return False
    parts = domain.split(".")
    if len(parts) < 2: return False
    # Allow country-code two-part TLDs (e.g. foo.com.br = 3 parts but valid root)
    is_cc_tld = any(domain.endswith(tld) for tld in MULTI_PART_TLDS)
    max_parts = 3 if is_cc_tld else 2
    if len(parts) > max_parts: return False
    return True

def get_domains(limit: int = 100, offset: int = 0, source: str = None):
    """Paginate through Supabase (max 1000 rows/page) and collect `limit` clean raw domains."""
    PAGE_SIZE = 1000
    collected = []
    db_offset = offset

    source_param = f"&source=eq.{source}" if source else ""

    while len(collected) < limit:
        r = httpx.get(
            f"{SUPABASE_BASE}/rest/v1/companies"
            f"?select=id,domain&research_status=eq.raw{source_param}&order=domain.asc"
            f"&limit={PAGE_SIZE}&offset={db_offset}",
            headers=SUPABASE_H, timeout=20,
        )
        r.raise_for_status()
        page = r.json()
        if not page:
            break  # exhausted all raw domains
        for c in page:
            if is_clean(c["domain"]):
                collected.append(c)
                if len(collected) >= limit:
                    break
        db_offset += PAGE_SIZE

    return collected

# ── HTTP check ────────────────────────────────────────────────────────────────

def check_homepage(domain: str) -> dict:
    """Try https then http. Returns {ok, content, error}"""
    for scheme in ("https", "http"):
        url = f"{scheme}://{domain}"
        try:
            r = httpx.get(
                url, headers=HEADERS, timeout=HTTP_TIMEOUT,
                follow_redirects=True,
            )
            content = r.text[:4000]  # only need first 4KB to detect parked
            if r.status_code >= 400:
                continue  # try http fallback
            return {"ok": True, "content": content, "status": r.status_code, "url": str(r.url)}
        except (httpx.TimeoutException, httpx.ConnectError, httpx.TooManyRedirects):
            continue
        except Exception as e:
            continue
    return {"ok": False, "content": "", "error": "unreachable"}

def is_parked(content: str) -> bool:
    low = content.lower()
    return any(sig in low for sig in PARKED_SIGNALS)

# ── Supabase helpers ──────────────────────────────────────────────────────────

def set_status(company_id: str, status: str):
    r = httpx.patch(
        f"{SUPABASE_BASE}/rest/v1/companies?id=eq.{company_id}",
        headers=SUPABASE_H,
        json={"research_status": status},
        timeout=10,
    )
    r.raise_for_status()

# ── Core logic ────────────────────────────────────────────────────────────────

def process_domain(company: dict) -> dict:
    domain = company["domain"]
    company_id = company["id"]

    result = {
        "domain": domain,
        "company_id": company_id,
        "status": None,
        "skip_reason": None,
        "notes": [],
    }

    homepage = check_homepage(domain)

    if not homepage["ok"]:
        result["status"] = "skip"
        result["skip_reason"] = "unreachable"
    elif len(homepage["content"].strip()) < MIN_CONTENT_LEN:
        result["status"] = "skip"
        result["skip_reason"] = "empty_page"
    elif is_parked(homepage["content"]):
        result["status"] = "skip"
        result["skip_reason"] = "parked"
    else:
        result["status"] = "prefiltered"

    try:
        set_status(company_id, result["status"])
    except Exception as e:
        result["notes"].append(f"db_error:{e}")

    # Save raw log
    try:
        import os
        safe_domain = domain.replace("/", "_").replace(":", "_")
        log_path = os.path.join(LOGS_DIR, f"{safe_domain}.json")
        with open(log_path, "w") as lf:
            json.dump({
                "domain": domain,
                "company_id": company_id,
                "checked_at": datetime.utcnow().isoformat(),
                "status": result["status"],
                "skip_reason": result.get("skip_reason"),
                "homepage_url": homepage.get("url"),
                "http_status": homepage.get("status"),
                "raw_content": homepage.get("content", "")[:5000],
                "error": homepage.get("error"),
            }, lf, indent=2)
    except Exception:
        pass

    return result

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=100, help="Number of domains to process")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--source", type=str, default=None, help="Filter by source (e.g. 6sense)")
    args = parser.parse_args()

    print(f"[{datetime.utcnow().isoformat()}] Fetching {args.limit} domains (source={args.source})...")
    companies = get_domains(limit=args.limit, offset=args.offset, source=args.source)
    print(f"  → {len(companies)} clean raw domains")

    passed, skipped, errors = [], [], []
    skip_reasons = {}
    start = datetime.utcnow()

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(process_domain, c): c for c in companies}
        done = 0
        for future in as_completed(futures):
            done += 1
            try:
                r = future.result()
                if r["status"] == "prefiltered":
                    passed.append(r["domain"])
                    if done % 50 == 0 or done <= 20:
                        print(f"  [{done}/{len(companies)}] ✅ {r['domain']}")
                else:
                    skipped.append(r["domain"])
                    skip_reasons[r["domain"]] = r["skip_reason"]
                    if done % 50 == 0 or done <= 20:
                        print(f"  [{done}/{len(companies)}] ❌ {r['domain']} ({r['skip_reason']})")
            except Exception as e:
                c = futures[future]
                errors.append({"domain": c["domain"], "error": str(e)})

    elapsed = (datetime.utcnow() - start).total_seconds()
    rate = len(companies) / elapsed * 60

    print("\n" + "=" * 60)
    print("PREFILTER SUMMARY")
    print("=" * 60)
    print(f"Total processed : {len(companies)}")
    print(f"Passed          : {len(passed)} ({100*len(passed)//max(len(companies),1)}%)")
    print(f"Skipped         : {len(skipped)}")
    print(f"Errors          : {len(errors)}")
    print(f"Elapsed         : {elapsed:.1f}s  ({rate:.0f} domains/min)")

    reason_counts = {}
    for r in skip_reasons.values():
        reason_counts[r] = reason_counts.get(r, 0) + 1
    print("\nSkip reasons:")
    for reason, count in sorted(reason_counts.items(), key=lambda x: -x[1]):
        print(f"  {reason}: {count}")

    output = {
        "run_at": datetime.utcnow().isoformat(),
        "total": len(companies),
        "passed_count": len(passed),
        "skipped_count": len(skipped),
        "elapsed_seconds": elapsed,
        "domains_per_minute": rate,
        "passed": passed,
        "skipped": skipped,
        "skip_reasons": skip_reasons,
        "errors": errors,
    }
    out_path = ".//scripts/prefilter_results.json"
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nResults → {out_path}")

if __name__ == "__main__":
    main()
