"""
run_sales_org.py — GTM Sales Org enrichment

Uses cached Firecrawl logs from run_research.py first (zero extra scrapes when possible),
falls back to live Firecrawl scrape only for pages not already in the cache.

Extracts open roles, infers headcount, detects tech stack, and writes results to
Supabase companies.sales_org and companies.tech_stack.

Usage:
    python3 run_sales_org.py [--limit=N] [--concurrency=N] [--dry-run]

Defaults:
    --limit=100
    --concurrency=5
    --dry-run=False
"""

import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Optional

import anthropic
import httpx

# ── Research log cache ────────────────────────────────────────────────────────

RESEARCH_LOGS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "logs", "research")

def load_research_cache(domain: str) -> Optional[dict]:
    """Load existing research log for domain if available."""
    safe = domain.replace("/", "_").replace(":", "_")
    path = os.path.join(RESEARCH_LOGS_DIR, f"{safe}.json")
    if os.path.exists(path):
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            pass
    return None

CAREERS_URL_SEGMENTS = {"careers", "jobs", "hiring", "join", "open-roles", "work-with-us", "join-us"}

def is_careers_url(url: str) -> bool:
    """Match on path segments only — avoids false positives like /blog/career-paths."""
    from urllib.parse import urlparse
    path = urlparse(url).path.lower()
    segments = [s for s in path.split("/") if s]
    # Only match if a top-level segment (depth ≤ 2) is an exact keyword
    return any(seg in CAREERS_URL_SEGMENTS for seg in segments[:2])

def extract_careers_from_cache(cache: dict) -> tuple[Optional[str], str]:
    """
    Find careers-related content from the research cache.
    Returns (careers_url_or_None, careers_content_only).
    
    IMPORTANT: Only returns content from the careers page section of raw_content.
    Falls back to None content if no careers page was scraped — do NOT fall back
    to full raw_content, which causes massive false positives from nav/footer mentions.
    """
    pages = cache.get("pages_crawled") or []
    raw = cache.get("raw_content") or ""

    careers_url = None
    for url in pages:
        if is_careers_url(url):
            careers_url = url
            break

    if not careers_url:
        return None, ""

    # Extract only the careers page section from the concatenated raw_content
    # Pages are separated by "\n\n## {url}\n\n" markers
    marker = f"## {careers_url}"
    if marker in raw:
        start = raw.index(marker)
        # Find next page marker after this one
        next_marker = raw.find("\n\n## http", start + len(marker))
        if next_marker > 0:
            careers_content = raw[start:next_marker]
        else:
            careers_content = raw[start:]
        return careers_url, careers_content

    return careers_url, ""

# ── Config ────────────────────────────────────────────────────────────────────

def _load_config():
    base = os.path.expanduser("~/.openclaw/workspace")
    with open(f"{base}/integrations/firecrawl.json") as f:
        fc = json.load(f)
    with open(f"{base}/integrations/soax.json") as f:
        soax = json.load(f)
    return fc["base_url"].rstrip("/"), soax

FIRECRAWL_ENDPOINT, SOAX_CONFIG = _load_config()

def _get_supabase_creds():
    config_path = os.path.expanduser("~/.openclaw/workspace/plugins/beton-gtm/config.local.json")
    with open(config_path) as f:
        cfg = json.load(f)
    return cfg["supabaseUrl"], cfg["supabaseKey"]

SUPABASE_BASE, SERVICE_KEY = _get_supabase_creds()
SUPABASE_H = {
    "apikey": SERVICE_KEY,
    "Authorization": f"Bearer {SERVICE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal",
}

def _get_anthropic_client():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        try:
            profiles_path = os.path.expanduser("~/.openclaw/config/llm-profiles.json")
            with open(profiles_path) as f:
                profiles = json.load(f)
            api_key = profiles.get("anthropic:manual", {}).get("token")
        except Exception:
            pass
    return anthropic.Anthropic(api_key=api_key)

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Soax proxy ────────────────────────────────────────────────────────────────

def _soax_proxy_url() -> Optional[str]:
    try:
        host = SOAX_CONFIG.get("host", "proxy.soax.com")
        port = SOAX_CONFIG.get("port", 5000)
        user = SOAX_CONFIG.get("username") or SOAX_CONFIG.get("user", "")
        pwd  = SOAX_CONFIG.get("password") or SOAX_CONFIG.get("pass", "")
        if user and pwd:
            return f"http://{user}:{pwd}@{host}:{port}"
        return f"http://{host}:{port}"
    except Exception:
        return None

# ── Careers URL candidates ────────────────────────────────────────────────────

CAREERS_PATHS = ["/careers", "/jobs", "/work-with-us", "/join-us", "/join", "/hiring", "/open-roles"]

ROLE_KEYWORDS = {
    "sales": ["account executive", "ae", "sdr", "bdr", "business development", "sales rep",
              "sales manager", "sales director", "vp sales", "account manager"],
    "revops": ["revenue operations", "revops", "sales operations", "sales ops", "crm admin",
               "gtm operations", "gtm ops"],
    "cs": ["customer success", "csm", "onboarding", "implementation", "customer support"],
    "marketing": ["marketing", "demand gen", "growth", "content", "seo", "lifecycle"],
    "engineering": ["engineer", "developer", "platform", "backend", "frontend", "devops", "sre"],
}

TECH_KEYWORDS = {
    "crm": ["salesforce", "hubspot", "pipedrive", "close.io", "close crm", "zoho crm", "attio"],
    "sales_engagement": ["outreach", "salesloft", "apollo", "groove", "mixmax", "yesware", "reply.io"],
    "data_enrichment": ["zoominfo", "apollo.io", "clay", "clearbit", "lusha", "linkedin sales nav"],
    "analytics": ["tableau", "looker", "metabase", "sisense", "mixpanel", "amplitude", "posthog"],
    "warehouse": ["snowflake", "bigquery", "redshift", "dbt", "databricks"],
}

# ── Firecrawl helpers ─────────────────────────────────────────────────────────

async def firecrawl_scrape(
    client: httpx.AsyncClient,
    url: str,
    use_proxy: bool = False,
    timeout: int = 30,
) -> Optional[str]:
    payload = {"url": url, "formats": ["markdown"], "onlyMainContent": True}
    if use_proxy:
        proxy_url = _soax_proxy_url()
        if proxy_url:
            payload["proxy"] = proxy_url

    for attempt in range(3):
        try:
            r = await client.post(
                f"{FIRECRAWL_ENDPOINT}/v1/scrape",
                json=payload,
                timeout=timeout,
            )
            if r.status_code == 200:
                data = r.json()
                return data.get("data", {}).get("markdown") or ""
            if r.status_code in (408, 429, 500, 502, 503) and attempt < 2:
                await asyncio.sleep(2 ** attempt)
                continue
            return None
        except Exception:
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
    return None


async def find_careers_page(client: httpx.AsyncClient, domain: str) -> Optional[str]:
    """Try standard careers paths, then Firecrawl map if none work."""
    for path in CAREERS_PATHS:
        url = f"https://{domain}{path}"
        try:
            r = await client.head(url, timeout=8, follow_redirects=True)
            if r.status_code < 400:
                return url
        except Exception:
            pass

    # Fallback: use Firecrawl map to find jobs/careers URL
    try:
        r = await client.post(
            f"{FIRECRAWL_ENDPOINT}/v1/map",
            json={"url": f"https://{domain}", "limit": 50},
            timeout=20,
        )
        if r.status_code == 200:
            urls = r.json().get("links") or r.json().get("urls") or []
            for u in urls:
                if is_careers_url(u):
                    return u
    except Exception:
        pass

    return None


# ── Role + tech extraction ────────────────────────────────────────────────────

def extract_roles_and_tech(text: str) -> dict:
    """
    Parse careers page text for open roles and tech stack.
    
    Role counting strategy: split into lines/paragraphs and count lines that 
    contain a role keyword — not raw substring count. This way a nav menu with
    "Sales" counts as 1, not N mentions per page. A job listing page with 
    "Account Executive - London", "Account Executive - Berlin" counts as 2.
    """
    import re
    text_lower = text.lower()
    role_counts = {fn: 0 for fn in ROLE_KEYWORDS}
    tech_stack = {}

    # Split into lines, count lines that match each role function
    # A line must contain a role keyword AND look like a job title line
    # (short line, or line with location/remote/apply signal nearby)
    lines = [l.strip() for l in re.split(r'[\n\r]+', text) if l.strip()]
    
    for line in lines:
        line_lower = line.lower()
        # Skip nav/footer/button lines (very short or contain UI words)
        if len(line) < 5 or any(skip in line_lower for skip in ['contact', 'talk to', 'click', 'submit', '©', 'cookie', 'privacy']):
            continue
        for fn, keywords in ROLE_KEYWORDS.items():
            if any(kw in line_lower for kw in keywords):
                role_counts[fn] += 1
                break  # count each line once even if multiple keywords match

    # Detect tech stack from full text (mentions anywhere = valid signal)
    for category, keywords in TECH_KEYWORDS.items():
        found = []
        for kw in keywords:
            if kw in text_lower:
                found.append(kw)
        if found:
            tech_stack[category] = found[0]

    # Store actual line counts (1 line ≈ 1 role mention)
    open_sales_roles = role_counts["sales"] if role_counts["sales"] > 0 else None
    open_revops_roles = role_counts["revops"] if role_counts["revops"] > 0 else None
    open_cs_roles = role_counts["cs"] if role_counts["cs"] > 0 else None

    # Hiring signal
    total_roles = sum(role_counts.values())
    if role_counts["revops"] > 0:
        hiring_signal = "building_revops"
    elif role_counts["sales"] > 2:
        hiring_signal = "scaling_sales"
    elif role_counts["sales"] > 0:
        hiring_signal = "growing_team"
    elif total_roles == 0:
        hiring_signal = "no_open_roles"
    else:
        hiring_signal = "growing_team"

    return {
        "sales_org": {
            "openSalesRoles": open_sales_roles,
            "openRevopsRoles": open_revops_roles,
            "openCsRoles": open_cs_roles,
            "roleCounts": role_counts,
            "hiringSignal": hiring_signal,
            "careersPageFound": True,
        },
        "tech_stack": tech_stack,
    }


# ── LinkedIn scrape ───────────────────────────────────────────────────────────

async def scrape_linkedin_jobs(client: httpx.AsyncClient, company_name: str) -> Optional[str]:
    """Scrape LinkedIn jobs page with Soax proxy."""
    query = company_name.replace(" ", "+")
    url = f"https://www.linkedin.com/jobs/search/?keywords={query}"
    text = await firecrawl_scrape(client, url, use_proxy=True, timeout=40)
    return text


# ── Supabase helpers ──────────────────────────────────────────────────────────

async def fetch_companies(client: httpx.AsyncClient, limit: int) -> list[dict]:
    companies = []
    offset = 0
    PAGE_SIZE = 100

    while len(companies) < limit:
        r = await client.get(
            f"{SUPABASE_BASE}/rest/v1/companies"
            f"?research_status=eq.scored&select=id,domain,name"
            f"&order=domain.asc&limit={PAGE_SIZE}&offset={offset}",
            headers={**SUPABASE_H, "Prefer": "count=exact"},
            timeout=20,
        )
        r.raise_for_status()
        page = r.json()
        if not page:
            break
        companies.extend(page)
        if len(page) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    return companies[:limit]


async def upsert_sales_org(client: httpx.AsyncClient, company_id: str, sales_org: dict, tech_stack: dict):
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()

    # Upsert company_sales_org
    r = await client.post(
        f"{SUPABASE_BASE}/rest/v1/company_sales_org?on_conflict=company_id",
        headers={**SUPABASE_H, "Prefer": "resolution=merge-duplicates,return=minimal"},
        json={
            "company_id": company_id,
            "open_sales_roles": sales_org.get("openSalesRoles"),
            "open_revops_roles": sales_org.get("openRevopsRoles"),
            "open_cs_roles": sales_org.get("openCsRoles"),
            "hiring_signal": sales_org.get("hiringSignal") not in (None, "no_open_roles", "no_data"),
            "enriched_at": now,
        },
        timeout=15,
    )
    r.raise_for_status()

    # Upsert company_tech_stack
    if tech_stack:
        r2 = await client.post(
            f"{SUPABASE_BASE}/rest/v1/company_tech_stack?on_conflict=company_id",
            headers={**SUPABASE_H, "Prefer": "resolution=merge-duplicates,return=minimal"},
            json={
                "company_id": company_id,
                "crm": tech_stack.get("crm"),
                "sales_engagement_tool": tech_stack.get("sales_engagement"),
                "data_warehouse": tech_stack.get("warehouse"),
                "analytics": tech_stack.get("analytics"),
                "enriched_at": now,
            },
            timeout=15,
        )
        r2.raise_for_status()


# ── Main processor ────────────────────────────────────────────────────────────

async def process_company(
    http: httpx.AsyncClient,
    company: dict,
    semaphore: asyncio.Semaphore,
    results: list,
    dry_run: bool = False,
):
    domain = company["domain"]
    company_id = company["id"]
    name = company.get("name") or domain

    async with semaphore:
        logger.info(f"[{domain}] Starting sales org enrichment...")

        careers_text = ""
        careers_url = None
        source = "none"

        # 1. Try research cache first (free — no Firecrawl calls)
        cache = load_research_cache(domain)
        if cache:
            careers_url, cached_content = extract_careers_from_cache(cache)
            if cached_content:
                careers_text = cached_content
                source = f"cache({'careers' if careers_url else 'raw'})"
                logger.info(f"[{domain}] Using cache ({len(careers_text)} chars, careers_url={careers_url})")

        # 2. If no cache content, fall back to live Firecrawl scrape
        if not careers_text:
            try:
                careers_url = await find_careers_page(http, domain)
                if careers_url:
                    logger.info(f"[{domain}] Live scrape: {careers_url}")
                    careers_text = await firecrawl_scrape(http, careers_url) or ""
                    source = "firecrawl_live"
                else:
                    logger.info(f"[{domain}] No careers page found (live)")
                    source = "none"
            except Exception as e:
                logger.warning(f"[{domain}] Careers scrape error: {e}")

        # 3. Parse combined text
        combined = careers_text.strip()

        if not combined:
            result = {
                "domain": domain,
                "status": "no_content",
                "source": source,
                "sales_org": {"careersPageFound": False, "hiringSignal": "no_data"},
                "tech_stack": {},
            }
        else:
            parsed = extract_roles_and_tech(combined)
            parsed["sales_org"]["careersPageFound"] = careers_url is not None
            parsed["sales_org"]["careersUrl"] = careers_url
            result = {
                "domain": domain,
                "status": "ok",
                "source": source,
                **parsed,
            }

        results.append(result)

        if not dry_run:
            try:
                await upsert_sales_org(http, company_id, result["sales_org"], result["tech_stack"])
                logger.info(f"[{domain}] ✓ Saved — hiring: {result['sales_org'].get('hiringSignal')} | tech: {result['tech_stack']}")
            except Exception as e:
                logger.error(f"[{domain}] Supabase error: {e}")
                result["status"] = "supabase_error"
        else:
            logger.info(f"[{domain}] [DRY RUN] Would save: {json.dumps(result, indent=2)[:200]}")


# ── Entry point ───────────────────────────────────────────────────────────────

async def main():
    limit = 100
    concurrency = 5
    dry_run = False
    rerun = False  # if True, skip exclusion filter and re-enrich all companies

    for arg in sys.argv[1:]:
        if arg.startswith("--limit="):
            limit = int(arg.split("=")[1])
        elif arg.startswith("--concurrency="):
            concurrency = int(arg.split("=")[1])
        elif arg == "--dry-run":
            dry_run = True
        elif arg == "--rerun":
            rerun = True

    logger.info(f"Starting sales org run (limit={limit}, concurrency={concurrency}, dry_run={dry_run})")

    while True:
        async with httpx.AsyncClient(follow_redirects=True) as http:
            companies = await fetch_companies(http, limit)
            logger.info(f"Fetched {len(companies)} researched companies to process")

            # Exclude companies that already have a sales org row (skip if --rerun)
            if companies and not rerun:
                all_ids = [c['id'] for c in companies]
                # Fetch existing sales org IDs in this batch
                chunk_size = 200
                existing_ids = set()
                for i in range(0, len(all_ids), chunk_size):
                    chunk = ','.join(all_ids[i:i+chunk_size])
                    r = await http.get(
                        f"{SUPABASE_BASE}/rest/v1/company_sales_org?company_id=in.({chunk})&select=company_id&limit=500",
                        headers=SUPABASE_H, timeout=20
                    )
                    existing_ids.update(row['company_id'] for row in r.json())
                before = len(companies)
                companies = [c for c in companies if c['id'] not in existing_ids]
                logger.info(f"Skipping {before - len(companies)} already enriched; {len(companies)} to process")

            if not companies:
                logger.info("Nothing to process — sleeping 5 minutes before next check")
                await asyncio.sleep(300)
                continue

            semaphore = asyncio.Semaphore(concurrency)
            results = []

            tasks = [
                process_company(http, c, semaphore, results, dry_run)
                for c in companies
            ]
            await asyncio.gather(*tasks)

    # Summary
    ok = sum(1 for r in results if r["status"] == "ok")
    no_content = sum(1 for r in results if r["status"] == "no_content")
    errors = sum(1 for r in results if "error" in r["status"])
    hiring_signals = {}
    for r in results:
        sig = r.get("sales_org", {}).get("hiringSignal", "unknown")
        hiring_signals[sig] = hiring_signals.get(sig, 0) + 1

    print(f"\n{'='*50}")
    print(f"SALES ORG RUN — COMPLETE")
    print(f"{'='*50}")
    print(f"Total processed : {len(results)}")
    print(f"Success         : {ok}")
    print(f"No content      : {no_content}")
    print(f"Errors          : {errors}")
    print(f"\nHiring signals:")
    for sig, count in sorted(hiring_signals.items(), key=lambda x: -x[1]):
        print(f"  {sig}: {count}")

    out_path = os.path.join(os.path.dirname(__file__), "sales_org_results.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults → {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
