#!/usr/bin/env python3
"""
Research script for the 6sense + PostHog cohort (122 companies).
Targets: prefiltered + skip + classified statuses.
Fallback: Yandex Search API when Firecrawl fails.

Usage:
    python3 run_research_6sense_posthog.py [--limit N] [--dry-run] [--yandex-only]
"""

import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Optional

import httpx
import anthropic

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
def _load_config() -> dict:
    config_path = os.path.join(os.path.dirname(__file__), "..", "config.local.json")
    try:
        with open(os.path.abspath(config_path)) as f:
            return json.load(f)
    except Exception:
        return {}

_cfg = _load_config()
SUPABASE_URL = _cfg.get("supabaseUrl", "")
SUPABASE_KEY = _cfg.get("supabaseKey", "")

def _load_firecrawl_endpoint() -> str:
    cfg_path = os.path.expanduser("~/.openclaw/workspace/integrations/firecrawl.json")
    try:
        with open(cfg_path) as f:
            return json.load(f)["base_url"].rstrip("/")
    except Exception:
        return "http://localhost:3002"

FIRECRAWL_ENDPOINT = _load_firecrawl_endpoint()

# Yandex Search API config — set via env or config.local.json
YANDEX_FOLDER_ID = os.getenv("YANDEX_FOLDER_ID", _cfg.get("yandexFolderId", ""))
YANDEX_API_KEY   = os.getenv("YANDEX_API_KEY",   _cfg.get("yandexApiKey",   ""))
YANDEX_SEARCH_URL = "https://yandex.com/search/xml/generative"

SUPABASE_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

CONCURRENCY = 4
LOGS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "logs", "research_6sense")
os.makedirs(LOGS_DIR, exist_ok=True)

def now_iso(): return datetime.now(timezone.utc).isoformat()

# ── Fetch cohort ──────────────────────────────────────────────────────────────
def fetch_cohort(limit: int = 0, all_statuses: bool = False) -> list[dict]:
    """Fetch 6sense + PostHog companies to research.
    
    all_statuses=True: re-run on ALL 122 (including scored/classified) for better scraping.
    all_statuses=False: only unresearched ones (prefiltered, skip, classified).
    """
    import httpx as _httpx
    params = {
        "source": "eq.6sense",
        "has_posthog": "eq.true",
        "select": "id,name,domain,research_status",
        "limit": str(limit) if limit else "200",
        "order": "research_status.asc",
    }
    if not all_statuses:
        params["research_status"] = "in.(prefiltered,skip,classified)"
    
    r = _httpx.get(f"{SUPABASE_URL}/rest/v1/companies", headers=SUPABASE_HEADERS, params=params)
    companies = r.json()
    logger.info(f"Fetched {len(companies)} companies to process (all_statuses={all_statuses})")
    return companies

def normalize_domain(domain: str) -> str:
    """Extract root domain from subdomains like dashboard.voltagepark.com → voltagepark.com"""
    # Strip known app/dashboard subdomains
    parts = domain.split(".")
    skip_prefixes = {"dashboard", "app", "console", "go", "portal", "www", "login", "api", "help"}
    while len(parts) > 2 and parts[0].lower() in skip_prefixes:
        parts = parts[1:]
    return ".".join(parts)

# ── Yandex Search fallback ────────────────────────────────────────────────────
async def yandex_search(client: httpx.AsyncClient, domain: str) -> Optional[str]:
    """Query Yandex Search API for company info. Returns combined text or None."""
    if not YANDEX_FOLDER_ID or not YANDEX_API_KEY:
        logger.warning(f"[{domain}] Yandex API not configured — skipping fallback")
        return None

    root = normalize_domain(domain)
    queries = [
        f"site:{root} product pricing customers",
        f"{root} B2B SaaS product GTM revenue team",
    ]
    texts = []
    for query in queries[:1]:  # start with 1 query per company to save quota
        try:
            resp = await client.post(
                YANDEX_SEARCH_URL,
                params={"folderid": YANDEX_FOLDER_ID, "apikey": YANDEX_API_KEY},
                json={"full_text": True, "query": query, "tld": "com"},
                timeout=15.0,
            )
            if resp.status_code == 200:
                data = resp.json()
                docs = data.get("docs", [])
                for doc in docs[:3]:
                    ft = doc.get("full_text") or doc.get("desc") or ""
                    if ft:
                        texts.append(f"[{doc.get('url','')}]\n{ft[:2000]}")
                if texts:
                    logger.info(f"[{domain}] Yandex fallback: got {len(docs)} results")
                    return "\n\n---\n\n".join(texts)
            else:
                logger.warning(f"[{domain}] Yandex returned {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            logger.warning(f"[{domain}] Yandex error: {e}")
    return None

# ── Firecrawl ─────────────────────────────────────────────────────────────────
async def firecrawl_map(client: httpx.AsyncClient, domain: str) -> list[str]:
    root = normalize_domain(domain)
    url = f"https://{root}"
    try:
        r = await client.post(
            f"{FIRECRAWL_ENDPOINT}/v1/map",
            json={"url": url, "limit": 15, "includeSubdomains": False},
            timeout=30.0,
        )
        if r.status_code == 200:
            data = r.json()
            return data.get("links", data.get("urls", []))[:15]
    except Exception as e:
        logger.debug(f"[{domain}] Firecrawl map error: {e}")
    return [url]

async def firecrawl_scrape(client: httpx.AsyncClient, url: str) -> Optional[str]:
    try:
        r = await client.post(
            f"{FIRECRAWL_ENDPOINT}/v1/scrape",
            json={"url": url, "formats": ["markdown"], "onlyMainContent": True},
            timeout=30.0,
        )
        if r.status_code == 200:
            data = r.json()
            return (data.get("data") or data).get("markdown") or (data.get("data") or data).get("content")
    except Exception as e:
        logger.debug(f"  scrape({url}): {e}")
    return None

def prioritize_urls(urls: list[str], domain: str) -> list[str]:
    priority = ["pricing", "about", "customers", "product", "solutions", "platform", "features", "team"]
    def score(u):
        u_low = u.lower()
        for i, kw in enumerate(priority):
            if kw in u_low:
                return i
        return 99
    return sorted(urls, key=score)[:8]

async def scrape_company(client: httpx.AsyncClient, domain: str) -> tuple[str, str]:
    """Returns (content, source) where source is 'firecrawl' or 'yandex'."""
    root = normalize_domain(domain)

    # Try Firecrawl
    urls = await firecrawl_map(client, domain)
    urls = prioritize_urls(urls, root)
    pages = []
    for url in urls[:6]:
        md = await firecrawl_scrape(client, url)
        if md and len(md) > 100:
            pages.append(md[:3000])
        if len(pages) >= 4:
            break

    if pages:
        return "\n\n---\n\n".join(pages), "firecrawl"

    # Firecrawl failed → Yandex fallback
    logger.info(f"[{domain}] Firecrawl got 0 pages — trying Yandex fallback")
    yandex_content = await yandex_search(client, domain)
    if yandex_content:
        return yandex_content, "yandex"

    return "", "failed"

# ── Claude classification ─────────────────────────────────────────────────────
CLASSIFY_PROMPT = """You are a B2B SaaS analyst. Analyze this company and output a JSON object.

Company domain: {domain}

Scraped content:
{content}

Output ONLY a valid JSON object with these exact fields:
{{
  "b2b": true/false,
  "saas": true/false,
  "gtm_motion": "PLG" | "SLG" | "hybrid" | "channel" | null,
  "vertical": "short industry label (e.g. devtools, fintech, healthcare AI, data infra)",
  "business_model": "subscription" | "usage-based" | "transactional" | "marketplace" | "services" | null,
  "sells_to": "enterprise" | "mid-market" | "SMB" | "developer" | "consumer" | null,
  "description": "1 sentence describing what they do and for whom",
  "evidence": "key facts that support your classification"
}}

Be concise. Output only valid JSON, nothing else."""

async def classify_company(content: str, domain: str, claude_client: anthropic.AsyncAnthropic) -> Optional[dict]:
    if not content or len(content) < 50:
        return None
    prompt = CLASSIFY_PROMPT.format(domain=domain, content=content[:8000])
    try:
        msg = await claude_client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}]
        )
        raw = msg.content[0].text.strip()
        # Extract JSON
        if "```" in raw:
            raw = raw.split("```")[1].replace("json", "").strip()
        return json.loads(raw)
    except Exception as e:
        logger.warning(f"[{domain}] Claude error: {e}")
        return None

# ── Supabase writes ───────────────────────────────────────────────────────────
async def save_results(client: httpx.AsyncClient, company_id: str, domain: str,
                       classification: Optional[dict], pages_crawled: list[str],
                       source: str, content: str):
    now = now_iso()

    # Update company research_status
    new_status = "classified" if classification else "research_failed"
    await client.patch(
        f"{SUPABASE_URL}/rest/v1/companies?id=eq.{company_id}",
        headers=SUPABASE_HEADERS,
        json={"research_status": new_status, "updated_at": now},
    )

    # Save research_raw log
    log_data = {
        "scraped_at": now,
        "pages_crawled": pages_crawled,
        "source": source,
        "char_count": len(content),
    }
    await client.patch(
        f"{SUPABASE_URL}/rest/v1/companies?id=eq.{company_id}",
        headers=SUPABASE_HEADERS,
        json={"research_raw": json.dumps(log_data), "enriched_at": now},
    )

    if not classification:
        return

    # Upsert company_classification
    cl_payload = {
        "company_id": company_id,
        "b2b": classification.get("b2b"),
        "saas": classification.get("saas"),
        "gtm_motion": classification.get("gtm_motion"),
        "vertical": classification.get("vertical"),
        "business_model": classification.get("business_model"),
        "sells_to": classification.get("sells_to"),
        "description": classification.get("description"),
        "evidence": classification.get("evidence"),
        "classified_at": now,
        "updated_at": now,
    }
    await client.post(
        f"{SUPABASE_URL}/rest/v1/company_classification",
        headers={**SUPABASE_HEADERS, "Prefer": "resolution=merge-duplicates,return=minimal"},
        json=cl_payload,
    )

# ── Main worker ───────────────────────────────────────────────────────────────
sem = asyncio.Semaphore(CONCURRENCY)

async def process_company(company: dict, http: httpx.AsyncClient, claude: anthropic.AsyncAnthropic,
                          dry_run: bool = False) -> dict:
    company_id = company["id"]
    domain = company["domain"]
    name = company.get("name", domain)
    status = company.get("research_status", "")

    async with sem:
        try:
            logger.info(f"[{domain}] Processing (status={status})...")

            content, source = await scrape_company(http, domain)

            if not content:
                logger.warning(f"[{domain}] No content from any source")
                if not dry_run:
                    await save_results(http, company_id, domain, None, [], "failed", "")
                return {"domain": domain, "status": "failed", "source": "none"}

            classification = await classify_company(content, domain, claude)

            if not dry_run:
                await save_results(http, company_id, domain, classification,
                                   [f"https://{normalize_domain(domain)}"], source, content)

            logger.info(f"[{domain}] Done — {source}, classified={classification is not None}, "
                        f"b2b={classification.get('b2b') if classification else '?'}, "
                        f"vertical={classification.get('vertical') if classification else '?'}")

            return {"domain": domain, "status": "ok", "source": source, "classification": classification}

        except Exception as e:
            logger.error(f"[{domain}] Unhandled error: {e}")
            return {"domain": domain, "status": "error", "error": str(e)}

# ── Entry point ───────────────────────────────────────────────────────────────
async def main():
    dry_run = "--dry-run" in sys.argv
    all_statuses = "--all" in sys.argv  # re-research already scored/classified companies too
    limit_arg = next((int(a.split("=")[1]) for a in sys.argv if a.startswith("--limit=")), 0)
    yandex_only = "--yandex-only" in sys.argv

    if not YANDEX_FOLDER_ID or not YANDEX_API_KEY:
        logger.warning("⚠️  Yandex Search API not configured — Firecrawl only.")

    companies = fetch_cohort(limit=limit_arg, all_statuses=all_statuses)
    logger.info(f"Starting research for {len(companies)} companies "
                f"(dry_run={dry_run}, yandex_configured={bool(YANDEX_FOLDER_ID)})")

    claude = anthropic.AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY", _cfg.get("anthropicApiKey", "")))

    async with httpx.AsyncClient(timeout=60.0) as http:
        tasks = [process_company(c, http, claude, dry_run=dry_run) for c in companies]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Summary
    ok = sum(1 for r in results if isinstance(r, dict) and r.get("status") == "ok")
    failed = sum(1 for r in results if isinstance(r, dict) and r.get("status") == "failed")
    errors = sum(1 for r in results if not isinstance(r, dict))
    yandex_used = sum(1 for r in results if isinstance(r, dict) and r.get("source") == "yandex")
    logger.info(f"\n{'='*50}")
    logger.info(f"Done: {ok} ok, {failed} no-content, {errors} errors, {yandex_used} used Yandex fallback")

if __name__ == "__main__":
    asyncio.run(main())
