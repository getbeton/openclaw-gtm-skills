#!/usr/bin/env python3
"""
run_scrape.py — Stage 1: Firecrawl scraping only.

Crawls prefiltered companies via Firecrawl, including a dedicated recursive
career page crawl. Writes raw content to logs/research/{domain}.json and
sets research_status = 'scraped' for successful companies.

Intentionally model-free — no Claude, no Gemini. Pure IO.
run_classify.py handles the LLM step separately.

Usage:
    python3 run_scrape.py [--limit=N] [--concurrency=N] [--dry-run] [--force]

Flags:
    --limit=N        Max companies to process (default: 200)
    --concurrency=N  Parallel workers (default: 25)
    --dry-run        Scrape + write logs but skip Supabase status update
    --force          Re-scrape companies with existing fresh logs (default: skip)
"""

import asyncio
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Optional

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

def _load_supabase_creds():
    with open(os.path.expanduser("~/.openclaw/workspace/plugins/beton-gtm/scripts/run_prefilter.py")) as f:
        content = f.read()
    m = re.search(r'SERVICE_KEY\s*=\s*\(([^)]+)\)', content, re.DOTALL)
    key = "".join(re.findall(r'"([^"]*)"', m.group(1)))
    base = re.search(r'SUPABASE_BASE\s*=\s*"([^"]+)"', content).group(1)
    return base, key

SUPABASE_BASE, SUPABASE_KEY = _load_supabase_creds()
SUPABASE_H = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

def _load_firecrawl_endpoint() -> str:
    cfg_path = os.path.expanduser("~/.openclaw/workspace/integrations/firecrawl.json")
    try:
        with open(cfg_path) as f:
            return json.load(f)["base_url"].rstrip("/")
    except Exception:
        return "YOUR_FIRECRAWL_URL"

FIRECRAWL_ENDPOINT = _load_firecrawl_endpoint()
LOGS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "logs", "research")
CACHE_MAX_AGE_DAYS = 30
MAX_CONTENT_PAGES = 10
MAX_CAREER_PAGES = 30  # max sub-pages within careers crawl
CONCURRENCY = 25
LIMIT = 200
PAGE_SIZE = 500

os.makedirs(LOGS_DIR, exist_ok=True)

# ── URL scoring ───────────────────────────────────────────────────────────────

HIGH_VALUE_PATTERNS = [
    "/pricing", "/about", "/about-us", "/customers", "/case-studies",
    "/success-stories", "/product", "/features", "/platform",
    "/how-it-works", "/solutions", "/integrations",
]
SKIP_PATTERNS = [
    "/blog/", "/privacy", "/terms", "/gdpr", "/cookie",
    "/login", "/signin", "/app/", "/dashboard/", "/press",
    "/news/page",
    # careers handled separately below — NOT in skip list
]
CAREER_SEGMENTS = {"careers", "jobs", "hiring", "join", "open-roles", "work-with-us",
                   "join-us", "positions", "vacancies", "opportunities", "team"}

def is_career_url(url: str) -> bool:
    path = url.lower().split("?")[0].rstrip("/")
    segments = set(path.split("/"))
    return bool(segments & CAREER_SEGMENTS)

def score_url(url: str) -> int:
    path = url.lower().split("?")[0]
    if is_career_url(url):
        return -1  # skip in content pass; handled separately
    for skip in SKIP_PATTERNS:
        if skip in path:
            return -1
    for i, pattern in enumerate(HIGH_VALUE_PATTERNS):
        if path.rstrip("/").endswith(pattern) or f"{pattern}/" in path:
            return len(HIGH_VALUE_PATTERNS) - i
    return 0

def select_content_pages(sitemap_urls: list[str], domain: str) -> list[str]:
    homepage = f"https://{domain}"
    scored = [(score_url(u), u) for u in sitemap_urls]
    scored.sort(key=lambda x: -x[0])
    selected = [homepage]
    seen = {homepage, homepage.rstrip("/")}
    for s, url in scored:
        if len(selected) >= MAX_CONTENT_PAGES:
            break
        if s < 0:
            continue
        norm = url.rstrip("/")
        if norm not in seen and url not in seen:
            selected.append(url)
            seen.add(norm)
    return selected

# ── Cache ─────────────────────────────────────────────────────────────────────

def load_cache(domain: str) -> Optional[dict]:
    safe = domain.replace("/", "_").replace(":", "_")
    path = os.path.join(LOGS_DIR, f"{safe}.json")
    if not os.path.exists(path):
        return None
    try:
        data = json.load(open(path))
        scraped_at = data.get("scraped_at") or data.get("classified_at")
        if scraped_at:
            age_days = (time.time() - datetime.fromisoformat(scraped_at.replace("Z", "+00:00")).timestamp()) / 86400
            if age_days <= CACHE_MAX_AGE_DAYS:
                return data
    except Exception:
        pass
    return None

def save_log(domain: str, data: dict):
    safe = domain.replace("/", "_").replace(":", "_")
    path = os.path.join(LOGS_DIR, f"{safe}.json")
    with open(path, "w") as f:
        json.dump(data, f)

# ── Firecrawl helpers ─────────────────────────────────────────────────────────

async def get_sitemap(client: httpx.AsyncClient, domain: str) -> list[str]:
    try:
        r = await client.post(
            f"{FIRECRAWL_ENDPOINT}/v1/map",
            json={"url": f"https://{domain}", "limit": 100},
            timeout=30.0,
        )
        r.raise_for_status()
        data = r.json()
        urls = data.get("links") or data.get("urls") or []
        return [u for u in urls if isinstance(u, str)]
    except Exception as e:
        logger.warning(f"[{domain}] Sitemap failed: {e}")
        return []

async def scrape_page(client: httpx.AsyncClient, url: str) -> Optional[str]:
    try:
        r = await client.post(
            f"{FIRECRAWL_ENDPOINT}/v1/scrape",
            json={"url": url, "formats": ["markdown"], "onlyMainContent": True},
            timeout=45.0,
        )
        r.raise_for_status()
        data = r.json()
        md = (data.get("data") or data).get("markdown") or ""
        return md.strip() if len(md.strip()) > 100 else None
    except Exception as e:
        logger.debug(f"Scrape failed {url}: {e}")
        return None

async def crawl_careers(client: httpx.AsyncClient, domain: str, sitemap_urls: list[str]) -> tuple[list[str], str]:
    """
    Crawl careers pages recursively using Firecrawl /v1/crawl.
    First finds the careers root URL from sitemap, then crawls all sub-pages.
    Returns (career_urls_scraped, concatenated_markdown).
    """
    # Find careers root from sitemap
    career_root = None
    for url in sitemap_urls:
        if is_career_url(url):
            # Prefer shallow career page (fewer path segments)
            parts = url.rstrip("/").split("/")
            if career_root is None or len(parts) < len(career_root.rstrip("/").split("/")):
                career_root = url

    # If no career URL in sitemap, try common paths
    if not career_root:
        for path in ["/careers", "/jobs", "/join", "/work-with-us", "/hiring", "/open-roles"]:
            try:
                r = await client.head(
                    f"https://{domain}{path}",
                    follow_redirects=True, timeout=10.0,
                )
                if r.status_code < 400:
                    career_root = str(r.url)
                    break
            except Exception:
                continue

    if not career_root:
        return [], ""

    # Use Firecrawl crawl (recursive) to get all career sub-pages
    career_parts = []
    career_urls = []
    try:
        r = await client.post(
            f"{FIRECRAWL_ENDPOINT}/v1/crawl",
            json={
                "url": career_root,
                "limit": MAX_CAREER_PAGES,
                "scrapeOptions": {"formats": ["markdown"], "onlyMainContent": True},
                "includePaths": [".*"],
                "maxDepth": 3,
            },
            timeout=120.0,
        )
        r.raise_for_status()
        result = r.json()
        pages = result.get("data") or []
        for page in pages:
            url = page.get("metadata", {}).get("url") or page.get("url", "")
            md = page.get("markdown") or ""
            if md.strip() and len(md.strip()) > 50:
                career_parts.append(f"\n\n## {url}\n\n{md.strip()}")
                career_urls.append(url)
    except Exception as e:
        # Crawl endpoint failed — fall back to single scrape of root
        logger.debug(f"[{domain}] Career crawl failed ({e}), falling back to single scrape")
        md = await scrape_page(client, career_root)
        if md:
            career_parts.append(f"\n\n## {career_root}\n\n{md}")
            career_urls.append(career_root)

    return career_urls, "\n".join(career_parts)

# ── Supabase helpers ──────────────────────────────────────────────────────────

async def fetch_companies(client: httpx.AsyncClient, limit: int, force: bool) -> list[dict]:
    """Fetch prefiltered companies. With --force, also re-scrape 'scraped' companies."""
    statuses = "research_status=in.(prefiltered)"
    if force:
        statuses = "research_status=in.(prefiltered,scraped)"

    companies = []
    offset = 0
    while len(companies) < limit:
        r = await client.get(
            f"{SUPABASE_BASE}/rest/v1/companies?{statuses}"
            f"&select=id,domain,name&limit={PAGE_SIZE}&offset={offset}&order=domain.asc",
            headers=SUPABASE_H, timeout=20,
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

async def set_status(client: httpx.AsyncClient, company_id: str, status: str):
    await client.patch(
        f"{SUPABASE_BASE}/rest/v1/companies?id=eq.{company_id}",
        headers={**SUPABASE_H, "Prefer": "return=minimal"},
        json={"research_status": status},
        timeout=10,
    )

async def set_error(client: httpx.AsyncClient, company_id: str, domain: str, error: str):
    safe = domain.replace("/", "_").replace(":", "_")
    path = os.path.join(LOGS_DIR, f"{safe}.error.json")
    with open(path, "w") as f:
        json.dump({"domain": domain, "error": error, "timestamp": datetime.now(timezone.utc).isoformat()}, f)

# ── Company processor ─────────────────────────────────────────────────────────

async def process_company(
    client: httpx.AsyncClient,
    company: dict,
    semaphore: asyncio.Semaphore,
    results: list,
    dry_run: bool,
    force: bool,
):
    domain = company["domain"]
    company_id = company["id"]

    async with semaphore:
        try:
            # Skip if fresh cache exists (unless --force)
            if not force:
                cached = load_cache(domain)
                if cached and cached.get("raw_content"):
                    logger.info(f"[{domain}] Cache hit — skipping scrape")
                    results.append({"domain": domain, "status": "cached"})
                    return

            # Step 1: Sitemap
            sitemap_urls = await get_sitemap(client, domain)
            logger.info(f"[{domain}] Sitemap: {len(sitemap_urls)} URLs")

            # Step 2: Select content pages (excludes careers)
            content_pages = select_content_pages(sitemap_urls, domain)
            logger.info(f"[{domain}] Content pages: {len(content_pages)}")

            # Step 3: Scrape content pages (parallel within company)
            content_parts = []
            pages_crawled = []
            failed_urls = []

            async def scrape_one(url: str):
                md = await scrape_page(client, url)
                if md:
                    content_parts.append(f"\n\n## {url}\n\n{md}")
                    pages_crawled.append(url)
                else:
                    failed_urls.append(url)

            await asyncio.gather(*[scrape_one(url) for url in content_pages])

            if not content_parts:
                raise ValueError("No content scraped from any page")

            # Step 4: Career pages (recursive crawl)
            career_urls, career_content = await crawl_careers(client, domain, sitemap_urls)
            logger.info(f"[{domain}] Career pages: {len(career_urls)}")

            # Step 5: Write log
            now = datetime.now(timezone.utc).isoformat()
            log = {
                "domain": domain,
                "company_id": company_id,
                "scraped_at": now,
                "pages_crawled": pages_crawled,
                "career_pages_crawled": career_urls,
                "failed_urls": failed_urls,
                "raw_content": "\n".join(content_parts),
                "career_content": career_content,
                "classified_at": None,
                "classification": None,
            }
            save_log(domain, log)

            # Step 6: Update status
            if not dry_run:
                await set_status(client, company_id, "scraped")

            results.append({"domain": domain, "status": "scraped", "pages": len(pages_crawled), "career_pages": len(career_urls)})
            logger.info(f"[{domain}] ✓ scraped ({len(pages_crawled)} content + {len(career_urls)} career pages)")

        except Exception as e:
            logger.error(f"[{domain}] Error: {e}")
            await set_error(client, company_id, domain, str(e))
            results.append({"domain": domain, "status": "error", "error": str(e)})


# ── Entry point ───────────────────────────────────────────────────────────────

async def main():
    limit = LIMIT
    concurrency = CONCURRENCY
    dry_run = False
    force = False

    for arg in sys.argv[1:]:
        if arg.startswith("--limit="):
            limit = int(arg.split("=")[1])
        elif arg.startswith("--concurrency="):
            concurrency = int(arg.split("=")[1])
        elif arg == "--dry-run":
            dry_run = True
        elif arg == "--force":
            force = True

    logger.info(f"run_scrape: limit={limit}, concurrency={concurrency}, dry_run={dry_run}, force={force}")
    logger.info(f"Firecrawl: {FIRECRAWL_ENDPOINT}")

    async with httpx.AsyncClient(follow_redirects=True, timeout=60.0) as client:
        companies = await fetch_companies(client, limit, force)
        logger.info(f"Fetched {len(companies)} companies to scrape")

        if not companies:
            logger.info("Nothing to scrape.")
            return

        semaphore = asyncio.Semaphore(concurrency)
        results = []
        tasks = [process_company(client, c, semaphore, results, dry_run, force) for c in companies]
        await asyncio.gather(*tasks)

    scraped = sum(1 for r in results if r["status"] == "scraped")
    cached = sum(1 for r in results if r["status"] == "cached")
    errors = sum(1 for r in results if r["status"] == "error")

    print(f"\n{'='*50}")
    print(f"SCRAPE RUN — COMPLETE")
    print(f"{'='*50}")
    print(f"Scraped : {scraped}")
    print(f"Cached  : {cached}")
    print(f"Errors  : {errors}")
    print(f"Total   : {len(results)}")


if __name__ == "__main__":
    asyncio.run(main())
