#!/usr/bin/env python3
"""
run_research_combined.py — Combined research + sales org in one Claude pass.

Scrapes homepage, pricing, careers, and job listings for each prefiltered company.
Passes all content to Claude in a single prompt that outputs both:
  - Classification (b2b, saas, gtm, vertical, etc.)
  - Sales org signals (hiring, headcounts, tech stack)

Writes to: company_classification + company_sales_org + updates research_status to 'classified'

Usage:
    python3 run_research_combined.py [--limit=N] [--concurrency=N] [--dry-run] [--reclassify]

Defaults:
    --limit=100
    --concurrency=3
"""

import asyncio
import gc
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Optional

import httpx
import anthropic

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def _load_local_config() -> dict:
    config_path = os.path.join(SCRIPT_DIR, "..", "config.local.json")
    try:
        with open(os.path.abspath(config_path)) as f:
            return json.load(f)
    except (FileNotFoundError, ValueError):
        return {}

_lc = _load_local_config()
SUPABASE_URL = os.getenv("SUPABASE_URL", _lc.get("supabaseUrl", ""))
SUPABASE_KEY = os.getenv("SUPABASE_KEY", _lc.get("supabaseKey", ""))

def _load_firecrawl_endpoint() -> str:
    cfg_path = os.path.expanduser("~/.openclaw/workspace/integrations/firecrawl.json")
    try:
        with open(cfg_path) as f:
            return json.load(f)["base_url"].rstrip("/")
    except Exception:
        return "http://localhost:3002"

FIRECRAWL_ENDPOINT = _load_firecrawl_endpoint()
CONCURRENCY = 3
COMPANY_LIMIT = 100
LOGS_DIR = os.path.join(SCRIPT_DIR, "..", "logs", "research")
LOG_MAX_AGE_DAYS = 30

os.makedirs(LOGS_DIR, exist_ok=True)

SUPABASE_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

# ── Combined Claude prompt ────────────────────────────────────────────────────

COMBINED_PROMPT = """You are a B2B sales intelligence analyst. Given website content for a company, extract TWO sets of structured data.

Output ONLY valid JSON with exactly these top-level keys: "classification" and "sales_org".

=== CLASSIFICATION ===
{{
  "companyResolvedName": "Official company name",
  "primaryWebsite": "canonical domain without https://",
  "b2b": true or false,
  "saas": true or false,
  "gtmMotion": "PLG" or "SLG" or "hybrid" or "D2C" or null,
  "vertical": "short vertical label (e.g. revenue intelligence, HR tech, fintech, devtools)",
  "businessModel": "subscription" or "usage-based" or "freemium" or "marketplace" or "ecommerce" or "services" or "other",
  "sellsTo": "SMB" or "mid-market" or "enterprise" or "consumer" or "developer" or "unknown",
  "pricingModel": "per-seat" or "flat-rate" or "usage-based" or "tiered" or "custom-only" or "freemium" or "transactional" or "unknown",
  "keyFeatures": ["feature1", "feature2", "feature3"],
  "description": "1-2 sentence plain English description of what the company does",
  "evidence": {{
    "gtmMotion": "quote or observation from the content",
    "vertical": "quote or observation from the content",
    "sellsTo": "quote or observation from the content"
  }}
}}

GTM motion rules:
- PLG: users can sign up and start without a sales call. Self-serve is primary.
- SLG: must talk to sales. "Contact sales"/"Request a demo" as primary/only CTA.
- hybrid: BOTH self-serve AND sales paths clearly coexist.
- D2C: physical goods direct to consumers.
- null: only if page has no usable content.

=== SALES_ORG ===
{{
  "hiringSignal": "building_revops" or "scaling_sales" or "growing_team" or "no_open_roles" or "no_data",
  "openSalesRoles": integer or null,
  "openRevopsRoles": integer or null,
  "openCsRoles": integer or null,
  "careersPageFound": true or false,
  "techStack": {{
    "crm": "salesforce" or "hubspot" or "pipedrive" or null,
    "analytics": "posthog" or "mixpanel" or "amplitude" or "segment" or null,
    "dataWarehouse": "snowflake" or "bigquery" or "redshift" or "databricks" or null,
    "automation": "outreach" or "salesloft" or "apollo" or "hubspot" or null
  }},
  "salesSignals": ["signal1", "signal2"]
}}

Hiring signal rules:
- "building_revops": any RevOps/Sales Ops/GTM Ops roles open
- "scaling_sales": 3+ AE/SDR/BDR roles open
- "growing_team": 1-2 sales roles open
- "no_open_roles": careers page found but 0 relevant roles
- "no_data": no careers page found in the content

Sales signals to look for (include any you observe):
- "self_serve_signup", "usage_based_pricing", "expansion_revenue_model",
- "land_and_expand", "plg_with_sales_assist", "founder_led_sales",
- "posthog_in_stack", "segment_in_stack", "no_crm_mentioned",
- "revops_gap" (sales roles but no RevOps), "high_eng_ratio"

Website content:
{content}"""

# ── Helpers ───────────────────────────────────────────────────────────────────

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def extract_json_from_text(text: str) -> Optional[dict]:
    text = text.strip()
    # Try direct parse
    try:
        return json.loads(text)
    except Exception:
        pass
    # Try extracting from markdown code block
    match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except Exception:
            pass
    # Try finding outermost JSON object
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except Exception:
            pass
    return None

def _get_anthropic_client() -> anthropic.Anthropic:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        token_path = os.path.expanduser("~/.openclaw/agents/main/agent/auth-profiles.json")
        if os.path.exists(token_path):
            profiles = json.load(open(token_path)).get("profiles", {})
            api_key = profiles.get("anthropic:manual", {}).get("token")
    if not api_key:
        raise RuntimeError("No Anthropic API key found.")
    return anthropic.Anthropic(api_key=api_key)

# ── Firecrawl helpers ─────────────────────────────────────────────────────────

async def firecrawl_scrape(http: httpx.AsyncClient, url: str, timeout: int = 30) -> Optional[str]:
    try:
        r = await http.post(
            f"{FIRECRAWL_ENDPOINT}/v1/scrape",
            json={"url": url, "formats": ["markdown"], "onlyMainContent": True},
            timeout=timeout,
        )
        if r.status_code == 200:
            data = r.json()
            return data.get("data", {}).get("markdown") or data.get("markdown")
    except Exception as e:
        logger.debug(f"Firecrawl scrape failed for {url}: {e}")
    return None

async def get_sitemap(http: httpx.AsyncClient, domain: str) -> list[str]:
    try:
        r = await http.post(
            f"{FIRECRAWL_ENDPOINT}/v1/map",
            json={"url": f"https://{domain}", "limit": 50},
            timeout=30,
        )
        if r.status_code == 200:
            data = r.json()
            return data.get("links") or []
    except Exception:
        pass
    return []

PRIORITY_PATHS = ["pricing", "price", "plans", "careers", "jobs", "hiring",
                  "about", "features", "product", "solutions", "customers", "blog"]

def select_pages(sitemap_urls: list[str], domain: str, max_pages: int = 10) -> list[str]:
    base = f"https://{domain}"
    selected = [base]
    seen_paths = {"/"}
    priority = []
    others = []
    for url in sitemap_urls:
        if domain not in url:
            continue
        try:
            from urllib.parse import urlparse
            path = urlparse(url).path.rstrip("/") or "/"
        except Exception:
            continue
        if path in seen_paths:
            continue
        seen_paths.add(path)
        parts = [p for p in path.split("/") if p]
        if not parts:
            continue
        if any(kw in parts[0].lower() for kw in PRIORITY_PATHS):
            priority.append(url)
        else:
            others.append(url)
    selected += priority[:6]
    selected += others[:max_pages - len(selected)]
    return selected[:max_pages]

# ── Claude ────────────────────────────────────────────────────────────────────

def analyze_with_claude(content: str, domain: str) -> Optional[dict]:
    client = _get_anthropic_client()
    prompt = COMBINED_PROMPT.replace("{content}", content[:18000])

    for model, label in [("claude-sonnet-4-6", "Sonnet"), ("claude-haiku-4-5", "Haiku")]:
        try:
            response = client.messages.create(
                model=model,
                max_tokens=2048,
                messages=[{"role": "user", "content": prompt}],
            )
            result = extract_json_from_text(response.content[0].text)
            if result and "classification" in result and "sales_org" in result:
                return result
            logger.warning(f"[{domain}] {label}: JSON parse failed or missing keys, retrying...")
            retry_prompt = "Respond with ONLY a raw JSON object (no markdown). " + prompt
            r2 = client.messages.create(
                model=model,
                max_tokens=2048,
                messages=[{"role": "user", "content": retry_prompt}],
            )
            result2 = extract_json_from_text(r2.content[0].text)
            if result2 and "classification" in result2 and "sales_org" in result2:
                return result2
            logger.warning(f"[{domain}] {label}: retry failed, trying next model...")
        except Exception as e:
            logger.warning(f"[{domain}] {label} error: {e} — trying next model...")

    logger.error(f"[{domain}] All models failed")
    return None

# ── Supabase writes ───────────────────────────────────────────────────────────

async def update_company(http: httpx.AsyncClient, company_id: str, data: dict):
    r = await http.patch(
        f"{SUPABASE_URL}/rest/v1/companies?id=eq.{company_id}",
        headers=SUPABASE_HEADERS,
        json=data,
        timeout=15,
    )
    r.raise_for_status()

async def upsert_classification(http: httpx.AsyncClient, data: dict):
    r = await http.post(
        f"{SUPABASE_URL}/rest/v1/company_classification?on_conflict=company_id",
        headers={**SUPABASE_HEADERS, "Prefer": "resolution=merge-duplicates,return=minimal"},
        json=data,
        timeout=15,
    )
    r.raise_for_status()

async def upsert_sales_org(http: httpx.AsyncClient, company_id: str, so: dict, tech: dict):
    r = await http.post(
        f"{SUPABASE_URL}/rest/v1/company_sales_org?on_conflict=company_id",
        headers={**SUPABASE_HEADERS, "Prefer": "resolution=merge-duplicates,return=minimal"},
        json={
            "company_id": company_id,
            "open_sales_roles": so.get("openSalesRoles"),
            "open_revops_roles": so.get("openRevopsRoles"),
            "open_cs_roles": so.get("openCsRoles"),
            "hiring_signal": so.get("hiringSignal") not in (None, "no_open_roles", "no_data"),
            "hiring_signal_type": so.get("hiringSignal"),
            "careers_page_found": so.get("careersPageFound", False),
            "tech_stack": json.dumps(tech) if tech else None,
            "sales_signals": json.dumps(so.get("salesSignals", [])),
            "enriched_at": now_iso(),
        },
        timeout=15,
    )
    r.raise_for_status()

# ── Fetch companies ───────────────────────────────────────────────────────────

async def fetch_companies(http: httpx.AsyncClient, limit: int, reclassify: bool = False) -> list[dict]:
    companies = []
    offset = 0
    PAGE_SIZE = 100
    statuses = "in.(classified,scored)" if reclassify else "eq.prefiltered"

    while len(companies) < limit:
        r = await http.get(
            f"{SUPABASE_URL}/rest/v1/companies"
            f"?research_status={statuses}&select=id,domain,name"
            f"&order=domain.asc&limit={PAGE_SIZE}&offset={offset}",
            headers={**SUPABASE_HEADERS, "Prefer": "count=exact"},
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

# ── Per-company processor ─────────────────────────────────────────────────────

def _load_cached_scrape(domain: str) -> Optional[dict]:
    safe = domain.replace("/", "_").replace(":", "_")
    path = os.path.join(LOGS_DIR, f"{safe}.json")
    if not os.path.exists(path):
        return None
    age = (time.time() - os.path.getmtime(path)) / 86400
    if age > LOG_MAX_AGE_DAYS:
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None

async def process_company(
    http: httpx.AsyncClient,
    company: dict,
    semaphore: asyncio.Semaphore,
    results: list,
    dry_run: bool = False,
    reclassify: bool = False,
):
    domain = company["domain"]
    company_id = company["id"]

    async with semaphore:
        pages_crawled = []
        failed_urls = []
        concatenated = ""

        try:
            cached = _load_cached_scrape(domain)
            if cached and cached.get("raw_content") and not reclassify:
                logger.info(f"[{domain}] Using cached scrape")
                concatenated = cached["raw_content"]
                pages_crawled = cached.get("pages_crawled", [])
            else:
                logger.info(f"[{domain}] Fresh scrape...")
                sitemap_urls = await get_sitemap(http, domain)
                pages = select_pages(sitemap_urls, domain)
                logger.info(f"[{domain}] Scraping {len(pages)} pages")

                content_parts = []
                for url in pages:
                    md = await firecrawl_scrape(http, url)
                    if md and len(md.strip()) > 100:
                        content_parts.append(f"\n\n## {url}\n\n{md}")
                        pages_crawled.append(url)
                    else:
                        failed_urls.append(url)
                    await asyncio.sleep(0.3)

                if not content_parts:
                    raise ValueError("No usable content scraped")

                concatenated = "\n".join(content_parts)

            logger.info(f"[{domain}] {len(pages_crawled)} pages, {len(concatenated)} chars → Claude")

            result = analyze_with_claude(concatenated, domain)
            if not result:
                raise ValueError("Claude returned invalid JSON")

            cl = result["classification"]
            so = result["sales_org"]
            tech = so.get("techStack", {})

            # Save log
            safe = domain.replace("/", "_").replace(":", "_")
            log_path = os.path.join(LOGS_DIR, f"{safe}.json")
            with open(log_path, "w") as lf:
                json.dump({
                    "domain": domain,
                    "company_id": company_id,
                    "scraped_at": now_iso(),
                    "pages_crawled": pages_crawled,
                    "failed_urls": failed_urls,
                    "raw_content": concatenated,
                    "classification": cl,
                    "sales_org": so,
                }, lf, indent=2)

            logger.info(f"[{domain}] b2b={cl.get('b2b')}, gtm={cl.get('gtmMotion')}, hiring={so.get('hiringSignal')}")

            if dry_run:
                results.append({"domain": domain, "status": "dry_run"})
                return

            # Write classification
            await update_company(http, company_id, {
                "name": cl.get("companyResolvedName") or company.get("name"),
                "research_status": "classified",
                "enriched_at": now_iso(),
            })
            evidence = cl.get("evidence", {})
            await upsert_classification(http, {
                "company_id": company_id,
                "b2b": cl.get("b2b"),
                "saas": cl.get("saas"),
                "gtm_motion": cl.get("gtmMotion"),
                "vertical": cl.get("vertical"),
                "business_model": cl.get("businessModel"),
                "sells_to": cl.get("sellsTo"),
                "pricing_model": cl.get("pricingModel"),
                "description": cl.get("description"),
                "evidence": json.dumps(evidence) if evidence else None,
                "classified_at": now_iso(),
            })

            # Write sales org
            await upsert_sales_org(http, company_id, so, tech)

            results.append({"domain": domain, "status": "success"})
            logger.info(f"[{domain}] ✓ Done")

        except Exception as e:
            logger.error(f"[{domain}] ✗ {e}")
            safe = domain.replace("/", "_").replace(":", "_")
            try:
                with open(os.path.join(LOGS_DIR, f"{safe}.error.json"), "w") as lf:
                    json.dump({"domain": domain, "failed_at": now_iso(), "error": str(e)}, lf)
            except Exception:
                pass
            if not dry_run:
                try:
                    await update_company(http, company_id, {"research_status": "raw"})
                except Exception:
                    pass
            results.append({"domain": domain, "status": "failed", "error": str(e)})
        finally:
            try:
                del concatenated
            except NameError:
                pass
            gc.collect()

# ── Entry point ───────────────────────────────────────────────────────────────

async def main():
    limit = COMPANY_LIMIT
    concurrency = CONCURRENCY
    dry_run = False
    reclassify = False

    for arg in sys.argv[1:]:
        if arg.startswith("--limit="):
            limit = int(arg.split("=")[1])
        elif arg.startswith("--concurrency="):
            concurrency = int(arg.split("=")[1])
        elif arg == "--dry-run":
            dry_run = True
        elif arg == "--reclassify":
            reclassify = True

    logger.info(f"Starting combined research (limit={limit}, concurrency={concurrency}, dry_run={dry_run}, reclassify={reclassify})")

    async with httpx.AsyncClient(timeout=60) as http:
        companies = await fetch_companies(http, limit, reclassify)
        logger.info(f"Fetched {len(companies)} companies to process")

        if not reclassify:
            # Exclude already-enriched
            all_ids = [c["id"] for c in companies]
            existing_ids = set()
            for i in range(0, len(all_ids), 200):
                chunk = ",".join(all_ids[i:i+200])
                r = await http.get(
                    f"{SUPABASE_URL}/rest/v1/company_classification?company_id=in.({chunk})&select=company_id",
                    headers=SUPABASE_HEADERS,
                    timeout=20,
                )
                for row in r.json():
                    existing_ids.add(row["company_id"])
            companies = [c for c in companies if c["id"] not in existing_ids]
            logger.info(f"{len(companies)} after excluding already-classified")

        semaphore = asyncio.Semaphore(concurrency)
        results = []
        tasks = [
            process_company(http, c, semaphore, results, dry_run, reclassify)
            for c in companies
        ]
        await asyncio.gather(*tasks)

    ok = sum(1 for r in results if r["status"] in ("success", "dry_run"))
    fail = sum(1 for r in results if r["status"] == "failed")
    logger.info(f"Done: {ok} success, {fail} failed")

if __name__ == "__main__":
    asyncio.run(main())
