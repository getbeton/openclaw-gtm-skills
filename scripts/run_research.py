#!/usr/bin/env python3
"""
GTM Research script — crawls prefiltered companies via Firecrawl,
classifies them with Claude Haiku, and writes results to Supabase.

Usage:
    python3 run_research.py [--limit N] [--dry-run]
"""

import asyncio
import gc
import json
import logging
import os
import sys
import re
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
def _load_local_config() -> dict:
    config_path = os.path.join(os.path.dirname(__file__), "..", "config.local.json")
    try:
        with open(os.path.abspath(config_path)) as f:
            import json as _j; return _j.load(f)
    except (FileNotFoundError, ValueError):
        return {}

_lc = _load_local_config()
SUPABASE_URL = os.getenv("SUPABASE_URL", _lc.get("supabaseUrl", "YOUR_SUPABASE_URL"))
SUPABASE_KEY = os.getenv("SUPABASE_KEY", _lc.get("supabaseKey", "YOUR_SUPABASE_SERVICE_KEY"))

def _load_firecrawl_endpoint() -> str:
    import json as _json, os as _os
    cfg_path = _os.path.expanduser("~/.openclaw/workspace/integrations/firecrawl.json")
    try:
        with open(cfg_path) as _f:
            return _json.load(_f)["base_url"].rstrip("/")
    except Exception:
        return "http://104.197.67.6:3002"  # fallback
FIRECRAWL_ENDPOINT = _load_firecrawl_endpoint()
CONCURRENCY = 3
MAX_PAGES = 10
COMPANY_LIMIT = 100
LOGS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "logs", "research")

SUPABASE_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation",
}

# Priority path patterns for page selection
HIGH_VALUE_PATTERNS = [
    "/pricing", "/about", "/about-us", "/customers", "/case-studies",
    "/success-stories", "/product", "/features", "/platform",
    "/how-it-works", "/solutions", "/integrations",
]
SKIP_PATTERNS = [
    "/blog/", "/privacy", "/terms", "/gdpr", "/cookie",
    "/login", "/signin", "/app/", "/dashboard/", "/press",
    "/news/page", "/careers",
]

# ── Helpers ───────────────────────────────────────────────────────────────────

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def score_url(url: str) -> int:
    """Higher = more valuable. Returns priority score for a URL."""
    path = url.lower().split("?")[0]
    # Skip patterns
    for skip in SKIP_PATTERNS:
        if skip in path:
            return -1
    # High-value exact matches
    for i, pattern in enumerate(HIGH_VALUE_PATTERNS):
        if path.rstrip("/").endswith(pattern) or f"{pattern}/" in path:
            return len(HIGH_VALUE_PATTERNS) - i  # higher score for earlier in list
    return 0


def select_pages(sitemap_urls: list[str], domain: str) -> list[str]:
    """Pick up to MAX_PAGES most relevant URLs from sitemap."""
    homepage = f"https://{domain}"
    scored = []
    for url in sitemap_urls:
        s = score_url(url)
        if s >= 0:
            scored.append((s, url))
    # Sort descending by score
    scored.sort(key=lambda x: -x[0])
    # Always include homepage
    selected = [homepage]
    seen = {homepage}
    for score, url in scored:
        if len(selected) >= MAX_PAGES:
            break
        normalized = url.rstrip("/")
        if normalized not in seen and url not in seen:
            selected.append(url)
            seen.add(normalized)
    return selected


def extract_json_from_text(text: str) -> Optional[dict]:
    """Try to extract JSON from Claude's response (may contain markdown fences)."""
    # Try direct parse
    try:
        return json.loads(text.strip())
    except json.JSONDecodeError:
        pass
    # Try extracting from markdown code block
    match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            pass
    # Try finding first { ... } block
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            pass
    return None


# ── Supabase ──────────────────────────────────────────────────────────────────

async def fetch_companies(
    client: httpx.AsyncClient,
    limit: int = COMPANY_LIMIT,
    statuses: list = None,
    source: str = None,
) -> list[dict]:
    """
    Paginate through Supabase to collect companies by status.
    Default: prefiltered only.
    Pass statuses=['prefiltered','classified'] to include already-classified (for reclassify mode).
    """
    if statuses is None:
        statuses = ["prefiltered"]

    PAGE_SIZE = 1000
    collected = []
    offset = 0

    # Build status filter: if multiple, use 'in' operator
    if len(statuses) == 1:
        status_param = f"eq.{statuses[0]}"
        filter_key = "research_status"
        extra_params = {filter_key: status_param}
    else:
        # PostgREST 'in' syntax
        status_list = ",".join(statuses)
        extra_params = {"research_status": f"in.({status_list})"}

    if source:
        extra_params["source"] = f"eq.{source}"

    while len(collected) < limit:
        resp = await client.get(
            f"{SUPABASE_URL}/rest/v1/companies",
            headers=SUPABASE_HEADERS,
            params={
                **extra_params,
                "order": "domain.asc",
                "limit": PAGE_SIZE,
                "offset": offset,
                "select": "id,domain,name",
            },
        )
        resp.raise_for_status()
        page = resp.json()
        if not page:
            break
        collected.extend(page)
        if len(page) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return collected[:limit]


async def update_company(client: httpx.AsyncClient, company_id: str, data: dict):
    resp = await client.patch(
        f"{SUPABASE_URL}/rest/v1/companies",
        headers={**SUPABASE_HEADERS, "Prefer": "return=minimal"},
        params={"id": f"eq.{company_id}"},
        json=data,
    )
    resp.raise_for_status()


async def upsert_classification(client: httpx.AsyncClient, record: dict):
    # gtm_motion check constraint: only PLG, SLG, hybrid, D2C allowed — anything else → NULL
    gtm = record.get("gtm_motion")
    if gtm not in ("PLG", "SLG", "hybrid", "D2C"):
        record = {**record, "gtm_motion": None}

    resp = await client.post(
        f"{SUPABASE_URL}/rest/v1/company_classification",
        headers={
            **SUPABASE_HEADERS,
            "Prefer": "resolution=merge-duplicates,return=minimal",
        },
        params={"on_conflict": "company_id"},
        json=record,
    )
    resp.raise_for_status()


# ── Firecrawl ─────────────────────────────────────────────────────────────────

async def get_sitemap(client: httpx.AsyncClient, domain: str) -> list[str]:
    try:
        resp = await client.post(
            f"{FIRECRAWL_ENDPOINT}/v1/map",
            json={"url": f"https://{domain}", "limit": 50},
            timeout=30.0,
        )
        resp.raise_for_status()
        data = resp.json()
        # Firecrawl map response: {"links": [...]} or {"urls": [...]}
        urls = data.get("links") or data.get("urls") or []
        if isinstance(urls, list):
            return [u for u in urls if isinstance(u, str)]
        return []
    except Exception as e:
        logger.warning(f"[{domain}] Sitemap failed: {e}")
        return []


async def scrape_page(client: httpx.AsyncClient, url: str, retries: int = 1) -> Optional[str]:
    for attempt in range(retries + 1):
        try:
            resp = await client.post(
                f"{FIRECRAWL_ENDPOINT}/v1/scrape",
                json={"url": url, "formats": ["markdown"], "onlyMainContent": True},
                timeout=60.0,
            )
            if resp.status_code in (500, 408, 429) and attempt < retries:
                await asyncio.sleep(1)
                continue
            resp.raise_for_status()
            data = resp.json()
            # Firecrawl scrape response: {"data": {"markdown": "..."}} or {"markdown": "..."}
            if "data" in data and isinstance(data["data"], dict):
                return data["data"].get("markdown", "")
            return data.get("markdown") or data.get("content") or ""
        except Exception as e:
            if attempt < retries:
                await asyncio.sleep(1)
            else:
                logger.warning(f"Scrape failed for {url}: {e}")
                return None
    return None


# ── Claude Classification ──────────────────────────────────────────────────────

CLASSIFICATION_PROMPT = """You are a business analyst. Based on the following website content, extract structured information about this company.

Output ONLY valid JSON with exactly these fields:
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
    "gtmMotion": "quote or observation",
    "vertical": "quote or observation",
    "sellsTo": "quote or observation"
  }}
}}

GTM motion definitions — pick the best fit, avoid null unless truly unclassifiable:

- PLG (Product-Led Growth): users can discover, sign up, and start using the product WITHOUT talking to a sales person. Self-serve is the PRIMARY acquisition path. PLG signals include: self-serve signup, public pricing page, freemium/free tier, free trial, "Get started" / "Sign up" / "Try for free" as main CTA, app store presence, open-source, developer sandbox/API keys. Does NOT require a free tier — a paid-only product with self-serve checkout is still PLG.

- SLG (Sales-Led Growth): a human must initiate or close the sale. No meaningful self-serve path. SLG signals: "Contact sales" / "Request a demo" / "Get a quote" / "Book a call" as the PRIMARY or ONLY entry point, no visible pricing, approval/application required, gated access.

- hybrid: BOTH a meaningful self-serve path AND a sales-assisted path clearly coexist. This is very common in B2B SaaS — e.g. self-serve for SMB tiers + "contact sales" for enterprise, or free/trial plan + dedicated sales team for upsells. Use hybrid whenever BOTH signals are clearly present, even if one dominates slightly. Do NOT collapse to PLG just because a sales CTA also exists.

- D2C: company sells physical goods directly to consumers (ecommerce, retail, CPG). NOT software. NOT services.

- null: only if the page has no usable content (loading spinners, blank, under construction).

Additional rules:
- B2C apps (health, fitness, dating, finance) with self-serve download/signup → PLG.
- Offline services (HVAC, legal, plumbing) → SLG. Ride-hailing/booking with self-serve online → PLG.
- Wholesale/trade-only (gated catalog, apply for pro access) → SLG, saas=false.
- Physical goods ecommerce → D2C, b2b=false, saas=false.
- Never output "unknown" — it is not a valid value.

Website content:
{content}"""


def _get_anthropic_client() -> anthropic.Anthropic:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        token_path = os.path.expanduser("~/.openclaw/agents/main/agent/auth-profiles.json")
        if os.path.exists(token_path):
            import json as _json
            profiles = _json.load(open(token_path)).get("profiles", {})
            api_key = profiles.get("anthropic:manual", {}).get("token")
    if not api_key:
        raise RuntimeError("No Anthropic API key found.")
    return anthropic.Anthropic(api_key=api_key)


def classify_with_claude(content: str, domain: str) -> Optional[dict]:
    """Call Claude Sonnet 4.6 to classify the company, falling back to Haiku on failure."""
    client = _get_anthropic_client()
    prompt = CLASSIFICATION_PROMPT.replace("{content}", content[:15000])

    for model, label in [("claude-sonnet-4-6", "Sonnet"), ("claude-haiku-4-5", "Haiku")]:
        try:
            response = client.messages.create(
                model=model,
                max_tokens=1024,
                messages=[{"role": "user", "content": prompt}],
            )
            raw_text = response.content[0].text
            result = extract_json_from_text(raw_text)
            if result:
                return result
            # JSON parse failed — retry once with stricter prompt
            logger.warning(f"[{domain}] {label}: JSON parse failed, retrying...")
            retry_prompt = (
                "You must respond with ONLY a raw JSON object, no markdown, no explanation.\n\n"
                + prompt
            )
            response2 = client.messages.create(
                model=model,
                max_tokens=1024,
                messages=[{"role": "user", "content": retry_prompt}],
            )
            result2 = extract_json_from_text(response2.content[0].text)
            if result2:
                return result2
            logger.warning(f"[{domain}] {label}: retry also failed to parse JSON, trying next model...")
        except Exception as e:
            logger.warning(f"[{domain}] {label} error: {e} — trying next model...")

    logger.error(f"[{domain}] All models failed")
    return None


# ── Main company processor ────────────────────────────────────────────────────

LOG_MAX_AGE_DAYS = 30  # Reuse cached scrape if log is fresher than this


def _load_cached_scrape(domain: str) -> Optional[dict]:
    """Return cached research log if it exists and is <LOG_MAX_AGE_DAYS old."""
    safe_domain = domain.replace("/", "_").replace(":", "_")
    log_path = os.path.join(LOGS_DIR, f"{safe_domain}.json")
    if not os.path.exists(log_path):
        return None
    age_days = (time.time() - os.path.getmtime(log_path)) / 86400
    if age_days > LOG_MAX_AGE_DAYS:
        return None
    try:
        with open(log_path) as f:
            return json.load(f)
    except Exception:
        return None


async def process_company(
    http: httpx.AsyncClient,
    company: dict,
    semaphore: asyncio.Semaphore,
    results: list,
    dry_run: bool = False,
):
    domain = company["domain"]
    company_id = company["id"]

    async with semaphore:
        pages_crawled = []
        failed_urls = []

        try:
            # ── Check for fresh cached scrape log ─────────────────────────────
            cached = _load_cached_scrape(domain)
            if cached and cached.get("raw_content"):
                logger.info(f"[{domain}] Using cached scrape log (skipping Firecrawl)")
                concatenated = cached["raw_content"]
                pages_crawled = cached.get("pages_crawled", [])
                failed_urls = cached.get("failed_urls", [])
            else:
                # ── Full Firecrawl scrape ─────────────────────────────────────
                logger.info(f"[{domain}] Starting fresh scrape...")

                # Step 1: Get sitemap
                sitemap_urls = await get_sitemap(http, domain)
                logger.info(f"[{domain}] Sitemap: {len(sitemap_urls)} URLs")

                # Step 2: Select pages
                pages = select_pages(sitemap_urls, domain)
                logger.info(f"[{domain}] Selected {len(pages)} pages: {pages[:3]}...")

                # Step 3: Scrape selected pages
                content_parts = []
                for url in pages:
                    md = await scrape_page(http, url)
                    if md and len(md.strip()) > 100:
                        content_parts.append(f"\n\n## {url}\n\n{md}")
                        pages_crawled.append(url)
                    else:
                        failed_urls.append(url)
                    await asyncio.sleep(0.3)  # gentle rate limiting

                if not content_parts:
                    raise ValueError("No usable content scraped")

                concatenated = "\n".join(content_parts)

            logger.info(f"[{domain}] Content: {len(pages_crawled)} pages, {len(concatenated)} chars")

            # Step 4: Classify with Claude
            classification = classify_with_claude(concatenated, domain)
            if not classification:
                raise ValueError("Claude returned invalid JSON after retry")

            # Save raw log for this domain
            import os
            safe_domain = domain.replace("/", "_").replace(":", "_")
            log_path = os.path.join(LOGS_DIR, f"{safe_domain}.json")
            with open(log_path, "w") as lf:
                json.dump({
                    "domain": domain,
                    "company_id": company_id,
                    "scraped_at": now_iso(),
                    "pages_crawled": pages_crawled,
                    "failed_urls": failed_urls,
                    "raw_content": concatenated,
                    "claude_prompt": CLASSIFICATION_PROMPT.replace("{content}", concatenated[:15000]),
                    "classification": classification,
                }, lf, indent=2)

            logger.info(
                f"[{domain}] Classified: b2b={classification.get('b2b')}, "
                f"saas={classification.get('saas')}, gtm={classification.get('gtmMotion')}, "
                f"vertical={classification.get('vertical')}"
            )

            if dry_run:
                logger.info(f"[{domain}] DRY RUN — skipping Supabase write")
                results.append({"domain": domain, "status": "dry_run", "classification": classification})
                return

            # Step 5: Write to Supabase
            # 5a. Update companies table
            await update_company(http, company_id, {
                "name": classification.get("companyResolvedName") or company.get("name"),
                "research_status": "classified",
                "enriched_at": now_iso(),
                "research_raw": json.dumps({
                    "pages_crawled": pages_crawled,
                    "failed_urls": failed_urls,
                }),
            })

            # 5b. Upsert company_classification
            evidence = classification.get("evidence", {})
            await upsert_classification(http, {
                "company_id": company_id,
                "b2b": classification.get("b2b"),
                "saas": classification.get("saas"),
                "gtm_motion": classification.get("gtmMotion"),
                "vertical": classification.get("vertical"),
                "business_model": classification.get("businessModel"),
                "sells_to": classification.get("sellsTo"),
                "pricing_model": classification.get("pricingModel"),
                "description": classification.get("description"),
                "evidence": json.dumps(evidence) if evidence else None,
                "classified_at": now_iso(),
            })

            results.append({"domain": domain, "status": "success", "classification": classification})
            logger.info(f"[{domain}] ✓ Done")

        except Exception as e:
            logger.error(f"[{domain}] ✗ Failed: {e}")
            # Save error log
            try:
                import os
                safe_domain = domain.replace("/", "_").replace(":", "_")
                log_path = os.path.join(LOGS_DIR, f"{safe_domain}.error.json")
                with open(log_path, "w") as lf:
                    json.dump({
                        "domain": domain,
                        "company_id": company_id,
                        "failed_at": now_iso(),
                        "error": str(e),
                        "pages_crawled": pages_crawled,
                        "failed_urls": failed_urls,
                    }, lf, indent=2)
            except Exception:
                pass
            # Mark as raw for retry
            if not dry_run:
                try:
                    await update_company(http, company_id, {
                        "research_status": "raw",
                        "research_raw": json.dumps({"error": str(e), "domain": domain}),
                    })
                except Exception as e2:
                    logger.error(f"[{domain}] Also failed to update status: {e2}")
            results.append({"domain": domain, "status": "failed", "error": str(e)})

        finally:
            # Explicit cleanup — raw_content can be 1-3MB per company and must not accumulate
            # across thousands of iterations in a long-running async process
            try:
                del concatenated
            except NameError:
                pass
            try:
                del content_parts
            except NameError:
                pass
            try:
                del classification
            except NameError:
                pass
            try:
                del cached
            except NameError:
                pass
            gc.collect()


# ── Entry point ───────────────────────────────────────────────────────────────

async def main():
    dry_run = "--dry-run" in sys.argv
    reclassify = "--reclassify" in sys.argv  # Re-run Claude on cached scrapes for classified companies
    limit = COMPANY_LIMIT
    source_filter = None
    for arg in sys.argv:
        if arg.startswith("--limit="):
            limit = int(arg.split("=")[1])
        if arg.startswith("--source="):
            source_filter = arg.split("=")[1]

    statuses = ["prefiltered"]
    if reclassify:
        statuses = ["prefiltered", "classified"]
        logger.info("Reclassify mode: will use cached scrape logs for classified companies")

    logger.info(f"Starting GTM research run (limit={limit}, dry_run={dry_run}, statuses={statuses}, source={source_filter})")

    async with httpx.AsyncClient(timeout=60.0) as http:
        # Fetch companies
        companies = await fetch_companies(http, limit=limit, statuses=statuses, source=source_filter)
        logger.info(f"Fetched {len(companies)} companies to process")

        if not companies:
            logger.info("No companies to process — all done!")
            return

        # Process with concurrency limit
        semaphore = asyncio.Semaphore(CONCURRENCY)
        results = []
        tasks = [
            process_company(http, company, semaphore, results, dry_run)
            for company in companies
        ]
        await asyncio.gather(*tasks)

    # ── Report ─────────────────────────────────────────────────────────────────
    total = len(results)
    success = [r for r in results if r["status"] in ("success", "dry_run")]
    failed = [r for r in results if r["status"] == "failed"]

    print("\n" + "="*60)
    print("GTM RESEARCH RUN — COMPLETE")
    print("="*60)
    print(f"Total processed : {total}")
    print(f"Classified       : {len(success)}")
    print(f"Failed           : {len(failed)}")

    if failed:
        print("\nFailed companies:")
        for r in failed[:10]:
            print(f"  {r['domain']}: {r.get('error', 'unknown')[:100]}")

    # Sample of 5 successful
    print("\nSample classifications (first 5 success):")
    for r in success[:5]:
        c = r.get("classification", {})
        print(
            f"  {r['domain']}: b2b={c.get('b2b')}, saas={c.get('saas')}, "
            f"gtm={c.get('gtmMotion')}, vertical={c.get('vertical')}"
        )

    # Patterns
    classified_list = [r["classification"] for r in success if "classification" in r]
    if classified_list:
        saas_pct = sum(1 for c in classified_list if c.get("saas")) / len(classified_list) * 100
        b2b_pct = sum(1 for c in classified_list if c.get("b2b")) / len(classified_list) * 100

        from collections import Counter
        gtm_counts = Counter(c.get("gtmMotion", "unknown") for c in classified_list)
        vertical_counts = Counter(c.get("vertical", "unknown") for c in classified_list)

        print(f"\nPatterns ({len(classified_list)} classified):")
        print(f"  SaaS: {saas_pct:.0f}%  |  B2B: {b2b_pct:.0f}%")
        print("  GTM motion breakdown:")
        for motion, count in gtm_counts.most_common():
            print(f"    {motion}: {count} ({count/len(classified_list)*100:.0f}%)")
        print("  Top verticals:")
        for vertical, count in vertical_counts.most_common(8):
            print(f"    {vertical}: {count}")

    # Save full results to JSON
    output_path = os.path.join(os.path.dirname(__file__), "research_results.json")
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\nFull results saved to: {output_path}")


if __name__ == "__main__":
    asyncio.run(main())
