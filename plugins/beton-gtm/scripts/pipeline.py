"""
Beton GTM Pipeline Orchestrator

Runs the full prospecting pipeline:
  0. Intake: normalize + dedup domains
  1. Prefilter: homepage + LinkedIn checks (parallel x10)
  2. Research: deep Firecrawl crawl (parallel x5)
  3. Sales Org: headcount + tech stack (parallel x5)
  4. Signals: news + LinkedIn + G2 (parallel x10)
  5. Segment: fit score + tier (batch)
  6. Sync to Supabase + Attio
  7. Outreach: draft sequences for T1 (if experiment_id provided)

Usage:
  python3 pipeline.py --domains path/to/domains.csv
  python3 pipeline.py --domains path/to/domains.csv --experiment-id <uuid>
  python3 pipeline.py --domains path/to/domains.csv --resume
  python3 pipeline.py --status  # Show pipeline run stats
"""

import os
import sys
import csv
import json
import time
import uuid
import logging
import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Optional
import httpx

# Add scripts dir to path
sys.path.insert(0, os.path.dirname(__file__))
from supabase_client import SupabaseClient, normalize_domain, load_soax_proxy
from attio import sync_company_to_attio, upsert_person

# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f"/tmp/beton_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
    ],
)
logger = logging.getLogger("pipeline")

# ============================================================
# Config
# ============================================================

FIRECRAWL_URL = os.getenv("FIRECRAWL_URL", "http://localhost:3002")
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

PARALLELISM = {
    "prefilter": 10,
    "research": 5,
    "sales_org": 5,
    "signals": 10,
}

# ============================================================
# Firecrawl helpers
# ============================================================

def firecrawl_scrape(url: str, soax_proxy: dict = None, timeout: int = 30) -> Optional[str]:
    """Scrape a URL via local Firecrawl. Returns markdown content or None."""
    payload = {
        "url": url,
        "formats": ["markdown"],
        "onlyMainContent": True,
    }
    if soax_proxy:
        payload["proxy"] = "stealth"  # Firecrawl uses soax if configured globally

    try:
        resp = httpx.post(
            f"{FIRECRAWL_URL}/v1/scrape",
            json=payload,
            timeout=timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", {}).get("markdown", "")
    except Exception as e:
        logger.warning(f"Firecrawl scrape failed for {url}: {e}")
        return None


def firecrawl_map(url: str) -> list[str]:
    """Get sitemap URLs via Firecrawl map endpoint."""
    try:
        resp = httpx.post(
            f"{FIRECRAWL_URL}/v1/map",
            json={"url": url, "limit": 100},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json().get("links", [])
    except Exception as e:
        logger.warning(f"Firecrawl map failed for {url}: {e}")
        return []


# ============================================================
# Skill 0: Intake
# ============================================================

def run_intake(domains_input: str | list[str], db: SupabaseClient) -> list[str]:
    """
    Normalize and dedup domains, insert new ones into Supabase.
    Returns list of new domain strings to process.
    """
    logger.info("=== SKILL 0: INTAKE ===")

    raw_domains = []
    if isinstance(domains_input, list):
        raw_domains = domains_input
    elif isinstance(domains_input, str) and os.path.exists(domains_input):
        # Parse CSV
        with open(domains_input) as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames or []
            domain_col = next(
                (h for h in headers if h.lower() in ("domain", "website", "url", "company_url")),
                headers[0] if headers else None,
            )
            if not domain_col:
                raise ValueError("CSV has no headers — cannot identify domain column")
            for row in reader:
                raw_domains.append(row.get(domain_col, ""))
    else:
        raise ValueError(f"domains_input must be a list or a CSV file path, got: {domains_input!r}")

    # Normalize
    normalized = []
    invalid_count = 0
    for raw in raw_domains:
        d = normalize_domain(raw)
        if d:
            normalized.append(d)
        else:
            invalid_count += 1
            logger.debug(f"Skipping invalid domain: {raw!r}")

    # Dedup within batch
    seen = set()
    deduped = []
    for d in normalized:
        if d not in seen:
            seen.add(d)
            deduped.append(d)

    duplicates_in_batch = len(normalized) - len(deduped)
    logger.info(f"Input: {len(raw_domains)} | Invalid: {invalid_count} | Batch dupes: {duplicates_in_batch} | Unique: {len(deduped)}")

    # Check Supabase for existing
    if not deduped:
        return []

    # Query existing
    existing_rows = db.select_raw("companies", {"domain": f"in.({','.join(deduped)})", "select": "domain"})
    existing_domains = {r["domain"] for r in existing_rows}
    new_domains = [d for d in deduped if d not in existing_domains]

    logger.info(f"Already in DB: {len(existing_domains)} | New to insert: {len(new_domains)}")

    # Insert new
    if new_domains:
        inserted = db.insert_companies_bulk(new_domains)
        logger.info(f"Inserted {inserted} new company records")

    return new_domains


# ============================================================
# Skill 1: Prefilter
# ============================================================

def _prefilter_one(domain: str, db: SupabaseClient, soax_proxy: dict) -> dict:
    """Run prefilter checks on a single domain."""
    result = {"domain": domain, "passed": False, "reason": None}
    company = db.get_company_by_domain(domain)
    if not company:
        result["reason"] = "not_found_in_db"
        return result

    company_id = company["id"]

    # 1. Homepage check
    homepage = firecrawl_scrape(f"https://{domain}", soax_proxy)
    if not homepage:
        # Try http
        homepage = firecrawl_scrape(f"http://{domain}", soax_proxy)

    if not homepage:
        db.update_company_status(company_id, "skip", {"firmographic": json.dumps({"skip_reason": "homepage_failed"})})
        result["reason"] = "homepage_failed"
        return result

    # Check for parked page
    parked_signals = [
        "this domain is for sale",
        "buy this domain",
        "domain for sale",
        "sedo.com",
        "godaddy.com/domain",
        "parked page",
        "register your domain",
    ]
    if any(signal in homepage.lower() for signal in parked_signals):
        db.update_company_status(company_id, "skip", {"firmographic": json.dumps({"skip_reason": "parked_page"})})
        result["reason"] = "parked_page"
        return result

    # 2. LinkedIn check
    domain_stem = domain.split(".")[0]
    time.sleep(1 + (hash(domain) % 2))  # jitter 1-2s
    linkedin_content = firecrawl_scrape(
        f"https://www.linkedin.com/company/{domain_stem}/",
        soax_proxy,
    )

    linkedin_found = bool(linkedin_content and len(linkedin_content) > 200)
    employee_estimate = None

    if linkedin_found:
        # Extract employee count
        import re
        patterns = [
            r"(\d[\d,]+)\s+employees",
            r"(\d[\d,]+)\s*–\s*(\d[\d,]+)\s+employees",
            r"(\d[\d,]+)\s*\+?\s+followers",
        ]
        for pat in patterns:
            m = re.search(pat, linkedin_content, re.IGNORECASE)
            if m:
                try:
                    count_str = m.group(1).replace(",", "")
                    employee_estimate = int(count_str)
                    break
                except ValueError:
                    pass

    # 3. Size filter
    if employee_estimate is not None:
        if employee_estimate < 10:
            db.update_company_status(company_id, "skip", {
                "firmographic": json.dumps({"skip_reason": f"too_small_employees={employee_estimate}"})
            })
            result["reason"] = f"too_small_employees={employee_estimate}"
            return result
        if employee_estimate > 5000:
            db.update_company_status(company_id, "skip", {
                "firmographic": json.dumps({"skip_reason": f"too_large_employees={employee_estimate}"})
            })
            result["reason"] = f"too_large_employees={employee_estimate}"
            return result

    # Passed
    firmographic = {"linkedin_found": linkedin_found}
    if employee_estimate:
        firmographic["employees_estimate"] = employee_estimate

    db.update_company_status(company_id, "prefiltered", {
        "firmographic": json.dumps(firmographic)
    })

    result["passed"] = True
    result["linkedin_found"] = linkedin_found
    result["employee_estimate"] = employee_estimate
    return result


def run_prefilter(domains: list[str], db: SupabaseClient) -> list[str]:
    """Run prefilter in parallel. Returns passed domains."""
    logger.info(f"=== SKILL 1: PREFILTER ({len(domains)} domains) ===")
    soax_proxy = load_soax_proxy()

    passed = []
    skipped = {}

    with ThreadPoolExecutor(max_workers=PARALLELISM["prefilter"]) as executor:
        futures = {executor.submit(_prefilter_one, d, db, soax_proxy): d for d in domains}
        for future in as_completed(futures):
            domain = futures[future]
            try:
                result = future.result()
                if result["passed"]:
                    passed.append(domain)
                else:
                    skipped[domain] = result.get("reason", "unknown")
            except Exception as e:
                logger.error(f"Prefilter error for {domain}: {e}")
                skipped[domain] = f"exception: {e}"

    logger.info(f"Prefilter: {len(passed)} passed | {len(skipped)} skipped")
    for d, r in list(skipped.items())[:10]:
        logger.info(f"  SKIP {d}: {r}")

    return passed


# ============================================================
# Skill 2: Research
# ============================================================

def _research_one(domain: str, db: SupabaseClient) -> dict:
    """Deep research a single domain via Firecrawl + Claude."""
    company = db.get_company_by_domain(domain)
    if not company:
        return {"domain": domain, "success": False, "error": "not_in_db"}

    company_id = company["id"]

    # Get sitemap
    urls = firecrawl_map(f"https://{domain}")

    # Filter relevant pages
    high_value = ["/pricing", "/about", "/customers", "/case-studies", "/product", "/features", "/platform", "/how-it-works", "/solutions"]
    medium_value = ["/integrations", "/blog"]
    skip_patterns = ["/privacy", "/terms", "/gdpr", "/cookie", "/login", "/signup", "/app/", "/dashboard/", "/signin", "/legal"]

    selected = []
    for url in urls:
        path = url.lower().split(domain)[-1] if domain in url.lower() else url
        if any(p in path for p in skip_patterns):
            continue
        if any(p in path for p in high_value):
            selected.append(url)
        elif any(p in path for p in medium_value) and len(selected) < 4:
            selected.append(url)

    # Add homepage
    selected = [f"https://{domain}"] + selected[:7]
    selected = list(dict.fromkeys(selected))[:8]  # dedup, max 8

    # Crawl pages
    pages_content = []
    failed_urls = []
    for url in selected:
        content = firecrawl_scrape(url)
        if content:
            pages_content.append(f"### {url}\n\n{content[:3000]}")
        else:
            failed_urls.append(url)
        time.sleep(0.5)

    if not pages_content:
        db.update_company_status(company_id, "skip", {"research_raw": json.dumps({"error": "all_pages_failed"})})
        return {"domain": domain, "success": False, "error": "all_pages_failed"}

    combined = "\n\n---\n\n".join(pages_content)

    # Claude distillation (placeholder — actual Claude call done via OpenClaw tool in skill context)
    # In pipeline, this calls an LLM endpoint or subprocess
    classification = _call_claude_classify(combined, domain)
    if not classification:
        db.update_company_status(company_id, "raw")
        return {"domain": domain, "success": False, "error": "claude_failed"}

    # Update Supabase
    db.set_company_classification(
        company_id,
        classification,
        name=classification.get("companyResolvedName"),
    )
    db.update("companies", {"id": company_id}, {
        "research_raw": json.dumps({"pages_crawled": selected, "failed_urls": failed_urls}),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    })

    return {"domain": domain, "success": True, "classification": classification}


def _call_claude_classify(content: str, domain: str) -> Optional[dict]:
    """
    Call Claude to classify company from web content.

    In production: calls Claude via anthropic SDK or OpenClaw tool proxy.
    Returns parsed JSON dict or None on failure.
    """
    try:
        import anthropic
        client = anthropic.Anthropic()
        prompt = f"""You are a B2B SaaS analyst. Based on the following website content for {domain}, extract structured information.

Output ONLY valid JSON with exactly these fields:
{{
  "companyResolvedName": "Official company name",
  "primaryWebsite": "canonical domain",
  "b2b": true/false,
  "saas": true/false,
  "gtmMotion": "PLG" | "SLG" | "hybrid" | "unknown",
  "vertical": "short vertical label",
  "businessModel": "subscription" | "usage-based" | "freemium" | "marketplace" | "services" | "other",
  "sellsTo": "SMB" | "mid-market" | "enterprise" | "all" | "unknown",
  "pricingModel": "per-seat" | "flat-rate" | "usage-based" | "tiered" | "custom-only" | "freemium" | "unknown",
  "keyFeatures": ["feature1", "feature2", "feature3"],
  "evidence": {{
    "gtmMotion": "quote or observation",
    "vertical": "quote or observation",
    "sellsTo": "quote or observation",
    "pricingModel": "quote or observation"
  }}
}}

GTM motion: PLG=self-serve/free trial, SLG=demo required/contact sales, hybrid=both.

Website content:
{content[:8000]}"""

        msg = client.messages.create(
            model="claude-opus-4-5",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = msg.content[0].text.strip()
        # Strip markdown code fences if present
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw)
    except ImportError:
        logger.warning("anthropic SDK not installed — classification skipped. Run: pip install anthropic")
        return None
    except Exception as e:
        logger.error(f"Claude classification failed for {domain}: {e}")
        return None


def run_research(domains: list[str], db: SupabaseClient) -> list[str]:
    """Run deep research in parallel. Returns successfully researched domains."""
    logger.info(f"=== SKILL 2: RESEARCH ({len(domains)} domains) ===")
    succeeded = []

    with ThreadPoolExecutor(max_workers=PARALLELISM["research"]) as executor:
        futures = {executor.submit(_research_one, d, db): d for d in domains}
        for future in as_completed(futures):
            domain = futures[future]
            try:
                result = future.result()
                if result["success"]:
                    succeeded.append(domain)
                else:
                    logger.warning(f"Research failed for {domain}: {result.get('error')}")
            except Exception as e:
                logger.error(f"Research exception for {domain}: {e}")

    logger.info(f"Research: {len(succeeded)}/{len(domains)} succeeded")
    return succeeded


# ============================================================
# Skill 3: Sales Org
# ============================================================

def _sales_org_one(domain: str, db: SupabaseClient, soax_proxy: dict) -> dict:
    """Extract sales org data for a single company."""
    company = db.get_company_by_domain(domain)
    if not company:
        return {"domain": domain, "success": False}

    company_id = company["id"]
    company_name = company.get("name", domain.split(".")[0])

    # Try careers page
    careers_content = None
    careers_urls = [
        f"https://{domain}/careers",
        f"https://{domain}/jobs",
        f"https://{domain}/work-with-us",
        f"https://{domain}/join-us",
    ]
    for url in careers_urls:
        content = firecrawl_scrape(url)
        if content and len(content) > 200:
            careers_content = content
            break
        time.sleep(0.3)

    # LinkedIn jobs
    time.sleep(1 + (hash(domain) % 2))
    domain_stem = domain.split(".")[0]
    linkedin_jobs = firecrawl_scrape(
        f"https://www.linkedin.com/jobs/search/?keywords={company_name}+sales+OR+revenue+operations",
        soax_proxy,
    )

    combined = ""
    if careers_content:
        combined += f"### Careers Page\n{careers_content[:4000]}\n\n"
    if linkedin_jobs:
        combined += f"### LinkedIn Jobs\n{linkedin_jobs[:3000]}"

    if not combined:
        db.set_company_sales_org(company_id, {"careers_page": False})
        return {"domain": domain, "success": False, "reason": "no_content"}

    # Parse roles (basic heuristic version — Claude can replace this)
    sales_org, tech_stack = _parse_sales_org(combined)

    db.set_company_sales_org(company_id, sales_org, tech_stack)
    return {"domain": domain, "success": True, "sales_org": sales_org}


def _parse_sales_org(content: str) -> tuple[dict, dict]:
    """Heuristic role + tech stack parser. Returns (sales_org, tech_stack)."""
    import re

    content_lower = content.lower()

    # Count open roles by function
    role_patterns = {
        "sales": ["account executive", "ae ", "sdr", "bdr", "sales representative", "sales manager", "business development"],
        "revops": ["revenue operations", "revops", "sales operations", "salesops", "gtm operations", "crm admin"],
        "cs": ["customer success", "csm", "onboarding", "implementation"],
    }

    counts = {k: 0 for k in role_patterns}
    open_roles = []

    for func, keywords in role_patterns.items():
        for kw in keywords:
            matches = re.findall(rf'\b{re.escape(kw)}\b', content_lower)
            counts[func] += len(matches)
            if matches:
                open_roles.append({"title": kw.title(), "function": func})

    sales_org = {
        "salesHeadcount": counts["sales"] * 15 if counts["sales"] > 0 else None,
        "revopsHeadcount": counts["revops"] * 15 if counts["revops"] > 0 else None,
        "csHeadcount": counts["cs"] * 15 if counts["cs"] > 0 else None,
        "openRoles": open_roles[:10],
        "hiringSignal": "building_revops" if counts["revops"] > 0 else ("scaling_sales" if counts["sales"] >= 3 else "not_hiring"),
    }

    # Tech stack
    tech_map = {
        "crm": {"salesforce": "Salesforce", "hubspot": "HubSpot", "pipedrive": "Pipedrive", "close.io": "Close", "zoho": "Zoho CRM"},
        "salesEngagementTool": {"outreach.io": "Outreach", "salesloft": "Salesloft", "apollo.io": "Apollo", "groove": "Groove", "mixmax": "Mixmax"},
        "dataTools": {"zoominfo": "ZoomInfo", "apollo": "Apollo", "clay.run": "Clay", "clearbit": "Clearbit", "lusha": "Lusha"},
        "analytics": {"tableau": "Tableau", "looker": "Looker", "metabase": "Metabase", "sisense": "Sisense", "mixpanel": "Mixpanel"},
    }

    tech_stack = {}
    for category, tools in tech_map.items():
        for kw, name in tools.items():
            if kw in content_lower:
                if category == "dataTools":
                    tech_stack.setdefault("dataTools", []).append(name)
                else:
                    tech_stack.setdefault(category, name)
                break

    return sales_org, tech_stack


def run_sales_org(domains: list[str], db: SupabaseClient) -> list[str]:
    """Run sales org extraction in parallel."""
    logger.info(f"=== SKILL 3: SALES ORG ({len(domains)} domains) ===")
    soax_proxy = load_soax_proxy()
    succeeded = []

    with ThreadPoolExecutor(max_workers=PARALLELISM["sales_org"]) as executor:
        futures = {executor.submit(_sales_org_one, d, db, soax_proxy): d for d in domains}
        for future in as_completed(futures):
            domain = futures[future]
            try:
                result = future.result()
                if result["success"]:
                    succeeded.append(domain)
            except Exception as e:
                logger.error(f"Sales org exception for {domain}: {e}")

    logger.info(f"Sales org: {len(succeeded)}/{len(domains)} succeeded")
    return domains  # Return all (sales org failure is non-blocking)


# ============================================================
# Skill 4: Signals
# ============================================================

def _signals_one(domain: str, db: SupabaseClient) -> dict:
    """Extract buying signals for a single company."""
    company = db.get_company_by_domain(domain)
    if not company:
        return {"domain": domain, "signals_found": 0}

    company_id = company["id"]
    company_name = company.get("name", domain)

    signals = []

    # Web search for news (funding + leadership + launches)
    search_queries = [
        f'"{company_name}" funding OR "raised" OR "series"',
        f'"{company_name}" CRO OR "VP Sales" OR "Head of Revenue" hired OR appointed OR joins',
        f'"{company_name}" launch OR launches OR "new product" OR announces',
    ]

    for query in search_queries:
        results = _web_search(query)
        for r in results[:5]:
            signal_type, urgency = _classify_signal(r["title"] + " " + r.get("snippet", ""), company_name)
            if signal_type and urgency >= 4:
                signals.append({
                    "company_id": company_id,
                    "type": signal_type,
                    "content": f"{r['title']} — {r.get('snippet', '')[:200]}",
                    "source": r.get("url", "web_search"),
                    "urgency_score": urgency,
                })
        time.sleep(0.5)

    # Deduplicate signals (same type + similar content)
    unique_signals = _dedup_signals(signals)

    # Store in Supabase
    for sig in unique_signals:
        try:
            db.insert_signal(
                company_id=sig["company_id"],
                signal_type=sig["type"],
                content=sig["content"],
                source=sig["source"],
                urgency_score=sig["urgency_score"],
            )
        except Exception as e:
            logger.warning(f"Failed to insert signal for {domain}: {e}")

    # Update enriched_at
    db.update("companies", {"id": company_id}, {
        "enriched_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    })

    return {"domain": domain, "signals_found": len(unique_signals)}


def _web_search(query: str) -> list[dict]:
    """Simple web search via DuckDuckGo instant answers or similar. Returns list of {title, url, snippet}."""
    # Placeholder: in OpenClaw context, use web_search tool
    # For standalone pipeline: use httpx to call a search API
    try:
        resp = httpx.get(
            "https://api.search.brave.com/res/v1/web/search",
            params={"q": query, "count": 5, "freshness": "pm"},
            headers={"Accept": "application/json", "X-Subscription-Token": os.getenv("BRAVE_API_KEY", "")},
            timeout=10,
        )
        if resp.status_code == 200:
            results = resp.json().get("web", {}).get("results", [])
            return [{"title": r.get("title", ""), "url": r.get("url", ""), "snippet": r.get("description", "")} for r in results]
    except Exception as e:
        logger.warning(f"Web search failed for query '{query[:50]}': {e}")
    return []


def _classify_signal(text: str, company_name: str) -> tuple[Optional[str], int]:
    """Quick heuristic signal classification. Returns (type, urgency_score)."""
    text_lower = text.lower()

    if any(w in text_lower for w in ["series a", "series b", "series c", "raised $", "funding round", "million in"]):
        return "funding", 9
    if any(w in text_lower for w in ["seed", "pre-seed", "angel round"]):
        return "funding", 7
    if any(w in text_lower for w in ["cro", "vp sales", "chief revenue", "head of sales", "hired", "appointed", "joins as"]):
        return "leadership_change", 8
    if any(w in text_lower for w in ["launch", "announces", "new product", "introducing", "releases"]):
        return "product_launch", 6
    if any(w in text_lower for w in ["hiring", "open roles", "growing team", "we're hiring"]):
        return "hiring", 5

    return None, 0


def _dedup_signals(signals: list[dict]) -> list[dict]:
    """Remove duplicate signals of the same type."""
    seen_types = set()
    deduped = []
    for s in sorted(signals, key=lambda x: -x["urgency_score"]):
        key = f"{s['type']}:{s['content'][:50]}"
        if key not in seen_types:
            seen_types.add(key)
            deduped.append(s)
    return deduped


def run_signals(domains: list[str], db: SupabaseClient) -> None:
    """Run signal extraction in parallel."""
    logger.info(f"=== SKILL 4: SIGNALS ({len(domains)} domains) ===")
    total_signals = 0

    with ThreadPoolExecutor(max_workers=PARALLELISM["signals"]) as executor:
        futures = {executor.submit(_signals_one, d, db): d for d in domains}
        for future in as_completed(futures):
            domain = futures[future]
            try:
                result = future.result()
                total_signals += result.get("signals_found", 0)
            except Exception as e:
                logger.error(f"Signals exception for {domain}: {e}")

    logger.info(f"Signals: {total_signals} total signals found across {len(domains)} companies")


# ============================================================
# Skill 5: Segment
# ============================================================

def run_segment(domains: list[str], db: SupabaseClient) -> None:
    """Batch segment assignment using Claude."""
    logger.info(f"=== SKILL 5: SEGMENT ({len(domains)} domains) ===")

    segments = db.get_active_segments()
    if not segments:
        logger.warning("No active segments in DB — skipping segmentation")
        return

    # Get company records
    companies = []
    for domain in domains:
        c = db.get_company_by_domain(domain)
        if c and c.get("classification"):
            companies.append(c)

    if not companies:
        logger.warning("No classified companies to segment")
        return

    # Process in batches of 10
    batch_size = 10
    assigned = 0

    for i in range(0, len(companies), batch_size):
        batch = companies[i:i+batch_size]
        results = _segment_batch(batch, segments)
        for r in results:
            if r.get("segment_id") and r.get("fit_score") is not None:
                db.set_company_segment(
                    r["company_id"],
                    r["segment_id"],
                    r["fit_score"],
                    r["fit_tier"],
                )
                assigned += 1

    logger.info(f"Segment: {assigned}/{len(companies)} companies assigned")


def _segment_batch(companies: list[dict], segments: list[dict]) -> list[dict]:
    """Batch segment matching via Claude. Returns list of {company_id, segment_id, fit_score, fit_tier}."""
    try:
        import anthropic
        client = anthropic.Anthropic()

        company_profiles = json.dumps([{
            "company_id": c["id"],
            "domain": c["domain"],
            "name": c.get("name"),
            "classification": c.get("classification"),
            "sales_org": c.get("sales_org"),
            "tech_stack": c.get("tech_stack"),
        } for c in companies], indent=2)

        segment_defs = json.dumps([{
            "id": s["id"],
            "name": s["name"],
            "slug": s["slug"],
            "icp_definition": s.get("icp_definition"),
            "core_pain_hypothesis": s.get("core_pain_hypothesis"),
        } for s in segments], indent=2)

        prompt = f"""Match each company to the best ICP segment.

SEGMENTS:
{segment_defs}

COMPANIES:
{company_profiles}

For each company, output fit_score (0-100) and fit_tier (T1=75+, T2=50-74, T3=30-49, pass=<30).

Output ONLY valid JSON array:
[
  {{
    "company_id": "uuid",
    "segment_id": "uuid or null if no fit",
    "fit_score": 0-100,
    "fit_tier": "T1|T2|T3|pass",
    "reasoning": "1 sentence"
  }}
]"""

        msg = client.messages.create(
            model="claude-opus-4-5",
            max_tokens=2048,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = msg.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw)
    except ImportError:
        logger.warning("anthropic SDK not installed — segmentation skipped")
        return []
    except Exception as e:
        logger.error(f"Segment batch failed: {e}")
        return []


# ============================================================
# Attio sync
# ============================================================

def run_attio_sync(domains: list[str], db: SupabaseClient) -> None:
    """Sync all processed companies to Attio."""
    logger.info(f"=== ATTIO SYNC ({len(domains)} domains) ===")
    synced = 0
    failed = 0

    for domain in domains:
        company = db.get_company_by_domain(domain)
        if not company:
            continue
        try:
            record_id = sync_company_to_attio(company)
            if record_id:
                db.update("companies", {"id": company["id"]}, {
                    "attio_record_id": record_id,
                    "attio_synced_at": datetime.now(timezone.utc).isoformat(),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                })
                synced += 1
            else:
                failed += 1
        except Exception as e:
            logger.error(f"Attio sync failed for {domain}: {e}")
            failed += 1
        time.sleep(0.2)  # Rate limit

    logger.info(f"Attio sync: {synced} synced | {failed} failed")


# ============================================================
# Main pipeline
# ============================================================

def run_pipeline(
    domains_input: str | list[str],
    experiment_id: str = None,
    resume: bool = False,
    run_id: str = None,
) -> dict:
    """
    Full GTM pipeline.

    Args:
        domains_input: CSV file path or list of domain strings
        experiment_id: if provided, draft outreach for T1 companies after segmentation
        resume: if True, skip steps that already completed in a previous run
        run_id: unique ID for this run (for state persistence)

    Returns:
        summary dict with counts at each stage
    """
    run_id = run_id or str(uuid.uuid4())[:8]
    logger.info(f"=== BETON GTM PIPELINE START | run_id={run_id} ===")
    start_time = time.time()

    # Init DB
    db = SupabaseClient()

    # Load state for resume
    state = db.get_pipeline_state(run_id) if resume else {}
    if state:
        logger.info(f"Resuming pipeline from saved state (run_id={run_id})")

    summary = {
        "run_id": run_id,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "intake": 0,
        "prefilter_passed": 0,
        "research_done": 0,
        "sales_org_done": 0,
        "signals_done": 0,
        "segmented": 0,
        "attio_synced": 0,
        "outreach_drafted": 0,
    }

    try:
        # STEP 0: Intake
        if not state.get("intake_done"):
            new_domains = run_intake(domains_input, db)
            summary["intake"] = len(new_domains)
            state["intake_done"] = True
            state["new_domains"] = new_domains
            db.save_pipeline_state(run_id, state)
        else:
            new_domains = state.get("new_domains", [])
            logger.info(f"SKIP intake (already done) — {len(new_domains)} domains loaded from state")

        if not new_domains:
            logger.info("No new domains to process — pipeline done")
            return summary

        # STEP 1: Prefilter
        if not state.get("prefilter_done"):
            passed_domains = run_prefilter(new_domains, db)
            summary["prefilter_passed"] = len(passed_domains)
            state["prefilter_done"] = True
            state["passed_domains"] = passed_domains
            db.save_pipeline_state(run_id, state)
        else:
            passed_domains = state.get("passed_domains", [])
            logger.info(f"SKIP prefilter — {len(passed_domains)} domains from state")

        if not passed_domains:
            logger.info("All domains filtered out — pipeline done")
            return summary

        # STEP 2: Research
        if not state.get("research_done"):
            researched = run_research(passed_domains, db)
            summary["research_done"] = len(researched)
            state["research_done"] = True
            state["researched_domains"] = researched
            db.save_pipeline_state(run_id, state)
        else:
            researched = state.get("researched_domains", [])
            logger.info(f"SKIP research — {len(researched)} domains from state")

        # STEP 3: Sales Org
        if not state.get("sales_org_done"):
            run_sales_org(researched, db)
            summary["sales_org_done"] = len(researched)
            state["sales_org_done"] = True
            db.save_pipeline_state(run_id, state)
        else:
            logger.info("SKIP sales org (already done)")

        # STEP 4: Signals
        if not state.get("signals_done"):
            run_signals(researched, db)
            summary["signals_done"] = len(researched)
            state["signals_done"] = True
            db.save_pipeline_state(run_id, state)
        else:
            logger.info("SKIP signals (already done)")

        # STEP 5: Segment
        if not state.get("segment_done"):
            run_segment(researched, db)
            summary["segmented"] = len(researched)
            state["segment_done"] = True
            db.save_pipeline_state(run_id, state)
        else:
            logger.info("SKIP segmentation (already done)")

        # STEP 6: Attio sync
        if not state.get("attio_done"):
            run_attio_sync(researched, db)
            summary["attio_synced"] = len(researched)
            state["attio_done"] = True
            db.save_pipeline_state(run_id, state)
        else:
            logger.info("SKIP Attio sync (already done)")

        # STEP 7: Outreach drafting (optional, requires experiment_id)
        if experiment_id and not state.get("outreach_done"):
            outreach_count = run_outreach(researched, experiment_id, db)
            summary["outreach_drafted"] = outreach_count
            state["outreach_done"] = True
            db.save_pipeline_state(run_id, state)
        elif not experiment_id:
            logger.info("Skipping outreach (no experiment_id provided)")

    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user — state saved for resume")
        db.save_pipeline_state(run_id, state)
    except Exception as e:
        logger.error(f"Pipeline error: {e}", exc_info=True)
        db.save_pipeline_state(run_id, state)
        raise
    finally:
        db.close()

    elapsed = time.time() - start_time
    summary["elapsed_seconds"] = round(elapsed, 1)
    summary["completed_at"] = datetime.now(timezone.utc).isoformat()

    logger.info(f"=== PIPELINE COMPLETE | run_id={run_id} | elapsed={elapsed:.0f}s ===")
    logger.info(json.dumps(summary, indent=2))

    return summary


# ============================================================
# Skill 6: Outreach (pipeline wrapper)
# ============================================================

def run_outreach(domains: list[str], experiment_id: str, db: SupabaseClient) -> int:
    """
    Draft outreach sequences for T1 companies with contacts.

    The actual drafting logic lives in skills/gtm-outreach/SKILL.md and is
    designed to run in the OpenClaw agent context (Claude has access to tools).

    This pipeline wrapper:
    1. Finds T1 companies from the given domain list
    2. Finds their top contact (highest seniority)
    3. Logs what needs drafting — actual drafts are generated by the skill

    Returns count of outreach records queued.
    """
    logger.info(f"=== SKILL 6: OUTREACH | experiment={experiment_id} ===")

    experiment = db.get_experiment(experiment_id)
    if not experiment:
        logger.error(f"Experiment {experiment_id} not found")
        return 0

    queued = 0
    for domain in domains:
        company = db.get_company_by_domain(domain)
        if not company or company.get("fit_tier") != "T1":
            continue

        contacts = db.get_contacts_for_company(company["id"])
        if not contacts:
            logger.info(f"No contacts for T1 company {domain} — skipping outreach")
            continue

        # Pick highest seniority contact
        seniority_order = {"c-suite": 0, "vp": 1, "director": 2, "manager": 3, "ic": 4}
        contacts_sorted = sorted(
            contacts,
            key=lambda c: seniority_order.get(c.get("seniority", "ic"), 5)
        )
        contact = contacts_sorted[0]

        # Check if outreach already exists
        existing = db.select_raw("outreach", {
            "company_id": f"eq.{company['id']}",
            "experiment_id": f"eq.{experiment_id}",
            "select": "id",
        })
        if existing:
            logger.debug(f"Outreach already exists for {domain} in experiment {experiment_id}")
            continue

        # Queue for drafting (actual draft generated by OpenClaw agent running the skill)
        # Here we create a placeholder record
        try:
            default_config = {
                "length": 3,
                "timing": [0, 3, 7],
                "steps": [
                    {"step": 1, "type": "email", "angle": "signal-led"},
                    {"step": 2, "type": "email", "angle": "value-prop"},
                    {"step": 3, "type": "email", "angle": "breakup"},
                ],
            }
            sequence_config = experiment.get("sequence_config") or default_config

            db.create_outreach_draft(
                experiment_id=experiment_id,
                company_id=company["id"],
                contact_id=contact["id"],
                sequence=[],  # Empty — to be filled by gtm-outreach skill
                sequence_config=sequence_config,
            )
            queued += 1
            logger.info(f"Queued outreach for {domain} — contact: {contact.get('title')}")
        except Exception as e:
            logger.error(f"Failed to queue outreach for {domain}: {e}")

    logger.info(f"Outreach: {queued} sequences queued for drafting")
    return queued


# ============================================================
# CLI entry point
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Beton GTM Pipeline")
    parser.add_argument("--domains", help="CSV file path or comma-separated domains")
    parser.add_argument("--experiment-id", help="Experiment UUID for outreach drafting")
    parser.add_argument("--resume", action="store_true", help="Resume an interrupted run")
    parser.add_argument("--run-id", help="Specific run ID to resume")
    parser.add_argument("--status", action="store_true", help="Show pipeline status stats")
    args = parser.parse_args()

    if args.status:
        db = SupabaseClient()
        try:
            stats = {
                "raw": len(db.get_companies_by_status("raw")),
                "prefiltered": len(db.get_companies_by_status("prefiltered")),
                "classified": len(db.get_companies_by_status("classified")),
                "scored": len(db.get_companies_by_status("scored")),
                "skip": len(db.get_companies_by_status("skip")),
            }
            print(json.dumps(stats, indent=2))
        finally:
            db.close()
        return

    if not args.domains:
        parser.print_help()
        sys.exit(1)

    # Parse domains input
    if "," in args.domains and not os.path.exists(args.domains):
        domains_input = [d.strip() for d in args.domains.split(",")]
    else:
        domains_input = args.domains

    summary = run_pipeline(
        domains_input=domains_input,
        experiment_id=args.experiment_id,
        resume=args.resume,
        run_id=args.run_id,
    )

    print("\n=== PIPELINE SUMMARY ===")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
