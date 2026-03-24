#!/usr/bin/env python3
"""
run_classify.py — Stage 2: LLM classification of scraped companies.

Reads logs/research/{domain}.json files written by run_scrape.py,
runs a single combined Claude call per company that does:
  1. Company classification (b2b, saas, gtm_motion, vertical, etc.)
  2. Open role extraction from career_content (department, seniority, title)
  3. Sales org inference (hiring_signal, open_sales_roles, open_revops_roles, open_cs_roles)
  4. Tech stack detection

Writes to Supabase: company_classification, company_sales_org, company_open_roles, company_tech_stack
Sets research_status = 'classified'

Designed to be called by a cron agentTurn every 10 minutes, but also runs standalone.

Usage:
    python3 run_classify.py [--limit=N] [--concurrency=N] [--dry-run] [--reclassify]

Flags:
    --limit=N        Max companies to classify (default: 100)
    --concurrency=N  Parallel Claude calls (default: 10)
    --dry-run        Run Claude but skip Supabase writes
    --reclassify     Also process already-classified companies (re-run Claude)
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

import anthropic
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

LOGS_DIR = ".//logs/research"
CONCURRENCY = 10
LIMIT = 100
PAGE_SIZE = 500

# Classification model — haiku-4-5 is the only model that works on oat01 subscription token
PRIMARY_MODEL = "claude-haiku-4-5"

# ── Anthropic client ──────────────────────────────────────────────────────────

def _get_anthropic_client() -> anthropic.Anthropic:
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        token_path = os.path.expanduser("~/.openclaw/agents/main/agent/auth-profiles.json")
        if os.path.exists(token_path):
            profiles = json.load(open(token_path)).get("profiles", {})
            key = profiles.get("anthropic:manual", {}).get("token")
    if not key:
        raise RuntimeError("No Anthropic API key found.")
    return anthropic.Anthropic(api_key=key)

_anthropic_client: Optional[anthropic.Anthropic] = None

def get_client() -> anthropic.Anthropic:
    global _anthropic_client
    if _anthropic_client is None:
        _anthropic_client = _get_anthropic_client()
    return _anthropic_client

# ── Prompt ────────────────────────────────────────────────────────────────────

COMBINED_PROMPT = """Analyze this company based on scraped website content and career pages.
Return a single JSON object with ALL fields below. No markdown, no explanation.

COMPANY CONTENT (website pages):
{content}

CAREER PAGE CONTENT (job listings):
{career_content}

Return this exact JSON structure:
{{
  "companyResolvedName": "string — cleaned company name",
  "primaryWebsite": "string — canonical domain",
  "b2b": true/false — sells to businesses (not consumers),
  "saas": true/false — software-as-a-service product,
  "gtmMotion": "PLG" | "SLG" | "hybrid" | "D2C" | null,
  "vertical": "string — specific industry vertical (e.g. devtools, fintech, healthtech, edtech, hrtech, martech, ecommerce, logistics, proptech, legaltech, cleantech, cybersecurity, other)",
  "businessModel": "subscription" | "usage-based" | "freemium" | "marketplace" | "ecommerce" | "services" | "other",
  "sellsTo": "enterprise" | "mid-market" | "SMB" | "developer" | "consumer" | "mixed",
  "pricingModel": "tiered" | "per-seat" | "usage-based" | "freemium" | "custom-only" | "transactional" | "unknown",
  "description": "string — 2-3 sentence product description",
  "evidence": "string — key signals that informed gtmMotion classification (CTAs seen, pricing structure, etc.)",
  "keyFeatures": ["string", ...],
  "openRoles": [
    {{
      "title": "string — exact job title",
      "department": "sales" | "revops" | "cs" | "engineering" | "marketing" | "product" | "design" | "hr" | "finance" | "operations" | "other",
      "seniority": "junior" | "mid" | "senior" | "lead" | "director" | "vp" | "c-level" | "unknown",
      "remote": true/false/null,
      "location": "string or null"
    }}
  ],
  "hiringSignal": true/false — any open roles found at all,
  "openSalesRoles": integer — count of roles in sales department,
  "openRevopsRoles": integer — count of roles in revops department,
  "openCsRoles": integer — count of roles in cs department,
  "crm": "salesforce" | "hubspot" | "pipedrive" | "attio" | "zoho" | "other" | null,
  "salesEngagementTool": "outreach" | "salesloft" | "apollo" | "lemlist" | "instantly" | "other" | null,
  "dataWarehouse": "snowflake" | "bigquery" | "redshift" | "databricks" | "other" | null,
  "analytics": "posthog" | "mixpanel" | "amplitude" | "heap" | "other" | null
}}

Rules:
- openRoles: extract from career content ONLY. Empty array if no career content or no open roles found.
- hiringSignal: true if openRoles is non-empty.
- openSalesRoles/openRevopsRoles/openCsRoles: count from openRoles by department.
- b2b/saas/gtmMotion: null if genuinely unknown, never guess.
- Respond with ONLY the JSON object, nothing else."""


# ── JSON extraction ───────────────────────────────────────────────────────────

def extract_json(text: str) -> Optional[dict]:
    try:
        return json.loads(text.strip())
    except Exception:
        pass
    match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except Exception:
            pass
    # Find largest {...} block
    match2 = re.search(r"\{.*\}", text, re.DOTALL)
    if match2:
        try:
            return json.loads(match2.group(0))
        except Exception:
            pass
    return None


# ── Classification ────────────────────────────────────────────────────────────

def classify(log: dict, domain: str) -> Optional[dict]:
    """Run combined classification + career extraction in one Claude call."""
    content = log.get("raw_content") or ""
    career_content = log.get("career_content") or ""

    # Truncate to fit context (content: 12k chars, career: 8k chars)
    content_trunc = content[:12000]
    career_trunc = career_content[:8000]

    if not content_trunc.strip():
        logger.warning(f"[{domain}] Empty content — skipping")
        return None

    prompt = COMBINED_PROMPT.format(content=content_trunc, career_content=career_trunc)
    client = get_client()

    for attempt in range(2):
        try:
            r = client.messages.create(
                model=PRIMARY_MODEL,
                max_tokens=2048,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = r.content[0].text
            result = extract_json(raw)
            if result:
                return result
            # Retry with stricter instruction
            if attempt == 0:
                logger.warning(f"[{domain}] JSON parse failed, retrying...")
                prompt = "Respond with ONLY a raw JSON object, no markdown.\n\n" + prompt
        except Exception as e:
            logger.warning(f"[{domain}] Claude error (attempt {attempt+1}): {e}")

    logger.error(f"[{domain}] Classification failed after 2 attempts")
    return None


# ── Supabase writes ───────────────────────────────────────────────────────────

async def upsert_classification(client: httpx.AsyncClient, company_id: str, cls: dict):
    gtm = cls.get("gtmMotion")
    if gtm not in ("PLG", "SLG", "hybrid", "D2C"):
        gtm = None
    record = {
        "company_id": company_id,
        "b2b": cls.get("b2b"),
        "saas": cls.get("saas"),
        "gtm_motion": gtm,
        "vertical": cls.get("vertical"),
        "business_model": cls.get("businessModel"),
        "sells_to": cls.get("sellsTo"),
        "pricing_model": cls.get("pricingModel"),
        "description": cls.get("description"),
        "evidence": json.dumps(cls.get("evidence")) if isinstance(cls.get("evidence"), dict) else cls.get("evidence"),
        "classified_at": datetime.now(timezone.utc).isoformat(),
    }
    r = await client.post(
        f"{SUPABASE_BASE}/rest/v1/company_classification?on_conflict=company_id",
        headers={**SUPABASE_H, "Prefer": "resolution=merge-duplicates,return=minimal"},
        json=record, timeout=15,
    )
    r.raise_for_status()

async def upsert_sales_org(client: httpx.AsyncClient, company_id: str, cls: dict):
    record = {
        "company_id": company_id,
        "hiring_signal": cls.get("hiringSignal", False),
        "open_sales_roles": cls.get("openSalesRoles", 0),
        "open_revops_roles": cls.get("openRevopsRoles", 0),
        "open_cs_roles": cls.get("openCsRoles", 0),
        "enriched_at": datetime.now(timezone.utc).isoformat(),
    }
    r = await client.post(
        f"{SUPABASE_BASE}/rest/v1/company_sales_org?on_conflict=company_id",
        headers={**SUPABASE_H, "Prefer": "resolution=merge-duplicates,return=minimal"},
        json=record, timeout=15,
    )
    r.raise_for_status()

async def upsert_tech_stack(client: httpx.AsyncClient, company_id: str, cls: dict):
    record = {
        "company_id": company_id,
        "crm": cls.get("crm"),
        "sales_engagement_tool": cls.get("salesEngagementTool"),
        "data_warehouse": cls.get("dataWarehouse"),
        "analytics": cls.get("analytics") or "posthog",  # all companies confirmed via Wappalyzer
        "enriched_at": datetime.now(timezone.utc).isoformat(),
    }
    r = await client.post(
        f"{SUPABASE_BASE}/rest/v1/company_tech_stack?on_conflict=company_id",
        headers={**SUPABASE_H, "Prefer": "resolution=merge-duplicates,return=minimal"},
        json=record, timeout=15,
    )
    r.raise_for_status()

async def upsert_open_roles(client: httpx.AsyncClient, company_id: str, roles: list):
    if not roles:
        return
    # Delete existing roles for this company and re-insert (clean slate)
    await client.delete(
        f"{SUPABASE_BASE}/rest/v1/company_open_roles?company_id=eq.{company_id}",
        headers={**SUPABASE_H, "Prefer": "return=minimal"}, timeout=10,
    )
    records = [
        {
            "company_id": company_id,
            "title": r.get("title"),
            "function": r.get("department"),
            "seniority": r.get("seniority"),
            "remote": r.get("remote"),
            "location": r.get("location"),
        }
        for r in roles
    ]
    r = await client.post(
        f"{SUPABASE_BASE}/rest/v1/company_open_roles",
        headers={**SUPABASE_H, "Prefer": "return=minimal"},
        json=records, timeout=15,
    )
    r.raise_for_status()

async def set_classified(client: httpx.AsyncClient, company_id: str):
    r = await client.patch(
        f"{SUPABASE_BASE}/rest/v1/companies?id=eq.{company_id}",
        headers={**SUPABASE_H, "Prefer": "return=minimal"},
        json={"research_status": "classified"},
        timeout=10,
    )
    r.raise_for_status()

# ── Fetch scraped companies ───────────────────────────────────────────────────

async def fetch_scraped(client: httpx.AsyncClient, limit: int, reclassify: bool) -> list[dict]:
    statuses = "research_status=in.(scraped,classified)" if reclassify else "research_status=eq.scraped"
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

# ── Company processor ─────────────────────────────────────────────────────────

async def process_company(
    http: httpx.AsyncClient,
    company: dict,
    semaphore: asyncio.Semaphore,
    results: list,
    dry_run: bool,
):
    domain = company["domain"]
    company_id = company["id"]

    async with semaphore:
        # Load log
        safe = domain.replace("/", "_").replace(":", "_")
        log_path = os.path.join(LOGS_DIR, f"{safe}.json")
        if not os.path.exists(log_path):
            logger.warning(f"[{domain}] No log file — skipping")
            results.append({"domain": domain, "status": "no_log"})
            return

        try:
            log = json.load(open(log_path))
        except Exception as e:
            logger.error(f"[{domain}] Log parse error: {e}")
            results.append({"domain": domain, "status": "error"})
            return

        # Run combined classification (blocking call in thread pool to not starve event loop)
        loop = asyncio.get_event_loop()
        cls = await loop.run_in_executor(None, classify, log, domain)

        if not cls:
            results.append({"domain": domain, "status": "classification_failed"})
            return

        roles = cls.get("openRoles") or []
        logger.info(
            f"[{domain}] classified: b2b={cls.get('b2b')} saas={cls.get('saas')} "
            f"gtm={cls.get('gtmMotion')} roles={len(roles)}"
        )

        # Update log with classification
        log["classified_at"] = datetime.now(timezone.utc).isoformat()
        log["classification"] = cls
        with open(log_path, "w") as f:
            json.dump(log, f)

        if dry_run:
            results.append({"domain": domain, "status": "classified_dry"})
            return

        # Write to Supabase (all tables, concurrently)
        try:
            await asyncio.gather(
                upsert_classification(http, company_id, cls),
                upsert_sales_org(http, company_id, cls),
                upsert_tech_stack(http, company_id, cls),
                upsert_open_roles(http, company_id, roles),
            )
            await set_classified(http, company_id)
            results.append({"domain": domain, "status": "classified", "roles": len(roles)})
            logger.info(f"[{domain}] ✓ saved ({len(roles)} roles)")
        except Exception as e:
            logger.error(f"[{domain}] Supabase write error: {e}")
            results.append({"domain": domain, "status": "db_error", "error": str(e)})


# ── Entry point ───────────────────────────────────────────────────────────────

async def main():
    limit = LIMIT
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

    logger.info(f"run_classify: limit={limit}, concurrency={concurrency}, dry_run={dry_run}, reclassify={reclassify}")

    async with httpx.AsyncClient(follow_redirects=True, timeout=60.0) as http:
        companies = await fetch_scraped(http, limit, reclassify)
        logger.info(f"Found {len(companies)} companies to classify")

        if not companies:
            logger.info("Nothing to classify.")
            print("CLASSIFY_DONE: 0 companies")
            return

        semaphore = asyncio.Semaphore(concurrency)
        results = []
        tasks = [process_company(http, c, semaphore, results, dry_run) for c in companies]
        await asyncio.gather(*tasks)

    classified = sum(1 for r in results if r["status"] == "classified")
    failed = sum(1 for r in results if "fail" in r["status"] or "error" in r["status"])
    total_roles = sum(r.get("roles", 0) for r in results)

    print(f"\n{'='*50}")
    print(f"CLASSIFY RUN — COMPLETE")
    print(f"{'='*50}")
    print(f"Classified : {classified}")
    print(f"Failed     : {failed}")
    print(f"Roles found: {total_roles}")
    print(f"Total      : {len(results)}")
    print(f"CLASSIFY_DONE: {classified} companies, {total_roles} roles")


if __name__ == "__main__":
    asyncio.run(main())
