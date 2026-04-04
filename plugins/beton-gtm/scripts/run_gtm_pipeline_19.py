#!/usr/bin/env python3
"""
GTM Pipeline for 19 6sense+PostHog companies.
Processes: research → sales-org (Apollo) → segment/score

Usage:
    python3 run_gtm_pipeline_19.py [--dry-run] [--skip-research] [--skip-sales-org] [--skip-segment]
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
import anthropic as _anthropic

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
SUPABASE_URL = "https://amygtwoqujluepibcnfs.supabase.co"
SUPABASE_KEY = "YOUR_SUPABASE_SERVICE_ROLE_KEY"
FIRECRAWL_ENDPOINT = "http://34.122.195.243:3002"
APOLLO_KEY = "M1roN-Djmy7kjJGYMQsSlg"

SUPABASE_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

def get_anthropic_key():
    try:
        with open(os.path.expanduser("~/.openclaw/agents/main/agent/auth-profiles.json")) as f:
            d = json.load(f)
        return d["profiles"]["anthropic:manual"]["token"]
    except Exception:
        return os.getenv("ANTHROPIC_API_KEY", "")

def now_iso():
    return datetime.now(timezone.utc).isoformat()

# ── Target companies ──────────────────────────────────────────────────────────
COMPANIES = [
    # prefiltered → need research + sales-org + segment
    {"domain": "vsim.ua",          "id": "a4d0df2e-4595-4c04-9143-121e903f6ad7", "name": "-",           "status": "prefiltered"},
    {"domain": "prompts.ai",       "id": "fbb4c7c8-02a3-423d-ab80-b955fe42b47d", "name": "[prompts.ai]", "status": "prefiltered"},
    {"domain": "opospills.com",    "id": "45374916-051d-4bc5-b086-4f93d2d6ad41", "name": "Opospills",   "status": "prefiltered"},
    {"domain": "workers.dev",      "id": "eeb573a2-a646-4abe-8bec-7dd481d98715", "name": "105648539",   "status": "prefiltered"},
    {"domain": "sitefire.ai",      "id": "e1a6fcc3-4cc8-4f14-b97a-83d424a2942a", "name": "106162152",   "status": "prefiltered"},
    {"domain": "natagent.ai",      "id": "1ab94e53-c65f-4415-810f-71d95487f3cb", "name": "107526313",   "status": "prefiltered"},
    {"domain": "topcmm.com",       "id": "dcb01849-4f0c-4be9-9412-640a93bd72f4", "name": "123AICHAT",   "status": "prefiltered"},
    {"domain": "sultan.sa",        "id": "4d889b2a-5036-4222-bf18-7823b68c9e1b", "name": "1970",        "status": "prefiltered"},
    {"domain": "hotels-salzburg.org", "id": "32bee76f-1622-4e7c-b190-933d58dcb1fc", "name": "2026",    "status": "prefiltered"},
    {"domain": "nscp.org",         "id": "72fa8a78-e957-4fbe-8181-20ddbff11a3c", "name": "281674",      "status": "prefiltered"},
    {"domain": "rentmyequipment.com","id": "8c943682-01c5-418e-b8d4-6a39dc04625a","name": "2QUIP",      "status": "prefiltered"},
    {"domain": "re2.ai",           "id": "1d1b2609-6141-4a99-85a7-b82d0d519f2e", "name": "36077507",    "status": "prefiltered"},
    {"domain": "bp.agency",        "id": "79839e1a-9083-435a-a905-0bb385e6b84a", "name": "416826",      "status": "prefiltered"},
    # classified → need sales-org + segment only
    {"domain": "move.nl",          "id": "2d5395b5-8fb6-46ef-9d29-1247ad314771", "name": "11 Makelaars","status": "classified"},
    {"domain": "join1440.com",     "id": "142d8831-97f6-4d4d-af08-899d78a968a5", "name": "1440",        "status": "classified"},
    {"domain": "2shortai.featureos.app","id": "88b6533d-4bef-47b3-8ba2-85787f97bef8","name": "2shortai","status": "classified"},
    {"domain": "emery.to",         "id": "62f786fe-dba5-40e9-8c28-202a459d6e96", "name": "2X Planner",  "status": "classified"},
    {"domain": "4.events",         "id": "91b7a88b-a4ae-4aa3-9b7d-aecf930e6c41", "name": "4.events",    "status": "classified"},
    {"domain": "mein-digitaler-energieberater.de","id": "e7b719cf-b0db-4156-ab27-21509b4731b4","name": "42watt","status": "classified"},
]

# ── Research step ─────────────────────────────────────────────────────────────
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
        logger.debug(f"scrape({url}): {e}")
    return None


async def research_company(client: httpx.AsyncClient, claude: _anthropic.AsyncAnthropic, domain: str) -> Optional[dict]:
    """Scrape and classify a company."""
    # Try main domain + pricing/about pages
    root = domain.split(".")
    # Remove app/dashboard subdomain prefixes
    skip_prefixes = {"dashboard", "app", "console", "go", "portal", "www", "login", "api", "help"}
    if len(root) > 2 and root[0].lower() in skip_prefixes:
        domain_clean = ".".join(root[1:])
    else:
        domain_clean = domain

    pages_to_try = [
        f"https://{domain_clean}",
        f"https://{domain_clean}/pricing",
        f"https://{domain_clean}/about",
    ]

    content_parts = []
    for url in pages_to_try:
        md = await firecrawl_scrape(client, url)
        if md and len(md) > 100:
            content_parts.append(md[:3000])
        if len(content_parts) >= 2:
            break

    if not content_parts:
        # Try Firecrawl map
        try:
            r = await client.post(
                f"{FIRECRAWL_ENDPOINT}/v1/map",
                json={"url": f"https://{domain_clean}", "limit": 10, "includeSubdomains": False},
                timeout=20.0,
            )
            if r.status_code == 200:
                links = r.json().get("links", r.json().get("urls", []))[:5]
                for url in links:
                    md = await firecrawl_scrape(client, url)
                    if md and len(md) > 100:
                        content_parts.append(md[:3000])
                    if len(content_parts) >= 2:
                        break
        except Exception:
            pass

    if not content_parts:
        logger.warning(f"[{domain}] No content from Firecrawl")
        return None

    content = "\n\n---\n\n".join(content_parts)

    # Classify with Claude
    try:
        prompt = CLASSIFY_PROMPT.format(domain=domain, content=content[:8000])
        msg = await claude.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}]
        )
        raw = msg.content[0].text.strip()
        if "```" in raw:
            raw = raw.split("```")[1].replace("json", "").strip()
        result = json.loads(raw)
        result["_content_len"] = len(content)
        return result
    except Exception as e:
        logger.warning(f"[{domain}] Claude classify error: {e}")
        return None


async def save_classification(client: httpx.AsyncClient, company_id: str, classification: dict):
    """Save classification to Supabase."""
    now = now_iso()
    # Update company status
    await client.patch(
        f"{SUPABASE_URL}/rest/v1/companies?id=eq.{company_id}",
        headers=SUPABASE_HEADERS,
        json={"research_status": "classified", "updated_at": now},
    )
    # Upsert classification
    payload = {
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
    r = await client.post(
        f"{SUPABASE_URL}/rest/v1/company_classification",
        headers={**SUPABASE_HEADERS, "Prefer": "resolution=merge-duplicates,return=minimal"},
        json=payload,
    )
    return r.status_code


# ── Apollo sales-org step ─────────────────────────────────────────────────────
async def apollo_enrich_org(client: httpx.AsyncClient, domain: str) -> Optional[dict]:
    """Get org data from Apollo (free, no credits)."""
    try:
        r = await client.get(
            "https://api.apollo.io/v1/organizations/enrich",
            headers={"X-Api-Key": APOLLO_KEY},
            params={"domain": domain},
            timeout=20.0,
        )
        if r.status_code == 200:
            org = r.json().get("organization", {})
            return {
                "org_id": org.get("id"),
                "employees_count": org.get("estimated_num_employees") or org.get("employee_count"),
                "industry": org.get("industry"),
                "name": org.get("name"),
            }
    except Exception as e:
        logger.warning(f"Apollo enrich error for {domain}: {e}")
    return None


async def save_sales_org(client: httpx.AsyncClient, company_id: str, org_data: dict):
    """Save Apollo org enrichment to Supabase."""
    now = now_iso()
    # Upsert company_sales_org
    r = await client.post(
        f"{SUPABASE_URL}/rest/v1/company_sales_org?on_conflict=company_id",
        headers={**SUPABASE_HEADERS, "Prefer": "resolution=merge-duplicates,return=minimal"},
        json={
            "company_id": company_id,
            "open_sales_roles": None,
            "open_revops_roles": None,
            "open_cs_roles": None,
            "hiring_signal": False,
            "enriched_at": now,
        },
        timeout=15,
    )
    # Save firmographics (only employees_count, no industry column)
    if org_data.get("employees_count"):
        await client.post(
            f"{SUPABASE_URL}/rest/v1/company_firmographics?on_conflict=company_id",
            headers={**SUPABASE_HEADERS, "Prefer": "resolution=merge-duplicates,return=minimal"},
            json={
                "company_id": company_id,
                "employees_count": org_data.get("employees_count"),
                "enriched_at": now,
            },
            timeout=15,
        )
    return r.status_code


# ── Segment matching ──────────────────────────────────────────────────────────
VERTICAL_MAP = {
    "devtools_infra": ["devtools", "developer tool", "developer platform", "developer experience",
                       "platform engineering", "mlops", "ai infrastructure", "ai devtools", "ml platform",
                       "data infrastructure", "open source tools", "api platform", "sdk platform",
                       "devops platform", "ci/cd", "observability platform", "monitoring platform",
                       "cloud infrastructure", "database platform", "infrastructure software"],
    "cybersecurity": ["cybersecurity", "cyber security", "network security", "application security",
                      "identity and access", "fraud detection", "data privacy", "compliance platform",
                      "zero trust", "endpoint security"],
    "ai_ml": ["artificial intelligence", "machine learning", "llm", "generative ai", "computer vision",
              "nlp", "natural language", "ai tools", "ai automation", "ai platform", "ai agents",
              "ai infrastructure", "ai devtools", "ai image", "ai video", "ai content",
              "ai productivity", "ai assistant", "ai workflow"],
    "analytics_bi": ["analytics platform", "business intelligence", "data analytics",
                     "data visualization", "reporting platform", "data platform", "revenue analytics",
                     "product analytics", "marketing analytics"],
    "fintech": ["fintech", "financial technology", "financial services", "payments platform",
                "banking platform", "neobank", "wealthtech", "crypto", "blockchain",
                "accounting software", "tax software", "payroll software",
                "billing platform", "invoicing", "expense management"],
    "healthtech": ["healthtech", "health technology", "healthcare platform", "medical software",
                   "clinical", "telemedicine", "mental health platform", "health analytics",
                   "patient management"],
    "edtech": ["edtech", "education technology", "e-learning", "online learning", "learning platform",
               "course platform", "tutoring", "higher education software"],
    "hrtech": ["hr tech", "hrtech", "human resources software", "hris", "people ops",
               "employee management", "performance management", "workforce management"],
    "martech_adtech": ["martech", "marketing technology", "adtech", "advertising technology",
                       "digital marketing platform", "marketing automation", "email marketing platform",
                       "seo platform", "social media management"],
    "sales_enablement": ["sales enablement", "sales intelligence", "sales engagement", "revenue intelligence",
                         "revenue operations", "revops", "sales automation", "crm",
                         "customer relationship", "sales analytics"],
    "ecommerce_retail": ["ecommerce", "e-commerce", "online retail", "marketplace platform",
                         "commerce platform", "d2c"],
    "real_estate": ["real estate platform", "commercial real estate", "residential real estate",
                    "real estate tech", "property listing", "real estate crm"],
    "proptech": ["proptech", "property technology", "property management software"],
    "travel_hospitality": ["travel platform", "travel technology", "hospitality software", "hotel software",
                           "booking platform", "tourism platform", "restaurant software", "hotel management"],
    "cleantech_climatetech": ["cleantech", "climate tech", "energy platform", "solar software",
                              "renewable energy", "sustainability platform", "carbon", "green tech",
                              "clean energy"],
    "legal_tech": ["legal tech", "legaltech", "legal platform", "legal software"],
    "media_entertainment": ["media platform", "entertainment platform", "streaming platform",
                            "content platform", "digital media", "podcast platform"],
    "logistics_supplychain": ["logistics platform", "supply chain software", "fleet management",
                              "shipping platform", "freight technology", "warehousing software"],
    "enterprise_saas": ["saas", "software as a service", "b2b software", "enterprise software",
                        "productivity software", "workflow platform", "project management software",
                        "collaboration software", "no-code platform", "low-code platform",
                        "business software", "information technology", "tool", "platform"],
    "other": [],
}

def score_vs_segment(company_data: dict, segment: dict) -> tuple[int, str, list]:
    icp = segment.get("icp_definition") or {}
    reasons = []
    disqualifiers = []
    score = 0
    max_score = 0

    cc = company_data.get("classification") or {}
    cso = company_data.get("sales_org") or {}
    ct = company_data.get("tech_stack") or {}
    firm = company_data.get("firmographics") or {}

    # B2B check
    if "customer_type" in icp:
        max_score += 20
        company_b2b = cc.get("b2b")
        seg_b2b = icp["customer_type"] == "b2b"
        if company_b2b is None:
            return 0, "Skipped: b2b unknown", []
        elif company_b2b == seg_b2b:
            if seg_b2b:
                score += 20
                reasons.append("b2b: ✓")
            else:
                score += 8
                reasons.append("b2b: ⚠ b2c")
                disqualifiers.append("b2c")
        else:
            return 0, f"Disqualified: b2b={company_b2b} vs segment b2b={seg_b2b}", []

    # SaaS check
    if "saas" in icp:
        max_score += 15
        company_saas = cc.get("saas")
        seg_saas = icp["saas"]
        if company_saas is None:
            return 0, "Skipped: saas unknown", []
        elif company_saas == seg_saas:
            score += 15
            reasons.append("saas: ✓")
        else:
            return 0, f"Disqualified: saas={company_saas} vs segment saas={seg_saas}", []

    # Headcount
    if "headcount_range" in icp:
        max_score += 20
        emp = firm.get("employees_count")
        hmin = icp["headcount_range"].get("min", 0)
        hmax = icp["headcount_range"].get("max", 999999)
        if emp is None:
            reasons.append(f"headcount: no data (segment wants {hmin}-{hmax})")
        elif hmin <= emp <= hmax:
            score += 20
            reasons.append(f"headcount: ✓ {emp} in [{hmin}-{hmax}]")
        elif emp < hmin * 0.5 or emp > hmax * 2:
            return 0, f"Disqualified: headcount {emp} far outside {hmin}-{hmax}", []
        else:
            score += 8
            reasons.append(f"headcount: partial {emp} vs [{hmin}-{hmax}]")

    # Vertical/industry
    if "industry" in icp:
        max_score += 20
        industry_key = icp["industry"]
        company_vertical = (cc.get("vertical") or "").lower()
        keywords = VERTICAL_MAP.get(industry_key, [industry_key.lower()])
        if industry_key == "other":
            score += 8
            reasons.append("vertical: other segment")
        elif not company_vertical:
            reasons.append("vertical: unknown")
        elif any(kw in company_vertical for kw in keywords):
            score += 20
            reasons.append(f"vertical: ✓ '{company_vertical}' → {industry_key}")
        else:
            return 0, f"Disqualified: vertical '{company_vertical}' ✗ {industry_key}", ["vertical_mismatch"]

    # RevOps signal
    if icp.get("has_revops"):
        max_score += 15
        open_revops = cso.get("open_revops_roles")
        hiring_signal = cso.get("hiring_signal")
        if open_revops and open_revops > 0:
            score += 15
            reasons.append(f"revops: ✓")
        elif hiring_signal:
            score += 8
            reasons.append("revops: hiring signal")
        else:
            reasons.append("revops: no signal")

    # Analytics (PostHog = always match for this cohort)
    if icp.get("has_dwh"):
        max_score += 10
        score += 10
        reasons.append("analytics: ✓ posthog")

    if max_score == 0:
        return 50, "No criteria to score", []
    normalized = int((score / max_score) * 100)
    return normalized, "; ".join(reasons), disqualifiers


def assign_tier(fit_score: int, disqualifiers: list = None) -> str:
    disqualifiers = disqualifiers or []
    if "b2c" in disqualifiers:
        if fit_score >= 50: return "T2"
        if fit_score >= 30: return "T3"
        return "pass"
    if fit_score >= 75 and "vertical_mismatch" not in disqualifiers: return "T1"
    if fit_score >= 50: return "T2"
    if fit_score >= 30: return "T3"
    return "pass"


async def fetch_segments(client: httpx.AsyncClient) -> list:
    r = await client.get(
        f"{SUPABASE_URL}/rest/v1/segments?is_active=eq.true"
        f"&select=id,name,slug,icp_definition&limit=1000",
        headers=SUPABASE_HEADERS, timeout=20,
    )
    r.raise_for_status()
    return r.json()


async def fetch_classification(client: httpx.AsyncClient, company_id: str) -> dict:
    r = await client.get(
        f"{SUPABASE_URL}/rest/v1/company_classification"
        f"?company_id=eq.{company_id}"
        f"&select=b2b,saas,gtm_motion,vertical,business_model,sells_to",
        headers=SUPABASE_HEADERS, timeout=15,
    )
    data = r.json()
    return data[0] if data else {}


async def fetch_firmographics(client: httpx.AsyncClient, company_id: str) -> dict:
    r = await client.get(
        f"{SUPABASE_URL}/rest/v1/company_firmographics"
        f"?company_id=eq.{company_id}&select=employees_count,hq_country",
        headers=SUPABASE_HEADERS, timeout=15,
    )
    if r.status_code != 200:
        return {}
    data = r.json()
    return data[0] if data else {}


async def save_segment_match(client: httpx.AsyncClient, company_id: str, match: dict):
    now = now_iso()
    r = await client.post(
        f"{SUPABASE_URL}/rest/v1/company_segments?on_conflict=company_id,segment_id",
        headers={**SUPABASE_HEADERS, "Prefer": "resolution=merge-duplicates,return=minimal"},
        json={
            "company_id": company_id,
            "segment_id": match["segment_id"],
            "fit_score": match["fit_score"],
            "fit_tier": match["fit_tier"],
            "fit_reasoning": match["fit_reasoning"],
            "assigned_at": now,
        },
        timeout=15,
    )
    r.raise_for_status()
    # Mark as scored
    await client.patch(
        f"{SUPABASE_URL}/rest/v1/companies?id=eq.{company_id}",
        headers={**SUPABASE_HEADERS, "Prefer": "return=minimal"},
        json={"research_status": "scored"},
        timeout=10,
    )


# ── Main pipeline ─────────────────────────────────────────────────────────────
async def process_batch(companies: list, segments: list, claude: _anthropic.AsyncAnthropic,
                        skip_research: bool = False, skip_sales_org: bool = False,
                        skip_segment: bool = False, dry_run: bool = False) -> list:
    results = []
    async with httpx.AsyncClient(follow_redirects=True, timeout=60.0) as http:
        for i, company in enumerate(companies):
            domain = company["domain"]
            company_id = company["id"]
            status = company["status"]
            result = {"domain": domain, "id": company_id, "initial_status": status}

            logger.info(f"\n[{i+1}/{len(companies)}] Processing {domain} (status={status})")

            # ── Step 1: Research (only for prefiltered) ─────────────────────
            if status == "prefiltered" and not skip_research:
                logger.info(f"  → Step 1: Research...")
                classification = await research_company(http, claude, domain)
                if classification:
                    result["classification"] = {
                        "b2b": classification.get("b2b"),
                        "saas": classification.get("saas"),
                        "vertical": classification.get("vertical"),
                        "gtm_motion": classification.get("gtm_motion"),
                        "description": classification.get("description"),
                    }
                    logger.info(f"  ✓ Classified: b2b={classification.get('b2b')} saas={classification.get('saas')} vertical={classification.get('vertical')}")
                    if not dry_run:
                        await save_classification(http, company_id, classification)
                    result["research_step"] = "ok"
                    status = "classified"  # Update for next steps
                else:
                    logger.warning(f"  ✗ Research failed — no content")
                    result["research_step"] = "failed"
                    results.append(result)
                    continue
            elif status == "prefiltered" and skip_research:
                logger.info(f"  → Step 1: Skipped (--skip-research)")
                result["research_step"] = "skipped"
            else:
                result["research_step"] = "not_needed"

            # ── Step 2: Apollo org enrichment ───────────────────────────────
            if not skip_sales_org:
                logger.info(f"  → Step 2: Apollo enrichment...")
                org_data = await apollo_enrich_org(http, domain)
                if org_data:
                    result["apollo"] = {
                        "org_id": org_data.get("org_id"),
                        "employees_count": org_data.get("employees_count"),
                        "industry": org_data.get("industry"),
                    }
                    logger.info(f"  ✓ Apollo: org_id={org_data.get('org_id')} employees={org_data.get('employees_count')} industry={org_data.get('industry')}")
                    if not dry_run:
                        await save_sales_org(http, company_id, org_data)
                    result["sales_org_step"] = "ok"
                else:
                    logger.warning(f"  ⚠ Apollo not found for {domain}")
                    result["sales_org_step"] = "not_found"
                    if not dry_run:
                        # Still save empty record to mark as processed
                        await save_sales_org(http, company_id, {})
            else:
                result["sales_org_step"] = "skipped"

            # ── Step 3: Segment matching ─────────────────────────────────────
            if not skip_segment:
                logger.info(f"  → Step 3: Segment matching...")
                # Fetch latest classification from Supabase
                cls_data = await fetch_classification(http, company_id)
                firm_data = await fetch_firmographics(http, company_id)
                so_data = {"analytics": "posthog"}  # PostHog always present

                company_enriched = {
                    "classification": cls_data,
                    "firmographics": firm_data,
                    "sales_org": so_data,
                    "tech_stack": {"analytics": "posthog"},
                }

                # Both b2b and saas are None → no real classification data
                if cls_data.get("b2b") is None and cls_data.get("saas") is None:
                    # Check if we have fresh classification from this run (dry-run or classified companies)
                    fresh_cls = result.get("classification")
                    if fresh_cls and (fresh_cls.get("b2b") is not None or fresh_cls.get("saas") is not None):
                        # Use in-memory classification for segmentation
                        cls_data = fresh_cls
                        company_enriched["classification"] = cls_data
                        logger.info(f"  → Using fresh in-memory classification for segmentation")
                    else:
                        logger.warning(f"  ✗ No classification data — skipping segmentation")
                        result["segment_step"] = "skipped_no_classification"
                        result["fit_tier"] = "pass"
                        results.append(result)
                        continue

                best_score = 0
                best_seg = None
                best_reasoning = ""
                best_disqualifiers = []

                for seg in segments:
                    score, reasoning, disqualifiers = score_vs_segment(company_enriched, seg)
                    if score > best_score:
                        best_score = score
                        best_seg = seg
                        best_reasoning = reasoning
                        best_disqualifiers = disqualifiers

                if best_seg and best_score > 0:
                    tier = assign_tier(best_score, best_disqualifiers)
                    match = {
                        "segment_id": best_seg["id"],
                        "segment_slug": best_seg["slug"],
                        "fit_score": best_score,
                        "fit_tier": tier,
                        "fit_reasoning": best_reasoning,
                    }
                    logger.info(f"  ✓ Segment: {tier} (score={best_score}) → {best_seg['slug']}")
                    result["segment_step"] = "ok"
                    result["fit_tier"] = tier
                    result["fit_score"] = best_score
                    result["segment_slug"] = best_seg["slug"]
                    if not dry_run:
                        try:
                            await save_segment_match(http, company_id, match)
                        except Exception as e:
                            logger.error(f"  ✗ Save segment error: {e}")
                            result["segment_step"] = "save_error"
                else:
                    logger.info(f"  → No segment match → pass")
                    result["segment_step"] = "no_match"
                    result["fit_tier"] = "pass"
            else:
                result["segment_step"] = "skipped"

            results.append(result)
            await asyncio.sleep(0.5)  # rate limit buffer

    return results


async def main():
    dry_run = "--dry-run" in sys.argv
    skip_research = "--skip-research" in sys.argv
    skip_sales_org = "--skip-sales-org" in sys.argv
    skip_segment = "--skip-segment" in sys.argv

    logger.info(f"GTM Pipeline for 19 companies (dry_run={dry_run})")

    anthropic_key = get_anthropic_key()
    claude = _anthropic.AsyncAnthropic(api_key=anthropic_key)

    async with httpx.AsyncClient(follow_redirects=True, timeout=60.0) as http:
        segments = await fetch_segments(http)
    logger.info(f"Loaded {len(segments)} active segments")

    # Process in batches of 5
    batch_size = 5
    all_results = []

    for batch_start in range(0, len(COMPANIES), batch_size):
        batch = COMPANIES[batch_start:batch_start + batch_size]
        batch_end = min(batch_start + batch_size, len(COMPANIES))
        logger.info(f"\n{'='*60}")
        logger.info(f"BATCH {batch_start//batch_size + 1}: companies {batch_start+1}-{batch_end}/{len(COMPANIES)}")
        logger.info(f"{'='*60}")

        batch_results = await process_batch(
            batch, segments, claude,
            skip_research=skip_research,
            skip_sales_org=skip_sales_org,
            skip_segment=skip_segment,
            dry_run=dry_run,
        )
        all_results.extend(batch_results)

        # Batch report
        print(f"\n--- Batch {batch_start//batch_size + 1} Summary ---")
        for r in batch_results:
            tier = r.get("fit_tier", "?")
            research = r.get("research_step", "-")
            so = r.get("sales_org_step", "-")
            seg = r.get("segment_step", "-")
            print(f"  {r['domain']:45} research={research:8} sales_org={so:12} segment={seg:8} tier={tier}")

    # Final summary
    print(f"\n\n{'='*60}")
    print(f"PIPELINE COMPLETE — {len(all_results)}/19 companies processed")
    print(f"{'='*60}")

    by_tier = {}
    research_ok = sum(1 for r in all_results if r.get("research_step") == "ok")
    research_fail = sum(1 for r in all_results if r.get("research_step") == "failed")
    sales_ok = sum(1 for r in all_results if r.get("sales_org_step") == "ok")
    sales_fail = sum(1 for r in all_results if r.get("sales_org_step") == "not_found")
    seg_ok = sum(1 for r in all_results if r.get("segment_step") == "ok")
    
    for r in all_results:
        tier = r.get("fit_tier", "?")
        by_tier[tier] = by_tier.get(tier, 0) + 1

    print(f"\nStep results:")
    print(f"  Research (prefiltered→classified): {research_ok} ok, {research_fail} failed")
    print(f"  Sales org (Apollo):                {sales_ok} ok, {sales_fail} not found")
    print(f"  Segment matching:                  {seg_ok} matched")

    print(f"\nFit tiers:")
    for tier in ["T1", "T2", "T3", "pass", "?"]:
        if tier in by_tier:
            print(f"  {tier}: {by_tier[tier]}")

    t1_t2 = [r for r in all_results if r.get("fit_tier") in ("T1", "T2")]
    if t1_t2:
        print(f"\nT1/T2 companies ready for contact enrichment:")
        for r in t1_t2:
            print(f"  {r['domain']:45} tier={r.get('fit_tier')} score={r.get('fit_score')} → {r.get('segment_slug', '')}")

    # Save results
    out_path = "/home/nadyyym/.openclaw/workspace/plugins/beton-gtm/scripts/pipeline_19_results.json"
    with open(out_path, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"\nResults → {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
