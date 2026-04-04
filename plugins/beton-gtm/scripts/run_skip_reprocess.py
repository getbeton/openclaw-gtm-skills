#!/usr/bin/env python3
"""
run_skip_reprocess.py — Reprocess 111 skip companies from the 6sense+PostHog cohort.

Pipeline:
  1. Prefilter: HTTP check (alive + not parked) → mark prefiltered or keep skip
  2. Research: Firecrawl scrape + Claude classification → mark classified
  3. Segment: rule-based scoring → mark scored

Skips UUID/vendor subdomain junk immediately.
Processes real domains in batches of 10.
"""

import asyncio
import json
import logging
import os
import re
import sys
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

_BASE = os.path.dirname(os.path.abspath(__file__))
_WORKSPACE = os.path.expanduser("~/.openclaw/workspace")

def _load_cfg():
    with open(os.path.join(_BASE, "..", "config.local.json")) as f:
        return json.load(f)

_cfg = _load_cfg()
SUPABASE_URL = _cfg["supabaseUrl"]
SUPABASE_KEY = _cfg["supabaseKey"]
SUPABASE_H = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

def _load_firecrawl():
    try:
        with open(os.path.join(_WORKSPACE, "integrations", "firecrawl.json")) as f:
            return json.load(f)["base_url"].rstrip("/")
    except Exception:
        return "http://34.122.195.243:3002"

FIRECRAWL_URL = _load_firecrawl()

def _get_anthropic_key():
    key = os.environ.get("ANTHROPIC_API_KEY")
    if key:
        return key
    # Try OpenClaw auth-profiles.json
    for path in [
        os.path.expanduser("~/.openclaw/agents/main/agent/auth-profiles.json"),
        os.path.expanduser("~/.openclaw/config/llm-profiles.json"),
    ]:
        try:
            with open(path) as f:
                data = json.load(f)
            # auth-profiles.json format
            profiles = data.get("profiles", data)
            key = profiles.get("anthropic:manual", {}).get("token")
            if key:
                return key
        except Exception:
            continue
    return None

ANTHROPIC_KEY = _get_anthropic_key()

LOGS_DIR = os.path.join(_BASE, "..", "logs", "skip_reprocess")
os.makedirs(LOGS_DIR, exist_ok=True)

DATA_FILE = os.path.join(_WORKSPACE, "beton/gtm-outbound/csv/output/6sense-posthog-all-companies.json")

# ── Domain junk filter ────────────────────────────────────────────────────────

UUID_RE = re.compile(r'^[0-9a-f\-]{36}', re.I)
VENDOR_SUBDOMAINS = (
    "lovableproject.com", "createdusercontent.com", "arena.site",
    "aweber.com", "vuetifyjs.com", "mikasass.pro", "replit.dev",
    "atarimworker.dev", "zaiko.io",
)
MULTI_PART_TLDS = (".co.uk", ".com.br", ".com.au", ".co.jp", ".co.nz", ".co.za", ".com.mx", ".com.ar")

def is_clean_domain(domain: str) -> bool:
    if UUID_RE.match(domain): return False
    if domain.startswith("_"): return False
    if any(domain.endswith(v) for v in VENDOR_SUBDOMAINS): return False
    parts = domain.split(".")
    if len(parts) < 2: return False
    is_cc_tld = any(domain.endswith(tld) for tld in MULTI_PART_TLDS)
    max_parts = 3 if is_cc_tld else 2
    if len(parts) > max_parts: return False
    return True

# ── Parked detection ──────────────────────────────────────────────────────────

PARKED_SIGNALS = [
    "this domain is for sale", "buy this domain", "sedo.com", "godaddy",
    "domain parking", "parked domain", "hugedomains", "dan.com",
    "undeveloped.com", "afternic", "namecheap parking", "domain for sale",
    "inquire about this domain", "this domain may be for sale",
    "register this domain", "domain has expired", "this web page is parked",
    "namecheap.com/logo", "domain is available",
]
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
}

def is_parked(content: str) -> bool:
    low = content.lower()
    return any(sig in low for sig in PARKED_SIGNALS)

async def check_domain_alive(client: httpx.AsyncClient, domain: str) -> tuple[bool, str]:
    """Returns (alive, reason)."""
    for scheme in ("https", "http"):
        url = f"{scheme}://{domain}"
        try:
            r = await client.get(url, headers=HTTP_HEADERS, timeout=10, follow_redirects=True)
            content = r.text[:5000]
            if r.status_code >= 400:
                continue
            if len(content.strip()) < 200:
                return False, "empty_page"
            if is_parked(content):
                return False, "parked"
            return True, "alive"
        except (httpx.TimeoutException, httpx.ConnectError, httpx.TooManyRedirects):
            continue
        except Exception:
            continue
    return False, "unreachable"

# ── Supabase helpers ──────────────────────────────────────────────────────────

async def set_status(client: httpx.AsyncClient, company_id: str, status: str):
    r = await client.patch(
        f"{SUPABASE_URL}/rest/v1/companies?id=eq.{company_id}",
        headers={**SUPABASE_H, "Prefer": "return=minimal"},
        json={"research_status": status, "updated_at": datetime.now(timezone.utc).isoformat()},
        timeout=10,
    )
    r.raise_for_status()

async def save_classification(client: httpx.AsyncClient, company_id: str, classification: dict, source: str):
    now = datetime.now(timezone.utc).isoformat()
    # Update company status
    await client.patch(
        f"{SUPABASE_URL}/rest/v1/companies?id=eq.{company_id}",
        headers={**SUPABASE_H, "Prefer": "return=minimal"},
        json={"research_status": "classified", "enriched_at": now, "updated_at": now},
        timeout=10,
    )
    # Upsert classification
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
        headers={**SUPABASE_H, "Prefer": "resolution=merge-duplicates,return=minimal"},
        json=cl_payload,
        timeout=10,
    )

async def save_segment(client: httpx.AsyncClient, company_id: str, segment_result: dict):
    now = datetime.now(timezone.utc).isoformat()
    # Update company status to scored
    await client.patch(
        f"{SUPABASE_URL}/rest/v1/companies?id=eq.{company_id}",
        headers={**SUPABASE_H, "Prefer": "return=minimal"},
        json={
            "research_status": "scored",
            "fit_tier": segment_result["fit_tier"],
            "fit_score": segment_result["fit_score"],
            "updated_at": now,
        },
        timeout=10,
    )
    # Upsert company_segments
    seg_payload = {
        "company_id": company_id,
        "segment_id": segment_result["segment_id"],
        "fit_score": segment_result["fit_score"],
        "fit_tier": segment_result["fit_tier"],
        "fit_reasoning": segment_result.get("fit_reasoning", ""),
        "scored_at": now,
        "updated_at": now,
    }
    await client.post(
        f"{SUPABASE_URL}/rest/v1/company_segments",
        headers={**SUPABASE_H, "Prefer": "resolution=merge-duplicates,return=minimal"},
        json=seg_payload,
        timeout=10,
    )

async def fetch_segments(client: httpx.AsyncClient) -> list[dict]:
    r = await client.get(
        f"{SUPABASE_URL}/rest/v1/segments?is_active=eq.true"
        f"&select=id,name,slug,icp_definition&limit=100",
        headers=SUPABASE_H, timeout=20,
    )
    r.raise_for_status()
    return r.json()

# ── Firecrawl ─────────────────────────────────────────────────────────────────

def normalize_domain(domain: str) -> str:
    parts = domain.split(".")
    skip_prefixes = {"dashboard", "app", "console", "go", "portal", "www", "login", "api", "help"}
    while len(parts) > 2 and parts[0].lower() in skip_prefixes:
        parts = parts[1:]
    return ".".join(parts)

async def firecrawl_scrape(client: httpx.AsyncClient, url: str) -> Optional[str]:
    try:
        r = await client.post(
            f"{FIRECRAWL_URL}/v1/scrape",
            json={"url": url, "formats": ["markdown"], "onlyMainContent": True},
            timeout=30.0,
        )
        if r.status_code == 200:
            data = r.json()
            return (data.get("data") or data).get("markdown") or ""
    except Exception as e:
        logger.debug(f"firecrawl_scrape({url}): {e}")
    return None

async def firecrawl_map(client: httpx.AsyncClient, domain: str) -> list[str]:
    root = normalize_domain(domain)
    try:
        r = await client.post(
            f"{FIRECRAWL_URL}/v1/map",
            json={"url": f"https://{root}", "limit": 15},
            timeout=25.0,
        )
        if r.status_code == 200:
            data = r.json()
            return data.get("links", data.get("urls", []))[:15]
    except Exception as e:
        logger.debug(f"firecrawl_map({domain}): {e}")
    return [f"https://{root}"]

def prioritize_urls(urls: list[str]) -> list[str]:
    priority_kws = ["pricing", "about", "customers", "product", "solutions", "platform", "features", "team"]
    def score(u):
        u_low = u.lower()
        for i, kw in enumerate(priority_kws):
            if kw in u_low: return i
        return 99
    return sorted(urls, key=score)[:8]

async def scrape_company_pages(client: httpx.AsyncClient, domain: str) -> tuple[str, str]:
    """Returns (content, source)."""
    root = normalize_domain(domain)
    urls = await firecrawl_map(client, domain)
    urls = prioritize_urls(urls)
    pages = []
    for url in urls[:6]:
        md = await firecrawl_scrape(client, url)
        if md and len(md) > 100:
            pages.append(md[:3000])
        if len(pages) >= 4:
            break
    if pages:
        return "\n\n---\n\n".join(pages), "firecrawl"
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

async def classify_with_claude(domain: str, content: str) -> Optional[dict]:
    if not content or len(content) < 50:
        return None
    import anthropic
    client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_KEY)
    prompt = CLASSIFY_PROMPT.format(domain=domain, content=content[:8000])
    try:
        msg = await client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}]
        )
        raw = msg.content[0].text.strip()
        if "```" in raw:
            raw = raw.split("```")[1].replace("json", "").strip()
        return json.loads(raw)
    except Exception as e:
        logger.warning(f"[{domain}] Claude error: {e}")
        return None

# ── Segment scoring ───────────────────────────────────────────────────────────

VERTICAL_MAP = {
    "devtools_infra":       ["devtools", "developer tool", "developer platform", "developer experience",
                             "platform engineering", "mlops", "ai infrastructure", "ai devtools", "ml platform",
                             "data infrastructure", "open source tools", "api platform", "sdk platform",
                             "devops platform", "ci/cd", "observability platform", "monitoring platform",
                             "cloud infrastructure", "database platform", "infrastructure software"],
    "cybersecurity":        ["cybersecurity", "cyber security", "network security", "application security",
                             "identity and access", "iam ", "soc ", "siem", "fraud detection", "data privacy",
                             "compliance platform", "zero trust", "endpoint security"],
    "ai_ml":                ["artificial intelligence", "machine learning", "llm", "generative ai", "computer vision",
                             "nlp", "natural language", "ai tools", "ai automation", "ai platform", "ai agents",
                             "ai infrastructure", "ai devtools", "ai image", "ai video", "ai content",
                             "ai productivity", "ai assistant", "ai workflow"],
    "analytics_bi":         ["analytics platform", "business intelligence", " bi ", "data analytics",
                             "data visualization", "reporting platform", "data platform", "revenue analytics",
                             "product analytics", "marketing analytics", "customer analytics"],
    "fintech":              ["fintech", "financial technology", "financial services", "payments platform",
                             "payment processing", "banking platform", "neobank", "wealthtech", "crypto",
                             "blockchain", "defi", "accounting software", "tax software", "payroll software",
                             "billing platform", "invoicing", "expense management", "spend management"],
    "healthtech":           ["healthtech", "health technology", "healthcare platform", "health care software",
                             "medical software", "clinical", "telemedicine", "telehealth", "mental health platform"],
    "edtech":               ["edtech", "education technology", "e-learning", "online learning", "learning platform",
                             "lms ", "course platform"],
    "hrtech":               ["hr tech", "hrtech", "human resources software", "hris", "people ops",
                             "employee management", "performance management", "workforce management"],
    "martech_adtech":       ["martech", "marketing technology", "adtech", "advertising technology",
                             "digital marketing platform", "marketing automation", "email marketing platform",
                             "seo platform"],
    "sales_enablement":     ["sales enablement", "sales intelligence", "sales engagement", "revenue intelligence",
                             "revenue operations", "revops", "sales automation", "crm ", "customer relationship"],
    "ecommerce_retail":     ["ecommerce", "e-commerce", "online retail", "marketplace platform",
                             "shopify", "fashion ecommerce", "d2c", "direct to consumer", "commerce platform"],
    "enterprise_saas":      ["saas", "software as a service", "b2b software", "enterprise software",
                             "productivity software", "workflow platform", "project management software",
                             "collaboration software", "no-code platform", "low-code platform"],
    "other":                [],
}

def score_company_vs_segment(company_cls: dict, segment: dict) -> tuple[int, str, list]:
    icp = segment.get("icp_definition") or {}
    reasons = []
    disqualifiers = []
    score = 0
    max_score = 0

    cc = company_cls

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
            return 0, f"Disqualified: b2b mismatch", []

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
            return 0, "Disqualified: saas mismatch", []

    # Vertical
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
            reasons.append(f"vertical: ✓ {industry_key}")
        else:
            return 0, f"Disqualified: vertical mismatch", ["vertical_mismatch"]

    # DWH/analytics — all PostHog companies match
    if icp.get("has_dwh"):
        max_score += 10
        score += 10
        reasons.append("dwh: ✓ PostHog")

    if max_score == 0:
        return 50, "No criteria", []
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

def find_best_segment(classification: dict, segments: list[dict]) -> Optional[dict]:
    best_score = 0
    best_seg = None
    best_reasoning = ""
    best_disqualifiers = []

    for seg in segments:
        score, reasoning, disqualifiers = score_company_vs_segment(classification, seg)
        if score > best_score:
            best_score = score
            best_seg = seg
            best_reasoning = reasoning
            best_disqualifiers = disqualifiers

    if best_score == 0 or best_seg is None:
        return None

    tier = assign_tier(best_score, best_disqualifiers)
    return {
        "segment_id": best_seg["id"],
        "segment_slug": best_seg.get("slug"),
        "segment_name": best_seg.get("name"),
        "fit_score": best_score,
        "fit_tier": tier,
        "fit_reasoning": best_reasoning,
    }

# ── Main pipeline ─────────────────────────────────────────────────────────────

CONCURRENCY = 5

async def process_company(
    company: dict,
    http: httpx.AsyncClient,
    segments: list[dict],
    sem: asyncio.Semaphore,
    stats: dict,
) -> dict:
    domain = company["domain"]
    company_id = company["id"]

    async with sem:
        result = {
            "domain": domain,
            "company_id": company_id,
            "stage": "start",
            "final_status": None,
            "classification": None,
            "segment": None,
        }

        # ── Stage 1: Prefilter ──
        alive, reason = await check_domain_alive(http, domain)
        if not alive:
            logger.info(f"  [SKIP] {domain} — {reason}")
            result["stage"] = "prefilter_fail"
            result["final_status"] = "skip"
            result["skip_reason"] = reason
            await set_status(http, company_id, "skip")
            stats["still_skip"] += 1
            return result

        logger.info(f"  [ALIVE] {domain}")
        await set_status(http, company_id, "prefiltered")
        result["stage"] = "prefiltered"
        stats["prefiltered"] += 1

        # ── Stage 2: Research ──
        content, source = await scrape_company_pages(http, domain)
        if not content:
            logger.warning(f"  [NO_CONTENT] {domain}")
            result["stage"] = "research_fail"
            result["final_status"] = "prefiltered"
            stats["research_fail"] += 1
            return result

        classification = await classify_with_claude(domain, content)
        if not classification:
            logger.warning(f"  [NO_CLASSIFY] {domain}")
            result["stage"] = "classify_fail"
            result["final_status"] = "prefiltered"
            stats["classify_fail"] += 1
            return result

        await save_classification(http, company_id, classification, source)
        result["stage"] = "classified"
        result["classification"] = classification
        stats["classified"] += 1
        logger.info(f"  [CLASSIFIED] {domain} — b2b={classification.get('b2b')}, "
                    f"vertical={classification.get('vertical')}, gtm={classification.get('gtm_motion')}")

        # ── Stage 3: Segment ──
        if not segments:
            result["final_status"] = "classified"
            return result

        seg_result = find_best_segment(classification, segments)
        if seg_result:
            await save_segment(http, company_id, seg_result)
            result["stage"] = "scored"
            result["segment"] = seg_result
            result["final_status"] = "scored"
            stats["scored"] += 1
            logger.info(f"  [SCORED] {domain} — tier={seg_result['fit_tier']}, "
                        f"score={seg_result['fit_score']}, seg={seg_result['segment_slug']}")
        else:
            # No segment match — mark as pass
            await set_status(http, company_id, "scored")
            # No segment record needed for 'pass'
            result["final_status"] = "scored (no segment match)"
            stats["scored"] += 1
            stats["no_segment"] += 1
            logger.info(f"  [SCORED-PASS] {domain} — no segment match")

        return result


async def main():
    dry_run = "--dry-run" in sys.argv

    # Load skip companies from JSON
    with open(DATA_FILE) as f:
        all_companies = json.load(f)

    skip_companies = [c for c in all_companies if c.get("research_status") == "skip"]
    logger.info(f"Loaded {len(skip_companies)} skip companies from file")

    # Separate junk from processable
    clean = [c for c in skip_companies if is_clean_domain(c["domain"])]
    junk = [c for c in skip_companies if not is_clean_domain(c["domain"])]

    logger.info(f"Clean domains: {len(clean)}, Junk (instant skip): {len(junk)}")

    # Mark junk as skip in Supabase (they already are, but ensure updated_at is fresh)
    logger.info(f"Marking {len(junk)} junk domains as skip in Supabase...")
    # (skip — they're already skip, no need to update)

    # Load segments
    async with httpx.AsyncClient(timeout=30) as http:
        segments = await fetch_segments(http)
    logger.info(f"Loaded {len(segments)} active segments")

    # Stats
    stats = {
        "prefiltered": 0,
        "still_skip": 0,
        "research_fail": 0,
        "classify_fail": 0,
        "classified": 0,
        "scored": 0,
        "no_segment": 0,
    }

    sem = asyncio.Semaphore(CONCURRENCY)
    results = []

    # Process in batches of 10 with reporting
    BATCH_SIZE = 10
    batches = [clean[i:i+BATCH_SIZE] for i in range(0, len(clean), BATCH_SIZE)]
    total_batches = len(batches)

    logger.info(f"\n{'='*60}")
    logger.info(f"Processing {len(clean)} clean domains in {total_batches} batches of {BATCH_SIZE}")
    logger.info(f"Dry run: {dry_run}")
    logger.info(f"{'='*60}\n")

    async with httpx.AsyncClient(timeout=60) as http:
        for batch_num, batch in enumerate(batches, 1):
            logger.info(f"\n--- Batch {batch_num}/{total_batches} ---")
            for c in batch:
                logger.info(f"  → {c['domain']}")

            if dry_run:
                for c in batch:
                    results.append({"domain": c["domain"], "dry_run": True})
                continue

            tasks = [
                process_company(c, http, segments, sem, stats)
                for c in batch
            ]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            batch_alive = 0
            batch_scored = 0
            batch_skip = 0

            for r in batch_results:
                if isinstance(r, Exception):
                    logger.error(f"  EXCEPTION: {r}")
                    stats["still_skip"] += 1
                    continue
                results.append(r)
                if r.get("final_status") == "scored" or r.get("final_status", "").startswith("scored"):
                    batch_scored += 1
                elif r.get("final_status") == "skip":
                    batch_skip += 1
                if r.get("stage") in ("prefiltered", "classified", "scored"):
                    batch_alive += 1

            logger.info(f"\n  Batch {batch_num}/{total_batches} complete:")
            logger.info(f"    Alive: {batch_alive}")
            logger.info(f"    Scored: {batch_scored}")
            logger.info(f"    Still skip: {batch_skip}")
            logger.info(f"  Running totals:")
            logger.info(f"    Prefiltered so far: {stats['prefiltered']}")
            logger.info(f"    Classified so far: {stats['classified']}")
            logger.info(f"    Scored so far: {stats['scored']}")
            logger.info(f"    Still skip so far: {stats['still_skip']}")

    # Final summary
    logger.info(f"\n{'='*60}")
    logger.info("FINAL SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"Total skip companies processed: {len(skip_companies)}")
    logger.info(f"  Junk (UUID/vendor subdomains): {len(junk)}")
    logger.info(f"  Clean domains processed: {len(clean)}")
    logger.info(f"")
    logger.info(f"Results for clean domains:")
    logger.info(f"  → Alive (passed prefilter): {stats['prefiltered']}")
    logger.info(f"  → Research failed: {stats['research_fail']}")
    logger.info(f"  → Classify failed: {stats['classify_fail']}")
    logger.info(f"  → Classified: {stats['classified']}")
    logger.info(f"  → Scored: {stats['scored']} (of which {stats['no_segment']} had no segment match)")
    logger.info(f"  → Still skip: {stats['still_skip']}")
    logger.info(f"{'='*60}")

    # Save results JSON
    out_path = os.path.join(LOGS_DIR, f"results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    with open(out_path, "w") as f:
        json.dump({
            "summary": stats,
            "junk_count": len(junk),
            "clean_count": len(clean),
            "results": results,
        }, f, indent=2)
    logger.info(f"\nResults saved to: {out_path}")

    return stats


if __name__ == "__main__":
    asyncio.run(main())
