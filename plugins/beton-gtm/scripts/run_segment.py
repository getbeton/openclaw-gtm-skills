"""
run_segment.py — GTM Segment matching

Matches classified companies against active ICP segments using rule-based scoring
(no Claude needed for most companies — segments encode all criteria in slug/icp_definition).

Writes to company_segments table (fit_score, fit_tier, fit_reasoning).
Updates companies.research_status → 'scored'.

Usage:
    python3 run_segment.py [--limit=N] [--concurrency=N] [--dry-run]

Defaults:
    --limit=100
    --concurrency=10
    --dry-run=False
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

# ── Config ────────────────────────────────────────────────────────────────────

def _get_supabase_creds():
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config.local.json")
    with open(os.path.abspath(config_path)) as f:
        cfg = json.load(f)
    return cfg["supabaseUrl"], cfg["supabaseKey"]

SUPABASE_BASE, SERVICE_KEY = _get_supabase_creds()
SUPABASE_H = {
    "apikey": SERVICE_KEY,
    "Authorization": f"Bearer {SERVICE_KEY}",
    "Content-Type": "application/json",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Headcount tier mapping ────────────────────────────────────────────────────

HEADCOUNT_TIERS = {
    "nano":   (1, 25),
    "micro":  (26, 100),
    "small":  (101, 500),
    "mid":    (501, 2000),
    "large":  (2001, 10000),
    "enterprise": (10001, 999999),
}

VERTICAL_MAP = {
    # icp_definition.industry slug → keywords in company_classification.vertical
    # Order matters: first match wins. More specific slugs should come before generic ones.
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
    "insurtech":            ["insurtech", "insurance technology", "insurance platform", "insurance software"],
    "payments_banking":     ["payments", "payment gateway", "banking", "remittance", "money transfer", "lending"],
    "healthtech":           ["healthtech", "health technology", "healthcare platform", "health care software",
                             "medical software", "clinical", "telemedicine", "telehealth", "mental health platform",
                             "dental software", "health analytics", "patient management", "ehr ", "emr "],
    "biotech_pharma":       ["biotech", "pharma", "pharmaceutical", "drug discovery", "life sciences",
                             "genomics", "diagnostics"],
    "edtech":               ["edtech", "education technology", "e-learning", "online learning", "learning platform",
                             "lms ", "course platform", "tutoring", "higher education software", "academic software",
                             "student management", "school management"],
    "hrtech":               ["hr tech", "hrtech", "human resources software", "hris", "people ops",
                             "employee management", "performance management", "workforce management",
                             "employee engagement", "payroll platform", "benefits platform"],
    "recruiting_staffing":  ["recruiting", "recruitment platform", "applicant tracking", "ats ", "talent acquisition",
                             "staffing platform", "job platform", "hiring platform"],
    "martech_adtech":       ["martech", "marketing technology", "adtech", "advertising technology",
                             "digital marketing platform", "marketing automation", "email marketing platform",
                             "seo platform", "influencer marketing", "content marketing platform",
                             "social media management", "brand management", "pr platform"],
    "sales_enablement":     ["sales enablement", "sales intelligence", "sales engagement", "revenue intelligence",
                             "revenue operations", "revops", "sales automation", "crm ", "customer relationship",
                             "sales analytics"],
    "ecommerce_retail":     ["ecommerce", "e-commerce", "online retail", "marketplace platform",
                             "shopify", "fashion ecommerce", "apparel", "d2c", "direct to consumer",
                             "luxury retail", "retail platform", "commerce platform"],
    "real_estate":          ["real estate platform", "commercial real estate", "residential real estate",
                             "real estate tech", "property listing", "real estate crm", "luxury real estate"],
    "proptech":             ["proptech", "property technology", "property management software",
                             "real estate software", "smart building"],
    "construction_tech":    ["construction tech", "construction management", "construction software",
                             "building management", "architecture software", "hvac software", "bim "],
    "travel_hospitality":   ["travel platform", "travel technology", "hospitality software", "hotel software",
                             "booking platform", "travel booking", "tourism platform", "restaurant software",
                             "hotel management"],
    "food_bev":             ["food technology", "food delivery platform", "foodtech", "restaurant tech",
                             "grocery tech", "food & beverage software", "agritech", "agtech", "food supply"],
    "logistics_supplychain":["logistics platform", "supply chain software", "fleet management", "shipping platform",
                             "freight technology", "last mile", "warehousing software", "3pl software",
                             "route optimization", "trucking software"],
    "automotive_mobility":  ["automotive software", "fleet platform", "vehicle management", "mobility platform",
                             "ev software", "telematics", "car rental software", "dealership software"],
    "gaming":               ["gaming platform", "game development", "esports", "video game", "game engine",
                             "game analytics", "game monetization"],
    "media_entertainment":  ["media platform", "entertainment platform", "streaming platform", "content platform",
                             "digital media", "podcast platform", "video platform", "news platform",
                             "sports tech", "creator platform"],
    "cleantech_climatetech":["cleantech", "climate tech", "energy platform", "solar software", "renewable energy",
                             "sustainability platform", "carbon", "environmental software", "green tech"],
    "legal_tech":           ["legal tech", "legaltech", "legal platform", "legal software", "law practice",
                             "contract management", "e-discovery", "compliance software", "legal analytics"],
    "telecom_iot":          ["telecommunications software", "telecom platform", "iot platform",
                             "internet of things", "wireless platform", "network management", "5g platform"],
    "agtech_foodtech":      ["agtech", "agricultural technology", "precision agriculture", "farm management",
                             "crop management", "livestock management"],
    "enterprise_saas":      ["saas", "software as a service", "b2b software", "enterprise software",
                             "productivity software", "workflow platform", "project management software",
                             "collaboration software", "no-code platform", "low-code platform",
                             "business software", "information technology"],
    # catch-all — must be last
    "other":                [],
}

# ── Segment matching ──────────────────────────────────────────────────────────

def score_company_vs_segment(company: dict, segment: dict) -> tuple[int, str, list]:
    """
    Rule-based scoring. Returns (fit_score 0-100, reasoning).
    Segments encode all criteria in icp_definition JSONB.
    """
    icp = segment.get("icp_definition") or {}
    reasons = []
    disqualifiers = []
    score = 0
    max_score = 0

    cc = company.get("classification") or {}
    cso = company.get("sales_org") or {}
    ct = company.get("tech_stack") or {}

    # ── Hard filters (any failure = score 0) ─────────────────────────────────
    # Unknowns score 0 — no evidence = no credit

    # B2B check
    # B2B match = full 20pts. B2C match = partial 8pts (b2c companies are lower priority,
    # not excluded — they land in T2 naturally due to lower score ceiling).
    if "customer_type" in icp:
        max_score += 20
        company_b2b = cc.get("b2b")
        seg_b2b = icp["customer_type"] == "b2b"
        if company_b2b is None:
            return 0, "Skipped: b2b unknown", []
        elif company_b2b == seg_b2b:
            # Perfect match
            if seg_b2b:
                score += 20
                reasons.append("b2b: ✓ b2b match")
            else:
                # b2c matching b2c segment — partial credit, deprioritised vs b2b
                score += 8
                reasons.append("b2b: ⚠ b2c match (lower priority)")
                disqualifiers.append("b2c")
        else:
            return 0, f"Disqualified: b2b={company_b2b} vs segment expects b2b={seg_b2b}", []

    # SaaS check
    if "saas" in icp:
        max_score += 15
        company_saas = cc.get("saas")
        seg_saas = icp["saas"]
        if company_saas is None:
            return 0, "Skipped: saas unknown", []
        elif company_saas == seg_saas:
            score += 15
            reasons.append("saas: ✓ match")
        else:
            return 0, f"Disqualified: saas={company_saas} vs segment expects saas={seg_saas}", []

    # ── Soft scoring ──────────────────────────────────────────────────────────

    # Headcount / tier — uses company_firmographics.employees_count when available
    # NOTE: sales_headcount/revops_headcount in company_sales_org are OPEN ROLE COUNTS,
    # not actual headcount — do not use them for headcount range matching.
    # Use company_firmographics.employees_range instead (populated separately).
    if "headcount_range" in icp:
        max_score += 20
        emp = (company.get("firmographics") or {}).get("employees_count")
        hmin = icp["headcount_range"].get("min", 0)
        hmax = icp["headcount_range"].get("max", 999999)
        if emp is None:
            # No data = treat as 0 employees — scores 0 for this dimension, no partial credit
            reasons.append(f"headcount: no data (treating as 0, segment wants {hmin}-{hmax})")
            # score += 0  (intentional — no data = no credit, pulls score down naturally)
        elif hmin <= emp <= hmax:
            score += 20
            reasons.append(f"headcount: ✓ {emp} in [{hmin}-{hmax}]")
        elif emp < hmin * 0.5 or emp > hmax * 2:
            return 0, f"Disqualified: headcount {emp} far outside {hmin}-{hmax}", []
        else:
            score += 8
            reasons.append(f"headcount: partial {emp} vs [{hmin}-{hmax}]")

    # Vertical / industry — mismatch = hard disqualifier (like b2b check)
    # Unknown vertical = skip this dimension (no credit, but not disqualified)
    if "industry" in icp:
        max_score += 20
        industry_key = icp["industry"]
        company_vertical = (cc.get("vertical") or "").lower()
        keywords = VERTICAL_MAP.get(industry_key, [industry_key.lower()])
        if industry_key == "other":
            # "other" segment matches anything — partial credit
            score += 8
            reasons.append("vertical: other segment (partial credit)")
        elif not company_vertical:
            # Unknown vertical — no credit but not disqualified
            reasons.append("vertical: unknown (no credit)")
        elif any(kw in company_vertical for kw in keywords):
            score += 20
            reasons.append(f"vertical: ✓ '{company_vertical}' matches {industry_key}")
        else:
            # Hard disqualifier — wrong industry, skip this segment entirely
            return 0, f"Disqualified: vertical '{company_vertical}' ✗ {industry_key}", ["vertical_mismatch"]

    # RevOps signal — uses open_revops_roles (careers page line count) + hiring_signal
    if icp.get("has_revops"):
        max_score += 15
        open_revops = cso.get("open_revops_roles")  # lines on careers page matching RevOps keywords
        hiring_signal = cso.get("hiring_signal")
        if open_revops and open_revops > 0:
            score += 15
            reasons.append(f"revops: ✓ {open_revops} RevOps role mention(s) on careers page")
        elif hiring_signal:
            score += 8
            reasons.append("revops: hiring but no RevOps roles specifically")
        else:
            reasons.append("revops: no RevOps signal")
            # score += 0

    # DWH/analytics — ALL companies have PostHog (Wappalyzer source), treat as match
    if icp.get("has_dwh"):
        max_score += 10
        dw = ct.get("data_warehouse") or ct.get("analytics")
        if dw:  # posthog is always set, so this always fires
            score += 10
            reasons.append(f"dwh/analytics: ✓ {dw}")
        else:
            reasons.append("dwh: not detected")

    # Normalize to 0-100
    if max_score == 0:
        return 50, "No criteria to score against", []
    normalized = int((score / max_score) * 100)
    return normalized, "; ".join(reasons), disqualifiers


def assign_tier(fit_score: int, disqualifiers: list = None) -> str:
    disqualifiers = disqualifiers or []
    # b2c companies are capped at T2 — lower priority than b2b regardless of score
    if "b2c" in disqualifiers:
        if fit_score >= 50: return "T2"
        if fit_score >= 30: return "T3"
        return "pass"
    if fit_score >= 75 and "vertical_mismatch" not in disqualifiers: return "T1"
    if fit_score >= 50: return "T2"
    if fit_score >= 30: return "T3"
    return "pass"


def find_best_segment(company: dict, segments: list[dict]) -> Optional[dict]:
    """Score company against all segments, return best match (or None if all pass)."""
    best_score = 0
    best_seg = None
    best_reasoning = ""
    best_disqualifiers = []

    for seg in segments:
        score, reasoning, disqualifiers = score_company_vs_segment(company, seg)
        if score > best_score:
            best_score = score
            best_seg = seg
            best_reasoning = reasoning
            best_disqualifiers = disqualifiers

    if best_score == 0:
        return None

    tier = assign_tier(best_score, best_disqualifiers)
    return {
        "segment_id": best_seg["id"],
        "segment_slug": best_seg["slug"],
        "segment_name": best_seg["name"],
        "fit_score": best_score,
        "fit_tier": tier,
        "fit_reasoning": best_reasoning,
    }


# ── Supabase helpers ──────────────────────────────────────────────────────────

async def fetch_segments(client: httpx.AsyncClient) -> list[dict]:
    segments = []
    offset = 0
    while True:
        r = await client.get(
            f"{SUPABASE_BASE}/rest/v1/segments?is_active=eq.true"
            f"&select=id,name,slug,icp_definition&limit=1000&offset={offset}",
            headers=SUPABASE_H, timeout=20,
        )
        r.raise_for_status()
        page = r.json()
        if not page:
            break
        segments.extend(page)
        if len(page) < 1000:
            break
        offset += 1000
    return segments


async def fetch_companies(client: httpx.AsyncClient, limit: int, rescore: bool = False, domain_offset: str = "") -> list[dict]:
    """
    Fetch classified companies with enrichment data.
    Entry point: companies WHERE research_status='classified' with real classification data.
    Skips companies where both b2b and saas are null (failed/empty classification).

    --rescore: fetch already-scored companies and re-run segment matching with fresh sales org data.
    """
    companies = []
    offset = 0
    PAGE_SIZE = 500  # larger pages

    status_filter = "research_status=in.(classified,scored)" if rescore else "research_status=eq.classified"
    domain_filter = f"&domain=gte.{domain_offset}" if domain_offset else ""

    while len(companies) < limit:
        # 1. Fetch a page of companies
        r = await client.get(
            f"{SUPABASE_BASE}/rest/v1/companies"
            f"?{status_filter}{domain_filter}&select=id,domain,name"
            f"&limit={PAGE_SIZE}&offset={offset}&order=domain.asc",
            headers=SUPABASE_H, timeout=30,
        )
        r.raise_for_status()
        page = r.json()
        if not page:
            break

        ids = [c["id"] for c in page]
        ids_str = "(" + ",".join(ids) + ")"

        # 2. Batch-fetch classification, sales_org, tech_stack in 3 parallel requests
        r_cls, r_so, r_ct = await asyncio.gather(
            client.get(
                f"{SUPABASE_BASE}/rest/v1/company_classification"
                f"?company_id=in.{ids_str}"
                f"&select=company_id,b2b,saas,gtm_motion,vertical,business_model,sells_to"
                f"&limit={PAGE_SIZE}",
                headers=SUPABASE_H, timeout=30,
            ),
            client.get(
                f"{SUPABASE_BASE}/rest/v1/company_sales_org"
                f"?company_id=in.{ids_str}"
                f"&select=company_id,open_sales_roles,open_revops_roles,open_cs_roles,hiring_signal"
                f"&limit={PAGE_SIZE}",
                headers=SUPABASE_H, timeout=30,
            ),
            client.get(
                f"{SUPABASE_BASE}/rest/v1/company_tech_stack"
                f"?company_id=in.{ids_str}"
                f"&select=company_id,crm,sales_engagement_tool,data_warehouse,analytics"
                f"&limit={PAGE_SIZE}",
                headers=SUPABASE_H, timeout=30,
            ),
        )

        cls_map  = {r["company_id"]: r for r in r_cls.json()}
        so_map   = {r["company_id"]: r for r in r_so.json()}
        ct_map   = {r["company_id"]: r for r in r_ct.json()}

        for company in page:
            cid = company["id"]
            classification = cls_map.get(cid, {})

            # Skip companies with no real classification data
            if classification.get("b2b") is None and classification.get("saas") is None:
                continue

            sales_org  = so_map.get(cid, {})
            tech_stack = ct_map.get(cid, {"analytics": "posthog"})
            if not tech_stack.get("analytics"):
                tech_stack["analytics"] = "posthog"

            companies.append({
                **company,
                "classification": classification,
                "sales_org": sales_org,
                "tech_stack": tech_stack,
            })

            if len(companies) >= limit:
                break

        if len(page) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    return companies[:limit]


async def save_segment_match(client: httpx.AsyncClient, company_id: str, match: dict, dry_run: bool):
    now = datetime.now(timezone.utc).isoformat()

    if dry_run:
        return

    # Upsert company_segments
    r = await client.post(
        f"{SUPABASE_BASE}/rest/v1/company_segments?on_conflict=company_id,segment_id",
        headers={**SUPABASE_H, "Prefer": "resolution=merge-duplicates,return=minimal"},
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

    # Mark as scored if T1/T2
    if match["fit_tier"] in ("T1", "T2"):
        await client.patch(
            f"{SUPABASE_BASE}/rest/v1/companies?id=eq.{company_id}",
            headers={**SUPABASE_H, "Prefer": "return=minimal"},
            json={"research_status": "scored"},
            timeout=10,
        )


# ── Main processor ────────────────────────────────────────────────────────────

async def process_company(
    company: dict,
    segments: list[dict],
    semaphore: asyncio.Semaphore,
    client: httpx.AsyncClient,
    results: list,
    dry_run: bool,
):
    domain = company["domain"]
    company_id = company["id"]

    async with semaphore:
        match = find_best_segment(company, segments)
        if not match:
            result = {"domain": domain, "status": "no_match", "fit_tier": "pass", "fit_score": 0}
            logger.info(f"[{domain}] No segment match → pass")
        else:
            result = {"domain": domain, "status": "matched", **match}
            logger.info(
                f"[{domain}] {match['fit_tier']} (score={match['fit_score']}) "
                f"→ {match['segment_slug']}"
            )

        results.append(result)

        if not dry_run and match:
            try:
                await save_segment_match(client, company_id, match, dry_run)
            except Exception as e:
                logger.error(f"[{domain}] Supabase error: {e}")
                result["status"] = "error"


# ── Entry point ───────────────────────────────────────────────────────────────

async def main():
    limit = 50000
    concurrency = 10
    dry_run = False
    rescore = False

    for arg in sys.argv[1:]:
        if arg.startswith("--limit="):
            limit = int(arg.split("=")[1])
        elif arg.startswith("--concurrency="):
            concurrency = int(arg.split("=")[1])
        elif arg == "--dry-run":
            dry_run = True
        elif arg == "--rescore":
            rescore = True

    domain_offset = ""
    for arg in sys.argv[1:]:
        if arg.startswith("--domain-offset="):
            domain_offset = arg.split("=")[1]

    logger.info(f"Starting segment run (limit={limit}, concurrency={concurrency}, dry_run={dry_run}, rescore={rescore}, domain_offset={domain_offset or 'none'})")

    async with httpx.AsyncClient(follow_redirects=True) as http:
        logger.info("Loading segments...")
        segments = await fetch_segments(http)
        logger.info(f"Loaded {len(segments)} active segments")

        logger.info("Fetching companies with sales org data...")
        companies = await fetch_companies(http, limit, rescore=rescore, domain_offset=domain_offset)
        logger.info(f"Fetched {len(companies)} companies to score")

        if not companies:
            logger.info("Nothing to process.")
            return

        semaphore = asyncio.Semaphore(concurrency)
        results = []

        tasks = [
            process_company(c, segments, semaphore, http, results, dry_run)
            for c in companies
        ]
        await asyncio.gather(*tasks)

    # Summary
    by_tier = {}
    for r in results:
        t = r.get("fit_tier", "pass")
        by_tier[t] = by_tier.get(t, 0) + 1

    print(f"\n{'='*50}")
    print(f"SEGMENT RUN — COMPLETE")
    print(f"{'='*50}")
    print(f"Total processed : {len(results)}")
    for tier in ["T1", "T2", "T3", "pass", "no_match"]:
        if tier in by_tier:
            print(f"  {tier:10}: {by_tier[tier]}")
    print()

    # Show T1s
    t1s = [r for r in results if r.get("fit_tier") == "T1"]
    if t1s:
        print(f"T1 companies ({len(t1s)}):")
        for r in t1s:
            print(f"  {r['domain']:35} score={r['fit_score']} → {r.get('segment_slug','')[:50]}")

    out_path = os.path.join(os.path.dirname(__file__), "segment_results.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults → {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
