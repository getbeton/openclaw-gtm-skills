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
    with open(os.path.expanduser("~/.openclaw/workspace/plugins/beton-gtm/scripts/run_prefilter.py")) as f:
        content = f.read()
    m = re.search(r'SERVICE_KEY\s*=\s*\(([^)]+)\)', content, re.DOTALL)
    key = "".join(re.findall(r'"([^"]*)"', m.group(1)))
    base = re.search(r'SUPABASE_BASE\s*=\s*"([^"]+)"', content).group(1)
    return base, key

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
    # icp_definition.industry → company_classification.vertical keywords
    "devtools_infra": ["developer tools", "developer", "devtools", "infrastructure", "platform", "api", "sdk", "cloud", "devops", "ci/cd", "monitoring"],
    "saas_b2b": ["saas", "software", "b2b software", "productivity", "workflow"],
    "fintech": ["fintech", "finance", "payments", "banking", "insurtech", "lending"],
    "ecommerce": ["ecommerce", "e-commerce", "retail", "marketplace", "shopify"],
    "healthtech": ["healthtech", "healthcare", "health", "medical", "biotech"],
    "edtech": ["edtech", "education", "learning", "e-learning"],
    "hrtech": ["hrtech", "hr", "human resources", "recruiting", "talent", "people ops"],
    "martech": ["martech", "marketing", "advertising", "seo", "email marketing"],
    "revops": ["revenue operations", "revops", "sales intelligence", "crm", "sales enablement"],
    "analytics": ["analytics", "data", "business intelligence", "bi", "reporting"],
    "security": ["security", "cybersecurity", "compliance", "privacy"],
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
            reasons.append(f"headcount: unknown (segment wants {hmin}-{hmax})")
            # score += 0
        elif hmin <= emp <= hmax:
            score += 20
            reasons.append(f"headcount: ✓ {emp} in [{hmin}-{hmax}]")
        elif emp < hmin * 0.5 or emp > hmax * 2:
            return 0, f"Disqualified: headcount {emp} far outside {hmin}-{hmax}", []
        else:
            score += 8
            reasons.append(f"headcount: partial {emp} vs [{hmin}-{hmax}]")

    # Vertical / industry — unknown = 0
    if "industry" in icp:
        max_score += 20
        industry_key = icp["industry"]
        company_vertical = (cc.get("vertical") or "").lower()
        keywords = VERTICAL_MAP.get(industry_key, [industry_key.lower()])
        if any(kw in company_vertical for kw in keywords):
            score += 20
            reasons.append(f"vertical: ✓ '{company_vertical}' matches {industry_key}")
        elif not company_vertical:
            reasons.append("vertical: unknown")
            # score += 0
        else:
            score += 0
            disqualifiers.append(f"vertical_mismatch")
            reasons.append(f"vertical: ✗ '{company_vertical}' vs {industry_key}")

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
    limit = 100
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
