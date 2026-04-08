#!/usr/bin/env python3
"""
run_value_hypothesis.py — Generate value hypotheses for all scored companies.

Two hypothesis types per company:
  1. data_grounded: segment-level pattern observed across ≥3 companies in the same segment
  2. context_specific: company-specific pain inferred from its own research log

Flow:
  - Group all scored companies by their best segment (highest fit_score)
  - For each segment group, sample up to 40 companies, read their research logs
  - Call Claude to generate 3-5 data_grounded hypotheses for the segment
  - Store hypotheses in `hypotheses` table, link companies via `company_hypotheses`
  - For each company, also generate a context_specific hypothesis from its own log

Requires: hypotheses + company_hypotheses tables to exist (run migration first)

Usage:
    python3 run_value_hypothesis.py [--limit=N] [--dry-run] [--segment=<slug>]
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
import anthropic

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def _load_local_config() -> dict:
    config_path = os.path.join(SCRIPT_DIR, "..", "config.local.json")
    try:
        with open(os.path.abspath(config_path)) as f:
            return json.load(f)
    except Exception:
        return {}

_lc = _load_local_config()
SUPABASE_URL = _lc.get("supabaseUrl", "")
SUPABASE_KEY = _lc.get("supabaseKey", "")
LOGS_DIR = os.path.join(SCRIPT_DIR, "..", "logs", "research")

SUPABASE_H = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal",
}

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def _get_anthropic_client():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        token_path = os.path.expanduser("~/.openclaw/agents/main/agent/auth-profiles.json")
        if os.path.exists(token_path):
            profiles = json.load(open(token_path)).get("profiles", {})
            api_key = profiles.get("anthropic:manual", {}).get("token")
    return anthropic.Anthropic(api_key=api_key)

def extract_json(text: str):
    text = text.strip()
    try:
        return json.loads(text)
    except Exception:
        pass
    m = re.search(r"```(?:json)?\s*(\[.*?\]|\{.*?\})\s*```", text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1))
        except Exception:
            pass
    m = re.search(r"(\[.*\]|\{.*\})", text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1))
        except Exception:
            pass
    return None

def load_research_log(domain: str) -> Optional[dict]:
    safe = domain.replace("/", "_").replace(":", "_")
    path = os.path.join(LOGS_DIR, f"{safe}.json")
    if os.path.exists(path):
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            pass
    return None

# ── Prompts ───────────────────────────────────────────────────────────────────

DATA_GROUNDED_PROMPT = """You are a B2B sales strategist at Beton — an open-source revenue intelligence platform.
Beton helps PLG and hybrid B2B SaaS companies detect product-qualified leads (PQLs) by analyzing product usage signals and routing them to sales reps.

You have analyzed {n} companies in the segment: "{segment_name}"
These are {segment_desc}

Here are summaries of {n} real companies in this segment:

{company_summaries}

Generate exactly 3 DATA-GROUNDED hypotheses about pain that Beton solves for this segment.

CRITICAL: Each hypothesis must cover a COMPLETELY DIFFERENT pain angle. Do not generate variations of the same theme.

The 3 angles must each come from a different category:
1. **Pricing model gap** — about how their pricing model creates blind spots (usage-based, freemium, tiered, transactional)
2. **GTM motion gap** — about the tension between their go-to-market motion and lack of signal routing (PLG without RevOps, founder-led sales scaling, hybrid with no handoff system)
3. **Hiring/org gap** — about the hiring pattern that reveals the gap (hiring AEs without RevOps, CS team without expansion tooling, sales scaling without data infrastructure)

Each hypothesis must:
- Be grounded in patterns seen across ≥3 companies in the data above (cite them)
- Name a specific, observable consequence of the pain (not just "they're missing signals")
- Connect directly to a specific Beton capability
- Be falsifiable — "if this is wrong, we'd observe X instead"

Output JSON array of exactly 3 objects:
[
  {{
    "angle": "pricing_model_gap | gtm_motion_gap | hiring_org_gap",
    "hypothesis_text": "2-3 sentences: the specific observed pattern, the concrete pain it creates (lost deals? wrong accounts contacted? ramp time?), and specifically how Beton addresses it",
    "evidence_base": "Seen across N of {n} companies: [specific pattern with domain examples]",
    "example_companies": ["domain1", "domain2", "domain3"],
    "personalization_hook": "Specific thing to look for on their site that makes the opener personal (e.g. 'Check their /pricing page — if they show usage tiers, reference the specific threshold where a user would become sales-ready')"
  }}
]

Output ONLY the JSON array. No markdown, no explanation."""

CONTEXT_SPECIFIC_PROMPT = """You are a B2B sales strategist at Beton — an open-source revenue intelligence platform.
Beton helps PLG and hybrid B2B SaaS companies detect product-qualified leads by analyzing product usage signals and routing them to sales reps.

Here is what we know about one specific company: {domain}

Company profile:
- Description: {description}
- GTM motion: {gtm_motion}
- Business model: {business_model}
- Vertical: {vertical}
- Sells to: {sells_to}
- Pricing model: {pricing_model}
- Hiring signal: {hiring_signal}
- Open sales roles: {open_sales_roles}
- Open RevOps roles: {open_revops_roles}
- Tech stack signals: {tech_stack}

Relevant website content excerpt:
{content_excerpt}

Generate ONE context-specific hypothesis about the pain this company likely has that Beton solves.

Output JSON:
{{
  "context_specific_text": "2-3 sentences: the specific gap you observed on this company's site/profile, why it creates pain for their sales motion, and how Beton would help specifically",
  "confidence_score": 0-100 (how confident are you this pain is real for them),
  "personalization_hook": "Specific thing to mention in an email opener about THIS company (from their actual content)"
}}

Output ONLY the JSON object."""

# ── Supabase helpers ──────────────────────────────────────────────────────────

async def fetch_b2b_company_ids(http: httpx.AsyncClient) -> set:
    """Fetch all company IDs confirmed as B2B from classification table."""
    b2b_ids = set()
    offset = 0
    page_size = 1000
    while True:
        r = await http.get(
            f"{SUPABASE_URL}/rest/v1/company_classification"
            f"?b2b=eq.true&saas=eq.true&select=company_id"
            f"&limit={page_size}&offset={offset}",
            headers=SUPABASE_H,
            timeout=30,
        )
        page = r.json()
        if not isinstance(page, list) or not page:
            break
        b2b_ids.update(row["company_id"] for row in page)
        if len(page) < page_size:
            break
        offset += page_size
    logger.info(f"Found {len(b2b_ids)} confirmed B2B SaaS companies")
    return b2b_ids

async def fetch_scored_companies_by_segment(http: httpx.AsyncClient, filter_slug: Optional[str] = None) -> dict:
    """Returns {segment_id: [company rows]} grouped by best segment — B2B SaaS only."""
    # Pre-load confirmed B2B SaaS company IDs
    b2b_ids = await fetch_b2b_company_ids(http)

    # Get all company segments, one per company (best match)
    page_size = 1000
    offset = 0
    all_rows = []
    while True:
        r = await http.get(
            f"{SUPABASE_URL}/rest/v1/company_segments"
            f"?select=company_id,segment_id,fit_tier,fit_score"
            f"&order=company_id.asc,fit_score.desc"
            f"&limit={page_size}&offset={offset}",
            headers=SUPABASE_H,
            timeout=30,
        )
        page = r.json()
        if not isinstance(page, list) or not page:
            break
        all_rows.extend(page)
        if len(page) < page_size:
            break
        offset += page_size

    # Keep only best segment per company, B2B SaaS only
    best: dict = {}
    for row in all_rows:
        cid = row["company_id"]
        if cid not in b2b_ids:
            continue  # skip B2C and non-SaaS
        if cid not in best or row["fit_score"] > best[cid]["fit_score"]:
            best[cid] = row

    logger.info(f"After B2B SaaS filter: {len(best)} companies across segments")

    # Group by segment
    grouped: dict = {}
    for cid, row in best.items():
        sid = row["segment_id"]
        grouped.setdefault(sid, []).append(cid)

    logger.info(f"Found {len(grouped)} segments with scored companies")

    # Fetch segment details
    seg_ids = list(grouped.keys())
    segments = {}
    for i in range(0, len(seg_ids), 200):
        chunk = ",".join(seg_ids[i:i+200])
        r = await http.get(
            f"{SUPABASE_URL}/rest/v1/segments?id=in.({chunk})&select=id,slug,name",
            headers=SUPABASE_H,
            timeout=20,
        )
        for s in r.json():
            segments[s["id"]] = s

    # Filter by slug if requested
    if filter_slug:
        grouped = {
            sid: cids for sid, cids in grouped.items()
            if segments.get(sid, {}).get("slug", "").startswith(filter_slug)
        }
        logger.info(f"Filtered to {len(grouped)} segments matching '{filter_slug}'")

    return grouped, segments

async def fetch_company_details(http: httpx.AsyncClient, company_ids: list) -> dict:
    """Returns {company_id: {domain, name, classification, sales_org}}"""
    result = {}
    batch = 100
    for i in range(0, len(company_ids), batch):
        chunk = company_ids[i:i+batch]
        id_filter = "(" + ",".join(chunk) + ")"

        # companies
        r = await http.get(
            f"{SUPABASE_URL}/rest/v1/companies?id=in.{id_filter}&select=id,domain,name",
            headers=SUPABASE_H, timeout=20,
        )
        for c in r.json():
            result[c["id"]] = {"domain": c["domain"], "name": c["name"]}

        # classification
        r = await http.get(
            f"{SUPABASE_URL}/rest/v1/company_classification?company_id=in.{id_filter}&select=company_id,vertical,gtm_motion,description,business_model,sells_to,pricing_model",
            headers=SUPABASE_H, timeout=20,
        )
        for c in r.json():
            if c["company_id"] in result:
                result[c["company_id"]].update(c)

        # sales org
        r = await http.get(
            f"{SUPABASE_URL}/rest/v1/company_sales_org?company_id=in.{id_filter}&select=company_id,hiring_signal,open_sales_roles,open_revops_roles",
            headers=SUPABASE_H, timeout=20,
        )
        for c in r.json():
            if c["company_id"] in result:
                c["hiring_signal_type"] = c.pop("hiring_signal", None)
                result[c["company_id"]].update(c)

    return result

async def upsert_hypothesis(http: httpx.AsyncClient, data: dict) -> Optional[str]:
    """Insert hypothesis row, return its ID."""
    # Map our fields to existing schema: statement = hypothesis_text, type = 'segment'
    payload = {
        "statement": data.get("hypothesis_text", ""),
        "type": "segment",
        "segment_id": data.get("segment_id"),
        "hypothesis_type": data.get("hypothesis_type"),
        "hypothesis_text": data.get("hypothesis_text"),
        "evidence_base": data.get("evidence_base"),
        "example_companies": data.get("example_companies"),
        "personalization_hook": data.get("personalization_hook"),
        "status": "untested",
        "confidence": "medium",
    }
    r = await http.post(
        f"{SUPABASE_URL}/rest/v1/hypotheses",
        headers={**SUPABASE_H, "Prefer": "return=representation"},
        json=payload,
        timeout=15,
    )
    try:
        rows = r.json()
        if isinstance(rows, list) and rows:
            return rows[0].get("id")
        elif isinstance(rows, dict):
            return rows.get("id")
    except Exception:
        pass
    return None

async def upsert_company_hypothesis(http: httpx.AsyncClient, data: dict):
    r = await http.post(
        f"{SUPABASE_URL}/rest/v1/company_hypotheses?on_conflict=company_id,hypothesis_id",
        headers={**SUPABASE_H, "Prefer": "resolution=merge-duplicates,return=minimal"},
        json=data,
        timeout=15,
    )
    r.raise_for_status()

# ── Core processing ───────────────────────────────────────────────────────────

async def process_segment(
    http: httpx.AsyncClient,
    segment_id: str,
    segment: dict,
    company_ids: list,
    dry_run: bool,
    processed_count: list,
    limit: int,
):
    slug = segment.get("slug", "?")
    name = segment.get("name", slug)

    # Sample up to 40 companies
    sample_ids = company_ids[:40]
    details = await fetch_company_details(http, sample_ids)

    # Build company summaries for the data-grounded prompt
    summaries = []
    for cid in sample_ids:
        d = details.get(cid, {})
        domain = d.get("domain", "?")
        log = load_research_log(domain)
        content_excerpt = ""
        if log:
            raw = log.get("raw_content", "")
            content_excerpt = raw[:2000]

        summary = f"""- {domain}: {d.get('description','?')} | GTM: {d.get('gtm_motion','?')} | Pricing: {d.get('pricing_model','?')} | Hiring: {d.get('hiring_signal_type','?')} | Sales roles: {d.get('open_sales_roles','?')} RevOps: {d.get('open_revops_roles','?')}"""
        summaries.append(summary)

    # Parse segment slug for description
    parts = slug.split("--") if "--" in slug else [slug]
    industry = parts[1] if len(parts) > 1 else slug
    stage = parts[0] if len(parts) > 0 else ""
    seg_desc = f"{stage} {industry} companies"

    # Generate data-grounded hypotheses for segment
    logger.info(f"[{slug}] Generating data-grounded hypotheses for {len(sample_ids)} companies...")
    client = _get_anthropic_client()
    dg_prompt = DATA_GROUNDED_PROMPT.format(
        n=len(summaries),
        segment_name=name,
        segment_desc=seg_desc,
        company_summaries="\n".join(summaries),
    )

    dg_hypotheses = []
    try:
        for model in ["claude-haiku-4-5", "claude-sonnet-4-6"]:
            try:
                resp = client.messages.create(
                    model=model,
                    max_tokens=3000,
                    messages=[{"role": "user", "content": dg_prompt}],
                )
                parsed = extract_json(resp.content[0].text)
                if parsed and isinstance(parsed, list):
                    dg_hypotheses = parsed
                    logger.info(f"[{slug}] Got {len(dg_hypotheses)} data-grounded hypotheses")
                    break
            except Exception as e:
                logger.warning(f"[{slug}] {model} failed: {e}")
    except Exception as e:
        logger.error(f"[{slug}] Hypothesis generation failed: {e}")

    if not dg_hypotheses:
        logger.warning(f"[{slug}] No data-grounded hypotheses generated, skipping segment")
        return

    if dry_run:
        for h in dg_hypotheses:
            logger.info(f"[DRY RUN] {slug}: {h.get('hypothesis_text','')[:100]}")
        return

    # Store each data-grounded hypothesis and link all companies in segment
    for h in dg_hypotheses:
        hyp_id = await upsert_hypothesis(http, {
            "segment_id": segment_id,
            "hypothesis_type": "data_grounded",
            "hypothesis_text": h.get("hypothesis_text", ""),
            "evidence_base": h.get("evidence_base"),
            "example_companies": h.get("example_companies", []),
            "personalization_hook": h.get("personalization_hook"),
        })
        if not hyp_id:
            logger.warning(f"[{slug}] Failed to insert hypothesis")
            continue

        # Link ALL companies in this segment (not just the sample)
        for cid in company_ids:
            if processed_count[0] >= limit:
                break
            try:
                await upsert_company_hypothesis(http, {
                    "company_id": cid,
                    "hypothesis_id": hyp_id,
                    "hypothesis_type": "data_grounded",
                    "confidence_score": 70,  # default for data-grounded
                })
                processed_count[0] += 1
            except Exception as e:
                logger.warning(f"company_hypothesis link failed {cid}: {e}")

    logger.info(f"[{slug}] ✓ {len(dg_hypotheses)} hypotheses stored, {len(company_ids)} companies linked")

    # Now generate context-specific hypotheses for sampled companies
    # (these are more expensive — one Claude call per company)
    cs_count = 0
    for cid in sample_ids[:20]:  # limit to top 20 per segment for cost
        d = details.get(cid, {})
        domain = d.get("domain", "?")
        log = load_research_log(domain)
        if not log:
            continue

        content_excerpt = (log.get("raw_content") or "")[:3000]
        tech = d.get("tech_stack") or "{}"
        if isinstance(tech, str):
            try:
                tech = json.loads(tech)
            except Exception:
                tech = {}

        cs_prompt = CONTEXT_SPECIFIC_PROMPT.format(
            domain=domain,
            description=d.get("description", "unknown"),
            gtm_motion=d.get("gtm_motion", "unknown"),
            business_model=d.get("business_model", "unknown"),
            vertical=d.get("vertical", "unknown"),
            sells_to=d.get("sells_to", "unknown"),
            pricing_model=d.get("pricing_model", "unknown"),
            hiring_signal=d.get("hiring_signal_type", "no_data"),
            open_sales_roles=d.get("open_sales_roles", "?"),
            open_revops_roles=d.get("open_revops_roles", "?"),
            tech_stack=json.dumps(tech),
            content_excerpt=content_excerpt,
        )

        try:
            resp = client.messages.create(
                model="claude-haiku-4-5",  # cheaper for per-company pass
                max_tokens=600,
                messages=[{"role": "user", "content": cs_prompt}],
            )
            parsed_cs = extract_json(resp.content[0].text)
            if not parsed_cs:
                continue

            # Store as a company-specific hypothesis (no segment-level row needed)
            # We use a shared hypothesis row per domain to keep it queryable
            hyp_id = await upsert_hypothesis(http, {
                "segment_id": segment_id,
                "hypothesis_type": "context_specific",
                "hypothesis_text": f"[{domain}] {parsed_cs.get('context_specific_text', '')}",
                "personalization_hook": parsed_cs.get("personalization_hook"),
            })
            if hyp_id:
                await upsert_company_hypothesis(http, {
                    "company_id": cid,
                    "hypothesis_id": hyp_id,
                    "hypothesis_type": "context_specific",
                    "confidence_score": parsed_cs.get("confidence_score", 60),
                    "context_specific_text": parsed_cs.get("context_specific_text"),
                })
                cs_count += 1
        except Exception as e:
            logger.warning(f"[{domain}] context-specific failed: {e}")

    logger.info(f"[{slug}] ✓ {cs_count} context-specific hypotheses generated")


async def main():
    limit = 10000
    dry_run = False
    filter_slug = None

    for arg in sys.argv[1:]:
        if arg.startswith("--limit="):
            limit = int(arg.split("=")[1])
        elif arg == "--dry-run":
            dry_run = True
        elif arg.startswith("--segment="):
            filter_slug = arg.split("=")[1]

    logger.info(f"Value hypothesis generation (limit={limit}, dry_run={dry_run}, segment={filter_slug})")

    async with httpx.AsyncClient(timeout=60) as http:
        grouped, segments = await fetch_scored_companies_by_segment(http, filter_slug)

        # Sort segments by company count (biggest opportunities first)
        sorted_segs = sorted(grouped.items(), key=lambda x: len(x[1]), reverse=True)
        logger.info(f"Processing {len(sorted_segs)} segments")

        processed_count = [0]
        for segment_id, company_ids in sorted_segs:
            if processed_count[0] >= limit:
                break
            segment = segments.get(segment_id, {"slug": "unknown", "name": "Unknown"})
            n = len(company_ids)
            logger.info(f"Segment {segment.get('slug')} — {n} companies")
            try:
                await process_segment(
                    http, segment_id, segment, company_ids,
                    dry_run, processed_count, limit
                )
            except Exception as e:
                logger.error(f"Segment {segment_id} failed: {e}")

    logger.info(f"Done — {processed_count[0]} company-hypothesis links created")


if __name__ == "__main__":
    asyncio.run(main())
