"""
run_enrich_vertical.py — Fill null vertical fields using Claude Sonnet 4.6

For classified companies where vertical=null but description is populated,
infers vertical from description and updates company_classification.

Usage:
    python3 run_enrich_vertical.py [--limit=N] [--concurrency=N] [--dry-run]

Defaults: --limit=1000, --concurrency=10
"""

import asyncio
import json
import logging
import os
import re
import sys
from typing import Optional

import anthropic
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
    "Prefer": "return=minimal",
}

def _get_anthropic_client():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        try:
            with open(os.path.expanduser("~/.openclaw/agents/main/agent/auth-profiles.json")) as f:
                data = json.load(f)
            profiles = data.get("profiles", data)  # handle both flat and nested
            api_key = profiles.get("anthropic:manual", {}).get("token")
        except Exception:
            pass
    return anthropic.Anthropic(api_key=api_key)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

VERTICAL_PROMPT = (
    'You are classifying a company\'s industry vertical.\n\n'
    'Company description: {description}\n'
    'Domain: {domain}\n\n'
    'Output ONLY a JSON object with a single key "vertical" containing a short label (2-5 words).\n\n'
    'Examples of good verticals:\n'
    '- "developer tools"\n'
    '- "luxury goods"\n'
    '- "consumer beauty"\n'
    '- "e-commerce"\n'
    '- "HR tech"\n'
    '- "cybersecurity"\n'
    '- "real estate"\n'
    '- "fitness"\n\n'
    'Output {{"vertical": null}} ONLY for parked domains, suspended accounts, or pages with zero usable content.\n'
    'Every commercial business — B2B or B2C — has a vertical. Assign one.\n\n'
    'Output ONLY valid JSON, no explanation.'
)


def infer_vertical(client: anthropic.Anthropic, description: str, domain: str) -> Optional[str]:
    try:
        prompt = VERTICAL_PROMPT.format(description=description[:500], domain=domain)
        resp = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=64,
            messages=[{"role": "user", "content": prompt}],
        )
        text = resp.content[0].text.strip()
        # Parse JSON — handle various Claude response formats
        try:
            data = json.loads(text)
            return data.get("vertical")
        except Exception:
            pass
        # Try extracting JSON object
        m = re.search(r'\{[^}]+\}', text, re.DOTALL)
        if m:
            try:
                data = json.loads(m.group(0))
                return data.get("vertical")
            except Exception:
                pass
        # Try extracting quoted string value directly
        m2 = re.search(r'"vertical"\s*:\s*"([^"]+)"', text)
        if m2:
            return m2.group(1)
        # If Claude returned a plain string (rare), use it if short
        if len(text) < 60 and '"' not in text and '{' not in text and text.lower() != 'null':
            return text
        return None
    except Exception as e:
        logger.warning(f"[{domain}] Claude error: {e}")
        return None


async def fetch_batch(client: httpx.AsyncClient, limit: int, offset: int) -> list[dict]:
    """Fetch classified companies with null vertical but non-null description."""
    r = await client.get(
        f"{SUPABASE_BASE}/rest/v1/company_classification"
        f"?vertical=is.null&description=not.is.null"
        f"&select=company_id,description"
        f"&limit={limit}&offset={offset}",
        headers=SUPABASE_H, timeout=20,
    )
    r.raise_for_status()
    rows = r.json()

    # Get domains for each
    result = []
    for row in rows:
        r2 = await client.get(
            f"{SUPABASE_BASE}/rest/v1/companies?id=eq.{row['company_id']}&select=domain",
            headers=SUPABASE_H, timeout=10,
        )
        company = r2.json()
        domain = company[0]["domain"] if company else row["company_id"]
        result.append({"company_id": row["company_id"], "domain": domain, "description": row["description"]})
    return result


async def process_company(
    row: dict,
    anthropic_client: anthropic.Anthropic,
    http: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    results: list,
    dry_run: bool,
):
    async with semaphore:
        domain = row["domain"]
        cid = row["company_id"]
        description = row["description"] or ""

        if not description.strip():
            return

        # Run Claude inference in thread pool (sync SDK)
        loop = asyncio.get_event_loop()
        vertical = await loop.run_in_executor(
            None, infer_vertical, anthropic_client, description, domain
        )

        if not vertical:
            logger.info(f"[{domain}] → null (non-commercial or vague)")
            results.append({"domain": domain, "vertical": None, "status": "null"})
            return

        logger.info(f"[{domain}] → '{vertical}'")
        results.append({"domain": domain, "vertical": vertical, "status": "ok"})

        if not dry_run:
            try:
                r = await http.patch(
                    f"{SUPABASE_BASE}/rest/v1/company_classification?company_id=eq.{cid}",
                    headers=SUPABASE_H,
                    json={"vertical": vertical},
                    timeout=10,
                )
                r.raise_for_status()
            except Exception as e:
                logger.error(f"[{domain}] Supabase error: {e}")
                results[-1]["status"] = "error"


async def main():
    limit = 1000
    concurrency = 10
    dry_run = False

    for arg in sys.argv[1:]:
        if arg.startswith("--limit="):
            limit = int(arg.split("=")[1])
        elif arg.startswith("--concurrency="):
            concurrency = int(arg.split("=")[1])
        elif arg == "--dry-run":
            dry_run = True

    logger.info(f"Starting vertical enrichment (limit={limit}, concurrency={concurrency}, dry_run={dry_run})")

    anthropic_client = _get_anthropic_client()
    semaphore = asyncio.Semaphore(concurrency)
    results = []

    async with httpx.AsyncClient(follow_redirects=True) as http:
        # Fetch in batches
        offset = 0
        PAGE_SIZE = 200
        all_rows = []

        while len(all_rows) < limit:
            batch = await fetch_batch(http, min(PAGE_SIZE, limit - len(all_rows)), offset)
            if not batch:
                break
            all_rows.extend(batch)
            offset += PAGE_SIZE
            if len(batch) < PAGE_SIZE:
                break

        logger.info(f"Fetched {len(all_rows)} companies with null vertical + description")

        tasks = [
            process_company(row, anthropic_client, http, semaphore, results, dry_run)
            for row in all_rows
        ]
        await asyncio.gather(*tasks)

    ok = sum(1 for r in results if r["status"] == "ok")
    null = sum(1 for r in results if r["status"] == "null")
    errors = sum(1 for r in results if r["status"] == "error")

    print(f"\n{'='*50}")
    print(f"VERTICAL ENRICHMENT — COMPLETE")
    print(f"{'='*50}")
    print(f"Total processed : {len(results)}")
    print(f"Vertical filled : {ok}")
    print(f"Null (vague)    : {null}")
    print(f"Errors          : {errors}")

    out_path = os.path.join(os.path.dirname(__file__), "vertical_enrichment_results.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults → {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
