---
name: gtm-prefilter
description: Fast homepage reachability check for 50k domains — no Firecrawl, no proxy, no LinkedIn. Marks domains as prefiltered or skip.
---

# gtm-prefilter

**Description:** Fast pre-filter — checks if a domain is alive and not a parked/for-sale page. Uses direct HTTP only (no Firecrawl, no proxy). LinkedIn enrichment is handled separately by `gtm-linkedin`.

## When to use

After `gtm-intake`, before `gtm-research`. Run on all raw domains before any expensive enrichment.

## Inputs

- `--limit N` — how many domains to process (default: 100)
- `--workers N` — parallel workers (default: 20)
- `--offset N` — skip first N domains (for resuming)

## What it does

For each domain:
1. Direct `httpx.get` with 8s timeout (https first, http fallback)
2. Check response:
   - Unreachable / timeout → `skip` (reason: `unreachable`)
   - Response < 200 bytes → `skip` (reason: `empty_page`)
   - Parked/for-sale page detected → `skip` (reason: `parked`)
   - Otherwise → `prefiltered`
3. Update `companies.research_status` in Supabase

## Parked page signals
"this domain is for sale", "buy this domain", "sedo", "domain parking", "hugedomains", "afternic", "domain has expired", "this web page is parked", etc.

## Speed
~300-500 domains/minute at 20 workers. Full 50k in ~2-3 hours.

## Output
- Supabase `research_status` updated for all processed domains
- `prefilter_results.json` written to `scripts/`

## Run it
```bash
cd ./scripts
python3 run_prefilter.py --limit 50000 --workers 20
```

## What it does NOT do
- No Firecrawl — plain HTTP only
- No LinkedIn — that's `gtm-linkedin` (optional, separate skill)
- No employee count filtering — that happens after LinkedIn enrichment
- No content analysis — just reachability + parked detection

## Dependencies
- `scripts/run_prefilter.py`
- Supabase client credentials (hardcoded in script)
