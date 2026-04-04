---
name: gtm-linkedin
description: Optional LinkedIn enrichment skill — finds LinkedIn company pages, extracts employee count, and filters by headcount. Runs after gtm-prefilter, before gtm-research.
---

# gtm-linkedin

**Description:** Optional LinkedIn enrichment pass. Finds LinkedIn company pages for prefiltered domains, extracts employee counts, and applies headcount filtering. Uses Firecrawl + Soax proxies.

## When to use

After `gtm-prefilter`, before `gtm-research`. **Optional** — skip if you want to run gtm-research on all prefiltered domains and filter later. Use when you want to cut volume before expensive deep research.

## Inputs

- `--limit N` — how many companies to process (default: 100)
- `--workers N` — parallel workers (default: 5 — LinkedIn blocks aggressive scraping)
- `--min-employees N` — skip companies below this headcount (default: 10)
- `--max-employees N` — skip companies above this headcount (default: 5000)

## What it does

For each `prefiltered` company:
1. Try `https://www.linkedin.com/company/{domain-stem}/` via Firecrawl + Soax proxy
2. If not found, try LinkedIn search: `https://www.linkedin.com/search/results/companies/?keywords={domain-stem}`
3. Extract employee count from page text
4. Apply headcount gate:
   - < min-employees → `skip` (reason: `too_small`)
   - > max-employees → `skip` (reason: `too_large`)
   - 2000–5000 → keep, flag as `borderline_large`
5. Update Supabase:
   - Pass → keep `research_status = 'prefiltered'`, upsert `company_social.linkedin_url`, upsert `company_firmographics.employees_count`
   - Fail → `research_status = 'skip'`

## Firecrawl config
- Endpoint: `YOUR_FIRECRAWL_URL (configured in config.local.json)`
- No auth required (Soax proxy configured internally on the VM)

## Speed
~3-5 domains/minute at 5 workers (LinkedIn rate limits). Full pass on 50k prefiltered ≈ slow — run selectively on high-priority batches.

## Run it
```bash
cd ./scripts
python3 run_linkedin.py --limit 1000 --workers 5 --min-employees 10 --max-employees 5000
```

## Dependencies
- Firecrawl at `YOUR_FIRECRAWL_URL (configured in config.local.json)`
- Supabase credentials
- Script: `scripts/run_linkedin.py` (to be written when needed)

## Notes
- LinkedIn actively blocks scraping — keep workers low (5 max), add jitter between requests
- If LinkedIn page not found: keep company as `prefiltered` (don't skip — some real companies have no LinkedIn)
- This skill is optional: pipeline works without it, employee count filtering just happens later in gtm-segment based on whatever data is available
