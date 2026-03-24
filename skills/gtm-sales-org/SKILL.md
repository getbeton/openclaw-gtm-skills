# gtm-sales-org

**Description:** Extract sales org headcount estimates, open role signals, and tech stack from careers pages and LinkedIn job listings.

## When to use

After `gtm-research`. Runs in parallel (up to 5 concurrent). Only runs on `research_status = 'classified'` companies.

## Inputs

- `domain`: single domain string
- `company_name`: from `companies.name` (for LinkedIn search)
- Firecrawl endpoint: `http://localhost:3002`
- Soax proxies: `integrations/soax.json`

## Steps

### 1. Find and scrape careers page

Try these URLs in order (stop at first success):
1. `https://{domain}/careers`
2. `https://{domain}/jobs`
3. `https://{domain}/work-with-us`
4. `https://{domain}/join-us`
5. Firecrawl map the site and look for a URL containing `careers`, `jobs`, `work`, `hiring`

Scrape with Firecrawl:
```json
POST http://localhost:3002/v1/scrape
{
  "url": "{careers_url}",
  "formats": ["markdown"],
  "onlyMainContent": true
}
```

### 2. Scrape LinkedIn jobs

Firecrawl scrape (with Soax proxy):
```
https://www.linkedin.com/jobs/search/?keywords={company_name}&f_C={company_id}
```

Alternative: search for open roles directly:
```
https://www.linkedin.com/jobs/search/?keywords={company_name}+sales+OR+revenue+OR+operations
```

### 3. Extract and categorize open roles

Parse job titles from both sources. Categorize by function:

| Function | Keywords |
|----------|----------|
| Sales | AE, SDR, BDR, Account Executive, Sales Rep, Business Development |
| RevOps | Revenue Operations, RevOps, Sales Operations, CRM Admin, GTM Operations |
| CS | Customer Success, CSM, Onboarding, Implementation |
| Marketing | Marketing, Demand Gen, Growth, Content |
| Engineering | Engineer, Developer, Platform, Backend, Frontend |

Count open roles per function.

### 4. Infer headcount from open roles

Use this heuristic (open roles ≈ 5-10% of function headcount at healthy companies):
- Open sales roles × 15 = rough sales team size estimate
- If zero open roles but company classified: use firmographic estimate from Skill 2

### 5. Extract tech stack signals from JD requirements

Scan all job description text for tool mentions:

**CRM signals:** Salesforce, HubSpot, Pipedrive, Close, Zoho CRM
**Sales engagement:** Outreach, Salesloft, Apollo, Groove, Mixmax, Yesware
**Data/enrichment:** ZoomInfo, Apollo, Clay, Clearbit, Lusha, LinkedIn Sales Nav
**Analytics/BI:** Tableau, Looker, Metabase, Sisense, Mixpanel, Amplitude
**Warehouse:** Snowflake, BigQuery, Redshift, dbt

Pick the most-mentioned tool per category as the primary.

### 6. Infer hiring signal

| Condition | Signal |
|-----------|--------|
| ≥3 open sales roles | `scaling_sales` |
| RevOps role open | `building_revops` |
| 0 open roles in known companies | `not_hiring` |
| Mix of IC + management | `growing_team` |
| Senior-only roles | `replacing_not_growing` |

### 7. Update Supabase

Write to dedicated tables (NOT `companies.sales_org` / `companies.tech_stack` — those columns don't exist):

**`company_sales_org`** (upsert on company_id):
```json
{
  "company_id": "uuid",
  "sales_headcount": 25,
  "revops_headcount": 3,
  "cs_headcount": 10,
  "hiring_signal": true,
  "enriched_at": "ISO timestamp"
}
```
Note: `hiring_signal` is boolean (true = any open roles found, false = none).

**`company_open_roles`** (insert per role, no upsert — delete old rows first if re-running):
```json
{
  "company_id": "uuid",
  "title": "Account Executive",
  "function": "sales",
  "seniority": "ic",
  "source_url": "https://acme.com/jobs"
}
```

**`company_tech_stack`** (upsert on company_id):
```json
{
  "company_id": "uuid",
  "crm": "Salesforce",
  "sales_engagement_tool": "Outreach",
  "data_warehouse": "Snowflake",
  "analytics": "Looker",
  "enriched_at": "ISO timestamp"
}
```

Research status stays `'classified'` after this step.

## Output

```json
{
  "domain": "acme.com",
  "salesHeadcount": 25,
  "revopsHeadcount": 3,
  "csHeadcount": 10,
  "openRoles": [...],
  "crm": "Salesforce",
  "salesEngagementTool": "Outreach",
  "dataTools": ["ZoomInfo"],
  "hiringSignal": "building_revops"
}
```

## Notes

- If careers page doesn't exist or returns 404: store `{"careers_page": false}` in `sales_org`, continue
- LinkedIn scraping may fail intermittently — use Soax proxies and add jitter
- Don't infer headcount if total open roles = 0 (unreliable)
- Greenhouse, Lever, Ashby ATS pages often list jobs better than /careers — Firecrawl these if linked

## Dependencies

- Firecrawl at `http://localhost:3002`
- Soax proxy config at `integrations/soax.json`
- Supabase client (`scripts/supabase_client.py`)
- Claude for categorization if job titles are ambiguous
