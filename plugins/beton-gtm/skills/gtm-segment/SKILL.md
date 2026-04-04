# gtm-segment

**Description:** Match a researched company against active ICP segment definitions to assign fit_score (0-100), fit_tier (T1/T2/T3/pass), and segment_id.

## When to use

After `gtm-research`, `gtm-sales-org`, and `gtm-signals` are complete. Can run as a batch over all `classified` companies. Updates company record with segment assignment.

## Inputs

- `company_id`: UUID (or list of UUIDs for batch mode)
- Supabase: reads all active segments + company research data

## Steps

### 1. Load active segments

```sql
SELECT id, name, slug, icp_definition, core_pain_hypothesis, value_prop_angle, target_personas
FROM segments
WHERE is_active = true
ORDER BY name
```

### 2. Load company profile

```sql
SELECT
  c.id, c.domain, c.name, c.research_status,
  cc.b2b, cc.saas, cc.gtm_motion, cc.vertical, cc.business_model,
  cc.sells_to, cc.pricing_model, cc.description,
  cso.sales_headcount, cso.revops_headcount, cso.cs_headcount, cso.hiring_signal,
  ct.crm, ct.sales_engagement_tool, ct.data_warehouse, ct.analytics,
  cf.employees_range, cf.employees_count, cf.funding_total, cf.hq_country,
  (SELECT json_agg(s.*) FROM signals s WHERE s.company_id = c.id) AS signals
FROM companies c
LEFT JOIN company_classification cc ON cc.company_id = c.id
LEFT JOIN company_sales_org cso ON cso.company_id = c.id
LEFT JOIN company_tech_stack ct ON ct.company_id = c.id
LEFT JOIN company_firmographics cf ON cf.company_id = c.id
WHERE c.id = $1
```

### 3. Claude matching

For each company, send its full profile + all segment ICP definitions to Claude:

```
You are a B2B sales analyst. Match this company against our ICP segments.

COMPANY PROFILE:
{company_profile_json}

SEGMENTS:
{segments_json}

For each segment, evaluate fit on these dimensions (score each 0-10):
1. GTM motion match (does their motion match what we target?)
2. Employee range match (are they the right size?)
3. Vertical/category match
4. Tech stack signals (do they use tools we integrate with or replace?)
5. Pain hypothesis fit (does their profile suggest the pain we solve?)
6. Persona availability (do they likely have our target persona?)

Pick the BEST matching segment (or none if no good fit).

Output ONLY valid JSON:
{
  "segment_id": "uuid of best matching segment, or null",
  "segment_slug": "slug or null",
  "fit_score": 0-100,
  "fit_tier": "T1" | "T2" | "T3" | "pass",
  "dimension_scores": {
    "gtm_motion": 0-10,
    "employee_range": 0-10,
    "vertical": 0-10,
    "tech_stack": 0-10,
    "pain_hypothesis": 0-10,
    "persona_availability": 0-10
  },
  "reasoning": "2-3 sentences explaining why this tier",
  "disqualifiers": ["any hard disqualifiers like 'non-B2B', 'too large'"]
}
```

### 4. Tier thresholds

| fit_score | fit_tier |
|-----------|----------|
| 75-100 | T1 |
| 50-74 | T2 |
| 30-49 | T3 |
| 0-29 | pass |

- `pass` = not a fit, don't pursue
- `T3` = weak fit, low priority
- `T2` = solid fit, batch outreach
- `T1` = strong fit, personalized outreach + priority

### 5. Update Supabase

Upsert into `company_segments` (actual table — companies has no segment_id/fit_score columns):
```sql
INSERT INTO company_segments (company_id, segment_id, fit_score, fit_tier, fit_reasoning, assigned_at)
VALUES ($1, $2, $3, $4, $5, now())
ON CONFLICT (company_id, segment_id) DO UPDATE
SET fit_score = EXCLUDED.fit_score,
    fit_tier = EXCLUDED.fit_tier,
    fit_reasoning = EXCLUDED.fit_reasoning,
    assigned_at = now()
```

Update research_status on companies:
```sql
UPDATE companies SET research_status = 'scored', updated_at = now() WHERE id = $1
```

Note: `company_segments` schema: `company_id`, `segment_id`, `fit_score` (integer), `fit_tier` (text), `fit_reasoning` (text), `assigned_at` (timestamptz).

### 6. Batch mode

If given a list of company_ids, process them all. You can batch up to 10 companies per Claude call (include all 10 profiles in one prompt, output array of results). This saves tokens.

Batch output format:
```json
[
  { "company_id": "uuid1", "segment_id": "...", "fit_score": 82, "fit_tier": "T1", ... },
  { "company_id": "uuid2", "segment_id": null, "fit_score": 15, "fit_tier": "pass", ... }
]
```

## Output

```json
{
  "company_id": "uuid",
  "domain": "acme.com",
  "segment_id": "uuid-of-segment",
  "segment_slug": "mid-market-slg",
  "fit_score": 78,
  "fit_tier": "T1",
  "reasoning": "Strong SLG motion, RevOps team being built, Salesforce shop. Matches mid-market-slg ICP closely.",
  "dimension_scores": {
    "gtm_motion": 9,
    "employee_range": 8,
    "vertical": 7,
    "tech_stack": 8,
    "pain_hypothesis": 8,
    "persona_availability": 9
  }
}
```

## Notes

- If no segments exist in DB: skip this step, warn Vlad, set `research_status = 'classified'`
- Hard disqualifiers (auto-set `pass` without Claude scoring):
  - `b2b = false` (consumer company)
  - `research_status = 'skip'`
  - Employee count clearly out of all segment ranges
- Don't reassign segment if already `scored` unless `--force` flag passed

## Dependencies

- Claude (via OpenClaw AI tool)
- Supabase client (`scripts/supabase_client.py`)
- `company_classification`, `company_sales_org`, `company_tech_stack` populated (Skills 2-3 must run first)
- `company_segments` table for output (not `companies.segment_id` — that column doesn't exist)
