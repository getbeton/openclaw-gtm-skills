# gtm-signals

**Description:** Extract urgency-scored buying signals from news, LinkedIn company posts, and review sites (last 90 days) and store them in the signals table.

## When to use

After `gtm-research` / `gtm-sales-org`, before `gtm-segment`. Runs in parallel (up to 10 concurrent). Target: find timely reasons to reach out.

## Inputs

- `company_id`: UUID from companies table
- `domain`: company domain
- `company_name`: from `companies.name`

## Signal types and sources

| Type | Source | Urgency range |
|------|---------|---------------|
| `funding` | News search, Crunchbase | 7-10 |
| `leadership_change` | News, LinkedIn | 6-9 |
| `product_launch` | News, company blog, LinkedIn | 5-8 |
| `hiring` | Job listings (from gtm-sales-org data) | 4-7 |
| `competitor_pain` | G2/Capterra reviews | 5-8 |
| `content_signal` | LinkedIn posts | 3-6 |
| `tech_change` | BuiltWith / job req mentions | 4-7 |

## Steps

### 1. News search (funding + leadership + product launches)

Use `web_search` tool with these queries (run all 3):
1. `"{company_name}" funding OR "raised" OR "series" after:90d`
2. `"{company_name}" "chief" OR "CRO" OR "VP Sales" OR "Head of" hired OR appointed OR joins after:90d`
3. `"{company_name}" launch OR launches OR "new product" OR "announces" after:90d`

For each search result:
- Extract: title, snippet, URL, published date
- Claude classifies type + assigns urgency_score (1-10)
- Skip results older than 90 days from today

### 2. LinkedIn company posts

Firecrawl scrape (with Soax proxy):
```
https://www.linkedin.com/company/{domain-stem}/posts/
```

Extract recent posts (last 90 days). Look for:
- Funding announcements
- Product launches
- Leadership announcements
- Events/conferences (buying signal: active GTM motion)
- Content about pain points Beton solves (e.g., "our sales team is manually...")

### 3. G2 / Capterra reviews

Firecrawl search for recent reviews:
```
https://www.g2.com/products/{domain-stem}/reviews?sort=most-recent
https://www.capterra.com/p/{domain-stem}/reviews
```

Look for reviews mentioning:
- CRM/data quality issues → `competitor_pain` signal
- "Too expensive", "switching to" → `competitor_pain`
- "Doesn't integrate with" → `tech_change` opportunity
- "We need better reporting" → pain signal

Limit: most recent 10 reviews from each source.

### 4. Score urgency

Use this rubric:

| Signal | Base score | +bonus |
|--------|------------|--------|
| Funding (Series A+) | 9 | +1 if <30 days |
| Funding (seed) | 7 | +1 if <30 days |
| New CRO/VP Sales | 8 | +1 if <60 days |
| New RevOps hire | 6 | — |
| Product launch | 6 | +1 if in ICP vertical |
| Competitor negative review | 6 | +1 if mentions specific pain |
| Hiring SDRs/AEs | 5 | — |
| LinkedIn content post | 3 | +2 if pain-aware |

### 5. Store signals in Supabase

For each signal found:
```sql
INSERT INTO signals (company_id, type, content, source, detected_at, urgency_score)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT DO NOTHING
```

Content should be a 1-2 sentence summary of the signal, not raw HTML.

### 6. Update company enriched_at

```sql
UPDATE companies SET enriched_at = now(), updated_at = now() WHERE id = $1
```

## Output

```json
{
  "company_id": "uuid",
  "signals_found": 4,
  "signals": [
    {
      "type": "funding",
      "content": "Acme Corp raised $12M Series A led by Andreessen Horowitz (March 2026)",
      "source": "TechCrunch",
      "detected_at": "2026-03-10",
      "urgency_score": 9
    },
    {
      "type": "leadership_change",
      "content": "Sarah Chen joins as CRO, previously VP Sales at Competitor X",
      "source": "LinkedIn",
      "detected_at": "2026-03-05",
      "urgency_score": 8
    }
  ]
}
```

## Notes

- If no signals found in 90 days: that's fine, store nothing (don't create empty/filler signals)
- Dedup: if the same funding round appears in 3 news sources, create ONE signal (pick highest-quality source)
- `content` field: keep it factual and brief — it gets used verbatim in outreach drafts
- Don't force signals — quality over quantity

## Dependencies

- `web_search` tool (OpenClaw built-in)
- Firecrawl at `http://localhost:3002` (for LinkedIn + G2)
- Soax proxies at `integrations/soax.json`
- Claude for signal classification and urgency scoring
- Supabase client (`scripts/supabase_client.py`)
