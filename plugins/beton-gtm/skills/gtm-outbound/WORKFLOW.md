# Beton GTM Outbound Workflow

End-to-end outbound campaign pipeline. Each skill is independent but designed to feed the next.

## Full Pipeline

```
[1] context-building         Build/maintain ICP, voice, proof points, win cases
         │
         ▼
[2] hypothesis-building      Generate pain hypotheses per vertical (fast, no API)
         │                   OR
         │  market-research  Research vertical pain points via Perplexity/web (deeper, needs API)
         │
         ▼
[3] list-segmentation        Tier classified companies from Supabase by hypothesis fit
         │                   Input: Supabase CSV export (b2b=true, saas=true, PLG/hybrid)
         │                   Output: tiered CSV (Tier 1/2/3) with hypothesis matched per row
         │
         ▼
[4] people-search            Find decision makers (Apollo api_search + people/match)
    email-search             Get verified emails (Apollo waterfall + LeadMagic)
         │
         ▼
[5] email-prompt-building    Build self-contained prompt template per campaign
         │                   Input: context file + hypothesis set + CSV column headers
         │                   Output: prompts/{vertical-slug}/en_first_email.md
         │
         ▼
[6] email-generation         Generate emails per CSV row using prompt template
         │                   Input: prompt template + contact CSV
         │                   Output: emails CSV (JSON per row)
         │
         ▼
[7] email-response-simulation  Tier 1 only — persona simulation + roast before sending
         │
         ▼
[8] email-verification       Validate emails before sending (Apollo waterfall / LeadMagic)
         │
         ▼
[9] campaign-sending         Enroll into Apollo.io sequences via API
                             Script: scripts/apollo_enroll.py
                             Activate sequences in Apollo UI (API cannot activate)
```

## Data Sources (Beton-specific)

| Step | Source | Notes |
|------|--------|-------|
| Company list | Supabase `beton-gtm-system` | 44k Wappalyzer leads, classified by `run_research.py` |
| Company research | Firecrawl (configured in config.local.json) + Gemini flash | Classification pipeline |
| People search | Apollo `mixed_people/api_search` + `people/match` | Search + enrich two-step |
| Email enrichment | Apollo waterfall (LeadMagic integrated) | Connect in Apollo Settings > Waterfall |
| Email sending | Apollo.io sequences | Custom dynamic variable approach (see gtm-send) |

## Supabase Export for Segmentation

To get the classified company list for segmentation:
```sql
SELECT domain, name, gtm_motion, vertical, business_model, sells_to, b2b, saas, description
FROM companies c
JOIN company_classification cc ON c.id = cc.company_id
WHERE c.research_status = 'classified'
  AND cc.b2b = true
  AND cc.saas = true
  AND cc.gtm_motion IN ('PLG', 'hybrid')
ORDER BY cc.classified_at DESC;
```

## Key Rules

- **context file is the source of truth** for voice, ICP, DNC, proof points — all skills read from it
- **hypothesis set is per-vertical** — one file per vertical slug, reused across campaigns
- **prompt template is per-campaign** — one file per campaign, self-contained (no file references at runtime)
- **Tier 1 always gets email-response-simulation review** before sending
- **DNC list in context file** — campaign-sending checks it before upload
- **Apollo.io handles scheduling, daily limits, bounce tracking, OOO detection natively**
