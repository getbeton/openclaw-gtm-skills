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
         │                   (outputs same hypothesis_set.md format — interchangeable)
         │
         ▼
[3] list-segmentation        Tier classified companies from Supabase by hypothesis fit
         │                   Input: Supabase CSV export (b2b=true, saas=true, PLG/hybrid)
         │                   Output: tiered CSV (Tier 1/2/3) with hypothesis matched per row
         │
         ▼
[4] people-search            Find decision makers (Apollo free tier / manual)
    email-search             Get verified emails (waterfall: Apollo → LeadMagic → Findymail)
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
[8] email-verification       Validate emails before sending (ZeroBounce / Findymail)
         │
         ▼
[9] campaign-sending         Upload to seqd (personal email sequencer on getbeton.info)
```

## File Structure

```
beton/gtm-outbound/
├── context/
│   ├── beton_context.md              ← ICP, voice, proof points, win cases, DNC
│   └── {vertical-slug}/
│       ├── hypothesis_set.md          ← Pain hypotheses with search angles
│       └── sourcing_research.md       ← Optional: market research output
├── prompts/
│   └── {vertical-slug}/
│       ├── en_first_email.md          ← Self-contained prompt template
│       └── en_follow_up_email.md
└── csv/
    ├── input/{campaign-slug}/
    │   ├── companies_tiered.csv       ← From list-segmentation
    │   ├── contacts.csv               ← From people-search + email-search
    │   └── contacts_verified.csv      ← From email-verification
    └── output/{campaign-slug}/
        └── emails.csv                 ← From email-generation
```

## Skill Locations

| Skill | Location | Extruct needed? |
|-------|----------|----------------|
| context-building | `~/.openclaw/workspace/skills/context-building/` | No |
| hypothesis-building | `~/.openclaw/workspace/skills/hypothesis-building/` | No |
| email-prompt-building | `~/.openclaw/workspace/skills/email-prompt-building/` | No |
| email-generation | `~/.openclaw/workspace/skills/email-generation/` | No |
| list-segmentation | `~/.openclaw/workspace/skills/list-segmentation/` | No (reads Supabase CSV) |
| market-research | extruct-ai/gtm-skills (reference) | No (Perplexity or web) |
| email-response-simulation | extruct-ai/gtm-skills (reference) | No |
| email-verification | extruct-ai/gtm-skills (reference) | No |
| campaign-sending | extruct-ai/gtm-skills (reference) | No (maps to seqd) |

## Data Sources (Beton-specific)

| Step | Source | Notes |
|------|--------|-------|
| Company list | Supabase `beton-gtm-system` | 44k Wappalyzer leads, classified by `run_research.py` |
| Company research | Firecrawl ({FIRECRAWL_URL}) + Gemini flash | Classification pipeline |
| People search | Apollo free tier (50 contacts/mo) | Manual for now |
| Email enrichment | Apollo → LeadMagic → Findymail → Hunter → Prospeo | Waterfall |
| Email sending | seqd (FastAPI on getbeton.info) | Personal email sequencer |
| CRM passthrough | Attio | CC/BCC integration in seqd |

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

Export as CSV → use as input to `list-segmentation`.

## Key Rules

- **context file is the source of truth** for voice, ICP, DNC, proof points — all skills read from it
- **hypothesis set is per-vertical** — one file per vertical slug, reused across campaigns
- **prompt template is per-campaign** — one file per campaign, self-contained (no file references at runtime)
- **Tier 1 always gets email-response-simulation review** before sending
- **DNC list in context file** — campaign-sending checks it before upload
- **seqd auto-triggers sales deck** on email.replied webhook
