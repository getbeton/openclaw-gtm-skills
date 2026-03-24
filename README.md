# openclaw-gtm-skills

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)

An [OpenClaw](https://openclaw.ai) plugin that turns a list of domains into researched, scored, segmented, and outreach-ready B2B prospects. A Clay.com alternative you run yourself.

Inspired by and partially based on [extruct-ai/gtm-skills](https://github.com/extruct-ai/gtm-skills). See [ATTRIBUTION.md](ATTRIBUTION.md).

---

## Pipeline

```
gtm-intake → gtm-prefilter → gtm-linkedin (optional) → gtm-research
    → gtm-sales-org → gtm-signals → gtm-segment → gtm-outreach → gtm-deck
```

| Skill | What it does |
|-------|-------------|
| `gtm-intake` | Normalizes raw domain lists and imports them into Supabase |
| `gtm-prefilter` | Fast HTTP reachability check — filters parked/dead domains (~300-500/min) |
| `gtm-linkedin` | Optional LinkedIn headcount enrichment via Firecrawl + proxies |
| `gtm-research` | Deep Firecrawl crawl → classifies GTM motion, vertical, pricing model |
| `gtm-sales-org` | Extracts sales headcount and RevOps hiring signals from LinkedIn/careers |
| `gtm-signals` | Detects buying signals (job posts, funding, tech changes, etc.) |
| `gtm-segment` | Scores and tiers companies against your ICP segments |
| `gtm-outreach` | Drafts personalized email sequences grounded in research + signals |
| `gtm-deck` | Generates a sales deck when a prospect replies positively |

---

## Requirements

- [OpenClaw](https://openclaw.ai) installed and running
- [Supabase](https://supabase.com) project (for the pipeline database)
- [Firecrawl](https://github.com/mendableai/firecrawl) — self-hosted or [cloud](https://firecrawl.dev)
- Attio API key (optional — only for CRM sync in `gtm-segment`)
- [seqd](https://github.com/getbeton/seqd) (optional — for outreach enrollment in `gtm-outreach`)

---

## Setup

### 1. Apply the database schema

```bash
supabase db push --workdir ./supabase --password <your-db-password>
```

### 2. Configure the plugin

Edit `openclaw.plugin.json` and fill in your values:

```json
{
  "config": {
    "supabaseUrl": "https://yourproject.supabase.co",
    "supabaseKey": "your-service-role-key",
    "attioApiKey": "optional",
    "firecrawlUrl": "http://localhost:3002",
    "seqdApiUrl": "optional"
  }
}
```

### 3. Install in OpenClaw

Copy this directory to your OpenClaw workspace `plugins/` folder:

```bash
cp -r openclaw-gtm-skills ~/.openclaw/workspace/plugins/gtm-skills
```

### 4. Run the pipeline

Start with a CSV of domains:

```bash
cd ./scripts
python3 run_intake.py --input domains.csv
python3 run_prefilter.py --limit 50000 --workers 20
python3 run_research.py --limit 1000 --workers 5
python3 run_segment.py
```

---

## Schema

Full Supabase schema in `supabase/migrations/`. Tables:

- `companies` — core entity, tracks `research_status` through the pipeline
- `company_classification` — GTM motion, vertical, pricing model
- `company_firmographics` — headcount, funding stage
- `company_tech_stack` — detected tools (CRM, analytics, etc.)
- `company_sales_org` — sales/revops headcount, hiring signals
- `company_social` — LinkedIn URL
- `segments` — your ICP segments with pain hypotheses + value props
- `company_segments` — company ↔ segment scoring (fit_score, fit_tier)
- `signals` — buying signals per company
- `contacts` — people found for outreach
- `experiments` — outreach experiments
- `outreach` — drafted sequences, review status
- `results` — outcomes (replied, bounced, etc.)

---

## Database conventions

- Flat relational tables with foreign keys — no JSONB for structured data
- All schema changes via `supabase migration new` — never raw SQL
- Migration files are the source of truth

---

## Attribution

Based on [extruct-ai/gtm-skills](https://github.com/extruct-ai/gtm-skills). See [ATTRIBUTION.md](ATTRIBUTION.md).

---

## License

[GNU Affero General Public License v3.0](LICENSE)
