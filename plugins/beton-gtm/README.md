# Beton GTM Intelligence Plugin

Outbound prospecting pipeline for [Beton](https://getbeton.ai). Bundles all GTM intelligence skills into a single plugin that takes a list of domains and turns them into researched, scored, segmented, and outreach-ready prospects.

## What it does

1. **Intake** — normalizes domains, deduplicates against Supabase
2. **Pre-filter** — checks if domain resolves, LinkedIn exists, employee range is plausible
3. **Research** — Firecrawl deep research → classification (B2B, SaaS, GTM motion, vertical, pricing model)
4. **Sales Org** — careers + LinkedIn jobs → headcount estimates, open roles, tech stack signals
5. **Signals** — news, LinkedIn posts, G2 reviews → urgency-scored signals (last 90 days)
6. **Segment** — matches company profile against active ICP segments → fit_score + fit_tier
7. **Contacts** — Apollo.io enrichment → decision-maker contacts with verified emails
8. **Outreach** — drafts personalized email sequences per experiment config → queues for Vlad review
9. **Send** — enrolls contacts into Apollo.io sequences with custom dynamic variables
10. **Deck** — triggered on positive reply → generates sales deck via sales-deck skill

## Pipeline

```
domains.csv
    │
    ▼
[gtm-intake]       → dedup, normalize
    │
    ▼
[gtm-prefilter]    → parallel x10, skip bad domains
    │
    ▼
[gtm-research]     → parallel x5, deep Firecrawl crawl
    │
    ▼
[gtm-sales-org]    → parallel x5, headcount + tech stack
    │
    ▼
[gtm-signals]      → parallel x10, news + LinkedIn + G2
    │
    ▼
[gtm-segment]      → batch, fit score + tier
    │
    ▼
Supabase + Attio sync
    │
    ▼
[gtm-contacts]     → Apollo enrichment, contact reveal
    │
    ▼
[gtm-outreach]     → if experiment_id, draft sequences for T1
    │
    ▼
Vlad reviews → Apollo.io enrollment
    │
    ▼
[gtm-send]         → enroll into Apollo sequences via API
    │
    ▼
[gtm-deck]         → triggered on replied_positive
```

For batch campaign operations, use `gtm-campaign-prep` which combines contacts + outreach + send into a single local workflow run from Claude Code.

## Configuration

Set in OpenClaw plugin config or environment:

| Key | Description |
|-----|-------------|
| `supabaseUrl` | Supabase project URL |
| `supabaseKey` | Supabase service key |
| `attioApiKey` | Attio API key |
| `firecrawlUrl` | Local Firecrawl instance (default: `http://localhost:3002`) |
| `apolloApiKey` | Apollo.io API key (stored in `integrations/apollo.json`) |

## Running the pipeline

```bash
# Full pipeline on a CSV
python3 scripts/pipeline.py --domains path/to/domains.csv

# With experiment
python3 scripts/pipeline.py --domains path/to/domains.csv --experiment-id <uuid>

# Resume interrupted run
python3 scripts/pipeline.py --domains path/to/domains.csv --resume

# Apollo enrollment (campaign operations)
python3 scripts/apollo_enroll.py --workstream ws3 --dry-run
python3 scripts/apollo_enroll.py --workstream ws3 --group sales
```

## Schema Design Principles

1. **Flat over JSONB.** Prefer flat relational tables with FKs over nesting data in JSONB columns.
2. **Migrate more, not less.** More tables = simpler analytics.
3. **Always use Supabase CLI migrations.** Never apply schema changes by executing raw SQL directly.

## Schema

See `supabase/migrations/` for the full schema history. Tables:
- `segments` — ICP definitions with hypotheses
- `companies` — researched prospects
- `company_classification`, `company_sales_org`, `company_segments`, `company_tech_stack` — flat research tables
- `contacts` — decision-makers
- `signals` — urgency-scored triggers
- `experiments` — A/B test tracking
- `outreach` — drafted sequences
- `campaign_sends` — send tracking (Apollo)
- `results` — outcomes per outreach
- `learnings` — post-experiment summaries

## Dependencies

- Firecrawl running at configured endpoint
- Soax proxies configured at `integrations/soax.json`
- Supabase project with schema applied
- Attio workspace with known attribute slugs (see `scripts/attio.py`)
- Apollo.io account with connected mailboxes and API key
- Existing `sales-deck` skill at `~/.openclaw/workspace/skills/sales-deck/`
