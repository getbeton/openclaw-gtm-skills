# Beton GTM Pipeline v2 — Design Spec

**Date:** 2026-04-08
**Status:** Draft
**Author:** Vlad + Claude

## Problem

The current Beton GTM pipeline has 12 steps implemented across 30 Python scripts with massive duplication:
- 5 separate Apollo API wrappers with identical retry/rate-limit logic
- 5 Firecrawl wrappers with duplicated page selection
- 3 Claude classification prompts doing essentially the same thing
- 6 copies of `_load_local_config()` loading the same config file
- No unified orchestrator in production — each script runs manually
- No email validation beyond Apollo's `email_status` field
- No DNC list enforcement, no cross-campaign dedup, no catch-all detection
- No bounce feedback loop from Apollo back to Supabase

The pipeline works the same for 1 and 1,000 domains on paper, but in practice requires manual orchestration of individual scripts with no resume capability.

## Solution

Collapse to a 7-step pipeline that is **agent-first** (skills + Python modules):
- Each step is a skill (SKILL.md) that Claude Code / OpenClaw executes, backed by a Python module
- **Apollo-first research**: free org enrichment for structured data, then Firecrawl for deep page content
- **Deterministic scoring**: no Claude for ICP scoring — pure rules
- Dedicated email verification gate (provider TBD after comparison) — hard reject catch-all emails
- Single CLI entry point (`python beton.py run domain.com`)
- Supabase-driven resume (per-company `pipeline_status`)
- Telegram notification after enrollment so Vlad creates/activates sequences in Apollo UI

## Pipeline: 12 Steps → 7 Steps

| # | Step | Merges | What it does |
|---|------|--------|--------------|
| 1 | **Input** | (same) | Accept domains from CLI args, CSV, or Supabase query |
| 2 | **Prefilter** | old 2+3 | Dedup, normalize, HTTP reachability, parked domain detection |
| 3 | **Research** | old 4+5+6+8a | Apollo org enrichment (FREE) → Firecrawl 12+ pages → single Claude call → Apollo people search (FREE, no reveal) → classification + sales org + tech stack + signals + found leads |
| 4 | **Score** | old 7+9 | Deterministic ICP scoring (uses found roles: VP RevOps → +15 pts) + agent-driven hypothesis generation ("what can we sell?") |
| 5 | **Contacts** | old 8b (enhanced) | Reveal emails for T1/T2 leads (found in step 3) + email verification + DNC + dedup. No gap fill. No catch-all. |
| 6 | **Sequences** | old 10 | 5-step email generation per contact + Haiku pre-send validation |
| 7 | **Enroll** | old 11 | Apollo contact creation + sequence enrollment + Telegram notification to Vlad |

Old step 12 (Deck on positive reply) is triggered separately, not part of the pipeline.

## Architecture

### Directory Structure

```
beton-gtm/
  beton/
    __init__.py
    cli.py                    # argparse with run/step/status/dnc subcommands
    config.py                 # single config loader (env vars → config.local.json → integrations/)
    types.py                  # dataclasses: Company, Contact, Signal, Hypothesis, EmailDraft
    pipeline.py               # orchestrator: runs steps in order, handles resume via pipeline_status
    concurrency.py            # async semaphore pool with per-step limits
    clients/
      __init__.py
      apollo.py               # search_people, enrich_person, enrich_org, create_contact, add_to_sequence
      firecrawl.py            # scrape, map, scrape_pages (with page selection logic)
      classifier.py           # single combined prompt → classification + sales_org + signals
      email_verifier.py       # pluggable interface: MillionVerifier, ZeroBounce, Reoon, etc.
      supabase.py             # extended with pipeline-aware methods
    steps/
      __init__.py
      s1_input.py             # parse CLI args / CSV / Supabase query → list of domains
      s2_prefilter.py         # httpx async reachability + parked detection + LinkedIn employee count
      s3_research.py          # firecrawl crawl + single Claude call (classification + sales org + signals)
      s4_score.py             # rule-based ICP scoring + Claude hypothesis generation
      s5_contacts.py          # Apollo 4-thread search + enrich + verify + DNC + dedup
      s6_sequences.py         # email generation (5 steps × role × hypothesis × vertical) + Haiku validation
      s7_enroll.py            # Apollo contact create + custom fields + sequence add + round-robin mailbox
  beton.py                    # CLI entry point
  legacy/                     # old scripts moved here after each step is verified working
```

### Shared Clients

**`beton/clients/apollo.py`** — Single Apollo client replacing 5 duplicated implementations.

Methods:
- `search_people(domain, titles, seniorities, limit)` → list of person dicts
- `enrich_person(person_id)` → full contact with email
- `enrich_org(domain)` → org info with employee count
- `create_contact(email, name, company, custom_fields)` → contact_id
- `add_to_sequence(contact_id, sequence_id, mailbox_id)` → enrollment result

Internals: httpx.AsyncClient, 429 exponential backoff (2^n seconds), Cloudflare User-Agent header, configurable base URL.

**`beton/clients/firecrawl.py`** — Single Firecrawl client replacing 5 wrappers.

Methods:
- `map(domain)` → list of discovered URLs
- `scrape(url)` → markdown content
- `scrape_pages(domain, max_pages=8)` → dict of {url: markdown} with intelligent page selection (homepage, about, pricing, product, careers, blog)

Internals: httpx.AsyncClient, 30s timeout, retry with backoff, page selection logic extracted from existing `run_research.py`.

**`beton/clients/classifier.py`** — Single Claude prompt replacing 3 variants.

One function: `classify_company(scraped_pages: dict) → CompanyProfile` that returns classification + sales_org + signals in a single call. Uses Haiku-4-5 (fast, cheap). Falls back to Sonnet-4-6 on parse errors.

**`beton/clients/email_verifier.py`** — Pluggable email verification.

Abstract interface:
```python
class EmailVerifier:
    async def verify_single(self, email: str) -> VerificationResult
    async def verify_bulk(self, emails: list[str]) -> list[VerificationResult]

@dataclass
class VerificationResult:
    email: str
    result: str           # valid, invalid, risky, catch_all, unknown, disposable, spam_trap
    is_role_based: bool
    is_catch_all: bool
    risk_score: int       # 0-100
    raw_response: dict
```

Concrete implementations to be chosen after provider comparison (MillionVerifier vs ZeroBounce vs Reoon vs NeverBounce) — research pricing, coverage, and accuracy before implementing this step.

**`beton/clients/supabase.py`** — Extended from existing `scripts/supabase_client.py`.

New pipeline-aware methods:
- `get_companies_at_step(status, limit)` → companies ready for processing
- `advance_status(company_id, new_status)` → update pipeline_status
- `save_research(company_id, classification, sales_org, signals)` → write to 3 tables
- `save_contacts(company_id, contacts)` → upsert to contacts table
- `is_on_dnc(email, domain)` → check DNC list
- `dedup_contacts(contacts, lookback_days=90)` → filter out recently contacted
- `cache_email_validation(email, result)` → save to email_validations
- `get_cached_validation(email)` → check if already verified

### Step Details

#### Step 1: Input (`s1_input.py`)

Accepts domains from three sources:
- CLI args: `python beton.py run domain1.com domain2.com`
- CSV file: `python beton.py run --csv domains.csv` (expects `domain` column)
- Supabase: `python beton.py run --supabase --status raw --limit 100`

Output: list of normalized domains inserted into `companies` table with `pipeline_status = 'input'`.

#### Step 2: Prefilter (`s2_prefilter.py`)

For each domain:
1. HTTP HEAD request (httpx async, 20 concurrent workers)
2. Detect parked domains (keyword matching on response body)
3. Check LinkedIn employee count (10-5000 range filter)
4. Mark unreachable/parked/too-small as `pipeline_status = 'skip'` with reason

Input status: `input` → Output status: `prefiltered` or `skip`

#### Step 3: Research (`s3_research.py`)

The big merge. For each domain:
1. Firecrawl `map(domain)` → discover available pages
2. Select up to 8 pages: homepage, about, pricing, product, careers, blog, team, contact
3. Firecrawl `scrape_pages(selected_urls)` → markdown content
4. Single Claude call with all page content → structured output:
   - **Classification:** b2b, saas, gtm_motion, vertical, business_model, sells_to, pricing_model, description
   - **Sales org:** open_sales_roles, open_revops_roles, open_cs_roles, hiring_signal, departments, tech_stack
   - **Signals:** list of {type, summary, urgency_score, source} for events in last 90 days

One Firecrawl crawl + one Claude call replaces what was previously 3 separate steps with 3 separate Claude calls.

Writes to: `company_classification`, `company_sales_org`, `company_tech_stack`, `signals`
Input status: `prefiltered` → Output status: `researched`
Concurrency: 3 workers (Firecrawl + Claude are expensive)

#### Step 4: Score (`s4_score.py`)

For each researched company:
1. Rule-based ICP scoring (existing logic from `run_segment.py`): points for b2b, saas, vertical match, headcount range, revops signals
2. Assign tier: T1 (fit_score >= 70), T2 (50-69), T3 (30-49), pass (< 30)
3. For T1 and T2 companies: Claude generates 1-2 pain hypotheses using classification + signals + sales org data
4. Skip/pass companies with low scores

Writes to: `company_segments`, `hypotheses`, `company_hypotheses`
Input status: `researched` → Output status: `scored` or `skip`
Concurrency: 10 workers (scoring is fast, hypothesis gen uses Haiku)

#### Step 5: Contacts (`s5_contacts.py`)

For each scored company (T1 and T2 only — T3 and pass companies stop here with `pipeline_status = 'scored'`):

**Search phase:**
1. Apollo `enrich_org(domain)` → get org_id (free)
2. Apollo `search_people()` for all 4 threads in parallel:
   - Strategy: CSO, VP Strategy, Head of Analytics
   - CS: Director CS, VP CS, Head of Retention
   - Revenue: CRO, VP Growth, Director Sales
   - Sales: VP Sales, Director Sales, Head of Sales
3. Client-side priority filtering: pick top contact per thread based on title match quality

**Enrich phase:**
4. Apollo `enrich_person()` → reveal email (costs credits)

**Validation chain:**
5. **Role-based filter:** reject info@, support@, noreply@, admin@, sales@, marketing@, hr@, billing@
6. **Email verifier:** bulk verify all emails for the batch → reject invalid, disposable, spam_trap. Flag catch_all as risky (proceed only if sole contact for thread)
7. **DNC check:** query `dnc_list` table for email or domain match
8. **Cross-campaign dedup:** query `contacts` table for same email contacted in last 90 days
9. **Cache results:** write all verification results to `email_validations` table

**Gap fill:**
10. If any thread has 0 valid contacts, broaden title search once and retry steps 4-9

Target: 1-4 validated contacts per company across 4 threads.

Writes to: `contacts`, `email_validations`
Input status: `scored` → Output status: `contacts_found`
Concurrency: 5 workers (Apollo rate limits)

#### Step 6: Sequences (`s6_sequences.py`)

For each contact with validated email:
1. Load hypothesis + vertical mapping + writing rules
2. Generate 5-step email sequence personalized to:
   - Thread (strategy/CS/revenue/sales)
   - Contact role and seniority
   - Company vertical (user_type, data_action, churn examples)
   - Pain hypothesis (data-grounded + context-specific)
3. Haiku pre-send validation: check that company facts, signals, and contact data are accurate in the generated emails
4. Flag failed validations for manual review

Writes to: `outreach` table (subjects + bodies for 5 steps per contact)
Input status: `contacts_found` → Output status: `sequenced`
Concurrency: 3 workers (Claude generation is expensive)

#### Step 7: Enroll (`s7_enroll.py`)

For each sequenced contact:
1. Apollo `create_contact()` with typed_custom_fields (step1_subject/body through step5_subject/body)
2. Apollo `add_to_sequence()` with correct sequence ID by thread
3. Round-robin mailbox assignment (v@, vlad@, vlad.nadymov@)

Supports testing protocol:
- `--test N`: enroll only N contacts (Phase A/B)
- `--dry-run`: preview enrollment without API calls

Input status: `sequenced` → Output status: `enrolled`
Concurrency: 2 workers (careful enrollment)

### CLI Interface

```bash
# Full pipeline
python beton.py run domain1.com domain2.com           # domains from args
python beton.py run --csv domains.csv                  # from CSV
python beton.py run --supabase --status raw --limit 100  # from Supabase
python beton.py run --from research                    # resume from step 3
python beton.py run --only contacts                    # single step
python beton.py run --dry-run                          # no side effects

# Individual steps
python beton.py step prefilter domain.com
python beton.py step contacts --supabase --status scored --limit 50
python beton.py step enroll --test 5

# Status
python beton.py status                                 # pipeline stats by step
python beton.py status domain.com                      # single company journey

# DNC management
python beton.py dnc add user@example.com --reason bounce
python beton.py dnc add example.com --reason manual
python beton.py dnc list
python beton.py dnc import bounces.csv
```

### Supabase Schema Changes

**New column on `companies`:**
```sql
ALTER TABLE companies ADD COLUMN pipeline_status TEXT DEFAULT 'input';
-- Values: input, prefiltered, researched, scored, contacts_found, sequenced, enrolled, skip
-- Separate from research_status (kept for backward compat)
```

**New table: `email_validations`**
```sql
CREATE TABLE email_validations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email TEXT NOT NULL,
  provider TEXT NOT NULL,          -- 'millionverifier', 'zerobounce', etc.
  result TEXT NOT NULL,            -- 'valid', 'invalid', 'risky', 'catch_all', 'unknown'
  is_role_based BOOLEAN DEFAULT false,
  is_catch_all BOOLEAN DEFAULT false,
  risk_score INTEGER,
  raw_response JSONB,
  validated_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(email, provider)
);
```

**New table: `dnc_list`**
```sql
CREATE TABLE dnc_list (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email TEXT,
  domain TEXT,
  reason TEXT NOT NULL,            -- 'bounce', 'unsubscribe', 'manual', 'complaint'
  source TEXT,                     -- 'apollo', 'manual', 'import'
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(email)
);
```

**New columns on `contacts`:**
```sql
ALTER TABLE contacts ADD COLUMN email_validated BOOLEAN DEFAULT false;
ALTER TABLE contacts ADD COLUMN email_validation_result TEXT;   -- 'valid', 'risky', 'invalid'
ALTER TABLE contacts ADD COLUMN thread TEXT;                    -- 'strategy', 'cs', 'revenue', 'sales'
```

### Concurrency Model

```python
STEP_CONCURRENCY = {
    "prefilter": 20,     # lightweight HTTP checks
    "research": 3,       # Firecrawl + Claude (expensive)
    "score": 10,         # Claude batch scoring (fast Haiku)
    "contacts": 5,       # Apollo rate limited
    "sequences": 3,      # Claude generation (expensive)
    "enroll": 2,         # careful enrollment
}
```

Steps run sequentially (all prefiltering finishes before research starts). Within each step, companies process in parallel up to the concurrency limit via `asyncio.Semaphore`.

For 1 domain: effectively synchronous through all steps.
For 100K domains: bounded concurrency per step. Supabase `pipeline_status` enables resume if process is interrupted.

### Resume Logic

Each company tracks its own `pipeline_status`. The pipeline orchestrator:
1. For each step, queries Supabase for companies at that step's input status
2. Processes the batch with bounded concurrency
3. Advances each company's status on success, marks as `skip` on disqualification

If the process dies mid-step, companies stay at their current status. On re-run, they're picked up automatically. No state files, no external queue — Supabase IS the state store.

### Migration Strategy

1. Build `beton/clients/` first (apollo, firecrawl, classifier, supabase) — extract from existing scripts
2. Build steps one at a time, starting with s2_prefilter (simplest)
3. After each step is verified working, move the old script(s) it replaces to `legacy/`
4. Build CLI last — wire steps together
5. Email verification provider comparison happens before implementing s5_contacts

### What Gets Deleted (→ legacy/)

| Old Script(s) | Replaced By |
|---------------|-------------|
| `run_prefilter.py`, `run_prefilter_homepage_only.py` | `beton/steps/s2_prefilter.py` |
| `run_research.py`, `run_research_combined.py`, `run_research_6sense_posthog.py`, `run_scrape.py` | `beton/steps/s3_research.py` |
| `run_classify.py` | `beton/clients/classifier.py` |
| `run_segment.py`, `run_value_hypothesis.py` | `beton/steps/s4_score.py` |
| `run_sales_org.py`, `run_sales_org_continuous.py`, `run_enrich_vertical.py` | `beton/steps/s3_research.py` |
| `apollo_batch_enrich.py`, `apollo_gap_fill.py`, `find_sales_leaders.py` | `beton/steps/s5_contacts.py` |
| `generate_campaign_emails.py` | `beton/steps/s6_sequences.py` |
| `apollo_enroll.py` | `beton/steps/s7_enroll.py` |
| `pipeline.py` | `beton/pipeline.py` |
| `run_6sense_posthog_full.py`, `run_gtm_pipeline_19.py`, `run_skip_reprocess.py` | `beton/pipeline.py` (with --from/--only flags) |
| `import_clay.py`, `import_wappalyzer.py`, `normalize_domains.py` | `beton/steps/s1_input.py` |

### Key Source Files for Implementation

These existing files contain the core logic to extract and refactor:

- `scripts/pipeline.py` (1145 lines) — orchestrator pattern, research+sales_org+signals logic
- `scripts/supabase_client.py` (426 lines) — Supabase client to extend
- `scripts/run_research_combined.py` — merged research+sales_org pattern (best reference for s3)
- `scripts/apollo_enroll.py` (571 lines) — enrollment logic, custom field mapping, mailbox round-robin
- `campaign-6sense/ws4_pipeline.py` — most complete self-contained pipeline (search→enrich→gen→enroll)
- `scripts/run_segment.py` — ICP scoring rules to preserve
- `scripts/run_value_hypothesis.py` — hypothesis generation prompt to preserve
- `campaign-big-b2b/email-templates.md` — email template structure
- `writing-rules.md` — mandatory style rules

### Verification

After implementation, verify end-to-end:
1. `python beton.py run --dry-run testdomain.com` — full pipeline dry run, no side effects
2. `python beton.py run testdomain.com --only prefilter` — verify HTTP reachability
3. `python beton.py run testdomain.com --only research` — verify Firecrawl + Claude output
4. `python beton.py status testdomain.com` — verify pipeline_status progression in Supabase
5. `python beton.py run --csv test_10.csv` — 10-domain batch, verify concurrency and resume
6. `python beton.py step enroll --test 1` — Phase A testing in Apollo
7. `python beton.py dnc add test@example.com --reason manual` — verify DNC enforcement
