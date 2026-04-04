# gtm-contacts

**Description:** Enrich companies with decision-maker contacts via Apollo.io. Prioritizes sales/CS leaders (not RevOps) based on buyer hierarchy learned from past campaigns.

## Auto-Reveal Policy (MANDATORY)

**When you find 2 good candidates per company (1 for each thread), AUTOMATICALLY reveal their emails.**

Why:
1. **More context** — real names/titles help sharpen email copy
2. **Immediate drafts** — can write sequences right away without placeholders
3. **Credit spend is OK** — Apollo credits are approved for campaign use

Do NOT ask permission — just reveal and proceed with sequence generation.

## 3-Step Reliable Apollo Pipeline

Apollo's `/mixed_people/api_search` endpoint ignores domain filters on basic tiers, returning unrelated people (e.g., Microsoft CEOs when searching for a small startup).

To solve this, the script uses a reliable 3-step process:
1. **Org Enrichment (Free):** `GET /organizations/enrich?domain=X` → gets Apollo internal `org_id`
2. **People Search (Free):** `POST /mixed_people/api_search` with `organization_ids` → finds hidden profiles
3. **Contact Reveal (1 Credit):** `POST /people/match` with person ID → reveals name and email

This ensures we *never* spend credits revealing emails for people at the wrong company.

## When to use

After `gtm-research` and `gtm-sales-org`. Runs on `research_status = 'scored'` companies when you need actual contacts for outbound campaigns.

## Buyer Hierarchy (Priority Order)

### Thread 1: Expansion Signals (Inbound Sales → CRO)

**Priority 1 — Inbound Sales Leader (feels expansion pain directly):**
- Director of Inbound Sales
- Head of Inbound Sales
- Director of Inside Sales
- Manager of Inbound Sales

**Priority 2 — Their boss:**
- VP of Inbound Sales
- VP of Inside Sales
- VP of Sales

**Priority 3 — Their boss's boss:**
- SVP of Sales
- Chief Revenue Officer

**Priority 4 — RevOps (fallback only if no sales leaders found):**
- VP Revenue Operations
- Head of Revenue Operations

### Thread 2: Churn/Performance Signals (CS → CCO)

**Priority 1 — CS Team Leader:**
- Director of Customer Success
- Head of Customer Success

**Priority 2 — Their boss:**
- VP of Customer Success

**Priority 3 — Their boss's boss:**
- Chief Customer Officer

**Priority 4 — CS Ops (fallback only):**
- Director of Customer Success Operations

## Strategy

**Single query + client-side filtering** (not multiple queries):
- Apollo search is free (credits only charged for email reveals)
- One query per company with all titles
- Client-side priority filtering to pick top 2 per thread
- Faster (244 API calls vs 732 if waterfall)

## Setup

### Apollo API Key

Store in `integrations/apollo.json`:
```json
{
  "api_key": "YOUR_APOLLO_API_KEY"
}
```

## Running the Script

### Input Modes (pick one)

**1. From CSV file:**
```bash
python3 scripts/enrich_contacts.py --csv companies.csv --dry-run
```
CSV must have `domain` column (case-insensitive). Optional: `name` column.

**2. From JSON file:**
```bash
python3 scripts/enrich_contacts.py --json companies.json --dry-run
```
JSON array of `{domain, name?, id?}` objects.

**3. From domain list:**
```bash
python3 scripts/enrich_contacts.py --domains "aerospike.com,clickhouse.com,redis.io" --dry-run
```

**4. From Supabase (default):**
```bash
python3 scripts/enrich_contacts.py --status scored --segment "6sense,PostHog" --limit 10 --dry-run
```

### Flags

**Input sources (mutually exclusive):**
- `--csv <file>`: CSV with domain column
- `--json <file>`: JSON with company objects
- `--domains <list>`: Comma-separated domains

**Filters (Supabase mode only):**
- `--status <value>`: Filter by research_status (default: `scored`)
- `--segment <slug>`: Filter by segment slug
- `--limit <n>`: Max companies to process

**Behavior:**
- `--max-per-company <n>`: Max contacts per thread (default: 2)
- `--dry-run`: Search only, don't reveal emails or save
- `--reveal`: Actually reveal emails and save (costs credits)

## Output

For each company:
1. Search Apollo for all target titles (expansion + churn threads)
2. Client-side filter by priority keywords
3. Take top 2 per thread
4. Save to `contacts` table with `company_id` link

## Script Logic

See `scripts/enrich_contacts.py`:

```python
def search_sales_contacts(domain, titles, api_key):
    """Search Apollo for all titles at once (free)."""
    response = requests.post(
        'https://api.apollo.io/v1/mixed_people/search',
        json={
            'api_key': api_key,
            'q_organization_domains': [domain],
            'person_titles': titles,
            'person_seniorities': ['director', 'vp', 'manager', 'c_suite'],
            'organization_num_employees_ranges': ['50,'],
            'per_page': 25
        }
    )
    return response.json().get('people', [])

def filter_by_priority(contacts, priority_keywords, avoid_keywords):
    """
    Sort by title priority with penalty for unwanted keywords.
    
    Scoring:
    - Avoid keywords ("Operations", "RevOps") → score 100 (deprioritized)
    - First matching priority keyword → score = keyword index (0 = best)
    - No match → score 999 (rejected)
    
    Example with priority=["Director", "VP"] and avoid=["Operations"]:
    - "Director of Sales" → 0 (best)
    - "VP of Sales" → 1
    - "Director of Revenue Operations" → 100 (penalized)
    - "Sales Manager" → 999 (rejected)
    """
    def priority_score(contact):
        title = contact.get('title', '').lower()
        # Penalty for avoid keywords
        for avoid in avoid_keywords:
            if avoid.lower() in title:
                return 100
        # Match priority keywords
        for idx, keyword in enumerate(priority_keywords):
            if keyword.lower() in title:
                return idx
        return 999
    return sorted(contacts, key=priority_score)[:2]

# Main loop
companies = get_companies_from_supabase(segment_filter)
for company in companies:
    # Thread 1: Expansion
    expansion_contacts = search_sales_contacts(company['domain'], EXPANSION_TITLES, api_key)
    top_expansion = filter_by_priority(expansion_contacts, EXPANSION_PRIORITY)
    
    # Thread 2: Churn
    churn_contacts = search_sales_contacts(company['domain'], CHURN_TITLES, api_key)
    top_churn = filter_by_priority(churn_contacts, CHURN_PRIORITY)
    
    if not dry_run:
        save_to_supabase(company['id'], top_expansion + top_churn)
```

## Cost Estimation

- **Search calls:** FREE (no credits)
- **Email reveals:** ~1 credit per contact
- **122 companies × 4 contacts (2 per thread):** ~488 credits

Check your Apollo credit balance before running with `--reveal`.

## Notes

- RevOps-focused campaigns historically underperformed → prioritize sales leaders first
- Use `--dry-run` first to preview results before spending credits
- Apollo rate limit: 200 requests/min (we'll stay under with 244 total calls)
- Script respects `contacts.company_id` foreign key (ensure migration applied)

## Dependencies

- Apollo.io API key
- Supabase client (`scripts/supabase_client.py`)
- `requests` Python module
