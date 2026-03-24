# gtm-intake

**Description:** Normalize and deduplicate a list of domains against the Supabase companies table before running the GTM pipeline.

## When to use

At the start of every pipeline run. Before any crawling or enrichment happens. Input can be a CSV file or a raw list of domains passed directly.

## Inputs

- `domains_input`: one of:
  - Path to a CSV file (any column containing `domain`, `website`, or `url` — auto-detected)
  - A list of domain strings (e.g. `["acme.com", "stripe.com"]`)

## Steps

1. **Parse input**
   - If CSV: detect the domain column by checking headers for `domain`, `website`, `url`, `company_url` (case-insensitive). If no match, use the first column.
   - If list: use as-is.

2. **Normalize each domain**
   - Strip `http://`, `https://`, `www.`
   - Strip trailing slashes and paths (keep only `host`)
   - Lowercase
   - Example: `https://www.Acme.com/about` → `acme.com`

3. **Deduplicate the input list** (remove duplicates within the batch itself)

4. **Check Supabase for existing records**
   ```sql
   SELECT domain FROM companies WHERE domain = ANY($1)
   ```
   - Skip any domain that already exists in the `companies` table (regardless of `research_status`)

5. **Insert new domains** with `research_status = 'raw'`
   ```sql
   INSERT INTO companies (domain, research_status)
   VALUES ($1, 'raw')
   ON CONFLICT (domain) DO NOTHING
   ```

6. **Return** the list of newly inserted domains (the ones that weren't already in the DB)

## Output

```json
{
  "total_input": 150,
  "already_existed": 23,
  "duplicates_in_batch": 4,
  "new_domains": ["acme.com", "hubspot.com", ...],
  "new_count": 123
}
```

## Notes

- Log any domains that failed normalization (e.g., clearly invalid strings like `N/A`, `http://`, bare IPs)
- Do NOT skip based on `research_status` — if it's in the DB, it's already been processed or is in progress
- The returned `new_domains` list is what gets passed to `gtm-prefilter`

## Schema changes

If you need to add or modify tables/columns, always create a new Supabase CLI migration — never run raw SQL directly. See the **Schema Design Principles** section in `plugins/beton-gtm/README.md`.

## Dependencies

- Supabase client (`scripts/supabase_client.py`)
- Python `csv`, `urllib.parse` standard libraries
