# gtm-research

**Description:** Deep Firecrawl research pass that crawls key company pages and distills them into a structured classification JSONB (GTM motion, vertical, pricing model, etc.).

## When to use

After `gtm-prefilter`. Runs in parallel (up to 5 concurrent — Firecrawl intensive). This is the heaviest step. Only runs on `research_status = 'prefiltered'` companies.

## Inputs

- `domain`: single domain string
- Firecrawl endpoint: `{FIRECRAWL_URL}`

## Steps

### 1. Get sitemap

```json
POST {FIRECRAWL_URL}/v1/map
{ "url": "https://{domain}", "limit": 100 }
```

Extract all URLs from the sitemap response.

### 2. Identify relevant pages

From the sitemap URLs, select pages matching these patterns (prioritized):

**High value (always crawl if present):**
- `/pricing` or `/pricing/*`
- `/about` or `/about-us`
- `/customers` or `/case-studies` or `/success-stories`
- `/product` or `/features` or `/platform`
- `/how-it-works`
- `/solutions/*`

**Medium value (crawl if few high-value pages found):**
- `/careers` (useful for Skill 3, defer to gtm-sales-org)
- `/blog` homepage only (not individual posts)
- `/integrations`

**Skip entirely:**
- Blog post archive pages (`/blog/page/`, `/blog?page=`, `/blog/tag/`)
- Individual blog posts (URLs with date patterns: `/blog/2024/`, `/blog/2025/`)
- Legal pages: `/privacy`, `/terms`, `/gdpr`, `/cookie`
- Press/news archives: `/press`, `/news/page/`
- Login/app pages: `/app/`, `/dashboard/`, `/signin`

Limit: max 8 pages total. Prefer quality over quantity.

### 3. Crawl selected pages

For each selected URL:
```json
POST {FIRECRAWL_URL}/v1/scrape
{
  "url": "{url}",
  "formats": ["markdown"],
  "onlyMainContent": true
}
```

Concatenate all page content (with URL headers) into a single research document.

### 4. Claude distillation

Send the concatenated content to Claude with this prompt:

```
You are a B2B SaaS analyst. Based on the following website content, extract structured information about this company.

Output ONLY valid JSON with exactly these fields:
{
  "companyResolvedName": "Official company name",
  "primaryWebsite": "canonical domain",
  "b2b": true/false,
  "saas": true/false,
  "gtmMotion": "PLG" | "SLG" | "hybrid" | "unknown",
  "vertical": "short vertical label (e.g. 'revenue intelligence', 'HR tech', 'fintech')",
  "businessModel": "subscription" | "usage-based" | "freemium" | "marketplace" | "services" | "other",
  "sellsTo": "SMB" | "mid-market" | "enterprise" | "all" | "unknown",
  "pricingModel": "per-seat" | "flat-rate" | "usage-based" | "tiered" | "custom-only" | "freemium" | "unknown",
  "keyFeatures": ["feature1", "feature2", "feature3"],
  "evidence": {
    "gtmMotion": "quote or observation that led to this conclusion",
    "vertical": "quote or observation",
    "sellsTo": "quote or observation",
    "pricingModel": "quote or observation"
  }
}

GTM motion definitions:
- PLG: self-serve signup, free tier, trial-first, usage triggers upgrade
- SLG: demo required, "contact sales", no visible pricing, SDR/AE motion
- hybrid: both PLG and SLG present

Website content:
{concatenated_content}
```

### 5. Parse and validate output

- Validate JSON structure. If invalid, retry Claude once with stricter prompt.
- If still invalid, store raw response in `research_raw.distillation_error` and mark `research_status = 'raw'` for manual retry.

### 6. Update Supabase

Schema uses flat tables (not JSONB blobs):
- `companies`: update `name`, `research_status = 'classified'`, `enriched_at`, `research_raw` (pages content only)
- `company_classification`: upsert with all classification fields
  - Fields: `b2b`, `saas`, `gtm_motion`, `vertical`, `business_model`, `sells_to`, `pricing_model`, `description`, `evidence`, `classified_at`
  - Use `ON CONFLICT (company_id) DO UPDATE`

## Output (classification JSONB stored in companies.classification)

```json
{
  "companyResolvedName": "Acme Corp",
  "primaryWebsite": "acme.com",
  "b2b": true,
  "saas": true,
  "gtmMotion": "hybrid",
  "vertical": "sales automation",
  "businessModel": "subscription",
  "sellsTo": "mid-market",
  "pricingModel": "per-seat",
  "keyFeatures": ["email sequencing", "CRM sync", "analytics"],
  "evidence": {
    "gtmMotion": "Free trial visible + 'Talk to sales' CTA on pricing page",
    "vertical": "Homepage: 'Sales teams close more deals with...'",
    "sellsTo": "Pricing page shows 'Teams' and 'Business' tiers, 25-500 seats",
    "pricingModel": "$X/user/month on pricing page"
  }
}
```

## Outreach Signal Extraction (What to look for)

When researching a company specifically for cold email signal generation (either manually or via AI), look for these concrete data points to ground the messaging:

1. **Hero/Positioning:** 5-word summary of what they do (used for email opening).
2. **Product Specifics:** Technical capabilities with numbers (e.g., latency, throughput, scale).
3. **Customers/Use Cases:** Named logos and the specific metrics achieved.
4. **Pricing/Tiers:** Metered dimensions (seats vs. usage) and upgrade triggers.
5. **Tech Stack:** Tools integrated or used (e.g., PostHog, 6sense, CRM).
6. **Hiring/Team:** Open roles indicating scaling pain (RevOps, Sales Ops).
7. **Recent Focus:** Topics from recent blogs or product updates.

*Rule: Look for specific numbers, technologies, and use cases, not marketing fluff.*

## Notes

- Store the raw page markdown in `research_raw.pages` for audit/debugging
- If a domain redirects to a different domain, update `companies.name` with resolved name
- Pages that 404 or fail to load: skip silently, note in `research_raw.failed_urls`
- Aim for <3 min per company total

## Dependencies

- Firecrawl at `http://localhost:3002`
- Claude (via OpenClaw AI tool)
- Supabase client (`scripts/supabase_client.py`)
