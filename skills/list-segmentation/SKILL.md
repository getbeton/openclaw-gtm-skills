---
name: list-segmentation
description: >
  Take a classified companies CSV (from Supabase) and a hypothesis set, then
  segment companies by hypothesis fit and assign tiers (1/2/3) based on data
  richness and signal strength. Outputs a tiered, segmented CSV ready for
  email-generation. Beton-specific: reads from Supabase export, not Extruct.
  Triggers on: "segment companies", "tier companies", "prioritize list",
  "segment and tier", "tiering", "which companies first", "who to email first".
---

# Segment and Tier

Take classified companies from Supabase + hypothesis set → produce a tiered, segmented list. This decides WHO gets which message and in what order.

## Pipeline Position

```
hypothesis-building → [list-segmentation] → email-prompt-building → email-generation
         ↑                     ↑
  hypothesis_set.md    Supabase CSV export
```

## Inputs

| Input | Source | Required |
|-------|--------|----------|
| Classified companies CSV | Supabase export (see query below) | yes |
| Hypothesis set | `beton/gtm-outbound/context/{vertical-slug}/hypothesis_set.md` | yes |
| Context file | `beton/gtm-outbound/context/beton_context.md` | recommended |

## Supabase Export Query

```sql
SELECT
  c.domain, c.name,
  cc.gtm_motion, cc.vertical, cc.business_model,
  cc.sells_to, cc.b2b, cc.saas, cc.description,
  cc.pricing_model, cc.key_features, cc.evidence
FROM companies c
JOIN company_classification cc ON c.id = cc.company_id
WHERE c.research_status = 'classified'
  AND cc.b2b = true
  AND cc.saas = true
  AND cc.gtm_motion IN ('PLG', 'hybrid')
ORDER BY cc.classified_at DESC;
```

Export as CSV → provide to this skill.

## Workflow

### Step 1: Load data

Read the CSV — parse all rows. Read the hypothesis set — parse each hypothesis into:
- Number + short name
- Description + pain mechanism
- Best-fit company type

### Step 2: Match companies to hypotheses

For each company row, evaluate which hypothesis fits best:

1. **Vertical alignment** — does the company's `vertical` match the hypothesis "best fit"?
2. **GTM signal** — does `gtm_motion` + `pricing_model` confirm or contradict the hypothesis?
3. **Description alignment** — does `description` reference the pain point?
4. **Evidence** — any `evidence` fields that confirm hypothesis signals?

Assign each company ONE primary hypothesis. If multiple fit, pick strongest evidence. If none fit, mark as "Unmatched."

### Step 3: Assign tiers

| Tier | Criteria | Next step |
|------|----------|-----------|
| **Tier 1** | Strong hypothesis fit + description-rich + clear hook signal in evidence | Full personalization, route through email-response-simulation |
| **Tier 2** | Medium fit OR rich data but no clear hook | Templated with hypothesis variation |
| **Tier 3** | Weak fit OR sparse description OR unmatched | Hold for re-enrichment or drop |

**Tier 1 signals:** specific evidence field populated, pricing/GTM matches hypothesis exactly, description mentions the exact pain mechanism
**Tier 3 signals:** description is null/generic, vertical doesn't match any hypothesis, all evidence fields empty

### Step 4: Output

**Markdown summary (for review):**
```
## Segmented List: [Campaign]

### Tier 1 — N companies
| Company | Domain | Hypothesis | Tier Rationale | Hook Signal |

### Tier 2 — N companies
| Company | Domain | Hypothesis | Tier Rationale |

### Tier 3 — N companies (hold)
| Company | Domain | Issue |
```

**CSV output** → `beton/gtm-outbound/csv/input/{campaign-slug}/segmented_list.csv`

Columns: `company_name, domain, tier, hypothesis_number, hypothesis_name, tier_rationale, hook_signal`

### Step 5: Review with user

Present summary stats and ask:
- "Does the distribution look right? (Expected: 10-20% Tier 1, 50-60% Tier 2, 20-30% Tier 3)"
- "Any companies to move tiers manually?"
- "Ready to proceed to people-search + email-prompt-building?"

## Reference

Detailed tiering decision matrix and scoring formula: [references/tiering-framework.md](references/tiering-framework.md)
