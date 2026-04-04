---
name: email-prompt-building
description: >
  Generate self-contained email prompt templates for cold outreach campaigns.
  Reads from the Beton context file (voice, value prop, proof points) and
  campaign research (hypotheses) to produce a prompt that the email-generation
  skill runs per-row against a contact CSV. One prompt per campaign.
  Triggers on: "cold email", "outreach prompt", "email campaign",
  "new vertical email", "draft email prompt", "email sequence", "write emails".
---

# Cold Email Prompt Builder

Generate self-contained prompt templates for cold outreach. Each prompt encodes everything email-generation needs: voice, research data, value prop, proof points, personalization rules. No external file references at runtime.

## Pipeline Position

```
hypothesis-building → [email-prompt-building] → email-generation
        ↑                       ↑
  context file           classified companies CSV
                         (from Supabase export)
```

## Architecture

```
         BUILD TIME (this skill)
         ┌──────────────────────────────────────┐
context  ─▶                                      │
hypothesis─▶  Synthesize into self-contained     ├──▶ prompt template (.md)
CSV cols ─▶   prompt with all reasoning baked in │
         └──────────────────────────────────────┘

         RUN TIME (email-generation skill)
         ┌──────────────────────────────────────┐
prompt   ─▶  Generate emails per CSV row        ├──▶ emails CSV
contact CSV─▶                                   │
         └──────────────────────────────────────┘
```

## Input Files

| Input | Source | What to extract |
|-------|--------|-----------------|
| Context file | `beton/gtm-outbound/context/beton_context.md` | Voice, sender, value prop, proof library, key numbers, banned words |
| Hypothesis set | `beton/gtm-outbound/context/{vertical-slug}/hypothesis_set.md` | Numbered hypotheses with mechanisms |
| Classified companies CSV | Supabase export: `b2b=true, saas=true, gtmMotion IN (PLG, hybrid)` | Column headers → enrichment fields |
| Campaign brief | User input | Target vertical, role types, campaign angle |

Note: `sourcing_research.md` from `market-research` is optional — if present, read it. If not, use hypothesis set only.

## Output

```
beton/gtm-outbound/prompts/{vertical-slug}/en_first_email.md
beton/gtm-outbound/prompts/{vertical-slug}/en_follow_up_email.md  (if needed)
```

## Workflow

### Step 1: Read upstream data

```
beton/gtm-outbound/context/beton_context.md
beton/gtm-outbound/context/{vertical-slug}/hypothesis_set.md
beton/gtm-outbound/context/{vertical-slug}/sourcing_research.md  (optional)
```

### Step 2: Ask for CSV column list

Ask the user to paste the CSV headers from the classified companies export. These become the enrichment fields referenced in the prompt.

### Step 3: Synthesize (the reasoning step)

**Voice → from context file:**
Copy sender name, tone, constraints, banned words verbatim. Never invent voice rules.

**P1 → from hypotheses:**
For each hypothesis, write a rich description with the MECHANISM (why pain exists, not just symptom). Include specific numbers if available. Reference enrichment fields by column name.

**Competitive awareness (embed in P1/P2):**
If enrichment data reveals prospect has overlapping capability:
- Never pitch as replacement — position as complementary layer
- Acknowledge their existing tool in P1
- Shift P2 to "what Beton adds to what you already have"
- If prospect FOUNDED a competing product → use Variant D or flag for manual review

**P2 → from context file → What We Do:**
Which value angle matters for THIS audience + THIS hypothesis. Use email-safe value prop, not raw version. Example queries must reference prospect's actual vertical — never generic.

**P4 → from context file → Proof Library:**
Select proof points by three criteria:
1. Peer relevance — proof company same size or larger than prospect
2. Hypothesis alignment — validates the same hypothesis used in P1
3. Non-redundancy — don't repeat stats already used in P2

If no proof point meets all three, drop P4 and use a shorter variant.

### Step 4: Assemble the prompt

Write the `.md` file with this skeleton:

```markdown
[Role line from context → Voice → Sender]

[Core pain — 2-3 sentences. Not generic.]

## Hard constraints
[From context → Voice. Copied verbatim.]

## Enrichment data fields
[Table: field name → what it tells you → how to use it]

## Hypothesis-based P1
[Per hypothesis: mechanism, evidence, usage rules. Grounded in data.]

## Role-based emphasis
[Map role keywords → emphasis angle]

## Structural variants
[Select variant per recipient based on role + seniority]

## Competitive awareness
[Rules for overlapping capabilities]

## Proof point selection
[Three-dimensional selection rules]

P1 — [Rules referencing hypotheses + enrichment fields]
P2 — [Value angles per hypothesis. Key numbers. Vertical-specific examples.]
P3 — [CTA rules]
P4 — [Proof points with conditions]

## Output format
[JSON keys]

## Banned phrasing
[From context → Voice + campaign additions]
```

### Step 5: Self-containment check

- [ ] Voice rules from context file, not hardcoded
- [ ] Structural variants defined with role-based selection logic
- [ ] P1 uses actual company description, not generic framing
- [ ] P2 example queries reference prospect's actual vertical
- [ ] P4 proof points pass all three criteria
- [ ] Research data has actual numbers, not "use research data"
- [ ] No references to external files
- [ ] Banned words from context file included

## Structural Variants

| Variant | Who | Paragraphs | Max words | Notes |
|---------|-----|------------|-----------|-------|
| A: Technical Evaluator | CTO, VP Eng, Head of Data, RevOps | 4 (P1-P4) | 120 | Full structure with PS |
| B: Founder / CEO | Small company (<50 people) | 3 (P1-P3) | 90 | Merge P2+P4, no PS |
| C: Executive / Board | Chairman, delegates decisions | 2-3 | 70 | Forwardable, sharp |
| D: Peer Founder | Built something adjacent/competing | 2 | 60 | Peer-to-peer, no pitch |
| Follow-up | Any | 2 | 60 | Different angle from first email |

## References

- Email structures (4P-short, 3P-question, 2P-followup, 4P-story, 3P-event): see [references/email-structures.md](references/email-structures.md)
- Prompt patterns (pain-theme segmentation, role-based, post-event, etc.): see [references/prompt-patterns.md](references/prompt-patterns.md)
