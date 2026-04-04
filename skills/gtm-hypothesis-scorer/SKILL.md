---
name: gtm-hypothesis-scorer
description: >
  Score and rank all GTM hypotheses in the Beton Supabase DB using a RICE-based framework
  (Reach × Impact × Confidence). Outputs a ranked terminal table and CSV ready for campaign
  planning. Use when: asked to score hypotheses, rank experiments, find the best outreach
  angle, prioritize GTM campaigns, see RICE scores, or decide which segment to target first.
  Triggers on: "score hypotheses", "rank experiments", "best outreach angle", "RICE score",
  "prioritize campaigns", "which segment to target", "top hypotheses by score".
---

# GTM Hypothesis Scorer

Scores all `data_grounded` hypotheses from Supabase using RICE (Reach × Impact × Confidence).
Deduplicates to one best hypothesis per segment × angle combination.

## Run it

```bash
python3 ~/.openclaw/workspace/skills/gtm-hypothesis-scorer/scripts/score_hypotheses.py
```

Outputs:
1. Terminal table (top 20 by score)
2. CSV → `beton/gtm-outbound/csv/hypothesis_scores_latest.csv` (all 181 rows)

## Scoring logic

See `references/scoring_rules.md` for the full breakdown.

**Quick summary:**
- **Score = Reach × Impact × Confidence** (Effort=1, always drops out)
- **Reach** = addressable_companies × ACV (nano=$1.5K | small=$6K | mid=$20K | large=$50K)
- **Impact** driven by angle clarity: hiring_org_gap (9) > pricing/gtm_motion (7) > other (6)
- **Confidence** driven by evidence quality: named vertical (8 base) + domain citations in evidence

## Interpreting output

The score is a **relative ranking unit**, not literal pipeline.

Three angles appear:
- `hiring_org_gap` — best: clear buyer (VP Sales), observable trigger (active AE job posting)
- `pricing_model_gap` — good: real pain, fuzzier buyer (Growth/CPO)
- `gtm_motion_gap` — usable: two-buyer problem, harder to write one crisp opener

**Practical heuristic:**
- Large-cap segments (fintech large, $50K ACV) score highest but are harder to test cold
- High-n named-vertical segments (devtools small, 426 companies) are lower score but lower risk as first experiment — especially if you have prior outreach data

## When to re-run

Re-run after:
- New hypothesis generation completes (`run_value_hypothesis.py` finishes a batch)
- Segment scoring runs on newly classified companies
- You want a fresh ranking after pipeline has processed more companies
