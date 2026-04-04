# GTM Hypothesis Scoring Rules

## Framework

**Score = Reach × Impact × Confidence**  
Effort = 1 (always drops out — email infrastructure and data already exist)

---

## Reach = Addressable Companies × ACV

### ACV by headcount bucket
| Bucket | ACV/yr |
|--------|--------|
| nano (1-25 people) | $1,500 |
| small (26-100) | $6,000 |
| mid (100-500) | $20,000 |
| large (500+) | $50,000 |

Extract headcount from segment slug: `seed_series_a--{vertical}--b2b--{headcount}--...`

### Addressable % by angle
| Angle | % | Rationale |
|-------|---|-----------|
| hiring_org_gap | 25% | Clear single buyer (VP Sales), observable trigger (job posting) |
| gtm_motion_gap | 20% | Two-buyer problem, but common pattern |
| pricing_model_gap | 15% | Fuzzier buyer (Growth/CPO/CEO) |
| other | 15% | Generic |

**Generic vertical haircut:** if vertical is `other` or `enterprise_saas`, multiply addressable% × 0.40 (lower signal quality in leftover bucket)

---

## Impact (1-10)

| Angle | Base | Rationale |
|-------|------|-----------|
| hiring_org_gap | 9 | Observable trigger (job posting), clear decision-maker (VP Sales), buying-moment timing |
| pricing_model_gap | 7 | Real pain but buyer is ambiguous (Growth vs CPO vs CEO) |
| gtm_motion_gap | 7 | Two-buyer problem — message optimized for neither |
| other | 6 | Generic |

**Adjustments (each max 1 point, cap at 10):**
- Named vertical (not `other`/`enterprise_saas`): +1
- Mid or large headcount bucket: +1

---

## Confidence (1-10)

| Condition | Score |
|-----------|-------|
| Named vertical base | 8 |
| Generic vertical (`other`/`enterprise_saas`) base | 5 |
| Evidence cites ≥6 specific company domains | +2 |
| Evidence cites 3-5 specific company domains | +1 |
| hiring_org_gap + devtools_infra vertical | +1 |

Domain count: regex `\b\w+\.\w{2,4}\b` on evidence_base field, capped at 15.

---

## Angle Detection (from hypothesis_text)

| Angle | Trigger keywords |
|-------|-----------------|
| pricing_model_gap | usage-based, freemium, tiered pric, transactional pric, billing, upgrade trigger, free tier, free-to-paid |
| hiring_org_gap | hiring, ae , sdr, ramp time, quota, headcount, new hire, new rep, new ae, sales rep |
| gtm_motion_gap | plg, hybrid gtm, self-serve, product-led, handoff, go-to-market, inbound, gtm motion, land and expand |
| other | anything else |

First match wins (checked in order above).

---

## Deduplication

Keep only the **highest-scoring hypothesis** per `(segment_id × angle)` combination.

Rationale: multiple hypothesis variants for the same segment+angle are redundant for campaign planning — you only need the best-evidenced one.

---

## Interpreting the Score

The score is a relative unit (`$M`), not literal pipeline. It captures:
- How large the addressable market is (Reach)
- How likely a company in that segment will feel the pain (Impact)
- How confident we are the pain is real and observable (Confidence)

Use it for **ranking**, not forecasting. A $650M score doesn't mean $650M pipeline.

**Key trade-off to watch:** Large-cap segments (fintech large, $50K ACV) dominate numerically but have fewer companies and longer sales cycles. High-n segments (devtools nano, 1,000+ companies, $1,500 ACV) are lower score but lower risk to test.

**Practical rule:** For first experiments, prefer segments where:
- You have prior outreach data (devtools is proven)
- The angle has an observable external trigger (job postings for hiring_org_gap)
- The segment slug contains a named vertical (not "other")
