---
name: gtm-campaign-prep
description: End-to-end campaign preparation — from hypothesis to send-ready CSV. Apollo enrichment, contact reveal, vertical mapping, email generation, LinkedIn sequences. Runs locally from Claude Code. Triggers on "prep campaign", "generate emails", "build campaign", "outreach prep", "campaign for", "run campaign on".
---

# gtm-campaign-prep

End-to-end campaign preparation. Combines Apollo enrichment, contact reveal, vertical mapping, email generation, and LinkedIn sequences. Runs locally without OpenClaw.

## pipeline

```
[1] define segment + hypothesis in Supabase
[2] query scored companies by fit_tier
[3] apollo batch enrichment (free) -> scripts/apollo_batch_enrich.py
[4] analyze results, build waves by employee count
[5] apollo contact reveal (credits) -> people/match endpoint
[6] gap fill — search for missing thread contacts per company
    target: every company gets strategy + CS + sales thread
    script: scripts/apollo_gap_fill.py
    input: gap_fill_needed.json (auto-generated from contact analysis)
    output: gap_fill_results.json -> merged into all_campaign_contacts.json
[7] generate personalized emails -> scripts/generate_campaign_emails.py
    reads from: all_campaign_contacts.json (combined deduped contacts)
[8] review + enroll into Apollo sequences (manual approval required)
```

## wave strategy

| wave | employee count | contacts to reveal | cadence |
|------|---------------|-------------------|---------|
| 1 | 1000+ | 3 per company (strategy + CS + revenue) | full 5-touch + linkedin, 3 threads |
| 2 | 500-999 | 2 per company | 5-touch + linkedin, 2 threads |
| 3 | 200-499 | 2 per company | 5-touch + linkedin, 2 threads |
| 4 | 100-199 | 1 per company | 5-touch email only |

priority: biggest first, all 3 threads available, vertical fit to proof point, stagger waves 2-3 days apart

## multi-thread choreography (per company)

```
day 0:  thread 1 (strategy) email 1 + linkedin connect (+2h)
day 2:  thread 2 (CS) email 1 + linkedin connect (+2h)
day 3:  thread 1 bump
day 4:  thread 3 (revenue) email 1 + linkedin connect (+2h)
day 5:  thread 1 email 3 + linkedin DM / thread 2 bump
day 7:  thread 2 email 3 + linkedin DM / thread 3 bump
day 8:  thread 1 email 4
day 9:  thread 3 email 3 + linkedin DM
day 10: thread 2 email 4
day 12: thread 1 breakup / thread 3 email 4
day 14: thread 2 breakup
day 15: thread 3 breakup
```

per company: 15 emails + 3 linkedin connects + 3 linkedin DMs = 21 touches over 15 days

## email rules (from writing-rules.md)

- all lowercase — subjects, bodies, sign-offs
- one sentence per paragraph, blank line between each
- no trailing periods
- max 60 words per email body (bump is 1 line)
- subject: 3-6 words, no articles. same subject for all follow-ups (re: [subject])
- bump (step 2): "hey {first_name}, did you have time to look into this?\n\nvlad"
- breakup must mention open source: "your data team can run it without me on your own infra"
- no banned words: AI-powered, game-changer, revolutionize, seamlessly, robust, leverage, utilize, cutting-edge, excited, synergy, delve, streamline
- no flattery
- CTAs concrete — say what the 20 minutes covers

## case study reference rules

- only in PS of email 1, never in subject or body
- never name ETG or RateHawk
- use: "closing a similar pilot in travel-tech right now (3,900 employees)"
- for related verticals: "closing a similar pilot with a travel-tech platform — same pattern, different vertical"
- never say "$1B" — too identifiable
- pilot price for these companies: $50K
- timeline: "1 month from signatures to validated signals"

## buyer hierarchy

### thread 1: strategy/analytics
targets: CSO, VP Strategy, Head of Analytics, Director of Data, VP BI
angle: "your segmentation is incomplete — behavioral signals predict what firmographics miss"

### thread 2: CS/retention
targets: Director CS, VP CS, Head of Retention, Sr Director CS Ops
angle: "you are losing users you could save — intervention timing is everything"

### thread 3: revenue/growth
targets: CRO, VP Growth, VP Sales, Director of Sales, CPO
angle: "expansion signals are in your data — nobody is reading them"

## scripts

- `scripts/apollo_batch_enrich.py` — org enrichment + people search + optional reveal
- `scripts/generate_campaign_emails.py` — template-based email generation with vertical mapping
- templates at `campaign-{slug}/email-templates.md`
- output at `campaign-{slug}/all_emails.csv`

## credit budget

typical campaign: ~190 companies, ~480 apollo credits (reveal + gap fill), ~2200 emails, ~900 linkedin touches

big-b2b-churn reference: 600 processed, 178 campaign-ready, 438 contacts, 483 credits, 3066 total actions
