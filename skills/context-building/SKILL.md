---
name: context-building
description: >
  Build and maintain the Beton GTM context file — the single source of truth
  for all outbound campaigns. Captures product info, voice rules, ICP, win
  cases, proof library, campaign history, hypotheses, and DNC list. All other
  GTM skills read from this file. Supports four modes: create (new context),
  update (add sections), call-recording (extract signals from transcripts),
  feedback-loop (import campaign results).
  Triggers on: "build context", "update context", "company context", "ICP",
  "win cases", "proof points", "DNC list", "campaign history", "call recording",
  "feedback loop", "outbound context".
---

# Company Context Builder

One global context file. Every other GTM skill reads from it for voice, value prop, ICP, win cases, proof points, and campaign learnings.

## Pipeline Position

```
[context-building] → hypothesis-building → email-prompt-building → email-generation
         ↑                                         ↑
  (always read first)               (voice, proof points, ICP)
```

## Context File Location

```
beton/gtm-outbound/context/beton_context.md
```

Single file, not per-campaign. All GTM skills reference this path.

## Modes

### Mode 1: Create

When no context file exists yet. Walk through each section one at a time.

Ask for:

| Section | What to ask |
|---------|------------|
| What We Do | Product one-liner, core value prop, email-safe value prop, key lingo, key numbers |
| Voice | Sender name, tone, hard constraints, banned words |
| ICP | Company size, roles, geographies, why they buy, anti-patterns |
| Win Cases | Past customers, what resonated, concrete outcomes |
| Proof Library | PS sentences ready to paste, mapped to audience + hypothesis |
| Campaign History | Past campaigns (empty on first run) |
| Active Hypotheses | Current working hypotheses (populate from hypothesis-building) |
| DNC | Any domains to exclude |

Write to `beton/gtm-outbound/context/beton_context.md` using the schema in [references/context-schema.md](references/context-schema.md).

**Key rules:**
- **Email-safe value prop** — version without jargon or banned words, used in prompt templates
- **Proof Library** — every proof point must trace back to a real win case. Write the full sentence as it appears in the email (including "PS.")

### Mode 2: Update

When context file exists and user wants to add/modify a section.

1. Read existing context file
2. Ask what to update (win case, campaign result, ICP change, new DNC entries, proof points)
3. **Append, never overwrite** — add new rows to tables, new bullets to lists
4. Save

### Mode 3: Call Recording Capture

When user pastes a transcript or meeting notes.

1. Read the transcript
2. Extract and categorize signals:
   - **ICP signals** — role, company size, what they care about
   - **Win case data** — pain confirmed, what resonated, workflow they described
   - **Proof point candidates** — specific results or quotes → Proof Library entries
   - **DNC signals** — companies mentioned as off-limits
   - **Hypothesis validation** — which hypotheses confirmed or refuted
   - **Voice feedback** — reactions to tone, language, positioning
3. Present extracted signals to user for confirmation
4. Update context file with confirmed signals

### Mode 4: Feedback Loop

When importing campaign results from seqd or manual tracking.

1. Read campaign results (CSV, pasted data, or sequencer export)
2. Extract: campaign name, vertical, list size, open rate, reply rate, top-performing hypotheses
3. Add a new row to `## Campaign History`
4. Update `## Active Hypotheses` — promote/retire based on reply rates
5. Update `## Proof Library` if new win cases surfaced

## Cross-Skill References

This file is consumed by:
- `hypothesis-building` — reads ICP + win cases to generate pain hypotheses
- `email-prompt-building` — reads Voice, What We Do, Proof Library, Active Hypotheses
- `list-segmentation` — reads hypotheses for tiering logic
- `campaign-sending` — reads DNC list before upload

## Reference

Full schema with all sections and field definitions: [references/context-schema.md](references/context-schema.md)
