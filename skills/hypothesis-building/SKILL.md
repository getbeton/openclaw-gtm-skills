---
name: hypothesis-building
description: >
  Generate testable pain hypotheses from the company context file (ICP, win
  cases, product knowledge) and user input. Fast, no API keys needed — pure
  reasoning. Outputs a hypothesis set with search angles that directly guide
  list-building and segmentation. Sits between context-building and
  email-prompt-building in the Beton GTM pipeline.
  Triggers on: "build hypotheses", "hypothesis set", "pain hypotheses",
  "define hypotheses", "what pain points", "campaign angles", "search angles",
  "refine hypotheses", "what verticals to target", "run on scored companies",
  "generate hypotheses from data".
---

# Hypothesis Building

Two modes: **deductive** (reason from ICP + context file) or **data-grounded** (induce from real company research logs). Use data-grounded whenever research logs exist — it produces more specific, personalization-ready hypotheses.

## Pipeline Position

```
context-building → [hypothesis-building] → email-prompt-building → email-generation
                         ↓
                   list-segmentation (tiering by hypothesis fit)
```

## Paths

```
Context file:   beton/gtm-outbound/context/beton_context.md
Research logs:  plugins/beton-gtm/logs/research/{domain}.json
Output:         beton/gtm-outbound/context/{vertical-slug}/hypothesis_set.md
```

---

## Mode A: Deductive (no research logs)

Use when research logs don't exist yet or for a new vertical with no data.

### Step 1: Read context file

Extract from `beton/gtm-outbound/context/beton_context.md`:
- ICP profiles, win cases, product value prop, active hypotheses

### Step 2: Gather vertical context from user

| Question | Why |
|----------|-----|
| What vertical are you targeting? | Defines the slug and scope |
| What do you know about how these companies operate? | Seeds the reasoning |
| Any signals or patterns you've noticed? | Captures practitioner knowledge |

Keep conversational — don't force all questions if user gives rich context upfront.

### Step 3: Extract patterns from win cases

For each win case:
1. **Trigger** — what made them look for a solution?
2. **Workflow gap** — what broke before Beton?
3. **Value delivered** — specific outcome
4. **Transferability** — applies to target vertical?

### Step 4: Draft hypotheses → go to [Output Format](#output-format)

---

## Mode B: Data-Grounded (recommended — use when research logs exist)

Instead of reasoning from ICP alone, sample real classified companies and read their research logs. Hypotheses become inductive — grounded in observed patterns, not assumed ones.

### Step 1: Read context file

Same as Mode A Step 1.

### Step 2: Sample companies from Supabase

Pull 30-50 T1 companies from the target segment. Use the Supabase REST API with the service key from `plugins/beton-gtm/scripts/run_prefilter.py`:

```sql
SELECT c.domain, c.name, cc.vertical, cc.gtm_motion, cc.description,
       cc.evidence, cc.sells_to, cc.business_model,
       cso.hiring_signal, cso.sales_headcount, cso.revops_headcount
FROM companies c
JOIN company_classification cc ON cc.company_id = c.id
JOIN company_segments cs ON cs.company_id = c.id
LEFT JOIN company_sales_org cso ON cso.company_id = c.id
WHERE cs.fit_tier = 'T1'
LIMIT 50
```

Or filter by vertical: add `AND cc.vertical ILIKE '%devtools%'`

### Step 3: Read research logs

For each company, read `plugins/beton-gtm/logs/research/{domain}.json`.

Extract from `raw_content` and `evidence` fields:

| Signal to look for | Why it matters |
|-------------------|----------------|
| GTM language ("self-serve", "sales-assisted", "usage-based", "land and expand") | Reveals their actual sales motion |
| Pricing page structure | Usage-based = expansion revenue driven by product data |
| Stack mentions (PostHog, Snowflake, Segment, CRM) | Shows data maturity and integration gaps |
| Hiring page (AE/SDR/CSM roles vs. RevOps/data) | Reveals where they're building vs. where they're blind |
| Pain language in customer stories | Often mirrors their own internal pain |
| "Contact sales" vs. self-serve signup | Degree of sales involvement |

### Step 4: Identify non-obvious patterns

Look for the gap between what they're doing and what they'd need to do it well.

Common patterns that signal Beton fit:

| Observed pattern | Non-obvious implication |
|-----------------|------------------------|
| Usage-based pricing page | Have product usage data — no system routing it to reps |
| "Sales-assisted" in docs, no RevOps hiring | Founder doing RevOps manually, will break at scale |
| Open AE/CSM roles, no RevOps role | About to hire reps with no prioritization system |
| PostHog in stack, no CRM integration mentioned | Product data siloed from sales motion |
| "Land and expand" in case studies | Expansion playbook in theory, not automated |
| PLG motion + pricing tiers | Upgrade triggers exist but probably unwatched |
| High engineering/product headcount, tiny sales team | Founder-led sales, just starting to build GTM |

Each hypothesis must be grounded in a pattern seen across **≥3 companies** in the sample.

### Step 5: Draft hypotheses → go to [Output Format](#output-format)

---

## Output Format

Save to `beton/gtm-outbound/context/{vertical-slug}/hypothesis_set.md`.

```markdown
## Hypothesis Set: [Vertical] — [date]

_Mode: deductive | data-grounded (N companies sampled)_
_Previous campaign note: [any learnings from past sends — what angle failed, what segment was different]_

### #1 [Short name]

**Description:** [2-3 sentences: the pain, why it exists, why Beton fits]
**Best fit:** [company type within the vertical]
**Search angle:** [Supabase filter or keyword criteria]
**Evidence base:** [N companies showed this pattern] _(data-grounded only)_
**Example companies:** [domain1, domain2, domain3] _(data-grounded only)_
**Personalization hook:** [what to pull from their specific site/content for email opener] _(data-grounded only)_

### #2 [Short name]
...
```

**Quality checks before saving:**
- Specific to a workflow or decision, not a vague trend?
- Can the recipient confirm it from their own experience?
- Connected to a specific Beton capability?
- Search angle concrete enough to drive a list query?
- (data-grounded) Pattern seen in ≥3 real companies?

---

## Review Step

Present the hypothesis set and ask:
- "Do these match your understanding of the vertical?"
- "Any to add, merge, or remove?"
- "Are the personalization hooks specific enough to use in an opener?"

Expect 1-2 rounds. This is interactive.

---

## Refine Mode

When a hypothesis set already exists at the output path:
1. Read the existing set
2. Ask what changed — new campaign results, new data, new vertical knowledge
3. Update, merge, or add hypotheses
4. Preserve hypothesis numbering (downstream references use `#N`)

---

---

## Grand Jury Mode

Spawn a swarm of 7 independent agents. Each generates 5 deductive + 5 inductive hypotheses, then votes on the full pool. Final output is a ranked list by committee vote.

### When to use
Trigger: "grand jury", "swarm hypotheses", "committee vote", "agent swarm"

### Step 1: Build shared context

Pull from Supabase + research logs:
- DB aggregates: GTM distribution, pricing distribution, top verticals, hiring signals
- 15+ PLG/hybrid B2B company log samples (domain, vertical, GTM, pricing, description, evidence)
- Save to `/tmp/swarm_context.json`

### Step 2: Spawn 7 agents

Each agent gets:
- The shared context JSON
- `beton_context.md` (product, ICP, voice rules)
- A unique **persona** (see below) that biases their reasoning lens
- Instructions to generate exactly 5 deductive + 5 inductive hypotheses

**Personas (assign one per agent):**
1. **The Skeptic** — challenges every assumption, focuses on anti-patterns and edge cases
2. **The Founder** — reasons from what a founder building GTM from scratch actually feels
3. **The RevOps Engineer** — thinks in data pipelines, trigger conditions, and CRM schema
4. **The Sales Rep** — focuses on what makes a rep's day easier or harder
5. **The Investor** — looks at structural market gaps and timing
6. **The Customer** — reasons from the buyer's perspective, objections, and switching costs
7. **The Data Scientist** — focuses on what's measurable, signal vs. noise, false positive rates

### Step 3: Each agent generates hypotheses

Each hypothesis must include:
- **ID:** `{agent_id}-D{n}` (deductive) or `{agent_id}-I{n}` (inductive)
- **Title:** short name
- **Type:** deductive | inductive
- **Core claim:** 2 sentences max — the gap and why Beton fits
- **Evidence:** for inductive: cite specific data from the context JSON; for deductive: cite the structural condition
- **Beton capability:** which specific feature addresses this

### Step 4: Voting round

After all 7 agents submit, each agent gets **7 votes** to allocate across the full hypothesis pool (excluding their own). Rules:
- **Agents vote on assumption quality, not novelty or impressiveness** — the question is: how solid is the underlying assumption?
- Agents may not vote for any hypothesis they generated
- Max 2 votes per single hypothesis (prevents pile-ons)
- Must vote for at least 3 different hypotheses
- Agents explain each vote in one sentence

### Step 5: Compile ranked output

Save to: `beton/gtm-outbound/context/grand-jury-hypotheses.md`

Format:
```
## Grand Jury Results — [date]
_7 agents × 10 hypotheses each = 70 total. Voting pool: all 70. Max 49 votes possible per hypothesis._

### Ranked by votes

| Rank | ID | Title | Type | Votes | Proposing agent |
|---|---|---|---|---|---|
| 1 | ... | ... | ... | N | ... |

### Full hypothesis text (in vote order)

#### #1 — [Title] (N votes)
...

### Dissents & minority views
[Any hypotheses with 0 votes but strong reasoning worth preserving]
```

### Implementation notes
- Use `sessions_spawn` with `runtime=subagent` for all 7 agents
- Pass context as task text (inline the JSON, not as a file path)
- Use `sessions_yield` after spawning all 7, wait for completions
- Collect outputs, run voting round as a second spawn wave (each agent reads the full pool)
- Final compilation done in main session

---

## Downstream consumers

- `email-prompt-building` — hypotheses become P1 email angles; personalization hooks become opener templates
- `email-generation` — runs prompt per contact row, pulls personalization hook per company
- `list-segmentation` — search angles filter which companies get which hypothesis

---

## Grand Jury Mode

Trigger phrases: "grand jury", "agent swarm", "committee vote", "spawn agents for hypotheses", "jury mode"

Spawn **7 isolated subagents**, each generating independent hypotheses. Then run a voting round where each agent ranks the full pool. Output: ranked hypothesis list by committee consensus.

### How it works

**Round 1 — Generation (7 agents in parallel):**

Each agent receives:
- The beton_context.md (product, ICP, voice)
- The shared data snapshot (DB aggregates + 15 company log samples)
- Its agent number (1–7) and a unique reasoning angle (see below)

Each agent produces exactly:
- 5 **inductive** hypotheses (backed by patterns in the data)
- 5 **deductive** hypotheses (structural reasoning from ICP)

Reasoning angles per agent (to ensure diversity, not groupthink):
1. **Bottoms-up signals analyst** — focus on hiring patterns and org structure signals
2. **GTM motion expert** — focus on PLG/hybrid motion design gaps
3. **Pricing analyst** — focus on pricing model → revenue signal gaps
4. **Founder psychology** — focus on founder-led sales and the transition moment
5. **Vertical specialist** — focus on vertical-specific structural gaps (devtools, fintech, healthtech)
6. **Contrarian** — challenge conventional wisdom, find non-obvious hypotheses
7. **Customer success lens** — focus on post-sale expansion and CS infrastructure gaps

**Round 2 — Voting (same 7 agents, all hypotheses pooled):**

After all 7 agents finish generating, collect all hypotheses into a numbered pool (up to 70 hypotheses). Send the full pool back to all 7 agents with this instruction:

> "Here are N hypotheses from your committee. You have 7 votes. Distribute them however you like — all 7 on one hypothesis, or spread across many. Vote based on how strong the underlying assumptions are — not on which hypothesis sounds impressive. Vote for hypotheses that are: (1) backed by real data patterns, (2) structurally tight, (3) actionable for Beton specifically, (4) differentiated from each other. **You may not vote for any hypothesis you generated yourself.**"

**Round 3 — Tally and output:**

Tally votes. Output a ranked list with:
- Total votes received
- Which agents voted for it
- Whether it's inductive or deductive
- The hypothesis text
- The generating agent's number

Save to: `beton/gtm-outbound/context/grand-jury-hypotheses.md`

### Implementation

When in grand jury mode, the orchestrating agent (this session):
1. Reads context files and data snapshot
2. Spawns 7 subagents with `sessions_spawn(runtime="subagent", mode="run")`
3. Yields and waits for all 7 to return their hypothesis sets
4. Collects all hypotheses into a numbered pool
5. Spawns 7 more subagents (the voting round) with the full pool
6. Tallies votes and writes the ranked output file
7. Commits to beton submodule

The orchestrator does NOT generate hypotheses itself — it coordinates.
