# gtm-outreach

**Description:** Draft a personalized email sequence for a specific company+contact+experiment, grounded in research and signals, using the outreach framework. Queues for Vlad review.

## When to use

After a company is `scored` (T1 preferred), a contact is identified, and an experiment is active. Triggered by pipeline when `experiment_id` is provided, or manually for individual companies.

## Inputs

- `company_id`: UUID
- `contact_id`: UUID
- `experiment_id`: UUID
- Outreach framework: `/home/nadyyym/.openclaw/workspace/research/outreach-framework.md`

## Steps

### 1. Load all context

**Company research:**
```sql
SELECT
  c.id, c.domain, c.name, c.research_status,
  cc.b2b, cc.saas, cc.gtm_motion, cc.vertical, cc.description, cc.sells_to,
  ct.crm, ct.sales_engagement_tool, ct.analytics,
  cso.sales_headcount, cso.revops_headcount, cso.hiring_signal,
  seg.name AS segment_name, seg.core_pain_hypothesis, seg.value_prop_angle,
  seg.target_personas, seg.icp_definition,
  cs.fit_score, cs.fit_tier, cs.fit_reasoning
FROM companies c
LEFT JOIN company_classification cc ON cc.company_id = c.id
LEFT JOIN company_tech_stack ct ON ct.company_id = c.id
LEFT JOIN company_sales_org cso ON cso.company_id = c.id
LEFT JOIN company_segments cs ON cs.company_id = c.id
LEFT JOIN segments seg ON seg.id = cs.segment_id
WHERE c.id = $1
ORDER BY cs.fit_score DESC
LIMIT 1
```

**Contact:**
```sql
SELECT * FROM contacts WHERE id = $1
```

**Signals (unused only, ordered by urgency):**
```sql
SELECT * FROM signals
WHERE company_id = $1 AND used_in_outreach = false
ORDER BY urgency_score DESC
LIMIT 5
```

**Experiment config:**
```sql
SELECT id, name, description, segment_id,
       -- sequence_config defines: length, angles per step, timing
       (SELECT sequence_config FROM outreach WHERE experiment_id = experiments.id LIMIT 1) AS sequence_config
FROM experiments
WHERE id = $1
```

If `sequence_config` is null on the experiment, use this default:
```json
{
  "length": 3,
  "timing": [0, 3, 7],
  "steps": [
    {"step": 1, "type": "email", "angle": "signal-led"},
    {"step": 2, "type": "email", "angle": "value-prop"},
    {"step": 3, "type": "email", "angle": "breakup"}
  ]
}
```

### 2. Load outreach framework

Read `/home/nadyyym/.openclaw/workspace/research/outreach-framework.md` in full. This defines tone, structure, length constraints, and angle guidelines. Follow it strictly.

### 3. Draft sequence with Claude

Build a prompt with all loaded context and call Claude:

```
You are drafting a cold outreach sequence for Beton (getbeton.ai), a revenue intelligence platform.

COMPANY: {company_name} ({domain})
Classification: {classification_summary}
Segment pain hypothesis: {core_pain_hypothesis}
Value prop angle: {value_prop_angle}

CONTACT: {first_name} {last_name}, {title} at {company_name}
Persona type: {persona_type}

TOP SIGNALS:
{signals_list}

EXPERIMENT: {experiment_name}
This experiment tests: {experiment_description}

SEQUENCE CONFIG:
{sequence_config_json}

OUTREACH FRAMEWORK:
{outreach_framework_content}

Draft {length} emails following the sequence config. For each email:
- Subject line: <15 words, no clickbait
- Body: follow framework guidelines for length and tone
- Use the signal as the opening hook where relevant (step 1 especially)
- Personalize to the contact's role and seniority
- Reference specific things from the company research (not generic)

Output ONLY valid JSON array:
[
  {
    "step": 1,
    "day": 0,
    "type": "email",
    "angle": "signal-led",
    "subject": "...",
    "body": "...",
    "personalization_notes": "what was customized and why",
    "status": "draft"
  },
  ...
]
```

### 4. Validate output

- Confirm JSON parses correctly
- Confirm `length` matches `sequence_config.length`
- Check each step has: step, day, type, angle, subject, body, status
- If invalid: retry Claude once. If still fails: log error, skip this outreach row.

### 5. Store in Supabase

```sql
INSERT INTO outreach (experiment_id, company_id, contact_id, sequence, sequence_config, review_status)
VALUES ($1, $2, $3, $4, $5, 'draft')
RETURNING id
```

Mark signals as used:
```sql
UPDATE signals
SET used_in_outreach = true
WHERE id = ANY($1)
```

### 6. Notify Vlad

Send a Telegram message (via OpenClaw notification):

```
📧 New outreach draft ready for review

Company: {company_name} ({domain}) — {fit_tier}
Contact: {first_name} {last_name}, {title}
Experiment: {experiment_name}
Sequence: {length} emails

Review and approve in Supabase outreach table (id: {outreach_id})
Or reply with: /approve {outreach_id} or /reject {outreach_id}
```

## Output

```json
{
  "outreach_id": "uuid",
  "company_id": "uuid",
  "contact_id": "uuid",
  "experiment_id": "uuid",
  "review_status": "draft",
  "sequence_length": 3,
  "signals_used": ["uuid1", "uuid2"]
}
```

## Notes

- Never enroll in seqd automatically — always require Vlad approval first
- Don't draft outreach for `pass` or `T3` companies without explicit override
- If contact has no email: draft anyway, note `email_required: true` in sequence
- Subject lines must be unique across the sequence (no repeats)
- Framework overrides everything — if framework says max 5 sentences, enforce it

## Dependencies

- Claude (via OpenClaw AI tool)
- Supabase client (`scripts/supabase_client.py`)
- Outreach framework file at `research/outreach-framework.md` (must exist)
- OpenClaw Telegram notification (built-in)
- Skills 2-5 must have run (company needs research + signals)
