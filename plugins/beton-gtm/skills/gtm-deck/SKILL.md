# gtm-deck

**Description:** Trigger wrapper that generates a personalized sales deck when a prospect replies positively, using company research from Supabase and the existing sales-deck skill.

## When to use

When a `results` row is created with `outcome = 'replied_positive'`. Can also be triggered manually for any company with complete research data.

## Inputs

- `result_id`: UUID from results table
- OR `company_id` + `contact_id` (for manual trigger)

## Steps

### 1. Load result context

```sql
SELECT r.*, c.domain, c.name, c.classification, c.sales_org, c.tech_stack,
       c.firmographic, c.segment_id, s.name AS segment_name,
       s.value_prop_angle, s.core_pain_hypothesis,
       ct.first_name, ct.last_name, ct.title, ct.email
FROM results r
JOIN companies c ON c.id = r.company_id
LEFT JOIN segments s ON s.id = c.segment_id
LEFT JOIN contacts ct ON ct.id = r.contact_id
WHERE r.id = $1
```

### 2. Check if deck already generated

```sql
SELECT deck_generated, deck_path FROM results WHERE id = $1
```

If `deck_generated = true` and `deck_path IS NOT NULL`: skip (deck already exists). Log and return existing path.

### 3. Prepare deck context

Build the context object that the sales-deck skill expects:

```json
{
  "company_name": "Acme Corp",
  "domain": "acme.com",
  "contact_name": "Sarah Chen",
  "contact_title": "CRO",
  "segment": "mid-market-slg",
  "pain_hypothesis": "...",
  "value_prop_angle": "...",
  "classification": { ... },
  "sales_org": { ... },
  "tech_stack": { ... },
  "firmographic": { ... },
  "reply_content": "..." // their actual reply, for tone calibration
}
```

### 4. Call sales-deck skill

Read the skill at `/home/nadyyym/.openclaw/workspace/skills/sales-deck/SKILL.md` and follow its instructions to generate the deck.

Pass the context above as the company profile. The sales-deck skill will:
- Research the company further if needed (it has its own research steps)
- Generate a Google Slides deck (or markdown, per skill config)
- Return the deck path or URL

### 5. Update results table

```sql
UPDATE results
SET
  deck_generated = true,
  deck_path = $1,
  notes = COALESCE(notes, '') || ' Deck generated: ' || $1
WHERE id = $2
```

### 6. Notify Vlad

```
🎯 Sales deck ready for {company_name}

Contact: {contact_name}, {contact_title}
Reply: "{reply_excerpt}"

Deck: {deck_path}
```

## Output

```json
{
  "result_id": "uuid",
  "company_id": "uuid",
  "deck_path": "/path/to/deck or URL",
  "deck_generated": true
}
```

## Notes

- This skill is intentionally thin — it delegates to the sales-deck skill for actual deck generation
- The `reply_content` from the results table should influence deck tone (if they mentioned specific pain, lead with that)
- If sales-deck skill fails: log error, set `notes` in results table, notify Vlad manually
- Don't regenerate if deck already exists (idempotent)

## Dependencies

- Sales-deck skill at `/home/nadyyym/.openclaw/workspace/skills/sales-deck/SKILL.md`
- Supabase client (`scripts/supabase_client.py`)
- Companies table with complete research (Skills 2-5 must have run)
- OpenClaw Telegram notification (built-in)
