# gtm-send

**Description:** Send outreach campaigns via Apollo.io (primary). Includes pre-send Haiku validation to ensure company data matches sequence content.

## When to use

After `gtm-outreach` has generated sequences and stored them in Supabase. Only send to `scored` companies with complete contact data.

## How it works

### 1. Load Campaign Data
- Fetch campaign from Supabase (by campaign_id or experiment_id)
- Load sequence from `gtm-outreach` results
- Load company research, org data, signals, contacts

### 2. Pre-Send Validation (Haiku)
Before sending, run a quick sanity check:
- Does the sequence reference accurate company data?
- Are the signals mentioned still relevant (not stale)?
- Is personalization grounded in real research?
- Are contact names/titles correct?

**If validation fails:** Flag for manual review, don't send.

### 3. Send via Apollo.io (primary)

Apollo.io sequences use a **custom dynamic variable** approach for fully personalized emails:

#### How it works
1. **Custom fields on contacts** hold the actual email content (subject + body per step)
2. **Sequence templates** in Apollo reference those fields: `{{step1_subject}}` / `{{step1_body}}`
3. **API creates/updates contacts** with pre-written content in custom fields
4. **API adds contacts to sequences** with mailbox routing
5. Apollo handles scheduling, daily limits, bounce tracking, OOO detection natively

#### Custom field IDs (created 2026-04-07)
```
step1_subject: 69d525488330dd00195f800b  (string)
step1_body:    69d525619054a300110ceb0f  (textarea)
step2_subject: 69d525669054a300110ceb46  (string)
step2_body:    69d52568581c6d000d43f69d  (textarea)
step3_subject: 69d5256a581c6d000d43f6f4  (string)
step3_body:    69d5256b1c86c20011f94dcd  (textarea)
step4_subject: 69d5256d581c6d000d43f719  (string)
step4_body:    69d5256f4d4f720015f6e404  (textarea)
step5_subject: 69d52571e45bd20015b6e896  (string)
step5_body:    69d525724d4f720015f6e421  (textarea)
```

#### Apollo API key facts (learned 2026-04-07/08)
- **Cannot create sequences via API** — sequences must be created in Apollo UI
- **Can create/update contacts** with custom fields: `POST /api/v1/contacts`
- **Can add contacts to sequences**: `POST /api/v1/emailer_campaigns/{id}/add_contact_ids`
- **Can search contacts**: `POST /api/v1/contacts/search` (unreliable — prefer `people/match`)
- **People search**: `POST /api/v1/mixed_people/api_search` (NOT `/mixed_people/search` — deprecated, returns 422)
- **Cannot activate sequences via API** — must be done in UI (returns 404)
- **Cloudflare blocks Python urllib** — must set `User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)` header
- **Rate limits**: plan-dependent, watch for 429 responses, use exponential backoff
- **Re-adding a contact to a sequence** updates the mailbox assignment (doesn't duplicate)
- **Waterfall enrichment via API**: `POST /api/v1/people/match` with `run_waterfall_email: true`
- **Email validation**: connect LeadMagic API key in Apollo Settings > Waterfall for email verification

#### Connected mailboxes
```
v@getbeton.info:              6914278de36ea5001911cf8e  (60/day)
vlad@getbeton.info:           68f4d6c21e0ee00015cc92e8  (60/day)
vlad.nadymov@getbeton.info:   6914298ff4b83f000df5e68c  (60/day)
```
Total capacity: 180 emails/day across 3 mailboxes.

#### HTML formatting
- Use `<br>` tags for line breaks in email bodies — Apollo renders them correctly in sent emails
- textarea custom fields have 2000 char limit (plenty for 50-80 word emails)

#### Enrollment script
`scripts/apollo_enroll.py` — universal enrollment script with workstream + group support:
```bash
python3 scripts/apollo_enroll.py --workstream ws1 --test 5       # test 5 contacts
python3 scripts/apollo_enroll.py --workstream ws1 --dry-run      # preview without API calls
python3 scripts/apollo_enroll.py --workstream ws1                 # full enrollment
python3 scripts/apollo_enroll.py --workstream ws2 --group 3step  # only 3-step group
python3 scripts/apollo_enroll.py --workstream ws3 --group sales  # only sales thread
```

#### Sales leader enrichment script
`scripts/find_sales_leaders.py` — finds VP/Director/Head of Sales via Apollo search + enrich:
- Uses `mixed_people/api_search` to find candidates by title + domain
- Enriches via `people/match` to get verified email
- Falls back to broader title search if specific titles not found
- Outputs to `campaign-big-b2b/ws3_sales_leaders.json`

#### Sequence setup in Apollo UI
Each sequence uses `{{stepN_subject}}` and `{{stepN_body}}` dynamic variables:
1. Create sequence in Apollo UI
2. Add auto-email steps with `{{step1_subject}}` as subject, `{{step1_body}}` as body
3. Set delays between steps (standard: +1 bday/+2d/+3d/+2d)
4. Enable: mark_finished_if_reply, mark_paused_if_ooo
5. Activate in UI (API cannot activate)

### seqd (deprecated — do not use)

**WARNING:** seqd has a critical bug — resuming paused sequences fires all steps at once instead of respecting delay_days. This caused a blast incident on 2026-04-07 (355 sequences, ~1,775 emails in 22 minutes). Do not use seqd for production outreach. Leave existing seqd sequences as-is; do not re-authenticate mailboxes.

### 4. Track Delivery
- Store send record in `campaign_sends` table
- Track: sent_at, sender (seqd/apollo), status (sent/failed)
- Link back to campaign, sequence, contact

## Pre-Send Validation Prompt

```
You are validating an outbound email sequence before send.

Company data (from Supabase):
- Name: {company_name}
- Domain: {domain}
- Vertical: {vertical}
- GTM Motion: {gtm_motion}
- Pricing Model: {pricing_model}
- Total Headcount: {total_headcount}
- Sales Headcount: {sales_headcount}
- Open Sales Roles: {open_sales_roles}
- Top Signals: {signals}

Sequence (Email 1):
{email_1_body}

Contact:
- Name: {contact_name}
- Title: {contact_title}

Validation checks:
1. Does the email reference accurate company facts? (e.g., if it mentions "500+ employees" but actual is 50, FLAG)
2. Are signals mentioned still relevant? (e.g., "hiring" but open_sales_roles = 0, FLAG)
3. Is personalization grounded in real data? (e.g., mentions "payment gateway" but vertical is not fintech, FLAG)
4. Is the contact name/title correct? (e.g., says "Sarah" but contact is "John", FLAG)

Reply with:
PASS — sequence matches company data, safe to send
FAIL — {reason} — do not send, flag for manual review
```

## Usage

### Send a single campaign
```bash
python3 scripts/send_campaign.py --campaign-id <uuid> --dry-run
```

### Send all ready campaigns
```bash
python3 scripts/send_campaign.py --status ready --limit 10 --sender apollo
```

### Validate without sending
```bash
python3 scripts/send_campaign.py --campaign-id <uuid> --validate-only
```

## Sender Configuration

### Apollo.io (primary)
Uses Apollo key from `integrations/apollo.json`:
```json
{
  "api_key": "<your-apollo-api-key>"
}
```

### Testing protocol (mandatory before bulk sends)
After the seqd blast incident, ALL bulk enrollments must follow this protocol:

**Phase A:** Enroll 1 contact → verify email preview in Apollo UI
**Phase B:** Enroll 5 contacts → activate → check for bounces/spam/timing issues
**Phase C:** Wait 24h → if clean, bulk enroll remaining
**Never:** Bulk-activate paused sequences without Phase A-B testing first

## Database Schema

### campaign_sends table
```sql
CREATE TABLE campaign_sends (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  campaign_id UUID REFERENCES campaigns(id),
  sequence_id UUID REFERENCES sequences(id),
  contact_id UUID REFERENCES contacts(id),
  company_id UUID REFERENCES companies(id),
  
  sender TEXT NOT NULL, -- 'apollo' (seqd deprecated)
  status TEXT NOT NULL, -- 'sent' | 'failed' | 'validated' | 'flagged'
  validation_result JSONB, -- {pass: true/false, reason: '...', model: 'haiku'}
  
  sent_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

## Error Handling

**Validation fails:** Store in `campaign_sends` with `status='flagged'`, notify via Telegram

**Apollo fails:** Store error, retry once after 5min (watch for Cloudflare 1010 errors — needs User-Agent header)

**Contact email invalid:** Skip contact, log error

## Monitoring

Check send status:
```bash
python3 scripts/send_campaign.py --status sent --since 2026-04-01
```

View flagged sequences:
```bash
python3 scripts/send_campaign.py --status flagged
```

---

**Safety first:** Always validate before send. Better to flag 5% as false positives than send 1% of bad sequences.
