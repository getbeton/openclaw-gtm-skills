# gtm-send

**Description:** Send outreach campaigns via seqd (primary) or Apollo.io (fallback). Includes pre-send Haiku validation to ensure company data matches sequence content.

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

### 3. Choose Sender
**Primary:** seqd (if available and mailbox connected)
- Creates sequence via seqd API
- Maps contacts to sequence steps
- Schedules sends

**Fallback:** Apollo.io Sender
- Uses Apollo's sequence API
- Requires Apollo credits (1 per email sent)
- Less flexible than seqd but works without mailbox setup

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
python3 scripts/send_campaign.py --status ready --limit 10 --sender seqd
```

### Validate without sending
```bash
python3 scripts/send_campaign.py --campaign-id <uuid> --validate-only
```

## Sender Configuration

### seqd Setup
1. Ensure seqd is running at `seqd.getbeton.org`
2. Link mailboxes (v@getbeton.ai, vlad@getbeton.info, etc.)
3. Store API token in `integrations/seqd.json`:
```json
{
  "api_url": "https://seqd.getbeton.org/api",
  "api_token": "YOUR_TOKEN"
}
```

### Apollo.io Fallback
Uses existing Apollo key from `integrations/apollo.json`. No extra setup needed.

## Database Schema

### campaign_sends table
```sql
CREATE TABLE campaign_sends (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  campaign_id UUID REFERENCES campaigns(id),
  sequence_id UUID REFERENCES sequences(id),
  contact_id UUID REFERENCES contacts(id),
  company_id UUID REFERENCES companies(id),
  
  sender TEXT NOT NULL, -- 'seqd' | 'apollo'
  status TEXT NOT NULL, -- 'sent' | 'failed' | 'validated' | 'flagged'
  validation_result JSONB, -- {pass: true/false, reason: '...', model: 'haiku'}
  
  sent_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

## Error Handling

**Validation fails:** Store in `campaign_sends` with `status='flagged'`, notify via Telegram

**seqd unavailable:** Fallback to Apollo automatically

**Apollo fails:** Store error, retry once after 5min

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
