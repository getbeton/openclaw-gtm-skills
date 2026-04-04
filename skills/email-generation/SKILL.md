---
name: email-generation
description: >
  Generate cold outreach emails from a contact CSV and a self-contained prompt
  template built by email-prompt-building. Campaign-agnostic — no hardcoded
  product or voice. The prompt template contains all voice rules, research data,
  value prop, proof points, and personalization rules. This skill just runs it
  per row. Triggers on: "generate emails", "email generation", "run emails",
  "create emails", "write emails for campaign", "generate outreach".
---

# Email Generation

Runner skill. Prompt template + contact CSV in → emails CSV out. All strategic reasoning was done by `email-prompt-building` and baked into the prompt. This skill does not read the context file, hypothesis set, or research files.

## Pipeline Position

```
email-prompt-building → [email-generation] → email-response-simulation (Tier 1)
        ↑                       ↑                        ↓
  prompt template          contact CSV              campaign-sending
```

## Storage: Google Drive + Google Sheets (canonical)

All follow-up sequences and outbound email drafts live in Google Drive, NOT on the VPS filesystem.

### Folder structure
All campaigns live inside: https://drive.google.com/drive/folders/1ouTHgkWr_nPxtCNXuLHz8q_6gIJCQDCJ (**Outbound Sequences** root)

**Naming convention:** All subfolders must start with `YYYY-MM-DD — Campaign Name`

**Current campaigns:**
| Folder | ID | Sheet ID | Description |
|---|---|---|---|
| `2026-03-31 — Attio Pipeline Follow-ups` | `1LcX4MaffZVyaoFmqQzQ1iB82h_1G1m3A` | `1FIUN0YodSinqZAXXUygp5qWBx7wjhH_rpjZzZt7ue3c` | Re-engagement for Approaching/Qualified deals |
| `2026-03-31 — 6sense+PostHog Cold Campaign` | `1IKZVwrFq7LwPLG-qTYnjeqkYtZvQXI70` | `1lWO_ahITUyauGa4ObKbsW3Xw0EfG57NVqpTzwW7GHHE` | Cold outreach to 6sense+PostHog cohort (122 companies) |

**Sheet format: ROW PER STEP** (grouped visually on Company+Contact+Stage)

**Pipeline follow-ups columns:** Company, Contact Name, Contact Email, Attio Stage, Step #, Send Day, Subject, Body (preview), Status, Drive Doc, Notes
**Cold campaign columns:** Company, Domain, Contact Name, Contact Email, Fit Score, Vertical, Step #, Send Day, Subject, Body (preview), Status, Drive Doc, Notes

Each sequence = 2 rows (Step 1 / Day 0, Step 2 / Day 5). Body preview = first 100 chars + "..."

Shared with: v@getbeton.ai, a@getbeton.ai (editor)

### When writing follow-up sequences (mandatory, in order):

1. **Save locally** — write emails as Markdown to `content/followup-sequences/{company_slug}_emails.md`
2. **Upload to Drive** — upload the `.md` file as a Google Doc to the folder above (see snippet below)
3. **Add row to Sheet** — append a row to the draft table (see snippet below)
4. **Notify Vlad** — include the Drive doc link in the sessions_send message so he can click directly to the doc

### Token refresh for Drive/Sheets:
```python
import json, requests
with open('/home/nadyyym/.openclaw/workspace/integrations/google_personal_token.json') as f:
    ptok = json.load(f)
with open('/home/nadyyym/.openclaw/workspace/integrations/google_oauth.json') as f:
    oauth = json.load(f)['installed']
r = requests.post('https://oauth2.googleapis.com/token', data={
    'client_id': oauth['client_id'], 'client_secret': oauth['client_secret'],
    'refresh_token': ptok.get('refresh_token'), 'grant_type': 'refresh_token'
})
access_token = r.json()['access_token']
```

### Upload markdown as Google Doc:
```python
meta = json.dumps({'name': f'{company_name} — Follow-up Sequence',
                   'mimeType': 'application/vnd.google-apps.document',
                   'parents': ['1ouTHgkWr_nPxtCNXuLHz8q_6gIJCQDCJ']})
with open(md_path, 'rb') as f:
    content = f.read()
resp = requests.post(
    'https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart',
    headers={'Authorization': f'Bearer {access_token}'},
    files={'metadata': ('metadata', meta, 'application/json; charset=UTF-8'),
           'file': ('file.md', content, 'text/plain')}
)
doc_id = resp.json()['id']
doc_url = f'https://docs.google.com/document/d/{doc_id}'
```

### Add row to draft table:
```python
r_data = requests.get(
    'https://sheets.googleapis.com/v4/spreadsheets/19sfeK68nSCn9--bUZvPruAz_hS2HDyB37sW_8osM3AQ/values/Sheet1',
    headers={'Authorization': f'Bearer {access_token}'})
last_row = len(r_data.json().get('values', [])) + 1

requests.put(
    f'https://sheets.googleapis.com/v4/spreadsheets/19sfeK68nSCn9--bUZvPruAz_hS2HDyB37sW_8osM3AQ/values/Sheet1!A{last_row}',
    headers={'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/json'},
    params={'valueInputOption': 'RAW'},
    json={'values': [[company_name, contact_name, contact_email, attio_stage, doc_url, email1_subject, email2_subject, 'Draft', '']]}
)
```

### Notify Vlad after each company:
Include the Drive doc URL in the reply/sessions_send message:
```
✅ {Company Name} done — [open in Drive]({doc_url})
```
```

## Inputs

| Input | Source | Required |
|-------|--------|----------|
| Prompt template | `beton/gtm-outbound/prompts/{vertical-slug}/en_first_email.md` | yes |
| Contact CSV | `beton/gtm-outbound/csv/input/{campaign-slug}/contacts.csv` | yes |

That's it. No other files.

## Contact CSV Required Columns

Check the prompt template's "Enrichment data fields" section for the exact column names expected. Minimum required:
- `first_name`, `last_name`, `company_name`, `job_title`, `email`

Plus any enrichment columns referenced in the prompt.

## Before Running

### Name sanitization

Run `scripts/sanitize-names.py` before generating:

```bash
python3 ~/.openclaw/workspace/skills/email-generation/scripts/sanitize-names.py \
  contacts.csv contacts_sanitized.csv
```

Strips titles (Dr, Prof), removes junk rows (single char names, emoji, N/A, Test), fixes all-caps. Review removed rows before proceeding — do not generate emails for invalid names.

## Running the Generator

### < 30 contacts: in-chat

1. Read the prompt template
2. Read the contact CSV
3. For each row: apply prompt + row data → generate email JSON
4. Save to output CSV

### 30+ contacts: batch mode

Process in batches of 10-20 rows:
1. Load prompt template + CSV
2. Process batch → accumulate results
3. Save to `beton/gtm-outbound/csv/output/{campaign-slug}/emails.csv`

## Writing Rules (mandatory — applies to all outbound sequences)

### Style
- **No caps anywhere** — subject lines, greetings, body text. **Exception: given names and company names ARE capitalized** (e.g. "Aerospike," not "aerospike,")
- **Half the words** — if your draft is 100 words, cut to 50. Every sentence earns its place.
- **No trailing periods** — drop the period at the end of the last sentence in each paragraph. Keep question marks.
- **Goal = meeting, not pitch** — don't explain Beton. Share one signal idea they're probably missing, hint that Beton helps with it, ask for a call.
- **Single thread** — all emails in a sequence use the same subject line (no "re:" prefix on follow-ups)

### Content (per email, always use all three sources)
1. **Research** — one specific thing from Firecrawl scrape or company knowledge (metric, product feature, GTM motion, pricing model)
2. **Value hypothesis** — one hypothesis from Supabase `company_hypotheses` table for this company, framed as a question or observation
3. **CRM history / Contact Profile** — check Attio notes and the recipient's LinkedIn profile.

### Profile-Driven Personalization (How to Sharpen Emails)

Once you have the target's name and title, use their profile to sharpen the copy:

**1. Geo/Region-Specific Hooks**
If they own a specific region (e.g., "VP Sales APJ"), use regional examples:
- *Instead of:* "when they add a cluster..."
- *Use:* "when a Singapore customer adds a cluster, they usually launch in India/Japan next..."

**2. Technical Precision by Role**
Match the vocabulary to their background:
- *CTO/Engineering:* Use technical constraints ("P99 latency", "query complexity", "retry storms")
- *Sales Leader:* Use pipeline language ("creates the deal," "reps reach out before they ask for a quote")

**3. The 6 Profile Intel Sources Checklist:**
Before writing, check these 6 things (via LinkedIn/Apollo):
1. **Job tenure** → validate they own the problem (new = wants quick wins; tenured = knows the pain)
2. **Skills/endorsements** → match their language (endorsed for 'Data Driven' = use metrics)
3. **Recent posts** → hook into current priorities (e.g., "saw your post on AI scaling...")
4. **Education/Background** → adjust technical depth (Engineering degree vs. MBA)
5. **Recommendations** → borrow social proof ("teams you've built probably see this...")
6. **Shared connections** → warm intro angle if available

### Signal Discovery Angle (mandatory positioning)

Beton's value prop is **not** "we track signals for you" — it's **"we discover which signals actually predict revenue/churn."**

In emails, emphasize:
- "you can track this manually, but finding which signals actually predict [revenue/churn] takes months of analysis"
- "you could build a dashboard for this, but discovering which patterns actually drive [expansion/retention] is what unlocks [revenue growth/churn reduction]"
- "Beton discovers the patterns automatically and validates them against your [pipeline/support ticket] history"

**Why this matters:**
Every company with PostHog can see events. The hard part is figuring out which events are predictive vs. noise. That's the unlock.

**Where to use:**
- Email 1: acknowledge they can track it, but discovery is the unlock
- Email 4: contrast manual tracking vs. automated pattern discovery

### Honesty rules (mandatory)
- **Do not lie or exaggerate where claims can be cross-referenced**
- Never claim "we've seen this with 5-6 companies" if you haven't
- Never say "your competitors are doing X" unless you can verify it
- Use "assuming" / "likely" / "probably" when making educated guesses
- If operationalizing a signal, be specific about how Beton wires it up ("pulls from PostHog events, scores in Beton, routes to CRM")

### Email structure

**Email 1 (Day 0):** Observation + Signal 1
```
[Company Name] — [one sentence: specific signal they're missing, grounded in research]

[one sentence: why that signal matters or what it unlocks]

[varied CTA — see CTA list below]
```

**Email 2 (Day 2):** Standard bump format
```
did you have time to look into this?
```

**Email 3 (Day 5):** Signal 1 operationalization
```
the [signal name] signal helps you [outcome]: Beton [specific mechanism], and your team [action they take]

[varied CTA]
```

**Email 4 (Day 7):** Signal 2 introduction
```
second signal we're seeing: [new pattern from research]

Beton can watch for both patterns [simultaneously / in parallel / etc.]

[varied CTA]
```

**Email 5 (Day 9):** Farewell, no pressure
```
i know you're busy. if [these signals / this] resonate[s], i'm here. if not, totally get it
```

### Call-to-Action Variations (mandatory: never repeat the same CTA twice in one sequence)

**Direct ask:**
- worth a call?
- can we talk through this?
- should we walk through this?
- want to dig into this?

**Demo/show:**
- want to see this in action?
- can I show you how this works?
- interested in seeing the setup?
- worth a quick demo?

**Low-commitment:**
- worth 15 minutes?
- worth exploring?
- curious to hear your take?
- does this resonate?

**Confidence/challenge:**
- want to validate this against your data?
- bet this shows up in your PostHog already — worth checking?
- can we prove this works for your use case?

**Rule:** Pick 3 different CTA styles across emails 1, 3, 4. Never use the same phrasing twice in a single sequence.

**Formatting rules:**
- Given names and company names always capitalized (e.g. "Aerospike," "Paula,")
- Use `->` not `→` (people don't type arrows from their phone)
- **Remove "Sent from Superhuman" from email body** — handle this via mailbox signature settings instead
- No trailing periods at end of paragraphs (keep question marks)

**Email 1:** ≤80 words. Signal-first, company name at start.
**Email 2:** ≤50 words. Standard bump format ("did you have time to look into this?").
**Emails 3-5:** ≤50 words each.

### Subject line rules
- no caps (not even first letter)
- **do NOT use company names** — use the subject line to name the problem or direction of change
- write it so any reader immediately understands what the email is about, even without knowing what Beton does
- frame as a problem they're facing or a result they want, not a product feature
- good examples: `find new warm leads`, `spot which users are ready to buy`, `missing expansion signals`, `know who's ready before they ask`
- bad examples: `n8n cloud signals` ❌ (jargon), `saleor api usage → expansion` ❌ (company name + jargon), `re: beton` ❌

## Quality Checks

After generating:
- [ ] Subject is lowercase, no Beton mention
- [ ] Email is ≤80 words
- [ ] One specific research fact used
- [ ] One value hypothesis referenced (from company_hypotheses in Supabase or general knowledge)
- [ ] Attio CRM history checked and referenced if exists
- [ ] No "hope this finds you well", "I wanted to reach out", "circling back", "as per", "touch base"
- [ ] Goal is meeting, not explaining product
- [ ] No markdown formatting in email body (no bold, bullets, headers)

## Tier-Aware Behavior

When contact CSV includes `tier` column (from list-segmentation):

- **Tier 1**: Generate individually, full attention to enrichment. Route to `email-response-simulation` before sending.
- **Tier 2**: Group by `hypothesis_number`, generate in batches per group, spot-check 2-3 per group.
- **Tier 3**: Skip. Do not generate. Route back to re-enrichment.

## Refinement Loop

If emails need fixing:
1. User identifies bad emails and explains what to change
2. **Update the prompt template** — fix should be systemic, not one-off
3. Re-run generator with updated prompt
4. Repeat until satisfied

Track prompt changes so user can see the evolution.

## Output Format

JSON per row (as specified in prompt template's "Output format" section):
```json
{
  "recipient_name": "...",
  "recipient_company": "...",
  "subject": "...",
  "greeting": "Hey {FirstName},",
  "p1": "...",
  "p2": "...",
  "p3": "...",
  "p4": "..."
}
```

Saved as CSV with one column per JSON key.
