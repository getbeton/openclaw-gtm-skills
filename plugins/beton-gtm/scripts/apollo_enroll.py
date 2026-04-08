#!/usr/bin/env python3
"""Universal Apollo.io enrollment script.

Usage:
    python3 apollo_enroll.py --workstream ws1 [--test N] [--dry-run]
    python3 apollo_enroll.py --workstream ws2 --group 3step [--test N]

Workstreams:
    ws1  - Bump email for blasted contacts (307 contacts, 1 step)
    ws2  - Continue 6sense x PostHog from step 2 (299 contacts, 4 steps)
    ws3  - Big B2B clean paused (84 contacts, 5 steps)
    ws4  - New 6sense outreach (TBD contacts, 5 steps)
"""

import argparse
import csv
import json
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path

def log(msg=""):
    print(msg, flush=True)

BASE_DIR = Path(__file__).parent.parent
APOLLO_KEY = json.loads((BASE_DIR / "integrations/apollo.json").read_text())["api_key"]
APOLLO_BASE = "https://api.apollo.io/api/v1"

# Custom field IDs (created 2026-04-07)
FIELD_IDS = {
    "step1_subject": "69d525488330dd00195f800b",
    "step1_body": "69d525619054a300110ceb0f",
    "step2_subject": "69d525669054a300110ceb46",
    "step2_body": "69d52568581c6d000d43f69d",
    "step3_subject": "69d5256a581c6d000d43f6f4",
    "step3_body": "69d5256b1c86c20011f94dcd",
    "step4_subject": "69d5256d581c6d000d43f719",
    "step4_body": "69d5256f4d4f720015f6e404",
    "step5_subject": "69d52571e45bd20015b6e896",
    "step5_body": "69d525724d4f720015f6e421",
}

# Mailbox IDs for round-robin
MAILBOXES = [
    "6914278de36ea5001911cf8e",   # v@getbeton.info
    "68f4d6c21e0ee00015cc92e8",   # vlad@getbeton.info
]
# vlad.nadymov@getbeton.info (6914298ff4b83f000df5e68c) excluded — not used in seqd blast

# Sequence IDs
SEQUENCES = {
    "ws1_bump": "69d5ebabf04314002165d3e7",
    "ws2_4step": "69d60c856a2d8800115b500c",
    "ws2_3step": "69d61bc0f0a4f9001900f18c",
    "ws3_strategy": "69d62804ded1b800157e9650",
    "ws3_cs": "69d6288abebd5a0019f561e8",
    "ws3_revenue": "69d6289bbebd5a0019f5640b",
    "ws3_sales": "69d62d89a521f100192afd16",
    # ws4 sequences to be added after creation
}


def apollo_api(method, path, data=None, retries=3):
    url = f"{APOLLO_BASE}{path}"
    body = json.dumps(data).encode() if data else None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, data=body, headers={
                "Content-Type": "application/json",
                "X-Api-Key": APOLLO_KEY,
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
            }, method=method)
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            rb = e.read().decode()
            try:
                rj = json.loads(rb)
            except Exception:
                rj = {"raw": rb[:500]}
            if e.code == 422 and "already exists" in rb.lower():
                return 409, rj
            if e.code == 429:
                wait = 2 ** (attempt + 1)
                log(f"    Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            if attempt < retries - 1 and e.code >= 500:
                time.sleep(2)
                continue
            return e.code, rj
        except Exception as ex:
            if attempt < retries - 1:
                time.sleep(2)
                continue
            return 0, {"error": str(ex)}
    return 0, {"error": "max retries"}


def find_or_create_contact(email, first_name, last_name, company, title="", custom_fields=None):
    """Create or find contact in Apollo, set custom fields."""
    payload = {
        "first_name": first_name,
        "last_name": last_name,
        "email": email,
        "organization_name": company,
        "title": title,
    }
    if custom_fields:
        payload["typed_custom_fields"] = custom_fields

    status, resp = apollo_api("POST", "/contacts", payload)
    contact = resp.get("contact", resp)
    cid = contact.get("id")

    if status in (200, 201) and cid:
        return cid, "created"

    # Contact might already exist — search
    s2, r2 = apollo_api("POST", "/contacts/search", {
        "q_keywords": email,
        "per_page": 1,
    })
    contacts = r2.get("contacts", [])
    if contacts:
        cid = contacts[0]["id"]
        # Update custom fields on existing contact
        if custom_fields:
            apollo_api("PATCH", f"/contacts/{cid}", {
                "typed_custom_fields": custom_fields,
            })
        return cid, "found+updated"

    return None, f"failed: {status} {json.dumps(resp)[:200]}"


def add_to_sequence(contact_id, sequence_id, mailbox_id, status="active"):
    """Add contact to Apollo sequence."""
    s, r = apollo_api("POST", f"/emailer_campaigns/{sequence_id}/add_contact_ids", {
        "emailer_campaign_id": sequence_id,
        "contact_ids": [contact_id],
        "send_email_from_email_account_id": mailbox_id,
        "status": status,
        "sequence_active_in_other_campaigns": True,
    })
    skipped = r.get("skipped_contact_ids", {})
    if contact_id in skipped:
        return False, f"skipped: {skipped[contact_id]}"
    contacts = r.get("contacts", [])
    if contacts:
        return True, "added"
    return False, f"unknown: {s} {json.dumps(r)[:200]}"


def _bump_variant_a(first_name, company):
    return (
        f"hey {first_name},<br><br>"
        f"our email system had a moment yesterday and sent you a week's worth of emails in about five minutes. not ideal<br><br>"
        f"the pitch itself is real though -- if {company} wants to catch churn signals before they show up in dashboards, 20 min to show you how<br><br>"
        f"Vlad"
    )

def _bump_variant_b(first_name, company):
    return (
        f"hey {first_name},<br><br>"
        f"you probably noticed we speed-ran an entire email sequence in your inbox yesterday. our sequencer had other plans<br><br>"
        f"if {company}'s churn detection is a real priority, happy to walk through the signal layer whenever<br><br>"
        f"Vlad"
    )

def _bump_variant_c(first_name, company):
    return (
        f"hey {first_name},<br><br>"
        f"ignore the inbox flood from yesterday -- that was a technical misfire on our side<br><br>"
        f"one real question: does {company} have a way to tell which active users are about to stop being active? 20 min to show you how the signals work<br><br>"
        f"Vlad"
    )

BUMP_VARIANTS = [_bump_variant_a, _bump_variant_b, _bump_variant_c]
VARIANT_NAMES = ["A", "B", "C"]


def run_ws1(test_count=None, dry_run=False):
    """WS1: Bump email for blasted contacts. A/B/C rotated within each company."""
    bump_csv = BASE_DIR / "campaign-big-b2b/bump_data.csv"
    with open(bump_csv) as f:
        rows = list(csv.DictReader(f))

    # Dedupe by email (keep first occurrence)
    seen = set()
    unique_rows = []
    for r in rows:
        if r["email"] not in seen:
            seen.add(r["email"])
            unique_rows.append(r)
    rows = unique_rows

    # Group by company, then assign variants rotating within each company
    from collections import defaultdict
    company_contacts = defaultdict(list)
    for r in rows:
        company_contacts[r["company"]].append(r)

    # Rebuild rows with variant assigned, rotating A/B/C within each company
    rows_with_variant = []
    variant_counts = defaultdict(int)
    for company in sorted(company_contacts):
        contacts = company_contacts[company]
        for idx, r in enumerate(contacts):
            variant_idx = idx % 3
            r["_variant"] = variant_idx
            r["_variant_name"] = VARIANT_NAMES[variant_idx]
            rows_with_variant.append(r)
            variant_counts[VARIANT_NAMES[variant_idx]] += 1

    rows = rows_with_variant
    if test_count:
        rows = rows[:test_count]

    log(f"WS1: Enrolling {len(rows)} contacts for bump email")
    log(f"  Variant distribution: {dict(variant_counts)}")
    seq_id = SEQUENCES["ws1_bump"]

    created = 0
    failed = 0
    failures = []

    for i, row in enumerate(rows):
        email = row["email"]
        first_name = row["first_name"]
        company = row["company"]
        original_subject = row["original_subject"]
        variant_idx = row["_variant"]
        variant_name = row["_variant_name"]

        # Build bump content using the assigned variant
        bump_subject = f"re: {original_subject}"
        bump_body = BUMP_VARIANTS[variant_idx](first_name, company)

        custom_fields = {
            FIELD_IDS["step1_subject"]: bump_subject,
            FIELD_IDS["step1_body"]: bump_body,
        }

        if dry_run:
            log(f"  [{i+1}/{len(rows)}] DRY RUN: {email}")
            log(f"    Subject: {bump_subject}")
            log(f"    Body: {bump_body[:80]}...")
            created += 1
            continue

        # Round-robin mailbox
        mailbox_id = MAILBOXES[i % len(MAILBOXES)]

        # Split name
        parts = first_name.strip().split(None, 1)
        fn = parts[0] if parts else first_name
        ln = parts[1] if len(parts) > 1 else ""

        # Create/find contact with custom fields
        contact_id, cstatus = find_or_create_contact(
            email, fn, ln, company, custom_fields=custom_fields
        )
        if not contact_id:
            log(f"  [{i+1}/{len(rows)}] CONTACT FAILED: {email} — {cstatus}")
            failures.append(f"Contact {email}: {cstatus}")
            failed += 1
            continue

        # Add to sequence
        ok, astatus = add_to_sequence(contact_id, seq_id, mailbox_id, status="active")
        if ok:
            created += 1
            if (i + 1) % 25 == 0 or (i + 1) <= 5:
                log(f"  [{i+1}/{len(rows)}] {email} — {cstatus} → {astatus}")
        else:
            failed += 1
            log(f"  [{i+1}/{len(rows)}] SEQ FAILED: {email} — {astatus}")
            failures.append(f"Seq {email}: {astatus}")

        time.sleep(0.3)

    log(f"\n{'='*60}")
    log(f"WS1 Done: enrolled={created}, failed={failed}")
    if failures:
        log(f"\nFailures ({len(failures)}):")
        for f_ in failures[:20]:
            log(f"  - {f_}")
    return created, failed


def run_ws2(test_count=None, dry_run=False, group_filter=None):
    """WS2: Continue 6sense x PostHog sequences from where seqd left off."""
    data_path = BASE_DIR / "campaign-6sense/ws2_posthog_continue.json"
    with open(data_path) as f:
        all_contacts = json.load(f)

    if test_count:
        all_contacts = all_contacts[:test_count]

    # Split by remaining step count
    need_4 = [c for c in all_contacts if c["remaining_count"] == 4]
    need_3 = [c for c in all_contacts if c["remaining_count"] == 3]
    need_2 = [c for c in all_contacts if c["remaining_count"] == 2]
    need_5 = [c for c in all_contacts if c["remaining_count"] == 5]

    log(f"WS2: {len(all_contacts)} contacts")
    log(f"  4-step (step 1 sent): {len(need_4)}")
    log(f"  3-step (steps 1-2 sent): {len(need_3)}")
    log(f"  2-step (steps 1-3 sent): {len(need_2)}")
    log(f"  5-step (0 sent, defer to WS3/4): {len(need_5)}")

    seq_4 = SEQUENCES["ws2_4step"]
    seq_3 = SEQUENCES.get("ws2_3step")

    # Build work list based on group filter
    groups = []
    if group_filter in (None, "4step"):
        groups.append((need_4, seq_4, "4-step"))
    if group_filter in (None, "3step"):
        if not seq_3:
            log(f"\n  ERROR: ws2_3step sequence ID not set.")
        else:
            groups.append((need_3, seq_3, "3-step"))
    if group_filter in (None, "2step"):
        if not seq_3:
            log(f"\n  ERROR: ws2_3step sequence ID not set (needed for 2-step too).")
        else:
            # 2-step contacts go into 3-step sequence; step 3 left blank
            groups.append((need_2, seq_3, "2-step (into 3-step seq)"))

    if group_filter:
        log(f"\n  Filter: only running '{group_filter}' group")

    created = 0
    failed = 0
    failures = []

    for group, seq_id, label in groups:
        if not group or not seq_id:
            continue

        log(f"\n--- Enrolling {len(group)} contacts into {label} sequence ---")

        for i, contact in enumerate(group):
            email = contact["email"]
            first_name = contact["first_name"]
            last_name = contact.get("last_name", "")
            company = contact["company"]
            title = contact.get("title", "")
            remaining = contact["remaining_steps"]

            # Map remaining steps to step1-N custom fields
            custom_fields = {}
            for idx, step in enumerate(remaining):
                step_num = idx + 1
                subj_key = FIELD_IDS.get(f"step{step_num}_subject")
                body_key = FIELD_IDS.get(f"step{step_num}_body")
                if subj_key and step.get("subject"):
                    custom_fields[subj_key] = step["subject"]
                if body_key and step.get("body"):
                    body = step["body"].replace("\n", "<br>") if "<br>" not in step["body"] else step["body"]
                    custom_fields[body_key] = body

            if dry_run:
                subj_preview = remaining[0].get("subject", "?")[:60] if remaining else "?"
                log(f"  [{i+1}/{len(group)}] DRY RUN: {email} | {len(remaining)} steps | {subj_preview}")
                created += 1
                continue

            mailbox_id = MAILBOXES[i % len(MAILBOXES)]

            contact_id, cstatus = find_or_create_contact(
                email, first_name, last_name, company, title=title,
                custom_fields=custom_fields
            )
            if not contact_id:
                failed += 1
                failures.append(f"Contact {email}: {cstatus}")
                log(f"  [{i+1}/{len(group)}] CONTACT FAILED: {email}")
                continue

            ok, astatus = add_to_sequence(contact_id, seq_id, mailbox_id, status="active")
            if ok:
                created += 1
                if (i+1) % 25 == 0 or (i+1) <= 3:
                    log(f"  [{i+1}/{len(group)}] {email} — {cstatus} → {astatus}")
            else:
                failed += 1
                failures.append(f"Seq {email}: {astatus}")
                log(f"  [{i+1}/{len(group)}] SEQ FAILED: {email} — {astatus}")

            time.sleep(0.3)

    if need_5 and group_filter is None:
        log(f"\n--- Deferred {len(need_5)} contacts with 0 steps sent (will handle in WS3/4) ---")

    log(f"\n{'='*60}")
    log(f"WS2 Done: enrolled={created}, failed={failed}")
    if failures:
        log(f"\nFailures ({len(failures)}):")
        for f_ in failures:
            log(f"  - {f_}")
    return created, failed


def run_ws3(test_count=None, dry_run=False, group_filter=None):
    """WS3: Big B2B clean paused contacts (0 steps sent) + new sales leaders."""
    # Load existing contacts (strategy/cs/revenue threads)
    contacts_path = BASE_DIR / "campaign-big-b2b/ws3_contacts.json"
    with open(contacts_path) as f:
        existing = json.load(f)

    # Load sales leaders
    sales_path = BASE_DIR / "campaign-big-b2b/ws3_sales_leaders.json"
    try:
        with open(sales_path) as f:
            sales_leaders = json.load(f)
    except FileNotFoundError:
        sales_leaders = []

    thread_seq = {
        "strategy": SEQUENCES["ws3_strategy"],
        "cs": SEQUENCES["ws3_cs"],
        "revenue": SEQUENCES["ws3_revenue"],
        "sales": SEQUENCES["ws3_sales"],
    }

    # Build work groups
    groups = {}
    for c in existing:
        thread = c["thread"]
        if group_filter and group_filter != thread:
            continue
        groups.setdefault(thread, []).append(c)

    if (not group_filter or group_filter == "sales") and sales_leaders:
        groups.setdefault("sales", [])
        for sl in sales_leaders:
            if not sl.get("email"):
                continue
            groups["sales"].append({
                "email": sl["email"],
                "first_name": sl["first_name"],
                "last_name": sl["last_name"],
                "company": sl["company"],
                "domain": sl["domain"],
                "title": sl["title"],
                "thread": "sales",
                "steps": [],  # sales leaders need email generation
            })

    total = sum(len(g) for g in groups.values())
    log(f"WS3: {total} contacts across {len(groups)} threads")
    for thread, contacts in sorted(groups.items()):
        seq_id = thread_seq.get(thread, "?")
        log(f"  {thread}: {len(contacts)} contacts → seq {seq_id[:12]}...")

    if group_filter:
        log(f"\n  Filter: only running '{group_filter}' group")

    created = 0
    failed = 0
    skipped = 0
    failures = []

    for thread in ("strategy", "cs", "revenue", "sales"):
        contacts = groups.get(thread, [])
        if not contacts:
            continue
        seq_id = thread_seq.get(thread)
        if not seq_id:
            log(f"\n  ERROR: No sequence ID for thread '{thread}'")
            continue

        log(f"\n--- Enrolling {len(contacts)} into {thread} sequence ---")

        if test_count:
            contacts = contacts[:test_count]

        for i, c in enumerate(contacts):
            email = c["email"]
            first_name = c["first_name"]
            last_name = c.get("last_name", "")
            company = c["company"]
            title = c.get("title", "")
            steps = c.get("steps", [])

            if not steps:
                skipped += 1
                if skipped <= 5:
                    log(f"  [{i+1}/{len(contacts)}] SKIP (no email content): {email}")
                continue

            # Map steps to custom fields
            custom_fields = {}
            for idx, step in enumerate(steps):
                step_num = idx + 1
                subj_key = FIELD_IDS.get(f"step{step_num}_subject")
                body_key = FIELD_IDS.get(f"step{step_num}_body")
                if subj_key and step.get("subject"):
                    custom_fields[subj_key] = step["subject"]
                if body_key and step.get("body"):
                    body = step["body"]
                    if "<br>" not in body:
                        body = body.replace("\n", "<br>")
                    custom_fields[body_key] = body

            if dry_run:
                subj = steps[0].get("subject", "?")[:60] if steps else "?"
                log(f"  [{i+1}/{len(contacts)}] DRY RUN: {email} | {len(steps)} steps | {subj}")
                created += 1
                continue

            mailbox_id = MAILBOXES[i % len(MAILBOXES)]

            contact_id, cstatus = find_or_create_contact(
                email, first_name, last_name, company, title=title,
                custom_fields=custom_fields
            )
            if not contact_id:
                failed += 1
                failures.append(f"Contact {email}: {cstatus}")
                log(f"  [{i+1}/{len(contacts)}] CONTACT FAILED: {email}")
                continue

            ok, astatus = add_to_sequence(contact_id, seq_id, mailbox_id, status="active")
            if ok:
                created += 1
                if (i+1) % 25 == 0 or (i+1) <= 3:
                    log(f"  [{i+1}/{len(contacts)}] {email} — {cstatus} → {astatus}")
            else:
                failed += 1
                failures.append(f"Seq {email}: {astatus}")
                log(f"  [{i+1}/{len(contacts)}] SEQ FAILED: {email} — {astatus}")

            time.sleep(0.3)

    log(f"\n{'='*60}")
    log(f"WS3 Done: enrolled={created}, failed={failed}, skipped={skipped}")
    if failures:
        log(f"\nFailures ({len(failures)}):")
        for f_ in failures:
            log(f"  - {f_}")
    return created, failed


def main():
    parser = argparse.ArgumentParser(description="Apollo.io enrollment")
    parser.add_argument("--workstream", "-w", required=True, choices=["ws1", "ws2", "ws3", "ws4"])
    parser.add_argument("--test", "-t", type=int, help="Only process N contacts (test mode)")
    parser.add_argument("--dry-run", "-d", action="store_true", help="Print what would be done without API calls")
    parser.add_argument("--group", "-g", help="Run specific group (ws2: 4step/3step/2step, ws3: strategy/cs/revenue/sales)")
    args = parser.parse_args()

    if args.workstream == "ws1":
        run_ws1(test_count=args.test, dry_run=args.dry_run)
    elif args.workstream == "ws2":
        run_ws2(test_count=args.test, dry_run=args.dry_run, group_filter=args.group)
    elif args.workstream == "ws3":
        run_ws3(test_count=args.test, dry_run=args.dry_run, group_filter=args.group)
    else:
        log(f"Workstream {args.workstream} not yet implemented")
        sys.exit(1)


if __name__ == "__main__":
    main()
