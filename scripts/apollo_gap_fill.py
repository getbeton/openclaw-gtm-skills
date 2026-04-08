#!/usr/bin/env python3
"""
Apollo gap fill — search for missing thread contacts at companies
where we already have 1-2 contacts but need more threads.
"""

import json, requests, time, os, sys

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
KEY = json.load(open(os.path.join(BASE, 'integrations', 'apollo.json')))['api_key']
HEADERS = {"Content-Type": "application/json", "X-Api-Key": KEY}

THREAD_TITLES = {
    'strategy': [
        "Head of Analytics", "Head of Data", "VP Analytics", "VP Strategy",
        "Director of Analytics", "Chief Strategy Officer", "VP Business Intelligence",
        "Director of Strategy", "Head of Business Development", "Chief Data Officer",
        "Director of Data", "Head of BI", "Director of Business Intelligence",
    ],
    'cs': [
        "Director of Customer Success", "VP Customer Success", "Head of Customer Success",
        "Head of Retention", "Senior Director Customer Success",
        "Director of Customer Success Operations", "VP of Client Success",
        "Head of Client Services", "Director of Account Management",
    ],
    'sales': [
        "VP of Sales", "Director of Sales", "Head of Sales",
        "Director of Inbound Sales", "Head of Inbound Sales", "VP Inbound Sales",
        "Director of Inside Sales", "Head of Inside Sales", "VP of Inside Sales",
        "VP of Sales Operations", "Sales Director", "Head of Revenue",
    ],
}


def search_and_reveal(domain, thread, org_id=None):
    """Search for a specific thread at a company and reveal best match."""
    titles = THREAD_TITLES[thread]

    payload = {
        "q_organization_domains_list": [domain],
        "person_titles": titles,
        "person_seniorities": ["director", "vp", "c_suite"],
        "page": 1,
        "per_page": 5,
    }
    if org_id:
        payload["organization_ids"] = [org_id]

    resp = requests.post(
        "https://api.apollo.io/api/v1/mixed_people/api_search",
        headers=HEADERS, json=payload, timeout=10
    )
    people = resp.json().get("people", [])

    if not people:
        # Try without seniority filter
        payload.pop("person_seniorities", None)
        resp = requests.post(
            "https://api.apollo.io/api/v1/mixed_people/api_search",
            headers=HEADERS, json=payload, timeout=10
        )
        people = resp.json().get("people", [])

    if not people:
        return None

    # Reveal the best match
    best = people[0]
    rev = requests.post(
        "https://api.apollo.io/api/v1/people/match",
        headers=HEADERS,
        json={"id": best["id"], "reveal_personal_emails": False},
        timeout=10
    )
    person = rev.json().get("person", {})

    if person and person.get("name"):
        return {
            "name": person.get("name"),
            "title": person.get("title", ""),
            "email": person.get("email", ""),
            "linkedin": person.get("linkedin_url", ""),
            "city": person.get("city", ""),
            "country": person.get("country", ""),
        }
    return None


def main():
    gap_file = os.path.join(BASE, "gap_fill_needed.json")
    gaps = json.load(open(gap_file))

    # Load existing org IDs from previous batches
    org_ids = {}
    for fname in ["apollo_6sense_300_results.json", "apollo_campaign_300_results.json"]:
        fpath = os.path.join(BASE, fname)
        if os.path.exists(fpath):
            for r in json.load(open(fpath)):
                org = r.get("org", {})
                if org and org.get("org_id"):
                    org_ids[r["domain"]] = org["org_id"]

    output_file = os.path.join(BASE, "gap_fill_results.json")

    # Resume support
    existing = []
    existing_keys = set()
    if os.path.exists(output_file):
        existing = json.load(open(output_file))
        existing_keys = {(r["domain"], r["thread"]) for r in existing}
        print(f"Resuming: {len(existing)} already done")

    results = list(existing)
    stats = {"searches": 0, "found": 0, "with_email": 0, "failed": 0}

    total = sum(len(g["needs"]) for g in gaps)
    done = 0

    for gap in gaps:
        domain = gap["domain"]
        oid = org_ids.get(domain)

        for thread in gap["needs"]:
            if (domain, thread) in existing_keys:
                done += 1
                continue

            done += 1
            print(f"[{done}/{total}] {domain:28} {thread:10}", end=" ", flush=True)
            stats["searches"] += 1

            try:
                result = search_and_reveal(domain, thread, oid)
                if result:
                    stats["found"] += 1
                    has_email = bool(result.get("email"))
                    if has_email:
                        stats["with_email"] += 1
                    print(f"-> {result['name'][:20]} | {result.get('email') or 'no email'}")
                    results.append({
                        "domain": domain,
                        "thread": thread,
                        "emp": gap.get("emp", 0),
                        **result,
                    })
                else:
                    print("-> not found")
                    stats["failed"] += 1
            except Exception as e:
                print(f"-> ERROR: {str(e)[:50]}")
                stats["failed"] += 1

            time.sleep(0.5)

            # Save every 30
            if done % 30 == 0:
                with open(output_file, "w") as f:
                    json.dump(results, f, indent=2)
                print(f"  --- saved {len(results)} ({stats['with_email']} with email) ---")

    # Final save
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\n{'='*60}")
    print(f"GAP FILL COMPLETE")
    print(f"  Searches: {stats['searches']}")
    print(f"  Found: {stats['found']}")
    print(f"  With email: {stats['with_email']}")
    print(f"  Failed: {stats['failed']}")
    print(f"  Saved to: {output_file}")


if __name__ == "__main__":
    main()
