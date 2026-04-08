#!/usr/bin/env python3
"""
Apollo batch enrichment for 6sense companies.
Step 1: Org enrichment (free) — get employee count, industry, org_id
Step 2: People search (free) — find decision makers
Step 3: (Optional) Contact reveal (1 credit each) — only for T1 companies

Usage:
    python3 apollo_batch_enrich.py --input 6sense_300_domains.json --output apollo_results.json [--reveal]
"""

import json, time, sys, os, argparse, requests
from datetime import datetime

INTEGRATIONS_DIR = os.path.join(os.path.dirname(__file__), '..', 'integrations')
APOLLO_KEY = json.load(open(os.path.join(INTEGRATIONS_DIR, 'apollo.json')))['api_key']
HEADERS = {"Content-Type": "application/json", "X-Api-Key": APOLLO_KEY}

SEARCH_TITLES = [
    "Head of Analytics", "Head of Data", "VP Analytics", "Head of Retention",
    "VP Marketing", "Head of Customer Success", "VP Growth",
    "Head of Performance Marketing", "Chief Product Officer", "Head of CRM",
    "Director of Analytics", "Director of Retention", "Director of Marketing",
    "Director of Customer Success", "Head of Strategy", "VP Strategy",
    "Director of Strategy", "Chief Strategy Officer", "Head of Business Development",
    "VP Business Intelligence", "Head of Revenue", "Chief Revenue Officer",
    "VP of Sales", "Director of Sales", "Chief Data Officer",
]

def enrich_org(domain):
    """Step 1: Get org info from Apollo (free)."""
    try:
        resp = requests.get(
            f"https://api.apollo.io/api/v1/organizations/enrich",
            headers=HEADERS,
            params={"domain": domain},
            timeout=10
        )
        data = resp.json()
        org = data.get("organization", {})
        if not org:
            return None
        return {
            "org_id": org.get("id"),
            "name": org.get("name"),
            "employee_count": org.get("estimated_num_employees"),
            "industry": org.get("industry"),
            "country": org.get("country"),
            "city": org.get("city"),
            "founded_year": org.get("founded_year"),
            "annual_revenue": org.get("annual_revenue"),
            "linkedin_url": org.get("linkedin_url"),
            "keywords": org.get("keywords", [])[:5],
        }
    except Exception as e:
        return {"error": str(e)}


def search_people(domain, org_id=None):
    """Step 2: Search for decision makers (free)."""
    try:
        payload = {
            "q_organization_domains_list": [domain],
            "person_titles": SEARCH_TITLES,
            "person_seniorities": ["director", "vp", "c_suite"],
            "page": 1,
            "per_page": 10
        }
        if org_id:
            payload["organization_ids"] = [org_id]

        resp = requests.post(
            "https://api.apollo.io/api/v1/mixed_people/api_search",
            headers=HEADERS,
            json=payload,
            timeout=10
        )
        data = resp.json()
        people = data.get("people", [])
        return [{
            "id": p.get("id"),
            "title": p.get("title"),
            "seniority": p.get("seniority"),
            "city": p.get("city"),
            "country": p.get("country"),
            "linkedin_url": p.get("linkedin_url"),
        } for p in people[:6]]
    except Exception as e:
        return [{"error": str(e)}]


def reveal_contact(person_id):
    """Step 3: Reveal email (costs 1 credit)."""
    try:
        resp = requests.post(
            "https://api.apollo.io/api/v1/people/match",
            headers=HEADERS,
            json={"id": person_id, "reveal_personal_emails": False},
            timeout=10
        )
        data = resp.json()
        person = data.get("person", {})
        if person:
            return {
                "name": person.get("name"),
                "email": person.get("email"),
                "title": person.get("title"),
                "linkedin_url": person.get("linkedin_url"),
                "city": person.get("city"),
                "country": person.get("country"),
            }
        return {"error": data.get("error", "unknown")}
    except Exception as e:
        return {"error": str(e)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="JSON file with companies [{id, domain, name}]")
    parser.add_argument("--output", default="apollo_results.json")
    parser.add_argument("--reveal", action="store_true", help="Reveal top 2 contacts per company (costs credits)")
    parser.add_argument("--limit", type=int, default=300)
    parser.add_argument("--resume", action="store_true", help="Skip already-processed domains")
    args = parser.parse_args()

    companies = json.load(open(args.input))[:args.limit]

    # Resume support
    existing = {}
    if args.resume and os.path.exists(args.output):
        existing = {r["domain"]: r for r in json.load(open(args.output))}
        print(f"Resuming: {len(existing)} already processed")

    results = list(existing.values())
    stats = {"total": len(companies), "orgs_found": 0, "people_found": 0, "emails_revealed": 0, "errors": 0}

    for i, company in enumerate(companies):
        domain = company["domain"]

        if domain in existing:
            continue

        print(f"[{i+1}/{len(companies)}] {domain}", end=" ", flush=True)

        # Step 1: Org enrichment
        org = enrich_org(domain)
        if not org or org.get("error"):
            print(f"x org failed")
            results.append({"id": company["id"], "domain": domain, "name": company.get("name"), "org": None, "people": []})
            stats["errors"] += 1
            time.sleep(0.3)
            continue

        stats["orgs_found"] += 1
        emp = org.get("employee_count", "?")
        print(f"org:{org['name']} emp:{emp}", end=" ", flush=True)

        # Step 2: People search
        people = search_people(domain, org.get("org_id"))
        people_count = len([p for p in people if not p.get("error")])
        stats["people_found"] += people_count
        print(f"people:{people_count}", end="", flush=True)

        # Step 3: Reveal (optional)
        if args.reveal and people_count > 0:
            revealed = []
            for p in people[:2]:
                if p.get("id"):
                    r = reveal_contact(p["id"])
                    if r.get("email"):
                        revealed.append(r)
                        stats["emails_revealed"] += 1
                    time.sleep(0.3)
            if revealed:
                print(f" revealed:{len(revealed)}", end="")
            people = revealed + people[2:]  # replace top 2 with revealed versions

        result = {
            "id": company["id"],
            "domain": domain,
            "name": company.get("name"),
            "org": org,
            "people": people
        }
        results.append(result)
        print()

        # Save periodically
        if (i + 1) % 25 == 0:
            with open(args.output, "w") as f:
                json.dump(results, f, indent=2)
            print(f"  --- saved {len(results)} results ---")

        # Rate limit: ~3 req/sec to stay safe
        time.sleep(0.5)

    # Final save
    with open(args.output, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\n{'='*60}")
    print(f"DONE: {len(results)} companies processed")
    print(f"  Orgs found: {stats['orgs_found']}/{stats['total']}")
    print(f"  People found: {stats['people_found']}")
    print(f"  Emails revealed: {stats['emails_revealed']}")
    print(f"  Errors: {stats['errors']}")
    print(f"  Saved to: {args.output}")


if __name__ == "__main__":
    main()
