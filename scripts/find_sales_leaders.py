#!/usr/bin/env python3
"""Find VP/Director/Head of Sales at companies missing sales leadership for WS3.

Uses Apollo api_search (search) + people/match (enrich) two-step flow.
"""

import json
import time
import urllib.request
import urllib.error
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
APOLLO_KEY = json.loads((BASE_DIR / "integrations/apollo.json").read_text())["api_key"]
APOLLO_BASE = "https://api.apollo.io/api/v1"

COMPANIES = [
    {"company": "#paid", "domain": "hashtagpaid.com"},
    {"company": "Acronis", "domain": "acronis.com"},
    {"company": "Alida", "domain": "visioncritical.com"},
    {"company": "Ceipal", "domain": "ceipal.com"},
    {"company": "Cellebrite", "domain": "cellebrite.com"},
    {"company": "Clari", "domain": "grooveapp.com"},
    {"company": "Cognyte", "domain": "cognyte.com"},
    {"company": "CommerceIQ", "domain": "commerceiq.ai"},
    {"company": "Crossover", "domain": "crossover.com"},
    {"company": "DAC", "domain": "dacgroup.com"},
    {"company": "Diligent", "domain": "diligent.com"},
    {"company": "Docebo", "domain": "docebo.com"},
    {"company": "Enverus", "domain": "enverus.com"},
    {"company": "Forma", "domain": "joinforma.com"},
    {"company": "GBG", "domain": "gbgplc.com"},
    {"company": "Huntress", "domain": "huntress.com"},
    {"company": "InMobi", "domain": "advertising.inmobi.com"},
    {"company": "Ironclad", "domain": "ironcladapp.com"},
    {"company": "Ivanti", "domain": "ivanti.com"},
    {"company": "Kinaxis", "domain": "mpo.com"},
    {"company": "Lafayette College", "domain": "lafayette.edu"},
    {"company": "Leaseweb", "domain": "leaseweb.com"},
    {"company": "Lion Parcel", "domain": "lionparcel.com"},
    {"company": "MX", "domain": "mx.com"},
    {"company": "Medable", "domain": "medable.com"},
    {"company": "Motorola Solutions", "domain": "motorolasolutions.com"},
    {"company": "Neo4j", "domain": "neotechnology.com"},
    {"company": "Partoo", "domain": "partoo.co"},
    {"company": "Quinyx", "domain": "quinyx.com"},
    {"company": "Sirion", "domain": "sirionlabs.com"},
    {"company": "Smarsh", "domain": "smarsh.com"},
    {"company": "Smartly", "domain": "smartly.io"},
    {"company": "Socure", "domain": "socure.com"},
    {"company": "Sonar", "domain": "sonarsource.com"},
    {"company": "Splunk", "domain": "splunk.com"},
    {"company": "Tradeshift", "domain": "tradeshift.com"},
    {"company": "Verisk", "domain": "verisk.com"},
    {"company": "WalkMe", "domain": "walkme.com"},
    {"company": "impact.com", "domain": "impact.com"},
]


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
            if e.code == 429:
                wait = 2 ** (attempt + 1)
                print(f"    Rate limited, waiting {wait}s...", flush=True)
                time.sleep(wait)
                continue
            if attempt < retries - 1 and e.code >= 500:
                time.sleep(2)
                continue
            try:
                rj = json.loads(rb)
            except Exception:
                rj = {"raw": rb[:500]}
            return e.code, rj
        except Exception as ex:
            if attempt < retries - 1:
                time.sleep(2)
                continue
            return 0, {"error": str(ex)}
    return 0, {"error": "max retries"}


def find_and_enrich(domain):
    """Search for sales leader, then enrich to get email."""
    s, r = apollo_api("POST", "/mixed_people/api_search", {
        "q_organization_domains_list": [domain],
        "person_titles": [
            "VP Sales", "Vice President Sales", "VP of Sales",
            "Head of Sales", "Director of Sales", "Senior Director Sales",
            "Chief Revenue Officer", "SVP Sales",
        ],
        "per_page": 1,
    })
    people = r.get("people", [])

    if not people:
        s, r = apollo_api("POST", "/mixed_people/api_search", {
            "q_organization_domains_list": [domain],
            "person_titles": ["Sales", "Revenue", "Business Development"],
            "person_seniorities": ["vp", "director", "c_suite"],
            "per_page": 1,
        })
        people = r.get("people", [])

    if not people:
        return None

    pid = people[0].get("id")
    if not pid:
        return None

    time.sleep(0.2)
    s2, r2 = apollo_api("POST", "/people/match", {"id": pid})
    person = r2.get("person", {})
    if not person:
        return None

    return {
        "first_name": person.get("first_name", ""),
        "last_name": person.get("last_name", ""),
        "title": person.get("title", ""),
        "email": person.get("email", ""),
        "email_status": person.get("email_status", ""),
        "linkedin_url": person.get("linkedin_url", ""),
        "apollo_id": person.get("id", ""),
    }


results = []
found = 0
not_found = 0

print(f"Searching for sales leaders at {len(COMPANIES)} companies...\n", flush=True)

for i, co in enumerate(COMPANIES):
    domain = co["domain"]
    company = co["company"]

    person = find_and_enrich(domain)

    if person and person.get("email"):
        found += 1
        print(f"  [{i+1}/{len(COMPANIES)}] {company}: {person['first_name']} {person['last_name']} — {person['title']} ({person['email']}, {person['email_status']})", flush=True)
        results.append({"company": company, "domain": domain, **person})
    elif person:
        print(f"  [{i+1}/{len(COMPANIES)}] {company}: {person['first_name']} {person['last_name']} — {person['title']} (NO EMAIL)", flush=True)
        not_found += 1
    else:
        not_found += 1
        print(f"  [{i+1}/{len(COMPANIES)}] {company}: NOT FOUND", flush=True)

    time.sleep(0.3)

print(f"\n{'='*60}", flush=True)
print(f"Found with email: {found}, Missing: {not_found}", flush=True)

out_path = BASE_DIR / "campaign-big-b2b/ws3_sales_leaders.json"
with open(out_path, "w") as f:
    json.dump(results, f, indent=2)
print(f"Saved {len(results)} results to {out_path}", flush=True)

verified = [r for r in results if r.get("email_status") == "verified"]
print(f"Verified emails: {len(verified)}/{len(results)}", flush=True)
