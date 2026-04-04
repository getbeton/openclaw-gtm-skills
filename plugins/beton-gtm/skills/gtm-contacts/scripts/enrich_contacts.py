#!/usr/bin/env python3
"""
Enrich companies with decision-maker contacts via Apollo.io.
Prioritizes sales/CS leaders (not RevOps) based on buyer hierarchy.
"""

import sys
import json
import argparse
from pathlib import Path

# Add parent scripts dir to path
scripts_dir = Path(__file__).parent.parent.parent.parent / 'scripts'
sys.path.insert(0, str(scripts_dir))

try:
    import requests
except ImportError:
    print("Error: requests module not found. Install with: pip3 install requests")
    sys.exit(1)

from supabase_client import SupabaseClient

# Title lists by thread
EXPANSION_TITLES = [
    "Director of Inbound Sales", "Head of Inbound Sales",
    "Manager of Inbound Sales", "Director of Inside Sales",
    "VP of Inbound Sales", "VP of Inside Sales", "VP of Sales",
    "Chief Revenue Officer", "SVP of Sales",
    "VP Revenue Operations", "Head of Revenue Operations"  # fallback
]

CHURN_TITLES = [
    "Director of Customer Success", "Head of Customer Success",
    "VP of Customer Success", "Chief Customer Officer",
    "Director of Customer Success Operations"  # fallback
]

# Priority keywords (order matters)
EXPANSION_PRIORITY = ["Director", "Manager", "VP", "Chief", "SVP", "Head"]
CHURN_PRIORITY = ["Director", "Head", "VP", "Chief"]


def load_apollo_key():
    """Load Apollo API key from integrations/apollo.json"""
    # Go up to workspace root
    workspace_root = Path(__file__).parent.parent.parent.parent.parent.parent
    key_path = workspace_root / 'integrations' / 'apollo.json'
    if not key_path.exists():
        print(f"Error: Apollo API key not found at {key_path}")
        print("Create integrations/apollo.json with: {\"api_key\": \"YOUR_KEY\"}")
        sys.exit(1)
    
    with open(key_path) as f:
        config = json.load(f)
    
    return config.get('api_key')


def get_org_id_from_apollo(domain, api_key):
    """Enrich company via Apollo to get fresh org_id (free, no credits)."""
    try:
        resp = requests.get(
            'https://api.apollo.io/v1/organizations/enrich',
            headers={'X-Api-Key': api_key},
            params={'domain': domain},
            timeout=30
        )
        if resp.status_code == 200:
            org = resp.json().get('organization', {})
            return org.get('id')
        return None
    except Exception as e:
        print(f"  ⚠️  Org enrichment error: {e}")
        return None

def search_apollo_contacts(org_id, titles, api_key):
    """
    Search Apollo for contacts by org_id and given titles.
    Returns all matches (search is free, only reveals cost credits).
    """
    response = requests.post(
        'https://api.apollo.io/v1/mixed_people/api_search',
        headers={'X-Api-Key': api_key},
        json={
            'organization_ids': [org_id],
            'person_titles': titles,
            'person_seniorities': ['director', 'vp', 'manager', 'c_suite', 'head'],
            'per_page': 25
        },
        timeout=30
    )
    
    if response.status_code != 200:
        print(f"  ⚠️  Apollo API error ({response.status_code}): {response.text[:200]}")
        return []
    
    data = response.json()
    # Trust the API filtering; org.id is hidden until revealed
    return [p for p in data.get('people', []) if p.get('id')]


def reveal_contact_email(person_id, api_key):
    """
    Reveal full contact details (including email) via Apollo's people/match endpoint.
    Costs 1 Apollo credit per reveal.
    """
    response = requests.post(
        'https://api.apollo.io/v1/people/match',
        headers={'X-Api-Key': api_key},
        json={'id': person_id},
        timeout=30
    )
    
    if response.status_code != 200:
        print(f"  ⚠️  Email reveal failed ({response.status_code}): {response.text[:200]}")
        return None
    
    data = response.json()
    return data.get('person')


def filter_by_priority(contacts, priority_keywords, max_contacts=2, avoid_keywords=None, auto_reveal=False, api_key=None):
    """
    Sort contacts by title priority and return top N.
    If auto_reveal=True, automatically reveal emails for top contacts.
    
    Priority logic:
    1. Penalize titles with avoid_keywords (e.g., "Operations", "RevOps")
    2. Score by first matching keyword position (lower index = higher priority)
    3. Sort ascending (lower score = better)
    4. Return top N
    5. If auto_reveal: enrich top N with full details (costs Apollo credits)
    
    Example with priority=["Director", "VP", "Chief"] and avoid=["Operations"]:
    - "Director of Sales" → score 0 (best)
    - "VP of Sales" → score 1
    - "Director of Revenue Operations" → score 100 (penalized)
    - "Chief Revenue Officer" → score 2
    - "Sales Manager" → score 999 (no match)
    """
    avoid_keywords = avoid_keywords or []
    
    def priority_score(contact):
        title = contact.get('title', '').lower()
        
        # Penalty for avoid keywords
        for avoid in avoid_keywords:
            if avoid.lower() in title:
                return 100  # Low priority but not rejected
        
        # Match priority keywords
        for idx, keyword in enumerate(priority_keywords):
            if keyword.lower() in title:
                return idx
        
        return 999  # No match = rejected
    
    scored = [(priority_score(c), c) for c in contacts]
    scored.sort(key=lambda x: x[0])
    top_contacts = [c for score, c in scored[:max_contacts]]
    
    # Auto-reveal emails for top contacts
    if auto_reveal and api_key:
        revealed = []
        for contact in top_contacts:
            person_id = contact.get('id')
            if person_id:
                full_contact = reveal_contact_email(person_id, api_key)
                if full_contact:
                    revealed.append(full_contact)
                else:
                    revealed.append(contact)  # Keep original if reveal fails
            else:
                revealed.append(contact)
        return revealed
    
    return top_contacts


def save_contacts_to_supabase(company_id, contacts, dry_run=False):
    """
    Save contacts to Supabase contacts table.
    Assumes contacts.company_id foreign key exists.
    """
    if dry_run:
        return
    
    client = SupabaseClient()
    
    for contact in contacts:
        row = {
            'company_id': company_id,
            'name': contact.get('name'),
            'first_name': contact.get('first_name'),
            'last_name': contact.get('last_name'),
            'email': contact.get('email'),
            'title': contact.get('title'),
            'seniority': contact.get('seniority'),
            'linkedin_url': contact.get('linkedin_url')
        }
        
        # Insert with upsert flag (on conflict do update)
        try:
            client.insert('contacts', row, upsert=False)
        except Exception:
            pass  # skip duplicates


def load_companies_from_input(args):
    """
    Load companies from various input sources.
    Priority: CSV > JSON > domains > Supabase query
    """
    client = SupabaseClient()
    
    # Option 1: CSV file with domains
    if args.csv:
        import csv
        companies = []
        with open(args.csv) as f:
            reader = csv.DictReader(f)
            for row in reader:
                domain = row.get('domain') or row.get('Domain')
                if domain:
                    companies.append({
                        'id': None,  # Will need to look up if saving
                        'name': row.get('name') or row.get('Name') or domain,
                        'domain': domain
                    })
        return companies
    
    # Option 2: JSON file
    if args.json:
        with open(args.json) as f:
            data = json.load(f)
            return data if isinstance(data, list) else [data]
    
    # Option 3: Domain list from CLI
    if args.domains:
        domains = args.domains.split(',')
        companies = []
        for domain in domains:
            domain = domain.strip()
            # Look up company_id if exists
            company = client.get_company_by_domain(domain)
            if company:
                companies.append(company)
            else:
                companies.append({'id': None, 'name': domain, 'domain': domain})
        return companies
    
    # Option 4: Supabase query with filters
    status = args.status or 'scored'
    limit = args.limit or 1000
    
    return client.get_companies_by_status(status, limit=limit)


def main():
    parser = argparse.ArgumentParser(description='Enrich companies with Apollo contacts')
    
    # Input sources (mutually exclusive)
    input_group = parser.add_mutually_exclusive_group()
    input_group.add_argument('--csv', help='CSV file with domain column')
    input_group.add_argument('--json', help='JSON file with company objects')
    input_group.add_argument('--domains', help='Comma-separated domain list')
    
    # Filters (for Supabase query mode)
    parser.add_argument('--status', help='Filter by research_status (default: scored)')
    parser.add_argument('--segment', help='Filter companies by segment slug (comma-separated)')
    parser.add_argument('--limit', type=int, help='Limit number of companies to process')
    
    # Behavior
    parser.add_argument('--max-per-company', type=int, default=2, help='Max contacts per thread')
    parser.add_argument('--dry-run', action='store_true', help='Search only, don\'t save')
    parser.add_argument('--reveal', action='store_true', help='Actually reveal emails (costs credits)')
    
    args = parser.parse_args()
    
    # Load API key
    api_key = load_apollo_key()
    
    # Load companies from input
    companies = load_companies_from_input(args)
    
    print(f"Processing {len(companies)} companies...")
    if args.dry_run:
        print("DRY RUN MODE — emails will NOT be revealed or saved")
    
    for idx, company in enumerate(companies, 1):
        domain = company['domain']
        print(f"\n[{idx}/{len(companies)}] {company['name']} ({domain})")
        
        # Get Apollo org_id first (free enrichment)
        org_id = get_org_id_from_apollo(domain, api_key)
        if not org_id:
            print(f"  ❌ Could not find Apollo org_id for {domain} (Skipping)")
            continue
        print(f"  📍 Apollo org_id: {org_id}")
        
        # Thread 1: Expansion signals (avoid RevOps titles)
        print("  Thread 1: Expansion signals...")
        expansion_results = search_apollo_contacts(org_id, EXPANSION_TITLES, api_key)
        top_expansion = filter_by_priority(
            expansion_results, 
            EXPANSION_PRIORITY, 
            args.max_per_company,
            auto_reveal=args.reveal,
            api_key=api_key,
            avoid_keywords=["Operations", "RevOps", "Enablement"]
        )
        print(f"    Found {len(expansion_results)} total, top {len(top_expansion)}:")
        for c in top_expansion:
            print(f"      - {c.get('name')} ({c.get('title')})")
        
        # Thread 2: Churn/performance signals (avoid CS Ops)
        print("  Thread 2: Churn/performance signals...")
        churn_results = search_apollo_contacts(org_id, CHURN_TITLES, api_key)
        top_churn = filter_by_priority(
            churn_results, 
            CHURN_PRIORITY, 
            args.max_per_company,
            avoid_keywords=["Operations"],
            auto_reveal=args.reveal,
            api_key=api_key
        )
        print(f"    Found {len(churn_results)} total, top {len(top_churn)}:")
        for c in top_churn:
            print(f"      - {c.get('name')} ({c.get('title')})")
        
        # Save to DB (unless dry-run)
        all_contacts = top_expansion + top_churn
        if all_contacts and not args.dry_run:
            if args.reveal:
                save_contacts_to_supabase(company['id'], all_contacts, dry_run=False)
                print(f"  ✅ Saved {len(all_contacts)} contacts")
            else:
                print(f"  ⚠️  Skipping save (use --reveal to actually save)")
    
    print(f"\n✅ Done. Processed {len(companies)} companies.")
    if args.dry_run:
        print("Run with --reveal to save contacts to database.")
    elif not args.reveal:
        print("⚠️  Contacts found but not saved (use --reveal to save)")


if __name__ == '__main__':
    main()
