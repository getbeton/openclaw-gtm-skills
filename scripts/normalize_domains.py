#!/usr/bin/env python3
"""
normalize_domains.py — Convert subdomain entries in raw companies to root domains.

Rules:
1. For raw companies with 3+ domain parts:
   - If root domain is a known hosting platform → keep subdomain as-is (it's a real landing page)
   - Otherwise → convert to root domain (e.g. adrianarreola.firstteam.com → firstteam.com)
2. After converting:
   - If root domain already exists in DB → delete the subdomain entry
   - If not → update domain in-place
3. Only touches research_status = 'raw'

Country-code TLDs handled: .co.uk, .com.br, .com.au, .co.jp, .co.nz, .co.za, .com.mx, .com.ar

Run:
    python3 normalize_domains.py [--dry-run]
"""

import httpx
import re
import sys
import time
from collections import defaultdict

# ── Config ────────────────────────────────────────────────────────────────────
SERVICE_KEY = (
    "YOUR_SUPABASE_SERVICE_KEY"
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImFteWd0d29xdWpsdWVwaWJjbmZzIiwicm9sZSI6"
    "InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3Mzg0OTIxMywiZXhwIjoyMDg5NDI1MjEzfQ."
    "f-dQgjCylDEaOJAcqNQwovL_v93--QLX3EI6kXqrdos"
)
SUPABASE_BASE = "YOUR_SUPABASE_URL"
H = {
    "apikey": SERVICE_KEY,
    "Authorization": f"Bearer {SERVICE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal",
}
QH = {
    "apikey": SERVICE_KEY,
    "Authorization": f"Bearer {SERVICE_KEY}",
}

# Hosting platforms where a 3rd-level domain = a real company landing page
# Keep these as-is — don't strip the subdomain
HOSTING_PLATFORMS = {
    "vercel.app", "netlify.app", "pages.dev", "github.io", "github.com",
    "fly.dev", "render.com", "railway.app", "onrender.com", "kinsta.cloud",
    "wpengine.com", "webflow.io", "framer.app", "framer.website",
    "bubble.io", "glide.page", "carrd.co", "squarespace.com", "wixsite.com",
    "webnode.com", "weebly.com", "strikingly.com", "notion.site",
    "super.so", "typedream.app", "softr.app", "glideapps.com",
    "n8n.cloud", "make.com", "zapier.com", "airtable.com",
    "herokuapp.com", "appspot.com", "azurewebsites.net", "cloudflare.net",
    "myshopify.com", "shoplineapp.com", "ecwid.com",
    "withloma.com", "hotelbee.co", "emergent.host", "taxmaro.com",
    "firstteam.com",  # real estate platform with agent subdomains
    "featureos.app", "withloma.com",
}

# Country-code TLD combos that are effectively 2-part
CC_TLDS = {
    ".co.uk", ".com.br", ".com.au", ".co.jp", ".co.nz", ".co.za",
    ".com.mx", ".com.ar", ".com.co", ".com.pe", ".com.ve",
    ".org.uk", ".net.au", ".net.br",
}


def get_root_domain(domain: str) -> str:
    """Return the effective root domain (2-part, or 3-part for cc-TLDs)."""
    parts = domain.split(".")
    # Check cc-TLDs
    for cc in CC_TLDS:
        if domain.endswith(cc):
            # e.g. foo.bar.co.uk → bar.co.uk
            cc_parts = cc.lstrip(".").split(".")
            return ".".join(parts[-(len(cc_parts) + 1):])
    # Standard: last 2 parts
    return ".".join(parts[-2:])


def is_subdomain(domain: str) -> bool:
    """Return True if domain has more parts than its root domain."""
    return domain != get_root_domain(domain)


def should_preserve(domain: str) -> bool:
    """Return True if this subdomain should be kept as-is (hosting platform)."""
    root = get_root_domain(domain)
    return root in HOSTING_PLATFORMS


def fetch_all_raw(client: httpx.Client) -> list[dict]:
    """Fetch all raw companies (id, domain) with pagination."""
    results = []
    offset = 0
    limit = 1000
    while True:
        r = client.get(
            f"{SUPABASE_BASE}/rest/v1/companies",
            headers=QH,
            params={
                "select": "id,domain",
                "research_status": "eq.raw",
                "limit": str(limit),
                "offset": str(offset),
                "order": "domain.asc",
            },
            timeout=30,
        )
        r.raise_for_status()
        page = r.json()
        results.extend(page)
        if len(page) < limit:
            break
        offset += limit
    return results


def check_domain_exists(client: httpx.Client, domain: str) -> str | None:
    """Return existing company ID if domain already in DB, else None."""
    r = client.get(
        f"{SUPABASE_BASE}/rest/v1/companies",
        headers=QH,
        params={"select": "id", "domain": f"eq.{domain}", "limit": "1"},
        timeout=10,
    )
    r.raise_for_status()
    rows = r.json()
    return rows[0]["id"] if rows else None


def update_domain(client: httpx.Client, company_id: str, new_domain: str):
    r = client.patch(
        f"{SUPABASE_BASE}/rest/v1/companies",
        headers=H,
        params={"id": f"eq.{company_id}"},
        json={"domain": new_domain},
        timeout=10,
    )
    r.raise_for_status()


def delete_company(client: httpx.Client, company_id: str):
    r = client.delete(
        f"{SUPABASE_BASE}/rest/v1/companies",
        headers=H,
        params={"id": f"eq.{company_id}"},
        timeout=10,
    )
    r.raise_for_status()


def main():
    dry_run = "--dry-run" in sys.argv

    print(f"{'[DRY RUN] ' if dry_run else ''}Domain normalization — raw companies only\n")

    stats = defaultdict(int)
    errors = []

    with httpx.Client(timeout=30) as client:
        print("Fetching all raw companies...")
        companies = fetch_all_raw(client)
        print(f"Fetched {len(companies)} raw companies\n")

        subdomains = [(c["id"], c["domain"]) for c in companies if is_subdomain(c["domain"])]
        root_domains = {c["domain"] for c in companies if not is_subdomain(c["domain"])}

        print(f"Root domains (2-part): {len(root_domains)}")
        print(f"Subdomains (3+ parts): {len(subdomains)}")
        print()

        for i, (cid, domain) in enumerate(subdomains, 1):
            root = get_root_domain(domain)

            # Hosting platform — keep as-is
            if should_preserve(domain):
                stats["preserved"] += 1
                if i <= 20 or i % 500 == 0:
                    print(f"  [KEEP] {domain} (hosting platform)")
                continue

            # Check if root already exists in DB
            existing_id = check_domain_exists(client, root)

            if existing_id:
                # Root domain already in DB → delete this subdomain entry
                stats["deleted_duplicate"] += 1
                if i <= 20 or i % 500 == 0:
                    print(f"  [DEL]  {domain} → {root} (duplicate, existing ID: {existing_id[:8]}...)")
                if not dry_run:
                    try:
                        delete_company(client, cid)
                    except Exception as e:
                        errors.append(f"DELETE {domain}: {e}")
            else:
                # Root doesn't exist → convert this entry
                stats["converted"] += 1
                if i <= 20 or i % 500 == 0:
                    print(f"  [FIX]  {domain} → {root}")
                if not dry_run:
                    try:
                        update_domain(client, cid, root)
                        root_domains.add(root)  # prevent converting another subdomain to same root
                    except Exception as e:
                        errors.append(f"UPDATE {domain}: {e}")

            # Rate limiting
            if i % 50 == 0:
                time.sleep(0.5)
                print(f"  ... processed {i}/{len(subdomains)}", flush=True)

    print("\n" + "=" * 60)
    print("NORMALIZATION COMPLETE")
    print("=" * 60)
    print(f"Total subdomains:      {len(subdomains)}")
    print(f"Preserved (hosting):   {stats['preserved']}")
    print(f"Converted (→ root):    {stats['converted']}")
    print(f"Deleted (duplicate):   {stats['deleted_duplicate']}")
    if errors:
        print(f"\nErrors ({len(errors)}):")
        for e in errors[:10]:
            print(f"  {e}")


if __name__ == "__main__":
    main()
