#!/usr/bin/env python3
"""
import_clay.py — Import Clay export CSV files into Supabase Beton GTM schema.

Usage:
    python3 import_clay.py

Sources:
    Type A (company-level): sales-Ops-*, Apollo-Search-Line-Salespeople-*
    Type B (contact-level):  rev-ops-*, RevOps-Export-*, Clay-Search-Line-*
"""

import os
import re
import sys
import math
import logging
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras

# ─── Config ────────────────────────────────────────────────────────────────────

SUPABASE_DB_URL = (
    "postgresql://postgres.YOUR_PROJECT_REF:"
    "YOUR_SUPABASE_SERVICE_KEY"  # placeholder — using REST API instead
    "@aws-0-us-east-1.pooler.supabase.com:6543/postgres"
)

# We'll use psycopg2 via the Supabase pooler connection string.
# The connection string format for Supabase: postgresql://postgres.[ref]:[password]@aws-0-[region].pooler.supabase.com:6543/postgres
# We need the database password (not the service key). Use the REST approach via requests instead.

SUPABASE_URL = "YOUR_SUPABASE_URL"
SUPABASE_KEY = (
    "YOUR_SUPABASE_SERVICE_KEY"
    ".YOUR_SUPABASE_KEY_MIDDLE"
    ".YOUR_SUPABASE_KEY_SIG"
)

DATA_DIR = Path("YOUR_WORKSPACE_PATH/beton/sketches/sales-research/data")

BATCH_SIZE = 500
LOG_EVERY = 1000

TYPE_A_FILES = [
    "sales-Ops-Default-view-export-1769800769445.csv",
    "sales-Ops-Default-view-export-1771277696960.csv",
    "Apollo-Search-Line-Salespeople-Default-view-export-1769800796521.csv",
    "Apollo-Search-Line-Salespeople-Exported-rows-export-1771277723001.csv",
]

TYPE_B_FILES = [
    "rev-ops-Default-view-export-1769800749934.csv",
    "rev-ops-Default-view-export-1769800859679.csv",
    "rev-ops-Default-view-export-1771277703515.csv",
    "RevOps-Export-table-Ready-for-export-export-1769631057542.csv",
    "Clay-Search-Line-salespeople-Default-view-export-1769800691710.csv",
    "Clay-Search-Line-salespeople-Default-view-export-1771277709798.csv",
]

# ─── Logging ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("import_clay")

# ─── HTTP client (Supabase REST API) ────────────────────────────────────────────

import requests

SESSION = requests.Session()
SESSION.headers.update({
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates",
})


def rest_post(table: str, rows: list, on_conflict: str = "", returning: str = "minimal") -> list:
    """POST a batch to a Supabase table. Returns response rows (empty for minimal)."""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {"Prefer": f"return={returning}"}
    if on_conflict:
        headers["Prefer"] += f",resolution=merge-duplicates"
    resp = SESSION.post(url, json=rows, headers=headers)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"POST {table} failed {resp.status_code}: {resp.text[:500]}")
    return resp.json() if returning != "minimal" else []


def rest_upsert(table: str, rows: list, on_conflict: str) -> None:
    """Upsert via POST with on_conflict param."""
    url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict={on_conflict}"
    headers = {"Prefer": "return=minimal,resolution=merge-duplicates"}
    resp = SESSION.post(url, json=rows, headers=headers)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"UPSERT {table} failed {resp.status_code}: {resp.text[:500]}")


def rest_get(table: str, select: str = "*", filters: dict = None, limit: int = None) -> list:
    """GET rows from a table with automatic pagination (Supabase caps at 1000/page)."""
    PAGE_SIZE = 1000
    base_params = f"select={select}"
    if filters:
        for k, v in filters.items():
            base_params += f"&{k}=eq.{v}"

    collected = []
    offset = 0
    while True:
        url = f"{SUPABASE_URL}/rest/v1/{table}?{base_params}&limit={PAGE_SIZE}&offset={offset}"
        resp = SESSION.get(url)
        if resp.status_code != 200:
            raise RuntimeError(f"GET {table} failed {resp.status_code}: {resp.text[:300]}")
        page = resp.json()
        if not page:
            break
        collected.extend(page)
        if limit and len(collected) >= limit:
            return collected[:limit]
        if len(page) < PAGE_SIZE:
            break  # last page
        offset += PAGE_SIZE
    return collected


# ─── Helpers ────────────────────────────────────────────────────────────────────

_DOMAIN_RE = re.compile(r'^(?:https?://)?(?:www\.)?([^/?\s]+)', re.IGNORECASE)


def normalize_domain(raw: str) -> str | None:
    """Strip protocol/www/path and lowercase."""
    if not raw or pd.isna(raw):
        return None
    raw = str(raw).strip()
    if not raw:
        return None
    m = _DOMAIN_RE.match(raw)
    if m:
        d = m.group(1).lower().rstrip('.')
        return d if '.' in d else None
    return None


def coerce_int(val) -> int | None:
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return None
    try:
        return int(float(str(val).replace(',', '').strip()))
    except (ValueError, TypeError):
        return None


def coerce_numeric(val) -> float | None:
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return None
    try:
        s = str(val).replace('$', '').replace(',', '').strip()
        return float(s) if s else None
    except (ValueError, TypeError):
        return None


def coerce_year(val) -> int | None:
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return None
    s = str(val).strip()
    # Handle ISO date strings like "2012-01-01T00:00:00.000Z"
    m = re.match(r'^(\d{4})', s)
    if m:
        y = int(m.group(1))
        return y if 1800 < y < 2100 else None
    return None


def get_col(row, *candidates):
    """Return first non-null value from a list of column name candidates."""
    for c in candidates:
        val = row.get(c)
        if val and not (isinstance(val, float) and math.isnan(val)) and str(val).strip():
            return str(val).strip()
    return None


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


# ─── Company cache ────────────────────────────────────────────────────────────────

# domain → company_id (UUID string)
_company_cache: dict[str, str] = {}


def load_company_cache():
    """Load all existing companies from DB into local cache."""
    log.info("Loading company cache from DB...")
    all_rows = []
    offset = 0
    limit = 1000
    while True:
        url = f"{SUPABASE_URL}/rest/v1/companies?select=id,domain&limit={limit}&offset={offset}"
        resp = SESSION.get(url)
        if resp.status_code != 200:
            raise RuntimeError(f"Failed to load companies: {resp.text[:300]}")
        batch = resp.json()
        all_rows.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
    for row in all_rows:
        _company_cache[row['domain']] = row['id']
    log.info(f"Loaded {len(_company_cache)} companies into cache.")


def ensure_company(domain: str, name: str = None) -> str | None:
    """Return company_id, inserting if not present."""
    if not domain:
        return None
    if domain in _company_cache:
        return _company_cache[domain]
    # Insert minimal record
    record = {"domain": domain, "research_status": "raw"}
    if name:
        record["name"] = name
    url = f"{SUPABASE_URL}/rest/v1/companies?on_conflict=domain"
    headers = {"Prefer": "return=representation,resolution=merge-duplicates"}
    resp = SESSION.post(url, json=[record], headers=headers)
    if resp.status_code in (200, 201):
        rows = resp.json()
        if rows:
            cid = rows[0]['id']
            _company_cache[domain] = cid
            return cid
    # If we got a conflict and minimal return, fetch it
    rows = rest_get("companies", select="id,domain", filters={"domain": domain})
    if rows:
        cid = rows[0]['id']
        _company_cache[domain] = cid
        return cid
    return None


# ─── Contact dedup cache ──────────────────────────────────────────────────────────

# email → contact_id, linkedin_url → contact_id
_contact_by_email: dict[str, str] = {}
_contact_by_linkedin: dict[str, str] = {}


def load_contact_cache():
    """Load existing contacts from DB."""
    log.info("Loading contact cache from DB...")
    all_rows = []
    offset = 0
    limit = 1000
    while True:
        url = f"{SUPABASE_URL}/rest/v1/contacts?select=id,email,linkedin_url&limit={limit}&offset={offset}"
        resp = SESSION.get(url)
        if resp.status_code != 200:
            raise RuntimeError(f"Failed to load contacts: {resp.text[:300]}")
        batch = resp.json()
        all_rows.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
    for row in all_rows:
        if row.get('email'):
            _contact_by_email[row['email'].lower()] = row['id']
        if row.get('linkedin_url'):
            _contact_by_linkedin[row['linkedin_url']] = row['id']
    log.info(f"Loaded {len(_contact_by_email)} contacts (by email) into cache.")


# ─── Batch insert helpers ──────────────────────────────────────────────────────────

def insert_companies_batch(records: list[dict]) -> dict[str, str]:
    """Insert companies, return domain→id mapping for newly inserted."""
    if not records:
        return {}
    records = normalize_batch(records)
    url = f"{SUPABASE_URL}/rest/v1/companies?on_conflict=domain"
    headers = {"Prefer": "return=representation,resolution=merge-duplicates"}
    resp = SESSION.post(url, json=records, headers=headers)
    result = {}
    if resp.status_code in (200, 201):
        for row in resp.json():
            if row.get('domain') and row.get('id'):
                result[row['domain']] = row['id']
                _company_cache[row['domain']] = row['id']
    return result


def normalize_batch(rows: list[dict]) -> list[dict]:
    """Ensure all rows in a batch have identical keys (fill missing with None).
    PostgREST requires uniform keys across all rows in a batch."""
    if not rows:
        return rows
    all_keys = set()
    for r in rows:
        all_keys.update(r.keys())
    return [{k: r.get(k, None) for k in all_keys} for r in rows]


def dedup_by_key(rows: list[dict], key: str) -> list[dict]:
    """Deduplicate rows by a key field, merging non-null values (last wins for each field)."""
    merged: dict[str, dict] = {}
    for r in rows:
        k = r.get(key)
        if k is None:
            continue
        if k not in merged:
            merged[k] = dict(r)
        else:
            # Merge: update with non-null values from later rows
            for field, val in r.items():
                if val is not None:
                    merged[k][field] = val
    return list(merged.values())


def upsert_firmographics(rows: list[dict]) -> None:
    if not rows:
        return
    rows = dedup_by_key(rows, 'company_id')
    rows = normalize_batch(rows)
    url = f"{SUPABASE_URL}/rest/v1/company_firmographics?on_conflict=company_id"
    headers = {"Prefer": "return=minimal,resolution=merge-duplicates"}
    resp = SESSION.post(url, json=rows, headers=headers)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"UPSERT company_firmographics failed {resp.status_code}: {resp.text[:400]}")


def upsert_social(rows: list[dict]) -> None:
    if not rows:
        return
    rows = dedup_by_key(rows, 'company_id')
    rows = normalize_batch(rows)
    url = f"{SUPABASE_URL}/rest/v1/company_social?on_conflict=company_id"
    headers = {"Prefer": "return=minimal,resolution=merge-duplicates"}
    resp = SESSION.post(url, json=rows, headers=headers)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"UPSERT company_social failed {resp.status_code}: {resp.text[:400]}")


def upsert_classification(rows: list[dict]) -> None:
    if not rows:
        return
    rows = dedup_by_key(rows, 'company_id')
    rows = normalize_batch(rows)
    url = f"{SUPABASE_URL}/rest/v1/company_classification?on_conflict=company_id"
    headers = {"Prefer": "return=minimal,resolution=merge-duplicates"}
    resp = SESSION.post(url, json=rows, headers=headers)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"UPSERT company_classification failed {resp.status_code}: {resp.text[:400]}")


def upsert_sales_org(rows: list[dict]) -> None:
    if not rows:
        return
    rows = dedup_by_key(rows, 'company_id')
    rows = normalize_batch(rows)
    url = f"{SUPABASE_URL}/rest/v1/company_sales_org?on_conflict=company_id"
    headers = {"Prefer": "return=minimal,resolution=merge-duplicates"}
    resp = SESSION.post(url, json=rows, headers=headers)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"UPSERT company_sales_org failed {resp.status_code}: {resp.text[:400]}")


def insert_contacts_batch(records: list[dict]) -> list[dict]:
    """Insert contacts, skip existing by email/linkedin. Returns inserted rows."""
    if not records:
        return []
    records = normalize_batch(records)
    url = f"{SUPABASE_URL}/rest/v1/contacts"
    headers = {"Prefer": "return=representation"}
    resp = SESSION.post(url, json=records, headers=headers)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"INSERT contacts failed {resp.status_code}: {resp.text[:400]}")
    inserted = resp.json()
    # Update caches
    for row in inserted:
        if row.get('email'):
            _contact_by_email[row['email'].lower()] = row['id']
        if row.get('linkedin_url'):
            _contact_by_linkedin[row['linkedin_url']] = row['id']
    return inserted


def insert_contact_companies_batch(rows: list[dict]) -> None:
    if not rows:
        return
    # Dedup by composite key (contact_id, company_id)
    seen = set()
    unique = []
    for r in rows:
        k = (r.get('contact_id'), r.get('company_id'))
        if k not in seen and None not in k:
            seen.add(k)
            unique.append(r)
    if not unique:
        return
    url = f"{SUPABASE_URL}/rest/v1/contact_companies?on_conflict=contact_id,company_id"
    headers = {"Prefer": "return=minimal,resolution=merge-duplicates"}
    resp = SESSION.post(url, json=unique, headers=headers)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"INSERT contact_companies failed {resp.status_code}: {resp.text[:400]}")


# ─── Type A processing ────────────────────────────────────────────────────────────

def process_type_a(filepath: Path):
    log.info(f"\n{'='*60}")
    log.info(f"Processing Type A: {filepath.name}")
    df = pd.read_csv(filepath, dtype=str)
    log.info(f"  Rows: {len(df)}")

    co_records = []        # for companies table
    firm_rows = []         # company_firmographics
    social_rows = []       # company_social
    class_rows = []        # company_classification
    sales_org_rows = []    # company_sales_org
    contact_records = []   # contacts (with domain for M2M linking)

    for idx, row in df.iterrows():
        if idx > 0 and idx % LOG_EVERY == 0:
            log.info(f"  Scanned {idx}/{len(df)} rows...")

        # ── Domain
        domain = normalize_domain(get_col(row, 'Website', 'Company Domain'))
        if not domain:
            continue

        # ── Company record
        name = get_col(row, 'Company Name', 'Company Name for Emails')
        co_records.append({
            "domain": domain,
            "name": name,
            "research_status": "raw",
        })

        # ── Firmographics
        firm = {"company_domain": domain}  # we'll resolve to company_id later
        employees = coerce_int(row.get('# Employees'))
        if employees is not None:
            firm['employees_count'] = employees
        funding = coerce_numeric(row.get('Total Funding'))
        if funding is not None:
            firm['funding_total'] = funding
        revenue = coerce_numeric(row.get('Annual Revenue'))
        if revenue is not None:
            firm['annual_revenue_estimate'] = revenue
        founded = coerce_year(row.get('Founded Year'))
        if founded is not None:
            firm['founded_year'] = founded
        city = get_col(row, 'Company City')
        if city:
            firm['hq_city'] = city
        country = get_col(row, 'Company Country')
        if country:
            firm['hq_country'] = country
        if len(firm) > 1:
            firm_rows.append(firm)

        # ── Social
        linkedin = get_col(row, 'Company Linkedin Url')
        if linkedin:
            social_rows.append({"company_domain": domain, "linkedin_url": linkedin})

        # ── Classification
        cls = {"company_domain": domain}
        desc = get_col(row, 'Short Description')
        if desc:
            cls['description'] = desc
        vertical = get_col(row, 'Industry')
        if vertical:
            cls['vertical'] = vertical
        if len(cls) > 1:
            class_rows.append(cls)

        # ── Sales org
        so = {"company_domain": domain}
        revops = coerce_int(get_col(row, 'Sales/revops headcount – Merged', 'Sales/rev ops headcount'))
        if revops is not None:
            so['revops_headcount'] = revops
        sales = coerce_int(get_col(row, 'Line Sales Headcount – Merged', 'Line sales headcount'))
        if sales is not None:
            so['sales_headcount'] = sales
        if len(so) > 1:
            sales_org_rows.append(so)

        # ── Contacts embedded in Type A files
        email = get_col(row, 'Work Email')
        linkedin_p = get_col(row, 'Personal Linkedin Url')
        first = get_col(row, 'First Name')
        last = get_col(row, 'Last Name')
        full = get_col(row, 'Full Name')
        title = get_col(row, 'Job Title')

        if email or linkedin_p:
            contact_records.append({
                "email": email.lower() if email else None,
                "linkedin_url": linkedin_p,
                "first_name": first,
                "last_name": last,
                "name": full,
                "title": title,
                "company_domain": domain,
            })

    log.info(f"  Companies to upsert: {len(co_records)}")
    log.info(f"  Firmographic rows:   {len(firm_rows)}")
    log.info(f"  Social rows:         {len(social_rows)}")
    log.info(f"  Classification rows: {len(class_rows)}")
    log.info(f"  Sales org rows:      {len(sales_org_rows)}")
    log.info(f"  Contact records:     {len(contact_records)}")

    # ── Insert companies in batches → build domain→id map
    co_records = dedup_by_key(co_records, 'domain')
    log.info("  Upserting companies...")
    for batch in chunks(co_records, BATCH_SIZE):
        insert_companies_batch(batch)

    # ── Resolve domain → company_id for satellite tables
    def resolve_rows(rows_with_domain, id_field='company_id'):
        resolved = []
        for r in rows_with_domain:
            d = r.pop('company_domain', None)
            cid = _company_cache.get(d) if d else None
            if not cid:
                cid = ensure_company(d)
            if cid:
                r[id_field] = cid
                resolved.append(r)
        return resolved

    firm_resolved = dedup_by_key(resolve_rows(firm_rows), 'company_id')
    social_resolved = dedup_by_key(resolve_rows(social_rows), 'company_id')
    class_resolved = dedup_by_key(resolve_rows(class_rows), 'company_id')
    so_resolved = dedup_by_key(resolve_rows(sales_org_rows), 'company_id')

    log.info("  Upserting firmographics...")
    for batch in chunks(firm_resolved, BATCH_SIZE):
        upsert_firmographics(batch)

    log.info("  Upserting social...")
    for batch in chunks(social_resolved, BATCH_SIZE):
        upsert_social(batch)

    log.info("  Upserting classification...")
    for batch in chunks(class_resolved, BATCH_SIZE):
        upsert_classification(batch)

    log.info("  Upserting sales_org...")
    for batch in chunks(so_resolved, BATCH_SIZE):
        upsert_sales_org(batch)

    # ── Contacts
    log.info("  Inserting contacts...")
    cc_links = []  # (contact_id, company_id)
    new_contacts = []
    seen_in_batch: set[str] = set()

    for c in contact_records:
        email = c.get('email')
        linkedin = c.get('linkedin_url')
        domain = c.pop('company_domain', None)

        # Deduplicate
        dedup_key = None
        if email:
            dedup_key = f"email:{email.lower()}"
            if dedup_key in seen_in_batch or email.lower() in _contact_by_email:
                # Already seen — still try to link company
                existing_id = _contact_by_email.get(email.lower())
                if existing_id and domain:
                    cid = _company_cache.get(domain) or ensure_company(domain)
                    if cid:
                        cc_links.append({"contact_id": existing_id, "company_id": cid, "is_primary": True})
                continue
        elif linkedin:
            dedup_key = f"li:{linkedin}"
            if dedup_key in seen_in_batch or linkedin in _contact_by_linkedin:
                existing_id = _contact_by_linkedin.get(linkedin)
                if existing_id and domain:
                    cid = _company_cache.get(domain) or ensure_company(domain)
                    if cid:
                        cc_links.append({"contact_id": existing_id, "company_id": cid, "is_primary": True})
                continue
        else:
            continue  # no email or linkedin — skip

        if dedup_key:
            seen_in_batch.add(dedup_key)

        record = {k: v for k, v in c.items() if k != 'company_domain' and v is not None}
        record['_domain'] = domain
        new_contacts.append(record)

    # Insert in batches
    for batch in chunks(new_contacts, BATCH_SIZE):
        domains = [r.pop('_domain', None) for r in batch]
        inserted = insert_contacts_batch(batch)
        for i, row in enumerate(inserted):
            cid = _company_cache.get(domains[i]) if i < len(domains) else None
            if not cid and i < len(domains):
                cid = ensure_company(domains[i])
            if row.get('id') and cid:
                cc_links.append({"contact_id": row['id'], "company_id": cid, "is_primary": True})

    # Dedup cc_links globally before batching
    seen_cc = set()
    cc_links_deduped = []
    for link in cc_links:
        k = (link.get('contact_id'), link.get('company_id'))
        if None not in k and k not in seen_cc:
            seen_cc.add(k)
            cc_links_deduped.append(link)
    log.info(f"  Linking {len(cc_links_deduped)} contact-company pairs...")
    for batch in chunks(cc_links_deduped, BATCH_SIZE):
        insert_contact_companies_batch(batch)

    log.info(f"Done with {filepath.name}")


# ─── Type B processing ────────────────────────────────────────────────────────────

def process_type_b(filepath: Path):
    log.info(f"\n{'='*60}")
    log.info(f"Processing Type B: {filepath.name}")
    df = pd.read_csv(filepath, dtype=str)
    log.info(f"  Rows: {len(df)}")

    contact_records = []
    sales_org_rows = []

    for idx, row in df.iterrows():
        if idx > 0 and idx % LOG_EVERY == 0:
            log.info(f"  Scanned {idx}/{len(df)} rows...")

        # Domain
        domain = normalize_domain(get_col(row, 'Company Domain', 'Website'))

        # Contact fields
        email = get_col(row, 'Work Email', 'Work Email – Apollo', 'Email - Person')
        if email:
            email = email.lower()
        linkedin = get_col(row, 'LinkedIn Profile')
        first = get_col(row, 'First Name')
        last = get_col(row, 'Last Name')
        full = get_col(row, 'Full Name')
        title = get_col(row, 'Job Title', 'Title - Person')
        company_name = get_col(row, 'Company Name')

        if not email and not linkedin:
            # check if we at least have company data worth processing
            pass
        else:
            contact_records.append({
                "email": email,
                "linkedin_url": linkedin,
                "first_name": first,
                "last_name": last,
                "name": full,
                "title": title,
                "company_domain": domain,
                "company_name": company_name,
            })

        # Sales org data from Type B
        if domain:
            so = {"company_domain": domain}
            revops = coerce_int(get_col(row, 'Sales/revops headcount – Merged', 'Sales/rev ops headcount'))
            if revops is not None:
                so['revops_headcount'] = revops
            sales = coerce_int(get_col(row, 'Line Sales Headcount – Merged', 'Line sales headcount'))
            if sales is not None:
                so['sales_headcount'] = sales
            if len(so) > 1:
                sales_org_rows.append(so)

    log.info(f"  Contact candidates:  {len(contact_records)}")
    log.info(f"  Sales org updates:   {len(sales_org_rows)}")

    # ── Ensure companies exist for all domains
    unique_domains = {r['company_domain'] for r in contact_records if r.get('company_domain')}
    unique_domains |= {r['company_domain'] for r in sales_org_rows if r.get('company_domain')}
    missing = [d for d in unique_domains if d and d not in _company_cache]
    if missing:
        log.info(f"  Ensuring {len(missing)} new company domains...")
        # Collect names from contacts
        domain_to_name = {}
        for r in contact_records:
            d = r.get('company_domain')
            if d and d not in domain_to_name and r.get('company_name'):
                domain_to_name[d] = r['company_name']
        mini_records = dedup_by_key([
            {"domain": d, "name": domain_to_name.get(d), "research_status": "raw"}
            for d in missing
        ], 'domain')
        for batch in chunks(mini_records, BATCH_SIZE):
            insert_companies_batch(batch)
        # Fetch any still missing
        still_missing = [d for d in missing if d not in _company_cache]
        if still_missing:
            for d in still_missing:
                ensure_company(d)

    # ── Upsert sales org
    so_resolved = []
    for r in sales_org_rows:
        d = r.pop('company_domain', None)
        cid = _company_cache.get(d)
        if cid:
            r['company_id'] = cid
            so_resolved.append(r)
    so_resolved = dedup_by_key(so_resolved, 'company_id')
    log.info(f"  Upserting {len(so_resolved)} sales_org rows...")
    for batch in chunks(so_resolved, BATCH_SIZE):
        upsert_sales_org(batch)

    # ── Insert contacts
    log.info("  Inserting contacts...")
    cc_links = []
    new_contacts = []
    seen_in_batch: set[str] = set()

    for c in contact_records:
        email = c.get('email')
        linkedin = c.get('linkedin_url')
        domain = c.get('company_domain')

        dedup_key = None
        if email:
            dedup_key = f"email:{email}"
            if dedup_key in seen_in_batch or email in _contact_by_email:
                existing_id = _contact_by_email.get(email)
                if existing_id and domain:
                    cid = _company_cache.get(domain)
                    if cid:
                        cc_links.append({"contact_id": existing_id, "company_id": cid, "is_primary": True})
                continue
        elif linkedin:
            dedup_key = f"li:{linkedin}"
            if dedup_key in seen_in_batch or linkedin in _contact_by_linkedin:
                existing_id = _contact_by_linkedin.get(linkedin)
                if existing_id and domain:
                    cid = _company_cache.get(domain)
                    if cid:
                        cc_links.append({"contact_id": existing_id, "company_id": cid, "is_primary": True})
                continue
        else:
            continue

        if dedup_key:
            seen_in_batch.add(dedup_key)

        record = {
            k: v for k, v in c.items()
            if k not in ('company_domain', 'company_name') and v is not None
        }
        record['_domain'] = domain
        new_contacts.append(record)

    for batch in chunks(new_contacts, BATCH_SIZE):
        domains = [r.pop('_domain', None) for r in batch]
        inserted = insert_contacts_batch(batch)
        for i, row in enumerate(inserted):
            d = domains[i] if i < len(domains) else None
            cid = _company_cache.get(d) if d else None
            if row.get('id') and cid:
                cc_links.append({"contact_id": row['id'], "company_id": cid, "is_primary": True})

    seen_cc = set()
    cc_links_deduped = []
    for link in cc_links:
        k = (link.get('contact_id'), link.get('company_id'))
        if None not in k and k not in seen_cc:
            seen_cc.add(k)
            cc_links_deduped.append(link)
    log.info(f"  Linking {len(cc_links_deduped)} contact-company pairs...")
    for batch in chunks(cc_links_deduped, BATCH_SIZE):
        insert_contact_companies_batch(batch)

    log.info(f"Done with {filepath.name}")


# ─── Final stats ──────────────────────────────────────────────────────────────────

def report_stats():
    log.info("\n" + "="*60)
    log.info("FINAL STATS")
    log.info("="*60)

    def count(table, filter_col=None, filter_val=None):
        url = f"{SUPABASE_URL}/rest/v1/{table}?select=id&limit=1"
        if filter_col:
            url += f"&{filter_col}=eq.{filter_val}"
        resp = SESSION.head(url, headers={"Prefer": "count=exact"})
        return resp.headers.get("content-range", "?/?").split("/")[-1]

    # Total companies
    log.info(f"Total companies: {count('companies')}")

    # Companies by research_status
    for status in ['raw', 'prefiltered', 'classified', 'scored', 'contacted', 'skip']:
        n = count('companies', 'research_status', status)
        if n != '0':
            log.info(f"  companies[{status}]: {n}")

    # Total contacts
    log.info(f"Total contacts: {count('contacts')}")

    # Contacts with email — query for non-null email
    url = f"{SUPABASE_URL}/rest/v1/contacts?select=id&email=not.is.null&limit=1"
    resp = SESSION.head(url, headers={"Prefer": "count=exact"})
    log.info(f"Contacts with email: {resp.headers.get('content-range','?/?').split('/')[-1]}")

    log.info(f"contact_companies links: {count('contact_companies')}")
    log.info(f"company_firmographics rows: {count('company_firmographics')}")
    log.info(f"company_classification rows: {count('company_classification')}")
    log.info(f"company_sales_org rows: {count('company_sales_org')}")


# ─── Main ──────────────────────────────────────────────────────────────────────────

def main():
    log.info("Starting Clay CSV import")
    log.info(f"Data dir: {DATA_DIR}")

    # Load existing data into memory caches
    load_company_cache()
    load_contact_cache()

    # Process Type A files
    for fname in TYPE_A_FILES:
        fpath = DATA_DIR / fname
        if fpath.exists():
            process_type_a(fpath)
        else:
            log.warning(f"File not found: {fpath}")

    # Reload company cache to pick up anything from Type A
    # (already updated in-memory during inserts — no need to reload)

    # Process Type B files
    for fname in TYPE_B_FILES:
        fpath = DATA_DIR / fname
        if fpath.exists():
            process_type_b(fpath)
        else:
            log.warning(f"File not found: {fpath}")

    report_stats()
    log.info("\nImport complete ✓")


if __name__ == "__main__":
    main()
