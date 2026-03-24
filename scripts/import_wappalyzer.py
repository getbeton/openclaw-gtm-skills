#!/usr/bin/env python3
"""
Import Wappalyzer PostHog CSV into Supabase.
Maps CSV columns to companies, company_social, company_firmographics, company_classification tables.
Skips domains already in the excluded contacts lists.

Key constraint: PostgREST requires ALL rows in a batch to have identical keys.
So we always include all columns with null for missing values.
"""

import csv
import re
import sys
import json
import time
import requests
import pandas as pd
from urllib.parse import urlparse

# ── Config ──────────────────────────────────────────────────────────────────
SUPABASE_URL = "YOUR_SUPABASE_URL"
SUPABASE_KEY = "YOUR_SUPABASE_SERVICE_KEY"

CSV_FILE = "YOUR_WORKSPACE_PATH/beton/leadgen/wappalyzer_export/posthog.csv"

EXCLUDED_FILES = [
    ("YOUR_WORKSPACE_PATH/beton/sketches/sales-research/data/RevOps-Export-table-Ready-for-export-export-1769631057542.csv", "Company Domain"),
    ("YOUR_WORKSPACE_PATH/beton/sketches/sales-research/data/rev-ops-Default-view-export-1769800749934.csv", "Company Domain"),
    ("YOUR_WORKSPACE_PATH/beton/sketches/sales-research/data/rev-ops-Default-view-export-1769800859679.csv", "Company Domain"),
    ("YOUR_WORKSPACE_PATH/beton/sketches/sales-research/data/rev-ops-Default-view-export-1771277703515.csv", "Company Domain"),
    ("YOUR_WORKSPACE_PATH/beton/sketches/sales-research/data/sales-Ops-Default-view-export-1769800769445.csv", "Company Domain"),
    ("YOUR_WORKSPACE_PATH/beton/sketches/sales-research/data/sales-Ops-Default-view-export-1771277696960.csv", "Company Domain"),
]

MASTER_PARQUET = "YOUR_WORKSPACE_PATH/beton/sketches/sales-research/data/master.parquet"

BATCH_SIZE = 500

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=ignore-duplicates,return=representation",
}

# ── Domain normalisation ─────────────────────────────────────────────────────
IP_RE = re.compile(r"^\d{1,3}(\.\d{1,3}){3}$")

def normalise_domain(url: str):
    """Strip protocol, www., path, query, fragment. Return lowercase domain or None."""
    if not url or not url.strip():
        return None
    url = url.strip()
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        domain = domain.split(":")[0]
        if domain.startswith("www."):
            domain = domain[4:]
        if not domain or "." not in domain or IP_RE.match(domain):
            return None
        return domain
    except Exception:
        return None


def normalise_domain_raw(raw: str):
    """Normalise a raw domain (no protocol) from excluded files."""
    if not raw or not raw.strip():
        return None
    raw = raw.strip().lower()
    if raw.startswith(("http://", "https://")):
        return normalise_domain(raw)
    if raw.startswith("www."):
        raw = raw[4:]
    raw = raw.split("/")[0]
    if not raw or "." not in raw or IP_RE.match(raw):
        return None
    return raw


# ── Load excluded domains ────────────────────────────────────────────────────
def load_excluded_domains():
    excluded = set()
    for filepath, col_name in EXCLUDED_FILES:
        try:
            with open(filepath, newline="", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                headers = reader.fieldnames or []
                actual_col = col_name if col_name in headers else None
                if actual_col is None:
                    print(f"  ⚠ Column '{col_name}' not found in {filepath.split('/')[-1]}")
                    print(f"    Available: {headers}")
                    for h in headers:
                        if "domain" in h.lower():
                            actual_col = h
                            print(f"    Using '{actual_col}' instead")
                            break
                if actual_col is None:
                    print(f"  ✗ Skipping — no domain column found")
                    continue
                count = 0
                for row in reader:
                    d = normalise_domain_raw(row.get(actual_col, ""))
                    if d:
                        excluded.add(d)
                        count += 1
                print(f"  ✓ {filepath.split('/')[-1]}: {count} domains")
        except FileNotFoundError:
            print(f"  ✗ File not found: {filepath}")

    try:
        df = pd.read_parquet(MASTER_PARQUET)
        if "company_domain" in df.columns:
            count = 0
            for d in df["company_domain"].dropna():
                nd = normalise_domain_raw(str(d))
                if nd:
                    excluded.add(nd)
                    count += 1
            print(f"  ✓ master.parquet: {count} domains")
        else:
            print(f"  ⚠ master.parquet: no 'company_domain' column")
    except Exception as e:
        print(f"  ✗ master.parquet: {e}")

    return excluded


# ── Value helpers ────────────────────────────────────────────────────────────
def to_int(val):
    if val is None or str(val).strip() == "":
        return None
    try:
        return int(str(val).strip().replace(",", "").split(".")[0])
    except Exception:
        return None


def to_float(val):
    if val is None or str(val).strip() == "":
        return None
    s = re.sub(r"[$,\s]", "", str(val).strip())
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def to_bool_nonempty(val):
    return bool(val and str(val).strip())


def first_nonempty(*vals):
    for v in vals:
        if v and str(v).strip():
            return str(v).strip()
    return None


def first_city(locations: str):
    if not locations or not locations.strip():
        return None
    parts = [p.strip() for p in locations.split(";")]
    return parts[0] if parts else None


def sv(val):
    """String value or None."""
    if val is None:
        return None
    s = str(val).strip()
    return s if s else None


# ── Supabase helpers ─────────────────────────────────────────────────────────
def supabase_upsert(table: str, rows: list, retry: int = 3):
    """POST batch to Supabase. Returns {"inserted": n, "errors": n}"""
    if not rows:
        return {"inserted": 0, "errors": 0}
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    for attempt in range(retry):
        try:
            resp = requests.post(url, headers=HEADERS, json=rows, timeout=90)
            if resp.status_code in (200, 201):
                data = resp.json()
                return {"inserted": len(data) if isinstance(data, list) else 0, "errors": 0}
            elif resp.status_code == 409:
                return {"inserted": 0, "errors": 0}
            else:
                if attempt < retry - 1:
                    time.sleep(2 ** attempt)
                else:
                    print(f"  ✗ {table} HTTP {resp.status_code}: {resp.text[:300]}")
                    return {"inserted": 0, "errors": len(rows)}
        except Exception as e:
            if attempt < retry - 1:
                time.sleep(2 ** attempt)
            else:
                print(f"  ✗ {table} request error: {e}")
                return {"inserted": 0, "errors": len(rows)}
    return {"inserted": 0, "errors": len(rows)}


def fetch_company_ids(domains: list):
    """Fetch domain→id map for a list of domains."""
    if not domains:
        return {}
    result = {}
    # Chunk to avoid URL length limits
    for i in range(0, len(domains), 100):
        chunk = domains[i:i+100]
        in_val = "(" + ",".join(chunk) + ")"
        url = f"{SUPABASE_URL}/rest/v1/companies"
        params = {"domain": f"in.{in_val}", "select": "id,domain"}
        try:
            resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
            if resp.status_code == 200:
                for row in resp.json():
                    result[row["domain"]] = row["id"]
            else:
                print(f"  ⚠ fetch_company_ids HTTP {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            print(f"  ✗ fetch_company_ids error: {e}")
    return result


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    print("═" * 60)
    print("Wappalyzer → Supabase Import")
    print("═" * 60)

    print("\n[1/4] Loading excluded domains...")
    excluded = load_excluded_domains()
    print(f"  Total unique excluded domains: {len(excluded)}")

    print("\n[2/4] Reading CSV...")
    rows_all = []
    with open(CSV_FILE, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows_all.append(row)
    print(f"  CSV rows: {len(rows_all)}")

    print("\n[3/4] Processing & inserting...")

    stats = {
        "total": len(rows_all),
        "inserted_companies": 0,
        "skipped_duplicate": 0,
        "skipped_excluded": 0,
        "skipped_bad_domain": 0,
        "errors": 0,
    }

    # Accumulate batches
    company_batch = []
    # Store companion data keyed by domain position in batch
    companion_data = {}  # domain -> {social, firmog, classif}

    def flush_batch():
        if not company_batch:
            return

        # Insert companies — all rows must have same keys
        # Ensure uniform keys
        all_keys = {"domain", "name", "research_status"}
        uniform_companies = []
        for r in company_batch:
            uniform_companies.append({k: r.get(k) for k in all_keys})

        result = supabase_upsert("companies", uniform_companies)
        stats["inserted_companies"] += result["inserted"]
        # Duplicates = batch_size - inserted - errors
        stats["skipped_duplicate"] += len(company_batch) - result["inserted"] - result["errors"]
        stats["errors"] += result["errors"]

        # Fetch IDs
        domains_in_batch = [r["domain"] for r in company_batch]
        domain_to_id = fetch_company_ids(domains_in_batch)

        # Build related rows with uniform keys
        social_rows = []
        firmog_rows = []
        classif_rows = []

        for domain in domains_in_batch:
            cid = domain_to_id.get(domain)
            if not cid or domain not in companion_data:
                continue
            cd = companion_data[domain]

            s = cd["social"]
            s["company_id"] = cid
            social_rows.append(s)

            f = cd["firmog"]
            f["company_id"] = cid
            firmog_rows.append(f)

            c = cd["classif"]
            c["company_id"] = cid
            classif_rows.append(c)

        if social_rows:
            supabase_upsert("company_social", social_rows)
        if firmog_rows:
            supabase_upsert("company_firmographics", firmog_rows)
        if classif_rows:
            supabase_upsert("company_classification", classif_rows)

        company_batch.clear()
        companion_data.clear()

    for idx, row in enumerate(rows_all):
        if (idx + 1) % 1000 == 0:
            print(f"  Progress: {idx+1}/{stats['total']} | "
                  f"inserted={stats['inserted_companies']} "
                  f"excluded={stats['skipped_excluded']} "
                  f"bad_domain={stats['skipped_bad_domain']} "
                  f"duplicate≈{stats['skipped_duplicate']}")

        # Normalise domain
        domain = normalise_domain(row.get("URL", ""))
        if not domain:
            stats["skipped_bad_domain"] += 1
            continue

        if domain in excluded:
            stats["skipped_excluded"] += 1
            continue

        # Company row — always all keys
        company_batch.append({
            "domain": domain,
            "name": first_nonempty(
                row.get("Company name"),
                row.get("Inferred company name"),
                row.get("Title"),
            ),
            "research_status": "raw",
        })

        # company_social — always all keys
        companion_data[domain] = {
            "social": {
                "company_id": None,  # filled after insert
                "linkedin_url": sv(row.get("LinkedIn")),
                "twitter_url": sv(row.get("Twitter")),
                "github_url": sv(row.get("GitHub")),
                "website_email": sv(row.get("Email address (safe)")),
                "spf_record": to_bool_nonempty(row.get("SPF record")),
                "dmarc_record": to_bool_nonempty(row.get("DMARC record")),
            },
            # company_firmographics
            "firmog": {
                "company_id": None,
                "employees_range": sv(row.get("Company size")),
                "funding_total": to_float(row.get("Funding total")),
                "funding_rounds": to_int(row.get("Funding rounds")),
                "founded_year": to_int(row.get("Company founded")),
                "hq_country": sv(row.get("Country")),
                "hq_city": first_city(row.get("Locations", "")),
                "traffic_rank": to_int(row.get("Traffic rank")),
                "company_type": sv(row.get("Company type")),
            },
            # company_classification
            "classif": {
                "company_id": None,
                "description": sv(row.get("About")),
                "vertical": sv(row.get("Industry")),
            },
        }

        if len(company_batch) >= BATCH_SIZE:
            flush_batch()

    flush_batch()  # final flush

    print("\n[4/4] Done!")
    print("═" * 60)
    print(f"  Total rows processed:   {stats['total']}")
    print(f"  Companies inserted:     {stats['inserted_companies']}")
    print(f"  Skipped (duplicate):    {stats['skipped_duplicate']}")
    print(f"  Skipped (excluded):     {stats['skipped_excluded']}")
    print(f"  Skipped (bad domain):   {stats['skipped_bad_domain']}")
    print(f"  Errors:                 {stats['errors']}")
    print("═" * 60)


if __name__ == "__main__":
    main()
