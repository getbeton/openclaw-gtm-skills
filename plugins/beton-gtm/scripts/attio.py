"""
Attio API upsert functions for beton-gtm pipeline.

Handles company, person, and deal records.
API docs: https://developers.attio.com/reference

API key is hardcoded here for convenience; also reads from ATTIO_API_KEY env var.
"""

import os
import logging
from typing import Any, Optional
import httpx

logger = logging.getLogger(__name__)

ATTIO_API_KEY = os.getenv(
    "ATTIO_API_KEY",
    "YOUR_ATTIO_API_KEY",
)
ATTIO_BASE_URL = "https://api.attio.com/v2"

# ============================================================
# Known Attio attribute slugs
# ============================================================

# Company attributes that EXIST in Attio workspace (safe to write)
COMPANY_ATTRS_EXISTING = {
    "name",
    "domains",
    "description",
    "linkedin",
    "twitter",
    "logo_url",
    "headcount_total",
    "headcount_sales",
    "technologies",
    "annual_revenue",
    "total_funding",
    "latest_funding_amount",
    "is_using_posthog",       # checkbox
    "gtm_motion_type",        # select
    "b2b",                    # checkbox
    "saas",                   # checkbox
    "is_doing_cold_outreach",  # checkbox
    "annual_money_lost_formula",  # currency
    "crm",                    # select
    "dwh",                    # select
    "source",                 # select
    "enrichment_vendor_3",    # text
}

# Company attributes that NEED to be created manually in Attio first
# These will be skipped with a warning until created
COMPANY_ATTRS_PENDING = {
    "segment",          # text
    "vertical",         # text
    "business_model",   # text
    "sells_to",         # text
    "pricing_model",    # text
    "research_status",  # select
    "fit_score",        # number
}


# ============================================================
# HTTP client
# ============================================================

def _client() -> httpx.Client:
    return httpx.Client(
        base_url=ATTIO_BASE_URL,
        headers={
            "Authorization": f"Bearer {ATTIO_API_KEY}",
            "Content-Type": "application/json",
        },
        timeout=30.0,
    )


def _build_attio_value(slug: str, value: Any) -> dict:
    """
    Wrap a value in Attio's attribute value format.
    Attio uses different wrappers depending on attribute type.
    """
    if value is None:
        return None

    # Checkbox attributes → wrap in {"value": bool}
    checkbox_attrs = {
        "is_using_posthog", "b2b", "saas", "is_doing_cold_outreach"
    }
    # Select attributes → wrap in {"option": "value"}
    select_attrs = {"gtm_motion_type", "crm", "dwh", "source", "research_status"}
    # Currency attributes
    currency_attrs = {"annual_money_lost_formula"}
    # Array attributes (domains, technologies)
    array_attrs = {"domains", "technologies"}

    if slug in checkbox_attrs:
        return [{"value": bool(value)}]
    elif slug in select_attrs:
        return [{"option": str(value)}]
    elif slug in currency_attrs:
        return [{"currency_value": float(value), "currency_code": "USD"}]
    elif slug in array_attrs:
        if isinstance(value, list):
            return [{"original_value": v} for v in value]
        return [{"original_value": str(value)}]
    elif slug == "domains":
        # Domain entries
        if isinstance(value, list):
            return [{"original_domain": d} for d in value]
        return [{"original_domain": str(value)}]
    else:
        # Default: text/number
        return [{"value": value}]


def _filter_attrs(data: dict, warn_pending: bool = True) -> dict:
    """
    Split attributes into safe (existing) and pending (not yet created).
    Returns only safe attributes for the API call.
    """
    safe = {}
    for k, v in data.items():
        if k in COMPANY_ATTRS_EXISTING:
            safe[k] = v
        elif k in COMPANY_ATTRS_PENDING:
            if warn_pending:
                logger.warning(
                    f"Attio attribute '{k}' hasn't been created yet — skipping. "
                    f"Create it manually in Attio workspace settings."
                )
        else:
            logger.debug(f"Unknown Attio attribute '{k}' — skipping")
    return safe


# ============================================================
# Company upsert
# ============================================================

def upsert_company(domain: str, data: dict) -> dict:
    """
    Upsert a company record in Attio by domain.

    Args:
        domain: canonical domain (e.g. "acme.com")
        data: dict with Attio attribute slugs as keys

    Returns:
        Attio record dict (id + attributes)
    """
    safe_data = _filter_attrs(data)

    # Build attributes payload
    attributes = {"domains": [{"domain": domain}]}
    for slug, value in safe_data.items():
        if slug == "domains":
            continue  # already set above
        built = _build_attio_value(slug, value)
        if built is not None:
            attributes[slug] = built

    payload = {"data": {"values": attributes}}

    with _client() as client:
        resp = client.put(
            "/objects/companies/records",
            params={"matching_attribute": "domains"},
            json=payload,
        )

    if resp.status_code in (200, 201):
        record = resp.json().get("data", {})
        logger.info(f"Attio company upserted: {domain} → {record.get('id', {}).get('record_id')}")
        return record
    else:
        logger.error(f"Attio company upsert failed for {domain}: {resp.status_code} {resp.text[:500]}")
        resp.raise_for_status()


def company_from_research(domain: str, company: dict) -> dict:
    """
    Build Attio company data dict from our internal research format.

    Args:
        domain: canonical domain
        company: companies table row (with classification, sales_org, tech_stack, firmographic)

    Returns:
        data dict ready for upsert_company()
    """
    classification = company.get("classification") or {}
    sales_org = company.get("sales_org") or {}
    tech_stack = company.get("tech_stack") or {}
    firmographic = company.get("firmographic") or {}

    data: dict[str, Any] = {}

    # Basic
    if company.get("name"):
        data["name"] = company["name"]

    data["domains"] = [domain]

    # Firmographic
    if firmographic.get("employees"):
        data["headcount_total"] = firmographic["employees"]
    if firmographic.get("revenue"):
        data["annual_revenue"] = firmographic["revenue"]
    if firmographic.get("total_funding"):
        data["total_funding"] = firmographic["total_funding"]
    if firmographic.get("latest_funding"):
        data["latest_funding_amount"] = firmographic["latest_funding"]

    # Classification
    if classification.get("b2b") is not None:
        data["b2b"] = classification["b2b"]
    if classification.get("saas") is not None:
        data["saas"] = classification["saas"]
    if classification.get("gtmMotion"):
        data["gtm_motion_type"] = classification["gtmMotion"]

    # Sales org → headcount_sales
    if sales_org.get("salesHeadcount"):
        data["headcount_sales"] = sales_org["salesHeadcount"]

    # Tech stack → technologies
    tech_list = []
    for key in ["crm", "salesEngagementTool", "dataTools", "analytics"]:
        val = tech_stack.get(key)
        if val:
            if isinstance(val, list):
                tech_list.extend(val)
            else:
                tech_list.append(val)
    if tech_list:
        data["technologies"] = tech_list

    # CRM select
    if tech_stack.get("crm"):
        data["crm"] = tech_stack["crm"]

    # Outreach flag
    data["is_doing_cold_outreach"] = True
    data["source"] = "beton_pipeline"

    # Pending attributes (won't be sent until created in Attio)
    if classification.get("vertical"):
        data["vertical"] = classification["vertical"]
    if classification.get("businessModel"):
        data["business_model"] = classification["businessModel"]
    if classification.get("sellsTo"):
        data["sells_to"] = classification["sellsTo"]
    if classification.get("pricingModel"):
        data["pricing_model"] = classification["pricingModel"]
    if company.get("fit_score") is not None:
        data["fit_score"] = company["fit_score"]
    if company.get("research_status"):
        data["research_status"] = company["research_status"]

    return data


# ============================================================
# Person (contact) upsert
# ============================================================

def upsert_person(email: str, data: dict, company_record_id: str = None) -> dict:
    """
    Upsert a person record in Attio by email.

    Args:
        email: contact email address
        data: dict with person attribute slugs (name, linkedin, title, etc.)
        company_record_id: Attio company record ID to link the person to

    Returns:
        Attio record dict
    """
    attributes: dict[str, Any] = {
        "email_addresses": [{"email_address": email}]
    }

    # Map our internal fields to Attio person attributes
    field_map = {
        "first_name": "first_name",
        "last_name": "last_name",
        "name": "name",
        "linkedin_url": "linkedin",
        "title": "job_title",
    }

    for our_key, attio_key in field_map.items():
        if data.get(our_key):
            attributes[attio_key] = [{"value": data[our_key]}]

    payload: dict[str, Any] = {"data": {"values": attributes}}

    with _client() as client:
        resp = client.put(
            "/objects/people/records",
            params={"matching_attribute": "email_addresses"},
            json=payload,
        )

    if resp.status_code in (200, 201):
        record = resp.json().get("data", {})
        person_record_id = record.get("id", {}).get("record_id")
        logger.info(f"Attio person upserted: {email} → {person_record_id}")

        # Link person to company if provided
        if company_record_id and person_record_id:
            _link_person_to_company(person_record_id, company_record_id, client_instance=None)

        return record
    else:
        logger.error(f"Attio person upsert failed for {email}: {resp.status_code} {resp.text[:500]}")
        resp.raise_for_status()


def _link_person_to_company(
    person_record_id: str,
    company_record_id: str,
    client_instance=None,
) -> Optional[dict]:
    """Create a relation between a person and a company in Attio."""
    payload = {
        "data": {
            "record_id": {"object": "companies", "record_id": company_record_id},
            "attribute": {"slug": "company"},
        }
    }

    def _do(client):
        resp = client.post(
            f"/objects/people/records/{person_record_id}/attributes/company/values",
            json=payload,
        )
        if resp.status_code in (200, 201):
            logger.debug(f"Linked person {person_record_id} → company {company_record_id}")
            return resp.json()
        else:
            logger.warning(f"Failed to link person to company: {resp.status_code} {resp.text[:300]}")
            return None

    if client_instance:
        return _do(client_instance)
    with _client() as c:
        return _do(c)


# ============================================================
# Deal upsert
# ============================================================

def upsert_deal(
    data: dict,
    company_record_id: str = None,
    person_record_id: str = None,
) -> dict:
    """
    Create a deal record in Attio.

    Note: Attio doesn't natively support deal dedup by external ID.
    This always POSTs a new record. Check for existing deals upstream
    if you want to avoid duplicates.

    Args:
        data: dict with deal attributes (name, stage, value, etc.)
        company_record_id: Attio company record ID to associate
        person_record_id: Attio person record ID to associate

    Returns:
        Attio record dict
    """
    attributes: dict[str, Any] = {}

    if data.get("name"):
        attributes["name"] = [{"value": data["name"]}]
    if data.get("stage"):
        attributes["stage"] = [{"option": data["stage"]}]
    if data.get("value"):
        attributes["value"] = [{"currency_value": float(data["value"]), "currency_code": "USD"}]
    if data.get("notes"):
        attributes["description"] = [{"value": data["notes"]}]

    # Associated records
    associated_records = []
    if company_record_id:
        associated_records.append({
            "object": "companies",
            "record_id": company_record_id,
        })
    if person_record_id:
        associated_records.append({
            "object": "people",
            "record_id": person_record_id,
        })

    payload: dict[str, Any] = {"data": {"values": attributes}}
    if associated_records:
        payload["data"]["associated_records"] = associated_records

    with _client() as client:
        resp = client.post("/objects/deals/records", json=payload)

    if resp.status_code in (200, 201):
        record = resp.json().get("data", {})
        logger.info(f"Attio deal created: {data.get('name')} → {record.get('id', {}).get('record_id')}")
        return record
    else:
        logger.error(f"Attio deal create failed: {resp.status_code} {resp.text[:500]}")
        resp.raise_for_status()


# ============================================================
# Batch sync helper
# ============================================================

def sync_company_to_attio(company: dict) -> Optional[str]:
    """
    Convenience wrapper: takes a full companies row, syncs to Attio,
    returns the Attio record_id (or None on failure).
    """
    domain = company.get("domain")
    if not domain:
        logger.error("sync_company_to_attio: no domain in company record")
        return None

    try:
        data = company_from_research(domain, company)
        record = upsert_company(domain, data)
        record_id = record.get("id", {}).get("record_id")
        return record_id
    except Exception as e:
        logger.error(f"Failed to sync {domain} to Attio: {e}")
        return None
