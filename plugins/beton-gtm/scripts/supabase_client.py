"""
Supabase client + helper functions for beton-gtm pipeline.

Reads connection config from environment variables:
  SUPABASE_URL = os.getenv("SUPABASE_URL", "YOUR_SUPABASE_URL")
  SUPABASE_KEY = os.getenv("SUPABASE_KEY", "YOUR_SUPABASE_SERVICE_KEY")

Or from OpenClaw plugin config passed as kwargs.
"""

import os
import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional
import httpx

logger = logging.getLogger(__name__)


# ============================================================
# Client
# ============================================================

def _load_local_config() -> dict:
    """Load config.local.json from the plugin dir if present (local-only, not committed)."""
    config_path = os.path.join(os.path.dirname(__file__), "..", "config.local.json")
    try:
        with open(os.path.abspath(config_path)) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

_local_config = _load_local_config()
SUPABASE_URL_DEFAULT = _local_config.get("supabaseUrl", "YOUR_SUPABASE_URL")
SUPABASE_KEY_DEFAULT = _local_config.get("supabaseKey", "YOUR_SUPABASE_SERVICE_KEY")

class SupabaseClient:
    def __init__(self, url: str = None, key: str = None):
        self.url = url or os.getenv("SUPABASE_URL", SUPABASE_URL_DEFAULT)
        self.key = key or os.getenv("SUPABASE_KEY", SUPABASE_KEY_DEFAULT)
        if not self.url or not self.key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set")
        self.headers = {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        }
        self._client = httpx.Client(
            base_url=f"{self.url}/rest/v1",
            headers=self.headers,
            timeout=30.0,
        )

    def _url(self, table: str) -> str:
        return f"{self.url}/rest/v1/{table}"

    # ----------------------------------------------------------
    # Generic CRUD
    # ----------------------------------------------------------

    def select(
        self,
        table: str,
        filters: dict[str, Any] = None,
        columns: str = "*",
        limit: int = None,
        order: str = None,
    ) -> list[dict]:
        """Fetch rows with automatic pagination (Supabase caps at 1000/page)."""
        PAGE_SIZE = 1000
        params = {"select": columns}
        if filters:
            for k, v in filters.items():
                params[k] = f"eq.{v}"
        if order:
            params["order"] = order

        collected = []
        offset = 0
        while True:
            page_params = {**params, "limit": PAGE_SIZE, "offset": offset}
            resp = self._client.get(f"/{table}", params=page_params)
            resp.raise_for_status()
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

    def select_raw(self, table: str, query_params: dict) -> list[dict]:
        """Pass PostgREST query params directly (single page, caller controls pagination)."""
        params = {"select": "*", **query_params}
        resp = self._client.get(f"/{table}", params=params)
        resp.raise_for_status()
        return resp.json()

    def insert(self, table: str, data: dict | list[dict], upsert: bool = False) -> list[dict]:
        headers = {}
        if upsert:
            headers["Prefer"] = "resolution=merge-duplicates,return=representation"
        resp = self._client.post(f"/{table}", json=data, headers=headers)
        resp.raise_for_status()
        return resp.json()

    def update(self, table: str, filters: dict[str, Any], data: dict) -> list[dict]:
        params = {}
        for k, v in filters.items():
            params[k] = f"eq.{v}"
        resp = self._client.patch(f"/{table}", params=params, json=data)
        resp.raise_for_status()
        return resp.json()

    def rpc(self, function_name: str, params: dict = None) -> Any:
        resp = self._client.post(f"/rpc/{function_name}", json=params or {})
        resp.raise_for_status()
        return resp.json()

    # ----------------------------------------------------------
    # Companies
    # ----------------------------------------------------------

    def get_company_by_domain(self, domain: str) -> Optional[dict]:
        rows = self.select("companies", filters={"domain": domain})
        return rows[0] if rows else None

    def get_company_by_id(self, company_id: str) -> Optional[dict]:
        rows = self.select("companies", filters={"id": company_id})
        return rows[0] if rows else None

    def upsert_company(self, domain: str, data: dict) -> dict:
        """Insert or update company by domain."""
        payload = {"domain": domain, **data, "updated_at": _now()}
        rows = self.insert("companies", payload, upsert=True)
        return rows[0] if rows else {}

    def insert_companies_bulk(self, domains: list[str]) -> int:
        """Insert new domains with research_status=raw. Returns count inserted."""
        rows = [{"domain": d, "research_status": "raw"} for d in domains]
        # Insert in batches of 100
        inserted = 0
        for i in range(0, len(rows), 100):
            batch = rows[i:i+100]
            try:
                result = self._client.post(
                    "/companies",
                    json=batch,
                    headers={"Prefer": "resolution=ignore-duplicates,return=representation"},
                )
                result.raise_for_status()
                inserted += len(result.json())
            except httpx.HTTPStatusError as e:
                logger.error(f"Bulk insert error: {e.response.text}")
        return inserted

    def get_companies_by_status(
        self, status: str, limit: int = 100
    ) -> list[dict]:
        return self.select(
            "companies",
            filters={"research_status": status},
            limit=limit,
            order="created_at.asc",
        )

    def update_company_status(self, company_id: str, status: str, extra: dict = None) -> dict:
        data = {"research_status": status, "updated_at": _now()}
        if extra:
            data.update(extra)
        rows = self.update("companies", {"id": company_id}, data)
        return rows[0] if rows else {}

    def set_company_classification(
        self, company_id: str, classification: dict, name: str = None
    ) -> dict:
        data = {
            "classification": json.dumps(classification),
            "research_status": "classified",
            "enriched_at": _now(),
            "updated_at": _now(),
        }
        if name:
            data["name"] = name
        rows = self.update("companies", {"id": company_id}, data)
        return rows[0] if rows else {}

    def set_company_sales_org(
        self, company_id: str, sales_org: dict, tech_stack: dict = None
    ) -> dict:
        data = {
            "sales_org": json.dumps(sales_org),
            "updated_at": _now(),
        }
        if tech_stack:
            data["tech_stack"] = json.dumps(tech_stack)
        rows = self.update("companies", {"id": company_id}, data)
        return rows[0] if rows else {}

    def set_company_segment(
        self,
        company_id: str,
        segment_id: str,
        fit_score: int,
        fit_tier: str,
        reasoning: dict = None,
    ) -> dict:
        data = {
            "segment_id": segment_id,
            "fit_score": fit_score,
            "fit_tier": fit_tier,
            "research_status": "scored",
            "updated_at": _now(),
        }
        if reasoning:
            # Merge into classification JSONB via RPC or raw update
            # Using jsonb concatenation via update (PostgREST doesn't support jsonb operators directly)
            # Store reasoning separately; pipeline.py can handle JSONB merge if needed
            data["_segment_reasoning"] = reasoning  # flagged for pipeline to handle
        rows = self.update("companies", {"id": company_id}, data)
        return rows[0] if rows else {}

    # ----------------------------------------------------------
    # Contacts
    # ----------------------------------------------------------

    def get_contacts_for_company(self, company_id: str) -> list[dict]:
        return self.select("contacts", filters={"company_id": company_id})

    def upsert_contact(self, company_id: str, data: dict) -> dict:
        payload = {"company_id": company_id, **data}
        rows = self.insert("contacts", payload, upsert=True)
        return rows[0] if rows else {}

    # ----------------------------------------------------------
    # Signals
    # ----------------------------------------------------------

    def insert_signal(
        self,
        company_id: str,
        signal_type: str,
        content: str,
        source: str,
        urgency_score: int,
        detected_at: str = None,
    ) -> dict:
        data = {
            "company_id": company_id,
            "type": signal_type,
            "content": content,
            "source": source,
            "urgency_score": urgency_score,
            "detected_at": detected_at or _now(),
        }
        rows = self.insert("signals", data)
        return rows[0] if rows else {}

    def get_unused_signals(self, company_id: str, limit: int = 5) -> list[dict]:
        return self.select(
            "signals",
            filters={"company_id": company_id, "used_in_outreach": "false"},
            limit=limit,
            order="urgency_score.desc",
        )

    def mark_signals_used(self, signal_ids: list[str]) -> None:
        for sid in signal_ids:
            self.update("signals", {"id": sid}, {"used_in_outreach": True})

    # ----------------------------------------------------------
    # Segments
    # ----------------------------------------------------------

    def get_active_segments(self) -> list[dict]:
        return self.select("segments", filters={"is_active": "true"})

    def get_segment_by_id(self, segment_id: str) -> Optional[dict]:
        rows = self.select("segments", filters={"id": segment_id})
        return rows[0] if rows else None

    # ----------------------------------------------------------
    # Experiments
    # ----------------------------------------------------------

    def get_experiment(self, experiment_id: str) -> Optional[dict]:
        rows = self.select("experiments", filters={"id": experiment_id})
        return rows[0] if rows else None

    def get_active_experiments(self) -> list[dict]:
        return self.select("experiments", filters={"status": "active"})

    # ----------------------------------------------------------
    # Outreach
    # ----------------------------------------------------------

    def create_outreach_draft(
        self,
        experiment_id: str,
        company_id: str,
        contact_id: str,
        sequence: list[dict],
        sequence_config: dict,
    ) -> dict:
        data = {
            "experiment_id": experiment_id,
            "company_id": company_id,
            "contact_id": contact_id,
            "sequence": json.dumps(sequence),
            "sequence_config": json.dumps(sequence_config),
            "review_status": "draft",
        }
        rows = self.insert("outreach", data)
        return rows[0] if rows else {}

    def approve_outreach(self, outreach_id: str) -> dict:
        rows = self.update(
            "outreach",
            {"id": outreach_id},
            {"review_status": "approved", "updated_at": _now()},
        )
        return rows[0] if rows else {}

    # ----------------------------------------------------------
    # Results
    # ----------------------------------------------------------

    def log_result(
        self,
        outreach_id: str,
        company_id: str,
        contact_id: str,
        experiment_id: str,
        outcome: str,
        reply_content: str = None,
        notes: str = None,
    ) -> dict:
        data = {
            "outreach_id": outreach_id,
            "company_id": company_id,
            "contact_id": contact_id,
            "experiment_id": experiment_id,
            "outcome": outcome,
            "reply_content": reply_content,
            "notes": notes,
        }
        rows = self.insert("results", data)
        return rows[0] if rows else {}

    def mark_deck_generated(self, result_id: str, deck_path: str) -> dict:
        rows = self.update(
            "results",
            {"id": result_id},
            {"deck_generated": True, "deck_path": deck_path},
        )
        return rows[0] if rows else {}

    # ----------------------------------------------------------
    # Pipeline state (for resumable runs)
    # ----------------------------------------------------------

    def get_pipeline_state(self, run_id: str) -> Optional[dict]:
        """Read pipeline run state from a simple JSON file."""
        state_path = f"/tmp/beton_pipeline_{run_id}.json"
        if os.path.exists(state_path):
            with open(state_path) as f:
                return json.load(f)
        return None

    def save_pipeline_state(self, run_id: str, state: dict) -> None:
        state_path = f"/tmp/beton_pipeline_{run_id}.json"
        with open(state_path, "w") as f:
            json.dump(state, f, indent=2, default=str)

    def close(self):
        self._client.close()


# ============================================================
# Helpers
# ============================================================

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_domain(raw: str) -> Optional[str]:
    """
    Strip protocol, www, paths, and trailing slashes.
    Returns None if input is clearly invalid.
    """
    if not raw or not isinstance(raw, str):
        return None
    s = raw.strip().lower()
    # Remove protocol
    for prefix in ("https://", "http://"):
        if s.startswith(prefix):
            s = s[len(prefix):]
    # Remove www.
    if s.startswith("www."):
        s = s[4:]
    # Keep only host (strip path)
    s = s.split("/")[0].split("?")[0].split("#")[0]
    # Validate: must have at least one dot and no spaces
    if "." not in s or " " in s or len(s) < 4:
        return None
    # Skip clearly invalid values
    if s in ("n/a", "na", "none", "null", "example.com", "test.com"):
        return None
    return s


def load_soax_proxy(config_path: str = None) -> dict:
    """Load Soax proxy credentials from config file."""
    path = config_path or "YOUR_WORKSPACE_PATH/integrations/soax.json"
    if not os.path.exists(path):
        logger.warning(f"Soax config not found at {path}")
        return {}
    with open(path) as f:
        return json.load(f)
