-- ============================================================
-- Add source column to companies + competitor_intel table
-- ============================================================

-- 1. Add source to companies
ALTER TABLE companies
  ADD COLUMN IF NOT EXISTS source text DEFAULT 'posthog_export'
    CHECK (source IN ('posthog_export', 'comp_research', 'manual', 'other'));

-- Backfill existing rows
UPDATE companies SET source = 'posthog_export' WHERE source IS NULL;

CREATE INDEX IF NOT EXISTS idx_companies_source ON companies(source);

-- 2. competitor_intel — comp-research-specific enrichment
-- One row per company (upsert on company_id).
-- Only populated when source = 'comp_research' (or when comp-research runs on an existing company).

CREATE TABLE IF NOT EXISTS competitor_intel (
  id                  uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  company_id          uuid NOT NULL REFERENCES companies(id) ON DELETE CASCADE,

  -- Pricing
  free_tier           boolean,
  open_source         boolean,
  self_hostable       boolean,
  pricing_model       text,         -- e.g. "per-seat", "custom/sales", "credit-based"
  entry_price         text,         -- e.g. "$1,000/mo", "$30K+/yr", "custom"
  pricing_tiers       jsonb,        -- [{name, price}]

  -- Market position
  tagline             text,
  positioning         text,
  target_icp          text,
  notable_customers   text[],
  key_integrations    text[],
  funding             text,         -- e.g. "$23M Series A (Coatue)"
  founded_year        int,

  -- Competitive intelligence
  strengths           text[],
  weaknesses          text[],
  competitive_notes   text,         -- how they compare to Beton specifically
  threat_level        text CHECK (threat_level IN ('high', 'medium', 'low', 'watch')),
  trajectory          text CHECK (trajectory IN ('growing', 'stable', 'declining', 'acquired', 'unknown')),
  acquisition_note    text,         -- e.g. "Acquired by Apollo.io, 2025"

  -- Meta
  scraped_urls        text[],
  researched_at       timestamptz DEFAULT now(),
  created_at          timestamptz DEFAULT now(),
  updated_at          timestamptz DEFAULT now(),

  UNIQUE (company_id)
);

CREATE INDEX IF NOT EXISTS idx_comp_intel_company_id ON competitor_intel(company_id);
CREATE INDEX IF NOT EXISTS idx_comp_intel_threat_level ON competitor_intel(threat_level);
CREATE INDEX IF NOT EXISTS idx_comp_intel_trajectory ON competitor_intel(trajectory);

CREATE TRIGGER set_competitor_intel_updated_at
  BEFORE UPDATE ON competitor_intel
  FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

-- 3. Convenience view: competitors with classification + intel
CREATE OR REPLACE VIEW v_competitors AS
SELECT
  c.id,
  c.domain,
  c.name,
  c.source,
  c.research_status,
  cc.gtm_motion,
  cc.vertical,
  cc.sells_to,
  cc.pricing_model AS classification_pricing_model,
  ci.tagline,
  ci.positioning,
  ci.target_icp,
  ci.free_tier,
  ci.open_source,
  ci.self_hostable,
  ci.entry_price,
  ci.pricing_model AS intel_pricing_model,
  ci.funding,
  ci.founded_year,
  ci.threat_level,
  ci.trajectory,
  ci.acquisition_note,
  ci.notable_customers,
  ci.key_integrations,
  ci.strengths,
  ci.weaknesses,
  ci.researched_at
FROM companies c
LEFT JOIN company_classification cc ON cc.company_id = c.id
LEFT JOIN competitor_intel ci ON ci.company_id = c.id
WHERE c.source = 'comp_research'
ORDER BY ci.threat_level NULLS LAST, c.name;
