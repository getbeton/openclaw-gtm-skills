-- Migration: Extend existing hypotheses/experiments for GTM pipeline
-- 2026-03-27

-- ── Extend hypotheses table ──────────────────────────────────────────────────
-- Existing: id, statement, type, segment_id, experiment_id, persona_type,
--           signal_type, status, confidence, evidence, created_at, updated_at
-- New fields for pipeline-generated hypotheses:

ALTER TABLE hypotheses
  ADD COLUMN IF NOT EXISTS hypothesis_type text
    CHECK (hypothesis_type IN ('data_grounded', 'context_specific')),
  ADD COLUMN IF NOT EXISTS hypothesis_text  text,
  ADD COLUMN IF NOT EXISTS evidence_base    text,
  ADD COLUMN IF NOT EXISTS example_companies text[],
  ADD COLUMN IF NOT EXISTS personalization_hook text;

CREATE INDEX IF NOT EXISTS idx_hypotheses_hypothesis_type ON hypotheses(hypothesis_type);

-- ── company_hypotheses (new — per-company mapping) ───────────────────────────
CREATE TABLE IF NOT EXISTS company_hypotheses (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id uuid NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  hypothesis_id uuid NOT NULL REFERENCES hypotheses(id) ON DELETE CASCADE,
  hypothesis_type text NOT NULL CHECK (hypothesis_type IN ('data_grounded', 'context_specific')),
  confidence_score int CHECK (confidence_score BETWEEN 0 AND 100),
  context_specific_text text,
  created_at timestamptz DEFAULT now(),
  UNIQUE(company_id, hypothesis_id)
);

CREATE INDEX IF NOT EXISTS idx_company_hypotheses_company ON company_hypotheses(company_id);
CREATE INDEX IF NOT EXISTS idx_company_hypotheses_hypothesis ON company_hypotheses(hypothesis_id);

-- ── Extend experiments table ─────────────────────────────────────────────────
-- Existing: id, name, segment_id, description, target_metric, expected_value,
--           actual_value, tracking_method, status, started_at, concluded_at, created_at
-- New HADI fields:

ALTER TABLE experiments
  ADD COLUMN IF NOT EXISTS hypothesis_id        uuid REFERENCES hypotheses(id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS action_type          text DEFAULT 'cold_email',
  ADD COLUMN IF NOT EXISTS target_company_count int DEFAULT 0,
  ADD COLUMN IF NOT EXISTS companies_reached    int DEFAULT 0,
  ADD COLUMN IF NOT EXISTS replies_received     int DEFAULT 0,
  ADD COLUMN IF NOT EXISTS meetings_booked      int DEFAULT 0,
  ADD COLUMN IF NOT EXISTS pipeline_generated_usd int DEFAULT 0,
  ADD COLUMN IF NOT EXISTS learnings            text,
  ADD COLUMN IF NOT EXISTS next_action          text,
  ADD COLUMN IF NOT EXISTS updated_at           timestamptz DEFAULT now();

CREATE INDEX IF NOT EXISTS idx_experiments_hypothesis_id ON experiments(hypothesis_id);

-- Add updated_at trigger to experiments if not already present
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger
    WHERE tgname = 'set_experiments_updated_at'
  ) THEN
    CREATE TRIGGER set_experiments_updated_at
      BEFORE UPDATE ON experiments
      FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();
  END IF;
END $$;
