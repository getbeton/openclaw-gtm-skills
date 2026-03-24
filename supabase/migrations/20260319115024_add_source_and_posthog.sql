-- Add source tracking and PostHog flag to companies

ALTER TABLE companies
  ADD COLUMN IF NOT EXISTS source text,         -- 'wappalyzer' | 'clay' | 'manual'
  ADD COLUMN IF NOT EXISTS has_posthog boolean; -- confirmed PostHog installation

CREATE INDEX IF NOT EXISTS idx_companies_source ON companies(source);
CREATE INDEX IF NOT EXISTS idx_companies_has_posthog ON companies(has_posthog);
