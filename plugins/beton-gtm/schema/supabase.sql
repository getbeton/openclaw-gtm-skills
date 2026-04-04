-- ============================================================
-- Beton GTM Intelligence — Supabase Schema
-- Version: 0.1.0
-- Apply via: supabase db push or psql
-- ============================================================

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================
-- updated_at trigger (reusable)
-- ============================================================

CREATE OR REPLACE FUNCTION trigger_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- segments
-- Defines ICP buckets. Created manually or via gtm-segment.
-- ============================================================

CREATE TABLE IF NOT EXISTS segments (
  id                    uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  name                  text NOT NULL,
  slug                  text UNIQUE NOT NULL,
  icp_definition        jsonb,          -- {size, gtm_motion, vertical, tech_stack, persona, ...}
  core_pain_hypothesis  text,
  value_prop_angle      text,
  target_personas       text[],         -- ['revops', 'sales-leader', 'founder']
  running_lean_stage    text CHECK (running_lean_stage IN ('problem_identified','solution_defined','validated','scaling')),
  is_active             boolean DEFAULT true,
  created_at            timestamptz DEFAULT now(),
  updated_at            timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_segments_slug ON segments(slug);
CREATE INDEX IF NOT EXISTS idx_segments_is_active ON segments(is_active);

CREATE TRIGGER set_segments_updated_at
  BEFORE UPDATE ON segments
  FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

-- ============================================================
-- companies
-- One row per domain. Central research record.
-- ============================================================

CREATE TABLE IF NOT EXISTS companies (
  id                uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  domain            text UNIQUE NOT NULL,
  name              text,
  segment_id        uuid REFERENCES segments(id) ON DELETE SET NULL,

  -- Pipeline state
  research_status   text DEFAULT 'raw' CHECK (research_status IN ('raw','prefiltered','classified','scored','contacted','skip')),
  fit_score         int CHECK (fit_score BETWEEN 0 AND 100),
  fit_tier          text CHECK (fit_tier IN ('T1','T2','T3','pass')),

  -- Research output blobs
  firmographic      jsonb,   -- {employees, revenue, funding, founded_year, hq_country}
  classification    jsonb,   -- {b2b, saas, gtmMotion, vertical, businessModel, sellsTo, pricingModel, keyFeatures, evidence}
  sales_org         jsonb,   -- {salesHeadcount, revopsHeadcount, csHeadcount, openRoles[], hiringSignal}
  tech_stack        jsonb,   -- {crm, salesEngagementTool, dataTools, analytics}
  research_raw      jsonb,   -- raw Firecrawl pages + evidence snippets

  -- Attio sync
  attio_record_id   text,
  attio_synced_at   timestamptz,

  -- Meta
  enriched_at       timestamptz,
  created_at        timestamptz DEFAULT now(),
  updated_at        timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_companies_domain ON companies(domain);
CREATE INDEX IF NOT EXISTS idx_companies_research_status ON companies(research_status);
CREATE INDEX IF NOT EXISTS idx_companies_segment_id ON companies(segment_id);
CREATE INDEX IF NOT EXISTS idx_companies_fit_tier ON companies(fit_tier);
CREATE INDEX IF NOT EXISTS idx_companies_fit_score ON companies(fit_score);

CREATE TRIGGER set_companies_updated_at
  BEFORE UPDATE ON companies
  FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

-- ============================================================
-- contacts
-- Decision-makers at companies.
-- ============================================================

CREATE TABLE IF NOT EXISTS contacts (
  id                uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  company_id        uuid NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  name              text,
  first_name        text,
  last_name         text,
  email             text,
  linkedin_url      text,
  title             text,
  seniority         text CHECK (seniority IN ('ic','manager','director','vp','c-suite')),
  persona_type      text CHECK (persona_type IN ('revops','sales-leader','founder','growth')),
  email_verified    boolean DEFAULT false,
  attio_record_id   text,
  created_at        timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_contacts_company_id ON contacts(company_id);
CREATE INDEX IF NOT EXISTS idx_contacts_email ON contacts(email);
CREATE INDEX IF NOT EXISTS idx_contacts_persona_type ON contacts(persona_type);
CREATE INDEX IF NOT EXISTS idx_contacts_seniority ON contacts(seniority);

-- ============================================================
-- signals
-- Urgency-scored triggers per company.
-- ============================================================

CREATE TABLE IF NOT EXISTS signals (
  id              uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  company_id      uuid NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  type            text NOT NULL CHECK (type IN ('funding','leadership_change','hiring','competitor_pain','content_signal','product_launch','tech_change')),
  content         text,
  source          text,
  detected_at     timestamptz DEFAULT now(),
  urgency_score   int CHECK (urgency_score BETWEEN 1 AND 10),
  used_in_outreach boolean DEFAULT false,
  created_at      timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_signals_company_id ON signals(company_id);
CREATE INDEX IF NOT EXISTS idx_signals_type ON signals(type);
CREATE INDEX IF NOT EXISTS idx_signals_urgency_score ON signals(urgency_score);
CREATE INDEX IF NOT EXISTS idx_signals_used_in_outreach ON signals(used_in_outreach);
CREATE INDEX IF NOT EXISTS idx_signals_detected_at ON signals(detected_at);

-- ============================================================
-- experiments
-- A/B tests for outreach angles, sequences, personas.
-- ============================================================

CREATE TABLE IF NOT EXISTS experiments (
  id                uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  name              text NOT NULL,
  segment_id        uuid REFERENCES segments(id) ON DELETE SET NULL,
  description       text,
  target_metric     text,           -- e.g. 'positive_reply_rate'
  expected_value    text,           -- e.g. '4-6%'
  actual_value      text,           -- calculated after conclusion
  tracking_method   text,
  status            text DEFAULT 'draft' CHECK (status IN ('draft','active','paused','concluded')),
  started_at        timestamptz,
  concluded_at      timestamptz,
  created_at        timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_experiments_segment_id ON experiments(segment_id);
CREATE INDEX IF NOT EXISTS idx_experiments_status ON experiments(status);

-- ============================================================
-- hypotheses
-- Testable claims. Can be linked to experiments and/or segments.
-- ============================================================

CREATE TABLE IF NOT EXISTS hypotheses (
  id              uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  statement       text NOT NULL,
  type            text NOT NULL CHECK (type IN ('segment','persona','signal','subject_line','cta','framing','sequence_length')),
  segment_id      uuid REFERENCES segments(id) ON DELETE SET NULL,
  experiment_id   uuid REFERENCES experiments(id) ON DELETE SET NULL,
  persona_type    text,
  signal_type     text,
  status          text DEFAULT 'untested' CHECK (status IN ('untested','testing','confirmed','invalidated','refined')),
  confidence      text DEFAULT 'low' CHECK (confidence IN ('low','medium','high')),
  evidence        text,
  created_at      timestamptz DEFAULT now(),
  updated_at      timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_hypotheses_segment_id ON hypotheses(segment_id);
CREATE INDEX IF NOT EXISTS idx_hypotheses_experiment_id ON hypotheses(experiment_id);
CREATE INDEX IF NOT EXISTS idx_hypotheses_status ON hypotheses(status);
CREATE INDEX IF NOT EXISTS idx_hypotheses_type ON hypotheses(type);

CREATE TRIGGER set_hypotheses_updated_at
  BEFORE UPDATE ON hypotheses
  FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

-- ============================================================
-- experiment_hypotheses
-- Junction: which hypotheses are tested in which experiment.
-- ============================================================

CREATE TABLE IF NOT EXISTS experiment_hypotheses (
  experiment_id   uuid NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
  hypothesis_id   uuid NOT NULL REFERENCES hypotheses(id) ON DELETE CASCADE,
  PRIMARY KEY (experiment_id, hypothesis_id)
);

CREATE INDEX IF NOT EXISTS idx_exp_hyp_hypothesis_id ON experiment_hypotheses(hypothesis_id);

-- ============================================================
-- outreach
-- Drafted sequences, one row per company+contact+experiment.
-- ============================================================

CREATE TABLE IF NOT EXISTS outreach (
  id                    uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  experiment_id         uuid REFERENCES experiments(id) ON DELETE SET NULL,
  company_id            uuid NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  contact_id            uuid REFERENCES contacts(id) ON DELETE SET NULL,
  sequence              jsonb,   -- [{step, day, type, angle, subject, body, status}]
  sequence_config       jsonb,   -- experiment's sequence definition (length, angles, timing)
  review_status         text DEFAULT 'draft' CHECK (review_status IN ('draft','approved','rejected')),
  seqd_campaign_id      text,
  seqd_enrollment_id    text,
  created_at            timestamptz DEFAULT now(),
  updated_at            timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_outreach_experiment_id ON outreach(experiment_id);
CREATE INDEX IF NOT EXISTS idx_outreach_company_id ON outreach(company_id);
CREATE INDEX IF NOT EXISTS idx_outreach_contact_id ON outreach(contact_id);
CREATE INDEX IF NOT EXISTS idx_outreach_review_status ON outreach(review_status);

CREATE TRIGGER set_outreach_updated_at
  BEFORE UPDATE ON outreach
  FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

-- ============================================================
-- results
-- One row per outcome event (reply, bounce, book, etc.)
-- ============================================================

CREATE TABLE IF NOT EXISTS results (
  id              uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  outreach_id     uuid REFERENCES outreach(id) ON DELETE SET NULL,
  company_id      uuid REFERENCES companies(id) ON DELETE SET NULL,
  contact_id      uuid REFERENCES contacts(id) ON DELETE SET NULL,
  experiment_id   uuid REFERENCES experiments(id) ON DELETE SET NULL,
  outcome         text NOT NULL CHECK (outcome IN ('replied_positive','replied_negative','replied_ooo','ghosted','bounced','booked','unsubscribed')),
  reply_content   text,
  deck_generated  boolean DEFAULT false,
  deck_path       text,
  notes           text,
  logged_at       timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_results_outreach_id ON results(outreach_id);
CREATE INDEX IF NOT EXISTS idx_results_company_id ON results(company_id);
CREATE INDEX IF NOT EXISTS idx_results_experiment_id ON results(experiment_id);
CREATE INDEX IF NOT EXISTS idx_results_outcome ON results(outcome);
CREATE INDEX IF NOT EXISTS idx_results_logged_at ON results(logged_at);

-- ============================================================
-- learnings
-- Post-experiment summaries. Written by AI after concluding.
-- ============================================================

CREATE TABLE IF NOT EXISTS learnings (
  id                            uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  experiment_id                 uuid REFERENCES experiments(id) ON DELETE SET NULL,
  summary                       text NOT NULL,
  actual_metric_value           text,
  hypothesis_updates            jsonb,   -- [{hypothesis_id, new_status, reasoning}]
  segment_insights              text,
  next_experiment_suggestions   text[],
  created_at                    timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_learnings_experiment_id ON learnings(experiment_id);

-- ============================================================
-- Convenience view: T1 companies ready for outreach
-- ============================================================

CREATE OR REPLACE VIEW v_t1_ready AS
SELECT
  c.id,
  c.domain,
  c.name,
  c.fit_score,
  c.fit_tier,
  c.segment_id,
  s.slug AS segment_slug,
  s.name AS segment_name,
  c.classification,
  c.sales_org,
  c.tech_stack,
  c.research_status,
  c.enriched_at
FROM companies c
LEFT JOIN segments s ON s.id = c.segment_id
WHERE c.fit_tier = 'T1'
  AND c.research_status IN ('scored','classified')
ORDER BY c.fit_score DESC;

-- ============================================================
-- Convenience view: active experiment stats
-- ============================================================

CREATE OR REPLACE VIEW v_experiment_stats AS
SELECT
  e.id,
  e.name,
  e.status,
  e.target_metric,
  e.expected_value,
  e.actual_value,
  COUNT(DISTINCT o.id) AS total_outreach,
  COUNT(DISTINCT CASE WHEN o.review_status = 'approved' THEN o.id END) AS approved_sequences,
  COUNT(DISTINCT r.id) AS total_results,
  COUNT(DISTINCT CASE WHEN r.outcome = 'replied_positive' THEN r.id END) AS positive_replies,
  COUNT(DISTINCT CASE WHEN r.outcome = 'booked' THEN r.id END) AS meetings_booked,
  ROUND(
    100.0 * COUNT(DISTINCT CASE WHEN r.outcome = 'replied_positive' THEN r.id END)
    / NULLIF(COUNT(DISTINCT r.id), 0), 1
  ) AS positive_reply_rate_pct
FROM experiments e
LEFT JOIN outreach o ON o.experiment_id = e.id
LEFT JOIN results r ON r.experiment_id = e.id
GROUP BY e.id, e.name, e.status, e.target_metric, e.expected_value, e.actual_value;
