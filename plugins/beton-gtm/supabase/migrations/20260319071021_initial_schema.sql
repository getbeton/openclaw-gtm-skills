-- ============================================================
-- Beton GTM Intelligence — Initial Schema
-- ============================================================

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
-- ============================================================

CREATE TABLE segments (
  id                    uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  name                  text NOT NULL,
  slug                  text UNIQUE NOT NULL,
  icp_definition        jsonb,
  core_pain_hypothesis  text,
  value_prop_angle      text,
  target_personas       text[],
  running_lean_stage    text CHECK (running_lean_stage IN ('problem_identified','solution_defined','validated','scaling')),
  is_active             boolean DEFAULT true,
  created_at            timestamptz DEFAULT now(),
  updated_at            timestamptz DEFAULT now()
);

CREATE INDEX idx_segments_slug ON segments(slug);
CREATE INDEX idx_segments_is_active ON segments(is_active);

CREATE TRIGGER set_segments_updated_at
  BEFORE UPDATE ON segments
  FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

-- ============================================================
-- companies
-- ============================================================

CREATE TABLE companies (
  id              uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  domain          text UNIQUE NOT NULL,
  name            text,
  research_status text DEFAULT 'raw' CHECK (research_status IN ('raw','prefiltered','classified','scored','contacted','skip')),
  attio_record_id text,
  attio_synced_at timestamptz,
  enriched_at     timestamptz,
  research_raw    jsonb,   -- raw Firecrawl page content only
  created_at      timestamptz DEFAULT now(),
  updated_at      timestamptz DEFAULT now()
);

CREATE INDEX idx_companies_domain ON companies(domain);
CREATE INDEX idx_companies_research_status ON companies(research_status);

CREATE TRIGGER set_companies_updated_at
  BEFORE UPDATE ON companies
  FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

-- ============================================================
-- company_firmographics
-- ============================================================

CREATE TABLE company_firmographics (
  id                      uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  company_id              uuid NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  employees_range         text,
  employees_count         int,
  funding_total           numeric,
  funding_rounds          int,
  founded_year            int,
  hq_country              text,
  hq_city                 text,
  company_type            text,
  traffic_rank            int,
  annual_revenue_estimate numeric,
  enriched_at             timestamptz,
  created_at              timestamptz DEFAULT now(),
  updated_at              timestamptz DEFAULT now(),
  UNIQUE (company_id)
);

CREATE INDEX idx_co_firm_company_id ON company_firmographics(company_id);
CREATE INDEX idx_co_firm_hq_country ON company_firmographics(hq_country);
CREATE INDEX idx_co_firm_employees_range ON company_firmographics(employees_range);

CREATE TRIGGER set_co_firm_updated_at
  BEFORE UPDATE ON company_firmographics
  FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

-- ============================================================
-- company_classification
-- ============================================================

CREATE TABLE company_classification (
  id              uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  company_id      uuid NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  b2b             boolean,
  saas            boolean,
  gtm_motion      text CHECK (gtm_motion IN ('PLG', 'SLG', 'hybrid')),
  vertical        text,
  business_model  text,
  sells_to        text,
  pricing_model   text,
  description     text,
  evidence        text,
  classified_at   timestamptz,
  created_at      timestamptz DEFAULT now(),
  updated_at      timestamptz DEFAULT now(),
  UNIQUE (company_id)
);

CREATE INDEX idx_co_class_company_id ON company_classification(company_id);
CREATE INDEX idx_co_class_gtm_motion ON company_classification(gtm_motion);
CREATE INDEX idx_co_class_vertical ON company_classification(vertical);
CREATE INDEX idx_co_class_b2b_saas ON company_classification(b2b, saas);

CREATE TRIGGER set_co_class_updated_at
  BEFORE UPDATE ON company_classification
  FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

-- ============================================================
-- company_social
-- ============================================================

CREATE TABLE company_social (
  id              uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  company_id      uuid NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  linkedin_url    text,
  twitter_url     text,
  github_url      text,
  website_email   text,
  spf_record      boolean,
  dmarc_record    boolean,
  created_at      timestamptz DEFAULT now(),
  updated_at      timestamptz DEFAULT now(),
  UNIQUE (company_id)
);

CREATE INDEX idx_co_social_company_id ON company_social(company_id);

CREATE TRIGGER set_co_social_updated_at
  BEFORE UPDATE ON company_social
  FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

-- ============================================================
-- company_sales_org
-- ============================================================

CREATE TABLE company_sales_org (
  id                  uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  company_id          uuid NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  sales_headcount     int,
  revops_headcount    int,
  cs_headcount        int,
  hiring_signal       boolean DEFAULT false,
  salaries_lost       numeric GENERATED ALWAYS AS (sales_headcount * 80000) STORED,
  enriched_at         timestamptz,
  created_at          timestamptz DEFAULT now(),
  updated_at          timestamptz DEFAULT now(),
  UNIQUE (company_id)
);

CREATE INDEX idx_co_sales_org_company_id ON company_sales_org(company_id);
CREATE INDEX idx_co_sales_org_headcount ON company_sales_org(sales_headcount);
CREATE INDEX idx_co_sales_org_hiring ON company_sales_org(hiring_signal);

CREATE TRIGGER set_co_sales_org_updated_at
  BEFORE UPDATE ON company_sales_org
  FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

-- ============================================================
-- company_open_roles
-- ============================================================

CREATE TABLE company_open_roles (
  id          uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  company_id  uuid NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  title       text NOT NULL,
  function    text,
  seniority   text,
  location    text,
  remote      boolean,
  posted_at   timestamptz,
  source_url  text,
  created_at  timestamptz DEFAULT now()
);

CREATE INDEX idx_co_roles_company_id ON company_open_roles(company_id);
CREATE INDEX idx_co_roles_function ON company_open_roles(function);
CREATE INDEX idx_co_roles_seniority ON company_open_roles(seniority);

-- ============================================================
-- company_tech_stack
-- ============================================================

CREATE TABLE company_tech_stack (
  id                      uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  company_id              uuid NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  crm                     text,
  sales_engagement_tool   text,
  data_warehouse          text,
  analytics               text,
  enriched_at             timestamptz,
  created_at              timestamptz DEFAULT now(),
  updated_at              timestamptz DEFAULT now(),
  UNIQUE (company_id)
);

CREATE INDEX idx_co_tech_company_id ON company_tech_stack(company_id);
CREATE INDEX idx_co_tech_crm ON company_tech_stack(crm);

CREATE TRIGGER set_co_tech_updated_at
  BEFORE UPDATE ON company_tech_stack
  FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

-- ============================================================
-- contacts
-- ============================================================

CREATE TABLE contacts (
  id              uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  name            text,
  first_name      text,
  last_name       text,
  email           text,
  linkedin_url    text,
  title           text,
  seniority       text CHECK (seniority IN ('ic','manager','director','vp','c-suite')),
  persona_type    text CHECK (persona_type IN ('revops','sales-leader','founder','growth')),
  email_verified  boolean DEFAULT false,
  attio_record_id text,
  created_at      timestamptz DEFAULT now()
);

CREATE INDEX idx_contacts_email ON contacts(email);
CREATE INDEX idx_contacts_persona_type ON contacts(persona_type);

-- ============================================================
-- contact_companies (M2M: contacts ↔ companies)
-- ============================================================

CREATE TABLE contact_companies (
  contact_id  uuid NOT NULL REFERENCES contacts(id) ON DELETE CASCADE,
  company_id  uuid NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  is_primary  boolean DEFAULT false,
  created_at  timestamptz DEFAULT now(),
  PRIMARY KEY (contact_id, company_id)
);

CREATE INDEX idx_cc_company_id ON contact_companies(company_id);

-- ============================================================
-- company_segments (M2M: companies ↔ segments)
-- ============================================================

CREATE TABLE company_segments (
  company_id    uuid NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  segment_id    uuid NOT NULL REFERENCES segments(id) ON DELETE CASCADE,
  fit_score     int CHECK (fit_score BETWEEN 0 AND 100),
  fit_tier      text CHECK (fit_tier IN ('T1','T2','T3','pass')),
  fit_reasoning text,
  assigned_at   timestamptz DEFAULT now(),
  PRIMARY KEY (company_id, segment_id)
);

CREATE INDEX idx_co_seg_segment_id ON company_segments(segment_id);
CREATE INDEX idx_co_seg_fit_tier ON company_segments(fit_tier);
CREATE INDEX idx_co_seg_fit_score ON company_segments(fit_score);

-- ============================================================
-- signals
-- ============================================================

CREATE TABLE signals (
  id               uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  company_id       uuid NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  type             text NOT NULL CHECK (type IN ('funding','leadership_change','hiring','competitor_pain','content_signal','product_launch','tech_change')),
  content          text,
  source           text,
  detected_at      timestamptz DEFAULT now(),
  urgency_score    int CHECK (urgency_score BETWEEN 1 AND 10),
  used_in_outreach boolean DEFAULT false,
  created_at       timestamptz DEFAULT now()
);

CREATE INDEX idx_signals_company_id ON signals(company_id);
CREATE INDEX idx_signals_type ON signals(type);
CREATE INDEX idx_signals_urgency_score ON signals(urgency_score);

-- ============================================================
-- experiments
-- ============================================================

CREATE TABLE experiments (
  id              uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  name            text NOT NULL,
  segment_id      uuid REFERENCES segments(id) ON DELETE SET NULL,
  description     text,
  target_metric   text,
  expected_value  text,
  actual_value    text,
  tracking_method text,
  status          text DEFAULT 'draft' CHECK (status IN ('draft','active','paused','concluded')),
  started_at      timestamptz,
  concluded_at    timestamptz,
  created_at      timestamptz DEFAULT now()
);

CREATE INDEX idx_experiments_segment_id ON experiments(segment_id);
CREATE INDEX idx_experiments_status ON experiments(status);

-- ============================================================
-- experiment_companies (M2M: experiments ↔ companies)
-- ============================================================

CREATE TABLE experiment_companies (
  experiment_id uuid NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
  company_id    uuid NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  segment_id    uuid REFERENCES segments(id) ON DELETE SET NULL,
  added_at      timestamptz DEFAULT now(),
  PRIMARY KEY (experiment_id, company_id)
);

CREATE INDEX idx_exp_co_company_id ON experiment_companies(company_id);

-- ============================================================
-- hypotheses
-- ============================================================

CREATE TABLE hypotheses (
  id            uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  statement     text NOT NULL,
  type          text NOT NULL CHECK (type IN ('segment','persona','signal','subject_line','cta','framing','sequence_length')),
  segment_id    uuid REFERENCES segments(id) ON DELETE SET NULL,
  experiment_id uuid REFERENCES experiments(id) ON DELETE SET NULL,
  persona_type  text,
  signal_type   text,
  status        text DEFAULT 'untested' CHECK (status IN ('untested','testing','confirmed','invalidated','refined')),
  confidence    text DEFAULT 'low' CHECK (confidence IN ('low','medium','high')),
  evidence      text,
  created_at    timestamptz DEFAULT now(),
  updated_at    timestamptz DEFAULT now()
);

CREATE INDEX idx_hypotheses_segment_id ON hypotheses(segment_id);
CREATE INDEX idx_hypotheses_experiment_id ON hypotheses(experiment_id);
CREATE INDEX idx_hypotheses_status ON hypotheses(status);

CREATE TRIGGER set_hypotheses_updated_at
  BEFORE UPDATE ON hypotheses
  FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

-- ============================================================
-- experiment_hypotheses (M2M junction)
-- ============================================================

CREATE TABLE experiment_hypotheses (
  experiment_id uuid NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
  hypothesis_id uuid NOT NULL REFERENCES hypotheses(id) ON DELETE CASCADE,
  PRIMARY KEY (experiment_id, hypothesis_id)
);

-- ============================================================
-- outreach
-- ============================================================

CREATE TABLE outreach (
  id                  uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  experiment_id       uuid REFERENCES experiments(id) ON DELETE SET NULL,
  company_id          uuid NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  contact_id          uuid REFERENCES contacts(id) ON DELETE SET NULL,
  sequence            jsonb,   -- [{step, day, type, angle, subject, body, status}]
  sequence_config     jsonb,   -- experiment sequence definition
  review_status       text DEFAULT 'draft' CHECK (review_status IN ('draft','approved','rejected')),
  seqd_campaign_id    text,
  seqd_enrollment_id  text,
  created_at          timestamptz DEFAULT now(),
  updated_at          timestamptz DEFAULT now()
);

CREATE INDEX idx_outreach_experiment_id ON outreach(experiment_id);
CREATE INDEX idx_outreach_company_id ON outreach(company_id);
CREATE INDEX idx_outreach_review_status ON outreach(review_status);

CREATE TRIGGER set_outreach_updated_at
  BEFORE UPDATE ON outreach
  FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

-- ============================================================
-- results
-- ============================================================

CREATE TABLE results (
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

CREATE INDEX idx_results_experiment_id ON results(experiment_id);
CREATE INDEX idx_results_outcome ON results(outcome);

-- ============================================================
-- learnings
-- ============================================================

CREATE TABLE learnings (
  id                          uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  experiment_id               uuid REFERENCES experiments(id) ON DELETE SET NULL,
  summary                     text NOT NULL,
  actual_metric_value         text,
  hypothesis_updates          jsonb,
  segment_insights            text,
  next_experiment_suggestions text[],
  created_at                  timestamptz DEFAULT now()
);

CREATE INDEX idx_learnings_experiment_id ON learnings(experiment_id);

-- ============================================================
-- Views
-- ============================================================

CREATE VIEW v_t1_ready AS
SELECT
  c.id, c.domain, c.name, c.research_status, c.enriched_at,
  cs.segment_id, cs.fit_score, cs.fit_tier,
  s.slug AS segment_slug, s.name AS segment_name,
  cc.gtm_motion, cc.vertical, cc.b2b, cc.saas,
  so.sales_headcount, so.revops_headcount, so.salaries_lost,
  ts.crm, ts.sales_engagement_tool
FROM companies c
JOIN company_segments cs ON cs.company_id = c.id AND cs.fit_tier = 'T1'
JOIN segments s ON s.id = cs.segment_id
LEFT JOIN company_classification cc ON cc.company_id = c.id
LEFT JOIN company_sales_org so ON so.company_id = c.id
LEFT JOIN company_tech_stack ts ON ts.company_id = c.id
WHERE c.research_status IN ('scored','classified')
ORDER BY cs.fit_score DESC;

CREATE VIEW v_experiment_stats AS
SELECT
  e.id, e.name, e.status, e.target_metric, e.expected_value, e.actual_value,
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
