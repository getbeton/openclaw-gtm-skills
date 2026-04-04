-- Grant service role full access to company_hypotheses (RLS bypass)
ALTER TABLE company_hypotheses ENABLE ROW LEVEL SECURITY;

CREATE POLICY "service_role_all" ON company_hypotheses
  FOR ALL TO service_role USING (true) WITH CHECK (true);
