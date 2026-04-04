-- Nuclear option: drop any existing policies and fully disable RLS
DO $$
DECLARE
  pol record;
BEGIN
  FOR pol IN SELECT policyname FROM pg_policies WHERE tablename = 'company_hypotheses' LOOP
    EXECUTE format('DROP POLICY IF EXISTS %I ON company_hypotheses', pol.policyname);
  END LOOP;
END $$;

ALTER TABLE company_hypotheses DISABLE ROW LEVEL SECURITY;
GRANT ALL PRIVILEGES ON TABLE company_hypotheses TO service_role, anon, authenticated;
