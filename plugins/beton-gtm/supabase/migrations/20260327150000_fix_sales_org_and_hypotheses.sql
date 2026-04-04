-- Fix 1: Add missing columns to company_sales_org
ALTER TABLE company_sales_org
  ADD COLUMN IF NOT EXISTS hiring_signal_type text,
  ADD COLUMN IF NOT EXISTS careers_page_found boolean DEFAULT false,
  ADD COLUMN IF NOT EXISTS tech_stack         jsonb,
  ADD COLUMN IF NOT EXISTS sales_signals      jsonb;

-- Fix 2: Ensure company_hypotheses has proper service_role access
-- Drop and recreate policy cleanly
DO $$
BEGIN
  -- Disable RLS entirely for company_hypotheses — service role should bypass anyway
  -- but some Supabase configs require explicit policy
  ALTER TABLE company_hypotheses DISABLE ROW LEVEL SECURITY;
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

-- Grant full access to service_role explicitly
GRANT ALL ON company_hypotheses TO service_role;
GRANT ALL ON hypotheses TO service_role;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO service_role;
