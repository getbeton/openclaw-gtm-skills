-- Add company_id to contacts table linking to companies
ALTER TABLE contacts 
ADD COLUMN IF NOT EXISTS company_id uuid REFERENCES companies(id) ON DELETE CASCADE;

-- Create index for faster lookups
CREATE INDEX IF NOT EXISTS idx_contacts_company_id ON contacts(company_id);

-- Backfill company_id from email domain
UPDATE contacts 
SET company_id = (
  SELECT id FROM companies 
  WHERE domain = SPLIT_PART(contacts.email, '@', 2)
  LIMIT 1
)
WHERE company_id IS NULL 
  AND email IS NOT NULL 
  AND email LIKE '%@%';
