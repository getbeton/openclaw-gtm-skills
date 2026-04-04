-- Rename misleadingly-named "headcount" columns to "open_roles"
-- These columns store counts of OPEN JOB POSTINGS found on careers pages,
-- not actual team headcount. The old names suggested real headcount which was wrong.

ALTER TABLE company_sales_org
  RENAME COLUMN sales_headcount TO open_sales_roles;

ALTER TABLE company_sales_org
  RENAME COLUMN revops_headcount TO open_revops_roles;

ALTER TABLE company_sales_org
  RENAME COLUMN cs_headcount TO open_cs_roles;
