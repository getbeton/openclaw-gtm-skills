-- Add D2C as a valid gtm_motion value in company_classification
ALTER TABLE company_classification
  DROP CONSTRAINT company_classification_gtm_motion_check;

ALTER TABLE company_classification
  ADD CONSTRAINT company_classification_gtm_motion_check
  CHECK (gtm_motion IN ('PLG', 'SLG', 'hybrid', 'D2C'));
