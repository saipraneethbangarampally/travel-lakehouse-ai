-- Generated from config/semantic_model.yaml. Do not edit by hand.
-- 1) Dynamic data masking on PII columns

ALTER TABLE gold.dim_member ALTER COLUMN first_name ADD MASKED WITH (FUNCTION = 'partial(1,"***",0)');
ALTER TABLE gold.dim_member ALTER COLUMN last_name ADD MASKED WITH (FUNCTION = 'partial(1,"***",0)');
ALTER TABLE gold.dim_member ALTER COLUMN email ADD MASKED WITH (FUNCTION = 'email()');
ALTER TABLE gold.dim_member ALTER COLUMN phone ADD MASKED WITH (FUNCTION = 'partial(1,"***",0)');

-- Only marketing ops may unmask (activation of approved audiences)
GRANT UNMASK TO [role_marketing_ops];

-- 2) Row-level security: a principal only sees its partners' rows
CREATE SCHEMA security;
GO
CREATE TABLE security.principal_partner (principal_name sysname, partner_code varchar(10));
GO
CREATE FUNCTION security.fn_partner_predicate(@partner_code varchar(10))
RETURNS TABLE WITH SCHEMABINDING AS
RETURN SELECT 1 AS allowed FROM security.principal_partner p
       WHERE p.principal_name = USER_NAME() AND (p.partner_code = @partner_code OR p.partner_code = '*');
GO
CREATE SECURITY POLICY security.partner_isolation
    ADD FILTER PREDICATE security.fn_partner_predicate(partner_code) ON gold.fact_booking,
    ADD FILTER PREDICATE security.fn_partner_predicate(partner_code) ON gold.member_360,
    ADD FILTER PREDICATE security.fn_partner_predicate(partner_code) ON gold.fact_email_engagement,
    ADD FILTER PREDICATE security.fn_partner_predicate(partner_code) ON gold.fact_web_event,
    ADD FILTER PREDICATE security.fn_partner_predicate(partner_code) ON gold.member_contactability,
    ADD FILTER PREDICATE security.fn_partner_predicate(partner_code) ON gold.dim_member
    WITH (STATE = ON);

-- 3) Purview sensitivity labels (apply via Purview / Fabric item labels)
--   gold.fact_booking: Confidential
--   gold.member_360: Confidential
--   gold.fact_email_engagement: Confidential
--   gold.fact_web_event: Confidential
--   gold.member_contactability: Confidential
--   gold.dim_member: Highly Confidential - Personal Data
