-- HMIS Privacy Plan compliant masking. Only caseworkers see the hash; analysts
-- see a tokenized form; HUD reporting sees an aggregated bucket only.

CREATE OR REPLACE MASKING POLICY hmis_client_mask AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() = 'R_HMIS_CASEWORKER'      THEN val
    WHEN CURRENT_ROLE() = 'R_HMIS_ANALYST'         THEN LEFT(val, 8) || '...'
    WHEN CURRENT_ROLE() = 'R_HMIS_HUD_REPORTING'   THEN 'AGG'
    ELSE NULL
  END;

ALTER TABLE HMIS.GOLD.fact_service_event
  MODIFY COLUMN client_id_hash SET MASKING POLICY hmis_client_mask;

-- Row-access policy: caseworkers only see their own org's projects.
CREATE OR REPLACE ROW ACCESS POLICY hmis_org_scope AS (project_id STRING) RETURNS BOOLEAN ->
  CURRENT_ROLE() IN ('R_HMIS_HUD_REPORTING')
  OR project_id IN (SELECT project_id FROM HMIS.GOLD.user_project_grants
                    WHERE user_name = CURRENT_USER());
