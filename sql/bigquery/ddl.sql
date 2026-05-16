CREATE SCHEMA IF NOT EXISTS hmis_raw;
CREATE SCHEMA IF NOT EXISTS hmis_curated;

CREATE TABLE IF NOT EXISTS hmis_raw.events (
  event_id    STRING,
  event_type  STRING,
  client_id   STRING,
  project_id  STRING,
  event_time  TIMESTAMP,
  payload     STRING
)
PARTITION BY DATE(event_time)
CLUSTER BY project_id;

CREATE OR REPLACE VIEW hmis_curated.daily_service_counts AS
SELECT
  DATE(event_time) AS service_date,
  project_id,
  event_type,
  COUNT(*) AS n
FROM hmis_raw.events
GROUP BY 1, 2, 3;
