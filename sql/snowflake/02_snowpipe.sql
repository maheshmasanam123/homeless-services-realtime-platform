-- Snowpipe continuous ingest from ADLS Gen2 gold layer.
CREATE STORAGE INTEGRATION IF NOT EXISTS hmis_azure_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = '<tenant-guid>'
  STORAGE_ALLOWED_LOCATIONS = ('azure://hmislake.dfs.core.windows.net/gold/');

CREATE OR REPLACE STAGE HMIS.GOLD.adls_gold_stage
  STORAGE_INTEGRATION = hmis_azure_int
  URL = 'azure://hmislake.dfs.core.windows.net/gold/fact_service_event/'
  FILE_FORMAT = (TYPE = PARQUET);

CREATE OR REPLACE TABLE HMIS.GOLD.fact_service_event (
  service_id           STRING,
  client_id_hash       STRING,
  enrollment_id        STRING,
  project_id           STRING,
  project_type         NUMBER,
  service_type         STRING,
  service_date         DATE,
  qty                  NUMBER,
  financial_assistance NUMBER(12,2)
)
CLUSTER BY (service_date, project_id);

CREATE OR REPLACE PIPE HMIS.GOLD.fact_service_pipe
  AUTO_INGEST = TRUE
  INTEGRATION = 'AZURE_EVENT_GRID_INT'
AS
COPY INTO HMIS.GOLD.fact_service_event
FROM @HMIS.GOLD.adls_gold_stage
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
