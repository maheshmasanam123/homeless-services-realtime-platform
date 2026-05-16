-- Hive external tables over the Sqoop landing directories, partitioned by date.
CREATE DATABASE IF NOT EXISTS hmis_raw;

CREATE EXTERNAL TABLE IF NOT EXISTS hmis_raw.services_ext (
  ServiceID            STRING,
  EnrollmentID         STRING,
  PersonalID           STRING,
  DateProvided         DATE,
  ServiceType          STRING,
  QuantityOfServices   INT,
  FAAmount             DECIMAL(12,2)
)
PARTITIONED BY (ingest_date STRING)
STORED AS PARQUET
LOCATION '/raw/hmis/services/';

MSCK REPAIR TABLE hmis_raw.services_ext;

-- Daily aggregation written back to Hive for compliance review.
CREATE TABLE IF NOT EXISTS hmis_raw.services_daily AS
SELECT ingest_date,
       ServiceType,
       COUNT(*)         AS n,
       SUM(FAAmount)    AS fa_total
FROM   hmis_raw.services_ext
GROUP BY ingest_date, ServiceType;
