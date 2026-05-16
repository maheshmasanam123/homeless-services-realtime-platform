# Architecture deep-dive

## Why federated multi-cloud

Real-world homeless service networks span agencies that already run on
different clouds. Forcing a single cloud is politically infeasible. This
platform demonstrates a federated pattern: each region keeps its native
ingest stack and converges into one Snowflake-served gold layer for HUD
reporting and cross-region analytics.

## Streaming vs batch boundary

- **Streaming** (Kafka → Spark Structured Streaming → Delta):
  bed check-ins, outreach encounters, case-management CDC. These need to
  update the live coordinator dashboard in seconds.
- **Batch** (ADF/Glue/Dataflow nightly):
  HMIS reference data (clients, projects, enrollments). HUD APR reporting
  is monthly; bronze is the system of record.

## Medallion layout

```
bronze/  raw + immutable. Includes PII (encrypted at rest).
silver/  cleansed, deduped, PII hashed, types conformed.
gold/    star schema for analytics + dashboards.
```

## CDC for SCD2 dim_client

Agency Postgres → Debezium → Kafka topic `hmis.case_updates` →
Spark Structured Streaming → Delta `silver/client_changes` → nightly Airflow
MERGE into `gold/dim_client` as SCD Type 2.

## Privacy & governance

See `docs/HMIS_DATA_DICT.md`. PII hashing happens before silver write; masking
policies at the Snowflake gold layer further restrict visibility per role.

## Disaster recovery

- Delta time travel: 14 days
- Snowflake fail-safe: 7 days
- All Terraform state in remote S3 backend with state locking
