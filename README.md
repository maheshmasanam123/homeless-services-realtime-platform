# Homeless Services Real-Time Platform

A federated, multi-cloud, HMIS-compatible data platform that coordinates
**shelter beds, outreach encounters, meal services, and case management**
across a network of homeless-service agencies in real time.

Built on the **HUD Homeless Management Information System (HMIS) Data
Standards** — a real, federally-published schema used by every funded
homeless services agency in the United States.

## Mission

A person in crisis arrives at any partner agency. Within seconds the platform
can answer:
- Where is the nearest open shelter bed tonight?
- Which clinic has the shortest wait?
- Has this client been seen before, and what's their case status?
- Which outreach zone is underserved this hour?

Front-line staff get a live dashboard. Funders and HUD get HMIS-compliant
reporting. Clients keep their identities protected by hashing + masking.

## Architecture

```
                     +-----------------------------------------+
                     |   Agencies (shelters, clinics, food,    |
                     |   outreach vans, case-management apps)  |
                     +-----------------------------------------+
                                       |
                 +---------------------+---------------------+
                 |                     |                     |
                 v                     v                     v
          +---------------+    +---------------+    +---------------+
          |  Kafka topic  |    |  Kafka topic  |    |  Kafka topic  |
          |  bed_events   |    |  outreach     |    |  case_updates |
          +---------------+    +---------------+    +---------------+
                 |                     |                     |
                 +----------+----------+----------+----------+
                            |                     |
                +-----------v-----------+ +-------v---------+
                | Spark Structured      | | Debezium CDC    |
                | Streaming (exactly-   | | from agency     |
                | once, watermarks)     | | Postgres / SQL  |
                +-----------+-----------+ +-------+---------+
                            |                     |
        +-------------------+---------+-----------+
        |                             |
        v                             v
  Azure branch              AWS branch              GCP branch
  ADF + ADLS Gen2 +         S3 + Glue + EMR +       Pub/Sub + Dataflow +
  Databricks (medallion)    Redshift + Lambda       BigQuery
        |                       |                       |
        +-----------+-----------+-----------+-----------+
                    |                       |
                    v                       v
            Snowflake (gold)         Power BI / Tableau
            star schema +            dashboards for
            Snowpipe ingest          coordinators & HUD
```

## HMIS schema (Universal Data Elements)

The HUD HMIS Data Standards define a universal client + enrollment schema.
We model:

- **Client**: anonymized ID, DOB (year only), gender, race, ethnicity, veteran status
- **Enrollment**: program, entry date, exit date, destination
- **Project**: shelter / outreach / RRH / PSH / day-center
- **Service**: bed-night, meal, case-management session, clinic visit
- **Disability**: physical, mental, substance use, chronic
- **HealthAndDV**: health insurance, DV history, current living situation

All PII fields (name, SSN, DOB) are SHA-256 hashed on entry to bronze and
never written in clear text past the ingestion gateway.

## What this exercises

| Skill area              | Where it shows up                                                |
| ----------------------- | ---------------------------------------------------------------- |
| Streaming               | Kafka producers + Spark Structured Streaming consumers           |
| Multi-cloud             | Azure, AWS, GCP branches all converging into Snowflake gold      |
| Lakehouse / medallion   | Bronze / silver / gold on Delta Lake                             |
| CDC                     | Debezium on agency Postgres → SCD2 dim_client                    |
| Warehousing             | Snowflake, Redshift, BigQuery, Synapse                           |
| Star schema             | fact_service_event + dim_client/program/agency/date/geography    |
| Orchestration           | Airflow DAGs (open-source) + ADF JSON + Control-M sample         |
| Data quality            | Great Expectations suites in CI                                  |
| Governance              | PII hashing, column-level masking, data lineage docs             |
| BI                      | Streamlit live ops view + Power BI + Tableau PBIDS/TWBX          |
| DevOps                  | Docker compose, Terraform, GitHub Actions + GitLab CI + Jenkins  |
| Legacy bridge           | Sqoop + Hive scripts simulating a CDH agency feed                |

## Quick start

**Zero-infra demo (3 commands, pure Python):**
```bash
pip install faker pandas pyarrow
python -m generator.run --clients 500 --projects 20
python demo.py
```

Expected output: 5,000+ service events flow through bronze → silver → gold
star schema, PII columns confirmed scrubbed, service mix and financial
assistance totals printed.

**Full streaming + multi-cloud flavor (Docker):**
```bash
docker compose -f docker/docker-compose.yml up -d
python -m generator.run --clients 2000
python -m streaming.producer.send_events --rate 30   # Kafka events
streamlit run dashboards/streamlit/app.py            # live dashboard
```

## Repo layout

```
generator/                  HMIS-compliant synthetic data generator
streaming/                  Kafka producer + Spark Structured Streaming
pipelines/azure/            ADF JSON + Databricks notebooks
pipelines/aws/              Glue PySpark jobs + Redshift loaders
pipelines/gcp/              Beam/Dataflow + BigQuery
pipelines/hadoop_legacy/    Sqoop + Hive + Oozie (CDH-style)
pipelines/snowflake/        Snowpipe + masking + tasks
pipelines/dbt/              Star schema models + tests
pipelines/airflow/          DAGs orchestrating all of the above
notebooks/                  bronze/, silver/, gold/ PySpark
sql/                        DDL + masking views (per warehouse)
lambda/                     Event-driven Glue/Pipe triggers
iac/terraform/              Reproducible infra
dashboards/                 Streamlit, Power BI (.pbids), Tableau (.twb)
great_expectations/         Suites + checkpoints
docker/                     Local stack: Kafka, MinIO, Spark, Postgres
.github/.gitlab/jenkins/    CI flavors
docs/                       HMIS data dictionary, masking spec, runbooks
```
