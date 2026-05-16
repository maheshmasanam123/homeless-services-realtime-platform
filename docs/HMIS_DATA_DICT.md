# HMIS Data Dictionary (subset)

This project uses a faithful subset of the **HUD Homeless Management
Information System (HMIS) Data Standards** Universal Data Elements. The full
spec is published by HUD; this dictionary documents the elements we model.

## Client (3.x UDEs)

| Field         | HMIS UDE | Type   | Notes                          |
| ------------- | -------- | ------ | ------------------------------ |
| ClientID      | 5.1      | string | Unique person identifier       |
| FirstName     | 3.01     | string | **PII — bronze only**          |
| LastName      | 3.01     | string | **PII — bronze only**          |
| SSN           | 3.02     | string | **PII — bronze only, hashed**  |
| DOB           | 3.03     | date   | **PII — silver retains year**  |
| Gender        | 3.06     | enum   |                                |
| Race          | 3.04     | enum   |                                |
| Ethnicity     | 3.05     | enum   |                                |
| VeteranStatus | 3.07     | 0/1    |                                |

## Enrollment (3.10–3.12)

EnrollmentID, EntryDate, ExitDate, Destination, LivingSituation,
RelationshipToHoH, DisablingCondition.

## Project (2.02)

ProjectID, ProjectName, ProjectType, OrganizationID, OperatingStartDate,
TargetPopulation, Latitude, Longitude. ProjectType uses HMIS codes 1–14.

## Service (4.x)

ServiceID, EnrollmentID, DateProvided, ServiceType, QuantityOfServices,
FAAmount (financial assistance dollars).

## PII handling rules (HMIS Privacy Plan)

1. Raw PII (`FirstName`, `LastName`, `SSN`, `DOB`, `PersonalEmail`) is **only**
   permitted in the bronze layer and only on encrypted-at-rest storage.
2. Silver layer retains SHA-256 hashes plus `YearOfBirth`.
3. Gold layer exposes `client_id_hash` only.
4. Snowflake masking policies further restrict who sees the hash:
   caseworkers see full hash, analysts see truncated, HUD reporting sees a
   bucket token.
5. Row-access policy restricts caseworkers to their own organization's
   projects.
