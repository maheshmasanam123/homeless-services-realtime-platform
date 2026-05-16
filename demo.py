"""End-to-end HMIS medallion demo using pandas (no Spark, no Docker, no cloud).

Pipeline:
    JSONL seed -> bronze (raw) -> silver (PII hashed) -> gold (star schema)
                                                              |
                                          fact_service_event + dim_client + dim_project + dim_date

Usage:
    python -m generator.run --clients 500 --projects 20
    python demo.py
"""
from __future__ import annotations

import hashlib
from pathlib import Path

import pandas as pd


SEED   = Path("data/seed")
BRONZE = Path("data/bronze")
SILVER = Path("data/silver")
GOLD   = Path("data/gold")

PII_DROP = ["FirstName", "LastName", "SSN", "PersonalEmail"]


def _hash(v: str) -> str:
    return hashlib.sha256(str(v).encode()).hexdigest()


def bronze() -> None:
    BRONZE.mkdir(parents=True, exist_ok=True)
    for name in ["clients", "projects", "enrollments", "services"]:
        df = pd.read_json(SEED / f"{name}.jsonl", lines=True)
        df.to_parquet(BRONZE / f"{name}.parquet", index=False)
        print(f"bronze.{name}: {len(df)} rows")


def silver() -> None:
    SILVER.mkdir(parents=True, exist_ok=True)

    clients = pd.read_parquet(BRONZE / "clients.parquet")
    clients["ClientID_hash"] = clients["ClientID"].map(_hash)
    clients["SSN_hash"]      = clients["SSN"].map(_hash)
    clients["Name_hash"]     = (clients["FirstName"] + clients["LastName"]).map(_hash)
    clients["DOB"]           = pd.to_datetime(clients["DOB"])
    clients["YearOfBirth"]   = clients["DOB"].dt.year
    clients = clients.drop(columns=PII_DROP + ["DOB"]).drop_duplicates("ClientID_hash")
    clients.to_parquet(SILVER / "clients.parquet", index=False)

    enrollments = pd.read_parquet(BRONZE / "enrollments.parquet")
    enrollments["EntryDate"]       = pd.to_datetime(enrollments["EntryDate"])
    enrollments["ExitDate"]        = pd.to_datetime(enrollments["ExitDate"])
    enrollments["PersonalID_hash"] = enrollments["PersonalID"].map(_hash)
    enrollments = enrollments.drop(columns=["PersonalID"]).drop_duplicates("EnrollmentID")
    enrollments.to_parquet(SILVER / "enrollments.parquet", index=False)

    services = pd.read_parquet(BRONZE / "services.parquet")
    services["DateProvided"]    = pd.to_datetime(services["DateProvided"])
    services["PersonalID_hash"] = services["PersonalID"].map(_hash)
    services = services.drop(columns=["PersonalID"]).drop_duplicates("ServiceID")
    services.to_parquet(SILVER / "services.parquet", index=False)

    print(f"silver.clients:     {len(clients)} rows")
    print(f"silver.enrollments: {len(enrollments)} rows")
    print(f"silver.services:    {len(services)} rows")


def gold() -> None:
    GOLD.mkdir(parents=True, exist_ok=True)
    services    = pd.read_parquet(SILVER / "services.parquet")
    enrollments = pd.read_parquet(SILVER / "enrollments.parquet")
    projects    = pd.read_parquet(BRONZE / "projects.parquet")
    clients     = pd.read_parquet(SILVER / "clients.parquet")

    fact = (
        services.merge(enrollments, on="EnrollmentID", how="inner")
                .merge(projects, on="ProjectID", how="left")
                .rename(columns={
                    "ServiceID":     "service_id",
                    "PersonalID_hash_x": "client_id_hash",
                    "EnrollmentID":  "enrollment_id",
                    "ProjectID":     "project_id",
                    "ProjectType":   "project_type",
                    "ServiceType":   "service_type",
                    "DateProvided":  "service_date",
                    "QuantityOfServices": "qty",
                    "FAAmount":      "financial_assistance",
                })
        [["service_id", "client_id_hash", "enrollment_id", "project_id",
          "project_type", "service_type", "service_date", "qty",
          "financial_assistance"]]
    )
    fact.to_parquet(GOLD / "fact_service_event.parquet", index=False)

    dim_client = clients[["ClientID_hash", "Gender", "Race", "Ethnicity",
                          "VeteranStatus", "YearOfBirth"]].copy()
    dim_client["client_key"] = range(len(dim_client))
    dim_client["is_current"] = True
    dim_client.to_parquet(GOLD / "dim_client.parquet", index=False)

    dim_project = projects.copy()
    dim_project["project_key"] = range(len(dim_project))
    dim_project.to_parquet(GOLD / "dim_project.parquet", index=False)

    dim_date = pd.DataFrame({"service_date": sorted(fact["service_date"].dropna().unique())})
    dim_date["date_key"] = pd.to_datetime(dim_date["service_date"]).dt.strftime("%Y%m%d").astype(int)
    dim_date.to_parquet(GOLD / "dim_date.parquet", index=False)

    print(f"\ngold.fact_service_event: {len(fact)} rows")
    print(f"gold.dim_client:         {len(dim_client)} rows")
    print(f"gold.dim_project:        {len(dim_project)} rows")
    print(f"gold.dim_date:           {len(dim_date)} rows")

    print("\n--- service mix ---")
    print(fact["service_type"].value_counts().to_string())

    print("\n--- top 5 project types by services delivered ---")
    print(fact.groupby("project_type")["qty"].sum().nlargest(5).to_string())

    print(f"\nfinancial assistance disbursed: ${fact['financial_assistance'].sum():,.2f}")

    print("\n--- PII check: no raw PII columns in silver/gold ---")
    forbidden = {"FirstName", "LastName", "SSN", "PersonalEmail", "DOB", "PersonalID"}
    leaked = forbidden & set(fact.columns) | forbidden & set(dim_client.columns)
    print(f"leaked PII columns: {leaked or 'none — clean'}")


def main() -> None:
    bronze(); silver(); gold()


if __name__ == "__main__":
    main()
