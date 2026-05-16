"""HMIS-compatible synthetic data generator.

Schema follows the HUD HMIS Data Standards (Universal Data Elements):
  Client, Enrollment, Project, Service, Disability, HealthAndDV.

All PII is fake (Faker). Real deployments would never write raw PII past the
ingestion gateway; here we emit it so the bronze/silver layers can demonstrate
hashing + masking.
"""
from __future__ import annotations

import argparse
import json
import random
from datetime import date, datetime, timedelta
from pathlib import Path

from faker import Faker

fake = Faker()
Faker.seed(11)
random.seed(11)

PROJECT_TYPES = {
    1:  "Emergency Shelter",
    2:  "Transitional Housing",
    3:  "PH - Permanent Supportive Housing",
    4:  "Street Outreach",
    6:  "Services Only",
    7:  "Other",
    8:  "Safe Haven",
    9:  "PH - Housing Only",
    10: "PH - Housing with Services",
    11: "Day Shelter",
    12: "Homelessness Prevention",
    13: "PH - Rapid Re-Housing",
    14: "Coordinated Entry",
}

LIVING_SITUATIONS = [
    "Place not meant for habitation", "Emergency shelter", "Safe Haven",
    "Transitional housing", "Hospital", "Jail/prison", "Substance abuse facility",
    "Psychiatric facility", "Hotel/motel (no voucher)", "Foster care",
    "Staying with family", "Staying with friends", "Rental by client",
    "Owned by client",
]

DESTINATIONS = [
    "Permanent housing", "Rental by client (no subsidy)", "Rental with subsidy",
    "Staying with family (permanent)", "Hotel/motel (no voucher)",
    "Emergency shelter", "Transitional housing", "Place not meant for habitation",
    "Deceased", "Other/unknown",
]

SERVICE_TYPES = [
    "Bed-night", "Meal", "Case management", "Clinic visit",
    "Transportation", "Hygiene kit", "Mental health session", "Housing search",
]


def make_client(i: int) -> dict:
    dob = fake.date_of_birth(minimum_age=18, maximum_age=80)
    return {
        "ClientID":      f"C{i:08d}",
        "FirstName":     fake.first_name(),
        "LastName":      fake.last_name(),
        "SSN":           fake.ssn(),
        "DOB":           dob.isoformat(),
        "Gender":        random.choice(["Female", "Male", "Trans Female",
                                        "Trans Male", "Gender non-conforming",
                                        "Client doesn't know", "Client refused"]),
        "Race":          random.choice(["AmIndAKNative", "Asian", "BlackAfAmerican",
                                        "NativeHIPacific", "White", "Multiracial"]),
        "Ethnicity":     random.choice(["Non-Hispanic", "Hispanic/Latino"]),
        "VeteranStatus": random.choices([1, 0], weights=[1, 9])[0],
        "PersonalEmail": fake.email(),
    }


def make_project(i: int) -> dict:
    ptype = random.choice(list(PROJECT_TYPES.keys()))
    return {
        "ProjectID":      f"P{i:04d}",
        "ProjectName":    f"{fake.city()} {PROJECT_TYPES[ptype]}",
        "ProjectType":    ptype,
        "ProjectTypeDesc": PROJECT_TYPES[ptype],
        "OrganizationID": f"O{random.randint(1, 30):03d}",
        "OperatingStartDate": fake.date_between("-5y", "-1y").isoformat(),
        "TargetPopulation": random.choice(["All", "Domestic violence", "Veterans",
                                            "Youth", "Families with children"]),
        "Latitude":  float(fake.latitude()),
        "Longitude": float(fake.longitude()),
    }


def make_enrollment(client: dict, project: dict, i: int) -> dict:
    entry = fake.date_between("-1y", "today")
    exited = random.random() < 0.6
    exit_date = (entry + timedelta(days=random.randint(1, 180))) if exited else None
    return {
        "EnrollmentID":  f"E{i:09d}",
        "PersonalID":    client["ClientID"],
        "ProjectID":     project["ProjectID"],
        "EntryDate":     entry.isoformat(),
        "ExitDate":      exit_date.isoformat() if exit_date else None,
        "Destination":   random.choice(DESTINATIONS) if exited else None,
        "LivingSituation": random.choice(LIVING_SITUATIONS),
        "RelationshipToHoH": random.choice(["Self", "Child", "Spouse", "Other"]),
        "DisablingCondition": random.choices([1, 0], weights=[4, 6])[0],
    }


def make_service(enrollment: dict, i: int) -> dict:
    return {
        "ServiceID":     f"S{i:010d}",
        "EnrollmentID":  enrollment["EnrollmentID"],
        "PersonalID":    enrollment["PersonalID"],
        "DateProvided":  fake.date_between(
            datetime.fromisoformat(enrollment["EntryDate"]).date(),
            "today",
        ).isoformat(),
        "ServiceType":   random.choice(SERVICE_TYPES),
        "QuantityOfServices": random.randint(1, 5),
        "FAAmount":      round(random.uniform(0, 1500), 2),
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--clients",    type=int, default=2000)
    p.add_argument("--projects",   type=int, default=40)
    p.add_argument("--enrollments-per-client", type=int, default=2)
    p.add_argument("--out", default="data/seed")
    args = p.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    clients   = [make_client(i)   for i in range(args.clients)]
    projects  = [make_project(i)  for i in range(args.projects)]

    enrollments, services = [], []
    s_id = 0
    for i, c in enumerate(clients):
        for _ in range(random.randint(1, args.enrollments_per_client)):
            e = make_enrollment(c, random.choice(projects), len(enrollments))
            enrollments.append(e)
            for _ in range(random.randint(1, 12)):
                services.append(make_service(e, s_id)); s_id += 1

    for name, rows in [("clients", clients), ("projects", projects),
                       ("enrollments", enrollments), ("services", services)]:
        path = out / f"{name}.jsonl"
        with path.open("w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r, default=str) + "\n")
        print(f"{name}: {len(rows):>7} -> {path}")


if __name__ == "__main__":
    main()
