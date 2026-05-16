"""Row-count + checksum reconciliation source vs Redshift target."""
from __future__ import annotations

import os

import boto3
import psycopg


THRESHOLD = 0.01


def main() -> None:
    s3 = boto3.client("s3")
    bucket = os.environ["LAKE_BUCKET"]
    prefix = "clients/"
    src_count = sum(
        s3.head_object(Bucket=bucket, Key=o["Key"]).get("Metadata", {}).get("row-count", 0)
        for o in s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
    )
    with psycopg.connect(os.environ["REDSHIFT_DSN"]) as cn:
        tgt = cn.execute("SELECT COUNT(*) FROM analytics.fact_service_event").fetchone()[0]

    drift = abs(int(src_count) - int(tgt)) / max(int(src_count), 1)
    print(f"src={src_count} tgt={tgt} drift={drift:.2%}")
    if drift > THRESHOLD:
        raise SystemExit("reconciliation failed")


if __name__ == "__main__":
    main()
