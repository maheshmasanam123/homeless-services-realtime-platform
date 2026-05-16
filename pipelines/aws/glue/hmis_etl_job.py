"""AWS Glue job: agency S3 landing -> partitioned Parquet on the lake."""
from __future__ import annotations

import argparse
import sys


def transform(spark, src: str, tgt: str) -> int:
    from pyspark.sql.functions import col, sha2, year

    df = spark.read.json(src)
    out = (
        df.withColumn("SSN_hash",  sha2(col("SSN").cast("string"),  256))
          .withColumn("Name_hash", sha2(col("FirstName").cast("string"), 256))
          .drop("SSN", "FirstName", "LastName", "PersonalEmail")
          .withColumn("YearOfBirth", year("DOB"))
          .drop("DOB")
    )
    (
        out.write.mode("append")
           .partitionBy("YearOfBirth")
           .format("parquet").option("compression", "snappy")
           .save(tgt)
    )
    return out.count()


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--src", default="s3://hmis-landing/clients/")
    p.add_argument("--tgt", default="s3://hmis-lake/clients/")
    p.add_argument("--local", action="store_true")
    args = p.parse_args()

    if args.local:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("hmis-glue-local").getOrCreate()
    else:
        from awsglue.context import GlueContext
        from pyspark.context import SparkContext
        spark = GlueContext(SparkContext.getOrCreate()).spark_session

    print(f"wrote {transform(spark, args.src, args.tgt)} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
