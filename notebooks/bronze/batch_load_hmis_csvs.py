"""Bronze batch loader (Auto Loader-style) for the HMIS seed extracts.

In Databricks this would use `cloudFiles`; locally we use the file source for
identical semantics.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name


TABLES = ["clients", "projects", "enrollments", "services"]


def spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("hmis-bronze-batch")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .getOrCreate()
    )


def main(seed: str = "data/seed", bronze: str = "data/bronze") -> None:
    spark = spark_session()
    for t in TABLES:
        df = (
            spark.read.json(f"{seed}/{t}.jsonl")
            .withColumn("_ingest_ts", current_timestamp())
            .withColumn("_source_file", input_file_name())
        )
        (
            df.write.format("delta").mode("append")
            .option("mergeSchema", "true")
            .save(f"{bronze}/{t}")
        )
        print(f"bronze.{t}: {df.count()} rows")


if __name__ == "__main__":
    main()
