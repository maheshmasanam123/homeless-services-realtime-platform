"""Silver: cleanse, conform types, hash PII, dedupe.

PII rules (per HMIS Privacy Plan template):
  - FirstName, LastName, SSN, DOB, PersonalEmail are NEVER stored beyond
    bronze. Silver retains only SHA-256 hashes plus a year-of-birth derivative.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, to_date, year


PII = ["FirstName", "LastName", "SSN", "DOB", "PersonalEmail"]


def spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("hmis-silver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def conform_clients(spark, bronze: str, silver: str) -> None:
    df = spark.read.format("delta").load(f"{bronze}/clients")
    df = (
        df.withColumn("ClientID_hash", sha2(col("ClientID"), 256))
          .withColumn("SSN_hash",      sha2(col("SSN"),      256))
          .withColumn("Name_hash",     sha2(col("FirstName"), 256))  # combined upstream
          .withColumn("DOB",           to_date("DOB"))
          .withColumn("YearOfBirth",   year("DOB"))
          .drop(*PII)
          .dropDuplicates(["ClientID_hash"])
    )
    df.write.format("delta").mode("overwrite").save(f"{silver}/clients")


def conform_enrollments(spark, bronze: str, silver: str) -> None:
    df = (
        spark.read.format("delta").load(f"{bronze}/enrollments")
        .withColumn("EntryDate", to_date("EntryDate"))
        .withColumn("ExitDate",  to_date("ExitDate"))
        .withColumn("PersonalID_hash", sha2(col("PersonalID"), 256))
        .drop("PersonalID")
        .dropDuplicates(["EnrollmentID"])
    )
    df.write.format("delta").mode("overwrite").save(f"{silver}/enrollments")


def conform_services(spark, bronze: str, silver: str) -> None:
    df = (
        spark.read.format("delta").load(f"{bronze}/services")
        .withColumn("DateProvided", to_date("DateProvided"))
        .withColumn("PersonalID_hash", sha2(col("PersonalID"), 256))
        .drop("PersonalID")
        .dropDuplicates(["ServiceID"])
    )
    df.write.format("delta").mode("overwrite").save(f"{silver}/services")


def main(bronze: str = "data/bronze", silver: str = "data/silver") -> None:
    spark = spark_session()
    conform_clients(spark, bronze, silver)
    conform_enrollments(spark, bronze, silver)
    conform_services(spark, bronze, silver)
    print("silver refreshed")


if __name__ == "__main__":
    main()
