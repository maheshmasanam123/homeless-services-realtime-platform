"""Gold: star schema for HUD reporting + operational dashboards.

  fact_service_event   one row per service delivered
  dim_client           SCD2 with current/historical
  dim_project          shelter / outreach / clinic
  dim_program_type     HMIS Project Type code lookup
  dim_date             calendar
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, current_timestamp, date_format, dayofmonth,
                                   lit, monotonically_increasing_id, month,
                                   quarter, to_date, year)


def spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("hmis-gold")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def build(silver: str, gold: str) -> None:
    spark = spark_session()

    clients     = spark.read.format("delta").load(f"{silver}/clients").alias("c")
    enrollments = spark.read.format("delta").load(f"{silver}/enrollments").alias("e")
    services    = spark.read.format("delta").load(f"{silver}/services").alias("s")
    projects    = spark.read.format("delta").load("data/bronze/projects").alias("p")

    fact = (
        services.join(enrollments,
                      col("s.EnrollmentID") == col("e.EnrollmentID"), "inner")
                .join(projects,
                      col("e.ProjectID") == col("p.ProjectID"), "left")
                .select(
                    col("s.ServiceID").alias("service_id"),
                    col("s.PersonalID_hash").alias("client_id_hash"),
                    col("e.EnrollmentID").alias("enrollment_id"),
                    col("p.ProjectID").alias("project_id"),
                    col("p.ProjectType").alias("project_type"),
                    col("s.ServiceType").alias("service_type"),
                    col("s.DateProvided").alias("service_date"),
                    col("s.QuantityOfServices").cast("int").alias("qty"),
                    col("s.FAAmount").cast("double").alias("financial_assistance"),
                )
    )
    (
        fact.write.format("delta").mode("overwrite")
            .partitionBy("service_date")
            .save(f"{gold}/fact_service_event")
    )

    # SCD2-lite dim_client (current snapshot here; SCD2 history built via CDC dag)
    dim_client = (
        clients.select("ClientID_hash", "Gender", "Race", "Ethnicity",
                       "VeteranStatus", "YearOfBirth")
               .withColumn("client_key", monotonically_increasing_id())
               .withColumn("valid_from", current_timestamp())
               .withColumn("valid_to",  lit(None).cast("timestamp"))
               .withColumn("is_current", lit(True))
    )
    dim_client.write.format("delta").mode("overwrite").save(f"{gold}/dim_client")

    dim_project = (
        projects.select("ProjectID", "ProjectName", "ProjectType",
                        "ProjectTypeDesc", "OrganizationID",
                        "TargetPopulation", "Latitude", "Longitude")
                .withColumn("project_key", monotonically_increasing_id())
    )
    dim_project.write.format("delta").mode("overwrite").save(f"{gold}/dim_project")

    dim_date = (
        fact.select("service_date").distinct()
            .withColumn("date_key", date_format("service_date", "yyyyMMdd").cast("int"))
            .withColumn("year",  year("service_date"))
            .withColumn("quarter", quarter("service_date"))
            .withColumn("month", month("service_date"))
            .withColumn("day",   dayofmonth("service_date"))
    )
    dim_date.write.format("delta").mode("overwrite").save(f"{gold}/dim_date")

    print("gold built: fact_service_event, dim_client, dim_project, dim_date")


if __name__ == "__main__":
    build("data/silver", "data/gold")
