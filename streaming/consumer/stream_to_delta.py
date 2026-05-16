"""Spark Structured Streaming consumer: Kafka -> Delta (bronze).

Three streams write to three Delta sinks with watermarking + exactly-once.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, lit, sha2
from pyspark.sql.types import (DoubleType, StringType, StructField, StructType,
                               TimestampType)


SCHEMAS = {
    "hmis.bed_events": StructType([
        StructField("event_id",   StringType()),
        StructField("event_type", StringType()),
        StructField("client_id",  StringType()),
        StructField("project_id", StringType()),
        StructField("bed_id",     StringType()),
        StructField("event_time", TimestampType()),
    ]),
    "hmis.outreach": StructType([
        StructField("event_id",        StringType()),
        StructField("zone",            StringType()),
        StructField("client_id",       StringType()),
        StructField("encounter_type",  StringType()),
        StructField("needs",           StringType()),
        StructField("lat",             DoubleType()),
        StructField("lon",             DoubleType()),
        StructField("event_time",      TimestampType()),
    ]),
    "hmis.case_updates": StructType([
        StructField("event_id",       StringType()),
        StructField("op",             StringType()),
        StructField("client_id",      StringType()),
        StructField("enrollment_id",  StringType()),
        StructField("field_changed",  StringType()),
        StructField("new_value",      StringType()),
        StructField("event_time",     TimestampType()),
    ]),
}


def spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("hmis-stream-bronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def stream_topic(spark, bootstrap: str, topic: str, sink_base: str) -> None:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", "5000")
        .load()
    )
    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS j")
        .select(from_json(col("j"), SCHEMAS[topic]).alias("e"))
        .select("e.*")
        .withColumn("_ingest_ts", current_timestamp())
        .withColumn("_source_topic", lit(topic))
        .withWatermark("event_time", "15 minutes")
    )
    if "client_id" in parsed.columns:
        parsed = parsed.withColumn(
            "client_id_hash",
            sha2(col("client_id").cast("string"), 256),
        ).drop("client_id")

    short = topic.split(".")[-1]
    (
        parsed.writeStream.format("delta")
        .option("checkpointLocation", f"{sink_base}/_chk/{short}")
        .outputMode("append")
        .start(f"{sink_base}/{short}")
    )


def main(bootstrap: str = "localhost:9092",
         sink_base: str = "s3a://hmis-bronze") -> None:
    spark = spark_session()
    for topic in SCHEMAS:
        stream_topic(spark, bootstrap, topic, sink_base)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
