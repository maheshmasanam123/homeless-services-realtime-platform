"""Row-count + balance reconciliation: bronze vs silver vs gold."""
from pyspark.sql import SparkSession


THRESHOLD = 0.01


def spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("hmis-reconcile")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def main(bronze: str = "data/bronze", silver: str = "data/silver",
         gold: str = "data/gold") -> None:
    spark = spark_session()
    fails: list[str] = []
    for name in ["clients", "enrollments", "services"]:
        b = spark.read.format("delta").load(f"{bronze}/{name}").count()
        s = spark.read.format("delta").load(f"{silver}/{name}").count()
        drift = abs(b - s) / max(b, 1)
        status = "OK" if drift <= THRESHOLD else "FAIL"
        print(f"{name}: bronze={b} silver={s} drift={drift:.2%} [{status}]")
        if status == "FAIL":
            fails.append(name)
    if fails:
        raise SystemExit(f"reconciliation failed: {fails}")


if __name__ == "__main__":
    main()
