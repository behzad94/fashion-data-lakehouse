from datetime import date
import sys
from pyspark.sql import SparkSession

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .master("spark://spark-master:7077")
        .appName("silver_transform_online_retail")
        .getOrCreate()
    )

def main() -> None:
    ingest_date = sys.argv[1] if len(sys.argv) > 1 else str(date.today())
    spark = build_spark()

    bronze_path = f"s3a://lake/bronze/online_retail/ingest_date={ingest_date}/"
    silver_path = f"s3a://lake/silver/online_retail/ingest_date={ingest_date}/"

    df = spark.read.parquet(bronze_path)

    (
        df.write
        .mode("overwrite")
        .parquet(silver_path)
    )

    spark.stop()

if __name__ == "__main__":
    main()
