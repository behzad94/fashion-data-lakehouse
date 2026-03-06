import argparse
import logging
import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def build_spark_session(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
    return spark


def main():
    # -----------------------------
    # 1. Parse arguments
    # -----------------------------
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to config file")
    parser.add_argument("--ds", required=True, help="Execution date (YYYY-MM-DD)")
    args = parser.parse_args()

    # -----------------------------
    # 2. Logging setup
    # -----------------------------
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("gold_marts")

    logger.info("Starting Gold Marts Job")
    logger.info(f"Config path: {args.config}")
    logger.info(f"Execution date (ds): {args.ds}")

    # -----------------------------
    # 3. Load config
    # -----------------------------
    config = load_config(args.config)

    bucket = config["minio"]["bucket"]

    silver_base = config["silver"]["base_path"]
    silver_dataset = config["silver"]["dataset_name"]

    gold_base = config["gold"]["base_path"]
    gold_dataset = config["gold"]["dataset_name"]

    # -----------------------------
    # 4. Build paths
    # -----------------------------
    silver_input_path = (
        f"s3a://{bucket}/"
        f"{silver_base}/"
        f"{silver_dataset}/"
        f"ingest_date={args.ds}/"
    )

    gold_output_base_path = (
        f"s3a://{bucket}/"
        f"{gold_base}/"
        f"{gold_dataset}/"
        f"ds={args.ds}/"
    )

    logger.info(f"Silver input path: {silver_input_path}")
    logger.info(f"Gold output base path: {gold_output_base_path}")

    # -----------------------------
    # 5. Start Spark
    # -----------------------------
    spark = build_spark_session("gold-marts")

    # -----------------------------
    # 6. Read Silver Data
    # -----------------------------
    df = spark.read.parquet(silver_input_path)

    record_count = df.count()
    logger.info(f"Silver record count: {record_count}")

    # -------------------------------------------------
    # DAILY REVENUE MART
    # -------------------------------------------------
    from pyspark.sql import functions as F

    logger.info("Building daily_revenue mart...")

    # Expect these columns exist in Silver:
    # InvoiceDate (timestamp), InvoiceNo (string), CustomerID (string/int), Quantity (int), UnitPrice (double)
    df2 = (
        df
        .withColumn("date", F.to_date(F.col("InvoiceDate")))
        .withColumn("line_revenue", F.col("Quantity") * F.col("UnitPrice"))
    )

    # Business rule: remove negative revenue lines (returns/cancellations)
    df2 = df2.filter(F.col("line_revenue") >= 0)

    daily_revenue = (
        df2.groupBy("date")
        .agg(
            F.round(F.sum("line_revenue"), 2).alias("revenue"),
            F.countDistinct("InvoiceNo").alias("orders"),
            F.countDistinct("CustomerID").alias("customers"),
        )
        .orderBy("date")
    )

    daily_count = daily_revenue.count()
    logger.info(f"daily_revenue rows: {daily_count}")

    daily_path = gold_output_base_path + "daily_revenue/"
    logger.info(f"Writing daily_revenue to: {daily_path}")

    (
        daily_revenue
        .repartition(1)  # small output, easier to inspect
        .write.mode("overwrite")
        .partitionBy("date")
        .parquet(daily_path)
    )

    # -------------------------------------------------
    # TOP PRODUCTS MART
    # -------------------------------------------------
    logger.info("Building top_products mart...")

    # Revenue per line (safe: does not depend on total_price existing)
    top_df = (
        df
        .withColumn("line_revenue", F.col("Quantity") * F.col("UnitPrice"))
        .filter(F.col("line_revenue") >= 0)
    )

    top_products = (
        top_df.groupBy("StockCode", "Description")
        .agg(
            F.round(F.sum("line_revenue"), 2).alias("revenue"),
            F.sum("Quantity").alias("qty"),
            F.countDistinct("CustomerID").alias("customers"),
        )
        .orderBy(F.col("revenue").desc())
    )

    top_count = top_products.count()
    logger.info(f"top_products rows: {top_count}")

    top_products_path = gold_output_base_path + "top_products/"
    logger.info(f"Writing top_products to: {top_products_path}")

    (
        top_products
        .repartition(1)
        .write.mode("overwrite")
        .parquet(top_products_path)
    )

    logger.info("Gold job completed successfully.")

    spark.stop()


if __name__ == "__main__":
    main()