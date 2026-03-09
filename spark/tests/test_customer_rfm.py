from pyspark.sql import SparkSession
from spark.jobs.lib.gold_online_retail import build_customer_rfm


def test_customer_rfm_basic():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_customer_rfm")
        .getOrCreate()
    )

    data = [
        ("10001", 1, "2011-12-01 10:00:00", 2, 10.0),
        ("10002", 1, "2011-12-02 11:00:00", 1, 20.0),
        ("10003", 2, "2011-12-03 12:00:00", 3, 5.0),
    ]

    columns = ["InvoiceNo", "CustomerID", "InvoiceDate", "Quantity", "UnitPrice"]

    df = spark.createDataFrame(data, columns)

    df = df.withColumn("InvoiceDate", df["InvoiceDate"].cast("timestamp"))

    result = build_customer_rfm(df, "2011-12-10")

    rows = {r["CustomerID"]: r for r in result.collect()}

    assert rows[1]["frequency_orders"] == 2
    assert rows[1]["monetary_value"] == 40.0

    assert rows[2]["frequency_orders"] == 1
    assert rows[2]["monetary_value"] == 15.0

    spark.stop()