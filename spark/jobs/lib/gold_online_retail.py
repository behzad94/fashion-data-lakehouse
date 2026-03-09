from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_customer_rfm(df: DataFrame, snapshot_date: str) -> DataFrame:
    """
    Build one customer-level RFM table from Silver transaction data.

    Output columns:
    - CustomerID
    - snapshot_date
    - last_order_date
    - recency_days
    - frequency_orders
    - monetary_value
    """
    return (
        df
        .withColumn("line_revenue", F.col("Quantity") * F.col("UnitPrice"))
        .groupBy("CustomerID")
        .agg(
            F.max("InvoiceDate").alias("last_order_ts"),
            F.countDistinct("InvoiceNo").alias("frequency_orders"),
            F.sum("line_revenue").alias("monetary_value"),
        )
        .withColumn("snapshot_date", F.to_date(F.lit(snapshot_date)))
        .withColumn("last_order_date", F.to_date(F.col("last_order_ts")))
        .withColumn(
            "recency_days",
            F.datediff(F.col("snapshot_date"), F.col("last_order_date"))
        )
        .select(
            "CustomerID",
            "snapshot_date",
            "last_order_date",
            "recency_days",
            "frequency_orders",
            "monetary_value",
        )
    )