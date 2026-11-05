"""Gold: star-schema fact with point-in-time dimension lookup, and a revenue aggregate."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

UNKNOWN_SK = -1


def build_fct_orders(orders: DataFrame, dim_customer: DataFrame) -> DataFrame:
    """Join each order to the customer version that was valid at order time.

    Orders with no matching version get the unknown member (-1) instead of being
    dropped, so revenue totals always reconcile with Silver.
    """
    d = dim_customer.select("customer_sk", "customer_id", "effective_from", "effective_to").alias("d")
    o = orders.alias("o")
    cond = (
        (F.col("o.customer_id") == F.col("d.customer_id"))
        & (F.col("o.order_ts") >= F.col("d.effective_from"))
        & (F.col("d.effective_to").isNull() | (F.col("o.order_ts") < F.col("d.effective_to")))
    )
    return o.join(d, cond, "left").select(
        F.col("o.order_id"),
        F.coalesce(F.col("d.customer_sk"), F.lit(UNKNOWN_SK).cast("long")).alias("customer_sk"),
        F.col("o.customer_id"),
        F.col("o.order_ts"),
        F.col("o.order_date"),
        F.col("o.status"),
        F.col("o.amount_usd"),
    )


def build_daily_revenue(fct: DataFrame, dim_customer: DataFrame) -> DataFrame:
    states = dim_customer.select("customer_sk", "state")
    return (
        fct.filter(F.col("status") != "CANCELLED")
        .join(states, "customer_sk", "left")
        .withColumn("state", F.coalesce("state", F.lit("UNKNOWN")))
        .groupBy("order_date", "state")
        .agg(F.count("*").alias("orders"), F.round(F.sum("amount_usd"), 2).alias("revenue_usd"))
    )
