"""Silver: typed, validated, deduplicated records and an SCD Type 2 customer dimension."""

from __future__ import annotations

from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

VALID_STATUSES = ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED"]

DIM_CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_sk", LongType(), False),
        StructField("customer_id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("state", StringType(), True),
        StructField("attr_hash", StringType(), True),
        StructField("effective_from", TimestampType(), True),
        StructField("effective_to", TimestampType(), True),
        StructField("is_current", BooleanType(), False),
        StructField("is_deleted", BooleanType(), False),
    ]
)
DIM_COLUMNS = [f.name for f in DIM_CUSTOMER_SCHEMA.fields]


def clean_orders(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (valid, quarantine). Quarantined rows keep every original column plus a reason."""
    typed = (
        bronze.withColumn("order_id", F.trim("order_id"))
        .withColumn("customer_id", F.trim("customer_id"))
        .withColumn("order_ts", F.to_timestamp("order_ts"))
        .withColumn("status", F.upper(F.trim("status")))
        .withColumn("amount_usd", F.col("amount_usd").cast("double"))
    )
    checks = [
        (F.col("order_id").isNull() | (F.col("order_id") == ""), "missing_order_id"),
        (F.col("customer_id").isNull() | (F.col("customer_id") == ""), "missing_customer_id"),
        (F.col("order_ts").isNull(), "invalid_order_ts"),
        (F.col("amount_usd").isNull() | (F.col("amount_usd") < 0), "invalid_amount"),
        (~F.col("status").isin(VALID_STATUSES) | F.col("status").isNull(), "invalid_status"),
    ]
    reasons = [F.when(cond, F.lit(name)) for cond, name in checks]
    typed = typed.withColumn("_dq_reasons", F.concat_ws(",", *reasons))

    quarantine = typed.filter(F.col("_dq_reasons") != "")
    valid = typed.filter(F.col("_dq_reasons") == "").drop("_dq_reasons")

    return latest_orders(valid).withColumn("order_date", F.to_date("order_ts")), quarantine


def latest_orders(orders: DataFrame) -> DataFrame:
    """Replayed files and late updates: keep the latest version of each order."""
    w = Window.partitionBy("order_id").orderBy(F.col("order_ts").desc(), F.col("_ingest_ts").desc())
    return orders.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def _attr_hash(*cols: str):
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c), F.lit("")) for c in cols]), 256)


def latest_customer_changes(changes: DataFrame) -> DataFrame:
    """Keep the newest change per customer in the batch (intra-batch history is collapsed)."""
    w = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc())
    return (
        changes.withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn("op", F.upper(F.trim("op")))
        .filter(F.col("customer_id").isNotNull() & F.col("updated_at").isNotNull())
        .withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .select("customer_id", "op", "name", "email", "state", "updated_at")
        .withColumn("attr_hash", _attr_hash("name", "email", "state"))
    )


def apply_scd2(dim: DataFrame | None, changes: DataFrame) -> DataFrame:
    """Merge a batch of CDC events into an SCD Type 2 dimension.

    - new customer (I/U)        -> insert current row
    - changed attributes        -> expire current row, insert new current row
    - delete (D)                -> expire current row and flag is_deleted
    - same attributes or stale  -> keep current row unchanged (makes replays idempotent)

    On Databricks the same logic is a single Delta Lake MERGE; it is written with
    DataFrame operations here so it runs anywhere Spark runs.
    """
    spark = changes.sparkSession
    if dim is None:
        dim = spark.createDataFrame([], DIM_CUSTOMER_SCHEMA)
    latest = latest_customer_changes(changes)
    history = dim.filter(~F.col("is_current"))
    current = dim.filter(F.col("is_current"))

    j = current.alias("c").join(latest.alias("n"), on="customer_id", how="full_outer")
    c_exists = F.col("c.customer_sk").isNotNull()
    n_exists = F.col("n.op").isNotNull()
    stale = c_exists & n_exists & (F.col("n.updated_at") <= F.col("c.effective_from"))
    changed = (
        c_exists
        & n_exists
        & ~stale
        & ((F.col("n.op") == "D") | (F.col("n.attr_hash") != F.col("c.attr_hash")))
    )
    j = (
        j.withColumn("_c", c_exists)
        .withColumn("_n", n_exists)
        .withColumn("_changed", F.coalesce(changed, F.lit(False)))
    )

    def from_current(df: DataFrame) -> DataFrame:
        return df.select(
            "c.customer_sk", "customer_id", "c.name", "c.email", "c.state", "c.attr_hash",
            "c.effective_from", "c.effective_to", "c.is_current", "c.is_deleted",
        )

    unchanged = from_current(j.filter(F.col("_c") & ~F.col("_changed")))

    expired = j.filter(F.col("_changed")).select(
        "c.customer_sk", "customer_id", "c.name", "c.email", "c.state", "c.attr_hash", "c.effective_from",
        F.col("n.updated_at").alias("effective_to"),
        F.lit(False).alias("is_current"),
        (F.col("n.op") == "D").alias("is_deleted"),
    )

    inserted = j.filter(F.col("_n") & (F.col("n.op") != "D") & (~F.col("_c") | F.col("_changed"))).select(
        F.xxhash64("customer_id", F.col("n.updated_at")).alias("customer_sk"),
        "customer_id", "n.name", "n.email", "n.state", "n.attr_hash",
        F.col("n.updated_at").alias("effective_from"),
        F.lit(None).cast(TimestampType()).alias("effective_to"),
        F.lit(True).alias("is_current"),
        F.lit(False).alias("is_deleted"),
    )

    parts = [p.select(*DIM_COLUMNS) for p in (history, unchanged, expired, inserted)]
    return reduce(DataFrame.unionByName, parts)
