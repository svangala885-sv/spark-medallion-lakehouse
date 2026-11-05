"""Bronze: land data as received, typed as strings, with ingestion metadata.

Nothing is dropped or corrected here so any load can be replayed and audited.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

ORDERS_RAW_SCHEMA = StructType(
    [StructField(c, StringType(), True) for c in ["order_id", "customer_id", "order_ts", "status", "amount_usd"]]
)

CUSTOMER_CDC_SCHEMA = StructType(
    [StructField(c, StringType(), True) for c in ["op", "customer_id", "name", "email", "state", "updated_at"]]
)


def _with_metadata(df: DataFrame, batch_id: str) -> DataFrame:
    return (
        df.withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )


def ingest_orders(spark: SparkSession, path: str, batch_id: str) -> DataFrame:
    raw = spark.read.option("header", True).schema(ORDERS_RAW_SCHEMA).csv(path)
    return _with_metadata(raw, batch_id)


def ingest_customer_changes(spark: SparkSession, path: str, batch_id: str) -> DataFrame:
    raw = spark.read.schema(CUSTOMER_CDC_SCHEMA).json(path)
    return _with_metadata(raw, batch_id)
