from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(app_name: str = "medallion-lakehouse", master: str = "local[2]") -> SparkSession:
    """Local session for development and tests. On Databricks the cluster session is used instead."""
    return (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .getOrCreate()
    )
