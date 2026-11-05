"""Run one batch through Bronze -> Silver -> Gold.

    python -m lakehouse.job --landing data/landing --lakehouse build/lakehouse --batch-id 2026-01-01

Tables are written as Parquet so the project runs on plain Spark. Set
--format delta on Databricks (or with delta-spark installed) to write Delta tables.
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from . import bronze, gold, silver
from .spark import get_spark

log = logging.getLogger("lakehouse.job")


class QuarantineThresholdExceeded(RuntimeError):
    pass


def _exists(spark: SparkSession, path: str) -> bool:
    jvm = spark.sparkContext._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = hadoop_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    return fs.exists(hadoop_path)


def write(df: DataFrame, path: str, fmt: str, mode: str = "overwrite", partition_by: list[str] | None = None) -> None:
    writer = df.write.format(fmt).mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


def run_batch(spark: SparkSession, landing: str, lakehouse: str, batch_id: str, fmt: str = "parquet",
              max_quarantine_rate: float = 0.05) -> dict:
    lh = lakehouse.rstrip("/")

    # Bronze: append raw batches
    orders_raw = bronze.ingest_orders(spark, f"{landing}/orders/{batch_id}", batch_id)
    customers_raw = bronze.ingest_customer_changes(spark, f"{landing}/customers/{batch_id}", batch_id)
    write(orders_raw, f"{lh}/bronze/orders", fmt, mode="append", partition_by=["_batch_id"])
    write(customers_raw, f"{lh}/bronze/customer_changes", fmt, mode="append", partition_by=["_batch_id"])

    # Silver: validate orders, quarantine failures, stop if the failure rate is abnormal
    batch_orders = spark.read.format(fmt).load(f"{lh}/bronze/orders").filter(f"_batch_id = '{batch_id}'")
    valid, quarantine = silver.clean_orders(batch_orders)
    total, bad = batch_orders.count(), quarantine.count()
    rate = bad / total if total else 0.0
    write(quarantine, f"{lh}/silver/orders_quarantine", fmt, mode="append")
    if rate > max_quarantine_rate:
        raise QuarantineThresholdExceeded(f"{bad}/{total} orders quarantined ({rate:.1%}) > {max_quarantine_rate:.0%}")
    write(valid, f"{lh}/silver/orders/batch={batch_id}", fmt)

    # Silver: SCD Type 2 customer dimension
    dim_path = f"{lh}/silver/dim_customer"
    current_dim = spark.read.format(fmt).load(dim_path) if _exists(spark, dim_path) else None
    batch_changes = spark.read.format(fmt).load(f"{lh}/bronze/customer_changes").filter(f"_batch_id = '{batch_id}'")
    new_dim = silver.apply_scd2(current_dim, batch_changes).localCheckpoint()  # materialise before overwrite
    write(new_dim, dim_path, fmt)

    # Gold: rebuild from all Silver order batches
    all_orders = silver.latest_orders(spark.read.format(fmt).load(f"{lh}/silver/orders"))
    dim = spark.read.format(fmt).load(dim_path)
    fct = gold.build_fct_orders(all_orders, dim)
    write(fct, f"{lh}/gold/fct_orders", fmt, partition_by=["order_date"])
    write(gold.build_daily_revenue(fct, dim), f"{lh}/gold/daily_revenue", fmt)

    stats = {
        "batch_id": batch_id,
        "orders_received": total,
        "orders_quarantined": bad,
        "orders_valid_deduped": valid.count(),
        "dim_customer_rows": dim.count(),
        "dim_customer_current": dim.filter("is_current").count(),
        "fct_orders_rows": fct.count(),
    }
    log.info("batch complete %s", json.dumps(stats))
    return stats


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--landing", default="data/landing")
    p.add_argument("--lakehouse", default="build/lakehouse")
    p.add_argument("--batch-id", action="append", help="repeatable; default runs every batch in landing/orders")
    p.add_argument("--format", default="parquet", choices=["parquet", "delta"])
    p.add_argument("--max-quarantine-rate", type=float, default=0.05)
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    batches = args.batch_id or sorted(d.name for d in Path(args.landing, "orders").iterdir() if d.is_dir())
    spark = get_spark()
    try:
        for b in batches:
            print(json.dumps(run_batch(spark, args.landing, args.lakehouse, b, args.format, args.max_quarantine_rate)))
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
