import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from lakehouse.job import run_batch  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from generate_data import generate  # noqa: E402


def test_two_batch_end_to_end(spark, tmp_path):
    landing, lake = tmp_path / "landing", tmp_path / "lake"
    generate(landing, n_customers=20, n_orders=60)
    s1 = run_batch(spark, str(landing), str(lake), "2026-01-01")
    s2 = run_batch(spark, str(landing), str(lake), "2026-01-02")

    assert s1["orders_quarantined"] == 0 and s1["dim_customer_current"] == 20
    assert s2["orders_quarantined"] == 3
    # +1 new customer, -1 deleted, 1 changed => same current count, two extra history rows
    assert s2["dim_customer_current"] == 20
    assert s2["dim_customer_rows"] == 22
    # 120 unique orders: replayed and corrected orders are deduplicated across batches
    assert s2["fct_orders_rows"] == 120

    fct = spark.read.parquet(str(lake / "gold" / "fct_orders"))
    assert fct.select("order_id").distinct().count() == fct.count()
    rev = spark.read.parquet(str(lake / "gold" / "daily_revenue"))
    total = fct.filter("status != 'CANCELLED'").groupBy().sum("amount_usd").first()[0]
    assert abs(rev.groupBy().sum("revenue_usd").first()[0] - total) < 0.05
