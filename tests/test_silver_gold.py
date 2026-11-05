from datetime import datetime

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402

from lakehouse import gold, silver  # noqa: E402


def ts(s: str) -> datetime:
    return datetime.fromisoformat(s)


def bronze_orders(spark, rows):
    cols = ["order_id", "customer_id", "order_ts", "status", "amount_usd"]
    df = spark.createDataFrame(rows, cols)
    return df.withColumn("_batch_id", F.lit("b")).withColumn("_ingest_ts", F.current_timestamp())


def changes(spark, rows):
    return spark.createDataFrame(rows, ["op", "customer_id", "name", "email", "state", "updated_at"])


def test_clean_orders_quarantines_and_dedupes(spark):
    df = bronze_orders(spark, [
        ("O1", "C1", "2026-01-01T10:00:00", "placed", "10.5"),
        ("O1", "C1", "2026-01-02T10:00:00", "SHIPPED", "10.5"),   # newer version wins
        ("O2", "", "2026-01-01T10:00:00", "PLACED", "5"),
        ("O3", "C2", "bad-date", "PLACED", "5"),
        ("O4", "C2", "2026-01-01T10:00:00", "LOST", "-1"),
    ])
    valid, quarantine = silver.clean_orders(df)
    rows = {r.order_id: r for r in valid.collect()}
    assert set(rows) == {"O1"} and rows["O1"].status == "SHIPPED"
    reasons = {r.order_id: r._dq_reasons for r in quarantine.collect()}
    assert reasons["O2"] == "missing_customer_id"
    assert reasons["O3"] == "invalid_order_ts"
    assert reasons["O4"] == "invalid_amount,invalid_status"


def test_scd2_lifecycle(spark):
    day1 = changes(spark, [
        ("I", "C1", "Ana", "a@example.com", "CA", "2026-01-01T00:00:00"),
        ("I", "C2", "Ben", "b@example.com", "TX", "2026-01-01T00:00:00"),
        ("I", "C3", "Cy", "c@example.com", "NY", "2026-01-01T00:00:00"),
        ("I", "C4", "Di", "d@example.com", "WA", "2026-01-01T00:00:00"),
    ])
    dim1 = silver.apply_scd2(None, day1).localCheckpoint()
    assert dim1.filter("is_current").count() == 4

    day2 = changes(spark, [
        ("U", "C1", "Ana", "a@example.com", "NV", "2026-01-02T00:00:00"),  # changed
        ("D", "C2", "Ben", "b@example.com", "TX", "2026-01-02T00:00:00"),  # deleted
        ("U", "C3", "Cy", "c@example.com", "NY", "2026-01-02T00:00:00"),   # same attributes
        ("U", "C4", "Di", "d@example.com", "OR", "2025-12-31T00:00:00"),   # stale event
        ("I", "C5", "Ed", "e@example.com", "IL", "2026-01-02T00:00:00"),   # new
    ])
    dim2 = silver.apply_scd2(dim1, day2).localCheckpoint()
    rows = dim2.collect()
    by_id = {}
    for r in rows:
        by_id.setdefault(r.customer_id, []).append(r)

    c1 = sorted(by_id["C1"], key=lambda r: r.effective_from)
    assert [r.state for r in c1] == ["CA", "NV"]
    assert c1[0].effective_to == ts("2026-01-02T00:00:00") and not c1[0].is_current and c1[1].is_current
    assert c1[0].customer_sk != c1[1].customer_sk

    assert len(by_id["C2"]) == 1 and by_id["C2"][0].is_deleted and not by_id["C2"][0].is_current
    assert len(by_id["C3"]) == 1 and by_id["C3"][0].is_current
    assert len(by_id["C4"]) == 1 and by_id["C4"][0].state == "WA"
    assert len(by_id["C5"]) == 1 and by_id["C5"][0].is_current

    # replaying the same batch must not change the dimension
    dim3 = silver.apply_scd2(dim2, day2)
    assert dim3.count() == dim2.count()


def test_point_in_time_fact_and_revenue(spark):
    dim = silver.apply_scd2(None, changes(spark, [
        ("I", "C1", "Ana", "a@example.com", "CA", "2026-01-01T00:00:00"),
    ])).localCheckpoint()
    dim = silver.apply_scd2(dim, changes(spark, [
        ("U", "C1", "Ana", "a@example.com", "NV", "2026-01-02T00:00:00"),
    ])).localCheckpoint()
    orders, _ = silver.clean_orders(bronze_orders(spark, [
        ("O1", "C1", "2026-01-01T12:00:00", "PLACED", "10"),
        ("O2", "C1", "2026-01-02T12:00:00", "PLACED", "20"),
        ("O3", "C9", "2026-01-02T12:00:00", "PLACED", "5"),       # unknown customer
        ("O4", "C1", "2026-01-02T13:00:00", "CANCELLED", "99"),
    ]))
    fct = gold.build_fct_orders(orders, dim)
    assert fct.count() == 4
    sks = {r.order_id: r.customer_sk for r in fct.collect()}
    assert sks["O1"] != sks["O2"] and sks["O3"] == gold.UNKNOWN_SK

    rev = {(str(r.order_date), r.state): r.revenue_usd for r in gold.build_daily_revenue(fct, dim).collect()}
    assert rev == {("2026-01-01", "CA"): 10.0, ("2026-01-02", "NV"): 20.0, ("2026-01-02", "UNKNOWN"): 5.0}
