"""Generate two synthetic landing batches (orders CSV + customer CDC JSON).

Every value is random and fictional. Batch 2 contains customer updates, a delete,
a replayed order, a corrected order and a few invalid rows.
"""

from __future__ import annotations

import csv
import json
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

STATES = ["CA", "TX", "NY", "WA", "IL", "CO"]
STATUSES = ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED"]


def generate(root: Path, n_customers: int = 50, n_orders: int = 300, seed: int = 7) -> None:
    rng = random.Random(seed)
    t0 = datetime(2026, 1, 1)
    b1, b2 = "2026-01-01", "2026-01-02"

    customers = [
        {"op": "I", "customer_id": f"C{i:04d}", "name": f"Customer {i}", "email": f"c{i}@example.com",
         "state": rng.choice(STATES), "updated_at": (t0 + timedelta(minutes=i)).isoformat()}
        for i in range(1, n_customers + 1)
    ]
    changes2 = [
        {**customers[0], "op": "U", "state": "NV", "updated_at": (t0 + timedelta(days=1, hours=1)).isoformat()},
        {**customers[1], "op": "D", "updated_at": (t0 + timedelta(days=1, hours=2)).isoformat()},
        {**customers[2], "op": "U", "updated_at": (t0 + timedelta(days=1, hours=3)).isoformat()},  # no real change
        {"op": "I", "customer_id": "C9999", "name": "Customer 9999", "email": "c9999@example.com",
         "state": "OR", "updated_at": (t0 + timedelta(days=1, hours=4)).isoformat()},
    ]

    def order(i: int, day: int) -> dict:
        return {"order_id": f"O{i:06d}", "customer_id": f"C{rng.randint(1, n_customers):04d}",
                "order_ts": (t0 + timedelta(days=day, minutes=rng.randint(60, 1380))).isoformat(),
                "status": rng.choice(STATUSES), "amount_usd": f"{rng.uniform(5, 500):.2f}"}

    orders1 = [order(i, 0) for i in range(1, n_orders + 1)]
    orders2 = [order(i, 1) for i in range(n_orders + 1, 2 * n_orders + 1)]
    orders2 += [
        orders1[0],                                                    # replayed file row
        {**orders1[1], "status": "DELIVERED",
         "order_ts": (t0 + timedelta(days=1, hours=5)).isoformat()},  # later version of an order
        {"order_id": "O900001", "customer_id": "", "order_ts": t0.isoformat(),
         "status": "PLACED", "amount_usd": "10"},
        {"order_id": "O900002", "customer_id": "C0005", "order_ts": "not-a-date",
         "status": "PLACED", "amount_usd": "10"},
        {"order_id": "O900003", "customer_id": "C0005", "order_ts": t0.isoformat(),
         "status": "LOST", "amount_usd": "-3"},
    ]

    for batch, rows in ((b1, orders1), (b2, orders2)):
        d = root / "orders" / batch
        d.mkdir(parents=True, exist_ok=True)
        with open(d / "orders.csv", "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(rows[0]))
            w.writeheader()
            w.writerows(rows)
    for batch, rows in ((b1, customers), (b2, changes2)):
        d = root / "customers" / batch
        d.mkdir(parents=True, exist_ok=True)
        with open(d / "changes.json", "w") as fh:
            fh.write("\n".join(json.dumps(r) for r in rows) + "\n")


if __name__ == "__main__":
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parents[1] / "data" / "landing"
    generate(out)
    print(f"wrote synthetic landing data to {out}")
