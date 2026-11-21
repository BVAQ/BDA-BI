"""
Module: predict.py
Mục đích: Chấm điểm rủi ro trễ hạn cho 1 đơn hàng giả định bằng mô hình tốt nhất (demo khi thuyết trình).

Ví dụ:
  python -m ml.predict --mode "Same Day" --hour 14 --market Europe
  python -m ml.predict --mode "Standard Class" --hour 9 --market LATAM --discount 0.1 --qty 3 --compare
"""

import sys
import argparse
from pathlib import Path

import joblib
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))
from ml.ml_config import MODEL_DIR, FEATURES, CURRENT_SLA  # noqa: E402
from ml.export import risk_band  # noqa: E402

DEFAULTS = {  # giá trị điển hình (mode/median) của tập train
    "payment_type": "DEBIT", "order_region": "Western Europe", "customer_segment": "Consumer",
    "department_name": "Fan Shop", "category_name": "Cleats", "order_dayofweek": 3, "order_month": 6,
    "is_peak_season": 0, "product_price": 59.99, "items_in_order": 3,
}


def score(mode, hour, market, region=None, discount=0.0, qty=1, price=59.99, **extra) -> float:
    model = joblib.load(MODEL_DIR / "best_model.joblib")
    row = dict(DEFAULTS)
    row.update(extra)
    row.update({"shipping_mode": mode, "order_hour": hour, "market": market,
                "discount_rate": discount, "order_item_quantity": qty, "product_price": price,
                "net_sales": price * qty * (1 - discount)})
    if region:
        row["order_region"] = region
    row["is_weekend"] = int(row["order_dayofweek"] >= 6)
    return float(model.predict_proba(pd.DataFrame([row])[FEATURES])[:, 1][0])


def main():
    ap = argparse.ArgumentParser(description="Dự báo xác suất trễ hạn cho 1 đơn hàng")
    ap.add_argument("--mode", required=True, choices=list(CURRENT_SLA))
    ap.add_argument("--hour", type=int, default=10)
    ap.add_argument("--market", default="Europe")
    ap.add_argument("--region", default=None)
    ap.add_argument("--discount", type=float, default=0.0)
    ap.add_argument("--qty", type=int, default=1)
    ap.add_argument("--price", type=float, default=59.99)
    ap.add_argument("--compare", action="store_true", help="So sánh cả 4 shipping mode")
    a = ap.parse_args()

    modes = list(CURRENT_SLA) if a.compare else [a.mode]
    for m in modes:
        p = score(m, a.hour, a.market, a.region, a.discount, a.qty, a.price)
        band = risk_band(pd.Series([p])).iloc[0]
        print(f"{m:<15} SLA {CURRENT_SLA[m]} ngày | P(trễ) = {p:6.1%} | {band}")


if __name__ == "__main__":
    main()
