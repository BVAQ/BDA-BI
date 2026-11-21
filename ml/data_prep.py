"""
Module: data_prep.py
Mục đích: Dựng bảng feature pre-shipment từ Gold Star Schema (tương đương view `vw_ml_pre_shipment`
trên BigQuery) và chia train/test theo thời gian (out-of-time validation).
"""

import pandas as pd

from ml.ml_config import (
    GOLD_DIR, TARGET, TEST_START, EXCLUDE_CANCELED_FROM_TRAINING,
)


def load_modeling_frame() -> pd.DataFrame:
    """Join Fact × 4 Dimensions → 1 dòng / order item, kèm feature pre-shipment và cột phụ trợ."""
    fact = pd.read_parquet(GOLD_DIR / "fact_order_shipments.parquet")
    ops  = pd.read_parquet(GOLD_DIR / "dim_shipping_ops.parquet")
    geo  = pd.read_parquet(GOLD_DIR / "dim_geography.parquet")
    prod = pd.read_parquet(GOLD_DIR / "dim_product.parquet")
    cust = pd.read_parquet(GOLD_DIR / "dim_customer.parquet")

    df = (
        fact
        .merge(ops[["shipping_ops_key", "shipping_mode", "payment_type", "order_status", "delivery_status"]],
               on="shipping_ops_key", how="left")
        .merge(geo[["destination_geo_key", "market", "order_region", "order_country"]],
               on="destination_geo_key", how="left")
        .merge(prod[["product_key", "department_name", "category_name"]], on="product_key", how="left")
        .merge(cust[["customer_key", "customer_segment"]], on="customer_key", how="left")
    )
    assert len(df) == len(fact), "Join làm thay đổi số dòng — kiểm tra lại khóa Dimension!"

    ts = df["order_date_ts"]
    df["order_date"]      = ts.dt.normalize()
    df["order_hour"]      = ts.dt.hour
    df["order_dayofweek"] = ts.dt.dayofweek + 1
    df["order_month"]     = ts.dt.month
    df["is_weekend"]      = (df["order_dayofweek"] >= 6).astype(int)
    df["is_peak_season"]  = df["order_month"].isin([10, 11, 12]).astype(int)
    df["items_in_order"]  = df.groupby("order_id")["order_item_id"].transform("count")

    # Cờ đơn bị hủy giao (không phải feature — dùng để lọc tập huấn luyện)
    df["is_canceled_shipment"] = (df["delivery_status"] == "Shipping canceled").astype(int)
    df["split"] = (ts >= pd.Timestamp(TEST_START)).map({True: "test", False: "train"})
    df[TARGET] = df[TARGET].astype(int)
    return df


def train_test_frames(df: pd.DataFrame):
    """Trả về (train_df, test_df) — loại đơn hủy nếu cấu hình yêu cầu."""
    pool = df[df["is_canceled_shipment"] == 0] if EXCLUDE_CANCELED_FROM_TRAINING else df
    train = pool[pool["split"] == "train"].reset_index(drop=True)
    test  = pool[pool["split"] == "test"].reset_index(drop=True)
    return train, test


def oracle_probability(df: pd.DataFrame) -> pd.Series:
    """
    Xác suất Bayes-optimal (trần lý thuyết) suy ra từ cơ chế sinh dữ liệu đã khảo sát:
      - First Class : luôn giao sau 2 ngày > SLA 1 ngày → P(late) = 1
      - Same Day    : đặt từ 12:00 trở đi thì giao ngày hôm sau → P(late) = 1[hour ≥ 12]
      - Second Class: real ~ Uniform{2..6}, SLA 2 → P(real > 2) = 4/5 = 0.8
      - Standard    : real ~ Uniform{2..6}, SLA 4 → P(real > 4) = 2/5 = 0.4
    Không có mô hình trung thực nào vượt được mức này; vượt ⇒ nghi Target Leakage.
    """
    mode = df["shipping_mode"]
    p = pd.Series(0.0, index=df.index)
    p[mode == "First Class"] = 1.0
    p[(mode == "Same Day") & (df["order_hour"] >= 12)] = 1.0
    p[mode == "Second Class"] = 0.8
    p[mode == "Standard Class"] = 0.4
    return p
