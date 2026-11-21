"""
Module: ml_config.py
Mục đích: Single Source of Truth cho phân hệ Predictive Analytics (Trang 3 — Quân).

Nguyên tắc chọn feature: CHỈ dùng thông tin biết TẠI THỜI ĐIỂM TẠO ĐƠN (pre-shipment).
Các cột sau bị CẤM vì là Target Leakage (chỉ biết sau khi giao / suy ra trực tiếp nhãn):
  - days_shipping_real, delay_days, shipping_date_ts, ship_date_key  → biết sau khi giao
  - delivery_status ('Late delivery' ⇔ late_delivery_risk = 1)        → chính là nhãn
  - shipping_ops_key  (Junk Dim gộp cả delivery_status)               → mã hóa ngầm nhãn
  - order_status      (CANCELED/SUSPECTED_FRAUD ⇒ nhãn luôn = 0; trạng thái chụp sau sự kiện)
  - net_profit, profit_ratio, is_loss_order, is_canceled_or_fraud     → hậu kiểm tài chính
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
from etl.config import DATA_DIR, GOLD_DIR  # noqa: E402

# ---------------------------------------------------------------------------
# Đường dẫn
# ---------------------------------------------------------------------------
ML_DIR      = DATA_DIR / "ml"            # Bảng CSV/Parquet xuất cho Power BI
MODEL_DIR   = ML_DIR / "models"          # Mô hình đã huấn luyện (.joblib)
FIGURE_DIR  = ML_DIR / "figures"         # Hình PNG cho Báo cáo
for _d in (ML_DIR, MODEL_DIR, FIGURE_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Biến mục tiêu & chiến lược chia dữ liệu
# ---------------------------------------------------------------------------
TARGET      = "late_delivery_risk"
GROUP_COL   = "order_id"                 # Các dòng của cùng 1 đơn chung nhãn → phải chia theo nhóm
TEST_START  = "2017-07-01"               # Out-of-time test: đơn đặt từ ngày này trở đi
CV_FOLDS    = 5                          # StratifiedGroupKFold trên tập train
RANDOM_STATE = 42

# Loại đơn bị hủy khỏi train/eval: hủy ⇒ nhãn luôn = 0 bất kể mode → nhiễu, không phải "giao đúng hạn"
EXCLUDE_CANCELED_FROM_TRAINING = True

# ---------------------------------------------------------------------------
# Feature pre-shipment
# ---------------------------------------------------------------------------
CATEGORICAL_FEATURES = [
    "shipping_mode",        # Phương thức vận chuyển khách chọn
    "payment_type",         # DEBIT / TRANSFER / PAYMENT / CASH
    "market",               # 5 thị trường
    "order_region",         # 23 khu vực
    "customer_segment",     # Consumer / Corporate / Home Office
    "department_name",      # 11 ngành hàng
    "category_name",        # 51 danh mục
]
NUMERIC_FEATURES = [
    "order_hour",           # Giờ đặt hàng (0–23) — quan trọng với cut-off Same Day
    "order_dayofweek",      # 1=Thứ Hai … 7=Chủ Nhật
    "order_month",
    "is_weekend",
    "is_peak_season",
    "order_item_quantity",
    "product_price",
    "discount_rate",
    "net_sales",
    "items_in_order",       # Số dòng sản phẩm trong cùng đơn (biết lúc checkout)
]
FEATURES = CATEGORICAL_FEATURES + NUMERIC_FEATURES

# Cột bị rò rỉ — chỉ dùng cho thí nghiệm minh họa "Leakage Demo"
LEAKY_FEATURES = ["days_shipping_real", "days_shipment_scheduled"]

# ---------------------------------------------------------------------------
# Ngưỡng quyết định & phân tầng rủi ro (Risk Bands) cho Dashboard
# ---------------------------------------------------------------------------
DECISION_THRESHOLD = 0.5
RISK_BANDS = [            # (cận dưới, nhãn) — xét từ cao xuống thấp
    (0.85, "4-Critical"),
    (0.60, "3-High"),
    (0.30, "2-Medium"),
    (0.00, "1-Low"),
]

# SLA cam kết hiện tại theo phương thức vận chuyển
CURRENT_SLA = {"Same Day": 0, "First Class": 1, "Second Class": 2, "Standard Class": 4}

# ---------------------------------------------------------------------------
# Siêu tham số mô hình (đã chọn bằng thử nghiệm CV; đủ nhanh để chạy < 5 phút)
# ---------------------------------------------------------------------------
MODEL_PARAMS = {
    "LogisticRegression": dict(C=1.0, max_iter=2000),
    "DecisionTree":       dict(max_depth=6, min_samples_leaf=50),
    "RandomForest":       dict(n_estimators=200, max_depth=14, min_samples_leaf=20, n_jobs=-1),
    "XGBoost":            dict(n_estimators=300, learning_rate=0.05, max_depth=6, subsample=0.8,
                               colsample_bytree=0.8, eval_metric="logloss", n_jobs=-1),
    "LightGBM":           dict(n_estimators=300, learning_rate=0.05, num_leaves=31, subsample=0.8,
                               subsample_freq=1, colsample_bytree=0.8, n_jobs=-1, verbose=-1),
}

# Lưới What-If Simulator (Feature 6)
WHATIF_GRID = {
    "shipping_mode": ["Standard Class", "Second Class", "First Class", "Same Day"],
    "order_hour": list(range(24)),
    "market": ["Africa", "Europe", "LATAM", "Pacific Asia", "USCA"],
    "discount_rate": [0.0, 0.05, 0.10, 0.15, 0.20, 0.25],
    "order_item_quantity": [1, 2, 3, 4, 5],
}
