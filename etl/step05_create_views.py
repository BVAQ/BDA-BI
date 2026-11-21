"""
Module: step05_create_views.py
Mục đích: BƯỚC 5 - Khởi tạo 4 Semantic Views (Data Marts) trên Google BigQuery.
Trách nhiệm duy nhất (Single Responsibility):
  Định nghĩa và deploy 4 Views nghiệp vụ chuyên biệt lên BigQuery:
     - vw_exec_overview         (Cho Quốc Khánh - Dashboard 1)
     - vw_olap_diagnostic       (Cho Vinh Khánh - Dashboard 2)
     - vw_ml_pre_shipment       (Cho Quân - Dashboard 3: Chống Target Leakage)
     - vw_strategy_optimization (Cho Vương - Dashboard 4)
  Nếu chưa có GCP credentials, in ra DDL SQL để chạy thủ công trên BigQuery Console.
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
from etl.config import GCP_PROJECT_ID, BQ_DATASET_ID

P = f"`{GCP_PROJECT_ID}.{BQ_DATASET_ID}`"  # Prefix chuẩn BigQuery

# ---------------------------------------------------------------------------
# VIEW 1: EXECUTIVE KPI OVERVIEW (QUỐC KHÁNH - TRANG 1)
# ---------------------------------------------------------------------------
DDL_VW_EXEC_OVERVIEW = f"""
CREATE OR REPLACE VIEW {P}.vw_exec_overview AS
SELECT
    f.order_item_id,
    f.order_id,
    d.year          AS order_year,
    d.quarter       AS order_quarter,
    d.month         AS order_month,
    d.month_name    AS order_month_name,
    d.is_peak_season,
    g.market,
    g.order_region,
    g.order_country,
    s.shipping_mode,
    s.payment_type,
    f.gross_sales,
    f.discount_amount,
    f.net_sales,
    f.net_profit,
    f.late_delivery_risk,
    f.delay_days,
    f.is_loss_order
FROM {P}.fact_order_shipments f
LEFT JOIN {P}.dim_date        d ON f.order_date_key     = d.date_key
LEFT JOIN {P}.dim_geography   g ON f.destination_geo_key = g.destination_geo_key
LEFT JOIN {P}.dim_shipping_ops s ON f.shipping_ops_key   = s.shipping_ops_key;
"""

# ---------------------------------------------------------------------------
# VIEW 2: MULTI-DIMENSIONAL OLAP & SLA DIAGNOSTIC (VINH KHÁNH - TRANG 2)
# ---------------------------------------------------------------------------
DDL_VW_OLAP_DIAGNOSTIC = f"""
CREATE OR REPLACE VIEW {P}.vw_olap_diagnostic AS
SELECT
    f.order_item_id,
    f.order_id,
    d.year          AS order_year,
    d.quarter       AS order_quarter,
    d.month         AS order_month,
    g.market,
    g.order_region,
    g.order_country,
    p.department_name,
    p.category_name,
    p.product_name,
    s.shipping_mode,
    s.default_sla_days,
    s.payment_type,
    s.order_status,
    s.delivery_status,
    f.days_shipping_real,
    f.days_shipment_scheduled,
    f.delay_days,
    f.late_delivery_risk,
    f.net_sales,
    f.net_profit,
    f.order_item_quantity
FROM {P}.fact_order_shipments f
LEFT JOIN {P}.dim_date        d ON f.order_date_key      = d.date_key
LEFT JOIN {P}.dim_geography   g ON f.destination_geo_key  = g.destination_geo_key
LEFT JOIN {P}.dim_product     p ON f.product_key          = p.product_key
LEFT JOIN {P}.dim_shipping_ops s ON f.shipping_ops_key    = s.shipping_ops_key;
"""

# ---------------------------------------------------------------------------
# VIEW 3: PRE-SHIPMENT ML FEATURES (QUÂN - TRANG 3)
# Chú ý: Đã loại bỏ HOÀN TOÀN các biến hậu kiểm để chống Target Leakage:
#   - days_shipping_real, delay_days (chỉ biết SAU KHI giao hàng)
#   - delivery_status (chỉ biết SAU KHI giao hàng)
#   - shipping_date_ts (chỉ biết SAU KHI xuất kho)
# ---------------------------------------------------------------------------
DDL_VW_ML_PRE_SHIPMENT = f"""
CREATE OR REPLACE VIEW {P}.vw_ml_pre_shipment AS
SELECT
    f.order_item_id,
    -- Target variable
    f.late_delivery_risk,
    -- Pre-shipment features (known at order creation time only)
    s.shipping_mode,
    s.default_sla_days,
    s.payment_type,
    f.days_shipment_scheduled,
    g.market,
    g.order_region,
    g.order_country,
    p.department_name,
    p.category_name,
    f.order_item_quantity,
    f.product_price,
    f.discount_rate,
    d.month         AS order_month,
    d.day_of_week   AS order_day_of_week,
    d.is_weekend,
    d.is_peak_season
FROM {P}.fact_order_shipments f
LEFT JOIN {P}.dim_date        d ON f.order_date_key      = d.date_key
LEFT JOIN {P}.dim_geography   g ON f.destination_geo_key  = g.destination_geo_key
LEFT JOIN {P}.dim_product     p ON f.product_key          = p.product_key
LEFT JOIN {P}.dim_shipping_ops s ON f.shipping_ops_key    = s.shipping_ops_key;
"""

# ---------------------------------------------------------------------------
# VIEW 4: STRATEGY & PROFIT OPTIMIZATION (VƯƠNG - TRANG 4)
# ---------------------------------------------------------------------------
DDL_VW_STRATEGY_OPTIMIZATION = f"""
CREATE OR REPLACE VIEW {P}.vw_strategy_optimization AS
SELECT
    f.order_item_id,
    f.order_id,
    g.market,
    g.order_region,
    g.order_country,
    p.department_name,
    p.category_name,
    s.shipping_mode,
    s.order_status,
    s.delivery_status,
    f.gross_sales,
    f.discount_rate,
    f.net_sales,
    f.net_profit,
    f.profit_ratio,
    f.is_loss_order,
    f.is_canceled_or_fraud,
    f.delay_days,
    f.late_delivery_risk
FROM {P}.fact_order_shipments f
LEFT JOIN {P}.dim_geography   g ON f.destination_geo_key = g.destination_geo_key
LEFT JOIN {P}.dim_product     p ON f.product_key          = p.product_key
LEFT JOIN {P}.dim_shipping_ops s ON f.shipping_ops_key    = s.shipping_ops_key;
"""

VIEWS = {
    "vw_exec_overview         (Trang 1 - Quốc Khánh)": DDL_VW_EXEC_OVERVIEW,
    "vw_olap_diagnostic       (Trang 2 - Vinh Khánh)": DDL_VW_OLAP_DIAGNOSTIC,
    "vw_ml_pre_shipment       (Trang 3 - Quân)":       DDL_VW_ML_PRE_SHIPMENT,
    "vw_strategy_optimization (Trang 4 - Vương)":      DDL_VW_STRATEGY_OPTIMIZATION,
}


def run_create_views():
    print("=" * 80)
    print(">>> BƯỚC 5: KHỞI TẠO 4 SEMANTIC VIEWS TRÊN GOOGLE BIGQUERY")
    print("=" * 80)

    try:
        from google.cloud import bigquery
        client = bigquery.Client(project=GCP_PROJECT_ID)

        print(f"[+] Đã kết nối BigQuery: Project={GCP_PROJECT_ID} | Dataset={BQ_DATASET_ID}\n")
        for view_label, ddl in VIEWS.items():
            job = client.query(ddl)
            job.result()
            print(f"    [+] View sẵn sàng: {view_label}")

        print(f"\n[✓] HOÀN TẤT BƯỚC 5: 4 Semantic Views đã deploy lên BigQuery!")

    except Exception as e:
        print(f"[!] Chưa có GCP credentials cục bộ ({e}).")
        print("[i] Dưới đây là DDL SQL để bạn chạy thủ công trên BigQuery Console:\n")
        for view_label, ddl in VIEWS.items():
            print(f"-- {view_label}")
            print(ddl)
            print("-" * 60)


if __name__ == "__main__":
    run_create_views()
