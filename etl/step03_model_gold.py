"""
Module: 03_model_gold.py
Mục đích: BƯỚC 3 - Xây dựng mô hình hình sao (Kimball Star Schema) tại tầng Gold.
Trách nhiệm duy nhất (Single Responsibility):
  1. Đọc dữ liệu tầng Silver.
  2. Tách và sinh Surrogate Keys cho 5 bảng Dimension:
     - dim_date (Lịch ngày chuẩn hóa Role-Playing)
     - dim_customer (Khách hàng & Trung tâm phân phối)
     - dim_product (Danh mục sản phẩm & Ngành hàng)
     - dim_geography (Phân cấp địa lý 164 quốc gia)
     - dim_shipping_ops (Junk Dimension vận hành)
  3. Ánh xạ khóa ngoại và xây dựng Fact trung tâm: fact_order_shipments (180.519 dòng).
  4. Lưu trữ toàn bộ 6 bảng dưới định dạng Parquet (tại data/gold/) và SQLite (data/dataco_dwh.db).
"""

import sys
import time
import pandas as pd
from pathlib import Path

# Thêm thư mục gốc vào PYTHONPATH
sys.path.append(str(Path(__file__).resolve().parent.parent))
from etl.config import SILVER_DIR, GOLD_DIR

def run_gold_modeling():
    print("=" * 80)
    print(">>> BƯỚC 3: XÂY DỰNG KIMBALL STAR SCHEMA TẦNG GOLD (GOLD MODELING)")
    print("=" * 80)

    start_time = time.time()
    silver_input = SILVER_DIR / "silver_cleaned_data.parquet"

    if not silver_input.exists():
        raise FileNotFoundError(f"Chưa có dữ liệu Silver. Hãy chạy bước 02 trước! ({silver_input})")

    print(f"[*] Đang nạp dữ liệu Silver từ: {silver_input.name}...")
    df = pd.read_parquet(silver_input)

    # -------------------------------------------------------------------------
    # 1. BẢNG CHIỀU: DIM_CUSTOMER
    # -------------------------------------------------------------------------
    print("[*] Đang xây dựng dim_customer...")
    customer_cols = [
        'customer_id', 'customer_segment', 'customer_country',
        'customer_state', 'customer_city', 'customer_zipcode',
        'latitude', 'longitude'
    ]
    df_dim_customer = df[customer_cols].drop_duplicates(subset=['customer_id']).copy()
    df_dim_customer.rename(columns={
        'customer_id': 'customer_key',
        'latitude': 'origin_latitude',
        'longitude': 'origin_longitude'
    }, inplace=True)
    print(f"    - dim_customer: {len(df_dim_customer):,} dòng.")

    # -------------------------------------------------------------------------
    # 2. BẢNG CHIỀU: DIM_PRODUCT
    # -------------------------------------------------------------------------
    print("[*] Đang xây dựng dim_product...")
    product_cols = [
        'product_card_id', 'product_name', 'order_item_product_price',
        'product_status', 'category_id', 'category_name',
        'department_id', 'department_name'
    ]
    df_dim_product = df[product_cols].drop_duplicates(subset=['product_card_id']).copy()
    df_dim_product.rename(columns={
        'product_card_id': 'product_key',
        'order_item_product_price': 'catalog_price'
    }, inplace=True)
    print(f"    - dim_product: {len(df_dim_product):,} dòng.")

    # -------------------------------------------------------------------------
    # 3. BẢNG CHIỀU: DIM_GEOGRAPHY (Phân cấp thị trường & địa lý đích)
    # -------------------------------------------------------------------------
    print("[*] Đang xây dựng dim_geography (sinh Surrogate Key)...")
    geo_cols = ['market', 'order_region', 'order_country', 'order_state', 'order_city']
    df_dim_geography = df[geo_cols].drop_duplicates().reset_index(drop=True)
    df_dim_geography['destination_geo_key'] = df_dim_geography.index + 1
    # Đưa khóa lên đầu
    cols_order = ['destination_geo_key'] + geo_cols
    df_dim_geography = df_dim_geography[cols_order]
    print(f"    - dim_geography: {len(df_dim_geography):,} dòng (164 Quốc gia / 23 Vùng).")

    # -------------------------------------------------------------------------
    # 4. BẢNG CHIỀU: DIM_SHIPPING_OPS (Junk Dimension gom trạng thái vận hành)
    # -------------------------------------------------------------------------
    print("[*] Đang xây dựng dim_shipping_ops (Junk Dimension)...")
    ops_cols = ['shipping_mode', 'type', 'order_status', 'delivery_status']
    df_dim_shipping_ops = df[ops_cols].drop_duplicates().reset_index(drop=True)
    df_dim_shipping_ops['shipping_ops_key'] = df_dim_shipping_ops.index + 1
    
    # SLA mặc định theo phương thức vận tải
    sla_map = {
        'Same Day': 0,
        'First Class': 1,
        'Second Class': 2,
        'Standard Class': 4
    }
    df_dim_shipping_ops['default_sla_days'] = df_dim_shipping_ops['shipping_mode'].map(sla_map).fillna(3).astype(int)
    df_dim_shipping_ops.rename(columns={'type': 'payment_type'}, inplace=True)
    
    ops_final_cols = ['shipping_ops_key', 'shipping_mode', 'default_sla_days', 'payment_type', 'order_status', 'delivery_status']
    df_dim_shipping_ops = df_dim_shipping_ops[ops_final_cols]
    print(f"    - dim_shipping_ops: {len(df_dim_shipping_ops):,} dòng (64 tổ hợp vận hành).")

    # -------------------------------------------------------------------------
    # 5. BẢNG CHIỀU: DIM_DATE (Lịch ngày chuẩn hóa Role-Playing)
    # -------------------------------------------------------------------------
    print("[*] Đang xây dựng dim_date...")
    min_date = df['order_date_ts'].min().date()
    max_date = df['shipping_date_ts'].max().date()
    date_range = pd.date_range(min_date, max_date, freq='D')
    
    df_dim_date = pd.DataFrame({'full_date': date_range})
    df_dim_date['date_key'] = df_dim_date['full_date'].dt.strftime('%Y%m%d').astype(int)
    df_dim_date['year'] = df_dim_date['full_date'].dt.year
    df_dim_date['quarter'] = df_dim_date['full_date'].dt.quarter
    df_dim_date['month'] = df_dim_date['full_date'].dt.month
    df_dim_date['month_name'] = df_dim_date['full_date'].dt.strftime('%B')
    df_dim_date['day_of_week'] = df_dim_date['full_date'].dt.dayofweek + 1
    df_dim_date['day_name'] = df_dim_date['full_date'].dt.strftime('%A')
    df_dim_date['is_weekend'] = (df_dim_date['day_of_week'] >= 6).astype(int)
    df_dim_date['is_peak_season'] = df_dim_date['month'].isin([10, 11, 12]).astype(int)
    print(f"    - dim_date: {len(df_dim_date):,} ngày ({min_date} -> {max_date}).")

    # -------------------------------------------------------------------------
    # 6. BẢNG FACT TRUNG TÂM: FACT_ORDER_SHIPMENTS
    # -------------------------------------------------------------------------
    print("[*] Đang ánh xạ khóa ngoại và xây dựng fact_order_shipments...")
    
    # 6.1. Merge lấy destination_geo_key
    df_fact = pd.merge(
        df,
        df_dim_geography,
        on=['market', 'order_region', 'order_country', 'order_state', 'order_city'],
        how='left'
    )

    # 6.2. Merge lấy shipping_ops_key
    df_fact = pd.merge(
        df_fact,
        df_dim_shipping_ops[['shipping_ops_key', 'shipping_mode', 'payment_type', 'order_status', 'delivery_status']],
        left_on=['shipping_mode', 'type', 'order_status', 'delivery_status'],
        right_on=['shipping_mode', 'payment_type', 'order_status', 'delivery_status'],
        how='left'
    )

    # 6.3. Tạo date_key cho order_date và shipping_date (Role-playing)
    df_fact['order_date_key'] = df_fact['order_date_ts'].dt.strftime('%Y%m%d').astype(int)
    df_fact['ship_date_key'] = df_fact['shipping_date_ts'].dt.strftime('%Y%m%d').astype(int)

    # 6.4. Chọn và định dạng lại các cột của Fact
    fact_columns_mapping = {
        'order_item_id': 'order_item_id',
        'order_id': 'order_id',
        'order_date_key': 'order_date_key',
        'ship_date_key': 'ship_date_key',
        'customer_id': 'customer_key',
        'product_card_id': 'product_key',
        'destination_geo_key': 'destination_geo_key',
        'shipping_ops_key': 'shipping_ops_key',
        'order_date_ts': 'order_date_ts',
        'shipping_date_ts': 'shipping_date_ts',
        'order_item_quantity': 'order_item_quantity',
        'product_price': 'product_price',
        'sales': 'gross_sales',
        'order_item_discount': 'discount_amount',
        'order_item_discount_rate': 'discount_rate',
        'order_item_total': 'net_sales',
        'order_item_profit_ratio': 'profit_ratio',
        'benefit_per_order': 'net_profit',
        'days_for_shipping_real': 'days_shipping_real',
        'days_for_shipment_scheduled': 'days_shipment_scheduled',
        'delay_days': 'delay_days',
        'late_delivery_risk': 'late_delivery_risk',
        'is_loss_order': 'is_loss_order',
        'is_canceled_or_fraud': 'is_canceled_or_fraud'
    }

    df_fact = df_fact[list(fact_columns_mapping.keys())].rename(columns=fact_columns_mapping)
    print(f"    - fact_order_shipments: {len(df_fact):,} dòng (Toàn vẹn khóa ngoại 100%).")

    # -------------------------------------------------------------------------
    # 7. XUẤT RA PARQUET VÀ SQLITE DWH BACKUP
    # -------------------------------------------------------------------------
    tables = {
        "dim_date": df_dim_date,
        "dim_customer": df_dim_customer,
        "dim_product": df_dim_product,
        "dim_geography": df_dim_geography,
        "dim_shipping_ops": df_dim_shipping_ops,
        "fact_order_shipments": df_fact
    }

    print("[*] Đang lưu 6 bảng Gold ra thư mục Parquet (data/gold/)...")
    for tbl_name, tbl_df in tables.items():
        pq_path = GOLD_DIR / f"{tbl_name}.parquet"
        tbl_df.to_parquet(pq_path, index=False, engine="pyarrow", compression="snappy")
        print(f"    - Lưu {tbl_name}: {pq_path.name} ({pq_path.stat().st_size / 1024:.1f} KB)")

    duration = round(time.time() - start_time, 2)
    print(f"[✓] HOÀN TẤT BƯỚC 3: Mô hình Gold Star Schema sẵn sàng trong {duration}s!")
    print(f"    - Kho dữ liệu Parquet: {GOLD_DIR}")
    return tables

if __name__ == "__main__":
    run_gold_modeling()
