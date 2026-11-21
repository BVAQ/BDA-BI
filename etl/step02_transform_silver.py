"""
Module: 02_transform_silver.py
Mục đích: BƯỚC 2 - Làm sạch dữ liệu, chuẩn hóa cấu trúc & làm giàu đặc trưng (Silver Layer).
Trách nhiệm duy nhất (Single Responsibility):
  1. Đọc dữ liệu tầng Bronze.
  2. Khắc phục lỗi font Latin-1 trên các địa danh quốc tế.
  3. Loại bỏ 5 cặp cột trùng lặp hoàn toàn, các cột rỗng và ẩn danh hóa PII.
  4. Xử lý giá trị Null còn lại (Imputation).
  5. Ép kiểu chuẩn datetime và sinh các đặc trưng nghiệp vụ (delay_days, is_loss_order, is_peak_season...).
  6. Lưu kết quả ra tầng Silver (Parquet).
"""

import sys
import json
import time
import pandas as pd
from pathlib import Path

# Thêm thư mục gốc vào PYTHONPATH
sys.path.append(str(Path(__file__).resolve().parent.parent))
from etl.config import (
    BRONZE_DIR, SILVER_DIR,
    REDUNDANT_COLUMNS_TO_DROP, EMPTY_COLUMNS_TO_DROP, PII_COLUMNS_TO_DROP,
    CITY_REPAIR_MAP
)

def run_silver_transformation():
    print("=" * 80)
    print(">>> BƯỚC 2: LÀM SẠCH DỮ LIỆU & TẠO ĐẶC TRƯNG TẦNG SILVER (SILVER CLEANING)")
    print("=" * 80)

    start_time = time.time()
    bronze_input = BRONZE_DIR / "bronze_raw_data.parquet"

    if not bronze_input.exists():
        raise FileNotFoundError(f"Chưa có dữ liệu Bronze. Hãy chạy bước 01 trước! ({bronze_input})")

    print(f"[*] Đang nạp dữ liệu Bronze từ: {bronze_input.name}...")
    df = pd.read_parquet(bronze_input)
    init_rows, init_cols = df.shape

    # 1. Sửa lỗi ký tự font chữ địa danh quốc tế do bảng mã Latin-1
    print("[*] Đang sửa lỗi ký tự font chữ quốc tế (Unicode repair)...")
    for bad_name, fixed_name in CITY_REPAIR_MAP.items():
        mask = df['Order City'] == bad_name
        if mask.any():
            df.loc[mask, 'Order City'] = fixed_name
            print(f"    - Đã sửa địa danh: '{bad_name}' -> '{fixed_name}' ({mask.sum()} dòng)")

    # 2. Loại bỏ các cột trùng lặp lược đồ, cột rỗng và cột PII
    cols_to_remove = REDUNDANT_COLUMNS_TO_DROP + EMPTY_COLUMNS_TO_DROP + PII_COLUMNS_TO_DROP
    existing_cols_to_remove = [c for c in cols_to_remove if c in df.columns]
    df.drop(columns=existing_cols_to_remove, inplace=True)
    print(f"[+] Đã loại bỏ {len(existing_cols_to_remove)} cột dư thừa, rỗng và PII nhạy cảm.")

    # 3. Xử lý giá trị Null còn lại (Imputation)
    if 'Customer Zipcode' in df.columns:
        df['Customer Zipcode'] = df['Customer Zipcode'].fillna(0).astype(int).astype(str).str.zfill(5)

    # 4. Chuẩn hóa tên cột về dạng snake_case
    def to_snake_case(col_name: str) -> str:
        clean = col_name.strip().replace("(", "").replace(")", "").replace(".", "").replace("-", " ")
        return "_".join(clean.lower().split())

    df.columns = [to_snake_case(c) for c in df.columns]

    # 5. Ép kiểu chuẩn Datetime
    print("[*] Đang chuyển đổi định dạng Timestamp (order_date & shipping_date)...")
    df['order_date_ts'] = pd.to_datetime(df['order_date_dateorders'], format='mixed')
    df['shipping_date_ts'] = pd.to_datetime(df['shipping_date_dateorders'], format='mixed')
    df.drop(columns=['order_date_dateorders', 'shipping_date_dateorders'], inplace=True)

    # 6. Feature Engineering (Tạo các đặc trưng nghiệp vụ cốt lõi)
    print("[*] Đang tạo các đặc trưng nghiệp vụ mới (Feature Engineering)...")
    
    # 6.1. Độ lệch ngày giao hàng thực tế vs cam kết
    df['delay_days'] = df['days_for_shipping_real'] - df['days_for_shipment_scheduled']
    
    # 6.2. Cờ phát hiện đơn hàng lỗ vốn (Benefit < 0)
    df['is_loss_order'] = (df['benefit_per_order'] < 0).astype(int)
    
    # 6.3. Cờ phát hiện đơn hàng hủy hoặc nghi ngờ gian lận
    df['is_canceled_or_fraud'] = df['order_status'].isin(['CANCELED', 'SUSPECTED_FRAUD']).astype(int)
    
    # 6.4. Các đặc trưng thời gian
    df['order_year'] = df['order_date_ts'].dt.year
    df['order_month'] = df['order_date_ts'].dt.month
    df['order_hour'] = df['order_date_ts'].dt.hour
    df['order_dayofweek'] = df['order_date_ts'].dt.dayofweek + 1 # 1 = Monday, 7 = Sunday
    df['is_weekend'] = (df['order_dayofweek'] >= 6).astype(int)
    df['is_peak_season'] = df['order_month'].isin([10, 11, 12]).astype(int) # Quý 4 cao điểm mua sắm

    # 7. Thống kê số liệu sau làm sạch
    silver_rows, silver_cols = df.shape
    silver_summary = {
        "initial_columns": init_cols,
        "clean_columns": silver_cols,
        "total_rows": silver_rows,
        "loss_orders_count": int(df['is_loss_order'].sum()),
        "late_orders_count": int((df['late_delivery_risk'] == 1).sum()),
        "total_net_sales": round(float(df['order_item_total'].sum()), 2),
        "total_net_profit": round(float(df['benefit_per_order'].sum()), 2),
        "transformation_duration_seconds": round(time.time() - start_time, 2)
    }

    # 8. Lưu dữ liệu tầng Silver
    silver_output_file = SILVER_DIR / "silver_cleaned_data.parquet"
    print(f"[*] Đang lưu snapshot Silver tại: {silver_output_file}...")
    df.to_parquet(silver_output_file, index=False, engine="pyarrow", compression="snappy")

    # Lưu metrics tóm tắt
    with open(SILVER_DIR / "silver_cleaning_metrics.json", "w", encoding="utf-8") as f:
        json.dump(silver_summary, f, indent=4, ensure_ascii=False)

    print(f"[✓] HOÀN TẤT BƯỚC 2: Tầng Silver hoàn thiện trong {silver_summary['transformation_duration_seconds']}s")
    print(f"    - Dữ liệu: {silver_output_file} ({silver_output_file.stat().st_size / (1024*1024):.2f} MB)")
    print(f"    - Cột ban đầu: {init_cols} -> Cột sau chuẩn hóa: {silver_cols}")
    return silver_output_file

if __name__ == "__main__":
    run_silver_transformation()
