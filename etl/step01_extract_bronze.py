"""
Module: 01_extract_bronze.py
Mục đích: BƯỚC 1 - Trích xuất thô và kiểm định dữ liệu ban đầu (Bronze Ingestion & Profiling).
Trách nhiệm duy nhất (Single Responsibility):
  1. Đọc file CSV nguồn với encoding chuẩn ISO-8859-1.
  2. Đo lường kích thước, số dòng, số cột, kiểm tra tỷ lệ Null và trùng lặp.
  3. Xuất file snapshot tầng Bronze (Parquet) và báo cáo Data Profiling JSON.
"""

import sys
import json
import time
import pandas as pd
from pathlib import Path

# Thêm thư mục gốc vào PYTHONPATH
sys.path.append(str(Path(__file__).resolve().parent.parent))
from etl.config import RAW_DATA_FILE, BRONZE_DIR, RAW_FILE_ENCODING

def run_bronze_extraction():
    print("=" * 80)
    print(">>> BƯỚC 1: TRÍCH XUẤT THÔ & KIỂM ĐỊNH TẦNG BRONZE (BRONZE INGESTION)")
    print("=" * 80)

    start_time = time.time()

    if not RAW_DATA_FILE.exists():
        raise FileNotFoundError(f"Không tìm thấy file dữ liệu gốc tại: {RAW_DATA_FILE}")

    print(f"[*] Đang đọc file nguồn: {RAW_DATA_FILE.name} (Encoding: {RAW_FILE_ENCODING})...")
    df_raw = pd.read_csv(RAW_DATA_FILE, encoding=RAW_FILE_ENCODING)

    row_count, col_count = df_raw.shape
    print(f"[+] Đọc thành công: {row_count:,} dòng và {col_count} cột.")

    # 1. Kiểm tra tỷ lệ khuyết thiếu (Missing Values)
    null_counts = df_raw.isnull().sum()
    null_report = {col: int(cnt) for col, cnt in null_counts[null_counts > 0].items()}
    print(f"[*] Phát hiện các cột chứa giá trị Null: {null_report}")

    # 2. Kiểm tra tính toàn vẹn khóa chính sơ bộ
    unique_items = df_raw['Order Item Id'].nunique()
    print(f"[*] Khóa chính 'Order Item Id': {unique_items:,} giá trị duy nhất (Toàn vẹn 100%: {unique_items == row_count})")

    # 3. Tạo báo cáo profiling tóm tắt
    profile_summary = {
        "source_file": str(RAW_DATA_FILE.name),
        "total_rows": int(row_count),
        "total_columns": int(col_count),
        "null_columns": null_report,
        "unique_orders": int(df_raw['Order Id'].nunique()),
        "unique_customers": int(df_raw['Customer Id'].nunique()),
        "unique_products": int(df_raw['Product Card Id'].nunique()),
        "extraction_duration_seconds": round(time.time() - start_time, 2)
    }

    # 4. Lưu dữ liệu tầng Bronze (Parquet nén Snappy)
    bronze_output_file = BRONZE_DIR / "bronze_raw_data.parquet"
    print(f"[*] Đang lưu snapshot Bronze tại: {bronze_output_file}...")
    df_raw.to_parquet(bronze_output_file, index=False, engine="pyarrow", compression="snappy")

    # 5. Lưu báo cáo profiling
    report_file = BRONZE_DIR / "bronze_profiling_report.json"
    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(profile_summary, f, indent=4, ensure_ascii=False)

    print(f"[✓] HOÀN TẤT BƯỚC 1: Tầng Bronze sẵn sàng trong {profile_summary['extraction_duration_seconds']}s")
    print(f"    - Dữ liệu: {bronze_output_file} ({bronze_output_file.stat().st_size / (1024*1024):.2f} MB)")
    print(f"    - Báo cáo: {report_file}")
    return bronze_output_file

if __name__ == "__main__":
    run_bronze_extraction()
