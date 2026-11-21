"""
File: run_pipeline.py
Mục đích: Orchestrator - Điều phối chạy toàn bộ hoặc từng bước trong quy trình ETL.
Cách dùng:
  - Chạy toàn bộ pipeline (Pandas):  python run_pipeline.py
  - Chạy từng bước riêng lẻ:
      python run_pipeline.py --step 1    (Ingestion Bronze)
      python run_pipeline.py --step 2    (Cleaning Silver - Pandas)
      python run_pipeline.py --step 2b   (Cleaning Silver - PySpark Distributed)
      python run_pipeline.py --step 2c   (Data Quality Validation)
      python run_pipeline.py --step 3    (Gold Modeling - Kimball Star Schema)
      python run_pipeline.py --step 4    (BigQuery Load)
      python run_pipeline.py --step 5    (Semantic Views)
      python run_pipeline.py --step 6    (Performance Benchmark)
      python run_pipeline.py --step 7    (Predictive ML - Trang 3)
"""

import sys
import time
import argparse
from pathlib import Path

# Thêm thư mục gốc vào path
sys.path.append(str(Path(__file__).resolve().parent))

from etl.step01_extract_bronze import run_bronze_extraction
from etl.step02_transform_silver import run_silver_transformation
from etl.step03_model_gold import run_gold_modeling
from etl.step04_load_bigquery import run_bigquery_load
from etl.step05_create_views import run_create_views
from etl.step06_benchmark import run_performance_benchmark

# Các step phụ (import lazy để tránh lỗi khi PySpark chưa cài)
def _run_pyspark():
    from etl.step02b_transform_silver_pyspark import run_silver_pyspark_transformation
    return run_silver_pyspark_transformation()

def _run_validate():
    from etl.step02c_validate_quality import run_data_quality_validation
    return run_data_quality_validation()

def _run_predictive():
    from ml.train_predictive import run_predictive_modeling
    return run_predictive_modeling()

# Map step string -> (tên hiển thị, hàm chạy)
PIPELINE_STEPS = {
    "1":  ("Bước 1 : Trích xuất thô & Profiling (Bronze Layer)",              run_bronze_extraction),
    "2":  ("Bước 2 : Làm sạch & Feature Engineering - Pandas (Silver Layer)", run_silver_transformation),
    "2b": ("Bước 2B: Làm sạch & Feature Engineering - PySpark Distributed",  _run_pyspark),
    "2c": ("Bước 2C: Kiểm tra Chất lượng Dữ liệu (Data Quality Validation)", _run_validate),
    "3":  ("Bước 3 : Xây dựng Kimball Star Schema (Gold Layer)",              run_gold_modeling),
    "4":  ("Bước 4 : Nạp Cloud DWH Google BigQuery",                          lambda: run_bigquery_load(dry_run=False)),
    "5":  ("Bước 5 : Khởi tạo 4 Semantic Views BigQuery",                    run_create_views),
    "6":  ("Bước 6 : Đo lường Benchmark Hiệu năng phục vụ Báo cáo",          run_performance_benchmark),
    "7":  ("Bước 7 : Predictive ML - Dự báo Rủi ro Trễ hạn (Trang 3)",       _run_predictive),
}

# Thứ tự chạy khi không chỉ định --step (không bao gồm 2b để tránh cần PySpark)
DEFAULT_SEQUENCE = ["1", "2", "2c", "3", "4", "5", "6"]


def main():
    parser = argparse.ArgumentParser(
        description="DataCo Global Supply Chain - Modular ETL Pipeline Runner",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--step",
        type=str,
        choices=list(PIPELINE_STEPS.keys()),
        metavar="STEP",
        help=(
            "Bước cụ thể cần chạy:\n"
            "  1   → Bronze Extraction\n"
            "  2   → Silver Cleaning (Pandas)\n"
            "  2b  → Silver Cleaning (PySpark - cần pip install pyspark)\n"
            "  2c  → Data Quality Validation\n"
            "  3   → Gold Modeling\n"
            "  4   → BigQuery Load\n"
            "  5   → BigQuery Views\n"
            "  6   → Performance Benchmark\n"
            "  7   → Predictive ML (cần scikit-learn, xgboost, lightgbm)\n"
            "Bỏ trống để chạy toàn bộ (1 → 2 → 2c → 3 → 4 → 5 → 6)."
        ),
    )
    args = parser.parse_args()

    total_start = time.time()
    print("\n" + "#" * 80)
    print("#  DATACO GLOBAL SUPPLY CHAIN - MODULAR BIG DATA ETL PIPELINE")
    print("#  CO4033 - Big Data Analytics & Business Intelligence (Bách Khoa TP.HCM)")
    print("#" * 80 + "\n")

    if args.step:
        step_name, step_fn = PIPELINE_STEPS[args.step]
        print(f"[*] Đang thực thi riêng lẻ: {step_name}...\n")
        step_fn()
    else:
        print(f"[*] Đang thực thi TOÀN BỘ quy trình ETL: {' → '.join(DEFAULT_SEQUENCE)}\n")
        print("    Lưu ý: Bước 2b (PySpark) không chạy tự động — chạy riêng bằng --step 2b\n")
        for step_key in DEFAULT_SEQUENCE:
            step_name, step_fn = PIPELINE_STEPS[step_key]
            print(f"\n{'─' * 60}")
            print(f"  ▶ {step_name}")
            print(f"{'─' * 60}")
            step_fn()

    elapsed = round(time.time() - total_start, 2)
    print("\n" + "=" * 80)
    print(f"[✓✓✓] HOÀN TẤT TOÀN BỘ QUY TRÌNH TRONG {elapsed} GIÂY!")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
