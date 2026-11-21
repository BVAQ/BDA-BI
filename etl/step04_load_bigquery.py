"""
Module: 04_load_bigquery.py
Mục đích: BƯỚC 4 - Nạp 6 bảng Gold lên Google Cloud BigQuery (Cloud DWH Load).
Trách nhiệm duy nhất (Single Responsibility):
  1. Kiểm tra xác thực Google Cloud (Google OAuth / Service Account).
  2. Tự động khởi tạo Dataset 'dataco_logistics_gold' nếu chưa tồn tại.
  3. Cấu hình Partitioning (DATE) và Clustering (shipping_ops_key, geo_key) cho bảng Fact.
  4. Đẩy 5 bảng Dimension và 1 bảng Fact từ Parquet lên BigQuery.
  5. Cung cấp chế độ Dry-Run / Local Check nếu chưa kết nối mạng GCP.
"""

import sys
import time
import pandas as pd
from pathlib import Path

# Thêm thư mục gốc vào PYTHONPATH
sys.path.append(str(Path(__file__).resolve().parent.parent))
from etl.config import GOLD_DIR, GCP_PROJECT_ID, BQ_DATASET_ID, BQ_LOCATION

def run_bigquery_load(dry_run: bool = False):
    print("=" * 80)
    print(">>> BƯỚC 4: NẠP DỮ LIỆU LÊN GOOGLE CLOUD BIGQUERY (BIGQUERY DWH LOAD)")
    print("=" * 80)
    print(f"[*] Cấu hình mục tiêu: Project={GCP_PROJECT_ID} | Dataset={BQ_DATASET_ID} | Region={BQ_LOCATION}")

    tables = ["dim_date", "dim_customer", "dim_product", "dim_geography", "dim_shipping_ops", "fact_order_shipments"]

    # Kiểm tra các file Parquet nguồn
    missing = [t for t in tables if not (GOLD_DIR / f"{t}.parquet").exists()]
    if missing:
        raise FileNotFoundError(f"Thiếu các bảng Gold Parquet: {missing}. Hãy chạy bước 03 trước!")

    try:
        from google.cloud import bigquery
        from google.cloud.exceptions import NotFound
        client = bigquery.Client(project=GCP_PROJECT_ID)
        gcp_available = True
    except Exception as e:
        print(f"[!] Không thể khởi tạo BigQuery Client ({e}).")
        print("[!] Đang chuyển sang chế độ hiển thị DDL Script (Dry-Run Mode).")
        gcp_available = False

    if dry_run or not gcp_available:
        print("\n--- BIGQUERY DDL & OPTIMIZATION SPECIFICATION (DRY-RUN) ---")
        print(f"1. Tạo Dataset: `CREATE SCHEMA IF NOT EXISTS `{GCP_PROJECT_ID}.{BQ_DATASET_ID}` OPTIONS (location = '{BQ_LOCATION}');`")
        print("2. Cấu hình bảng Fact tối ưu hóa:")
        print("""
        CREATE OR REPLACE TABLE `{project}.{dataset}.fact_order_shipments`
        PARTITION BY DATE_TRUNC(DATE(order_date_ts), MONTH)
        CLUSTER BY shipping_ops_key, destination_geo_key, late_delivery_risk
        AS SELECT * FROM ...;
        """.format(project=GCP_PROJECT_ID, dataset=BQ_DATASET_ID))
        print("[i] Để nạp trực tiếp lên Cloud, hãy chạy file này trên Google Colab với lệnh `auth.authenticate_user()`.")
        return

    # Nếu có kết nối GCP thực tế:
    dataset_ref = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}"
    try:
        client.get_dataset(dataset_ref)
        print(f"[+] Dataset '{dataset_ref}' đã tồn tại.")
    except Exception:
        print(f"[*] Đang tạo mới Dataset '{dataset_ref}' tại {BQ_LOCATION}...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = BQ_LOCATION
        client.create_dataset(dataset, timeout=30)
        print(f"[+] Đã tạo Dataset '{dataset_ref}'.")

    # Nạp từng bảng
    for tbl in tables:
        pq_file = GOLD_DIR / f"{tbl}.parquet"
        df_tbl = pd.read_parquet(pq_file)
        table_ref = f"{dataset_ref}.{tbl}"

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            source_format=bigquery.SourceFormat.PARQUET
        )

        # Cấu hình Partitioning và Clustering riêng cho Fact
        if tbl == "fact_order_shipments":
            job_config.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.MONTH,
                field="order_date_ts"
            )
            job_config.clustering_fields = ["shipping_ops_key", "destination_geo_key", "late_delivery_risk"]

        print(f"[*] Đang nạp bảng '{tbl}' ({len(df_tbl):,} dòng) lên BigQuery...")
        job = client.load_table_from_dataframe(df_tbl, table_ref, job_config=job_config)
        job.result() # Đợi nạp xong
        print(f"[+] Nạp thành công: {table_ref}")

    print(f"[✓] HOÀN TẤT BƯỚC 4: Toàn bộ 6 bảng đã sẵn sàng trên Cloud BigQuery!")

if __name__ == "__main__":
    run_bigquery_load()
