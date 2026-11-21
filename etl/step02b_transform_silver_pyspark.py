"""
Module: step02b_transform_silver_pyspark.py
Mục đích: BƯỚC 2B - Thực thi cùng logic làm sạch & feature engineering của Silver Layer bằng Apache PySpark.
Trách nhiệm duy nhất (Single Responsibility):
  - Minh chứng kiến trúc xử lý phân tán (Distributed Processing) đáp ứng yêu cầu Big Data.
  - Áp dụng Broadcast Join để tối ưu khi join bảng lớn với bảng nhỏ (Fact × Dimension).
  - Benchmark so sánh thời gian PySpark vs Pandas (kết quả ghi vào JSON).
  - Xuất Silver Parquet (thư mục) tương thích với BigQuery Storage API.

Lưu ý môi trường:
  - Trên Windows local: cần cài pyspark (`pip install pyspark`) + Java 11+
  - Trên Google Colab: `!pip install pyspark` là đủ
  - Output: data/silver/silver_pyspark/ (thư mục nhiều part-files Parquet)
"""

import sys
import json
import time
from pathlib import Path

# Thêm thư mục gốc vào PYTHONPATH để import config
sys.path.append(str(Path(__file__).resolve().parent.parent))
from etl.config import (
    BRONZE_DIR, SILVER_DIR,
    REDUNDANT_COLUMNS_TO_DROP, EMPTY_COLUMNS_TO_DROP, PII_COLUMNS_TO_DROP,
    CITY_REPAIR_MAP
)

# ---------------------------------------------------------------------------
# Hàm kiểm tra PySpark và khởi động SparkSession
# ---------------------------------------------------------------------------
def create_spark_session():
    """Khởi tạo SparkSession tối ưu cho môi trường single-node (Colab / local)."""
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        raise ImportError(
            "PySpark chưa được cài đặt!\n"
            "  - Google Colab: !pip install pyspark\n"
            "  - Local:        pip install pyspark  (cần Java 11+)\n"
        )

    spark = (
        SparkSession.builder
        .appName("DataCo_Silver_PySpark")
        # Tối ưu cho single-node: dùng toàn bộ CPU core
        .config("spark.master", "local[*]")
        # Bộ nhớ driver (quan trọng khi chạy local)
        .config("spark.driver.memory", "4g")
        # Tắt log rác Spark (giữ output sạch)
        .config("spark.sql.shuffle.partitions", "8")
        # Bật Adaptive Query Execution (tự điều chỉnh plan)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Ngưỡng Broadcast Join: bảng < 50 MB tự động broadcast
        .config("spark.sql.autoBroadcastJoinThreshold", "52428800")
        # Tắt giao diện Spark UI để giảm overhead khi chạy script
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Logic làm sạch & Feature Engineering bằng PySpark
# ---------------------------------------------------------------------------
def run_silver_pyspark_transformation():
    print("=" * 80)
    print(">>> BƯỚC 2B: LÀM SẠCH DỮ LIỆU (PYSPARK - DISTRIBUTED PROCESSING)")
    print("=" * 80)

    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType, StringType

    # -------------------------------------------------------------------------
    # 0. Khởi động SparkSession
    # -------------------------------------------------------------------------
    print("[*] Khởi động SparkSession...")
    spark = create_spark_session()
    spark_version = spark.version
    print(f"[+] PySpark {spark_version} đã sẵn sàng. Master: {spark.sparkContext.master}")

    bronze_input = BRONZE_DIR / "bronze_raw_data.parquet"
    if not bronze_input.exists():
        raise FileNotFoundError(
            f"Chưa có dữ liệu Bronze. Hãy chạy bước 01 trước! ({bronze_input})"
        )

    # -------------------------------------------------------------------------
    # 1. Nạp dữ liệu Bronze (Parquet → Spark DataFrame)
    # -------------------------------------------------------------------------
    print(f"[*] Đang nạp Bronze Parquet vào Spark DataFrame: {bronze_input.name}...")
    t_start = time.time()
    df = spark.read.parquet(str(bronze_input))
    init_rows = df.count()
    init_cols = len(df.columns)
    print(f"[+] Nạp xong: {init_rows:,} dòng × {init_cols} cột.")

    # -------------------------------------------------------------------------
    # 2. Sửa lỗi font chữ địa danh quốc tế (Unicode Repair)
    # -------------------------------------------------------------------------
    print("[*] Đang sửa lỗi ký tự địa danh quốc tế...")
    # Dùng chuỗi F.when().when()...otherwise() để xử lý tất cả trong 1 lần pass
    city_expr = F.col("Order City")
    for bad_name, fixed_name in CITY_REPAIR_MAP.items():
        city_expr = F.when(F.col("Order City") == bad_name, fixed_name).otherwise(city_expr)
    df = df.withColumn("Order City", city_expr)
    print(f"[+] Đã áp dụng sửa font cho {len(CITY_REPAIR_MAP)} địa danh.")

    # -------------------------------------------------------------------------
    # 3. Loại bỏ cột trùng lặp, rỗng và PII
    # -------------------------------------------------------------------------
    cols_to_remove = REDUNDANT_COLUMNS_TO_DROP + EMPTY_COLUMNS_TO_DROP + PII_COLUMNS_TO_DROP
    existing_cols = df.columns
    cols_to_drop = [c for c in cols_to_remove if c in existing_cols]
    df = df.drop(*cols_to_drop)
    print(f"[+] Đã loại bỏ {len(cols_to_drop)} cột (trùng lặp + rỗng + PII).")

    # -------------------------------------------------------------------------
    # 4. Xử lý giá trị Null (Imputation)
    # -------------------------------------------------------------------------
    if "Customer Zipcode" in df.columns:
        df = df.withColumn(
            "Customer Zipcode",
            F.lpad(F.coalesce(F.col("Customer Zipcode").cast(IntegerType()), F.lit(0)).cast(StringType()), 5, "0")
        )

    # -------------------------------------------------------------------------
    # 5. Chuẩn hóa tên cột sang snake_case
    # -------------------------------------------------------------------------
    def to_snake_case(col_name: str) -> str:
        clean = col_name.strip().replace("(", "").replace(")", "").replace(".", "").replace("-", " ")
        return "_".join(clean.lower().split())

    # Đổi tên tất cả cột một lần
    for old_col in df.columns:
        new_col = to_snake_case(old_col)
        if old_col != new_col:
            df = df.withColumnRenamed(old_col, new_col)

    # -------------------------------------------------------------------------
    # 6. Ép kiểu Timestamp
    # -------------------------------------------------------------------------
    print("[*] Đang chuyển đổi cột Timestamp...")
    df = df.withColumn("order_date_ts",    F.to_timestamp("order_date_dateorders"))
    df = df.withColumn("shipping_date_ts", F.to_timestamp("shipping_date_dateorders"))
    df = df.drop("order_date_dateorders", "shipping_date_dateorders")

    # -------------------------------------------------------------------------
    # 7. Feature Engineering bằng PySpark Functions
    # -------------------------------------------------------------------------
    print("[*] Đang tạo đặc trưng nghiệp vụ (Feature Engineering - PySpark)...")

    # 7.1. delay_days: Độ lệch ngày giao hàng thực tế vs cam kết
    df = df.withColumn(
        "delay_days",
        F.col("days_for_shipping_real") - F.col("days_for_shipment_scheduled")
    )

    # 7.2. is_loss_order: Cờ đơn hàng lỗ vốn (benefit_per_order < 0)
    df = df.withColumn(
        "is_loss_order",
        F.when(F.col("benefit_per_order") < 0, 1).otherwise(0)
    )

    # 7.3. is_canceled_or_fraud: Cờ đơn hàng hủy / nghi gian lận
    df = df.withColumn(
        "is_canceled_or_fraud",
        F.when(F.col("order_status").isin("CANCELED", "SUSPECTED_FRAUD"), 1).otherwise(0)
    )

    # 7.4. Đặc trưng thời gian
    df = df.withColumn("order_year",      F.year("order_date_ts"))
    df = df.withColumn("order_month",     F.month("order_date_ts"))
    df = df.withColumn("order_hour",      F.hour("order_date_ts"))
    # dayofweek trong PySpark: 1=Sunday,...,7=Saturday → cộng offset về 1=Monday,7=Sunday
    df = df.withColumn("order_dayofweek", ((F.dayofweek("order_date_ts") + 5) % 7) + 1)
    df = df.withColumn("is_weekend",
        F.when(F.col("order_dayofweek") >= 6, 1).otherwise(0)
    )

    # 7.5. is_peak_season: Quý 4 (Oct-Nov-Dec) là mùa cao điểm mua sắm toàn cầu
    df = df.withColumn(
        "is_peak_season",
        F.when(F.col("order_month").isin(10, 11, 12), 1).otherwise(0)
    )

    # -------------------------------------------------------------------------
    # 8. BONUS: Broadcast Join minh họa kiến trúc xử lý phân tán
    # Tình huống: Enrich dữ liệu Silver với bảng lookup nhỏ "shipping_mode_sla"
    # Đây là pattern chuẩn trong production khi join Fact (180K rows) × Dim (nhỏ)
    # -------------------------------------------------------------------------
    print("[*] Minh họa Broadcast Join (Fact lớn × Dimension nhỏ)...")

    # Tạo bảng lookup nhỏ: SLA cam kết theo shipping_mode (4 dòng)
    sla_data = [
        ("First Class",   1, "express"),
        ("Second Class",  2, "standard"),
        ("Standard Class",4, "economy"),
        ("Same Day",      0, "same_day"),
    ]
    sla_schema = ["shipping_mode", "sla_days_promised", "service_tier"]
    dim_sla = spark.createDataFrame(sla_data, schema=sla_schema)

    # broadcast() force Spark gửi bảng nhỏ đến tất cả executor → tránh shuffle
    df = df.join(
        F.broadcast(dim_sla),
        on="shipping_mode",
        how="left"
    )
    print("[+] Broadcast Join hoàn tất: đã thêm cột 'sla_days_promised' và 'service_tier'.")

    # -------------------------------------------------------------------------
    # 9. Cache DataFrame trước khi tính toán thống kê (tránh recompute)
    # -------------------------------------------------------------------------
    df.cache()
    final_rows = df.count()   # trigger action để materialize cache
    final_cols = len(df.columns)

    # -------------------------------------------------------------------------
    # 10. Lưu ra Silver Layer (thư mục Parquet phân tán)
    # -------------------------------------------------------------------------
    silver_pyspark_dir = SILVER_DIR / "silver_pyspark"
    print(f"[*] Đang ghi Silver Parquet (PySpark) vào: {silver_pyspark_dir}/...")

    # coalesce(4): gộp thành 4 part-files (phù hợp single-node, tránh hàng nghìn file nhỏ)
    (
        df.coalesce(4)
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(str(silver_pyspark_dir))
    )

    t_end = time.time()
    pyspark_duration = round(t_end - t_start, 2)

    # -------------------------------------------------------------------------
    # 11. So sánh thời gian PySpark vs Pandas (đọc kết quả Pandas từ metrics cũ)
    # -------------------------------------------------------------------------
    pandas_metrics_path = SILVER_DIR / "silver_cleaning_metrics.json"
    pandas_duration = None
    if pandas_metrics_path.exists():
        with open(pandas_metrics_path, "r", encoding="utf-8") as f:
            pandas_metrics = json.load(f)
            pandas_duration = pandas_metrics.get("transformation_duration_seconds")

    # -------------------------------------------------------------------------
    # 12. Lưu Benchmark Report
    # -------------------------------------------------------------------------
    benchmark = {
        "spark_version": spark_version,
        "mode": "local[*]",
        "initial_rows": init_rows,
        "final_rows": final_rows,
        "initial_cols": init_cols,
        "final_cols": final_cols,
        "pyspark_duration_seconds": pyspark_duration,
        "pandas_duration_seconds": pandas_duration,
        "speedup_factor": (
            round(pandas_duration / pyspark_duration, 2)
            if pandas_duration and pyspark_duration > 0
            else "N/A (cần chạy step02 trước)"
        ),
        "broadcast_join_applied": True,
        "new_features_added": [
            "delay_days", "is_loss_order", "is_canceled_or_fraud",
            "order_year", "order_month", "order_hour", "order_dayofweek",
            "is_weekend", "is_peak_season", "sla_days_promised", "service_tier"
        ],
        "output_format": "Parquet (Snappy, 4 partitions)",
        "output_path": str(silver_pyspark_dir)
    }

    benchmark_path = SILVER_DIR / "silver_pyspark_benchmark.json"
    with open(benchmark_path, "w", encoding="utf-8") as f:
        json.dump(benchmark, f, indent=4, ensure_ascii=False)

    # -------------------------------------------------------------------------
    # 13. In tóm tắt kết quả
    # -------------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("[✓] HOÀN TẤT BƯỚC 2B: Silver Layer (PySpark)")
    print("=" * 80)
    print(f"    - Thời gian chạy PySpark : {pyspark_duration}s")
    if pandas_duration:
        speedup = benchmark['speedup_factor']
        note = "(PySpark nhanh hơn)" if isinstance(speedup, float) and speedup > 1 else "(Pandas nhanh hơn trên dữ liệu nhỏ - bình thường)"
        print(f"    - Thời gian Pandas (step02): {pandas_duration}s")
        print(f"    - Hệ số speedup            : {speedup}x {note}")
    print(f"    - Dòng đầu vào  : {init_rows:,} | Dòng đầu ra: {final_rows:,}")
    print(f"    - Cột đầu vào   : {init_cols}  | Cột đầu ra : {final_cols}")
    print(f"    - Broadcast Join: ✓ (dim_sla 4 dòng được broadcast)")
    print(f"    - Output        : {silver_pyspark_dir}/")
    print(f"    - Benchmark     : {benchmark_path.name}")
    print(
        "\n  [NOTE] Trên dữ liệu 180K dòng, PySpark có thể chậm hơn Pandas vì overhead\n"
        "         khởi động JVM + serialization. PySpark cho thấy ưu thế rõ ràng khi\n"
        "         dataset > 10 triệu dòng hoặc chạy trên cluster nhiều node.\n"
        "         → Mục đích ở đây là minh chứng kiến trúc xử lý phân tán (Distributed\n"
        "           Processing Architecture), không phải so sánh tốc độ thuần túy.\n"
    )

    spark.stop()
    return silver_pyspark_dir


if __name__ == "__main__":
    run_silver_pyspark_transformation()

