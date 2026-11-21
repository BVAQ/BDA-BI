"""
Module: step02c_validate_quality.py
Mục đích: BƯỚC 2C - Kiểm tra chất lượng dữ liệu tự động sau ETL (Data Quality Validation).
Trách nhiệm duy nhất (Single Responsibility):
  1. Row-count assertions: đảm bảo không mất/nhân dòng qua các tầng Bronze → Silver → Gold.
  2. Schema assertions: kiểm tra tất cả cột mong đợi đều tồn tại.
  3. Null assertions: các cột nghiệp vụ quan trọng phải không có giá trị null.
  4. Referential integrity: FK trong Fact phải có giá trị hợp lệ (không null/âm).
  5. Business rule assertions: kiểm tra ràng buộc nghiệp vụ (giá trị hợp lệ, khoảng giá trị).
  6. Outlier detection: phát hiện bất thường bằng IQR và Z-score.
  7. Xuất báo cáo Data Quality Report (JSON) với pass/fail chi tiết.

Output: data/gold/data_quality_report.json
"""

import sys
import json
import time
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime

# Thêm thư mục gốc vào PYTHONPATH
sys.path.append(str(Path(__file__).resolve().parent.parent))
from etl.config import BRONZE_DIR, SILVER_DIR, GOLD_DIR


# ---------------------------------------------------------------------------
# Hằng số kỳ vọng nghiệp vụ
# ---------------------------------------------------------------------------
EXPECTED_ROW_COUNT      = 180_519        # Tổng dòng gốc của DataCo dataset
EXPECTED_SILVER_COLS    = [              # Các cột bắt buộc phải có ở Silver
    "order_id", "order_item_id", "order_status",
    "shipping_mode", "late_delivery_risk",
    "benefit_per_order", "order_item_total",
    "order_date_ts", "shipping_date_ts",
    "delay_days", "is_loss_order", "is_canceled_or_fraud",
    "order_year", "order_month", "order_hour", "order_dayofweek",
    "is_weekend", "is_peak_season",
]
EXPECTED_GOLD_TABLES    = [              # Tất cả bảng Gold (Kimball Star Schema)
    "dim_date", "dim_customer", "dim_product",
    "dim_geography", "dim_shipping_ops", "fact_order_shipments",
]
CRITICAL_NULL_COLS      = [              # Cột nghiệp vụ không được phép null ở Silver
    "order_id", "order_item_id", "late_delivery_risk",
    "order_item_total", "benefit_per_order", "order_date_ts",
]
BINARY_FLAG_COLS        = [              # Cột cờ nhị phân phải chỉ chứa {0, 1}
    "late_delivery_risk", "is_loss_order", "is_canceled_or_fraud",
    "is_weekend", "is_peak_season",
]
VALID_ORDER_STATUSES    = {              # Tập giá trị hợp lệ của order_status
    "COMPLETE", "PENDING", "CLOSED", "PENDING_PAYMENT",
    "CANCELED", "PROCESSING", "SUSPECTED_FRAUD",
    "ON_HOLD", "PAYMENT_REVIEW",
}
VALID_SHIPPING_MODES    = {              # Tập giá trị hợp lệ của shipping_mode
    "First Class", "Second Class", "Standard Class", "Same Day",
}


# ---------------------------------------------------------------------------
# Tiện ích tạo kết quả kiểm tra
# ---------------------------------------------------------------------------
def _pass(check_name: str, detail: str = "") -> dict:
    return {"check": check_name, "status": "PASS", "detail": detail}

def _fail(check_name: str, detail: str = "") -> dict:
    return {"check": check_name, "status": "FAIL", "detail": detail}

def _warn(check_name: str, detail: str = "") -> dict:
    return {"check": check_name, "status": "WARN", "detail": detail}


# ---------------------------------------------------------------------------
# Các nhóm kiểm tra
# ---------------------------------------------------------------------------

def check_row_counts(bronze_df: pd.DataFrame, silver_df: pd.DataFrame) -> list:
    """
    Kiểm tra số dòng qua các tầng:
    - Bronze phải bằng đúng EXPECTED_ROW_COUNT (không mất dòng khi đọc CSV).
    - Silver phải bằng đúng Bronze (ETL không được xóa hay nhân dòng).
    """
    results = []
    bronze_rows = len(bronze_df)
    silver_rows = len(silver_df)

    # Bronze vs kỳ vọng gốc
    if bronze_rows == EXPECTED_ROW_COUNT:
        results.append(_pass("row_count.bronze_vs_expected",
            f"Bronze = {bronze_rows:,} dòng = kỳ vọng {EXPECTED_ROW_COUNT:,}"))
    else:
        results.append(_fail("row_count.bronze_vs_expected",
            f"Bronze = {bronze_rows:,} dòng, kỳ vọng {EXPECTED_ROW_COUNT:,} "
            f"(lệch {abs(bronze_rows - EXPECTED_ROW_COUNT):,} dòng)"))

    # Silver vs Bronze (ETL transformation không được thay đổi số dòng)
    if silver_rows == bronze_rows:
        results.append(_pass("row_count.silver_vs_bronze",
            f"Silver = {silver_rows:,} = Bronze {bronze_rows:,} ✓ (không mất/nhân dòng)"))
    else:
        results.append(_fail("row_count.silver_vs_bronze",
            f"Silver = {silver_rows:,} ≠ Bronze {bronze_rows:,} "
            f"(lệch {silver_rows - bronze_rows:+,} dòng — có thể bị drop/duplicate lỗi)"))

    return results


def check_schema(silver_df: pd.DataFrame) -> list:
    """Kiểm tra tất cả cột bắt buộc phải tồn tại ở Silver."""
    results = []
    missing = [c for c in EXPECTED_SILVER_COLS if c not in silver_df.columns]
    if not missing:
        results.append(_pass("schema.required_columns",
            f"Tất cả {len(EXPECTED_SILVER_COLS)} cột bắt buộc đều có mặt ở Silver"))
    else:
        results.append(_fail("schema.required_columns",
            f"Thiếu {len(missing)} cột: {missing}"))
    return results


def check_gold_tables(gold_dir: Path) -> list:
    """Kiểm tra tất cả bảng Gold (Parquet) đã được tạo."""
    results = []
    for table in EXPECTED_GOLD_TABLES:
        parquet_file = gold_dir / f"{table}.parquet"
        if parquet_file.exists():
            size_mb = parquet_file.stat().st_size / (1024 * 1024)
            results.append(_pass(f"gold_tables.{table}",
                f"{table}.parquet tồn tại ({size_mb:.2f} MB)"))
        else:
            results.append(_fail(f"gold_tables.{table}",
                f"Không tìm thấy {table}.parquet — chạy step03 trước!"))
    return results


def check_null_assertions(silver_df: pd.DataFrame) -> list:
    """Kiểm tra các cột nghiệp vụ quan trọng không được có null."""
    results = []
    for col in CRITICAL_NULL_COLS:
        if col not in silver_df.columns:
            results.append(_warn(f"null_check.{col}", f"Cột '{col}' không tồn tại, bỏ qua"))
            continue
        null_count = int(silver_df[col].isna().sum())
        if null_count == 0:
            results.append(_pass(f"null_check.{col}", f"0 null trong cột '{col}'"))
        else:
            pct = null_count / len(silver_df) * 100
            results.append(_fail(f"null_check.{col}",
                f"{null_count:,} null ({pct:.2f}%) trong cột '{col}'"))
    return results


def check_referential_integrity(gold_dir: Path) -> list:
    """
    Kiểm tra referential integrity của Fact table:
    - Các cột FK trong fact_order_shipments không được null.
    - Khóa chính của Dimension tables phải là duy nhất.
    """
    results = []
    fact_path = gold_dir / "fact_order_shipments.parquet"
    if not fact_path.exists():
        return [_warn("referential_integrity", "fact_order_shipments.parquet chưa tồn tại — bỏ qua")]

    fact_df = pd.read_parquet(fact_path)

    # Kiểm tra FK không null trong Fact
    fk_columns = [c for c in fact_df.columns if c.endswith("_key")]
    for fk in fk_columns:
        null_count = int(fact_df[fk].isna().sum())
        if null_count == 0:
            results.append(_pass(f"fk_integrity.{fk}", f"Không có null FK trong '{fk}'"))
        else:
            results.append(_fail(f"fk_integrity.{fk}",
                f"{null_count:,} null FK trong '{fk}' — Fact có orphan records!"))

    # Kiểm tra PK của Dimension tables là duy nhất
    dim_pk_map = {
        "dim_date":         "date_key",
        "dim_customer":     "customer_key",
        "dim_product":      "product_key",
        "dim_geography":    "destination_geo_key",   # PK thực tế trong step03
        "dim_shipping_ops": "shipping_ops_key",
    }
    for dim_table, pk_col in dim_pk_map.items():
        dim_path = gold_dir / f"{dim_table}.parquet"
        if not dim_path.exists():
            results.append(_warn(f"pk_unique.{dim_table}", f"{dim_table}.parquet chưa tồn tại"))
            continue
        dim_df = pd.read_parquet(dim_path)
        if pk_col not in dim_df.columns:
            results.append(_warn(f"pk_unique.{dim_table}", f"Cột PK '{pk_col}' không tìm thấy"))
            continue
        total_rows  = len(dim_df)
        unique_keys = dim_df[pk_col].nunique()
        if unique_keys == total_rows:
            results.append(_pass(f"pk_unique.{dim_table}",
                f"PK '{pk_col}' là duy nhất ({unique_keys:,} giá trị / {total_rows:,} dòng)"))
        else:
            results.append(_fail(f"pk_unique.{dim_table}",
                f"PK '{pk_col}' có {total_rows - unique_keys:,} giá trị trùng lặp!"))

    return results


def check_business_rules(silver_df: pd.DataFrame) -> list:
    """Kiểm tra các ràng buộc nghiệp vụ cụ thể của DataCo dataset."""
    results = []

    # (a) Cột cờ nhị phân chỉ chứa {0, 1}
    for col in BINARY_FLAG_COLS:
        if col not in silver_df.columns:
            results.append(_warn(f"biz_rule.binary.{col}", f"Cột '{col}' không tồn tại"))
            continue
        invalid_vals = set(silver_df[col].dropna().unique()) - {0, 1}
        if not invalid_vals:
            results.append(_pass(f"biz_rule.binary.{col}",
                f"'{col}' chỉ chứa {{0, 1}}"))
        else:
            results.append(_fail(f"biz_rule.binary.{col}",
                f"'{col}' chứa giá trị ngoài {{0,1}}: {invalid_vals}"))

    # (b) order_status phải nằm trong tập giá trị hợp lệ
    if "order_status" in silver_df.columns:
        actual_vals = set(silver_df["order_status"].dropna().unique())
        unknown_vals = actual_vals - VALID_ORDER_STATUSES
        if not unknown_vals:
            results.append(_pass("biz_rule.order_status_values",
                f"Tất cả {len(actual_vals)} trạng thái đơn hàng đều hợp lệ"))
        else:
            results.append(_fail("biz_rule.order_status_values",
                f"Phát hiện {len(unknown_vals)} trạng thái chưa biết: {unknown_vals}"))

    # (c) shipping_mode phải nằm trong tập hợp lệ
    if "shipping_mode" in silver_df.columns:
        actual_modes = set(silver_df["shipping_mode"].dropna().unique())
        unknown_modes = actual_modes - VALID_SHIPPING_MODES
        if not unknown_modes:
            results.append(_pass("biz_rule.shipping_mode_values",
                f"Tất cả {len(actual_modes)} shipping mode đều hợp lệ"))
        else:
            results.append(_fail("biz_rule.shipping_mode_values",
                f"Phát hiện shipping mode lạ: {unknown_modes}"))

    # (d) order_item_quantity phải > 0
    if "order_item_quantity" in silver_df.columns:
        non_positive = int((silver_df["order_item_quantity"] <= 0).sum())
        if non_positive == 0:
            results.append(_pass("biz_rule.quantity_positive",
                "Tất cả order_item_quantity > 0"))
        else:
            results.append(_fail("biz_rule.quantity_positive",
                f"{non_positive:,} dòng có order_item_quantity ≤ 0"))

    # (e) order_item_discount_rate phải trong [0.0, 1.0]
    disc_col = next((c for c in silver_df.columns if "discount" in c and "rate" in c), None)
    if disc_col:
        out_of_range = int(((silver_df[disc_col] < 0) | (silver_df[disc_col] > 1)).sum())
        if out_of_range == 0:
            results.append(_pass(f"biz_rule.discount_rate_range",
                f"'{disc_col}' hoàn toàn nằm trong [0.0, 1.0]"))
        else:
            results.append(_fail(f"biz_rule.discount_rate_range",
                f"{out_of_range:,} dòng có '{disc_col}' ngoài [0, 1]"))

    # (f) days_for_shipping_real phải ≥ 0
    if "days_for_shipping_real" in silver_df.columns:
        negative_days = int((silver_df["days_for_shipping_real"] < 0).sum())
        if negative_days == 0:
            results.append(_pass("biz_rule.shipping_days_non_negative",
                "Tất cả days_for_shipping_real ≥ 0"))
        else:
            results.append(_fail("biz_rule.shipping_days_non_negative",
                f"{negative_days:,} dòng có days_for_shipping_real < 0"))

    return results


def check_outliers(silver_df: pd.DataFrame) -> list:
    """
    Phát hiện ngoại lệ cực đoan bằng IQR (Interquartile Range) và Z-score.
    - Dữ liệu được gán cờ WARN (không phải lỗi) vì outlier đôi khi là dữ liệu thực.
    """
    results = []

    numeric_checks = [
        ("benefit_per_order",        "IQR"),   # Lợi nhuận đơn hàng
        ("order_item_total",         "IQR"),   # Doanh thu đơn hàng
        ("order_item_quantity",      "IQR"),   # Số lượng sản phẩm
        ("days_for_shipping_real",   "ZSCORE"), # Thời gian giao thực tế
        ("delay_days",               "ZSCORE"), # Độ trễ giao hàng
    ]

    for col, method in numeric_checks:
        if col not in silver_df.columns:
            continue
        series = silver_df[col].dropna()

        if method == "IQR":
            q1  = series.quantile(0.25)
            q3  = series.quantile(0.75)
            iqr = q3 - q1
            lower_fence = q1 - 3.0 * iqr   # Dùng 3×IQR (extreme outlier, không phải mild)
            upper_fence = q3 + 3.0 * iqr
            outlier_count = int(((series < lower_fence) | (series > upper_fence)).sum())
            pct = outlier_count / len(series) * 100
            detail = (
                f"{outlier_count:,} ngoại lệ cực đoan ({pct:.2f}%) | "
                f"IQR fence: [{lower_fence:.2f}, {upper_fence:.2f}] | "
                f"Thực tế: [{series.min():.2f}, {series.max():.2f}]"
            )
            results.append(_warn(f"outlier.iqr.{col}", detail) if outlier_count > 0
                           else _pass(f"outlier.iqr.{col}", f"Không có outlier cực đoan (3×IQR)"))

        elif method == "ZSCORE":
            mean  = series.mean()
            std   = series.std()
            if std == 0:
                results.append(_pass(f"outlier.zscore.{col}", "std = 0, không tính Z-score"))
                continue
            z_scores      = np.abs((series - mean) / std)
            outlier_count = int((z_scores > 3.5).sum())   # |Z| > 3.5: extreme outlier
            pct = outlier_count / len(series) * 100
            detail = (
                f"{outlier_count:,} ngoại lệ cực đoan ({pct:.2f}%) | "
                f"|Z| > 3.5 | mean={mean:.2f}, std={std:.2f}"
            )
            results.append(_warn(f"outlier.zscore.{col}", detail) if outlier_count > 0
                           else _pass(f"outlier.zscore.{col}", f"Không có outlier |Z| > 3.5"))

    return results


def check_temporal_consistency(silver_df: pd.DataFrame) -> list:
    """
    Kiểm tra tính nhất quán thời gian:
    - shipping_date phải >= order_date (không thể giao trước khi đặt).
    - Khoảng thời gian order_date phải hợp lý (trong phạm vi dataset).
    """
    results = []

    if "order_date_ts" in silver_df.columns and "shipping_date_ts" in silver_df.columns:
        # shipping trước order là bất hợp lý
        invalid_time = int((silver_df["shipping_date_ts"] < silver_df["order_date_ts"]).sum())
        if invalid_time == 0:
            results.append(_pass("temporal.shipping_after_order",
                "Tất cả shipping_date ≥ order_date ✓"))
        else:
            results.append(_fail("temporal.shipping_after_order",
                f"{invalid_time:,} dòng có shipping_date < order_date (giao trước khi đặt!)"))

    if "order_date_ts" in silver_df.columns:
        min_date = silver_df["order_date_ts"].min()
        max_date = silver_df["order_date_ts"].max()
        # DataCo dataset trải dài 2015–2018
        expected_min = pd.Timestamp("2015-01-01")
        expected_max = pd.Timestamp("2018-12-31")
        if min_date >= expected_min and max_date <= expected_max:
            results.append(_pass("temporal.date_range",
                f"order_date trong [{min_date.date()}, {max_date.date()}] ⊆ [2015, 2018]"))
        else:
            results.append(_warn("temporal.date_range",
                f"order_date range: [{min_date.date()}, {max_date.date()}] "
                f"— một số ngày nằm ngoài phạm vi kỳ vọng [2015, 2018]"))

    return results


# ---------------------------------------------------------------------------
# Hàm chính
# ---------------------------------------------------------------------------
def run_data_quality_validation():
    print("=" * 80)
    print(">>> BƯỚC 2C: KIỂM TRA CHẤT LƯỢNG DỮ LIỆU (DATA QUALITY VALIDATION)")
    print("=" * 80)

    t_start = time.time()
    all_results = []

    # ---- Nạp dữ liệu Bronze & Silver ----------------------------------------
    bronze_path = BRONZE_DIR / "bronze_raw_data.parquet"
    silver_path = SILVER_DIR / "silver_cleaned_data.parquet"

    if not bronze_path.exists():
        raise FileNotFoundError(f"Bronze chưa tồn tại: {bronze_path}. Chạy step01 trước!")
    if not silver_path.exists():
        raise FileNotFoundError(f"Silver chưa tồn tại: {silver_path}. Chạy step02 trước!")

    print(f"[*] Đang nạp Bronze: {bronze_path.name}...")
    bronze_df = pd.read_parquet(bronze_path)

    print(f"[*] Đang nạp Silver: {silver_path.name}...")
    silver_df = pd.read_parquet(silver_path)

    # ---- Chạy các nhóm kiểm tra --------------------------------------------
    print("\n[1/7] Kiểm tra Row Counts (Bronze vs Silver vs kỳ vọng)...")
    all_results += check_row_counts(bronze_df, silver_df)

    print("[2/7] Kiểm tra Schema (cột bắt buộc)...")
    all_results += check_schema(silver_df)

    print("[3/7] Kiểm tra Gold Tables (Parquet files tồn tại)...")
    all_results += check_gold_tables(GOLD_DIR)

    print("[4/7] Kiểm tra Null Assertions (cột nghiệp vụ quan trọng)...")
    all_results += check_null_assertions(silver_df)

    print("[5/7] Kiểm tra Referential Integrity (FK / PK Gold)...")
    all_results += check_referential_integrity(GOLD_DIR)

    print("[6/7] Kiểm tra Business Rules (ràng buộc miền dữ liệu)...")
    all_results += check_business_rules(silver_df)

    print("[7/7] Kiểm tra Outlier Detection (IQR + Z-score)...")
    all_results += check_outliers(silver_df)

    # ---- Kiểm tra thêm: Tính nhất quán thời gian ----------------------------
    all_results += check_temporal_consistency(silver_df)

    # ---- Tổng hợp kết quả ---------------------------------------------------
    total   = len(all_results)
    passed  = sum(1 for r in all_results if r["status"] == "PASS")
    failed  = sum(1 for r in all_results if r["status"] == "FAIL")
    warned  = sum(1 for r in all_results if r["status"] == "WARN")
    overall = "PASSED" if failed == 0 else "FAILED"

    t_end     = time.time()
    duration  = round(t_end - t_start, 2)

    report = {
        "report_generated_at": datetime.now().isoformat(),
        "duration_seconds": duration,
        "overall_status": overall,
        "summary": {
            "total_checks": total,
            "passed": passed,
            "failed": failed,
            "warnings": warned,
            "pass_rate_pct": round(passed / total * 100, 2) if total > 0 else 0,
        },
        "dataset_info": {
            "bronze_rows": len(bronze_df),
            "bronze_cols": len(bronze_df.columns),
            "silver_rows": len(silver_df),
            "silver_cols": len(silver_df.columns),
        },
        "checks": all_results,
        "failed_checks": [r for r in all_results if r["status"] == "FAIL"],
    }

    # ---- Lưu báo cáo --------------------------------------------------------
    report_path = GOLD_DIR / "data_quality_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=4, ensure_ascii=False, default=str)

    # ---- In tóm tắt ---------------------------------------------------------
    print("\n" + "=" * 80)
    status_icon = "✅" if overall == "PASSED" else "❌"
    print(f"{status_icon} KẾT QUẢ KIỂM TRA CHẤT LƯỢNG DỮ LIỆU: {overall}")
    print("=" * 80)
    print(f"    - Tổng số kiểm tra : {total}")
    print(f"    - PASS             : {passed} ({report['summary']['pass_rate_pct']}%)")
    print(f"    - FAIL             : {failed}")
    print(f"    - WARN             : {warned}")
    print(f"    - Thời gian        : {duration}s")
    print(f"    - Báo cáo đã lưu  : {report_path}")

    if failed > 0:
        print("\n  [!] CÁC KIỂM TRA THẤT BẠI:")
        for r in report["failed_checks"]:
            print(f"      ✗ [{r['check']}] {r['detail']}")

    if warned > 0:
        print(f"\n  [!] {warned} cảnh báo (WARN) — xem chi tiết trong data_quality_report.json")

    print("=" * 80)

    # Trả về exit code 1 nếu có lỗi (hữu ích khi tích hợp CI/CD)
    if failed > 0:
        sys.exit(1)

    return report_path


if __name__ == "__main__":
    run_data_quality_validation()
