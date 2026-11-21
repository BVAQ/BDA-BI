"""
Module: step06_benchmark.py
Mục đích: BƯỚC 6 - Đo lường hiệu năng thực nghiệm (Empirical Performance Benchmark).
Trách nhiệm duy nhất (Single Responsibility):
  1. Benchmark tốc độ đọc: Raw CSV vs Parquet Columnar (I/O & schema inference).
  2. Benchmark tốc độ OLAP Aggregation: Nhiều loại truy vấn tổng hợp trên Parquet.
  3. Đo tỷ lệ nén dung lượng lưu trữ (Compression Ratio).
  4. Xuất kết quả JSON phục vụ Báo cáo học thuật Chương 4.
"""

import sys
import time
import json
import pandas as pd
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
from etl.config import GOLD_DIR, DATA_DIR, RAW_DATA_FILE, RAW_FILE_ENCODING

FACT_PARQUET = GOLD_DIR / "fact_order_shipments.parquet"
GEO_PARQUET  = GOLD_DIR / "dim_geography.parquet"
OPS_PARQUET  = GOLD_DIR / "dim_shipping_ops.parquet"
DATE_PARQUET = GOLD_DIR / "dim_date.parquet"


def _time_ms(fn) -> tuple:
    """Chạy fn(), trả về (kết quả, thời gian ms)."""
    t0 = time.perf_counter()
    result = fn()
    return result, round((time.perf_counter() - t0) * 1000, 2)


def run_performance_benchmark():
    print("=" * 80)
    print(">>> BƯỚC 6: ĐO LƯỜNG HIỆU NĂNG THỰC NGHIỆM (PERFORMANCE BENCHMARK)")
    print("=" * 80)

    if not FACT_PARQUET.exists():
        raise FileNotFoundError("Chưa có dữ liệu Gold Parquet. Hãy chạy bước 03 trước!")

    results = {}

    # -------------------------------------------------------------------------
    # BENCHMARK 1: Tốc độ đọc — Raw CSV vs Parquet
    # -------------------------------------------------------------------------
    print("\n[*] Benchmark 1: Tốc độ đọc (I/O) — Raw CSV vs Parquet Columnar...")

    _, csv_read_ms = _time_ms(
        lambda: pd.read_csv(RAW_DATA_FILE, encoding=RAW_FILE_ENCODING)
    )
    _, pq_read_ms = _time_ms(
        lambda: pd.read_parquet(FACT_PARQUET)
    )

    print(f"    - Đọc Raw CSV (91 MB):          {csv_read_ms:>8.2f} ms")
    print(f"    - Đọc Fact Parquet (6 MB):       {pq_read_ms:>8.2f} ms")
    print(f"    - Tốc độ tăng (Speedup):         {csv_read_ms / max(pq_read_ms, 1):.2f}x")

    results["benchmark_1_read_io"] = {
        "raw_csv_read_ms":     csv_read_ms,
        "parquet_read_ms":     pq_read_ms,
        "speedup":             round(csv_read_ms / max(pq_read_ms, 1), 2)
    }

    # -------------------------------------------------------------------------
    # BENCHMARK 2: OLAP Aggregation — 3 loại truy vấn tổng hợp trên Parquet
    # -------------------------------------------------------------------------
    print("\n[*] Benchmark 2: OLAP Aggregation trên Parquet (3 loại truy vấn)...")

    df_f   = pd.read_parquet(FACT_PARQUET)
    df_geo = pd.read_parquet(GEO_PARQUET)
    df_ops = pd.read_parquet(OPS_PARQUET)
    df_dt  = pd.read_parquet(DATE_PARQUET)

    # Query A: Group by Market & Shipping Mode (KPI Trang 1)
    def query_a():
        m = df_f.merge(df_geo[['destination_geo_key', 'market']], on='destination_geo_key') \
                .merge(df_ops[['shipping_ops_key', 'shipping_mode']], on='shipping_ops_key')
        return m.groupby(['market', 'shipping_mode']).agg(
            total_shipments=('order_item_id', 'count'),
            total_net_sales=('net_sales', 'sum'),
            avg_late_risk=('late_delivery_risk', 'mean'),
            avg_delay_days=('delay_days', 'mean')
        ).reset_index()

    # Query B: Monthly Revenue Trend + Late Delivery Rate (KPI Trang 2)
    def query_b():
        m = df_f.merge(df_dt[['date_key', 'year', 'month']], left_on='order_date_key', right_on='date_key')
        return m.groupby(['year', 'month']).agg(
            total_net_sales=('net_sales', 'sum'),
            total_shipments=('order_item_id', 'count'),
            late_delivery_rate=('late_delivery_risk', 'mean'),
            avg_delay_days=('delay_days', 'mean')
        ).reset_index()

    # Query C: Profit Erosion Analysis — đơn hàng lỗ vốn theo Shipping Mode & Region
    def query_c():
        m = df_f.merge(df_geo[['destination_geo_key', 'market', 'order_region']], on='destination_geo_key') \
                .merge(df_ops[['shipping_ops_key', 'shipping_mode']], on='shipping_ops_key')
        loss = m[m['is_loss_order'] == 1]
        return loss.groupby(['market', 'shipping_mode']).agg(
            loss_order_count=('order_item_id', 'count'),
            total_loss_amount=('net_profit', 'sum'),
            avg_discount_rate=('discount_rate', 'mean')
        ).reset_index().sort_values('total_loss_amount')

    res_a, qa_ms = _time_ms(query_a)
    res_b, qb_ms = _time_ms(query_b)
    res_c, qc_ms = _time_ms(query_c)

    print(f"    - Query A (KPI Overview - 20 nhóm):      {qa_ms:>8.2f} ms")
    print(f"    - Query B (Monthly Trend - {len(res_b)} tháng):       {qb_ms:>8.2f} ms")
    print(f"    - Query C (Profit Loss - {len(res_c)} nhóm):         {qc_ms:>8.2f} ms")

    results["benchmark_2_olap_queries"] = {
        "query_A_kpi_overview_ms":      qa_ms,
        "query_B_monthly_trend_ms":     qb_ms,
        "query_C_profit_erosion_ms":    qc_ms,
        "avg_query_ms":                 round((qa_ms + qb_ms + qc_ms) / 3, 2)
    }

    # -------------------------------------------------------------------------
    # BENCHMARK 3: Dung lượng lưu trữ (Storage Footprint)
    # -------------------------------------------------------------------------
    print("\n[*] Benchmark 3: Tối ưu hóa dung lượng lưu trữ (Storage Compression)...")

    raw_mb  = round(RAW_DATA_FILE.stat().st_size / (1024 * 1024), 2)
    pq_bytes = sum(f.stat().st_size for f in GOLD_DIR.glob("*.parquet"))
    pq_mb   = round(pq_bytes / (1024 * 1024), 2)
    ratio   = round(raw_mb / pq_mb, 2)

    print(f"    - Raw CSV (1 file flat):                {raw_mb:>6.2f} MB")
    print(f"    - Gold Star Schema Parquet (6 tables):  {pq_mb:>6.2f} MB")
    print(f"    - Tỷ lệ nén (Compression Ratio):        {ratio}x")

    results["benchmark_3_storage"] = {
        "raw_csv_mb":        raw_mb,
        "gold_parquet_mb":   pq_mb,
        "compression_ratio": f"{ratio}x",
        "storage_saved_pct": f"{round((1 - pq_mb / raw_mb) * 100, 1)}%"
    }

    # -------------------------------------------------------------------------
    # XUẤT KẾT QUẢ
    # -------------------------------------------------------------------------
    summary = {
        "dataset_rows": 180_519,
        **results,
        "conclusion": (
            f"Đọc Parquet nhanh hơn CSV {results['benchmark_1_read_io']['speedup']}x. "
            f"Lưu trữ Columnar Parquet (Star Schema) tiết kiệm "
            f"{results['benchmark_3_storage']['storage_saved_pct']} dung lượng so với Raw CSV."
        )
    }

    out_file = GOLD_DIR / "benchmark_performance_report.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=4, ensure_ascii=False)

    print(f"\n[✓] HOÀN TẤT BƯỚC 6: Báo cáo Benchmark → {out_file.name}")
    print(f"    {summary['conclusion']}")
    return summary


if __name__ == "__main__":
    run_performance_benchmark()
