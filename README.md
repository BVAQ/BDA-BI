# DataCo Global Supply Chain — BI & Big Data Analytics

**Hệ thống BI & Big Data Analytics giám sát hiệu suất vận tải chuỗi cung ứng toàn cầu và dự báo rủi ro trễ hạn giao hàng**
*Global Supply Chain Logistics Performance & Late Delivery Risk Predictive BI System*

Bài tập lớn môn **CO4033 — Big Data Analytics and Business Intelligence**, Khoa Khoa học & Kỹ thuật Máy tính, Trường ĐH Bách Khoa — ĐHQG-HCM.

| | |
| :--- | :--- |
| **Dataset** | [DataCo Smart Supply Chain for Big Data Analysis](https://data.mendeley.com/datasets/8gx2fvg2k6/5): 180.519 dòng vận đơn × 53 thuộc tính, 65.752 đơn hàng, 164 quốc gia, 2015–2018 |
| **Kiến trúc** | Medallion Lakehouse (Bronze → Silver → Gold) · Kimball Star Schema · Google BigQuery · PySpark |
| **Phân tích** | Descriptive · Diagnostic (OLAP) · Predictive (ML) · Prescriptive |
| **BI** | Power BI, 4 trang dashboard, 8 tính năng (F1–F8) |

---

## Thành viên & phân công

| Thành viên | Vai trò | Dashboard | Features |
| :--- | :--- | :--- | :--- |
| Quốc Khánh | Data Platform & ETL Lead | Trang 1: Executive KPI & Global Overview | F1, F2 |
| Vinh Khánh | OLAP & Diagnostic Analysis | Trang 2: Multi-Dimensional OLAP & SLA Diagnostic | F3, F4 |
| Quân | Predictive Analytics & ML | Trang 3: Predictive Delay Risk & What-If Simulator | F5, F6 |
| Vương | Prescriptive Strategy & Report | Trang 4: Actionable Strategy & Profit Optimization | F7, F8 |

---

## Kiến trúc

```
DataCoSupplyChainDataset.csv (96 MB, ISO-8859-1)
        │  step01  Ingestion & profiling
        ▼
 🥉 Bronze  data/bronze/bronze_raw_data.parquet
        │  step02  Làm sạch, sửa Unicode, bỏ cột trùng/rỗng/PII, feature engineering
        │  step02b (PySpark, broadcast join)   step02c  Quality gate: 43 kiểm tra
        ▼
 🥈 Silver  data/silver/silver_cleaned_data.parquet
        │  step03  Kimball Star Schema (1 fact + 5 dimensions)
        ▼
 🥇 Gold    data/gold/*.parquet  ──step04──►  ☁️ BigQuery (partition + cluster)
        │                                         │ step05  4 semantic views
        │  step06  Benchmark                      ▼
        │  step07  Predictive ML ──► data/ml/ ──► 📊 Power BI (4 trang × 2 features)
```

## Cấu trúc thư mục

```
├── etl/                         # Pipeline ETL (step01 → step06), config tập trung
├── ml/                          # Bước 7: mô hình dự báo trễ hạn pre-shipment (Trang 3)
├── dashboard/                   # Power BI Project (.pbip) cho Trang 3 + script sinh tự động + theme
├── report/                      # Báo cáo LaTeX (XeLaTeX)
├── data/
│   ├── DataCoSupplyChainDataset.csv   # dữ liệu gốc
│   ├── bronze/ silver/ gold/          # các tầng Medallion
│   └── ml/                            # kết quả ML: bảng cho Power BI, mô hình, hình vẽ
├── run_pipeline.py              # Orchestrator CLI
├── etl_dataco_pipeline.ipynb    # Notebook chạy pipeline trên Google Colab
├── deploy_to_bigquery.ipynb     # Notebook nạp Gold + tạo views lên BigQuery
├── LOGISTICS_BI_PROJECT_SPEC.md       # Đặc tả dự án
├── ETL_DATABASE_EXECUTION_PLAN.md     # Kỹ thuật ETL & DWH
└── PREDICTIVE_ML_MODEL.md             # Kỹ thuật mô hình dự báo & dashboard Trang 3
```

---

## Cài đặt

Yêu cầu: Python 3.10+. Bước 2b (PySpark) cần thêm Java 11+.

```bash
pip install pandas pyarrow
pip install -r requirements-ml.txt          # scikit-learn, xgboost, lightgbm, matplotlib, joblib
pip install pyspark                         # (tuỳ chọn) bước 2b
pip install google-cloud-bigquery db-dtypes # (tuỳ chọn) bước 4–5 nạp lên BigQuery
```

## Chạy pipeline

```bash
python run_pipeline.py              # toàn bộ ETL: 1 → 2 → 2c → 3 → 4 → 5 → 6
python run_pipeline.py --step 2b    # Silver bằng PySpark
python run_pipeline.py --step 7     # mô hình dự báo (~3 phút)
```

| Step | Module | Output |
| :---: | :--- | :--- |
| 1 | `etl/step01_extract_bronze.py` | `data/bronze/` |
| 2 | `etl/step02_transform_silver.py` | `data/silver/` |
| 2b | `etl/step02b_transform_silver_pyspark.py` | `data/silver/silver_pyspark/` |
| 2c | `etl/step02c_validate_quality.py` | `data/gold/data_quality_report.json` (41 PASS / 0 FAIL / 2 WARN) |
| 3 | `etl/step03_model_gold.py` | `data/gold/*.parquet` |
| 4 | `etl/step04_load_bigquery.py` | BigQuery `dataco_logistics_gold.*` (dry-run nếu chưa có credentials) |
| 5 | `etl/step05_create_views.py` | 4 semantic views |
| 6 | `etl/step06_benchmark.py` | `data/gold/benchmark_performance_report.json` |
| 7 | `ml/train_predictive.py` | `data/ml/` |

Trên Windows, nếu console báo lỗi Unicode thì đặt `PYTHONIOENCODING=utf-8` trước khi chạy.

---

## Kết quả nổi bật

**Dữ liệu & DWH:** Parquet đọc nhanh hơn CSV **30,8×**; Star Schema nén **14,3×** (91,5 MB → 6,4 MB); truy vấn OLAP mất 34–62 ms.

**Mô hình dự báo trễ hạn pre-shipment (Trang 3)**, đánh giá trên tập test out-of-time (đơn từ 01/07/2017):

| Mô hình | Accuracy | Precision | Recall | ROC-AUC |
| :--- | ---: | ---: | ---: | ---: |
| Baseline (tỷ lệ trễ theo mode) | 69,9% | 89,1% | 54,5% | 0,750 |
| Logistic Regression | 71,0% | 89,4% | 56,3% | 0,754 |
| **Decision Tree** (được chọn) | **73,1%** | **90,0%** | **60,0%** | **0,788** |
| Random Forest / XGBoost / LightGBM | ~72–73% | | | 0,783–0,786 |
| *Trần lý thuyết (Bayes-optimal)* | *73,1%* | *90,0%* | *60,0%* | *0,781* |

- Trễ hạn do **thiết kế SLA** quyết định: First Class luôn giao sau 2 ngày (SLA 1 ngày) nên trễ 100%; Same Day đặt sau 12:00 thì trễ; Second/Standard có ngày giao ngẫu nhiên đều 2–6 ngày.
- Mô hình đạt khoảng 100% trần lý thuyết. Khu vực, ngành hàng và chiết khấu không ảnh hưởng.
- Đề xuất tái cấu trúc SLA (First 1→2, Second 2→4, Standard 4→5, cut-off 12:00 cho Same Day) giúp OTD tăng từ ~43% lên ~80%.

Chi tiết: [`PREDICTIVE_ML_MODEL.md`](PREDICTIVE_ML_MODEL.md).

---

## Dashboard Power BI (Trang 3)

```bash
python run_pipeline.py --step 7       # sinh dữ liệu data/ml/
python dashboard/build_pbip.py        # sinh dashboard/Trang3_PredictiveRisk.pbip
```

Mở `dashboard/Trang3_PredictiveRisk.pbip` bằng **Power BI Desktop**, rồi bấm **Refresh**.

- 3 trang tiếng Anh: *3.1 Model Evaluation*, *3.2 Risk Scoring*, *3.3 What-If Simulator*.
- Theme **DataCo Logistics Harbor**: [`dashboard/themes/DataCo_Harbor_Theme.json`](dashboard/themes/DataCo_Harbor_Theme.json).
- Đường dẫn dữ liệu nằm trong tham số Power Query `DataFolder`. Trên máy khác, chạy `python dashboard/build_pbip.py --data-folder "<path>/data/ml"` hoặc sửa trong *Transform data → Manage parameters*.
- ⚠️ `build_pbip.py` ghi đè phần report. Nếu đã chỉnh layout bằng tay trong Power BI Desktop thì chỉ cần Save, không chạy lại script.

---

## Báo cáo LaTeX

Thư mục [`report/`](report/), biên dịch bằng **XeLaTeX**:

```bash
cd report
latexmk -xelatex main.tex
```

Trên **Overleaf**: upload thư mục `report/`, chọn *Menu → Compiler → XeLaTeX*.

| File | Nội dung |
| :--- | :--- |
| `main.tex` | Cấu hình, trang bìa, mục lục, danh mục hình/bảng |
| `chapters/00_*.tex` | Bìa, lời cảm ơn, tóm tắt, phân công, từ viết tắt |
| `chapters/01`–`09_*.tex` | Giới thiệu → Cơ sở lý thuyết → Dữ liệu → Kiến trúc → Trang 1–2 → **Trang 3 (đã viết)** → Trang 4 → Benchmark → Kết luận |
| `chapters/10_appendix.tex` | Phụ lục |
| `references.bib` | Tài liệu tham khảo (IEEE) |
| `figures/` | Hình (`ml/` đã có sẵn; đặt ảnh dashboard vào `dashboard/`, logo vào `hcmut_logo.png`) |

Các chỗ cần điền được đánh dấu `\todo{...}` (hiện chữ đỏ trong PDF). Xoá hết trước khi nộp.

---

## Tài liệu

- [`LOGISTICS_BI_PROJECT_SPEC.md`](LOGISTICS_BI_PROJECT_SPEC.md): đặc tả dự án, pain points, 8 tính năng
- [`ETL_DATABASE_EXECUTION_PLAN.md`](ETL_DATABASE_EXECUTION_PLAN.md): kiến trúc ETL, Star Schema, BigQuery, DAX Trang 1
- [`PREDICTIVE_ML_MODEL.md`](PREDICTIVE_ML_MODEL.md): mô hình dự báo, kết quả, hướng dẫn dashboard Trang 3
