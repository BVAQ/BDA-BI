# ĐẶC TẢ DỰ ÁN HỆ THỐNG BUSINESS INTELLIGENCE & BIG DATA ANALYTICS (CO4033)

## 1. THÔNG TIN CHUNG DỰ ÁN

| Mục | Nội dung |
| :--- | :--- |
| **Tên đề tài** | Xây dựng Hệ thống BI & Big Data Analytics Giám sát Hiệu suất Vận tải Chuỗi Cung ứng Toàn cầu và Dự báo Rủi ro Trễ hạn Giao hàng |
| **Tên tiếng Anh** | *Global Supply Chain Logistics Performance & Late Delivery Risk Predictive BI System* |
| **Môn học** | Big Data Analytics and Business Intelligence (`CO4033` — Khoa KH&KT Máy tính, ĐH Bách Khoa ĐHQG-HCM) |
| **Dataset chính** | `DataCo SMART SUPPLY CHAIN FOR BIG DATA ANALYSIS` (`data/DataCoSupplyChainDataset.csv`) |
| **Quy mô dữ liệu** | **180.519 bản ghi** × **53 thuộc tính** (~95.9 MB raw), trải dài **65.752 đơn hàng**, **20.652 khách hàng** tại **164 quốc gia / 23 khu vực**, **5 thị trường châu lục** trong giai đoạn **2015 – 2018** |

---

## 2. PHÂN CÔNG TRÁCH NHIỆM (4 THÀNH VIÊN — 4 TRANG DASHBOARD)

| Thành viên | Vai trò | Trang Dashboard | Features |
| :--- | :--- | :--- | :--- |
| **Quốc Khánh** | Data Platform & ETL Lead | **Trang 1:** Executive KPI & Global Logistics Overview | Feature 1 & 2 |
| **Vinh Khánh** | OLAP & Diagnostic Analysis | **Trang 2:** Multi-Dimensional OLAP & SLA Diagnostic | Feature 3 & 4 |
| **Quân** | Predictive Analytics & ML | **Trang 3:** Predictive Delay Risk & What-If Simulator | Feature 5 & 6 |
| **Vương** | Prescriptive Strategy & Report | **Trang 4:** Actionable Strategy & Profit Optimization | Feature 7 & 8 |

> [!NOTE]
> **Quốc Khánh** đảm nhận toàn bộ hạ tầng dữ liệu (Pipeline ETL, BigQuery Cloud DWH, 4 Semantic Views) và bàn giao cho 3 thành viên còn lại kết nối Power BI. Chi tiết kỹ thuật xem tại [`ETL_DATABASE_EXECUTION_PLAN.md`](./ETL_DATABASE_EXECUTION_PLAN.md).

---

## 3. BỐI CẢNH DOANH NGHIỆP & ĐẶC TRƯNG 5Vs BIG DATA

Dự án giải quyết bài toán vận hành chuỗi cung ứng toàn cầu của tập đoàn thương mại & phân phối đa quốc gia **DataCo Global**. Hàng hóa thuộc **11 ngành hàng (Departments)** và **51 danh mục (Categories)** được điều phối từ hệ thống kho trung tâm (Distribution Hubs tại Bắc Mỹ / Puerto Rico) đi khắp **5 châu lục** qua 4 phương thức dịch vụ vận chuyển: `Standard Class` (SLA 4 ngày), `Second Class` (SLA 2 ngày), `First Class` (SLA 1 ngày), và `Same Day` (SLA trong ngày).

### Đặc trưng 5Vs của Hệ thống Big Data trong Dự án

| V | Đặc trưng | Biểu hiện trong DataCo Dataset |
| :---: | :--- | :--- |
| **Volume** | Khối lượng lớn | 180.519 bản ghi chi tiết dòng đơn hàng × 53 chiều (~96 MB raw), tạo ra hàng triệu điểm dữ liệu khi kết hợp đa chiều theo thời gian, địa lý, sản phẩm và phương thức vận tải |
| **Velocity** | Tốc độ phát sinh | Dữ liệu đơn hàng được ghi nhận liên tục theo từng phút (Timestamp `YYYY-MM-DD HH:MM`), đòi hỏi kiến trúc xử lý phân tán và truy vấn OLAP tốc độ cao để cảnh báo trễ hạn tại thời điểm tạo đơn |
| **Variety** | Đa dạng kiểu dữ liệu | Kết hợp dữ liệu tài chính (`Sales`, `Discount`, `Benefit`), vận hành thời gian thực (`Scheduled vs. Real Shipping Days`), phân cấp địa lý & tọa độ GPS (`Latitude/Longitude`, `Market`, `Region`, `Country`, `City`) và thông tin định danh khách hàng |
| **Veracity** | Độ tin cậy & Nhiễu | Lỗi bảng mã Latin-1 (6 thành phố sai ký tự), 5 cặp cột trùng lặp 100%, cột rỗng hoàn toàn (`Product Description` 100% null, `Order Zipcode` 86.24% null), nguy cơ Target Leakage trong ML |
| **Value** | Giá trị kinh doanh | Chuyển hóa dữ liệu thô thành quyết sách giúp cắt giảm tỷ lệ trễ hạn (hiện **54.83%**) và ngăn thất thoát từ đơn hàng âm lợi nhuận (**-\$3.88 triệu USD**) |

---

## 4. BÀI TOÁN THỰC TẾ & NỖI ĐAU DOANH NGHIỆP (BUSINESS PAIN POINTS)

Qua khảo sát dữ liệu thực tế trên 180.519 vận đơn (tổng doanh thu gộp **\$36.78M USD**, doanh thu thuần **\$33.05M USD**, lợi nhuận ròng **\$3.97M USD**), DataCo Global đối mặt với **3 thách thức sống còn**:

### Pain Point 1 — Khủng hoảng Trễ hạn Giao hàng (54.83%) & "Nghịch lý SLA"

Có tới **98.977 / 180.519 vận đơn (54.83%)** bị giao trễ so với cam kết (`late_delivery_risk = 1`). Nghịch lý đặc biệt: **các gói dịch vụ cao cấp có tỷ lệ vỡ cam kết cao nhất**:

| Shipping Mode | SLA | Số đơn | Tỷ lệ trễ hạn |
| :--- | :---: | ---: | ---: |
| First Class | 1 ngày | 27.814 | **95.32%** ⚠️ |
| Second Class | 2 ngày | 35.216 | **76.63%** ⚠️ |
| Standard Class | 4 ngày | 107.666 | 38.07% |
| Same Day | 0 ngày | 9.823 | 65.03% |

### Pain Point 2 — Xói mòn Lợi nhuận (-\$3.88M) & Rủi ro Gian lận

- **33.784 dòng đơn hàng (18.71%)** ghi nhận lợi nhuận âm → thất thoát **-\$3.88 triệu USD**
- **4.062 vận đơn** ở trạng thái `SUSPECTED_FRAUD` và **7.754 vận đơn** bị hủy giao hàng

### Pain Point 3 — Quản trị Bị động, Thiếu Cảnh báo Sớm

Ban điều hành chỉ biết đơn hàng bị trễ **sau khi sự cố đã xảy ra**. Doanh nghiệp cần một mô hình ML dự báo xác suất trễ hạn **ngay tại thời điểm khởi tạo đơn hàng (Pre-shipment)** để chủ động điều chỉnh phương thức vận chuyển.

---

## 5. MỤC TIÊU HỆ THỐNG & 8 TÍNH NĂNG DASHBOARD (RUBRIC LEVEL 5: >6 FEATURES)

Để đạt chuẩn xuất sắc (Level 5: 8.5–10 điểm — *"Excellent application with more than 6 features & beautiful modern GUI"*), hệ thống BI được thiết kế với **4 Trang Dashboard chuyên sâu** tích hợp **8 tính năng nghiệp vụ cao cấp**:

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│ TRANG 1: EXECUTIVE KPI & GLOBAL LOGISTICS OVERVIEW  (Quốc Khánh)                        │
│ Feature 1: Dynamic KPI Scorecard & Target Pacing ($33.05M, OTD%, Net Profit...)         │
│ Feature 2: Interactive Global Geospatial Supply Chain Map (164 quốc gia, 5 châu lục)    │
└────────────────────────────────────────────┬─────────────────────────────────────────────┘
                                             ▼
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│ TRANG 2: MULTI-DIMENSIONAL OLAP & SLA DIAGNOSTIC  (Vinh Khánh)                          │
│ Feature 3: AI-Powered Root-Cause Decomposition Tree (cây chẩn đoán nguyên nhân trễ hạn) │
│ Feature 4: Shipping Mode SLA Paradox & Lead-Time Variance Matrix                        │
└────────────────────────────────────────────┬─────────────────────────────────────────────┘
                                             ▼
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│ TRANG 3: PREDICTIVE DELAY RISK & ML EARLY-WARNING  (Quân)                               │
│ Feature 5: Pre-Shipment ML Risk Scoring & Feature Importance (Dự báo trễ hạn bằng ML)   │
│ Feature 6: Interactive What-If Order Delay Simulator (Mô phỏng kịch bản đơn hàng)       │
└────────────────────────────────────────────┬─────────────────────────────────────────────┘
                                             ▼
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│ TRANG 4: ACTIONABLE STRATEGY & PROFIT OPTIMIZATION  (Vương)                             │
│ Feature 7: Profit Erosion & Fraud Anomaly Radar (Kiểm soát đơn lỗ -$3.88M & Fraud)      │
│ Feature 8: SLA Re-calibration & Financial Impact Calculator                             │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

### Bảng Đặc tả 8 Tính năng Dashboard

| Feature | Tên tính năng | Trang | Mô tả kỹ thuật & giá trị nghiệp vụ |
| :---: | :--- | :---: | :--- |
| **F1** | Dynamic Executive KPI Cards & YoY/MoM Sparklines | 1 | Hiển thị 5 chỉ số cốt lõi (`Net Sales $33.05M`, `Net Profit $3.97M`, `Shipments 180.5K`, `OTD 45.17%`, `Avg Delay +0.57 Days`) kèm biểu đồ xu hướng thu nhỏ và cảnh báo màu tự động theo ngưỡng SLA 80% |
| **F2** | Interactive Global Geospatial Map | 1 | Bản đồ tương tác phân bố đơn hàng & rủi ro trễ hạn tại 164 quốc gia; hỗ trợ Drill-down `Market → Region → Country → City` |
| **F3** | AI-Powered Root-Cause Decomposition Tree | 2 | Cây phân rã đa chiều (OLAP Drill-through) chẩn đoán tự động yếu tố đóng góp vào 98.977 đơn trễ hạn (`Shipping Mode → Region → Department → Payment Type`) |
| **F4** | SLA Paradox & Lead-Time Variance Matrix | 2 | Ma trận so sánh `Scheduled Days` vs. `Real Days` phơi bày điểm nghẽn tại First Class (trễ 95.32%) và Second Class (trễ 76.63%) |
| **F5** | ML Model Evaluation & Driver Attribution Hub | 3 | Trực quan hóa so sánh các mô hình ML (Logistic Regression, Random Forest, XGBoost / BigQuery ML) qua Confusion Matrix, ROC-AUC và Feature Importance |
| **F6** | Interactive What-If Order Delay Simulator | 3 | Mô phỏng dùng What-If Parameters: điều phối viên chọn `Shipping Mode`, `Region`, `Discount Rate`, `Quantity` → hệ thống tính tức thì xác suất trễ hạn |
| **F7** | Profit Erosion & Fraud Anomaly Radar | 4 | Phân tích Pareto (80/20) truy vết 33.784 đơn lỗ vốn (**-\$3.88M**) theo Discount Rate & ngành hàng, kết hợp bộ lọc 4.062 đơn `SUSPECTED_FRAUD` theo khu vực |
| **F8** | SLA Re-calibration & Financial Impact Calculator | 4 | Mô hình tính toán hiệu quả kinh tế khi tái cấu trúc SLA (ví dụ: điều chỉnh First Class từ 1 → 2 ngày) giúp nâng OTD từ 45.17% lên >82% |

---

## 6. ĐỊNH HƯỚNG BÁO CÁO & THUYẾT TRÌNH ĐẠT 10/10 (RUBRIC ALIGNMENT)

### Big Data Analytics (4.5/10 điểm)

| Tiêu chí | Điểm | Giải pháp đáp ứng |
| :--- | :---: | :--- |
| **Preprocessing** | 1.5đ | Xử lý triệt để lỗi Latin-1, 5 cặp cột trùng lặp, PII masking, Target Leakage prevention, Data Quality Validation tự động (43 assertions — 41 PASS) |
| **Storage** *(+ Bonus "Excellent data architecture")* | 1.5đ | Medallion Lakehouse (Bronze→Silver→Gold) + Kimball Star Schema trên Google BigQuery với `PARTITION BY DATE` và `CLUSTER BY shipping_ops_key` |
| **Processing** *(+ Bonus "Excellent processing architecture")* | 1.5đ | Pipeline 8 modules độc lập (SRP), Apache PySpark Distributed Processing với Broadcast Join, Adaptive Query Execution |

### Business Intelligence (3.0/10 điểm)

4 trang Dashboard Power BI với **8 tính năng tương tác** (>6 theo Level 5 Rubric), giao diện Modern Dark UI, App Navigation Bar, Bookmarks, Drill-through và What-If Sliders.

### Report & Presentation (2.5/10 điểm + Bonus)

Báo cáo đầy đủ: lý thuyết nền tảng (5Vs, Medallion, Kimball vs. Inmon, OLAP Operations, Descriptive/Predictive/Prescriptive Analytics), Benchmark thực nghiệm so sánh công nghệ, Mục lục, Danh mục Hình/Bảng, Từ viết tắt, Tài liệu tham khảo chuẩn IEEE và Phụ lục.

---

*Chi tiết kỹ thuật về kiến trúc ETL, Star Schema trên BigQuery, PySpark Pipeline, Benchmark và DAX xem tại [`ETL_DATABASE_EXECUTION_PLAN.md`](./ETL_DATABASE_EXECUTION_PLAN.md).*
