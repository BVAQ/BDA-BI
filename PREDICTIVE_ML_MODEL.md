# MÔ HÌNH DỰ BÁO RỦI RO TRỄ HẠN PRE-SHIPMENT — TRANG 3 (QUÂN)

> **Phạm vi:** Feature 5 (Pre-Shipment ML Risk Scoring & Driver Attribution) và Feature 6 (What-If Order Delay Simulator)
> **Code:** [`ml/`](./ml/) · **Chạy:** `python run_pipeline.py --step 7` · **Output:** [`data/ml/`](./data/ml/)
> **Đầu vào:** 6 bảng Gold Star Schema ([`data/gold/`](./data/gold/)), cũng là dữ liệu đang có trên BigQuery `dataco_logistics_gold`

---

## 0. TÓM TẮT KẾT QUẢ

| | Giá trị |
| :--- | :--- |
| **Bài toán** | Phân loại nhị phân `late_delivery_risk`, chỉ dùng thông tin có **tại thời điểm tạo đơn** |
| **Mô hình được chọn** | **Decision Tree** (max_depth=6), chọn theo CV ROC-AUC, không nhìn tập test |
| **Test out-of-time** (đơn từ 01/07/2017) | Accuracy **73,08%** · Precision **90,03%** · Recall **59,97%** · F1 **0,720** · ROC-AUC **0,788** [0,782–0,792] |
| **So với baseline** (tỷ lệ trễ lịch sử theo shipping mode) | ROC-AUC 0,750 → 0,788; Accuracy 69,9% → 73,1% |
| **So với trần lý thuyết** | CV AUC đạt **100%** trần Bayes-optimal; Accuracy/Precision/Recall/F1 **trùng khớp** trần |
| **Yếu tố quyết định** | `shipping_mode` (ΔAUC 0,286) ≫ `order_hour` (0,055) ≫ các yếu tố còn lại (< 0,005, coi như nhiễu) |
| **Thí nghiệm Leakage** | Thêm `days_shipping_real` → AUC = **1,000**, Accuracy = **100%**: "quá đẹp để là thật", vô dụng khi triển khai |

**Thông điệp chính cho báo cáo:** Trễ hạn ở DataCo **không phải ngẫu nhiên theo khu vực hay sản phẩm**. Nó do **thiết kế SLA** quyết định: First Class luôn trễ, Same Day trễ khi đặt sau 12:00, còn Second/Standard trễ theo một tỷ lệ cố định mà không feature nào dự báo thêm được. Vì vậy hành động đúng là **tái cấu trúc SLA và cut-off time** (liên kết sang Feature 8 của Vương), chứ không phải "tìm thêm feature".

---

## 1. PHÂN TÍCH BÀI TOÁN

### 1.1. Định nghĩa
- **Target:** `late_delivery_risk` = 1 nếu `days_shipping_real > days_shipment_scheduled`
- **Grain:** 1 dòng = 1 order item (180.519 dòng, 65.752 đơn)
- **Thời điểm dự báo:** ngay khi khách bấm đặt hàng (pre-shipment). Lúc này chưa biết ngày xuất kho, ngày giao hay trạng thái giao.

### 1.2. Chống Target Leakage: các cột bị loại và lý do

| Cột | Lý do loại |
| :--- | :--- |
| `days_shipping_real`, `delay_days`, `shipping_date_ts`, `ship_date_key` | Chỉ biết **sau** khi giao; target được suy ra trực tiếp từ các cột này |
| `delivery_status` | `'Late delivery'` ⇔ target = 1 (đã kiểm chứng: crosstab chéo tuyệt đối) |
| `shipping_ops_key` | Junk Dimension **gộp cả `delivery_status`**, dùng key này tức là mã hóa ngầm đáp án |
| `order_status` | `CANCELED` / `SUSPECTED_FRAUD` ⇒ target luôn bằng 0; là trạng thái chụp sau sự kiện |
| `net_profit`, `profit_ratio`, `is_loss_order`, `is_canceled_or_fraud` | Số liệu hậu kiểm tài chính / trạng thái sau xử lý |

> [!WARNING]
> View `vw_ml_pre_shipment` có **2 phiên bản khác nhau** (trong [`etl/step05_create_views.py`](./etl/step05_create_views.py) và trong [`deploy_to_bigquery.ipynb`](./deploy_to_bigquery.ipynb)). Vì vậy pipeline ML **đọc thẳng từ các bảng Gold** và tự join, để không phụ thuộc vào việc bản view nào được deploy sau cùng. Danh sách feature chuẩn nằm ở [`ml/ml_config.py`](./ml/ml_config.py).

### 1.3. Feature pre-shipment (17 biến)

| Nhóm | Feature |
| :--- | :--- |
| Vận chuyển & thanh toán | `shipping_mode`, `payment_type` |
| Địa lý đích | `market`, `order_region` |
| Khách hàng / sản phẩm | `customer_segment`, `department_name`, `category_name` |
| Thời điểm đặt | `order_hour`, `order_dayofweek`, `order_month`, `is_weekend`, `is_peak_season` |
| Giỏ hàng | `order_item_quantity`, `product_price`, `discount_rate`, `net_sales`, `items_in_order` |

`order_country` (164 giá trị) không đưa vào vì `order_region` đã thể hiện cùng thông tin với số chiều nhỏ hơn. `days_shipment_scheduled` / `default_sla_days` cũng không đưa vào vì chúng là hàm 1-1 của `shipping_mode`.

### 1.4. Phát hiện then chốt: cơ chế sinh dữ liệu (Data-Generating Process)

Khảo sát dữ liệu Silver/Gold (không tính 7.754 đơn hủy) cho thấy:

| Shipping Mode | SLA | Ngày giao thực tế | Tỷ lệ trễ | Dự báo được? |
| :--- | :---: | :--- | :---: | :--- |
| First Class | 1 | **Luôn = 2 ngày** | **100%** | ✅ Chắc chắn trễ |
| Same Day | 0 | 0 nếu đặt **trước 12:00**, 1 nếu đặt **từ 12:00** | 47,9% | ✅ Hoàn hảo theo `order_hour` |
| Second Class | 2 | **Đều ngẫu nhiên 2–6 ngày** (mỗi mức ≈ 20%) | 79,8% | ⚠️ Chỉ biết xác suất 0,8 |
| Standard Class | 4 | **Đều ngẫu nhiên 2–6 ngày** (mỗi mức ≈ 20%) | 39,8% | ⚠️ Chỉ biết xác suất 0,4 |
| *(Đơn hủy)* | | | luôn 0 | Loại khỏi train/eval |

Hình minh họa: [`data/ml/figures/06_data_generating_process.png`](./data/ml/figures/06_data_generating_process.png)

Kiểm định bổ sung: bên trong Second/Standard, mọi feature (giờ, thứ, tháng, khu vực, thanh toán, phân khúc, ngành hàng, chiết khấu, số lượng) đều có chi-square ≈ bậc tự do, và AUC out-of-time của từng feature ∈ [0,48; 0,51]. Nghĩa là **không có tín hiệu**.

⇒ **Trần lý thuyết (Bayes-optimal)**: P(late) = 1 (First), 1[hour ≥ 12] (Same Day), 0,8 (Second), 0,4 (Standard). Không mô hình trung thực nào có thể vượt **có hệ thống** mức này. Code: `oracle_probability()` trong [`ml/data_prep.py`](./ml/data_prep.py).

> [!NOTE]
> **Sửa số liệu trong spec:** [`LOGISTICS_BI_PROJECT_SPEC.md`](./LOGISTICS_BI_PROJECT_SPEC.md) ghi Same Day trễ **65,03%**, nhưng dữ liệu thực cho **45,74%** (tính cả đơn hủy) hoặc 47,9% (không tính đơn hủy). Nên báo các bạn cập nhật lại.

---

## 2. THIẾT KẾ THỰC NGHIỆM

| Hạng mục | Lựa chọn | Lý do |
| :--- | :--- | :--- |
| Chia dữ liệu | **Out-of-time**: train < 01/07/2017 (149.478 dòng) · test ≥ 01/07/2017 (23.287 dòng) | Mô phỏng triển khai thật: học quá khứ, dự báo tương lai |
| Cross-validation | **5-fold StratifiedGroupKFold theo `order_id`** trên tập train | Các item của cùng 1 đơn có chung nhãn; nếu chia ngẫu nhiên theo dòng sẽ rò rỉ giữa các fold |
| Đơn hủy | Loại khỏi train/eval (vẫn được chấm điểm trong bảng predictions, gắn cờ `eval_eligible = 0`) | Đơn hủy luôn có nhãn 0 bất kể mode, nên là nhiễu chứ không phải "giao đúng hạn" |
| Tiền xử lý | Nằm **trong** sklearn Pipeline (One-Hot + Scaler cho LR; Ordinal cho mô hình cây) | Không rò thống kê từ tập test; lưu và nạp chỉ cần 1 file `.joblib` |
| Chọn mô hình | CV ROC-AUC cao nhất (làm tròn 3 chữ số), nếu hòa thì chọn CV Brier thấp hơn | Không dùng tập test để chọn, tránh optimistic bias |
| Ngưỡng | 0,5 (mặc định); bảng quét ngưỡng 0,05–0,95 cho Power BI | Người dùng tự chỉnh trade-off precision/recall |

### Các mô hình so sánh
1. **Baseline_ModeRate**: xác suất = tỷ lệ trễ lịch sử của shipping mode (heuristic nghiệp vụ, không ML)
2. **Logistic Regression**: tuyến tính, dễ giải thích
3. **Decision Tree** (depth 6): luật IF-THEN đọc được ([`data/ml/decision_tree_rules.txt`](./data/ml/decision_tree_rules.txt))
4. **Random Forest** (200 cây): bagging
5. **XGBoost** (300 cây): gradient boosting
6. **LightGBM** (300 cây): gradient boosting dạng histogram
7. *Theoretical_Ceiling*: mốc tham chiếu Bayes-optimal, không phải mô hình

---

## 3. KẾT QUẢ CHI TIẾT

### 3.1. Bảng so sánh (tập test out-of-time, ngưỡng 0,5)

| Mô hình | Accuracy | Precision | Recall | F1 | ROC-AUC [95% CI] | PR-AUC | Brier | CV AUC | % trần (CV) |
| :--- | ---: | ---: | ---: | ---: | :---: | ---: | ---: | ---: | ---: |
| Baseline_ModeRate | 69,90% | 89,13% | 54,45% | 0,676 | 0,750 [0,745–0,755] | 0,791 | 0,186 | 0,740 | 87,9% |
| Logistic Regression | 70,98% | 89,44% | 56,33% | 0,691 | 0,754 [0,748–0,760] | 0,844 | 0,187 | 0,744 | 89,3% |
| **Decision Tree** ⭐ | **73,08%** | **90,03%** | **59,97%** | **0,720** | **0,788** [0,782–0,792] | 0,853 | **0,172** | **0,774** | **100,3%** |
| Random Forest | 73,06% | 89,92% | 60,02% | 0,720 | 0,784 [0,778–0,789] | 0,863 | 0,176 | 0,771 | 98,9% |
| XGBoost | 72,41% | 86,59% | 61,71% | 0,721 | 0,783 [0,778–0,790] | 0,863 | 0,173 | 0,770 | 98,8% |
| LightGBM | 72,84% | 88,47% | 60,84% | 0,721 | 0,786 [0,781–0,792] | 0,864 | 0,173 | 0,771 | 99,0% |
| *Theoretical_Ceiling* | *73,08%* | *90,03%* | *59,97%* | *0,720* | *0,781* [0,776–0,785] | *0,808* | *0,172* | *0,774* | *100%* |

*(Số liệu lấy từ [`data/ml/ml_model_comparison.csv`](./data/ml/ml_model_comparison.csv). Mỗi lần chạy lại kết quả giống hệt vì `random_state = 42`.)*

### 3.2. Diễn giải

1. **Các mô hình cây đều chạm trần.** Decision Tree học được đúng 2 luật cốt lõi: "First Class → trễ" và "Same Day & giờ ≥ 12 → trễ". Accuracy, Precision, Recall, F1 của nó trùng khớp với trần lý thuyết.
2. **Logistic Regression thua rõ** (0,754) vì không tự học được **tương tác** `shipping_mode × order_hour`: giờ đặt hàng chỉ có tác dụng với Same Day. Đây là ví dụ tốt để giải thích ưu thế của mô hình phi tuyến.
3. **Vì sao mô hình cây có Test AUC (0,788) nhỉnh hơn "trần" (0,781)?** Trần cho mọi đơn Second/Standard cùng một xác suất (0,8 / 0,4), tức là "hòa" khi xếp hạng. Mô hình cây tạo ra những khác biệt nhỏ trong nhóm này. Trên riêng giai đoạn test, cách xếp hạng đó tình cờ được AUC 0,52 ở Standard nhưng chỉ 0,476 ở Second ([`ml_auc_by_mode.csv`](./data/ml/ml_auc_by_mode.csv)). Trên **CV của tập train**, mô hình = trần (0,7743 so với 0,7736), và không feature đơn lẻ nào có tín hiệu out-of-time. ⇒ Phần chênh lệch chỉ là **dao động lấy mẫu**, không phải năng lực dự báo thật. Nên trình bày điều này một cách trung thực.
4. **Precision 90% / Recall 60%** ở ngưỡng 0,5:
   - Khi mô hình cảnh báo "trễ" thì 9/10 lần đúng (First Class và Same Day đặt chiều).
   - 40% số đơn trễ bị bỏ sót, chủ yếu là các đơn Standard rơi vào 2/5 kịch bản giao 5–6 ngày, phần không thể biết trước.
   - Hạ ngưỡng xuống 0,40 thì Recall lên 87%, đổi lại Precision giảm còn 65% ([`ml_threshold_metrics.csv`](./data/ml/ml_threshold_metrics.csv)).

### 3.3. Feature Importance (Permutation, ΔROC-AUC trên test)

| # | Feature | ΔAUC | Ý nghĩa |
| :---: | :--- | ---: | :--- |
| 1 | `shipping_mode` | 0,286 | Quyết định chính (SLA so với năng lực giao thực) |
| 2 | `order_hour` | 0,055 | Cut-off 12:00 của Same Day |
| 3 | `payment_type` | 0,004 | ≈ nhiễu |
| 4 | `items_in_order` | 0,002 | ≈ nhiễu |
| 5+ | các feature khác | < 0,002 | Không đóng góp |

**Kết luận nghiệp vụ:** Khu vực, thị trường, ngành hàng, chiết khấu, số lượng **không** ảnh hưởng đến trễ hạn. Điều này bác bỏ giả thuyết "trễ do địa lý / sản phẩm" và quy nguyên nhân về **chính sách SLA**.

### 3.4. Leakage Demo ([`ml_leakage_demo.csv`](./data/ml/ml_leakage_demo.csv))

| Kịch bản | ROC-AUC | Accuracy |
| :--- | ---: | ---: |
| Pre-shipment (hợp lệ) | 0,788 | 73,1% |
| **Thêm `days_shipping_real`** (leakage) | **1,000** | **100%** |

Mô hình có leakage trông hoàn hảo nhưng không dùng được, vì tại thời điểm đặt hàng chưa ai biết số ngày giao thực tế. Đây là minh chứng cho tiêu chí *Preprocessing – Target Leakage prevention* trong rubric.

### 3.5. Kịch bản SLA ([`ml_sla_scenarios.csv`](./data/ml/ml_sla_scenarios.csv)), dùng chung với Feature 8

| Mode | SLA hiện tại → đề xuất | Tỷ lệ trễ hiện tại → mới |
| :--- | :---: | :---: |
| First Class | 1 → **2** ngày | 100% → **0%** |
| Same Day | 0 → 0 + cut-off 12:00 (đơn chiều hứa "next day") | 47,9% → **0%** |
| Second Class | 2 → **4** ngày | 79,8% → 39,8% |
| Standard Class | 4 → **5** ngày | 39,8% → 20,2% |

Áp dụng cả 4 điều chỉnh thì tỷ lệ trễ trên đơn không hủy giảm từ 57,3% xuống khoảng **19,8%** (OTD ≈ 80%). Nếu tính cả đơn hủy là "không trễ" như spec thì OTD ≈ 81%. Con số này khớp với mục tiêu ">82%" của F8 ở mức gần đúng. **Gửi file này cho Vương** để dùng cho Trang 4.

---

## 4. HƯỚNG DẪN CHẠY (INSTRUCTIONS)

### 4.1. Cài đặt
```bash
cd btl
pip install -r requirements-ml.txt
```
Yêu cầu: đã có 6 file Gold trong `data/gold/` (đã có sẵn trong repo; nếu thiếu thì chạy `python run_pipeline.py --step 1`, `--step 2`, `--step 3`).

### 4.2. Huấn luyện & xuất kết quả
```bash
python run_pipeline.py --step 7              # đầy đủ (~3 phút trên máy 12 core)
python -m ml.train_predictive --fast          # 3-fold CV, nhanh hơn khi thử nghiệm
python -m ml.train_predictive --upload-bq     # + đẩy các bảng ml_* lên BigQuery (cần credentials GCP)
```
Trên Windows, nếu console báo lỗi Unicode thì chạy `set PYTHONIOENCODING=utf-8` (CMD) hoặc `$env:PYTHONIOENCODING="utf-8"` (PowerShell) trước.

### 4.3. Demo chấm điểm 1 đơn hàng (khi thuyết trình)
```bash
python -m ml.predict --mode "Same Day" --hour 14 --market Europe --compare
#  Same Day        SLA 0 ngày | P(trễ) = 100.0% | 4-Critical
#  First Class     SLA 1 ngày | P(trễ) = 100.0% | 4-Critical
#  Second Class    SLA 2 ngày | P(trễ) =  80.5% | 3-High
#  Standard Class  SLA 4 ngày | P(trễ) =  40.3% | 2-Medium
python -m ml.predict --mode "Same Day" --hour 9 --market LATAM     # → 0.0% | 1-Low
```

### 4.4. (Tùy chọn) Phiên bản BigQuery ML
File [`ml/bqml_late_delivery.sql`](./ml/bqml_late_delivery.sql): thay `PROJECT` bằng Project ID, rồi chạy từng khối trong BigQuery SQL Editor. Kết quả gồm:
- `LOGISTIC_REG` và `BOOSTED_TREE_CLASSIFIER` huấn luyện bằng SQL
- `ML.EVALUATE` trên test, `ML.GLOBAL_EXPLAIN`, và bảng `bqml_predictions`

Phần này đáp ứng ý "XGBoost / BigQuery ML" của F5 và cho thêm 1 dòng so sánh "Cloud ML vs Python ML" trong báo cáo.

### 4.5. Cấu trúc code

```
btl/
├── ml/
│   ├── ml_config.py          # Features, leakage list, split date, siêu tham số, risk bands, lưới what-if
│   ├── data_prep.py          # Join Gold → feature frame; split out-of-time; oracle_probability()
│   ├── models.py             # Baseline + 5 sklearn Pipelines; mô hình leakage demo
│   ├── evaluate.py           # Metrics, GroupKFold CV, bootstrap CI, ROC, CM, permutation importance, calibration
│   ├── export.py             # Bảng Power BI, what-if grid, SLA scenarios, 6 hình PNG, upload BigQuery
│   ├── train_predictive.py   # Orchestrator Bước 7
│   ├── predict.py            # CLI chấm điểm 1 đơn
│   └── bqml_late_delivery.sql
├── dashboard/
│   ├── build_pbip.py                       # Sinh Power BI Project từ data/ml/
│   ├── Trang3_PredictiveRisk.pbip          # ← MỞ FILE NÀY bằng Power BI Desktop
│   ├── Trang3_PredictiveRisk.SemanticModel/  (TMDL)
│   └── Trang3_PredictiveRisk.Report/         (PBIR + dark theme)
├── data/ml/                  # ← OUTPUT
│   ├── ml_*.csv              # 12 bảng cho Power BI (mục 5)
│   ├── ml_predictions.parquet
│   ├── ml_report.json        # Tóm tắt số liệu cho báo cáo
│   ├── decision_tree_rules.txt
│   ├── models/*.joblib       # 6 mô hình + best_model.joblib
│   └── figures/*.png         # 6 hình cho báo cáo
└── requirements-ml.txt
```

Muốn thay đổi thí nghiệm (ngày chia, feature, siêu tham số, ngưỡng, risk band) thì **chỉ cần sửa [`ml/ml_config.py`](./ml/ml_config.py)**.

---

## 5. TỪ ĐIỂN BẢNG OUTPUT (NGUỒN CHO DASHBOARD TRANG 3)

| Bảng | Dòng | Dùng cho | Cột chính |
| :--- | ---: | :--- | :--- |
| `ml_model_comparison` | 7 | F5: bảng/bar so sánh mô hình | `model, model_type (baseline/ml/reference), accuracy, precision, recall, f1, roc_auc, roc_auc_ci_low/high, pr_auc, brier, cv_roc_auc_mean, pct_of_ceiling_cv_auc, is_best` |
| `ml_roc_curve` | ~835 | F5: line chart ROC | `model, fpr, tpr, threshold` |
| `ml_confusion_matrix` | 24 | F5: matrix | `model, actual, predicted, cell (TP/FP/FN/TN), count, pct_of_total` |
| `ml_feature_importance` | 85 | F5: bar driver attribution | `model, feature, importance_auc_drop, importance_std, rank` |
| `ml_calibration` | 48 | F5 (phụ): độ tin cậy xác suất | `model, bin, mean_predicted, observed_late_rate, n` |
| `ml_threshold_metrics` | 19 | F5/F6: slider ngưỡng cảnh báo | `threshold, precision, recall, f1, accuracy, flagged_pct, late_caught, false_alarms` |
| `ml_auc_by_mode` | 28 | Báo cáo: AUC bên trong từng mode | `model, segment, rows, late_rate, mean_pred, within_auc` |
| `ml_segment_accuracy` | 8 | F5: dự báo so với thực tế theo mode × market | `shipping_mode, market, shipments, actual_late_rate, predicted_late_rate, accuracy` |
| `ml_leakage_demo` | 3 | F5: thẻ "Leakage vs Honest" | `scenario, model, roc_auc, accuracy, …` |
| `ml_predictions` | 180.519 | F5: risk scoring từng đơn, bản đồ / bảng đơn nguy cơ | `order_item_id, order_id, order_date, order_hour, shipping_mode, market, order_region, order_country, …, actual_late, pred_prob_late, pred_label, risk_band, is_correct, split, eval_eligible` |
| `ml_whatif_grid` | 14.400 | **F6: What-If Simulator** | `shipping_mode, order_hour, market, discount_rate, order_item_quantity, pred_prob_late, risk_band, recommended_mode, recommended_mode_prob` |
| `ml_sla_scenarios` | 32 | F6 + F8: chỉnh SLA | `shipping_mode, current_sla_days, proposed_sla_days, expected_late_rate, expected_otd_rate, late_shipments_avoided` |

**Risk band:** `1-Low` < 0,30 ≤ `2-Medium` < 0,60 ≤ `3-High` < 0,85 ≤ `4-Critical`.

---

## 6. DASHBOARD TRANG 3 TRÊN POWER BI

### 6.0. Dashboard dựng sẵn (Power BI Project `.pbip`) — dùng cái này

Toàn bộ dashboard được **sinh tự động bằng code** từ [`dashboard/build_pbip.py`](./dashboard/build_pbip.py):

```bash
cd btl
python run_pipeline.py --step 7          # (1) sinh dữ liệu data/ml/*.parquet
python dashboard/build_pbip.py           # (2) sinh dashboard/Trang3_PredictiveRisk.pbip
```

**Mở:** Power BI Desktop → *File → Open → Browse* → `btl/dashboard/Trang3_PredictiveRisk.pbip` → bấm **Refresh** trên thanh Home (lần đầu mở model chưa có dữ liệu).

**Giao diện:** toàn bộ chữ trên dashboard (tên trang, tiêu đề, nhãn, thông điệp trong measure) bằng **tiếng Anh**. Theme dùng **DataCo Logistics Harbor** của nhóm ([`dashboard/themes/DataCo_Harbor_Theme.json`](./dashboard/themes/DataCo_Harbor_Theme.json)). Muốn đổi theme thì thay file này rồi chạy lại builder. Riêng cỡ chữ tiêu đề visual được giảm từ 15pt xuống 12pt để vừa lưới 1280×720.

| Trang | Feature | Nội dung |
| :--- | :---: | :--- |
| **3.1 Model Evaluation** | F5 | Slicer mô hình · 7 KPI (AUC, Accuracy, Precision, Recall, % trần, AUC leakage) · Bar so sánh AUC test vs CV · ROC curves · Permutation importance · Confusion matrix · Slicer ngưỡng + đường Precision/Recall/F1 · Bảng AUC trong từng mode · Insight |
| **3.2 Risk Scoring** | F5 | Slicer tập/mode/market · 5 KPI · Donut risk band · Thực tế vs dự báo theo mode và theo giờ · Rủi ro theo khu vực · Bảng cảnh báo sớm đơn Critical |
| **3.3 What-If Simulator** | F6 | 5 slicer tham số → Gauge P(trễ), risk band, SLA, khuyến nghị đổi phương thức · So sánh 4 mode cùng bối cảnh · Đường P(trễ) theo giờ × mode · Kịch bản SLA đề xuất (OTD mới, số đơn tránh trễ) |

**Semantic model:** 12 bảng `ml_*` (Parquet) + `dim_model` + `dim_shipping_mode` (slicer dùng chung, có quan hệ) + bảng `_Measures` chứa 53 measure DAX (thư mục F5 Model / F5 Threshold / F5 Scoring / F6 What-If / F6 SLA).

**Đã kiểm chứng:** 62 file PBIR hợp lệ theo JSON schema của Power BI Desktop; TMDL được `TmdlSerializer` của Power BI parse thành công; đã mở trong Power BI Desktop, refresh 15 bảng không lỗi, và chạy DAX kiểm tra các measure (AUC 0,788 · Precision 90,0% · What-If Same Day 14h → 100% + gợi ý · SLA Standard 5 ngày → trễ 20,2%).

**Lưu ý:**
- Đường dẫn dữ liệu nằm trong tham số Power Query `DataFolder`. Khi chuyển sang máy khác thì chạy lại `python dashboard/build_pbip.py --data-folder "<đường dẫn>\btl\data\ml"`, hoặc sửa trong *Transform data → Manage parameters*.
- `build_pbip.py` **ghi đè** thư mục `definition/` của report. Nếu bạn đã chỉnh layout bằng tay trong Power BI Desktop thì đừng chạy lại builder, chỉ cần *Save* (Ctrl+S) trong Desktop.
- Muốn gộp với dashboard của nhóm: mở file `.pbip` của bạn, rồi trong file chung dùng *Get data → Power BI semantic models*, hoặc copy các bảng/measure + copy-paste visual giữa 2 cửa sổ Desktop.

### 6.1. (Thủ công) Nạp dữ liệu
- **Cách A (nhanh, khuyến nghị cho local):** *Get Data → Text/CSV*, chọn các file `data/ml/ml_*.csv`. Với `ml_predictions` thì nên dùng *Get Data → Parquet* (file 3 MB, nạp nhanh hơn CSV 33 MB).
- **Cách B (đồng bộ cả nhóm):** chạy `python -m ml.train_predictive --upload-bq`, sau đó trong Power BI chọn *Get Data → Google BigQuery → dataco_logistics_gold → ml_**.

Không cần tạo relationship. Mỗi bảng phục vụ một nhóm visual riêng. `ml_whatif_grid` và `ml_threshold_metrics` là **bảng tham số độc lập** (disconnected).

### 6.2. Bố cục gợi ý (Modern Dark, đồng bộ với Trang 1)

```
┌───────────────────────────────────────────────────────────────────────────────────────┐
│ TRANG 3: PREDICTIVE DELAY RISK & ML EARLY-WARNING           [Nav: 1 | 2 | 3 | 4]      │
├────────────┬────────────┬────────────┬────────────┬───────────────────────────────────┤
│ BEST MODEL │  ROC-AUC   │ PRECISION  │  RECALL    │ % TRẦN LÝ THUYẾT                  │
│ DecisionTr │   0.788    │   90.0%    │   60.0%    │ 100%  (Leakage demo: AUC 1.00 ⚠)  │
├────────────┴────────────┴──────────────────────────┬────────────────────────────────────┤
│ F5-a  So sánh mô hình (bar ROC-AUC + CI,           │ F5-b  ROC Curves (line, legend=model)│
│       đường tham chiếu trần lý thuyết)             │                                    │
├────────────────────────────────────────────────────┼────────────────────────────────────┤
│ F5-c  Driver Attribution (bar importance,          │ F5-d  Confusion Matrix (matrix,     │
│       slicer chọn model)                           │       conditional formatting)       │
├────────────────────────────────────────────────────┴────────────────────────────────────┤
│ F6  WHAT-IF ORDER DELAY SIMULATOR                                                        │
│ [Shipping Mode ▼] [Giờ đặt ═══○═══] [Market ▼] [Discount ▼] [Qty ▼]                      │
│   ┌───────────────┐   Risk band: 4-Critical       Gợi ý: chuyển sang "Same Day" (0%)    │
│   │ GAUGE  P(trễ) │   SLA hiện tại: 0 ngày                                              │
│   │     100%      │   ┌ Kịch bản SLA: [SLA đề xuất ═══○═══] → OTD mới 80.2%  ┐          │
│   └───────────────┘   └ (ml_sla_scenarios)                                    ┘          │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│ Danh sách đơn nguy cơ cao (ml_predictions: risk_band ≥ High, split = test) + Ngưỡng slider │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

### 6.3. Các measure DAX

```dax
// ===== F5: Model Evaluation =====
Best_Model_Name = CALCULATE(SELECTEDVALUE(ml_model_comparison[model]), ml_model_comparison[is_best] = 1)
Best_AUC        = CALCULATE(MAX(ml_model_comparison[roc_auc]),   ml_model_comparison[is_best] = 1)
Best_Precision  = CALCULATE(MAX(ml_model_comparison[precision]), ml_model_comparison[is_best] = 1)
Best_Recall     = CALCULATE(MAX(ml_model_comparison[recall]),    ml_model_comparison[is_best] = 1)
Pct_Of_Ceiling  = CALCULATE(MAX(ml_model_comparison[pct_of_ceiling_cv_auc]), ml_model_comparison[is_best] = 1)
Ceiling_AUC     = CALCULATE(MAX(ml_model_comparison[roc_auc]), ml_model_comparison[model] = "Theoretical_Ceiling")
Leakage_AUC     = CALCULATE(MAX(ml_leakage_demo[roc_auc]), CONTAINSSTRING(ml_leakage_demo[scenario], "Leakage"))

// ===== F5: Risk Scoring trên ml_predictions =====
Scored_Orders        = CALCULATE(COUNTROWS(ml_predictions), ml_predictions[eval_eligible] = 1)
Predicted_Late_Rate  = CALCULATE(AVERAGE(ml_predictions[pred_prob_late]), ml_predictions[eval_eligible] = 1)
Actual_Late_Rate     = CALCULATE(AVERAGE(ml_predictions[actual_late]),    ml_predictions[eval_eligible] = 1)
Test_Accuracy        = CALCULATE(AVERAGE(ml_predictions[is_correct]),
                         ml_predictions[split] = "test", ml_predictions[eval_eligible] = 1)
High_Risk_Orders     = CALCULATE(DISTINCTCOUNT(ml_predictions[order_id]),
                         ml_predictions[risk_band] IN {"3-High", "4-Critical"})

// ===== F6: What-If Simulator (slicer single-select trên các cột của ml_whatif_grid) =====
WhatIf_Prob_Late   = AVERAGE(ml_whatif_grid[pred_prob_late])
WhatIf_Risk_Band   = SELECTEDVALUE(ml_whatif_grid[risk_band], "Chọn đủ 5 tham số")
WhatIf_Recommend   = SELECTEDVALUE(ml_whatif_grid[recommended_mode])
WhatIf_Recommend_P = SELECTEDVALUE(ml_whatif_grid[recommended_mode_prob])
WhatIf_Message =
    IF( [WhatIf_Recommend] <> SELECTEDVALUE(ml_whatif_grid[shipping_mode]),
        "Đề xuất chuyển sang " & [WhatIf_Recommend] & " — P(trễ) " & FORMAT([WhatIf_Recommend_P], "0%"),
        "Phương thức hiện tại đã tối ưu" )

// ===== Ngưỡng cảnh báo (slicer trên ml_threshold_metrics[threshold]) =====
Thr_Precision   = SELECTEDVALUE(ml_threshold_metrics[precision])
Thr_Recall      = SELECTEDVALUE(ml_threshold_metrics[recall])
Thr_Flagged_Pct = SELECTEDVALUE(ml_threshold_metrics[flagged_pct])

// ===== Kịch bản SLA (slicer shipping_mode + proposed_sla_days trên ml_sla_scenarios) =====
SLA_New_Late_Rate = SELECTEDVALUE(ml_sla_scenarios[expected_late_rate])
SLA_Avoided       = SELECTEDVALUE(ml_sla_scenarios[late_shipments_avoided])
```

### 6.4. Mẹo làm visual
- **ROC Curve:** Line chart với X = `fpr`, Y = `tpr`, Legend = `model`. Tắt "Summarize" để giữ từng điểm (hoặc đặt X là *Don't summarize*).
- **Confusion Matrix:** Matrix với Rows = `actual`, Columns = `predicted`, Values = `count`, kèm slicer `model`. Dùng conditional formatting kiểu background gradient.
- **Gauge What-If:** Value = `[WhatIf_Prob_Late]`, Min 0, Max 1, Target 0,5. Slicer `order_hour` nên để kiểu *Single value slider*.
- **Bản đồ rủi ro:** Map hoặc Filled map trên `ml_predictions[order_country]`, Color = `[Predicted_Late_Rate]`.
- Dùng **Bookmarks** để chuyển giữa "Honest model" và "Leakage demo" khi thuyết trình.

---

## 7. ĐIỂM NHẤN CHO BÁO CÁO & THUYẾT TRÌNH

1. **Predictive → Prescriptive:** mô hình chỉ ra rằng nguyên nhân là SLA chứ không phải địa lý, từ đó dẫn sang kịch bản tái cấu trúc SLA (F8).
2. **Khoa học dữ liệu trung thực:** có baseline, có trần lý thuyết, có CI bootstrap, có leakage demo, và chia dữ liệu out-of-time cùng GroupKFold.
3. **So sánh công nghệ (bonus):** 5 thuật toán Python, đối chiếu với BigQuery ML (SQL).
4. **Hình cho báo cáo** nằm ở [`data/ml/figures/`](./data/ml/figures/):

| File | Nội dung |
| :--- | :--- |
| `01_model_comparison_auc.png` | So sánh ROC-AUC kèm CI và đường trần |
| `02_roc_curves.png` | Đường ROC của các mô hình |
| `03_confusion_matrix_best.png` | Confusion matrix của mô hình tốt nhất |
| `04_feature_importance_best.png` | Feature importance |
| `05_calibration_best.png` | Calibration |
| `06_data_generating_process.png` | Cơ chế sinh dữ liệu (cut-off Same Day, phân phối ngày giao) |

### Hạn chế cần nêu
- DataCo là **dữ liệu bán tổng hợp (semi-synthetic)**: ngày giao của Second/Standard được sinh ngẫu nhiên đều. Trên dữ liệu thật, các yếu tố như khoảng cách, hãng vận chuyển, thời tiết hay tải kho sẽ cho tín hiệu mạnh hơn.
- Dataset không có feature về năng lực vận hành (tải kho, hãng vận chuyển, tuyến) nên phần trễ của Second/Standard là nhiễu không thể khử với dữ liệu hiện có.
- Từ Q3/2017 phân bố market thay đổi (ít market hơn): mô hình vẫn ổn định vì không phụ thuộc market, nhưng cần lưu ý khi diễn giải xu hướng.
