"""
Package: ml
Mục đích: Phân hệ Predictive Analytics (Trang 3 Dashboard — Feature 5 & 6).
  - ml_config.py         : Cấu hình tập trung (features, đường dẫn, siêu tham số)
  - data_prep.py         : Nạp Gold Star Schema, dựng feature pre-shipment, chia train/test theo thời gian
  - models.py            : Định nghĩa các mô hình (Baseline, LR, DT, RF, XGBoost, LightGBM)
  - evaluate.py          : Chỉ số đánh giá, ROC, Confusion Matrix, Permutation Importance, Calibration
  - export.py            : Xuất bảng cho Power BI (predictions, what-if grid, SLA scenarios) + hình vẽ báo cáo
  - train_predictive.py  : Orchestrator — chạy toàn bộ (python run_pipeline.py --step 7)
  - predict.py           : CLI chấm điểm rủi ro cho 1 đơn hàng giả định
"""
