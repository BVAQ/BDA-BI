"""
Module: train_predictive.py
Mục đích: BƯỚC 7 — Predictive Analytics: Dự báo rủi ro trễ hạn TRƯỚC KHI GIAO HÀNG (Trang 3 — Quân).

Luồng xử lý:
  1. Nạp Gold Star Schema → bảng feature pre-shipment (không target leakage)
  2. Chia Out-of-time: train < 2017-07-01 ≤ test; loại đơn hủy khỏi train/eval
  3. 5-fold StratifiedGroupKFold (nhóm theo order_id) trên train → chọn mô hình tốt nhất
  4. Fit trên train → đánh giá trên test (Accuracy, Precision, Recall, F1, ROC-AUC, PR-AUC, LogLoss, Brier)
  5. So sánh với Baseline nghiệp vụ và Trần lý thuyết (Bayes-optimal)
  6. Thí nghiệm Leakage Demo (thêm days_shipping_real) để chứng minh vì sao phải loại cột rò rỉ
  7. Xuất bảng cho Power BI + hình vẽ báo cáo + mô hình .joblib

Chạy:  python run_pipeline.py --step 7
       python -m ml.train_predictive [--fast] [--upload-bq]
"""

import sys
import json
import time
import argparse
from datetime import datetime
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))

from ml.ml_config import (  # noqa: E402
    ML_DIR, MODEL_DIR, FIGURE_DIR, FEATURES, CATEGORICAL_FEATURES, NUMERIC_FEATURES,
    LEAKY_FEATURES, TARGET, GROUP_COL, TEST_START, DECISION_THRESHOLD, CV_FOLDS,
)
from ml.data_prep import load_modeling_frame, train_test_frames, oracle_probability  # noqa: E402
from ml.models import build_models, build_leaky_model  # noqa: E402
from ml import evaluate as ev  # noqa: E402
from ml import export as ex  # noqa: E402


def run_predictive_modeling(fast: bool = False, upload_bq: bool = False) -> dict:
    print("=" * 80)
    print(">>> BƯỚC 7: PREDICTIVE ANALYTICS — DỰ BÁO RỦI RO TRỄ HẠN PRE-SHIPMENT (TRANG 3)")
    print("=" * 80)
    t_start = time.time()
    n_folds = 3 if fast else CV_FOLDS

    # ------------------------------------------------------------------ 1. Dữ liệu
    print("[1/7] Nạp Gold Star Schema & dựng feature pre-shipment...")
    df_all = load_modeling_frame()
    train, test = train_test_frames(df_all)
    X_tr, y_tr, g_tr = train[FEATURES], train[TARGET], train[GROUP_COL]
    X_te, y_te = test[FEATURES], test[TARGET]
    print(f"    - Tổng: {len(df_all):,} dòng | Đơn hủy (loại khỏi train/eval): {df_all['is_canceled_shipment'].sum():,}")
    print(f"    - Train (< {TEST_START}): {len(train):,} dòng | late rate = {y_tr.mean():.2%}")
    print(f"    - Test  (≥ {TEST_START}): {len(test):,} dòng | late rate = {y_te.mean():.2%}")
    print(f"    - {len(FEATURES)} features: {len(CATEGORICAL_FEATURES)} categorical + {len(NUMERIC_FEATURES)} numeric")

    # ------------------------------------------------------------------ 2. CV + Fit + Test
    models = build_models()
    print(f"\n[2/7] Huấn luyện & đánh giá {len(models)} mô hình ({n_folds}-fold StratifiedGroupKFold)...")
    rows, roc_frames, cm_frames, calib_frames, seg_frames = [], [], [], [], []
    fitted, test_probs = {}, {}
    for name, model in models.items():
        t0 = time.perf_counter()
        cv = ev.cross_validate_grouped(model, X_tr, y_tr, g_tr, n_splits=n_folds)
        model, fit_s = ev.fit_timed(model, X_tr, y_tr)
        prob = model.predict_proba(X_te)[:, 1]
        m = ev.classification_metrics(y_te, prob)
        lo, hi = ev.bootstrap_auc_ci(y_te, prob)
        rows.append({"model": name, "model_type": "baseline" if name.startswith("Baseline") else "ml",
                     **m, "roc_auc_ci_low": lo, "roc_auc_ci_high": hi, **cv, "fit_seconds": fit_s})
        roc_frames.append(ev.roc_points(name, y_te, prob))
        cm_frames.append(ev.confusion_long(name, y_te, prob))
        calib_frames.append(ev.calibration_df(name, y_te, prob))
        seg_frames.append(ev.auc_by_segment(name, test["shipping_mode"], y_te, prob))
        fitted[name], test_probs[name] = model, prob
        print(f"    - {name:<20} CV AUC={cv['cv_roc_auc_mean']:.4f}±{cv['cv_roc_auc_std']:.4f} | "
              f"Test AUC={m['roc_auc']:.4f} [{lo:.3f}–{hi:.3f}] Acc={m['accuracy']:.4f} F1={m['f1']:.4f} "
              f"({time.perf_counter() - t0:.1f}s)")

    # Trần lý thuyết (không phải mô hình — tham chiếu). Tính cả trên train để so với CV.
    oracle = oracle_probability(test).to_numpy()
    oracle_tr = oracle_probability(train).to_numpy()
    m_or = ev.classification_metrics(y_te, oracle)
    m_or_tr = ev.classification_metrics(y_tr, oracle_tr)
    lo, hi = ev.bootstrap_auc_ci(y_te, oracle)
    rows.append({"model": "Theoretical_Ceiling", "model_type": "reference", **m_or,
                 "roc_auc_ci_low": lo, "roc_auc_ci_high": hi,
                 "cv_roc_auc_mean": m_or_tr["roc_auc"], "cv_roc_auc_std": 0.0,
                 "cv_brier_mean": m_or_tr["brier"], "fit_seconds": 0.0})
    roc_frames.append(ev.roc_points("Theoretical_Ceiling", y_te, oracle))
    seg_frames.append(ev.auc_by_segment("Theoretical_Ceiling", test["shipping_mode"], y_te, oracle))
    print(f"    - {'Theoretical_Ceiling':<20} CV AUC={m_or_tr['roc_auc']:.4f}         | "
          f"Test AUC={m_or['roc_auc']:.4f} [{lo:.3f}–{hi:.3f}] Acc={m_or['accuracy']:.4f} "
          f"(Bayes-optimal — mốc tham chiếu)")

    comparison = pd.DataFrame(rows)
    # Chọn mô hình tốt nhất theo CV (không nhìn tập test): AUC cao nhất, hòa thì Brier thấp hơn
    ml_rows = comparison[comparison["model_type"] == "ml"].copy()
    ml_rows["_auc"] = ml_rows["cv_roc_auc_mean"].round(3)
    best_name = ml_rows.sort_values(["_auc", "cv_brier_mean"], ascending=[False, True])["model"].iloc[0]
    comparison["is_best"] = (comparison["model"] == best_name).astype(int)
    # % trần đo trên CV (ổn định hơn 1 tập test): (AUC - 0.5) / (AUC_trần - 0.5)
    comparison["pct_of_ceiling_cv_auc"] = (comparison["cv_roc_auc_mean"] - 0.5) / (m_or_tr["roc_auc"] - 0.5)
    comparison["gap_to_ceiling_test_auc"] = m_or["roc_auc"] - comparison["roc_auc"]
    best_model = fitted[best_name]
    print(f"\n[✓] Mô hình được chọn: {best_name} "
          f"(CV AUC đạt {comparison.loc[comparison.model == best_name, 'pct_of_ceiling_cv_auc'].iloc[0]:.1%} "
          f"trần lý thuyết; chênh lệch Test so với trần nằm trong dao động lấy mẫu)")

    # ------------------------------------------------------------------ 3. Feature importance
    print("\n[3/7] Permutation Importance trên tập test (mọi mô hình ML)...")
    fi_frames = []
    for name, model in fitted.items():
        if name.startswith("Baseline"):
            continue
        fi_frames.append(ev.permutation_importance_df(name, model, X_te, y_te,
                                                      n_repeats=2 if fast else 3))
    feature_importance = pd.concat(fi_frames, ignore_index=True)
    fi_best = feature_importance[feature_importance["model"] == best_name]
    print("    Top-5 feature của mô hình tốt nhất:")
    for _, r in fi_best.head(5).iterrows():
        print(f"      {r['rank']}. {r['feature']:<20} ΔAUC = {r['importance_auc_drop']:.4f}")

    # ------------------------------------------------------------------ 4. Leakage demo
    print("\n[4/7] Thí nghiệm Leakage Demo (thêm cột chỉ biết sau khi giao)...")
    leaky = build_leaky_model(CATEGORICAL_FEATURES, NUMERIC_FEATURES + LEAKY_FEATURES)
    leaky.fit(train[FEATURES + LEAKY_FEATURES], y_tr)
    p_leak = leaky.predict_proba(test[FEATURES + LEAKY_FEATURES])[:, 1]
    m_leak = ev.classification_metrics(y_te, p_leak)
    leakage_demo = pd.DataFrame([
        {"scenario": "Pre-shipment (hợp lệ)", "model": best_name, **comparison.loc[comparison.model == best_name, list(m_leak)].iloc[0].to_dict()},
        {"scenario": "Có Target Leakage (days_shipping_real)", "model": "RandomForest", **m_leak},
        {"scenario": "Trần lý thuyết pre-shipment", "model": "Theoretical_Ceiling", **m_or},
    ])
    print(f"    - Với leakage: AUC={m_leak['roc_auc']:.4f}, Acc={m_leak['accuracy']:.4f} → 'quá đẹp để là thật'")
    print(f"    - Không leakage: AUC={comparison.loc[comparison.model == best_name, 'roc_auc'].iloc[0]:.4f} (trung thực)")

    # ------------------------------------------------------------------ 5. Bảng Power BI
    print("\n[5/7] Xuất bảng cho Power BI (data/ml/)...")
    predictions = ex.build_predictions_table(df_all, best_model, best_name)
    whatif = ex.build_whatif_grid(best_model, train)
    sla = ex.build_sla_scenarios(df_all)
    thresholds = ev.threshold_sweep(y_te.to_numpy(), test_probs[best_name])
    roc_df = pd.concat(roc_frames, ignore_index=True)
    cm_df = pd.concat(cm_frames, ignore_index=True)
    calib_df = pd.concat(calib_frames, ignore_index=True)
    segment = (predictions[(predictions.eval_eligible == 1) & (predictions.split == "test")]
               .groupby(["shipping_mode", "market"])
               .agg(shipments=("order_item_id", "size"), actual_late_rate=("actual_late", "mean"),
                    predicted_late_rate=("pred_prob_late", "mean"), accuracy=("is_correct", "mean"))
               .reset_index())

    tables = {
        "ml_model_comparison": comparison,
        "ml_roc_curve": roc_df,
        "ml_confusion_matrix": cm_df,
        "ml_feature_importance": feature_importance,
        "ml_calibration": calib_df,
        "ml_threshold_metrics": thresholds,
        "ml_leakage_demo": leakage_demo,
        "ml_predictions": predictions,
        "ml_whatif_grid": whatif,
        "ml_sla_scenarios": sla,
        "ml_segment_accuracy": segment,
        "ml_auc_by_mode": pd.concat(seg_frames, ignore_index=True),
    }
    for name, tbl in tables.items():
        ex.save_table(tbl, name)
        print(f"    - {name + '.csv':<30} {len(tbl):>8,} dòng")

    # ------------------------------------------------------------------ 6. Mô hình + hình
    print("\n[6/7] Lưu mô hình (.joblib), luật cây quyết định & hình vẽ báo cáo...")
    for name, model in fitted.items():
        joblib.dump(model, MODEL_DIR / f"{name}.joblib")
    joblib.dump(best_model, MODEL_DIR / "best_model.joblib")
    if "DecisionTree" in fitted:
        from sklearn.tree import export_text
        dt = fitted["DecisionTree"]
        names = list(dt.named_steps["prep"].get_feature_names_out())
        names = [n.split("__", 1)[-1] for n in names]
        cat_maps = dict(zip(CATEGORICAL_FEATURES, dt.named_steps["prep"].named_transformers_["cat"].categories_))
        header = "# Mã hóa categorical (OrdinalEncoder):\n" + "\n".join(
            f"#   {c}: " + ", ".join(f"{i}={v}" for i, v in enumerate(cats)) for c, cats in cat_maps.items())
        (ML_DIR / "decision_tree_rules.txt").write_text(
            header + "\n\n" + export_text(dt.named_steps["clf"], feature_names=names, max_depth=4),
            encoding="utf-8")
    ex.make_figures(comparison, roc_df, cm_df[cm_df.model == best_name], fi_best,
                    calib_df[calib_df.model == best_name], df_all, best_name, m_or["roc_auc"])
    print(f"    - Mô hình: {MODEL_DIR}")
    print(f"    - Hình:    {FIGURE_DIR} (6 PNG)")

    # ------------------------------------------------------------------ 7. Báo cáo JSON
    best_row = comparison[comparison.model == best_name].iloc[0]
    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "duration_seconds": round(time.time() - t_start, 1),
        "split": {"strategy": "out-of-time", "test_start": TEST_START,
                  "train_rows": len(train), "test_rows": len(test),
                  "canceled_excluded": int(df_all["is_canceled_shipment"].sum()),
                  "cv": f"{n_folds}-fold StratifiedGroupKFold by order_id"},
        "features": {"categorical": CATEGORICAL_FEATURES, "numeric": NUMERIC_FEATURES,
                     "excluded_leakage": ["days_shipping_real", "delay_days", "shipping_date_ts",
                                          "delivery_status", "shipping_ops_key", "order_status",
                                          "net_profit", "is_loss_order", "is_canceled_or_fraud"]},
        "best_model": {"name": best_name, "threshold": DECISION_THRESHOLD,
                       **{k: round(float(best_row[k]), 4) for k in
                          ["accuracy", "precision", "recall", "f1", "roc_auc", "pr_auc", "log_loss", "brier",
                           "roc_auc_ci_low", "roc_auc_ci_high", "cv_roc_auc_mean", "pct_of_ceiling_cv_auc"]}},
        "theoretical_ceiling": {"test": {k: round(float(v), 4) for k, v in m_or.items()},
                                "train": {k: round(float(v), 4) for k, v in m_or_tr.items()}},
        "leakage_demo": {k: round(float(v), 4) for k, v in m_leak.items()},
        "top_features": fi_best.head(5)[["feature", "importance_auc_drop"]].round(4).to_dict("records"),
    }
    with open(ML_DIR / "ml_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=4, ensure_ascii=False)

    if upload_bq:
        print("\n[*] Đẩy bảng kết quả lên BigQuery...")
        ex.upload_to_bigquery(tables)

    print(f"\n[7/7] [✓] HOÀN TẤT BƯỚC 7 trong {report['duration_seconds']}s — báo cáo: data/ml/ml_report.json")
    return report


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Huấn luyện mô hình dự báo trễ hạn pre-shipment")
    ap.add_argument("--fast", action="store_true", help="3-fold CV & ít lần permutation hơn (chạy nhanh)")
    ap.add_argument("--upload-bq", action="store_true", help="Đẩy bảng kết quả lên BigQuery (cần credentials)")
    a = ap.parse_args()
    run_predictive_modeling(fast=a.fast, upload_bq=a.upload_bq)
