"""
Module: evaluate.py
Mục đích: Các hàm đánh giá mô hình — metrics, Cross-Validation theo nhóm đơn hàng, ROC,
Confusion Matrix, Permutation Importance, Calibration, Threshold sweep.
"""

import time
import numpy as np
import pandas as pd
from sklearn.base import clone
from sklearn.inspection import permutation_importance
from sklearn.metrics import (
    accuracy_score, average_precision_score, brier_score_loss, confusion_matrix,
    f1_score, log_loss, precision_score, recall_score, roc_auc_score, roc_curve,
)
from sklearn.model_selection import StratifiedGroupKFold

from ml.ml_config import CV_FOLDS, DECISION_THRESHOLD, RANDOM_STATE


def classification_metrics(y_true, prob, threshold: float = DECISION_THRESHOLD) -> dict:
    y_true = np.asarray(y_true)
    prob = np.clip(np.asarray(prob, dtype=float), 1e-6, 1 - 1e-6)
    pred = (prob >= threshold).astype(int)
    return {
        "accuracy":  accuracy_score(y_true, pred),
        "precision": precision_score(y_true, pred, zero_division=0),
        "recall":    recall_score(y_true, pred, zero_division=0),
        "f1":        f1_score(y_true, pred, zero_division=0),
        "roc_auc":   roc_auc_score(y_true, prob),
        "pr_auc":    average_precision_score(y_true, prob),
        "log_loss":  log_loss(y_true, prob),
        "brier":     brier_score_loss(y_true, prob),
    }


def cross_validate_grouped(model, X, y, groups, n_splits: int = CV_FOLDS) -> dict:
    """StratifiedGroupKFold: mọi dòng của 1 đơn hàng nằm cùng 1 fold (tránh rò rỉ giữa các fold)."""
    cv = StratifiedGroupKFold(n_splits=n_splits, shuffle=True, random_state=RANDOM_STATE)
    aucs, briers = [], []
    for tr_idx, va_idx in cv.split(X, y, groups):
        m = clone(model).fit(X.iloc[tr_idx], y.iloc[tr_idx])
        p = m.predict_proba(X.iloc[va_idx])[:, 1]
        aucs.append(roc_auc_score(y.iloc[va_idx], p))
        briers.append(brier_score_loss(y.iloc[va_idx], p))
    return {
        "cv_roc_auc_mean": float(np.mean(aucs)), "cv_roc_auc_std": float(np.std(aucs)),
        "cv_brier_mean": float(np.mean(briers)),
    }


def bootstrap_auc_ci(y_true, prob, n_boot: int = 200, alpha: float = 0.05) -> tuple:
    """Khoảng tin cậy 95% của ROC-AUC bằng bootstrap (resample dòng)."""
    y_true, prob = np.asarray(y_true), np.asarray(prob)
    rng = np.random.default_rng(RANDOM_STATE)
    stats = []
    for _ in range(n_boot):
        idx = rng.integers(0, len(y_true), len(y_true))
        stats.append(roc_auc_score(y_true[idx], prob[idx]))
    return float(np.quantile(stats, alpha / 2)), float(np.quantile(stats, 1 - alpha / 2))


def auc_by_segment(model_name, seg, y_true, prob) -> pd.DataFrame:
    """ROC-AUC bên trong từng nhóm (vd. shipping_mode) — cho thấy mô hình phân biệt được ở đâu."""
    d = pd.DataFrame({"seg": np.asarray(seg), "y": np.asarray(y_true), "p": np.asarray(prob)})
    rows = []
    for s, g in d.groupby("seg"):
        rows.append({"model": model_name, "segment": s, "rows": len(g), "late_rate": g["y"].mean(),
                     "mean_pred": g["p"].mean(),
                     "within_auc": roc_auc_score(g["y"], g["p"]) if g["y"].nunique() > 1 else np.nan})
    return pd.DataFrame(rows)


def fit_timed(model, X, y):
    t0 = time.perf_counter()
    model.fit(X, y)
    return model, time.perf_counter() - t0


def roc_points(model_name, y_true, prob, max_points: int = 200) -> pd.DataFrame:
    fpr, tpr, thr = roc_curve(y_true, prob)
    if len(fpr) > max_points:  # giảm số điểm cho Power BI nhưng giữ 2 đầu mút
        idx = np.unique(np.linspace(0, len(fpr) - 1, max_points).astype(int))
        fpr, tpr, thr = fpr[idx], tpr[idx], thr[idx]
    return pd.DataFrame({"model": model_name, "fpr": fpr, "tpr": tpr,
                         "threshold": np.clip(thr, 0, 1)})


def confusion_long(model_name, y_true, prob, threshold: float = DECISION_THRESHOLD) -> pd.DataFrame:
    """Confusion Matrix dạng long (Power BI Matrix visual: Rows=actual, Columns=predicted)."""
    pred = (np.asarray(prob) >= threshold).astype(int)
    cm = confusion_matrix(y_true, pred, labels=[0, 1])
    labels = {0: "On-time", 1: "Late"}
    cell = {(0, 0): "TN", (0, 1): "FP", (1, 0): "FN", (1, 1): "TP"}
    rows = [{"model": model_name, "actual": labels[a], "predicted": labels[p],
             "cell": cell[(a, p)], "count": int(cm[a, p]),
             "pct_of_total": cm[a, p] / cm.sum()} for a in (0, 1) for p in (0, 1)]
    return pd.DataFrame(rows)


def permutation_importance_df(model_name, model, X, y, n_repeats=3, sample=10_000) -> pd.DataFrame:
    """Permutation Importance trên tập test (đơn vị: mức giảm ROC-AUC khi xáo trộn 1 feature)."""
    if len(X) > sample:
        Xs = X.sample(sample, random_state=RANDOM_STATE)
        ys = y.loc[Xs.index]
    else:
        Xs, ys = X, y
    r = permutation_importance(model, Xs, ys, scoring="roc_auc", n_repeats=n_repeats,
                               random_state=RANDOM_STATE, n_jobs=1)
    out = pd.DataFrame({"model": model_name, "feature": X.columns,
                        "importance_auc_drop": r.importances_mean,
                        "importance_std": r.importances_std})
    out = out.sort_values("importance_auc_drop", ascending=False)
    out["rank"] = range(1, len(out) + 1)
    return out


def calibration_df(model_name, y_true, prob, n_bins=10) -> pd.DataFrame:
    d = pd.DataFrame({"y": np.asarray(y_true), "p": np.asarray(prob)})
    d["bin"] = pd.cut(d["p"], bins=np.linspace(0, 1, n_bins + 1), include_lowest=True)
    g = d.groupby("bin", observed=True).agg(mean_predicted=("p", "mean"),
                                            observed_late_rate=("y", "mean"),
                                            n=("y", "size")).reset_index()
    g["bin"] = g["bin"].astype(str)
    g.insert(0, "model", model_name)
    return g


def threshold_sweep(y_true, prob) -> pd.DataFrame:
    """Bảng ngưỡng 0.05→0.95 — dùng làm What-If Parameter 'Alert Threshold' trên Power BI."""
    y_true = np.asarray(y_true)
    rows = []
    for t in np.round(np.arange(0.05, 0.96, 0.05), 2):
        pred = (prob >= t).astype(int)
        rows.append({
            "threshold": t,
            "precision": precision_score(y_true, pred, zero_division=0),
            "recall": recall_score(y_true, pred, zero_division=0),
            "f1": f1_score(y_true, pred, zero_division=0),
            "accuracy": accuracy_score(y_true, pred),
            "flagged_pct": pred.mean(),
            "late_caught": int(((pred == 1) & (y_true == 1)).sum()),
            "false_alarms": int(((pred == 1) & (y_true == 0)).sum()),
        })
    return pd.DataFrame(rows)
