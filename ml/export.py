"""
Module: export.py
Mục đích: Xuất các bảng phục vụ Power BI Trang 3 + hình vẽ cho Báo cáo + (tùy chọn) đẩy lên BigQuery.
"""

import itertools
import pandas as pd

from ml.ml_config import (
    ML_DIR, FIGURE_DIR, FEATURES, CATEGORICAL_FEATURES, NUMERIC_FEATURES,
    WHATIF_GRID, RISK_BANDS, CURRENT_SLA, DECISION_THRESHOLD, TARGET,
)


# ---------------------------------------------------------------------------
# Tiện ích
# ---------------------------------------------------------------------------
def risk_band(prob: pd.Series) -> pd.Series:
    out = pd.Series("1-Low", index=prob.index)
    for lower, label in sorted(RISK_BANDS):          # tăng dần → nhãn cao ghi đè
        out[prob >= lower] = label
    return out


def save_table(df: pd.DataFrame, name: str) -> None:
    """CSV (đọc bằng mắt / Excel) + Parquet (nguồn của Power BI: giữ kiểu dữ liệu, nạp nhanh)."""
    df.to_csv(ML_DIR / f"{name}.csv", index=False, encoding="utf-8-sig")
    out = df.copy()
    for c in out.columns:  # datetime → date thuần, tránh timestamp[ns] khó đọc ở Power Query
        if pd.api.types.is_datetime64_any_dtype(out[c]):
            out[c] = out[c].dt.date
    out.to_parquet(ML_DIR / f"{name}.parquet", index=False, engine="pyarrow")


# ---------------------------------------------------------------------------
# 1. Bảng chấm điểm toàn bộ 180.519 dòng (Risk Scoring — Feature 5)
# ---------------------------------------------------------------------------
def build_predictions_table(df_all: pd.DataFrame, model, model_name: str) -> pd.DataFrame:
    prob = pd.Series(model.predict_proba(df_all[FEATURES])[:, 1], index=df_all.index)
    out = df_all[[
        "order_item_id", "order_id", "order_date", "order_hour", "order_dayofweek", "order_month",
        "shipping_mode", "payment_type", "market", "order_region", "order_country",
        "customer_segment", "department_name", "category_name",
        "order_item_quantity", "discount_rate", "net_sales", "split", "is_canceled_shipment",
    ]].copy()
    out["actual_late"] = df_all[TARGET].astype(int)
    out["pred_prob_late"] = prob.round(4)
    out["pred_label"] = (prob >= DECISION_THRESHOLD).astype(int)
    out["risk_band"] = risk_band(prob)
    out["is_correct"] = (out["pred_label"] == out["actual_late"]).astype(int)
    out["current_sla_days"] = out["shipping_mode"].map(CURRENT_SLA)
    out["model_name"] = model_name
    # Đơn hủy không có "đúng hạn/trễ hạn" thực sự → không dùng để đánh giá
    out["eval_eligible"] = (out["is_canceled_shipment"] == 0).astype(int)
    return out


# ---------------------------------------------------------------------------
# 2. Lưới What-If Simulator (Feature 6)
# ---------------------------------------------------------------------------
def build_whatif_grid(model, reference: pd.DataFrame) -> pd.DataFrame:
    """
    Tích Descartes các tham số điều phối viên có thể chỉnh trên Dashboard; các feature còn lại
    cố định ở giá trị điển hình (mode/median) của tập train. order_region chọn theo market tương ứng.
    """
    base = {c: reference[c].mode().iloc[0] for c in CATEGORICAL_FEATURES}
    base.update({c: float(reference[c].median()) for c in NUMERIC_FEATURES})
    region_by_market = reference.groupby("market")["order_region"].agg(lambda s: s.mode().iloc[0]).to_dict()
    median_price = float(reference["product_price"].median())

    keys = list(WHATIF_GRID.keys())
    rows = []
    for combo in itertools.product(*WHATIF_GRID.values()):
        r = dict(base)
        r.update(dict(zip(keys, combo)))
        r["order_region"] = region_by_market.get(r["market"], base["order_region"])
        r["net_sales"] = median_price * r["order_item_quantity"] * (1 - r["discount_rate"])
        r["is_weekend"] = int(r["order_dayofweek"] >= 6)
        rows.append(r)
    grid = pd.DataFrame(rows)
    grid["pred_prob_late"] = model.predict_proba(grid[FEATURES])[:, 1].round(4)
    grid["risk_band"] = risk_band(grid["pred_prob_late"])
    grid["current_sla_days"] = grid["shipping_mode"].map(CURRENT_SLA)

    # Gợi ý hành động: phương thức có xác suất trễ thấp nhất với cùng bối cảnh
    ctx = ["order_hour", "market", "discount_rate", "order_item_quantity"]
    best = grid.loc[grid.groupby(ctx)["pred_prob_late"].idxmin(), ctx + ["shipping_mode", "pred_prob_late"]]
    best = best.rename(columns={"shipping_mode": "recommended_mode", "pred_prob_late": "recommended_mode_prob"})
    grid = grid.merge(best, on=ctx, how="left")
    keep = ["shipping_mode", "order_hour", "market", "order_region", "discount_rate", "order_item_quantity",
            "net_sales", "current_sla_days", "pred_prob_late", "risk_band", "recommended_mode",
            "recommended_mode_prob"]
    return grid[keep]


# ---------------------------------------------------------------------------
# 3. Kịch bản SLA (hỗ trợ What-If + Feature 8 của Vương)
# ---------------------------------------------------------------------------
def build_sla_scenarios(df_all: pd.DataFrame) -> pd.DataFrame:
    """
    Với mỗi shipping_mode và mỗi mức SLA đề xuất s ∈ [0..7] ngày:
    expected_late_rate = P(days_shipping_real > s) theo phân phối thực nghiệm (loại đơn hủy).
    """
    d = df_all[df_all["is_canceled_shipment"] == 0]
    rows = []
    for mode, g in d.groupby("shipping_mode"):
        real = g["days_shipping_real"].to_numpy()
        cur = CURRENT_SLA[mode]
        cur_rate = float((real > cur).mean())
        for s in range(0, 8):
            rate = float((real > s).mean())
            rows.append({
                "shipping_mode": mode, "current_sla_days": cur, "proposed_sla_days": s,
                "is_current": int(s == cur), "shipments": len(g),
                "expected_late_rate": rate, "expected_otd_rate": 1 - rate,
                "current_late_rate": cur_rate,
                "late_shipments_avoided": int(round((cur_rate - rate) * len(g))),
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 4. Hình vẽ cho Báo cáo (PNG)
# ---------------------------------------------------------------------------
def make_figures(comparison, roc_df, cm_best, fi_best, calib_best, df_all, best_name, ceiling_auc):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.rcParams.update({"figure.dpi": 130, "axes.spines.top": False, "axes.spines.right": False})

    # 4.1 So sánh ROC-AUC
    comp = comparison.sort_values("roc_auc")
    fig, ax = plt.subplots(figsize=(8, 4.5))
    colors = ["#9aa5b1" if t == "reference" else ("#d1495b" if n == best_name else "#2e86ab")
              for n, t in zip(comp["model"], comp["model_type"])]
    ax.barh(comp["model"], comp["roc_auc"], color=colors)
    ax.axvline(ceiling_auc, ls="--", c="#d1495b", lw=1)
    ax.text(ceiling_auc, -0.7, f" trần lý thuyết (test) {ceiling_auc:.3f}", color="#d1495b", fontsize=8)
    if "roc_auc_ci_low" in comp:
        ax.errorbar(comp["roc_auc"], range(len(comp)), xerr=[comp["roc_auc"] - comp["roc_auc_ci_low"],
                    comp["roc_auc_ci_high"] - comp["roc_auc"]], fmt="none", ecolor="black", lw=0.8, capsize=2)
    ax.set_xlim(0.5, 1.0); ax.set_xlabel("ROC-AUC (tập test out-of-time)")
    ax.set_title("So sánh mô hình dự báo trễ hạn")
    for y, v in enumerate(comp["roc_auc"]):
        ax.text(v + 0.003, y, f"{v:.3f}", va="center", fontsize=8)
    fig.tight_layout(); fig.savefig(FIGURE_DIR / "01_model_comparison_auc.png"); plt.close(fig)

    # 4.2 ROC curves
    fig, ax = plt.subplots(figsize=(5.5, 5.5))
    for name, g in roc_df.groupby("model"):
        ax.plot(g["fpr"], g["tpr"], lw=2 if name == best_name else 1, label=name)
    ax.plot([0, 1], [0, 1], "k:", lw=0.8)
    ax.set_xlabel("False Positive Rate"); ax.set_ylabel("True Positive Rate")
    ax.set_title("ROC Curves (test)"); ax.legend(fontsize=7, loc="lower right")
    fig.tight_layout(); fig.savefig(FIGURE_DIR / "02_roc_curves.png"); plt.close(fig)

    # 4.3 Confusion matrix
    m = cm_best.pivot(index="actual", columns="predicted", values="count").loc[["On-time", "Late"], ["On-time", "Late"]]
    fig, ax = plt.subplots(figsize=(4.5, 4))
    ax.imshow(m.values, cmap="Blues")
    for i in range(2):
        for j in range(2):
            ax.text(j, i, f"{m.values[i, j]:,}", ha="center", va="center",
                    color="white" if m.values[i, j] > m.values.max() / 2 else "black")
    ax.set_xticks([0, 1], m.columns); ax.set_yticks([0, 1], m.index)
    ax.set_xlabel("Dự báo"); ax.set_ylabel("Thực tế"); ax.set_title(f"Confusion Matrix — {best_name}")
    fig.tight_layout(); fig.savefig(FIGURE_DIR / "03_confusion_matrix_best.png"); plt.close(fig)

    # 4.4 Feature importance
    fi = fi_best.sort_values("importance_auc_drop")
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.barh(fi["feature"], fi["importance_auc_drop"], xerr=fi["importance_std"], color="#2e86ab")
    ax.set_xlabel("Mức giảm ROC-AUC khi xáo trộn feature"); ax.set_title(f"Permutation Importance — {best_name}")
    fig.tight_layout(); fig.savefig(FIGURE_DIR / "04_feature_importance_best.png"); plt.close(fig)

    # 4.5 Calibration
    fig, ax = plt.subplots(figsize=(5, 5))
    ax.plot([0, 1], [0, 1], "k:", lw=0.8)
    ax.plot(calib_best["mean_predicted"], calib_best["observed_late_rate"], "o-", c="#d1495b")
    ax.set_xlabel("Xác suất dự báo trung bình"); ax.set_ylabel("Tỷ lệ trễ thực tế")
    ax.set_title(f"Calibration — {best_name}")
    fig.tight_layout(); fig.savefig(FIGURE_DIR / "05_calibration_best.png"); plt.close(fig)

    # 4.6 Khám phá cơ chế: Same Day theo giờ đặt & phân phối ngày giao thực tế
    d = df_all[df_all["is_canceled_shipment"] == 0]
    fig, axes = plt.subplots(1, 2, figsize=(11, 4))
    sd = d[d["shipping_mode"] == "Same Day"].groupby("order_hour")[TARGET].mean()
    axes[0].bar(sd.index, sd.values, color=["#2e86ab" if h < 12 else "#d1495b" for h in sd.index])
    axes[0].set_xlabel("Giờ đặt hàng"); axes[0].set_ylabel("Tỷ lệ trễ")
    axes[0].set_title("Same Day: cut-off 12:00 quyết định trễ hạn")
    dist = pd.crosstab(d["shipping_mode"], d["days_shipping_real"], normalize="index")
    dist.T.plot(kind="bar", ax=axes[1], width=0.8)
    axes[1].set_xlabel("Số ngày giao thực tế"); axes[1].set_ylabel("Tỷ lệ")
    axes[1].set_title("Phân phối ngày giao thực tế theo Shipping Mode"); axes[1].legend(fontsize=7)
    fig.tight_layout(); fig.savefig(FIGURE_DIR / "06_data_generating_process.png"); plt.close(fig)


# ---------------------------------------------------------------------------
# 5. (Tùy chọn) Đẩy kết quả lên BigQuery để Power BI dùng chung 1 nguồn
# ---------------------------------------------------------------------------
def upload_to_bigquery(tables: dict) -> None:
    from etl.config import GCP_PROJECT_ID, BQ_DATASET_ID
    try:
        from google.cloud import bigquery
        client = bigquery.Client(project=GCP_PROJECT_ID)
    except Exception as e:  # noqa: BLE001
        print(f"[!] Không kết nối được BigQuery ({e}) — bỏ qua upload. Dùng file CSV trong data/ml/.")
        return
    cfg = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    for name, df in tables.items():
        ref = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.{name}"
        client.load_table_from_dataframe(df, ref, job_config=cfg).result()
        print(f"    [+] BigQuery: {ref} ({len(df):,} dòng)")
