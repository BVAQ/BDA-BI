"""
build_pbip.py — Sinh Power BI Project (.pbip) cho Dashboard TRANG 3 (Predictive Delay Risk — Quân).

Output (mở bằng Power BI Desktop → File → Open → *.pbip):
  dashboard/Trang3_PredictiveRisk.pbip
  dashboard/Trang3_PredictiveRisk.SemanticModel/   (TMDL: 15 bảng, quan hệ, ~45 measure DAX)
  dashboard/Trang3_PredictiveRisk.Report/          (PBIR: 3 trang, ~45 visual, dark theme)

Nguồn dữ liệu: các file Parquet trong btl/data/ml/ (sinh bởi `python run_pipeline.py --step 7`).
Đường dẫn được đặt trong tham số Power Query `DataFolder` (đổi trong Transform data → Manage Parameters).

Chạy:  python dashboard/build_pbip.py [--data-folder "D:\\...\\btl\\data\\ml"]
"""

import argparse
import json
import shutil
import uuid
from pathlib import Path

import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent
BTL_DIR = ROOT.parent
NAME = "Trang3_PredictiveRisk"
NS = uuid.UUID("6f1d3c2a-9b7e-4c1d-8a2f-5e4b3c2d1a0f")   # namespace cho ID tất định

SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition"
V_VISUAL, V_PAGE, V_REPORT = "2.8.0", "2.1.0", "3.2.0"

# Theme "DataCo Logistics Harbor" (dashboard/themes/DataCo_Harbor_Theme.json) — màu dùng cho text/KPI
THEME_FILE = ROOT / "themes" / "DataCo_Harbor_Theme.json"
C = dict(bg="#0A1626", panel="#0F1F36", panel2="#132744", border="#1E3454", text="#E9EEF4",
         header="#C9D6E5", muted="#8CA0B8", title="#E9EEF4", teal="#3CC8DA", amber="#F2A33A",
         green="#2FB98A", red="#E8564A", indigo="#7C8CF8", blue="#5B9BD5")


def uid(key: str) -> str:
    return str(uuid.uuid5(NS, key))


def vname(key: str) -> str:
    return uuid.uuid5(NS, key).hex[:20]


# =============================================================================
# 1. SEMANTIC MODEL (TMDL)
# =============================================================================
PARQUET_TABLES = [
    "ml_model_comparison", "ml_roc_curve", "ml_confusion_matrix", "ml_feature_importance",
    "ml_calibration", "ml_threshold_metrics", "ml_leakage_demo", "ml_auc_by_mode",
    "ml_segment_accuracy", "ml_predictions", "ml_whatif_grid", "ml_sla_scenarios",
]

ARROW_TO_TMDL = {"int8": "int64", "int16": "int64", "int32": "int64", "int64": "int64",
                 "double": "double", "float": "double", "string": "string", "large_string": "string",
                 "bool": "boolean", "date32[day]": "dateTime"}

PERCENT_COLS = {"accuracy", "precision", "recall", "f1", "pct_of_total", "flagged_pct", "late_rate",
                "actual_late_rate", "predicted_late_rate", "expected_late_rate", "expected_otd_rate",
                "current_late_rate", "discount_rate", "pred_prob_late", "recommended_mode_prob",
                "pct_of_ceiling_cv_auc", "observed_late_rate", "mean_predicted", "mean_pred"}

# Bảng chiều phục vụ slicer dùng chung
DIM_SHIPPING_MODE_M = """let
    Source = #table(
        type table [shipping_mode = text, sla_days = Int64.Type, mode_order = Int64.Type, service_tier = text],
        {
            {"Same Day", 0, 1, "Express"},
            {"First Class", 1, 2, "Premium"},
            {"Second Class", 2, 3, "Standard"},
            {"Standard Class", 4, 4, "Economy"}
        })
in
    Source"""

DIM_MODEL_M = """let
    Source = Parquet.Document(File.Contents(DataFolder & "ml_model_comparison.parquet")),
    Selected = Table.SelectColumns(Source, {"model", "model_type", "is_best"}),
    Ordered = Table.AddIndexColumn(Selected, "model_order", 1, 1, Int64.Type)
in
    Ordered"""

MEASURES_M = """let
    Source = #table(type table [Dummy = text], {})
in
    Source"""

RELATIONSHIPS = [  # (from_table.col, to_table.col)  — many-to-one
    ("ml_whatif_grid.shipping_mode", "dim_shipping_mode.shipping_mode"),
    ("ml_sla_scenarios.shipping_mode", "dim_shipping_mode.shipping_mode"),
    ("ml_predictions.shipping_mode", "dim_shipping_mode.shipping_mode"),
    ("ml_segment_accuracy.shipping_mode", "dim_shipping_mode.shipping_mode"),
    ("ml_roc_curve.model", "dim_model.model"),
    ("ml_confusion_matrix.model", "dim_model.model"),
    ("ml_feature_importance.model", "dim_model.model"),
    ("ml_calibration.model", "dim_model.model"),
    ("ml_auc_by_mode.model", "dim_model.model"),
    ("ml_model_comparison.model", "dim_model.model"),
]

# (tên, DAX, formatString, displayFolder)
SEL_MODEL = "VAR m = [Selected Model]\n"
MEASURES = [
    # ---------- F5: Model evaluation ----------
    ("Best Model", 'CALCULATE(SELECTEDVALUE(ml_model_comparison[model]), REMOVEFILTERS(dim_model), ml_model_comparison[is_best] = 1)', None, "F5 Model"),
    ("Selected Model", 'IF(HASONEVALUE(dim_model[model]) && ISFILTERED(dim_model[model]), VALUES(dim_model[model]), [Best Model])', None, "F5 Model"),
    ("Model Accuracy", SEL_MODEL + "RETURN CALCULATE(MAX(ml_model_comparison[accuracy]), REMOVEFILTERS(dim_model), ml_model_comparison[model] = m)", "0.0%", "F5 Model"),
    ("Model Precision", SEL_MODEL + "RETURN CALCULATE(MAX(ml_model_comparison[precision]), REMOVEFILTERS(dim_model), ml_model_comparison[model] = m)", "0.0%", "F5 Model"),
    ("Model Recall", SEL_MODEL + "RETURN CALCULATE(MAX(ml_model_comparison[recall]), REMOVEFILTERS(dim_model), ml_model_comparison[model] = m)", "0.0%", "F5 Model"),
    ("Model F1", SEL_MODEL + "RETURN CALCULATE(MAX(ml_model_comparison[f1]), REMOVEFILTERS(dim_model), ml_model_comparison[model] = m)", "0.000", "F5 Model"),
    ("Model ROC-AUC", SEL_MODEL + "RETURN CALCULATE(MAX(ml_model_comparison[roc_auc]), REMOVEFILTERS(dim_model), ml_model_comparison[model] = m)", "0.000", "F5 Model"),
    ("Model PR-AUC", SEL_MODEL + "RETURN CALCULATE(MAX(ml_model_comparison[pr_auc]), REMOVEFILTERS(dim_model), ml_model_comparison[model] = m)", "0.000", "F5 Model"),
    ("Model Brier", SEL_MODEL + "RETURN CALCULATE(MAX(ml_model_comparison[brier]), REMOVEFILTERS(dim_model), ml_model_comparison[model] = m)", "0.000", "F5 Model"),
    ("Model CV ROC-AUC", SEL_MODEL + "RETURN CALCULATE(MAX(ml_model_comparison[cv_roc_auc_mean]), REMOVEFILTERS(dim_model), ml_model_comparison[model] = m)", "0.000", "F5 Model"),
    ("Model % of Ceiling", SEL_MODEL + "RETURN CALCULATE(MAX(ml_model_comparison[pct_of_ceiling_cv_auc]), REMOVEFILTERS(dim_model), ml_model_comparison[model] = m)", "0.0%", "F5 Model"),
    ("Ceiling ROC-AUC", 'CALCULATE(MAX(ml_model_comparison[roc_auc]), REMOVEFILTERS(dim_model), ml_model_comparison[model] = "Theoretical_Ceiling")', "0.000", "F5 Model"),
    ("Baseline ROC-AUC", 'CALCULATE(MAX(ml_model_comparison[roc_auc]), REMOVEFILTERS(dim_model), ml_model_comparison[model_type] = "baseline")', "0.000", "F5 Model"),
    ("AUC Lift vs Baseline", "[Model ROC-AUC] - [Baseline ROC-AUC]", "+0.000;-0.000", "F5 Model"),
    ("ROC-AUC (by model)", "MAX(ml_model_comparison[roc_auc])", "0.000", "F5 Model"),
    ("CV ROC-AUC (by model)", "MAX(ml_model_comparison[cv_roc_auc_mean])", "0.000", "F5 Model"),
    ("Accuracy (by model)", "MAX(ml_model_comparison[accuracy])", "0.0%", "F5 Model"),
    ("Leakage ROC-AUC", 'CALCULATE(MAX(ml_leakage_demo[roc_auc]), ml_leakage_demo[model] = "RandomForest")', "0.000", "F5 Model"),
    ("Leakage Accuracy", 'CALCULATE(MAX(ml_leakage_demo[accuracy]), ml_leakage_demo[model] = "RandomForest")', "0.0%", "F5 Model"),
    ("Leakage Warning", '"Using days_shipping_real (known only AFTER delivery): AUC = " & FORMAT([Leakage ROC-AUC], "0.000") & " — perfect but useless"', None, "F5 Model"),
    ("CM Count", SEL_MODEL + "RETURN CALCULATE(SUM(ml_confusion_matrix[count]), REMOVEFILTERS(dim_model), ml_confusion_matrix[model] = m)", "#,0", "F5 Model"),
    ("CM % of Actual", "DIVIDE([CM Count], CALCULATE([CM Count], REMOVEFILTERS(ml_confusion_matrix[predicted])))", "0.0%", "F5 Model"),
    ("Feature Importance", SEL_MODEL + "RETURN CALCULATE(MAX(ml_feature_importance[importance_auc_drop]), REMOVEFILTERS(dim_model), ml_feature_importance[model] = m)", "0.0000", "F5 Model"),
    ("Model Subtitle", '"Viewing: " & [Selected Model] & "  ·  Out-of-time test ≥ 2017-07-01  ·  5-fold GroupKFold CV"', None, "F5 Model"),
    # ---------- Threshold ----------
    ("Selected Threshold", "IF(HASONEVALUE(ml_threshold_metrics[threshold]), VALUES(ml_threshold_metrics[threshold]), 0.5)", "0.00", "F5 Threshold"),
    ("Thr Precision", "VAR t = [Selected Threshold]\nRETURN CALCULATE(MAX(ml_threshold_metrics[precision]), FILTER(ALL(ml_threshold_metrics[threshold]), ABS(ml_threshold_metrics[threshold] - t) < 0.001))", "0.0%", "F5 Threshold"),
    ("Thr Recall", "VAR t = [Selected Threshold]\nRETURN CALCULATE(MAX(ml_threshold_metrics[recall]), FILTER(ALL(ml_threshold_metrics[threshold]), ABS(ml_threshold_metrics[threshold] - t) < 0.001))", "0.0%", "F5 Threshold"),
    ("Thr Flagged %", "VAR t = [Selected Threshold]\nRETURN CALCULATE(MAX(ml_threshold_metrics[flagged_pct]), FILTER(ALL(ml_threshold_metrics[threshold]), ABS(ml_threshold_metrics[threshold] - t) < 0.001))", "0.0%", "F5 Threshold"),
    ("Thr Late Caught", "VAR t = [Selected Threshold]\nRETURN CALCULATE(MAX(ml_threshold_metrics[late_caught]), FILTER(ALL(ml_threshold_metrics[threshold]), ABS(ml_threshold_metrics[threshold] - t) < 0.001))", "#,0", "F5 Threshold"),
    ("Thr False Alarms", "VAR t = [Selected Threshold]\nRETURN CALCULATE(MAX(ml_threshold_metrics[false_alarms]), FILTER(ALL(ml_threshold_metrics[threshold]), ABS(ml_threshold_metrics[threshold] - t) < 0.001))", "#,0", "F5 Threshold"),
    # ---------- F5: Risk scoring ----------
    ("Shipments", "COUNTROWS(ml_predictions)", "#,0", "F5 Scoring"),
    ("Scored Shipments", "CALCULATE(COUNTROWS(ml_predictions), ml_predictions[eval_eligible] = 1)", "#,0", "F5 Scoring"),
    ("Actual Late Rate", "CALCULATE(AVERAGE(ml_predictions[actual_late]), ml_predictions[eval_eligible] = 1)", "0.0%", "F5 Scoring"),
    ("Predicted Late Rate", "CALCULATE(AVERAGE(ml_predictions[pred_prob_late]), ml_predictions[eval_eligible] = 1)", "0.0%", "F5 Scoring"),
    ("Prediction Accuracy", "CALCULATE(AVERAGE(ml_predictions[is_correct]), ml_predictions[eval_eligible] = 1)", "0.0%", "F5 Scoring"),
    ("High-Risk Shipments", 'CALCULATE(COUNTROWS(ml_predictions), ml_predictions[risk_band] IN {"3-High", "4-Critical"})', "#,0", "F5 Scoring"),
    ("High-Risk Share", "DIVIDE([High-Risk Shipments], [Shipments])", "0.0%", "F5 Scoring"),
    ("Critical Orders", 'CALCULATE(DISTINCTCOUNT(ml_predictions[order_id]), ml_predictions[risk_band] = "4-Critical")', "#,0", "F5 Scoring"),
    # ---------- F6: What-If ----------
    ("What-If P(Late)", "AVERAGE(ml_whatif_grid[pred_prob_late])", "0.0%", "F6 What-If"),
    ("What-If Is Complete",
     "HASONEVALUE(ml_whatif_grid[shipping_mode]) && HASONEVALUE(ml_whatif_grid[order_hour]) && HASONEVALUE(ml_whatif_grid[market])"
     " && HASONEVALUE(ml_whatif_grid[discount_rate]) && HASONEVALUE(ml_whatif_grid[order_item_quantity])", None, "F6 What-If"),
    ("What-If Risk Band",
     "IF(NOT [What-If Is Complete], \"—\",\n    VAR p = [What-If P(Late)]\n    RETURN SWITCH(TRUE(), p >= 0.85, \"4-Critical\", p >= 0.6, \"3-High\", p >= 0.3, \"2-Medium\", \"1-Low\"))",
     None, "F6 What-If"),
    ("What-If Current SLA",
     'IF(HASONEVALUE(ml_whatif_grid[shipping_mode]), "Promised SLA: " & SELECTEDVALUE(ml_whatif_grid[current_sla_days]) & " day(s)", "Select a mode")',
     None, "F6 What-If"),
    ("What-If Recommendation",
     "IF([What-If Is Complete],\n"
     "    VAR cur = SELECTEDVALUE(ml_whatif_grid[shipping_mode])\n"
     "    VAR rec = SELECTEDVALUE(ml_whatif_grid[recommended_mode])\n"
     "    VAR rp = SELECTEDVALUE(ml_whatif_grid[recommended_mode_prob])\n"
     "    RETURN IF(rec = cur, \"✔ \" & cur & \" is already the lowest-risk option\", \"➜ Switch to \" & rec & \" — P(late) drops to \" & FORMAT(rp, \"0%\")),\n"
     "    \"Pick one value for every input to get a recommendation\")", None, "F6 What-If"),
    ("What-If Risk Reduction",
     "IF([What-If Is Complete], [What-If P(Late)] - SELECTEDVALUE(ml_whatif_grid[recommended_mode_prob]))", "0.0%", "F6 What-If"),
    # ---------- F6/F8: SLA scenarios ----------
    ("SLA Current Late Rate", "DIVIDE(SUMX(ml_sla_scenarios, ml_sla_scenarios[current_late_rate] * ml_sla_scenarios[shipments]), SUM(ml_sla_scenarios[shipments]))", "0.0%", "F6 SLA"),
    ("SLA Expected Late Rate", "DIVIDE(SUMX(ml_sla_scenarios, ml_sla_scenarios[expected_late_rate] * ml_sla_scenarios[shipments]), SUM(ml_sla_scenarios[shipments]))", "0.0%", "F6 SLA"),
    ("SLA Expected OTD", "IF(HASONEVALUE(ml_sla_scenarios[proposed_sla_days]), 1 - [SLA Expected Late Rate])", "0.0%", "F6 SLA"),
    ("SLA Shipments Avoided", "IF(HASONEVALUE(ml_sla_scenarios[proposed_sla_days]), SUM(ml_sla_scenarios[late_shipments_avoided]))", "#,0", "F6 SLA"),
    ("What-If Risk Reduction Label", 'IF([What-If Is Complete], FORMAT([What-If Risk Reduction], "0.0%"), "—")', None, "F6 What-If"),
    ("SLA Late Rate Label", 'IF(HASONEVALUE(ml_sla_scenarios[proposed_sla_days]), FORMAT([SLA Expected Late Rate], "0.0%"), "—")', None, "F6 SLA"),
    ("SLA OTD Label", 'IF(HASONEVALUE(ml_sla_scenarios[proposed_sla_days]), FORMAT([SLA Expected OTD], "0.0%"), "—")', None, "F6 SLA"),
    ("SLA Avoided Label", 'IF(HASONEVALUE(ml_sla_scenarios[proposed_sla_days]), FORMAT([SLA Shipments Avoided], "#,0"), "—")', None, "F6 SLA"),
    ("SLA Caption",
     'IF(HASONEVALUE(ml_sla_scenarios[proposed_sla_days]), "Proposed SLA = " & VALUES(ml_sla_scenarios[proposed_sla_days]) & " day(s)", "Select a proposed SLA")',
     None, "F6 SLA"),
]


def _tmdl_name(n: str) -> str:
    return n if n.replace("_", "").isalnum() else "'" + n.replace("'", "''") + "'"


def _indent(text: str, tabs: int) -> str:
    return "\n".join("\t" * tabs + line for line in text.splitlines())


def _partition(table: str, m: str) -> str:
    return (f"\tpartition {_tmdl_name(table)} = m\n\t\tmode: import\n\t\tsource =\n"
            f"{_indent(m, 4)}\n")


def _column(name, dtype, table, fmt=None, hidden=False, sort_by=None):
    lines = [f"\tcolumn {_tmdl_name(name)}", f"\t\tdataType: {dtype}"]
    if fmt:
        lines.append(f"\t\tformatString: {fmt}")
    if hidden:
        lines.append("\t\tisHidden")
    lines += [f"\t\tlineageTag: {uid(table + '.' + name)}", "\t\tsummarizeBy: none",
              f"\t\tsourceColumn: {name}"]
    if sort_by:
        lines.append(f"\t\tsortByColumn: {sort_by}")
    lines += ["", "\t\tannotation SummarizationSetBy = Automatic", ""]
    return "\n".join(lines)


def _table(name, columns, m, measures=None, hidden=False):
    out = [f"table {_tmdl_name(name)}"]
    if hidden:
        out.append("\tisHidden")
    out += [f"\tlineageTag: {uid('table.' + name)}", ""]
    for (mname, dax, fmt, folder) in (measures or []):
        if "\n" in dax:
            out.append(f"\tmeasure {_tmdl_name(mname)} =\n{_indent(dax, 3)}")
        else:
            out.append(f"\tmeasure {_tmdl_name(mname)} = {dax}")
        if fmt:
            out.append(f"\t\tformatString: {fmt}")
        out.append(f"\t\tdisplayFolder: {folder}")
        out.append(f"\t\tlineageTag: {uid('measure.' + mname)}")
        out.append("")
    out += columns
    out.append(_partition(name, m))
    out.append("\tannotation PBI_ResultType = Table\n")
    return "\n".join(out)


def build_semantic_model(sm_dir: Path, data_folder: str) -> list:
    d = sm_dir / "definition"
    (d / "tables").mkdir(parents=True, exist_ok=True)
    table_files = {}

    for t in PARQUET_TABLES:
        schema = pq.read_schema(Path(data_folder) / f"{t}.parquet")
        cols = []
        for f in schema:
            dtype = ARROW_TO_TMDL.get(str(f.type))
            if dtype is None:
                raise TypeError(f"{t}.{f.name}: kiểu Parquet {f.type} chưa được ánh xạ")
            fmt = "0.0%" if f.name in PERCENT_COLS else ("Short Date" if dtype == "dateTime" else None)
            cols.append(_column(f.name, dtype, t, fmt))
        m = (f'let\n    Source = Parquet.Document(File.Contents(DataFolder & "{t}.parquet"))\nin\n    Source')
        table_files[t] = _table(t, cols, m)

    table_files["dim_shipping_mode"] = _table("dim_shipping_mode", [
        _column("shipping_mode", "string", "dim_shipping_mode", sort_by="mode_order"),
        _column("sla_days", "int64", "dim_shipping_mode"),
        _column("mode_order", "int64", "dim_shipping_mode", hidden=True),
        _column("service_tier", "string", "dim_shipping_mode"),
    ], DIM_SHIPPING_MODE_M)
    table_files["dim_model"] = _table("dim_model", [
        _column("model", "string", "dim_model", sort_by="model_order"),
        _column("model_type", "string", "dim_model"),
        _column("is_best", "int64", "dim_model"),
        _column("model_order", "int64", "dim_model", hidden=True),
    ], DIM_MODEL_M)
    table_files["_Measures"] = _table("_Measures", [
        _column("Dummy", "string", "_Measures", hidden=True)], MEASURES_M, measures=MEASURES)

    for t, body in table_files.items():
        (d / "tables" / f"{t}.tmdl").write_text(body, encoding="utf-8")

    (d / "database.tmdl").write_text("database\n\tcompatibilityLevel: 1600\n\n", encoding="utf-8")
    order = ["_Measures", "dim_model", "dim_shipping_mode"] + PARQUET_TABLES
    model = ["model Model", "\tculture: en-US", "\tdefaultPowerBIDataSourceVersion: powerBI_V3",
             "\tsourceQueryCulture: en-US", "\tdataAccessOptions", "\t\tlegacyRedirects",
             "\t\treturnErrorValuesAsNull", "",
             "annotation __PBI_TimeIntelligenceEnabled = 0", "",
             f"annotation PBI_QueryOrder = {json.dumps(['DataFolder'] + order)}", ""]
    model += [f"ref table {_tmdl_name(t)}" for t in order] + [""]
    (d / "model.tmdl").write_text("\n".join(model), encoding="utf-8")

    folder = data_folder.rstrip("\\/") + "\\"
    (d / "expressions.tmdl").write_text(
        f'expression DataFolder = "{folder}" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]\n'
        f"\tlineageTag: {uid('expr.DataFolder')}\n\n\tannotation PBI_ResultType = Text\n\n", encoding="utf-8")

    rel = []
    for f, t in RELATIONSHIPS:
        rel += [f"relationship {uid('rel.' + f + t)}", f"\tfromColumn: {f}", f"\ttoColumn: {t}", ""]
    (d / "relationships.tmdl").write_text("\n".join(rel), encoding="utf-8")

    (sm_dir / "definition.pbism").write_text(json.dumps({"version": "4.0", "settings": {}}, indent=2), encoding="utf-8")
    (sm_dir / ".platform").write_text(json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {"type": "SemanticModel", "displayName": NAME},
        "config": {"version": "2.0", "logicalId": uid("sm")}}, indent=2), encoding="utf-8")
    return order


# =============================================================================
# 2. REPORT (PBIR)
# =============================================================================
def lit(v):
    if isinstance(v, bool):
        s = "true" if v else "false"
    elif isinstance(v, int):
        s = f"{v}L"
    elif isinstance(v, float):
        s = f"{v}D"
    else:
        s = "'" + str(v).replace("'", "''") + "'"
    return {"expr": {"Literal": {"Value": s}}}


def color(hexv):
    return {"solid": {"color": lit(hexv)}}


def col(t, c):
    return {"Column": {"Expression": {"SourceRef": {"Entity": t}}, "Property": c}}


def meas(m, t="_Measures"):
    return {"Measure": {"Expression": {"SourceRef": {"Entity": t}}, "Property": m}}


AGG = {"Sum": 0, "Avg": 1, "Min": 3, "Max": 4, "Count": 5}


def agg(t, c, fn):
    return {"Aggregation": {"Expression": col(t, c), "Function": AGG[fn]}}


def proj(field, display=None):
    if "Measure" in field:
        e = field["Measure"]
        ref = f"{e['Expression']['SourceRef']['Entity']}.{e['Property']}"
        native = e["Property"]
    elif "Column" in field:
        e = field["Column"]
        ref = f"{e['Expression']['SourceRef']['Entity']}.{e['Property']}"
        native = e["Property"]
    else:
        fn = {v: k for k, v in AGG.items()}[field["Aggregation"]["Function"]]
        e = field["Aggregation"]["Expression"]["Column"]
        ref = f"{fn}({e['Expression']['SourceRef']['Entity']}.{e['Property']})"
        native = f"{fn} of {e['Property']}"
    p = {"field": field, "queryRef": ref, "nativeQueryRef": native}
    if display:
        p["displayName"] = display
    return p


def container_objects(title=None, subtitle=None, bg=None, border=None):
    """Chỉ đặt tiêu đề/phụ đề; nền, viền, font màu do theme Harbor quyết định (trừ khi override)."""
    o = {}
    if title:
        o["title"] = [{"properties": {"show": lit(True), "text": lit(title), "fontSize": lit(12.0)}}]  # theme 15pt → 12pt cho lưới 1280×720
    else:
        o["title"] = [{"properties": {"show": lit(False)}}]
    if subtitle:
        o["subTitle"] = [{"properties": {"show": lit(True), "text": lit(subtitle), "fontSize": lit(9.0)}}]
    if bg is not None:
        o["background"] = [{"properties": {"show": lit(True), "color": color(bg), "transparency": lit(0.0)}}]
    if border is not None:
        o["border"] = [{"properties": {"show": lit(border)}}]
    return o


class Page:
    def __init__(self, key, display):
        self.key, self.name, self.display = key, vname("page." + key), display
        self.visuals, self.interactions, self._z = [], [], 0

    def add(self, key, vtype, x, y, w, h, query=None, objects=None, title=None, subtitle=None,
            sort=None, bg=None, border=None, filters=None):
        name = vname(self.key + "." + key)
        visual = {"visualType": vtype}
        if query is not None:
            q = {"queryState": {role: {"projections": ps} for role, ps in query.items()}}
            if sort:
                q["sortDefinition"] = {"sort": [{"field": f, "direction": d} for f, d in sort], "isDefaultSort": False}
            visual["query"] = q
        if objects:
            visual["objects"] = objects
        visual["visualContainerObjects"] = container_objects(title, subtitle, bg, border)
        visual["drillFilterOtherVisuals"] = True
        vc = {"$schema": f"{SCHEMA}/visualContainer/{V_VISUAL}/schema.json", "name": name,
              "position": {"x": x, "y": y, "z": self._z * 1000, "height": h, "width": w, "tabOrder": self._z * 1000},
              "visual": visual}
        if filters:
            vc["filterConfig"] = {"filters": filters}
        self._z += 1
        self.visuals.append(vc)
        return name

    def no_filter(self, source, targets):
        for t in targets:
            self.interactions.append({"source": source, "target": t, "type": "NoFilter"})


# ---------- helpers cho từng loại visual ----------
def textbox(page, key, x, y, w, h, runs, bg=None):
    paragraphs = []
    for para in runs:  # mỗi phần tử: list[(text, size, colorhex, bold)]
        paragraphs.append({"textRuns": [{"value": t, "textStyle": {
            "fontSize": f"{s}pt", "color": c, **({"fontWeight": "bold"} if b else {})}} for (t, s, c, b) in para]})
    return page.add(key, "textbox", x, y, w, h, objects={"general": [{"properties": {"paragraphs": paragraphs}}]},
                    bg=bg, border=(True if bg else None))


def card(page, key, x, y, w, h, measure, label, value_color=C["text"], size=18.0):
    h = max(h, 70)
    return page.add(key, "card", x, y, w, h, query={"Values": [proj(meas(measure), label)]}, objects={
        "labels": [{"properties": {"color": color(value_color), "fontSize": lit(size)}}],
        "categoryLabels": [{"properties": {"show": lit(True), "fontSize": lit(9.0)}}],
    })


def slicer(page, key, x, y, w, h, field, title, single=True, mode="Dropdown"):
    h = max(h, 70)
    return page.add(key, "slicer", x, y, w, h, query={"Values": [proj(field)]}, objects={
        "data": [{"properties": {"mode": lit(mode)}}],
        "selection": [{"properties": {"singleSelect": lit(single), "selectAllCheckboxEnabled": lit(False)}}],
        "header": [{"properties": {"show": lit(True), "text": lit(title), "textSize": lit(10.0)}}],
    })


def axis_objects(scalar=False):
    return {"categoryAxis": [{"properties": {"showAxisTitle": lit(False),
                                             **({"axisType": lit("Scalar")} if scalar else {})}}],
            "valueAxis": [{"properties": {"showAxisTitle": lit(False)}}],
            "legend": [{"properties": {"show": lit(True), "position": lit("Top")}}]}


def bar(page, key, x, y, w, h, cat, values, title, subtitle=None, series=None, sort_desc=True, labels=True,
        vtype="clusteredBarChart", fill=None, filters=None):
    q = {"Category": [proj(cat)], "Y": [proj(*v) if isinstance(v, tuple) else proj(v) for v in values]}
    if series is not None:
        q["Series"] = [proj(series)]
    obj = axis_objects()
    obj["labels"] = [{"properties": {"show": lit(labels), "fontSize": lit(9.0)}}]
    if fill:
        obj["dataPoint"] = [{"properties": {"fill": color(fill)}}]
    if series is None and len(values) == 1:
        obj["legend"] = [{"properties": {"show": lit(False)}}]
    first = values[0][0] if isinstance(values[0], tuple) else values[0]
    sort = [(first, "Descending")] if sort_desc else [(cat, "Ascending")]
    return page.add(key, vtype, x, y, w, h, query=q, objects=obj, title=title, subtitle=subtitle, sort=sort,
                    filters=filters)


def line(page, key, x, y, w, h, cat, values, title, subtitle=None, series=None, scalar=False):
    q = {"Category": [proj(cat)], "Y": [proj(*v) if isinstance(v, tuple) else proj(v) for v in values]}
    if series is not None:
        q["Series"] = [proj(series)]
    obj = axis_objects(scalar=scalar)
    obj["lineStyles"] = [{"properties": {"strokeWidth": lit(2)}}]
    return page.add(key, "lineChart", x, y, w, h, query=q, objects=obj, title=title, subtitle=subtitle,
                    sort=[(cat, "Ascending")])


def table_visual(page, key, x, y, w, h, fields, title, subtitle=None, sort=None, filters=None):
    obj = {"columnHeaders": [{"properties": {"fontSize": lit(9.0)}}],
           "values": [{"properties": {"fontSize": lit(9.0)}}],
           "total": [{"properties": {"totals": lit(False)}}]}
    return page.add(key, "tableEx", x, y, w, h, query={"Values": [proj(f, d) for f, d in fields]}, objects=obj,
                    title=title, subtitle=subtitle, sort=sort, filters=filters)


def categorical_filter(key, table, column, values):
    return {"name": vname("flt." + key), "field": col(table, column), "type": "Categorical",
            "filter": {"Version": 2, "From": [{"Name": "t", "Entity": table, "Type": 0}],
                       "Where": [{"Condition": {"In": {
                           "Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "t"}}, "Property": column}}],
                           "Values": [[{"Literal": {"Value": lit(v)["expr"]["Literal"]["Value"]}}] for v in values]}}}]}}


def header(page, title, subtitle):
    textbox(page, "hdr", 16, 4, 900, 66, [[(title, 16, C["title"], True)], [(subtitle, 10, C["muted"], False)]])
    textbox(page, "nav", 930, 14, 334, 40, [[("Page 3 · Predictive Delay Risk  |  CO4033 · DataCo", 9, C["header"], False)]])


# ---------------------------------------------------------------------------
# PAGE 1 — F5: Model Evaluation & Driver Attribution
# ---------------------------------------------------------------------------
def page_model_eval() -> Page:
    p = Page("p1", "3.1 Model Evaluation (F5)")
    header(p, "F5 · PRE-SHIPMENT ML RISK MODEL — EVALUATION & KEY DRIVERS",
           "Predicts late delivery at order creation · 6 models vs. Bayes-optimal theoretical ceiling · no target leakage")
    s_model = slicer(p, "s_model", 16, 74, 220, 70, col("dim_model", "model"), "Model (default: best)")
    cards = [("Selected Model", "Model", C["teal"], 14.0), ("Model ROC-AUC", "ROC-AUC (test)", C["text"], 20.0),
             ("Model Accuracy", "Accuracy", C["text"], 20.0), ("Model Precision", "Precision", C["text"], 20.0),
             ("Model Recall", "Recall", C["text"], 20.0), ("Model % of Ceiling", "% of ceiling (CV)", C["green"], 20.0),
             ("Leakage ROC-AUC", "AUC with leakage ⚠", C["red"], 20.0)]
    x = 244
    for i, (m, lbl, colr, sz) in enumerate(cards):
        card(p, f"c{i}", x, 74, 140, 70, m, lbl, colr, sz)
        x += 146
    auc_bar = bar(p, "auc", 16, 152, 400, 272, col("ml_model_comparison", "model"),
                  [(meas("ROC-AUC (by model)"), "Test ROC-AUC"), (meas("CV ROC-AUC (by model)"), "CV ROC-AUC")],
                  "ROC-AUC: Test vs. Cross-Validation", "Theoretical_Ceiling = upper bound for any honest model")
    roc = line(p, "roc", 424, 152, 420, 272, col("ml_roc_curve", "fpr"), [(agg("ml_roc_curve", "tpr", "Max"), "TPR")],
               "ROC Curves (out-of-time test set)", "X: False Positive Rate · Y: True Positive Rate",
               series=col("ml_roc_curve", "model"), scalar=True)
    fi = bar(p, "fi", 852, 152, 412, 272, col("ml_feature_importance", "feature"), [(meas("Feature Importance"), "ΔAUC")],
             "Key Drivers — Permutation Importance", "ROC-AUC drop when a feature is shuffled",
             fill=C["teal"])
    cm = p.add("cm", "pivotTable", 16, 432, 400, 200, query={
        "Rows": [proj(col("ml_confusion_matrix", "actual"), "Actual")],
        "Columns": [proj(col("ml_confusion_matrix", "predicted"), "Predicted")],
        "Values": [proj(meas("CM Count"), "Rows"), proj(meas("CM % of Actual"), "% of actual")]},
        objects={"subTotals": [{"properties": {"rowSubtotals": lit(False), "columnSubtotals": lit(False)}}]},
        title="Confusion Matrix (threshold 0.5)", subtitle="Late = delivered late · On-time = on time")
    s_thr = slicer(p, "s_thr", 424, 432, 150, 70, col("ml_threshold_metrics", "threshold"), "Threshold")
    for i, (m, lbl) in enumerate([("Thr Precision", "Precision"), ("Thr Recall", "Recall"),
                                  ("Thr Flagged %", "% flagged")]):
        card(p, f"t{i}", 580 + i * 88, 432, 84, 70, m, lbl, C["amber"], 13.0)
    thr = line(p, "thr", 424, 508, 420, 124, col("ml_threshold_metrics", "threshold"),
               [(agg("ml_threshold_metrics", "precision", "Max"), "Precision"),
                (agg("ml_threshold_metrics", "recall", "Max"), "Recall"),
                (agg("ml_threshold_metrics", "f1", "Max"), "F1")], "Precision / Recall trade-off by threshold")
    seg = table_visual(p, "seg", 852, 432, 412, 200, [
        (col("ml_auc_by_mode", "model"), "Model"), (col("ml_auc_by_mode", "segment"), "Shipping mode"),
        (agg("ml_auc_by_mode", "late_rate", "Max"), "Late rate"), (agg("ml_auc_by_mode", "mean_pred", "Max"), "Avg predicted"),
        (agg("ml_auc_by_mode", "within_auc", "Max"), "Within-group AUC")],
        "Where Can the Model Discriminate?", "First/Same Day: deterministic · Second/Standard: AUC ≈ 0.5")
    textbox(p, "insight", 16, 640, 1248, 72, [
        [("Insight: ", 10, C["amber"], True),
         ("Lateness is driven by SLA design — First Class always ships in 2 days (SLA 1) → 100% late; Same Day orders placed after 12:00 → late. "
          "Region, department and discount have no effect. Tree models reach ~100% of the theoretical ceiling; "
          "Logistic Regression lags because it cannot learn the mode × hour interaction.",
          10, C["text"], False)]], bg=C["panel2"])
    p.no_filter(s_model, [auc_bar, roc, thr])
    p.no_filter(s_thr, [auc_bar, roc, fi, cm, seg, thr])
    return p


# ---------------------------------------------------------------------------
# PAGE 2 — F5: Risk Scoring (180,519 rows)
# ---------------------------------------------------------------------------
def page_scoring() -> Page:
    p = Page("p2", "3.2 Risk Scoring (F5)")
    header(p, "F5 · PRE-SHIPMENT RISK SCORING — 180,519 SHIPMENTS",
           "Every order line is scored at creation time · Risk band: Low < 30% ≤ Medium < 60% ≤ High < 85% ≤ Critical")
    s_split = slicer(p, "s_split", 16, 74, 150, 70, col("ml_predictions", "split"), "Dataset split", single=False)
    s_mode = slicer(p, "s_mode", 172, 74, 170, 70, col("dim_shipping_mode", "shipping_mode"), "Shipping mode", single=False)
    s_mkt = slicer(p, "s_mkt", 348, 74, 150, 70, col("ml_predictions", "market"), "Market", single=False)
    for i, (m, lbl, colr) in enumerate([("Scored Shipments", "Scored shipments", C["text"]),
                                        ("Actual Late Rate", "Actual late rate", C["red"]),
                                        ("Predicted Late Rate", "Predicted late rate", C["amber"]),
                                        ("Prediction Accuracy", "Accuracy", C["green"]),
                                        ("High-Risk Share", "% High + Critical", C["red"])]):
        card(p, f"c{i}", 504 + i * 152, 74, 146, 70, m, lbl, colr)
    donut = p.add("band", "donutChart", 16, 152, 300, 242, query={
        "Category": [proj(col("ml_predictions", "risk_band"), "Risk band")],
        "Y": [proj(agg("ml_predictions", "order_item_id", "Count"), "Shipments")]},
        objects={"legend": [{"properties": {"show": lit(False)}}]},
        title="Risk Band Distribution", sort=[(col("ml_predictions", "risk_band"), "Ascending")])
    bymode = bar(p, "bymode", 324, 152, 460, 242, col("dim_shipping_mode", "shipping_mode"),
                 [(meas("Actual Late Rate"), "Actual"), (meas("Predicted Late Rate"), "Predicted")],
                 "Actual vs. Predicted by Shipping Mode", "Predictions match reality for every mode",
                 vtype="clusteredColumnChart", sort_desc=False)
    byhour = line(p, "byhour", 792, 152, 472, 242, col("ml_predictions", "order_hour"),
                  [(meas("Actual Late Rate"), "Actual"), (meas("Predicted Late Rate"), "Predicted")],
                  "By Order Hour", "Filter to Same Day to reveal the 12:00 cut-off")
    byreg = bar(p, "byreg", 16, 402, 380, 310, col("ml_predictions", "order_region"),
                [(meas("Predicted Late Rate"), "Predicted late rate")], "Predicted Risk by Region",
                "Nearly flat → geography is NOT the root cause", fill=C["teal"])
    tbl = table_visual(p, "tbl", 404, 402, 860, 310, [
        (col("ml_predictions", "order_id"), "Order ID"), (col("ml_predictions", "order_date"), "Order date"),
        (col("ml_predictions", "order_hour"), "Hour"), (col("ml_predictions", "shipping_mode"), "Mode"),
        (col("ml_predictions", "order_region"), "Region"), (col("ml_predictions", "category_name"), "Category"),
        (agg("ml_predictions", "pred_prob_late", "Max"), "P(late)"), (col("ml_predictions", "risk_band"), "Risk band"),
        (agg("ml_predictions", "actual_late", "Max"), "Actually late")],
        "Early-Warning List — Critical Orders (test set)", "Prioritise upgrading the mode or re-setting the promise for these orders",
        sort=[(agg("ml_predictions", "pred_prob_late", "Max"), "Descending")],
        filters=[categorical_filter("p2.tbl.band", "ml_predictions", "risk_band", ["4-Critical"]),
                 categorical_filter("p2.tbl.split", "ml_predictions", "split", ["test"])])
    _ = (s_split, s_mode, s_mkt, donut, bymode, byhour, byreg, tbl)
    return p


# ---------------------------------------------------------------------------
# PAGE 3 — F6: What-If Order Delay Simulator
# ---------------------------------------------------------------------------
def page_whatif() -> Page:
    p = Page("p3", "3.3 What-If Simulator (F6)")
    header(p, "F6 · WHAT-IF ORDER DELAY SIMULATOR",
           "Dispatchers pick an order scenario → instant late-delivery probability + the lowest-risk shipping mode")
    s_mode = slicer(p, "s_mode", 16, 74, 200, 70, col("dim_shipping_mode", "shipping_mode"), "① Shipping mode")
    s_hour = slicer(p, "s_hour", 222, 74, 140, 70, col("ml_whatif_grid", "order_hour"), "② Order hour")
    s_mkt = slicer(p, "s_mkt", 368, 74, 160, 70, col("ml_whatif_grid", "market"), "③ Market")
    s_disc = slicer(p, "s_disc", 534, 74, 140, 70, col("ml_whatif_grid", "discount_rate"), "④ Discount")
    s_qty = slicer(p, "s_qty", 680, 74, 130, 70, col("ml_whatif_grid", "order_item_quantity"), "⑤ Quantity")
    textbox(p, "hint", 816, 74, 448, 70, [[("Pick one value for each of the 5 inputs. ", 9, C["amber"], True),
                                           ("Try: Same Day + 14:00 → 100%; switch to 09:00 → 0%. Market / discount / quantity barely move the result.",
                                            9, C["muted"], False)]], bg=C["panel2"])
    gauge = p.add("gauge", "gauge", 16, 152, 360, 242, query={"Y": [proj(meas("What-If P(Late)"), "P(late)")]},
                  objects={"axis": [{"properties": {"min": lit(0.0), "max": lit(1.0)}}],
                           "dataPoint": [{"properties": {"fill": color(C["red"]), "target": color(C["amber"])}}],
                           "calloutValue": [{"properties": {"color": color(C["text"])}}]},
                  title="Predicted Probability of Late Delivery")
    card(p, "band", 384, 152, 200, 72, "What-If Risk Band", "Risk band", C["red"], 18.0)
    card(p, "sla", 590, 152, 220, 72, "What-If Current SLA", "Current promise", C["text"], 14.0)
    card(p, "red", 384, 230, 426, 72, "What-If Risk Reduction Label", "Risk reduction if recommendation is followed", C["green"], 18.0)
    card(p, "rec", 384, 308, 426, 86, "What-If Recommendation", "Recommended action", C["teal"], 12.0)
    cmp_modes = bar(p, "cmp", 818, 152, 446, 242, col("dim_shipping_mode", "shipping_mode"),
                    [(meas("What-If P(Late)"), "P(late)")],
                    "Same Scenario — All 4 Modes Compared", "Not filtered by slicer ①", vtype="clusteredColumnChart",
                    sort_desc=False, fill=C["indigo"])
    hour_curve = line(p, "hours", 16, 402, 620, 310, col("ml_whatif_grid", "order_hour"),
                      [(meas("What-If P(Late)"), "P(late)")],
                      "P(late) by Order Hour × Shipping Mode", "Same Day 12:00 cut-off — afternoon orders ship next day",
                      series=col("dim_shipping_mode", "shipping_mode"))
    s_sla = slicer(p, "s_sla", 644, 402, 150, 70, col("ml_sla_scenarios", "proposed_sla_days"), "Proposed SLA (days)")
    card(p, "sla_new", 800, 402, 150, 70, "SLA Late Rate Label", "New late rate", C["amber"], 16.0)
    card(p, "sla_otd", 956, 402, 150, 70, "SLA OTD Label", "New OTD", C["green"], 16.0)
    card(p, "sla_av", 1112, 402, 152, 70, "SLA Avoided Label", "Late shipments avoided", C["green"], 16.0)
    sla_curve = line(p, "slacurve", 644, 480, 620, 232, col("ml_sla_scenarios", "proposed_sla_days"),
                     [(agg("ml_sla_scenarios", "expected_late_rate", "Max"), "Expected late rate")],
                     "SLA Re-calibration Scenarios (links to F8)", "Expected late rate = P(actual shipping days > proposed SLA)",
                     series=col("dim_shipping_mode", "shipping_mode"))
    p.no_filter(s_mode, [cmp_modes, hour_curve])
    p.no_filter(s_hour, [hour_curve])
    p.no_filter(s_sla, [sla_curve])
    _ = (s_mkt, s_disc, s_qty, gauge)
    return p


def theme() -> dict:
    """Theme Harbor của nhóm (dashboard/themes/DataCo_Harbor_Theme.json)."""
    return json.loads(THEME_FILE.read_text(encoding="utf-8"))


def build_report(rp_dir: Path, pages: list, start_page: int = 1) -> None:
    d = rp_dir / "definition"
    if d.exists():
        shutil.rmtree(d)
    (d / "pages").mkdir(parents=True)
    res = rp_dir / "StaticResources" / "RegisteredResources"
    res.mkdir(parents=True, exist_ok=True)
    theme_file = "DataCo_Harbor_Theme.json"
    (res / theme_file).write_text(json.dumps(theme(), indent=2, ensure_ascii=False), encoding="utf-8")

    def dump(path: Path, obj):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")

    dump(d / "version.json", {"$schema": f"{SCHEMA}/versionMetadata/1.0.0/schema.json", "version": "2.0.0"})
    dump(d / "report.json", {
        "$schema": f"{SCHEMA}/report/{V_REPORT}/schema.json",
        "themeCollection": {"customTheme": {"name": theme_file, "reportVersionAtImport":
                                            {"visual": V_VISUAL, "page": V_PAGE, "report": V_REPORT},
                                            "type": "RegisteredResources"}},
        "resourcePackages": [{"name": "RegisteredResources", "type": "RegisteredResources",
                              "items": [{"name": theme_file, "path": theme_file, "type": "CustomTheme"}]}],
    })
    dump(d / "pages" / "pages.json", {"$schema": f"{SCHEMA}/pagesMetadata/1.0.0/schema.json",
                                      "pageOrder": [p.name for p in pages], "activePageName": pages[start_page - 1].name})
    for p in pages:
        page = {"$schema": f"{SCHEMA}/page/{V_PAGE}/schema.json", "name": p.name, "displayName": p.display,
                "displayOption": "FitToPage", "height": 720, "width": 1280}
        if p.interactions:
            page["visualInteractions"] = p.interactions
        dump(d / "pages" / p.name / "page.json", page)
        for v in p.visuals:
            dump(d / "pages" / p.name / "visuals" / v["name"] / "visual.json", v)

    (rp_dir / "definition.pbir").write_text(json.dumps(
        {"version": "4.0", "datasetReference": {"byPath": {"path": f"../{NAME}.SemanticModel"}}}, indent=2),
        encoding="utf-8")
    (rp_dir / ".platform").write_text(json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {"type": "Report", "displayName": NAME},
        "config": {"version": "2.0", "logicalId": uid("report")}}, indent=2), encoding="utf-8")


def main():
    ap = argparse.ArgumentParser(description="Sinh Power BI Project cho Dashboard Trang 3")
    ap.add_argument("--data-folder", default=str(BTL_DIR / "data" / "ml"),
                    help="Thư mục chứa các file ml_*.parquet (mặc định: btl/data/ml)")
    ap.add_argument("--start-page", type=int, default=1, choices=[1, 2, 3], help="Trang mở mặc định")
    a = ap.parse_args()
    data_folder = str(Path(a.data_folder).resolve())
    missing = [t for t in PARQUET_TABLES if not (Path(data_folder) / f"{t}.parquet").exists()]
    if missing:
        raise SystemExit(f"Thiếu {missing} — hãy chạy `python run_pipeline.py --step 7` trước.")

    sm_dir, rp_dir = ROOT / f"{NAME}.SemanticModel", ROOT / f"{NAME}.Report"
    if (sm_dir / "definition").exists():
        shutil.rmtree(sm_dir / "definition")
    tables = build_semantic_model(sm_dir, data_folder)
    pages = [page_model_eval(), page_scoring(), page_whatif()]
    build_report(rp_dir, pages, a.start_page)
    (ROOT / f"{NAME}.pbip").write_text(json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/pbip/pbipProperties/1.0.0/schema.json",
        "version": "1.0", "artifacts": [{"report": {"path": f"{NAME}.Report"}}],
        "settings": {"enableAutoRecovery": True}}, indent=2), encoding="utf-8")
    (ROOT / ".gitignore").write_text("**/.pbi/localSettings.json\n**/.pbi/cache.abf\n", encoding="utf-8")

    n_vis = sum(len(p.visuals) for p in pages)
    print(f"[✓] Đã sinh {NAME}.pbip — {len(tables)} bảng, {len(MEASURES)} measures, {len(pages)} trang, {n_vis} visuals")
    print(f"    DataFolder = {data_folder}")
    print(f"    Mở: Power BI Desktop → File → Open → {ROOT / (NAME + '.pbip')}")


if __name__ == "__main__":
    main()
