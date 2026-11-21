"""
Module: models.py
Mục đích: Định nghĩa các mô hình phân loại dưới dạng sklearn Pipeline nhận trực tiếp DataFrame thô
(preprocessing nằm trong pipeline → không rò rỉ thống kê từ tập test, và lưu/nạp 1 file duy nhất).
"""

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, ClassifierMixin
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder, StandardScaler
from sklearn.tree import DecisionTreeClassifier

from ml.ml_config import CATEGORICAL_FEATURES, NUMERIC_FEATURES, MODEL_PARAMS, RANDOM_STATE


class ModeRateBaseline(BaseEstimator, ClassifierMixin):
    """Baseline nghiệp vụ: xác suất trễ = tỷ lệ trễ lịch sử của shipping_mode (không học gì thêm)."""

    def fit(self, X, y):
        y = pd.Series(np.asarray(y), index=X.index)
        self.rates_ = y.groupby(X["shipping_mode"]).mean().to_dict()
        self.prior_ = float(y.mean())
        self.classes_ = np.array([0, 1])
        return self

    def predict_proba(self, X):
        p = X["shipping_mode"].map(self.rates_).fillna(self.prior_).to_numpy(dtype=float)
        return np.column_stack([1 - p, p])

    def predict(self, X):
        return (self.predict_proba(X)[:, 1] >= 0.5).astype(int)


def _onehot_preprocessor():
    return ColumnTransformer([
        ("cat", OneHotEncoder(handle_unknown="infrequent_if_exist", min_frequency=30), CATEGORICAL_FEATURES),
        ("num", StandardScaler(), NUMERIC_FEATURES),
    ])


def _ordinal_preprocessor():
    return ColumnTransformer([
        ("cat", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1), CATEGORICAL_FEATURES),
        ("num", "passthrough", NUMERIC_FEATURES),
    ])


def build_models() -> dict:
    """Trả về {tên: estimator chưa fit}. Thư viện tùy chọn (xgboost/lightgbm) thiếu thì bỏ qua."""
    models = {
        "Baseline_ModeRate": ModeRateBaseline(),
        "LogisticRegression": Pipeline([
            ("prep", _onehot_preprocessor()),
            ("clf", LogisticRegression(random_state=RANDOM_STATE, **MODEL_PARAMS["LogisticRegression"])),
        ]),
        "DecisionTree": Pipeline([
            ("prep", _ordinal_preprocessor()),
            ("clf", DecisionTreeClassifier(random_state=RANDOM_STATE, **MODEL_PARAMS["DecisionTree"])),
        ]),
        "RandomForest": Pipeline([
            ("prep", _ordinal_preprocessor()),
            ("clf", RandomForestClassifier(random_state=RANDOM_STATE, **MODEL_PARAMS["RandomForest"])),
        ]),
    }
    try:
        from xgboost import XGBClassifier
        models["XGBoost"] = Pipeline([
            ("prep", _ordinal_preprocessor()),
            ("clf", XGBClassifier(random_state=RANDOM_STATE, **MODEL_PARAMS["XGBoost"])),
        ])
    except ImportError:
        print("[!] Chưa cài xgboost — bỏ qua mô hình XGBoost (pip install xgboost)")
    try:
        from lightgbm import LGBMClassifier
        models["LightGBM"] = Pipeline([
            ("prep", _ordinal_preprocessor()),
            ("clf", LGBMClassifier(random_state=RANDOM_STATE, **MODEL_PARAMS["LightGBM"])),
        ])
    except ImportError:
        print("[!] Chưa cài lightgbm — bỏ qua mô hình LightGBM (pip install lightgbm)")
    return models


def build_leaky_model(categorical, numeric):
    """Mô hình dùng cho Leakage Demo (cùng kiến trúc RandomForest, thêm cột bị rò rỉ)."""
    prep = ColumnTransformer([
        ("cat", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1), categorical),
        ("num", "passthrough", numeric),
    ])
    return Pipeline([
        ("prep", prep),
        ("clf", RandomForestClassifier(random_state=RANDOM_STATE, **MODEL_PARAMS["RandomForest"])),
    ])
