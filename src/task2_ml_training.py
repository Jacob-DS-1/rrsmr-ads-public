"""
Task 2 -- ERA5 -> capacity-factor model training.

Benchmarks three gradient-boosting libraries on the same temporal 80/20
split, picks the best by test R^2 (tie-break: lower RMSE, then faster
training time), refits each winner on the **full** training table inside
``ClippedRegressor`` (predictions clipped to [0, 1]), and saves the
production bundle for Task 4 to consume.

Inputs
------
- ``outputs/objective1_generation/supply_model_training_ready.parquet`` (Task 1)

Outputs
-------
- ``era5_renewable_models.joblib`` -- production wind / solar models.
- ``weather_model_metrics.csv``    -- selected models' R^2 / RMSE on the
                                    hold-out test window.
- ``model_performance_report.csv`` -- full benchmark table (every library,
                                    R^2, RMSE, training time, selected flag).
- ``benchmark_plot.png``           -- bar chart of R^2 and training time.

The downstream Task 4 uses the predicted CF only as part of Priyanshi's
Baseline + Weather Adjustment ratio (``predicted CF / typical historic CF``);
this file is not invoked anywhere else.
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import joblib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, RegressorMixin, clone
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import r2_score, root_mean_squared_error

try:
    from lightgbm import LGBMRegressor
except OSError as e:  # pragma: no cover
    raise ImportError(
        "LightGBM failed to load (often missing libomp on macOS). Try: brew install libomp"
    ) from e

try:
    from xgboost import XGBRegressor
except ImportError as e:  # pragma: no cover
    raise ImportError("Install XGBoost: pip install xgboost") from e


class ClippedRegressor(BaseEstimator, RegressorMixin):
    """Sklearn regressor wrapper that clips ``predict`` output to [clip_low, clip_high]."""

    def __init__(
        self,
        estimator: BaseEstimator | None = None,
        clip_low: float = 0.0,
        clip_high: float = 1.0,
    ):
        self.estimator = estimator
        self.clip_low = clip_low
        self.clip_high = clip_high

    def fit(self, X, y):
        if self.estimator is None:
            raise ValueError("estimator must be set before fit.")
        self.estimator_ = clone(self.estimator)
        self.estimator_.fit(X, y)
        return self

    def predict(self, X):
        raw = self.estimator_.predict(X)
        return np.clip(raw, self.clip_low, self.clip_high)

    def score(self, X, y, sample_weight=None):
        return r2_score(y, self.predict(X), sample_weight=sample_weight)


# Back-compat: existing era5_renewable_models.joblib pickles may reference
# either the legacy module path ``clipped_regressor`` (pre-consolidation) or
# the ``__main__`` namespace (when this script was run directly with
# ``python src/task2_ml_training.py``). Both resolutions are wired up here so
# joblib.load works regardless of how the pickle was originally produced.
sys.modules.setdefault("clipped_regressor", sys.modules[__name__])
_main_mod = sys.modules.get("__main__")
if _main_mod is not None and getattr(_main_mod, "__name__", "") != __name__:
    if not hasattr(_main_mod, "ClippedRegressor"):
        _main_mod.ClippedRegressor = ClippedRegressor

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_ROOT / "outputs" / "objective1_generation"

TRAINING_PARQUET = OUTPUT_DIR / "supply_model_training_ready.parquet"
PRODUCTION_JOBLIB = OUTPUT_DIR / "era5_renewable_models.joblib"
METRICS_CSV = OUTPUT_DIR / "weather_model_metrics.csv"
PERFORMANCE_REPORT_CSV = OUTPUT_DIR / "model_performance_report.csv"
BENCHMARK_PLOT_PNG = OUTPUT_DIR / "benchmark_plot.png"

TRAIN_FRACTION = 0.8
RANDOM_STATE = 42
CLIP_LOW = 0.0
CLIP_HIGH = 1.0

_CPU = os.cpu_count() or 1
os.environ.setdefault("OMP_NUM_THREADS", str(_CPU))

WIND_FEATURES = ["wind_speed_100m_ms"]
SOLAR_FEATURES = ["ssrd_j_m2"]


def _temporal_train_test_indices(
    n_rows: int, train_fraction: float
) -> tuple[np.ndarray, np.ndarray]:
    n_train = int(np.floor(n_rows * train_fraction))
    n_train = max(1, min(n_train, n_rows - 1))
    idx = np.arange(n_rows)
    return idx[:n_train], idx[n_train:]


def _clip01(y: np.ndarray) -> np.ndarray:
    return np.clip(y.astype(np.float64, copy=False), CLIP_LOW, CLIP_HIGH)


def _make_template_estimators() -> dict[str, BaseEstimator]:
    """``random_state=42`` everywhere; XGB/LGBM use all CPU cores via ``n_jobs=-1``."""
    return {
        "HistGradientBoosting": HistGradientBoostingRegressor(random_state=RANDOM_STATE),
        "XGBoost": XGBRegressor(
            random_state=RANDOM_STATE,
            n_jobs=-1,
            tree_method="hist",
        ),
        "LightGBM": LGBMRegressor(
            random_state=RANDOM_STATE,
            n_jobs=-1,
            verbose=-1,
        ),
    }


def _benchmark_one(
    model: BaseEstimator,
    x_train: pd.DataFrame,
    y_train: np.ndarray,
    x_test: pd.DataFrame,
    y_test: np.ndarray,
) -> tuple[float, float, float]:
    t0 = time.perf_counter()
    model.fit(x_train, y_train.ravel())
    training_time_s = time.perf_counter() - t0
    y_pred = _clip01(np.asarray(model.predict(x_test), dtype=np.float64))
    r2 = float(r2_score(y_test, y_pred))
    rmse = float(root_mean_squared_error(y_test, y_pred))
    return training_time_s, r2, rmse


def _pick_winner(rows: list[dict]) -> str:
    best = max(
        rows,
        key=lambda r: (
            r["R2_Score"],
            -r["RMSE"],
            -r["Training_Time_s"],
            r["Model"],
        ),
    )
    return str(best["Model"])


def _plot_benchmark(df: pd.DataFrame, path: Path) -> None:
    models = df["Model"].unique().tolist()
    energy_types = df["Energy_Type"].unique().tolist()
    x = np.arange(len(models))
    width = 0.35

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    for ax, metric, title in zip(
        axes,
        ["R2_Score", "Training_Time_s"],
        [r"$R^2$ (higher is better)", "Training time (s)"],
    ):
        for i, et in enumerate(energy_types):
            sub = df.loc[df["Energy_Type"] == et].set_index("Model").reindex(models)
            offset = width * (i - 0.5 * (len(energy_types) - 1))
            ax.bar(x + offset, sub[metric].to_numpy(dtype=float), width, label=et)
        ax.set_xticks(x)
        ax.set_xticklabels(models, rotation=15, ha="right")
        ax.set_ylabel(metric.replace("_", " "))
        ax.set_title(title)
        ax.legend(title="Energy_Type")
        ax.grid(True, axis="y", alpha=0.3)

    fig.suptitle("Gradient boosting benchmark -- temporal 80/20 (test metrics)")
    fig.tight_layout()
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(TRAINING_PARQUET)
    df = df.sort_values("timestamp_utc").reset_index(drop=True)

    train_idx, test_idx = _temporal_train_test_indices(len(df), TRAIN_FRACTION)
    train_df = df.iloc[train_idx]
    test_df = df.iloc[test_idx]

    templates = _make_template_estimators()
    tasks = [
        ("Wind", WIND_FEATURES, "wind_cf"),
        ("Solar", SOLAR_FEATURES, "solar_cf"),
    ]

    rows: list[dict] = []
    for energy_label, features, target in tasks:
        x_train = train_df[features]
        y_train = train_df[target].to_numpy(dtype=np.float64)
        x_test = test_df[features]
        y_test = test_df[target].to_numpy(dtype=np.float64)
        for model_name, template in templates.items():
            est = clone(template)
            train_s, r2, rmse = _benchmark_one(est, x_train, y_train, x_test, y_test)
            rows.append(
                {
                    "Energy_Type": energy_label,
                    "Model": model_name,
                    "R2_Score": r2,
                    "RMSE": rmse,
                    "Training_Time_s": train_s,
                }
            )

    bench_df = pd.DataFrame(rows)
    bench_df["Selected_For_Production"] = False
    bench_df["Phase"] = "benchmark_test_split"

    winner_wind = _pick_winner(bench_df.loc[bench_df["Energy_Type"] == "Wind"].to_dict("records"))
    winner_solar = _pick_winner(bench_df.loc[bench_df["Energy_Type"] == "Solar"].to_dict("records"))

    bench_df.loc[
        (bench_df["Energy_Type"] == "Wind") & (bench_df["Model"] == winner_wind),
        "Selected_For_Production",
    ] = True
    bench_df.loc[
        (bench_df["Energy_Type"] == "Solar") & (bench_df["Model"] == winner_solar),
        "Selected_For_Production",
    ] = True

    wind_metrics = bench_df.loc[
        (bench_df["Energy_Type"] == "Wind") & (bench_df["Model"] == winner_wind)
    ].iloc[0]
    solar_metrics = bench_df.loc[
        (bench_df["Energy_Type"] == "Solar") & (bench_df["Model"] == winner_solar)
    ].iloc[0]

    pd.DataFrame(
        [
            {"model": "wind", "r2": wind_metrics["R2_Score"], "rmse": wind_metrics["RMSE"]},
            {"model": "solar", "r2": solar_metrics["R2_Score"], "rmse": solar_metrics["RMSE"]},
        ]
    ).to_csv(METRICS_CSV, index=False)

    inner_wind = clone(templates[winner_wind])
    inner_solar = clone(templates[winner_solar])

    x_w_full = df[WIND_FEATURES]
    y_w_full = df["wind_cf"].to_numpy(dtype=np.float64)
    x_s_full = df[SOLAR_FEATURES]
    y_s_full = df["solar_cf"].to_numpy(dtype=np.float64)

    wind_model = ClippedRegressor(estimator=inner_wind, clip_low=CLIP_LOW, clip_high=CLIP_HIGH)
    solar_model = ClippedRegressor(estimator=inner_solar, clip_low=CLIP_LOW, clip_high=CLIP_HIGH)
    wind_model.fit(x_w_full, y_w_full)
    solar_model.fit(x_s_full, y_s_full)

    bundle = {
        "wind": wind_model,
        "solar": solar_model,
        "wind_feature_columns": WIND_FEATURES,
        "solar_feature_columns": SOLAR_FEATURES,
        "train_fraction": TRAIN_FRACTION,
        "renewable_model_benchmark_winners": {
            "wind_model": winner_wind,
            "solar_model": winner_solar,
            "selection_metric": "max_test_r2_tiebreak_rmse_then_time",
            "production_fit": "full_training_parquet",
        },
    }
    joblib.dump(bundle, PRODUCTION_JOBLIB)

    bench_df.to_csv(PERFORMANCE_REPORT_CSV, index=False)
    _plot_benchmark(
        bench_df[["Energy_Type", "Model", "R2_Score", "RMSE", "Training_Time_s"]],
        BENCHMARK_PLOT_PNG,
    )

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 120)
    print("Task 2 -- ML training\n")
    print(
        bench_df[
            [
                "Energy_Type",
                "Model",
                "R2_Score",
                "RMSE",
                "Training_Time_s",
                "Selected_For_Production",
            ]
        ].to_string(index=False)
    )
    print(f"\nSelected for production -> Wind: {winner_wind} | Solar: {winner_solar}")
    print(f"Saved: {METRICS_CSV.name}")
    print(f"Saved: {PERFORMANCE_REPORT_CSV.name}")
    print(f"Saved: {BENCHMARK_PLOT_PNG.name}")
    print(f"Saved: {PRODUCTION_JOBLIB.name}")


if __name__ == "__main__":
    main()
