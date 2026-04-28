from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.dummy import DummyRegressor
from sklearn.ensemble import ExtraTreesRegressor, GradientBoostingRegressor, RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


DATA_DIR = Path("data/processed/satellite_pm25_daily")
OUTPUT_DIR = Path("outputs/satellite_pm25_models")
TARGET = "pm25"
RANDOM_STATE = 42


def rmse(y_true: pd.Series, y_pred: np.ndarray) -> float:
    return float(np.sqrt(mean_squared_error(y_true, y_pred)))


def metrics_for(y_true: pd.Series, y_pred: np.ndarray) -> dict[str, float]:
    return {
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "rmse": rmse(y_true, y_pred),
        "r2": float(r2_score(y_true, y_pred)),
    }


def load_splits() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    train = pd.read_csv(DATA_DIR / "train.csv", parse_dates=["date"])
    validation = pd.read_csv(DATA_DIR / "validation.csv", parse_dates=["date"])
    test = pd.read_csv(DATA_DIR / "test.csv", parse_dates=["date"])
    return train, validation, test


def feature_columns(df: pd.DataFrame) -> tuple[list[str], list[str]]:
    exclude = {
        TARGET,
        "pm25_daily_std",
        "pm25_hour_count",
        "split",
        "source_file",
        "date",
        "location_name",
    }
    candidates = [col for col in df.columns if col not in exclude]
    numeric_features = [
        col for col in candidates if pd.api.types.is_numeric_dtype(df[col])
    ]
    categorical_features = [
        col for col in candidates if not pd.api.types.is_numeric_dtype(df[col])
    ]
    return numeric_features, categorical_features


def build_preprocessor(
    numeric_features: list[str],
    categorical_features: list[str],
    scale_numeric: bool,
) -> ColumnTransformer:
    numeric_steps = [("imputer", SimpleImputer(strategy="median"))]
    if scale_numeric:
        numeric_steps.append(("scaler", StandardScaler()))

    transformers = [
        ("numeric", Pipeline(numeric_steps), numeric_features),
    ]
    if categorical_features:
        transformers.append(
            (
                "categorical",
                Pipeline(
                    [
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("onehot", OneHotEncoder(handle_unknown="ignore")),
                    ]
                ),
                categorical_features,
            )
        )
    return ColumnTransformer(transformers=transformers, remainder="drop")


def build_models(
    numeric_features: list[str],
    categorical_features: list[str],
) -> dict[str, Pipeline]:
    return {
        "dummy_mean": Pipeline(
            [
                (
                    "preprocess",
                    build_preprocessor(numeric_features, categorical_features, False),
                ),
                ("model", DummyRegressor(strategy="mean")),
            ]
        ),
        "ridge": Pipeline(
            [
                (
                    "preprocess",
                    build_preprocessor(numeric_features, categorical_features, True),
                ),
                ("model", Ridge(alpha=10.0)),
            ]
        ),
        "random_forest": Pipeline(
            [
                (
                    "preprocess",
                    build_preprocessor(numeric_features, categorical_features, False),
                ),
                (
                    "model",
                    RandomForestRegressor(
                        n_estimators=400,
                        min_samples_leaf=4,
                        random_state=RANDOM_STATE,
                        n_jobs=1,
                    ),
                ),
            ]
        ),
        "extra_trees": Pipeline(
            [
                (
                    "preprocess",
                    build_preprocessor(numeric_features, categorical_features, False),
                ),
                (
                    "model",
                    ExtraTreesRegressor(
                        n_estimators=400,
                        min_samples_leaf=3,
                        random_state=RANDOM_STATE,
                        n_jobs=1,
                    ),
                ),
            ]
        ),
        "gradient_boosting": Pipeline(
            [
                (
                    "preprocess",
                    build_preprocessor(numeric_features, categorical_features, False),
                ),
                (
                    "model",
                    GradientBoostingRegressor(
                        learning_rate=0.05,
                        n_estimators=300,
                        max_depth=3,
                        random_state=RANDOM_STATE,
                    ),
                ),
            ]
        ),
    }


def split_xy(
    df: pd.DataFrame,
    numeric_features: list[str],
    categorical_features: list[str],
) -> tuple[pd.DataFrame, pd.Series]:
    return df[numeric_features + categorical_features], df[TARGET]


def model_feature_names(model: Pipeline) -> np.ndarray:
    return model.named_steps["preprocess"].get_feature_names_out()


def save_feature_importance(best_model: Pipeline, best_model_name: str) -> None:
    estimator = best_model.named_steps["model"]
    if hasattr(estimator, "feature_importances_"):
        values = estimator.feature_importances_
        value_col = "importance"
        title = f"Top Feature Importance: {best_model_name}"
        xlabel = "Importance"
    elif hasattr(estimator, "coef_"):
        values = np.abs(np.ravel(estimator.coef_))
        value_col = "abs_coefficient"
        title = f"Top Absolute Coefficients: {best_model_name}"
        xlabel = "Absolute coefficient"
    else:
        return

    importance = pd.DataFrame(
        {
            "feature": model_feature_names(best_model),
            value_col: values,
        }
    ).sort_values(value_col, ascending=False)
    importance.to_csv(OUTPUT_DIR / "feature_importance.csv", index=False)

    top = importance.head(20).sort_values(value_col)
    plt.figure(figsize=(8, 7))
    plt.barh(top["feature"], top[value_col], color="#33658a")
    plt.title(title)
    plt.xlabel(xlabel)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "feature_importance.png", dpi=180)
    plt.close()


def save_plots(
    metrics: pd.DataFrame,
    predictions: pd.DataFrame,
    best_model_name: str,
) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    order = metrics.loc[metrics["split"] == "test"].sort_values("rmse")
    plt.figure(figsize=(8, 4.8))
    plt.bar(order["model"], order["rmse"], color="#2f4858")
    plt.xticks(rotation=30, ha="right")
    plt.ylabel("Test RMSE")
    plt.title("Model Comparison on Test Set")
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "model_comparison_rmse.png", dpi=180)
    plt.close()

    best_test = predictions[
        (predictions["model"] == best_model_name) & (predictions["split"] == "test")
    ].copy()
    min_value = min(best_test["pm25"].min(), best_test["prediction"].min())
    max_value = max(best_test["pm25"].max(), best_test["prediction"].max())

    plt.figure(figsize=(6.2, 6.2))
    plt.scatter(best_test["pm25"], best_test["prediction"], alpha=0.75, color="#33658a")
    plt.plot([min_value, max_value], [min_value, max_value], color="#b33f62", linewidth=2)
    plt.xlabel("Observed PM2.5")
    plt.ylabel("Predicted PM2.5")
    plt.title(f"Observed vs Predicted: {best_model_name}")
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "observed_vs_predicted_test.png", dpi=180)
    plt.close()

    best_test["residual"] = best_test["prediction"] - best_test["pm25"]
    plt.figure(figsize=(9, 4.8))
    for station_id, group in best_test.groupby("location_id"):
        group = group.sort_values("date")
        plt.plot(group["date"], group["residual"], marker="o", linewidth=1, label=str(station_id))
    plt.axhline(0, color="#222222", linewidth=1)
    plt.ylabel("Prediction - observed")
    plt.title(f"Test Residuals over Time: {best_model_name}")
    plt.legend(title="Station")
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "test_residuals_by_date.png", dpi=180)
    plt.close()

    for station_id, group in best_test.groupby("location_id"):
        group = group.sort_values("date")
        plt.figure(figsize=(9, 4.8))
        plt.plot(group["date"], group["pm25"], marker="o", linewidth=1.5, label="Observed")
        plt.plot(group["date"], group["prediction"], marker="o", linewidth=1.5, label="Predicted")
        plt.ylabel("PM2.5")
        plt.title(f"Test PM2.5 Time Series: station {station_id}")
        plt.legend()
        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / f"test_timeseries_station_{station_id}.png", dpi=180)
        plt.close()


def train_and_evaluate() -> tuple[pd.DataFrame, pd.DataFrame, str]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    train, validation, test = load_splits()
    numeric_features, categorical_features = feature_columns(train)

    x_train, y_train = split_xy(train, numeric_features, categorical_features)
    x_validation, y_validation = split_xy(validation, numeric_features, categorical_features)
    x_test, y_test = split_xy(test, numeric_features, categorical_features)

    records = []
    predictions = []
    models = build_models(numeric_features, categorical_features)
    fitted_models = {}

    for model_name, model in models.items():
        model.fit(x_train, y_train)
        fitted_models[model_name] = model

        for split_name, x_split, y_split, frame in [
            ("train", x_train, y_train, train),
            ("validation", x_validation, y_validation, validation),
            ("test", x_test, y_test, test),
        ]:
            y_pred = model.predict(x_split)
            row = {
                "model": model_name,
                "split": split_name,
                "n_rows": len(y_split),
                **metrics_for(y_split, y_pred),
            }
            records.append(row)

            pred_frame = frame[["location_id", "date", TARGET]].copy()
            pred_frame["model"] = model_name
            pred_frame["split"] = split_name
            pred_frame["prediction"] = y_pred
            pred_frame["abs_error"] = (pred_frame[TARGET] - pred_frame["prediction"]).abs()
            predictions.append(pred_frame)

    metrics = pd.DataFrame(records).sort_values(["split", "rmse", "model"])
    prediction_df = pd.concat(predictions, ignore_index=True)
    best_model_name = (
        metrics.loc[metrics["split"] == "validation"]
        .sort_values(["rmse", "mae"])
        .iloc[0]["model"]
    )

    metrics.to_csv(OUTPUT_DIR / "metrics.csv", index=False)
    prediction_df.to_csv(OUTPUT_DIR / "predictions.csv", index=False)
    save_plots(metrics, prediction_df, best_model_name)
    save_feature_importance(fitted_models[best_model_name], best_model_name)
    save_summary(metrics, best_model_name)
    return metrics, prediction_df, str(best_model_name)


def save_summary(metrics: pd.DataFrame, best_model_name: str) -> None:
    best_rows = metrics[metrics["model"] == best_model_name].copy()
    test_rows = metrics[metrics["split"] == "test"].sort_values("rmse")
    lines = [
        "# Satellite PM2.5 Model Results",
        "",
        f"Best model by validation RMSE: `{best_model_name}`",
        "",
        "## Best Model Metrics",
        "",
        markdown_table(best_rows),
        "",
        "## Test Set Ranking",
        "",
        markdown_table(test_rows),
        "",
        "## Generated Visualizations",
        "",
        "- `model_comparison_rmse.png`",
        "- `observed_vs_predicted_test.png`",
        "- `test_residuals_by_date.png`",
        "- `test_timeseries_station_2161292.png`",
        "- `test_timeseries_station_2161306.png`",
        "- `feature_importance.png`",
        "",
    ]
    (OUTPUT_DIR / "model_results.md").write_text("\n".join(lines), encoding="utf-8")


def markdown_table(df: pd.DataFrame) -> str:
    formatted = df.copy()
    for col in formatted.select_dtypes(include="number").columns:
        if pd.api.types.is_integer_dtype(formatted[col]):
            formatted[col] = formatted[col].map(lambda value: f"{value:d}")
        else:
            formatted[col] = formatted[col].map(lambda value: f"{value:.4f}")

    headers = [str(col) for col in formatted.columns]
    rows = formatted.astype(str).values.tolist()
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    lines.extend("| " + " | ".join(row) + " |" for row in rows)
    return "\n".join(lines)


def main() -> None:
    metrics, _, best_model_name = train_and_evaluate()
    print(f"Best model by validation RMSE: {best_model_name}")
    print("")
    print("Validation metrics:")
    print(
        metrics.loc[metrics["split"] == "validation"]
        .sort_values("rmse")
        .to_string(index=False)
    )
    print("")
    print("Test metrics:")
    print(
        metrics.loc[metrics["split"] == "test"]
        .sort_values("rmse")
        .to_string(index=False)
    )
    print("")
    print(f"Outputs written to: {OUTPUT_DIR.as_posix()}")


if __name__ == "__main__":
    main()
