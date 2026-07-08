"""Train the DB-internal model committee and report HONEST signal metrics.

Members trained here (both leakage-safe, time-split):
  * attention-persistence : will the ticker still be trending in 24h? (classifier)
  * forward volatility     : std of forward step-returns over 24h (regressor)

We deliberately use a strict time-ordered train/val split (no shuffling) and report
the metrics that actually matter for a noisy financial signal:
  - classifier : ROC-AUC, PR-AUC vs base rate, Brier (calibration)
  - regressor  : Spearman rank-IC (does the ranking carry signal?), plus MAE/R2
A rank-IC of 0.05-0.15 is a genuinely useful weak signal in this domain; do not
expect more, and treat anything near 0.5 as a leakage bug, not a triumph.
"""

import argparse
import os
import numpy as np
import pandas as pd
import lightgbm as lgb
import joblib
from scipy.stats import spearmanr
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import roc_auc_score, average_precision_score, brier_score_loss

from build_dataset import FEATURE_COLS, ece

CATEGORICAL = ["sector_code", "industry_code", "hour_utc", "dow", "novelty"]


def time_split(df, frac=0.8):
    df = df.sort_values("utc_timestamp")
    cut = df["utc_timestamp"].quantile(frac)
    return df[df["utc_timestamp"] <= cut], df[df["utc_timestamp"] > cut], cut


def lgb_datasets(tr, va, target):
    cats = [c for c in CATEGORICAL if c in FEATURE_COLS]
    dtr = lgb.Dataset(tr[FEATURE_COLS], tr[target], categorical_feature=cats,
                      free_raw_data=False)
    dva = lgb.Dataset(va[FEATURE_COLS], va[target], reference=dtr,
                      categorical_feature=cats, free_raw_data=False)
    return dtr, dva


def train_classifier(df):
    d = df.dropna(subset=["persistence_24h"]).copy()
    tr, va, cut = time_split(d)
    base = va["persistence_24h"].mean()
    dtr, dva = lgb_datasets(tr, va, "persistence_24h")
    params = dict(objective="binary", metric="auc", learning_rate=0.05,
                  num_leaves=63, min_child_samples=200, feature_fraction=0.8,
                  bagging_fraction=0.8, bagging_freq=1, verbose=-1)
    model = lgb.train(params, dtr, num_boost_round=600, valid_sets=[dva],
                      callbacks=[lgb.early_stopping(40, verbose=False)])
    p = model.predict(va[FEATURE_COLS])
    y = va["persistence_24h"].values
    print("\n=== attention-persistence (24h) ===")
    print(f"  split at {cut}  | train={len(tr):,} val={len(va):,}")
    print(f"  val base rate (reappears) = {base:.3f}")
    print(f"  ROC-AUC = {roc_auc_score(y, p):.4f}")
    print(f"  PR-AUC  = {average_precision_score(y, p):.4f}  (base {base:.3f})")
    print(f"  Brier   = {brier_score_loss(y, p):.4f}  (calibration; lower better)")
    # also report fade-detection: PR-AUC for the MINORITY (fade) class
    print(f"  fade PR-AUC = {average_precision_score(1 - y, 1 - p):.4f}"
          f"  (base {1 - base:.3f})")
    calibrate_persistence(model, va)
    return model, va, p


def calibrate_persistence(model, va):
    """Fit an isotonic calibrator on the model's held-out val fold. Report ECE/Brier
    raw-vs-calibrated on a time-later test half (honest), then save a calibrator fit
    on the whole val fold for deployment."""
    va = va.sort_values("utc_timestamp")
    p = model.predict(va[FEATURE_COLS])
    y = va["persistence_24h"].values
    mid = len(va) // 2
    iso = IsotonicRegression(out_of_bounds="clip").fit(p[:mid], y[:mid])
    raw_te, y_te = p[mid:], y[mid:]
    cal_te = iso.predict(raw_te)
    ece_raw, ece_cal = ece(raw_te, y_te), ece(cal_te, y_te)
    print("  calibration (persistence, test half):")
    print(f"    ECE   raw={ece_raw:.4f} -> cal={ece_cal:.4f}")
    # Only deploy the calibrator if it actually improves held-out ECE; isotonic can
    # overfit and HURT when the raw model is already well-calibrated.
    path = "ml/calib_persistence.joblib"
    if ece_cal < ece_raw:
        joblib.dump(IsotonicRegression(out_of_bounds="clip").fit(p, y), path)
        print("    saved (calibration improved ECE)")
    else:
        if os.path.exists(path):
            os.remove(path)
        print("    NOT saved — raw probs already better; emit will use raw")


def train_regressor(df, target="fwd_vol_24h"):
    d = df.dropna(subset=[target]).copy()
    # winsorize the heavy right tail, model log1p
    hi = d[target].quantile(0.99)
    d[target] = d[target].clip(upper=hi)
    d["_y"] = np.log1p(d[target])
    tr, va, cut = time_split(d)
    dtr, dva = lgb_datasets(tr, va, "_y")
    params = dict(objective="regression_l2", metric="l2", learning_rate=0.05,
                  num_leaves=63, min_child_samples=200, feature_fraction=0.8,
                  bagging_fraction=0.8, bagging_freq=1, verbose=-1)
    model = lgb.train(params, dtr, num_boost_round=800, valid_sets=[dva],
                      callbacks=[lgb.early_stopping(40, verbose=False)])
    pred = model.predict(va[FEATURE_COLS])
    y = va["_y"].values
    ic, _ = spearmanr(pred, va[target].values)
    ss_res = np.sum((y - pred) ** 2)
    ss_tot = np.sum((y - y.mean()) ** 2)
    print(f"\n=== forward volatility ({target}) ===")
    print(f"  split at {cut}  | train={len(tr):,} val={len(va):,}")
    print(f"  Spearman rank-IC = {ic:.4f}   <- the signal metric")
    print(f"  R2 (log space)   = {1 - ss_res / ss_tot:.4f}")
    print(f"  MAE (log space)  = {np.mean(np.abs(y - pred)):.4f}")
    return model, va, pred


def feature_importance(model, name):
    imp = pd.Series(model.feature_importance(importance_type="gain"),
                    index=FEATURE_COLS).sort_values(ascending=False)
    print(f"\n  top features ({name}, by gain):")
    for f, v in imp.head(8).items():
        print(f"    {f:18} {v:,.0f}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="ml/dataset.parquet")
    args = ap.parse_args()
    df = pd.read_parquet(args.data)

    clf, _, _ = train_classifier(df)
    feature_importance(clf, "persistence")
    reg, _, _ = train_regressor(df, "fwd_vol_24h")
    feature_importance(reg, "fwd_vol")

    clf.save_model("ml/model_persistence_24h.txt")
    reg.save_model("ml/model_fwd_vol_24h.txt")
    print("\nsaved models to ml/model_*.txt")


if __name__ == "__main__":
    main()
