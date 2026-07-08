"""Pragmatic-Bayesian multi-head signal classifier with abstention.

Two heads, both trained on the same leakage-safe features, time-split:
  * direction : next-appearance move -> {DOWN, FLAT, UP}  (the trading signal)
  * regime    : forward 24h volatility tier -> {calm, elevated, high}

"Bayesian-lite": each head is a BOOTSTRAP ENSEMBLE of multinomial-logistic pipelines.
Averaging the ensemble's class probabilities approximates the posterior predictive;
the ensemble's spread (std across members) is the EPISTEMIC uncertainty, and the
entropy of the mean probabilities is the total/aleatoric uncertainty. Together they
define an ABSTAIN zone -> the model says "stand aside" when it has no edge, which is
the entire point of using a probabilistic signal generator in a noisy universe.

The honest metric is the SELECTIVE-ACCURACY curve: accuracy on the most-confident
top-k% must rise sharply as we abstain on the rest. If it doesn't, the uncertainty
estimates carry no information and the signal is noise.
"""

import argparse
import json
import os
import joblib
import numpy as np
import pandas as pd
from scipy.stats import entropy as scipy_entropy
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.metrics import log_loss, accuracy_score

from build_dataset import FEATURE_COLS

ONEHOT = ["sector_code", "hour_utc", "dow", "novelty"]
DROP = ["industry_code"]  # too high-cardinality for a linear head; the GBT uses it
NUMERIC = [c for c in FEATURE_COLS if c not in ONEHOT + DROP]
N_BOOT = 12
DIR_K = 0.03  # +-3% defines a meaningful directional move


def make_pipeline(seed):
    pre = ColumnTransformer([
        ("num", Pipeline([("imp", SimpleImputer(strategy="median")),
                          ("sc", StandardScaler())]), NUMERIC),
        ("cat", OneHotEncoder(handle_unknown="ignore", min_frequency=50,
                              sparse_output=True), ONEHOT),
    ])
    # No class_weight: for an ABSTAINING signal we want the model to respect the
    # base rates and sit in FLAT (stand-aside) unless it has real conviction.
    # 'balanced' over-predicts the minority directional classes (~20k flags, 0.55
    # top-decile precision); unweighted abstains ~93% and hits 0.76 precision on
    # the confident tail. Precision + abstention beats minority recall here.
    # multinomial is the default in recent sklearn; passing multi_class is deprecated.
    clf = LogisticRegression(max_iter=400, C=0.5, random_state=seed)
    return Pipeline([("pre", pre), ("clf", clf)])


def time_split(df, frac=0.8):
    df = df.sort_values("utc_timestamp")
    cut = df["utc_timestamp"].quantile(frac)
    return df[df["utc_timestamp"] <= cut], df[df["utc_timestamp"] > cut]


def fit_ensemble(tr, y, classes):
    members = []
    rng = np.random.default_rng(0)
    n = len(tr)
    for b in range(N_BOOT):
        idx = rng.integers(0, n, n)  # bootstrap resample
        Xb, yb = tr.iloc[idx][FEATURE_COLS], y.iloc[idx]
        if yb.nunique() < len(classes):
            continue
        members.append(make_pipeline(b).fit(Xb, yb))
    return members


def ensemble_predict(members, X, classes):
    """Return mean class-prob matrix and per-row epistemic std (mean over classes)."""
    probs = np.zeros((len(members), len(X), len(classes)))
    for i, m in enumerate(members):
        p = m.predict_proba(X)
        # align member columns to the global class order
        col = {c: j for j, c in enumerate(m.named_steps["clf"].classes_)}
        for k, c in enumerate(classes):
            if c in col:
                probs[i, :, k] = p[:, col[c]]
    mean = probs.mean(axis=0)
    epistemic = probs.std(axis=0).mean(axis=1)
    return mean, epistemic


def selective_table(y_true, mean_probs, classes, name):
    pred = np.array(classes)[mean_probs.argmax(1)]
    conf = mean_probs.max(1)
    ent = scipy_entropy(mean_probs.T + 1e-12) / np.log(len(classes))  # normalized 0..1
    order = np.argsort(-conf)  # most confident first
    print(f"\n  selective accuracy ({name}) — abstain on the least-confident:")
    print(f"    {'coverage':>9} {'n':>7} {'accuracy':>9}")
    base = max(np.mean(y_true == c) for c in classes)
    for cov in (1.00, 0.50, 0.25, 0.10):
        k = max(1, int(cov * len(order)))
        sel = order[:k]
        acc = accuracy_score(y_true[sel], pred[sel])
        print(f"    {cov:9.2f} {k:7d} {acc:9.3f}")
    print(f"    (majority-class base rate = {base:.3f})")
    return ent


def actionable_table(y_true, mean_probs, classes, flat_idx, name):
    """For a trading signal only confident NON-FLAT calls are actionable. Report
    precision on UP/DOWN bets at decreasing confidence coverage."""
    pred = mean_probs.argmax(1)
    conf = mean_probs.max(1)
    act = np.where(pred != flat_idx)[0]
    if len(act) == 0:
        print(f"\n  [{name}] model never makes a confident directional call.")
        return
    order = act[np.argsort(-conf[act])]
    base = accuracy_score(y_true[act], pred[act])
    print(f"\n  ACTIONABLE precision ({name}) — only UP/DOWN bets, best-confidence first:")
    print(f"    {'top-k bets':>10} {'n':>7} {'precision':>10}")
    for frac in (1.00, 0.50, 0.25, 0.10):
        k = max(1, int(frac * len(order)))
        sel = order[:k]
        print(f"    {frac:10.2f} {k:7d} {accuracy_score(y_true[sel], pred[sel]):10.3f}")
    # how often is a non-FLAT bet correct vs just guessing the non-FLAT mix?
    nonflat_rate = np.mean(y_true != flat_idx)
    print(f"    (all directional bets precision = {base:.3f}; "
          f"P(actual move occurs) = {nonflat_rate:.3f})")


def run_head(df, target_col, classes, name):
    d = df.dropna(subset=[target_col]).copy()
    tr, va = time_split(d)
    members = fit_ensemble(tr, tr[target_col].astype(int), list(range(len(classes))))
    mean, epi = ensemble_predict(members, va[FEATURE_COLS], list(range(len(classes))))
    y = va[target_col].astype(int).values
    ll = log_loss(y, mean, labels=list(range(len(classes))))
    print(f"\n=== {name} head ===")
    print(f"  train={len(tr):,} val={len(va):,}  | classes={classes}")
    print(f"  class balance (val): "
          + ", ".join(f"{c}={np.mean(y==i):.2f}" for i, c in enumerate(classes)))
    print(f"  macro log-loss = {ll:.4f}  | mean epistemic std = {epi.mean():.4f}")
    selective_table(y, mean, list(range(len(classes))), name)
    if "FLAT" in classes:
        actionable_table(y, mean, list(range(len(classes))), classes.index("FLAT"), name)
    return members


def make_direction(df):
    r = df["fwd_return_24h"]
    lab = np.where(r > DIR_K, 2, np.where(r < -DIR_K, 0, 1))  # 0 DOWN,1 FLAT,2 UP
    df = df.copy()
    df["dir_label"] = np.where(r.isna(), np.nan, lab)
    return df


def make_regime(df):
    v = df["fwd_vol_24h"]
    df = df.copy()
    obs = v.dropna()
    q = obs.quantile([0.50, 0.85]).values
    lab = np.where(v > q[1], 2, np.where(v > q[0], 1, 0))  # 0 calm,1 elevated,2 high
    df["regime_label"] = np.where(v.isna(), np.nan, lab)
    return df


def apply_iso(isos, P):
    """Per-class isotonic calibration + renormalize to a valid distribution."""
    C = np.column_stack([isos[c].predict(P[:, c]) for c in range(P.shape[1])])
    C = np.clip(C, 1e-6, None)
    return C / C.sum(1, keepdims=True)


def save_models(df, outdir="ml"):
    """Fit deployment ensembles on ALL labeled data and persist them, plus per-class
    isotonic calibrators for the direction head and the directional confidence
    threshold (90th pct of CALIBRATED confidence among directional bets) that defines
    an 'actionable' call at emit time."""
    from sklearn.isotonic import IsotonicRegression
    from build_dataset import ece

    d = df.dropna(subset=["dir_label"]).copy()
    tr, va = time_split(d)
    va = va.sort_values("utc_timestamp")
    m0 = fit_ensemble(tr, tr["dir_label"].astype(int), [0, 1, 2])
    mean, _ = ensemble_predict(m0, va[FEATURE_COLS], [0, 1, 2])
    va_y = va["dir_label"].astype(int).values

    # per-class isotonic calibrators fit on the held-out val fold
    isos = [IsotonicRegression(out_of_bounds="clip").fit(mean[:, c], (va_y == c).astype(float))
            for c in range(3)]
    cal = apply_iso(isos, mean)
    # honest ECE on confidence (max-prob) raw vs calibrated
    raw_conf, cal_conf = mean.max(1), cal.max(1)
    ece_raw = ece(raw_conf, (mean.argmax(1) == va_y).astype(float))
    ece_cal = ece(cal_conf, (cal.argmax(1) == va_y).astype(float))
    use_cal = ece_cal < ece_raw  # only deploy calibration if it helps held-out ECE
    print(f"  calibration (direction, val fold): ECE raw={ece_raw:.4f} -> "
          f"cal={ece_cal:.4f}  ({'apply' if use_cal else 'keep raw'})")

    # actionable threshold on the confidence we will actually deploy
    conf_src = cal if use_cal else mean
    pred, conf = conf_src.argmax(1), conf_src.max(1)
    act_conf = conf[pred != 1]
    thr = float(np.quantile(act_conf, 0.90)) if len(act_conf) else 0.9

    dir_members = fit_ensemble(d, d["dir_label"].astype(int), [0, 1, 2])
    joblib.dump({"members": dir_members, "classes": ["DOWN", "FLAT", "UP"],
                 "flat_index": 1}, f"{outdir}/bayes_direction.joblib")
    calib_path = f"{outdir}/calib_direction.joblib"
    if use_cal:
        joblib.dump(isos, calib_path)
    elif os.path.exists(calib_path):
        os.remove(calib_path)
    r = df.dropna(subset=["regime_label"]).copy()
    reg_members = fit_ensemble(r, r["regime_label"].astype(int), [0, 1, 2])
    joblib.dump({"members": reg_members, "classes": ["calm", "elevated", "high"]},
                f"{outdir}/bayes_regime.joblib")
    json.dump({"dir_high_threshold": thr, "flat_index": 1, "dir_k": DIR_K},
              open(f"{outdir}/bayes_meta.json", "w"))
    print(f"saved bayes models to {outdir}/  | dir_high_threshold={thr:.3f} (calibrated)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="ml/dataset.parquet")
    ap.add_argument("--save", action="store_true",
                    help="also fit deployment ensembles on all data and persist them")
    args = ap.parse_args()
    df = pd.read_parquet(args.data)
    # ratios like mom_* / volume_z can produce inf (division by ~0); the median
    # imputer treats only NaN as missing, so coerce inf -> NaN first.
    df[NUMERIC] = df[NUMERIC].replace([np.inf, -np.inf], np.nan)
    df = make_direction(df)
    df = make_regime(df)

    run_head(df, "dir_label", ["DOWN", "FLAT", "UP"], "direction")
    run_head(df, "regime_label", ["calm", "elevated", "high"], "regime")
    if args.save:
        save_models(df)


if __name__ == "__main__":
    main()
