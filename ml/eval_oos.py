"""Out-of-sample evaluation on fresh data pulled from GitHub.

The deployed models were trained/validated on data up to a cutoff (default: the max
timestamp in dataset.parquet). After a `git pull`, the DB contains snapshots that
postdate that cutoff -- never seen in train OR validation. This scores the deployed
committee on those rows against their REALIZED outcomes, so we learn whether the
val-fold metrics (persistence AUC ~0.74, direction top-decile precision ~0.71) hold
up on genuinely unseen data, or were optimistic.

Run AFTER `git pull`:  python ml/eval_oos.py
"""

import argparse
import os
import numpy as np
import pandas as pd
import joblib
import lightgbm as lgb
from scipy.stats import spearmanr
from sklearn.metrics import roc_auc_score, average_precision_score, accuracy_score

from build_dataset import load, add_targets, add_features, FEATURE_COLS
from bayes_signals import ensemble_predict, NUMERIC, make_direction, make_regime


def parquet_cutoff(path="ml/dataset.parquet"):
    return pd.read_parquet(path, columns=["utc_timestamp"])["utc_timestamp"].max()


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default="trending-tickers.db")
    ap.add_argument("--since", default=None,
                    help="OOS cutoff (default: max timestamp in dataset.parquet)")
    ap.add_argument("--model-dir", default="ml")
    args = ap.parse_args()

    cutoff = pd.Timestamp(args.since) if args.since else parquet_cutoff()
    print(f"OOS cutoff (training max): {cutoff}")

    df = add_features(add_targets(load(args.db)))
    df[NUMERIC] = df[NUMERIC].replace([np.inf, -np.inf], np.nan)
    oos = df[df["utc_timestamp"] > cutoff].copy()
    if len(oos) == 0:
        print("\nNo OOS rows yet — run `git pull --rebase` to fetch newer snapshots "
              "from GitHub, then re-run this.")
        return
    print(f"OOS rows: {len(oos):,}  ({oos.utc_timestamp.min()} -> {oos.utc_timestamp.max()})")

    md = args.model_dir
    X = oos[FEATURE_COLS]

    # --- persistence (LightGBM, + optional calibrator) ---
    clf = lgb.Booster(model_file=f"{md}/model_persistence_24h.txt")
    d = oos.dropna(subset=["persistence_24h"])
    if len(d):
        p = clf.predict(d[FEATURE_COLS])
        cal = f"{md}/calib_persistence.joblib"
        if os.path.exists(cal):
            p = joblib.load(cal).predict(p)
        y = d["persistence_24h"].values
        print(f"\npersistence-24h  n={len(d):,}  base={y.mean():.3f}")
        print(f"  ROC-AUC={roc_auc_score(y, p):.4f}  PR-AUC={average_precision_score(y, p):.4f}"
              f"  fade PR-AUC={average_precision_score(1 - y, 1 - p):.4f} (base {1 - y.mean():.3f})")

    # --- forward volatility (rank-IC) ---
    reg = lgb.Booster(model_file=f"{md}/model_fwd_vol_24h.txt")
    d = oos.dropna(subset=["fwd_vol_24h"])
    if len(d):
        pv = reg.predict(d[FEATURE_COLS])
        ic, _ = spearmanr(pv, d["fwd_vol_24h"].values)
        print(f"\nforward-vol      n={len(d):,}  Spearman rank-IC={ic:.4f}")

    # --- direction (bayes ensemble, + calibrator, abstain rule) ---
    dj = joblib.load(f"{md}/bayes_direction.joblib")
    meta = __import__("json").load(open(f"{md}/bayes_meta.json"))
    dd = make_direction(oos).dropna(subset=["dir_label"])
    if len(dd):
        mean, _ = ensemble_predict(dj["members"], dd[FEATURE_COLS], [0, 1, 2])
        cp = f"{md}/calib_direction.joblib"
        if os.path.exists(cp):
            from bayes_signals import apply_iso
            mean = apply_iso(joblib.load(cp), mean)
        y = dd["dir_label"].astype(int).values
        pred, conf = mean.argmax(1), mean.max(1)
        act = np.where(pred != 1)[0]
        thr = meta["dir_high_threshold"]
        actionable = act[conf[act] >= thr]
        print(f"\ndirection        n={len(dd):,}  base move={np.mean(y != 1):.3f}")
        if len(act):
            order = act[np.argsort(-conf[act])]
            k = max(1, int(0.10 * len(order)))
            print(f"  all directional bets  prec={accuracy_score(y[act], pred[act]):.3f}  n={len(act)}")
            print(f"  top-decile confidence prec={accuracy_score(y[order[:k]], pred[order[:k]]):.3f}  n={k}")
        print(f"  actionable (>= thr {thr:.2f}) prec="
              f"{accuracy_score(y[actionable], pred[actionable]):.3f}  n={len(actionable)}"
              if len(actionable) else "  actionable: none cleared the threshold")

    # --- regime ---
    rj = joblib.load(f"{md}/bayes_regime.joblib")
    rr = make_regime(oos).dropna(subset=["regime_label"])
    if len(rr):
        rmean, _ = ensemble_predict(rj["members"], rr[FEATURE_COLS], [0, 1, 2])
        y = rr["regime_label"].astype(int).values
        pred = rmean.argmax(1)
        base = max(np.mean(y == k) for k in range(3))
        print(f"\nregime           n={len(rr):,}  accuracy={accuracy_score(y, pred):.3f}"
              f"  (base {base:.3f})")


if __name__ == "__main__":
    main()
