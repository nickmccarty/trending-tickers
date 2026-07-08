"""Emit per-ticker signals for the latest snapshot, for the harness agents to consume.

Scores the most recent trending-board snapshot with the trained committee and writes
one JSONL record per ticker (mirroring the harness's data/*.jsonl convention):

  {snapshot, ticker, company, sector, last_price, percent_change,
   signals: {persistence_prob_24h, fade_risk, vol_regime, vol_estimate_24h,
             attention_intensity},
   reasons: [...verbalized top feature contributions...],
   model_versions: {...}}

`reasons` are derived from LightGBM per-row feature contributions (pred_contrib), so the
agent can *cite why* a ticker is flagged rather than treating the score as a black box.
Features are the exact same leakage-safe set used in training (info <= snapshot time).
"""

import argparse
import json
import os
import numpy as np
import pandas as pd
import lightgbm as lgb
import joblib

from build_dataset import load, add_features, FEATURE_COLS
from bayes_signals import ensemble_predict, NUMERIC, apply_iso
from scipy.stats import entropy as scipy_entropy
import sentiment as sent

# Human-readable templates: feature -> (low_phrase, high_phrase).
PHRASES = {
    "time_since_last_h": ("reappeared on the board very recently", "long gap since last appearance"),
    "volume_z": ("volume below its own recent norm", "volume spiking vs its own history"),
    "appearances_so_far": ("new to the board", "persistent board regular"),
    "percent_change": ("down on the day", "up sharply on the day"),
    "rank_pct_change": ("among the day's laggards on the board", "among the day's biggest movers on the board"),
    "rank_volume": ("light volume vs the board", "heavy volume vs the board"),
    "sector_heat": ("sector lightly represented on the board", "its sector is crowding the board"),
    "log_mktcap": ("small-cap (structurally volatile)", "large-cap (structurally calmer)"),
    "log_volume": ("thin trading", "heavy trading"),
    "mom_1": ("pulling back vs last appearance", "momentum up vs last appearance"),
    "mom_3": ("weak 3-step momentum", "strong 3-step momentum"),
    "mom_5": ("weak 5-step momentum", "strong 5-step momentum"),
    "pc_vol_5": ("steady recent prints", "choppy recent prints"),
    "board_size": ("narrow board", "crowded board"),
    "novelty": ("", "first-ever board appearance"),
    "has_news": ("no fresh headline", "fresh headline attached"),
    "hour_utc": ("", ""),
    "dow": ("", ""),
    "last_price": ("low nominal price", "high nominal price"),
    "industry_code": ("", ""),
    "sector_code": ("", ""),
    "pct_change_prev": ("", ""),
}


def verbalize(contribs, row, k=3):
    """Top-k feature contributions -> phrases. contribs is a dict feature->shap value."""
    order = sorted(contribs.items(), key=lambda kv: abs(kv[1]), reverse=True)
    out = []
    for feat, val in order:
        if len(out) >= k:
            break
        lo, hi = PHRASES.get(feat, ("", ""))
        phrase = hi if val >= 0 else lo
        if phrase:
            out.append(phrase)
    return out


def vol_tiers(model, df):
    """Derive vol-regime tier thresholds from the model's own predictions (log space)."""
    pred = model.predict(df[FEATURE_COLS])
    vol = np.expm1(pred)
    q = np.quantile(vol, [0.50, 0.80, 0.95])
    return q  # [calm|moderate, moderate|elevated, elevated|high]


def tier_label(v, q):
    if v < q[0]:
        return "calm"
    if v < q[1]:
        return "moderate"
    if v < q[2]:
        return "elevated"
    return "high"


def compute_bayes(cur, model_dir):
    """Return a list of per-row 'bayes' blocks (or None if models aren't present).
    Actionable is True only for confident, non-FLAT directional calls."""
    dp = os.path.join(model_dir, "bayes_direction.joblib")
    rp = os.path.join(model_dir, "bayes_regime.joblib")
    mp = os.path.join(model_dir, "bayes_meta.json")
    if not (os.path.exists(dp) and os.path.exists(rp) and os.path.exists(mp)):
        return None
    d = joblib.load(dp)
    r = joblib.load(rp)
    meta = json.load(open(mp))
    X = cur[FEATURE_COLS].copy()
    X[NUMERIC] = X[NUMERIC].replace([np.inf, -np.inf], np.nan)

    dm, de = ensemble_predict(d["members"], X, list(range(len(d["classes"]))))
    rm, re = ensemble_predict(r["members"], X, list(range(len(r["classes"]))))
    # apply the isotonic direction calibrator if present (probs become trustworthy)
    cp = os.path.join(model_dir, "calib_direction.joblib")
    if os.path.exists(cp):
        dm = apply_iso(joblib.load(cp), dm)
    flat = meta["flat_index"]
    thr = meta["dir_high_threshold"]

    blocks = []
    for i in range(len(cur)):
        di = int(dm[i].argmax())
        dconf = float(dm[i].max())
        ri = int(rm[i].argmax())
        actionable = bool(di != flat and dconf >= thr)
        ent = float(scipy_entropy(dm[i] + 1e-12) / np.log(len(d["classes"])))
        blocks.append({
            "direction": d["classes"][di],
            "p_down": round(float(dm[i][0]), 3),
            "p_flat": round(float(dm[i][flat]), 3),
            "p_up": round(float(dm[i][2]), 3),
            "confidence": "high" if actionable else ("low" if di == flat else "medium"),
            "actionable": actionable,
            "action": (d["classes"][di].lower() if actionable else "stand_aside"),
            "entropy": round(ent, 3),
            "epistemic_std": round(float(de[i]), 4),
            "regime": r["classes"][ri],
            "regime_prob": round(float(rm[i].max()), 3),
            "regime_epistemic_std": round(float(re[i]), 4),
        })
    return blocks


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default="trending-tickers.db")
    ap.add_argument("--persist-model", default="ml/model_persistence_24h.txt")
    ap.add_argument("--vol-model", default="ml/model_fwd_vol_24h.txt")
    ap.add_argument("--out", default="ml/signals.jsonl")
    ap.add_argument("--snapshots", type=int, default=1,
                    help="Score the last N snapshots (default 1 = latest).")
    args = ap.parse_args()

    df = load(args.db)
    df = add_features(df)

    snaps = sorted(df["utc_timestamp"].unique())[-args.snapshots:]
    cur = df[df["utc_timestamp"].isin(snaps)].copy()
    print(f"scoring {len(cur)} rows across {len(snaps)} snapshot(s); "
          f"latest = {pd.Timestamp(snaps[-1])}")

    clf = lgb.Booster(model_file=args.persist_model)
    reg = lgb.Booster(model_file=args.vol_model)
    q = vol_tiers(reg, df)

    X = cur[FEATURE_COLS]
    p_persist = clf.predict(X)
    cal_p = os.path.join(os.path.dirname(args.persist_model) or ".", "calib_persistence.joblib")
    if os.path.exists(cal_p):
        p_persist = joblib.load(cal_p).predict(p_persist)
    vol_pred = np.expm1(reg.predict(X))
    # per-row feature contributions (last column is the base/expected value)
    contrib_persist = clf.predict(X, pred_contrib=True)[:, :-1]

    # attention_intensity: percentile blend of recency + accumulation + volume_z
    def pctl(s):
        return s.rank(pct=True)
    intensity = (
        pctl(-cur["time_since_last_h"].fillna(cur["time_since_last_h"].max()))
        .add(pctl(cur["appearances_so_far"]), fill_value=0)
        .add(pctl(cur["volume_z"].fillna(0)), fill_value=0)
    ) / 3.0

    cur = cur.reset_index(drop=True)
    bayes = compute_bayes(cur, os.path.dirname(args.persist_model) or ".")
    if bayes is None:
        print("  (bayes models not found — run `bayes_signals.py --save` to enable)")

    titles = cur["article_title"].fillna("").tolist() if "article_title" in cur else [""] * len(cur)
    sent_scores = sent.score_titles(titles)
    print(f"  sentiment backend: {sent.get_backend()}")

    records = []
    for i, row in cur.iterrows():
        cdict = dict(zip(FEATURE_COLS, contrib_persist[i]))
        rec = {
            "snapshot": pd.Timestamp(row["utc_timestamp"]).isoformat(),
            "ticker": row["ticker_symbol"],
            "sector": row["sector"],
            "last_price": _num(row["last_price"]),
            "percent_change": _num(row["percent_change"]),
            "signals": {
                "persistence_prob_24h": round(float(p_persist[i]), 4),
                "fade_risk": round(float(1 - p_persist[i]), 4),
                "vol_regime": tier_label(vol_pred[i], q),
                "vol_estimate_24h": round(float(vol_pred[i]), 5),
                "attention_intensity": round(float(intensity.iloc[i]), 3),
            },
            "reasons": verbalize(cdict, row),
            "model_versions": {"persistence": "lgb_24h_v1", "vol": "lgb_24h_v1",
                               "bayes": "boot_logistic_v1"},
        }
        s = sent_scores[i]
        rec["sentiment"] = {
            "score": round(float(s), 3),
            "label": sent.label(s),
            "headline": (titles[i][:120] if titles[i] else None),
            "backend": sent.get_backend(),
        }
        if bayes is not None:
            rec["bayes"] = bayes[i]
        records.append(rec)

    # sort by fade_risk desc so the most "about to drop off" names surface first
    records.sort(key=lambda r: r["signals"]["fade_risk"], reverse=True)
    with open(args.out, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    print(f"wrote {len(records)} signals -> {args.out}")
    print("\nsample (3 highest fade-risk):")
    for r in records[:3]:
        print(" ", r["ticker"], r["signals"], "| " + "; ".join(r["reasons"]))


def _num(v):
    try:
        f = float(v)
        return None if np.isnan(f) else round(f, 4)
    except (TypeError, ValueError):
        return None


if __name__ == "__main__":
    main()
