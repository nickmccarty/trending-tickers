"""Build a leakage-safe feature + label table from trending-tickers.db.

This is the bedrock for the model committee (attention-persistence, forward
volatility, sentiment). Every FEATURE uses only information available at or before
a row's snapshot time T; every TARGET is constructed strictly from T+horizon, using
only the DB itself (self-supervised) -- no external price source.

Targets (DB-internal)
---------------------
The board only contains a ticker's price while it is trending, so "forward" is
defined over the ticker's own future appearances:

  persistence_6h / persistence_24h : does the ticker re-appear within the horizon?
                                     (= time_to_next_appearance <= horizon)
  fwd_return_24h                    : last_price at the last in-window appearance
                                     / last_price now - 1   (NaN if it never reappears)
  fwd_absmove_24h                   : abs(fwd_return_24h)  -- the volatility target
  fwd_vol_24h                       : std of step log-returns across in-window
                                     appearances (NaN if < 2)

Rows whose horizon window extends past the last snapshot in the DB are dropped for
the affected target (the window is not fully observed -> label undefined).
"""

import argparse
import glob
import sqlite3
import numpy as np
import pandas as pd

import sentiment as _sent

PERSIST_HORIZONS = {"persistence_6h": 6.0, "persistence_24h": 24.0}
RETURN_HORIZON_H = 24.0


def ece(conf, correct, n_bins=10):
    """Expected Calibration Error. conf = predicted probability (P(class1) for binary,
    or max-class prob for multiclass); correct = 0/1 outcome the prob is predicting."""
    conf = np.asarray(conf, float)
    correct = np.asarray(correct, float)
    bins = np.linspace(0, 1, n_bins + 1)
    idx = np.clip(np.digitize(conf, bins) - 1, 0, n_bins - 1)
    e = 0.0
    for b in range(n_bins):
        m = idx == b
        if m.sum():
            e += (m.sum() / len(conf)) * abs(conf[m].mean() - correct[m].mean())
    return e


def parse_magnitude(series):
    """Parse '19.87M' / '1.211T' / '6368000.0' / '' -> float."""
    s = series.astype(str).str.strip()
    mult = pd.Series(1.0, index=s.index)
    mult[s.str.endswith("K")] = 1e3
    mult[s.str.endswith("M")] = 1e6
    mult[s.str.endswith("B")] = 1e9
    mult[s.str.endswith("T")] = 1e12
    num = pd.to_numeric(s.str.replace(r"[KMBT]$", "", regex=True), errors="coerce")
    return num * mult


def load(db_path):
    """Load one DB path, a list of paths, or a glob (e.g. 'archives/*.db'), concatenating
    all matching trending_tickers tables (dedup on ticker+timestamp)."""
    if isinstance(db_path, str):
        paths = glob.glob(db_path) if any(ch in db_path for ch in "*?[") else [db_path]
    else:
        paths = [p for pat in db_path for p in
                 (glob.glob(pat) if any(ch in pat for ch in "*?[") else [pat])]
    frames = []
    for p in paths:
        conn = sqlite3.connect(p)
        frames.append(pd.read_sql(
            "SELECT utc_timestamp, ticker_symbol, company_name, sector, industry, "
            "last_price, percent_change, trading_volume, market_cap, "
            "article_timestamp, article_title FROM trending_tickers", conn))
        conn.close()
    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates(subset=["ticker_symbol", "utc_timestamp"], keep="last")
    df["utc_timestamp"] = pd.to_datetime(df["utc_timestamp"], errors="coerce")
    df = df.dropna(subset=["utc_timestamp", "ticker_symbol"])
    df["last_price"] = pd.to_numeric(df["last_price"], errors="coerce")
    df["percent_change"] = pd.to_numeric(df["percent_change"], errors="coerce")
    df["volume"] = parse_magnitude(df["trading_volume"])
    df["mktcap"] = parse_magnitude(df["market_cap"])
    df["has_news"] = (df["article_title"].fillna("").str.len() > 0).astype(int)
    # Collapse exact-duplicate (ticker, timestamp) rows if any.
    df = df.sort_values(["ticker_symbol", "utc_timestamp"]).reset_index(drop=True)
    return df


def add_targets(df):
    max_t = df["utc_timestamp"].max()
    g = df.groupby("ticker_symbol", sort=False)
    df["t_next"] = g["utc_timestamp"].shift(-1)
    df["price_next"] = g["last_price"].shift(-1)
    gap_next_h = (df["t_next"] - df["utc_timestamp"]).dt.total_seconds() / 3600.0

    for col, hz in PERSIST_HORIZONS.items():
        observed = (df["utc_timestamp"] + pd.Timedelta(hours=hz)) <= max_t
        df[col] = np.where(observed, (gap_next_h <= hz).astype(float), np.nan)

    # Forward return / vol over the 24h window, across the ticker's appearances.
    hz = RETURN_HORIZON_H
    fwd_ret = np.full(len(df), np.nan)
    fwd_vol = np.full(len(df), np.nan)
    for _, idx in g.groups.items():
        idx = np.asarray(idx)
        t = df["utc_timestamp"].values[idx].astype("datetime64[ns]")
        p = df["last_price"].values[idx].astype(float)
        horizon = t + np.timedelta64(int(hz * 3600), "s")
        for j in range(len(idx)):
            # in-window future appearances: t in (t[j], t[j]+hz]
            hi = np.searchsorted(t, horizon[j], side="right")
            future = range(j + 1, hi)
            fut = [k for k in future if k > j]
            if horizon[j] > t[-1] and hi <= len(t):
                # window may extend past last appearance but still within observed
                pass
            if not fut:
                continue
            p_last = p[fut[-1]]
            if p[j] and p_last and p[j] > 0 and p_last > 0:
                fwd_ret[idx[j]] = p_last / p[j] - 1.0
            seq = [p[j]] + [p[k] for k in fut]
            seq = np.array([x for x in seq if x and x > 0], dtype=float)
            if len(seq) >= 3:
                logret = np.diff(np.log(seq))
                fwd_vol[idx[j]] = float(np.std(logret))
    # Only keep return/vol where the 24h window is fully observed.
    observed24 = (df["utc_timestamp"] + pd.Timedelta(hours=hz)) <= max_t
    df["fwd_return_24h"] = np.where(observed24, fwd_ret, np.nan)
    df["fwd_absmove_24h"] = np.abs(df["fwd_return_24h"])
    df["fwd_vol_24h"] = np.where(observed24, fwd_vol, np.nan)
    return df


def add_features(df):
    g = df.groupby("ticker_symbol", sort=False)

    df["log_mktcap"] = np.log1p(df["mktcap"])
    df["log_volume"] = np.log1p(df["volume"])

    # --- per-ticker time-series (history up to and including T) ---
    df["t_prev"] = g["utc_timestamp"].shift(1)
    df["time_since_last_h"] = (
        (df["utc_timestamp"] - df["t_prev"]).dt.total_seconds() / 3600.0)
    df["novelty"] = df["t_prev"].isna().astype(int)
    df["appearances_so_far"] = g.cumcount()
    df["pct_change_prev"] = g["percent_change"].shift(1)

    for k in (1, 3, 5):
        prev_price = g["last_price"].shift(k)
        df[f"mom_{k}"] = df["last_price"] / prev_price - 1.0

    # rolling stats over PRIOR appearances only (shift(1) before rolling)
    prior_pc = g["percent_change"].shift(1)
    df["pc_vol_5"] = prior_pc.groupby(df["ticker_symbol"]).rolling(5, min_periods=2)\
        .std().reset_index(level=0, drop=True)
    prior_vol = g["volume"].shift(1)
    roll_mean = prior_vol.groupby(df["ticker_symbol"]).rolling(5, min_periods=2)\
        .mean().reset_index(level=0, drop=True)
    roll_std = prior_vol.groupby(df["ticker_symbol"]).rolling(5, min_periods=2)\
        .std().reset_index(level=0, drop=True)
    df["volume_z"] = (df["volume"] - roll_mean) / roll_std

    # --- cross-sectional (within each snapshot) ---
    snap = df.groupby("utc_timestamp", sort=False)
    df["rank_pct_change"] = snap["percent_change"].rank(pct=True)
    df["rank_volume"] = snap["volume"].rank(pct=True)
    df["board_size"] = snap["ticker_symbol"].transform("count")
    sect_ct = df.groupby(["utc_timestamp", "sector"])["ticker_symbol"].transform("count")
    df["sector_heat"] = sect_ct / df["board_size"]

    # news sentiment of the attached headline (leakage-safe: it is the most recent
    # article <= T). Computed here so training AND emit share the same feature. 0.0 for
    # no-news rows; has_news disambiguates "no news" from genuinely "neutral news".
    df["sentiment"] = _sent.score_titles(df["article_title"].tolist()) \
        if "article_title" in df.columns else 0.0

    # --- calendar ---
    df["hour_utc"] = df["utc_timestamp"].dt.hour
    df["dow"] = df["utc_timestamp"].dt.dayofweek

    # categorical codes
    df["sector_code"] = df["sector"].astype("category").cat.codes
    df["industry_code"] = df["industry"].astype("category").cat.codes
    return df


FEATURE_COLS = [
    "last_price", "percent_change", "log_mktcap", "log_volume",
    "time_since_last_h", "novelty", "appearances_so_far", "pct_change_prev",
    "mom_1", "mom_3", "mom_5", "pc_vol_5", "volume_z",
    "rank_pct_change", "rank_volume", "board_size", "sector_heat",
    "hour_utc", "dow", "sector_code", "industry_code", "has_news", "sentiment",
]
TARGET_COLS = ["persistence_6h", "persistence_24h",
               "fwd_return_24h", "fwd_absmove_24h", "fwd_vol_24h"]


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", nargs="+", default=["trending-tickers.db"],
                    help="one or more DB paths/globs to concatenate (archives + current)")
    ap.add_argument("--out", default="ml/dataset.parquet")
    args = ap.parse_args()

    print("loading...")
    df = load(args.db)
    print(f"  {len(df):,} rows, {df.ticker_symbol.nunique():,} tickers")
    print("targets...")
    df = add_targets(df)
    print("features...")
    df = add_features(df)

    keep = ["utc_timestamp", "ticker_symbol", "sector"] + FEATURE_COLS + TARGET_COLS
    out = df[keep].copy()
    out.to_parquet(args.out, index=False)
    print(f"wrote {args.out}  ({len(out):,} rows x {out.shape[1]} cols)\n")

    print("=== target coverage / distribution ===")
    for c in ["persistence_6h", "persistence_24h"]:
        v = out[c].dropna()
        print(f"  {c:16} n={len(v):>7,}  positive rate={v.mean():.3f}")
    for c in ["fwd_return_24h", "fwd_absmove_24h", "fwd_vol_24h"]:
        v = out[c].dropna()
        print(f"  {c:16} n={len(v):>7,}  mean={v.mean():.4f}  median={v.median():.4f}")
    print("\n=== feature null rates (top 8) ===")
    nulls = out[FEATURE_COLS].isna().mean().sort_values(ascending=False)
    for name, rate in nulls.head(8).items():
        print(f"  {name:20} {rate:.3f}")


if __name__ == "__main__":
    main()
