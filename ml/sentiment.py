"""Pluggable financial-sentiment scorer for news headlines.

Backends (auto-selected):
  * finbert  -- ProsusAI/finbert via transformers+torch, if importable. Most accurate.
  * lexicon  -- Loughran-McDonald-style finance word lists (default). Zero deps, instant,
                transparent; handles simple negation. Less nuanced than the transformer.

`score_titles(titles)` returns a list of floats in [-1, 1] (negative..positive), with a
small SQLite cache so repeated/duplicate headlines are scored once. Use `label(score)`
for a {negative, neutral, positive} bucket.
"""

import os
import re
import sqlite3

CACHE_PATH = os.path.join(os.path.dirname(__file__), "sentiment_cache.db")
_TOKEN = re.compile(r"[a-z']+")
_NEGATORS = {"not", "no", "never", "without", "n't", "less", "fails", "fail"}

# Compact Loughran-McDonald-style finance sentiment lexicons (headline-oriented).
POSITIVE = {
    "beat", "beats", "gain", "gains", "gained", "surge", "surges", "surged", "soar",
    "soars", "soared", "jump", "jumps", "jumped", "rise", "rises", "rose", "rally",
    "rallies", "rallied", "growth", "grow", "grows", "grew", "profit", "profits",
    "profitable", "strong", "stronger", "strength", "upgrade", "upgraded", "outperform",
    "outperforms", "record", "higher", "boost", "boosted", "bullish", "positive",
    "exceed", "exceeds", "exceeded", "tops", "topped", "win", "wins", "won", "success",
    "successful", "opportunity", "optimistic", "upside", "buy", "accelerate", "expand",
    "expansion", "breakthrough", "momentum", "recovery", "rebound", "rebounds",
    "rebounded", "robust", "solid", "improve", "improves", "improved", "improvement",
    "advance", "advances", "advanced", "climbs", "climbed", "raises", "raised", "leap",
    "outperformed", "wins", "approval", "approved", "landmark",
}
NEGATIVE = {
    "miss", "misses", "missed", "loss", "losses", "fall", "falls", "fell", "drop",
    "drops", "dropped", "plunge", "plunges", "plunged", "slump", "slumps", "slumped",
    "decline", "declines", "declined", "sink", "sinks", "sank", "tumble", "tumbles",
    "tumbled", "crash", "crashes", "crashed", "weak", "weaker", "weakness", "downgrade",
    "downgraded", "underperform", "cut", "cuts", "warns", "warning", "warned", "lawsuit",
    "probe", "investigation", "fraud", "bearish", "negative", "concern", "concerns",
    "risk", "risks", "slow", "slows", "slowdown", "plummet", "plummets", "plummeted",
    "lower", "sell", "selloff", "layoff", "layoffs", "bankruptcy", "bankrupt", "default",
    "deficit", "recall", "halt", "halted", "suspend", "suspended", "delay", "delayed",
    "disappoint", "disappoints", "disappointing", "disappointed", "struggle", "struggles",
    "struggled", "fear", "fears", "worry", "worries", "slashes", "slashed", "sues",
    "sued", "collapse", "collapses", "collapsed", "drag", "drags", "dragged", "woes",
    "downturn", "sinks", "guidance",
}

_finbert = None
_backend = None


def get_backend():
    """Return the sentiment backend. Honors SENTIMENT_BACKEND env override
    ('lexicon' or 'finbert'); otherwise auto-selects 'finbert' if transformers+torch
    are importable, else 'lexicon'. CI sets lexicon for reproducibility (no torch)."""
    global _backend
    if _backend is not None:
        return _backend
    override = os.environ.get("SENTIMENT_BACKEND", "").strip().lower()
    if override in ("lexicon", "finbert"):
        _backend = override
        return _backend
    try:
        import torch  # noqa: F401
        import transformers  # noqa: F401
        _backend = "finbert"
    except Exception:
        _backend = "lexicon"
    return _backend


def _lexicon_score(title):
    toks = _TOKEN.findall((title or "").lower())
    pos = neg = 0
    for i, t in enumerate(toks):
        sign = -1 if (i and toks[i - 1] in _NEGATORS) else 1
        if t in POSITIVE:
            pos += 1 if sign > 0 else 0
            neg += 1 if sign < 0 else 0
        elif t in NEGATIVE:
            neg += 1 if sign > 0 else 0
            pos += 1 if sign < 0 else 0
    if pos + neg == 0:
        return 0.0
    return (pos - neg) / (pos + neg)


def _finbert_score(titles):
    global _finbert
    if _finbert is None:
        from transformers import pipeline
        _finbert = pipeline("text-classification", model="ProsusAI/finbert",
                            top_k=None, truncation=True)
    titles = list(titles)
    out = []
    chunk = 2000
    for i in range(0, len(titles), chunk):
        for res in _finbert(titles[i:i + chunk], batch_size=16):
            d = {r["label"].lower(): r["score"] for r in res}
            out.append(float(d.get("positive", 0.0) - d.get("negative", 0.0)))
        if len(titles) > chunk:
            print(f"    finbert scored {min(i + chunk, len(titles)):,}/{len(titles):,} titles",
                  flush=True)
    return out


def _cache():
    c = sqlite3.connect(CACHE_PATH)
    c.execute("CREATE TABLE IF NOT EXISTS sent (key TEXT PRIMARY KEY, score REAL)")
    return c


def score_titles(titles):
    """Score an iterable of headline strings -> list[float] in [-1, 1]. Dedups to
    unique headlines and caches by (backend, title), so it scales to large batches."""
    titles = ["" if t is None else str(t) for t in titles]
    backend = get_backend()
    c = _cache()
    uniq = sorted({t for t in titles if t.strip()})
    cached = {}
    for i in range(0, len(uniq), 400):  # stay under SQLite's variable limit
        chunk = uniq[i:i + 400]
        keys = [f"{backend}:{t}" for t in chunk]
        rows = c.execute(
            "SELECT key, score FROM sent WHERE key IN (%s)" % ",".join("?" * len(keys)),
            keys)
        cached.update(rows)
    todo = [t for t in uniq if f"{backend}:{t}" not in cached]
    if todo:
        if backend == "finbert":
            scores = _finbert_score(todo)
        else:
            scores = [_lexicon_score(t) for t in todo]
        for t, s in zip(todo, scores):
            cached[f"{backend}:{t}"] = s
            c.execute("INSERT OR REPLACE INTO sent VALUES (?,?)", (f"{backend}:{t}", s))
        c.commit()
    return [cached.get(f"{backend}:{t}", 0.0) for t in titles]


def label(score, thr=0.15):
    return "positive" if score > thr else "negative" if score < -thr else "neutral"


if __name__ == "__main__":
    demo = [
        "Nvidia surges to record high after strong earnings beat",
        "Rivian plunges as company slashes guidance and warns of losses",
        "Analysts downgrade Boeing amid safety probe and lawsuit",
        "Apple stock holds flat ahead of product event",
        "GameStop is not falling despite bearish concerns",
    ]
    print("backend:", get_backend())
    for t, s in zip(demo, score_titles(demo)):
        print(f"  {s:+.2f} [{label(s):8}] {t}")
