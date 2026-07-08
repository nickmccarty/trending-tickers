"""Render the enriched signal dashboard from ml/signals.jsonl.

Reads the per-ticker signal payload emitted by ml/emit_signals.py and renders it into
an interactive HTML page (sortable/filterable, actionable-first, with the OOS trust
caveats baked in). In CI this runs after emit_signals.py with --out index.html.
"""

import argparse
import json
from datetime import datetime, timezone
from jinja2 import Environment, FileSystemLoader


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--signals", default="ml/signals.jsonl")
    ap.add_argument("--template-dir", default=".")
    ap.add_argument("--out", default="dashboard.html")
    args = ap.parse_args()

    records = [json.loads(l) for l in open(args.signals, encoding="utf-8") if l.strip()]
    snapshot = records[0]["snapshot"] if records else "n/a"
    backend = records[0].get("sentiment", {}).get("backend", "n/a") if records else "n/a"
    mv = records[0].get("model_versions", {}) if records else {}

    env = Environment(loader=FileSystemLoader(args.template_dir))
    html = env.get_template("dashboard.html.j2").render(
        signals_json=json.dumps(records),
        generated_at=datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", ""),
        snapshot=snapshot, n=len(records), backend=backend,
        model_versions=", ".join(f"{k}={v}" for k, v in mv.items()),
    )
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(html)
    n_act = sum(1 for r in records if r.get("bayes", {}).get("actionable"))
    print(f"wrote {args.out}  ({len(records)} tickers, {n_act} actionable)")


if __name__ == "__main__":
    main()
