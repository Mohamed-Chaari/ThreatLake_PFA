# ThreatLake_PFA_Core

Personal study reference — a stripped-down version of cowrie's path
through the real ThreatLake_AI pipeline, with everything else removed,
for understanding the core shape. The actual project (ThreatLake_AI) is
the one that gets submitted/presented — this folder exists only so I can
study the mechanics without the full complexity in front of me at once.

This is a separate, isolated copy: its own directory, its own fresh git
history, no connection to the real project's GitHub remote. Nothing here
gets submitted.

## What's in here

One source (`cowrie`), one pass through bronze → silver → a single
detector, run by one script:

```
run_pipeline.py                        <- run this
study_pipeline/
  spark_session.py                     <- minimal local Spark + Delta setup
  bronze.py                            <- landing JSON -> parsed, stamped rows
  silver.py                            <- bronze -> the unified event shape
  detector.py                          <- silver -> port-scan flags
  cowrie_schema.py                     <- Cowrie's raw JSON schema
  sample_landing/cowrie/sample.ndjson  <- hand-crafted sample data to run against
src/threatlake/
  transform/silver/{cowrie,schema}.py  <- REAL code from the real project, unmodified
  ml/rules.py                          <- REAL code from the real project, unmodified
```

The two files under `src/threatlake/` are copied byte-for-byte from the
real project — not simplified, not rewritten. Everything under
`study_pipeline/` is new code written for this copy, standing in for the
parts of the real pipeline that got removed.

## What got removed, and why

Only three moving parts survive: **bronze → silver → one detector.**
Everything else that makes the real project a full lakehouse is gone:

- **Three of the four honeypot sources** (Suricata, Dionaea, Heralding)
  — only Cowrie's mapper remains.
- **Streaming.** Bronze here is batch-only: read the landing folder once,
  parse, write. The real project's Structured Streaming version, and the
  per-source-table split it needed, aren't here.
- **Quarantine.** The real project routes a line that fails to parse into
  a separate queryable table, so nothing is silently lost. This copy just
  drops it, with one explicit filter in `bronze.py` — and that filter
  isn't decorative: running this script without it crashes, because a
  garbage line makes `map_cowrie`'s `credentials_attempted` column come
  out `NULL` instead of `true`/`false` (three-valued SQL logic on a null
  `eventid`), which the schema forbids. The real quarantine step exists
  precisely so `map_cowrie` never has to defend against that. See the
  comment in `bronze.py` for the full story.
- **Dedup.** No exact-match or fuzzy windowed deduplication. With one
  source and a dozen sample rows there's nothing to deduplicate.
- **Gold tables, enrichment (geo/reputation), the API, the dashboard,
  the copilot.** None of them are needed to see bronze → silver → a
  detector work end to end, so none of them are here.
- **Two of the three detectors.** No XGBoost classifier, no
  IsolationForest, no MLflow. Only `port_scan_rule` — the one detector
  that's a plain function, not a trained model — survives, imported
  unmodified from the real project's `ml/rules.py`. It's also
  simplified in how it's *fed*: the real project computes "distinct
  ports touched" over a trailing time window; this copy computes it
  once over the whole sample batch. `detector.py` explains that
  trade-off in its own docstring.
- **The whole config/Settings/paths system.** No YAML layering, no
  Databricks branch, no `THREATLAKE_ENV`. Paths are just plain local
  folders (`study_pipeline/sample_landing/`, `data/`).

## Running it

Needs `pyspark` and `delta-spark` (same versions the real project uses).
Two ways to get them:

**Quickest — reuse the real project's existing venv** (this only *reads*
that Python interpreter to run this folder's own code; nothing here
touches the real project's files — see the `sys.path` comment at the top
of `run_pipeline.py` for exactly why that's safe):

```bash
/Users/MohamedChaari/Development/PFA/ThreatLake_AI/.venv/bin/python run_pipeline.py
```

**Fully standalone — your own venv:**

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python run_pipeline.py
```

Either way, it prints three sections: bronze rows read, silver rows
mapped, and which sample IP got flagged by the port-scan rule (one of
the three sample attackers touches 7 distinct ports on purpose, to
actually trip it).
