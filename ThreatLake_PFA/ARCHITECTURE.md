# Architecture

ThreatLake PFA is a deliberate, focused subset of a larger original
design (`ThreatLake_AI` — a separate project, not included here). This
document explains what's here, why it's shaped the way it is, and —
explicitly, not by omission — what was left out to keep this a
learning-scoped project someone can read top to bottom and explain out
loud.

## Data flow

```
data/landing/cowrie/*.ndjson
        |  scripts/generate_synthetic_cowrie.py writes these
        v
BRONZE   ingestion/bronze_writer.py + bronze_transform.py
         parses each line against config/schema/cowrie.py, splits
         malformed JSON into a quarantine table, appends the rest to
         the bronze_cowrie Delta table (partitioned by ingest_date).
        v
SILVER   transform/silver/cowrie.py (map_cowrie) + schema.py
         maps bronze rows to one unified event schema - the same shape
         every honeypot source in the original project maps into.
         Full recompute + overwrite on every pipeline run (see
         scripts/run_pipeline.py's own docstring for why that means no
         separate dedup step is needed here).
        v
GOLD     transform/gold/{attacker_profiles,attack_timeline}.py
         two aggregated, analyst-facing Delta tables, each a full
         recompute from silver, written idempotently (writer.py).
         attacker_profiles is additionally geo-enriched (see below).
        v
ML       ml/train_anomaly.py fits an IsolationForest on ml/features.py's
         trailing-window feature space and saves it with joblib.
         ml/score_events.py loads that model, combines it with
         ml/rules.py's port_scan_rule, and appends results to the
         ml_scores Delta table - alert_source records whether the rule,
         the model, or both flagged a given event.
        v
API      api/app.py exposes ml_scores+silver as GET /alerts,
         attacker_profiles (incl. lat/lon) as GET /attacker_profiles(/{ip}),
         and a guardrailed natural-language-to-SQL endpoint
         (copilot/{guardrails,text_to_sql,prompts}.py) as
         POST /copilot/query.
        v
DASHBOARD  a 3-tab React app: an alert list, an attacker map
           (react-leaflet + OpenStreetMap), and a chat panel over the
           copilot endpoint.
```

## GeoIP enrichment and the Map tab

`transform/gold/attacker_profiles.py` takes an optional `geo_enricher:
GeoEnricher | None` parameter, wired to a real one in
`scripts/run_pipeline.py`. `enrichment/geo.py` (`GeoEnricher`,
`enrich_geo`) is reused byte-for-byte unmodified from ThreatLake_AI: it
reads two local MaxMind GeoLite2 `.mmdb` files (City, ASN) via the
official `geoip2` client - fully offline, no network call, no API key,
just a free MaxMind account to download the databases from
<https://www.maxmind.com/en/geolite2/signup>. The two files
(`GeoLite2-City.mmdb`, `GeoLite2-ASN.mmdb`, ~65MB/~12MB) live at
`data/geoip/` - gitignored (see `.gitignore`'s `data/` entry), same
pattern as every other lakehouse artifact this project produces or
consumes locally.

An IP that doesn't resolve (private/reserved range, or simply outside
MaxMind's free-tier coverage) yields a present struct with null fields,
never a missing column or an error - `enrichment/geo.py`'s own docstring
calls this out explicitly: a missing *database file* is a configuration
error (raised at construction), but an unresolvable *IP* is normal.
Measured on this project's own synthetic dataset (`scripts/
generate_synthetic_cowrie.py`'s 62 distinct attacker IPs, uniformly
random octets): **61 of 62 resolved to a real lat/lon** - the one miss
landed in an address range MaxMind's free GeoLite2 tier doesn't map, not
a bug. That ratio is reported by `run_pipeline.py` on every run rather
than assumed.

The dashboard's Map tab (`dashboard/src/MapView.tsx`) fetches
`GET /attacker_profiles`, filters to the rows with a non-null lat/lon,
and plots each as a circle marker on an OpenStreetMap base layer via
`react-leaflet` - radius scaled by `total_events`, click for a popup
showing `src_ip` and its event count. No clustering, no heatmap, no
filters: one map, real markers, real data.

`reputation_score` (AbuseIPDB) is the one enrichment ThreatLake AI has
that PFA still doesn't - see "Future extensions" below for why.

## Why batch, not streaming

The original project runs a live Structured Streaming path (bronze and
silver stream continuously, a websocket pushes new alerts as they're
scored) alongside a separately-scheduled batch gold job. PFA has no
live data source to stream from — the synthetic generator writes one
batch, once — so every one of those moving parts (checkpointing,
watermark-based dedup, a streaming-vs-batch decoupling story, a
websocket poll loop) would be complexity with nothing to exercise. A
single `scripts/run_pipeline.py` script that runs the whole chain once
and prints what happened at each stage is what's actually needed to
demonstrate the same architecture.

## Why two gold tables, not five

The original also builds `service_targeting`, `credential_intelligence`,
and `campaign_candidates`. PFA keeps `attacker_profiles` and
`attack_timeline` — enough to show the aggregation pattern (full
recompute, idempotent overwrite, one shared writer) without three more
near-identical `groupBy`/`agg` modules that would teach the same lesson
a third and fourth time.

## Why two detectors, not three

The original also trains a supervised XGBoost classifier against a
labeled benchmark dataset (UNSW-NB15), and runs an ablation study
comparing a "full feature set" arm against a "benchmark-transferable"
arm to measure how much accuracy is lost when only honeypot-observable
features are used. That entire methodology exists to answer one
question: *how well does a benchmark-trained classifier generalize to
live honeypot data it was never trained on?* It's a real, defensible
research question, and a substantial one — not a small addition on top
of the two detectors kept here. PFA keeps only the unsupervised path
(IsolationForest, trained directly on unlabeled honeypot data — no
benchmark, no labels, no transfer question) plus the port-scan rule.

`ml/features.py` follows from the same decision: the original's feature
module is built around a strict "shared feature space" contract so a
classifier trained on benchmark data can later score honeypot data —
most of that module's size is the enumeration of exactly which UNSW-NB15
columns do and don't have a honeypot equivalent. With no classifier and
no benchmark, PFA's `features.py` only builds the small, honeypot-native
feature set the two detectors actually consume.

## Why one honeypot source, not four

The original ingests cowrie, suricata, dionaea, and heralding, each with
its own bronze schema and silver mapper, all converging on the same
unified silver event shape. Adding a second source is mechanically a new
`config/schema/<source>.py` file, a new `transform/silver/<source>.py`
mapper, and one more entry in `ingestion/schemas.SOURCE_TYPES` — the
architecture doesn't change, only the number of sources feeding it. PFA
keeps cowrie because it alone already exercises every stage of the
pipeline (connections, credential attempts, command execution, malware
downloads) and because a second/third/fourth near-identical mapper
wouldn't teach a different lesson than the first.

## Why joblib, not MLflow

The original tracks every training run through MLflow — parameters,
metrics, a registered model with staged versions
(`models:/name/Staging`). That's real, useful machinery for a system
with a retraining schedule and multiple models competing for a
deployment slot. PFA trains one IsolationForest, once, per pipeline run;
a plain `joblib.dump`/`joblib.load` round-trip is the whole persistence
story, and is something a reader can hold in their head without also
learning MLflow's tracking/registry API.

## The copilot guardrail chain

`POST /copilot/query` is three strictly separate steps, unchanged from
the original design:

1. `copilot/text_to_sql.py` asks an LLM (Gemini) for SQL. Its output is
   never trusted.
2. `copilot/guardrails.py` — the ONLY thing that decides whether that
   SQL is allowed to run. Parses it with `sqlglot` and inspects the
   resulting AST (not a regex over the raw text): rejects anything that
   isn't a bare `SELECT`/`UNION`/`INTERSECT`/`EXCEPT`, rejects any table
   reference outside `ALLOWED_TABLES`, rejects a qualified
   (catalog/schema-prefixed) table name, and rewrites the `LIMIT` clause
   to a server-side cap regardless of what was asked for. A raw keyword
   scan runs first and rejects `DROP`/`DELETE`/etc. even inside a SQL
   comment.
3. The now-validated SQL runs against temp views registered for exactly
   `ALLOWED_TABLES` — not the live gold Delta tables directly — so
   nothing this endpoint does can ever reach bronze, silver, or
   ml_scores even if steps 1–2 had a bug.

The one edit made to this file for PFA: `ALLOWED_TABLES` is trimmed from
the original's 5 gold tables to PFA's 2 (`attacker_profiles`,
`attack_timeline`). Everything else — the AST checks, the keyword scan,
the row-limit rewrite — is unmodified.

## Future extensions

Documented honestly, not hidden: this is what a next iteration would add,
each one a real, scoped piece of work rather than a small tweak.

- **Structured Streaming** for bronze/silver, decoupled from a
  separately-scheduled gold job, with a live-push alerts endpoint.
- **Three more honeypot sources** (suricata, dionaea, heralding) — same
  mapper pattern, extending `ingestion/schemas.SOURCE_TYPES`.
- **Three more gold tables**: `service_targeting`, `credential_intelligence`,
  `campaign_candidates`.
- **A supervised classifier** trained on a labeled benchmark dataset
  (e.g. UNSW-NB15), applied to honeypot data, with an ablation study
  measuring the cost of the benchmark-to-honeypot feature-transfer gap.
- **IP reputation** (AbuseIPDB) alongside the GeoIP enrichment
  `attacker_profiles` already has - a second, genuinely optional
  best-effort lookup this project leaves out only because it needs a
  real (rate-limited, signup-gated) API key.
- **MLflow** tracking and model-registry-based staged deployment, once
  there's more than one model or a retraining schedule to manage.
- **A Databricks deployment target** — the original's `Settings`/paths
  design already separates logical config from physical location
  specifically to make this a config change, not a rewrite.
