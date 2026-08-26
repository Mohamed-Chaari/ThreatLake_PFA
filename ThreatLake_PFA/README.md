# ThreatLake PFA

A focused, local-only cyber-threat detection lakehouse — one honeypot
source, a batch Bronze → Silver → Gold Delta pipeline, two attack
detectors, and a small read-only API + dashboard.

This is a **deliberate subset** of a larger original design
(`ThreatLake_AI`, a separate project). Real logic — the silver schema,
the port-scan rule, the copilot's guardrail/SQL-generation pipeline — is
reused from it, some byte-for-byte unmodified, some adapted to a smaller
scope. What was left out, and why, is documented honestly in
[ARCHITECTURE.md](ARCHITECTURE.md) rather than hidden.

## Scope

- **One honeypot source, real data**: [HoneyDB](https://honeydb.io)'s
  community sensor-data feed — real connections from real internet
  scanners, not synthetic. (The project's original active source,
  cowrie/SSH-Telnet with a synthetic generator, is kept fully working
  for reference but no longer wired in — see ARCHITECTURE.md's "Real
  data, not synthetic" section.)
- **Batch, not streaming**: `scripts/run_pipeline.py` runs
  bronze → silver → gold → train → score once, start to finish. No
  Structured Streaming.
- **Two gold tables**: `attacker_profiles` (one row per attacker IP) and
  `attack_timeline` (hourly event counts by category/port).
- **Two detectors**: an unsupervised IsolationForest anomaly detector and
  a fixed-threshold port-scan rule, run alongside each other — see
  `src/threatlake/ml/train_anomaly.py`'s docstring for why neither
  replaces the other.
- **A small API**: `GET /alerts`, `GET /attacker_profiles`,
  `GET /attacker_profiles/{ip}`, and `POST /copilot/query` (natural
  language → guardrailed SQL → real gold-table rows).
- **GeoIP enrichment**: `attacker_profiles` gets a real lat/lon per src_ip
  via offline MaxMind GeoLite2 lookups (no API key, no network call).
- **A small dashboard**: one page, three tabs — an alert list, an
  attacker map, and the copilot chat panel.
- **Local only**: plain filesystem paths, no Databricks/cloud branch.

## Project layout

```
src/threatlake/
  common/       settings, filesystem paths, the local SparkSession factory
  ingestion/    bronze: landing NDJSON -> parsed, quarantine-split Delta rows
  transform/
    silver/     bronze -> the unified event schema (honeydb mapper active;
                cowrie mapper kept, not wired in - see ARCHITECTURE.md)
    gold/       silver -> attacker_profiles, attack_timeline
  ml/           feature engineering, IsolationForest, the port-scan rule,
                and the combined scorer that writes ml_scores
  enrichment/   offline GeoIP lookups (MaxMind GeoLite2) for attacker_profiles
  copilot/      guardrails, NL->SQL generation, the system prompt builder
  api/          the FastAPI app and its 3 route groups
config/         config/local.yaml (settings) + config/schema/honeydb.py (the
                raw JSON schema bronze ingestion parses against; cowrie.py
                kept alongside it, not active)
scripts/        fetch_honeydb.py, run_pipeline.py (generate_synthetic_cowrie.py
                kept, not active)
dashboard/      the small React + TypeScript app (Alerts / Map / Copilot)
tests/unit/     bronze, silver, both detectors, geo enrichment, guardrails,
                API endpoints
```

## Running it

Requires Python 3.11+, Java 21 (for Spark), and Node 18+ (for the
dashboard).

**GeoIP databases** (optional but recommended - powers `attacker_profiles`'
lat/lon and the dashboard's Map tab): download `GeoLite2-City.mmdb` and
`GeoLite2-ASN.mmdb` from a free MaxMind account
(<https://www.maxmind.com/en/geolite2/signup>) and place both at
`data/geoip/`. Without them, `scripts/run_pipeline.py` fails at the
`GeoEnricher(...)` call in its GOLD step with a clear
`FileNotFoundError` telling you exactly what's missing and where to get
it - not a silent skip.

**HoneyDB API credentials** (required for step 2 below): a free API ID +
key from <https://honeydb.io> (sign up, then find them under your
account), placed in a `.env` file at the repo root as
`HONEYDB_API_ID=...` / `HONEYDB_API_KEY=...` (gitignored).

```bash
# 1. Set up the environment (uv, or plain venv+pip works too)
uv venv --python 3.11 .venv
uv pip install --python .venv/bin/python -e ".[dev]"

# 2. Fetch a real batch of HoneyDB community sensor-data (a few thousand
#    events from real internet scanners hitting real honeypot sensors -
#    see the script's own docstring)
export JAVA_HOME=$(/usr/libexec/java_home -v 21)   # macOS; see your own JDK path otherwise
.venv/bin/python scripts/fetch_honeydb.py

# 3. Run the whole pipeline: bronze -> silver -> gold -> train -> score.
#    Prints real row counts and samples at every stage.
.venv/bin/python scripts/run_pipeline.py

# 4. Start the API
.venv/bin/uvicorn threatlake.api.app:app --app-dir src --port 8000

# 5. In a second terminal, start the dashboard (proxies to the API above)
cd dashboard
npm install
npm run dev
```

Then open the dashboard's printed local URL, or hit the API directly:

```bash
curl http://127.0.0.1:8000/alerts
curl http://127.0.0.1:8000/attacker_profiles
curl -X POST http://127.0.0.1:8000/copilot/query \
  -H "Content-Type: application/json" \
  -d '{"question": "which 5 attackers have the most events?"}'
```

The copilot needs a free Gemini API key
(<https://aistudio.google.com/app/apikey>) exported as `GEMINI_API_KEY`
before starting uvicorn — without one, `/copilot/query` returns a clear
rejection reason rather than a 500, which is itself worth seeing once.

### Tests

```bash
.venv/bin/python -m pytest tests/ -q
```

## Reused from ThreatLake_AI vs. written for PFA

| Reused byte-for-byte unmodified | Adapted from the real logic | Newly written for PFA |
|---|---|---|
| `transform/silver/schema.py`, `transform/silver/cowrie.py` (kept, inactive) | `transform/gold/attacker_profiles.py` (geo enrichment wired in; no reputation) | `ml/features.py` (small, honeypot-native features — no benchmark-transfer contract) |
| `ml/rules.py` (`port_scan_rule`) | `ml/train_anomaly.py` (joblib, not MLflow) | `ml/score_events.py`, `transform/silver/honeydb.py`, `scripts/fetch_honeydb.py`, the dashboard (incl. `MapView.tsx`) |
| `transform/gold/writer.py`, `transform/gold/attack_timeline.py` | `copilot/guardrails.py` (`ALLOWED_TABLES` trimmed to 2 tables) | `common/config.py`, `common/paths.py` (trimmed shape), `config/schema/honeydb.py` |
| `enrichment/geo.py` (`GeoEnricher`, `enrich_geo`) | `api/schemas.py`, `api/routers/attacker_profiles.py` (lat/lon flattened out of `geo`) | |
| `copilot/text_to_sql.py`, `copilot/prompts.py`, `api/routers/copilot.py`, `api/deps.py`, `common/fs.py`, `config/schema/cowrie.py` (kept, inactive) | `api/_tables.py`, `api/app.py`, `api/routers/alerts.py` | |

`scripts/generate_synthetic_cowrie.py` is also kept, unmodified, inactive
— PFA's original synthetic-data generator, superseded by
`scripts/fetch_honeydb.py` but not deleted. See ARCHITECTURE.md's "Real
data, not synthetic" section for the full story.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full reasoning behind each
adaptation and everything left out entirely.
