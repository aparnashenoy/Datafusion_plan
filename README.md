# DataFusion

A Snowflake multimodal data pipeline that answers **"why did this vessel delay?"**
by ingesting AIS position JSON, CSV voyage schedules, weather JSON, PDF port
notices, and scanned inspection images through a Bronze/Silver/Gold medallion
architecture, enriched with Snowflake Cortex AI.

## Architecture

| Layer  | Purpose                          |
|--------|----------------------------------|
| Bronze | Raw landing — never modified     |
| Silver | Typed, cleaned, AI-enriched      |
| Gold   | Joined views, ML features, scores|

### End-to-end data flow

```
1. INGEST     Raw files in S3  →  COPY INTO Bronze tables
2. TRANSFORM  Snowpark Python  →  Silver typed tables + delay_hours
3. ENRICH     Cortex AI SQL     →  classified notices + embeddings
4. ANALYZE    Gold dynamic view →  joined delay analysis + risk score
5. PREDICT    Snowflake ML      →  delay probability per voyage
6. ORCHESTRATE Streams + Tasks  →  automated CDC pipeline (no external scheduler)
```

```
  AIS JSON ──┐
  CSV schedules ──┤
  Weather JSON ───┼──► S3 stage ──► BRONZE ──► SILVER ──► GOLD
  Port notices ───┤      (load)    (raw)    (typed +   (views +
  Media metadata ─┘                          AI)        ML scores)
```

## Project layout

```
.
  CLAUDE.md              # Project guide / conventions
  .env.example           # Credential template
  requirements.txt       # Python dependencies
  pytest.ini
  config/
    settings.py          # Snowpark session builder (get_session, test_connection)
  transforms/            # Snowpark Python — Silver layer
    ais_flatten.py
    schedules.py
    weather.py
    run_all.py
  sql/
    bronze/              # Landing DDL + COPY INTO
    silver/              # Cortex AI enrichment + semantic search
    gold/                # Delay views, ML features, model
    orchestration/       # Streams + Tasks DAG
  tests/                 # pytest integration tests
```

## Setup

From the repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in Snowflake credentials (see below)
```

`.env` and `.venv/` are listed in the root `.gitignore` and must never be committed.

### Snowflake credentials (`.env`)

| Variable | Description |
|----------|-------------|
| `SNOWFLAKE_ACCOUNT` | Account identifier (e.g. `xy12345.ap-southeast-1.aws`) |
| `SNOWFLAKE_USER` | Login username |
| `SNOWFLAKE_PASSWORD` | Login password |
| `SNOWFLAKE_WAREHOUSE` | Compute warehouse (e.g. `COMPUTE_WH`) |
| `SNOWFLAKE_DATABASE` | Default database (`DATAFUSION` after setup) |
| `SNOWFLAKE_ROLE` | Role with warehouse + schema privileges |

### Verify connection

With the venv activated:

```bash
python -c "from config.settings import test_connection; test_connection()"
```

---

## Build phases (8)

### Phase 1 — Bootstrap

Project scaffold: `CLAUDE.md`, `.env.example`, `requirements.txt`, directory structure.

**Dependencies:** `snowflake-snowpark-python`, `python-dotenv`, `pytest`, `loguru`

---

### Phase 2 — Connection

`config/settings.py` loads credentials from `.env` at the repo root and exposes:

- `get_session()` — returns a configured Snowpark `Session`
- `test_connection()` — runs `SELECT CURRENT_VERSION()`

---

### Phase 3 — Bronze DDL (`sql/bronze/`)

| Script | Purpose |
|--------|---------|
| `setup.sql` | Create `DATAFUSION` database and `BRONZE` / `SILVER` / `GOLD` schemas |
| `stage.sql` | JSON + CSV file formats; external S3 stage `@BRONZE.RAW_STAGE` |
| `tables.sql` | Five raw landing tables |
| `load.sql` | `COPY INTO` from S3 (requires real bucket + storage integration) |

**Bronze tables**

| Table | Content |
|-------|---------|
| `BRONZE.AIS_POSITIONS` | Raw AIS JSON (`VARIANT`) |
| `BRONZE.VESSEL_SCHEDULES` | Structured voyage schedule rows |
| `BRONZE.WEATHER` | Raw weather JSON (`VARIANT`) |
| `BRONZE.PORT_NOTICES` | Port notice text (from PDFs) |
| `BRONZE.MEDIA_FILES` | Scanned image / file catalog metadata |

**Run order (Snowflake Worksheet or snowsql):**

```bash
snowsql -f sql/bronze/setup.sql
snowsql -f sql/bronze/stage.sql
snowsql -f sql/bronze/tables.sql
# load.sql — after S3 is configured
```

Before running `stage.sql`, replace the S3 URL and storage integration placeholder with your bucket.

---

### Phase 4 — Silver transforms (`transforms/`)

Snowpark Python reads Bronze, types/flattens data, writes Silver with `mode("overwrite")`.

| Module | Source → Target | Key output |
|--------|-----------------|------------|
| `ais_flatten.py` | `BRONZE.AIS_POSITIONS` → `SILVER.AIS_POSITIONS` | vessel_mmsi, lat/lon, speed, nav_status |
| `schedules.py` | `BRONZE.VESSEL_SCHEDULES` → `SILVER.VESSEL_SCHEDULES` | adds `DELAY_HOURS` via `DATEDIFF` |
| `weather.py` | `BRONZE.WEATHER` → `SILVER.WEATHER` | wind, wave height, condition |
| `run_all.py` | Runs all three in order | logs row counts before/after |

```bash
python -m transforms.run_all
```

---

### Phase 5 — Cortex AI (`sql/silver/`)

| Script | Output |
|--------|--------|
| `enrichment.sql` | `SILVER.PORT_NOTICES_ENRICHED` — classify, summarize, extract delay reason |
| `embeddings.sql` | `SILVER.DOC_EMBEDDINGS` — 768-dim vectors for semantic search |
| `semantic_search.sql` | Parameterized top-10 cosine similarity query (`:query_text`) |

**Cortex functions used**

- `SNOWFLAKE.CORTEX.CLASSIFY_TEXT` → delay category (weather, congestion, inspection, mechanical, customs, strike)
- `SNOWFLAKE.CORTEX.SUMMARIZE` → short summary
- `SNOWFLAKE.CORTEX.EXTRACT_ANSWER` → extracted delay reason
- `SNOWFLAKE.CORTEX.EMBED_TEXT_768` → embedding vectors

```bash
snowsql -f sql/silver/enrichment.sql
snowsql -f sql/silver/embeddings.sql
```

**Trial account note:** Cortex AI functions (`COMPLETE`, `CLASSIFY_TEXT`, `SUMMARIZE`, etc.) are **not available on Snowflake trial accounts**. Upgrade to a paid account, then grant:

```sql
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE ACCOUNTADMIN;
```

Re-run `enrichment.sql` and `embeddings.sql` after upgrading.

---

### Phase 6 — Orchestration (`sql/orchestration/`)

**Streams** (append-only CDC on Bronze):

- `BRONZE.AIS_POSITIONS_STREAM`
- `BRONZE.VESSEL_SCHEDULES_STREAM`
- `BRONZE.PORT_NOTICES_STREAM`

**Tasks DAG**

```
SILVER.FLATTEN_AIS_TASK       (every 5 min, when AIS stream has data)
        ↓
SILVER.ENRICH_NOTICES_TASK    (when port notices stream has data)
        ↓
GOLD.BUILD_DELAY_VIEW_TASK    (refresh GOLD.VOYAGE_DELAY_ANALYSIS)
```

Also includes `SILVER.SP_FLATTEN_AIS()` — Python stored procedure mirroring `transforms/ais_flatten.py`.

Resume tasks **root → leaf**; suspend **leaf → root**.

Replace `DATAFUSION_WH` with your warehouse name before running.

```bash
snowsql -f sql/orchestration/streams.sql
snowsql -f sql/orchestration/tasks.sql
```

Run Phase 7 (`voyage_delay_view.sql`) before enabling the leaf task that refreshes the Gold dynamic table.

---

### Phase 7 — Gold layer (`sql/gold/`)

| Script | Output |
|--------|--------|
| `voyage_delay_view.sql` | `GOLD.VOYAGE_DELAY_ANALYSIS` — dynamic table (15 min lag) joining schedules, AIS, weather, enriched notices + heuristic risk score |
| `ml_features.sql` | `GOLD.DELAY_FEATURES` — ML features + `is_delayed` label |
| `ml_model.sql` | `GOLD.DELAY_MODEL` — Snowflake ML classification model + scoring query |

```bash
snowsql -f sql/gold/voyage_delay_view.sql
snowsql -f sql/gold/ml_features.sql
snowsql -f sql/gold/ml_model.sql
```

**Example Gold query**

```sql
SELECT voyage_id, vessel_name, delay_hours,
       delay_category, extracted_delay_reason, predicted_risk_score
FROM GOLD.VOYAGE_DELAY_ANALYSIS
WHERE delay_hours > 0
ORDER BY predicted_risk_score DESC;
```

---

### Phase 8 — Tests (`tests/`)

Integration tests against live Snowflake (isolated `TEST_*` schemas, dropped after each run).

| Test | Validates |
|------|-----------|
| `test_ais_flatten.py` | AIS flatten row count, float types, no null MMSI |
| `test_schedules.py` | `DELAY_HOURS` values (6, 0, 12) |
| `test_weather.py` | Weather float typing |

```bash
pytest tests/ -v
```

---

## Full run order

```bash
# 1. Local setup
source .venv/bin/activate
python -c "from config.settings import test_connection; test_connection()"

# 2. Bronze DDL (Snowflake Worksheets or snowsql)
#    setup.sql → stage.sql → tables.sql → load.sql

# 3. Silver transforms
python -m transforms.run_all

# 4. Cortex enrichment (paid account)
#    enrichment.sql → embeddings.sql

# 5. Gold layer
#    voyage_delay_view.sql → ml_features.sql → ml_model.sql

# 6. Orchestration
#    streams.sql → tasks.sql

# 7. Tests
pytest tests/ -v
```

---

## Snowflake object map

After a full deploy, expect:

```
DATAFUSION
├── BRONZE
│   ├── AIS_POSITIONS, VESSEL_SCHEDULES, WEATHER
│   ├── PORT_NOTICES, MEDIA_FILES
│   ├── JSON_FORMAT, CSV_FORMAT, RAW_STAGE
│   └── *_STREAM (orchestration)
├── SILVER
│   ├── AIS_POSITIONS, VESSEL_SCHEDULES, WEATHER
│   ├── PORT_NOTICES_ENRICHED, DOC_EMBEDDINGS
│   ├── SP_FLATTEN_AIS (stored procedure)
│   └── FLATTEN_AIS_TASK, ENRICH_NOTICES_TASK (tasks)
└── GOLD
    ├── VOYAGE_DELAY_ANALYSIS (dynamic table)
    ├── DELAY_FEATURES, DELAY_MODEL
    └── BUILD_DELAY_VIEW_TASK (task)
```

Browse in Snowflake UI: **Database → DATAFUSION → schema → Tables → Data**.

---

## Key conventions

- All credentials via `.env` — never hardcoded
- Snowpark transforms write with `mode("overwrite")` to Silver tables
- Cortex enrichment runs at write time, not query time
- Tasks DAG: flatten → enrich → gold (resume root → leaf)
- Tests use isolated temp schemas, cleaned up after each run

See also `CLAUDE.md` for agent/IDE project guidance.
