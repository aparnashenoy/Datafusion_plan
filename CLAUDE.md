# DataFusion — Multimodal Vessel Delay Pipeline

## Project Purpose
Snowflake pipeline that answers "why did this vessel delay?" by ingesting
AIS position JSON, CSV voyage schedules, weather JSON, PDF port notices,
and scanned inspection images — all through Bronze/Silver/Gold medallion
architecture, enriched with Cortex AI.

## Stack
- **Snowflake** — warehouse, storage, compute
- **Snowpark Python** — DataFrame transforms in Silver layer
- **Snowflake Cortex AI** — classify/summarise/embed port notices
- **Streams + Tasks** — CDC orchestration, no external scheduler
- **Snowflake ML** — CLASSIFICATION model for delay prediction

## Layer Responsibilities
| Layer  | Purpose                              |
|--------|--------------------------------------|
| Bronze | Raw landing — never modified         |
| Silver | Typed, cleaned, AI-enriched          |
| Gold   | Joined views, ML features, scores    |

## Key Conventions
- All credentials via .env — never hardcoded
- Snowpark transforms write with mode="overwrite" to Silver tables
- Cortex enrichment runs at write time, not query time
- Tasks DAG: flatten → enrich → gold (resume root → leaf; suspend leaf → root)
- Tests use isolated temp schemas, cleaned up after each run

## Files to Know
- config/settings.py      — Snowpark session builder
- transforms/run_all.py   — runs all Silver transforms in order
- sql/orchestration/tasks.sql — full Tasks DAG definition
- sql/gold/voyage_delay_view.sql — primary Gold output
