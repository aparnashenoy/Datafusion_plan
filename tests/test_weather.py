"""Unit tests for transforms/weather.py."""

from __future__ import annotations

import json

from snowflake.snowpark import Session

from transforms import weather

_RAW_WEATHER = [
    {
        "voyage_id": "V001",
        "observed_at": "2026-01-10 06:00:00",
        "wind_speed_knots": 18.5,
        "wave_height_m": 2.4,
        "condition": "rough",
    },
    {
        "voyage_id": "V002",
        "observed_at": "2026-01-12 03:00:00",
        "wind_speed_knots": 5.2,
        "wave_height_m": 0.6,
        "condition": "calm",
    },
    {
        "voyage_id": "V003",
        "observed_at": "2026-01-15 21:00:00",
        "wind_speed_knots": 33.0,
        "wave_height_m": 5.1,
        "condition": "storm",
    },
]


def test_weather_typed_floats(session: Session, tmp_schema: str, monkeypatch) -> None:
    src = f"{tmp_schema}.WEATHER_RAW"
    tgt = f"{tmp_schema}.WEATHER"

    session.sql(f"CREATE TABLE {src} (raw_payload VARIANT)").collect()
    for record in _RAW_WEATHER:
        session.sql(
            f"INSERT INTO {src} SELECT PARSE_JSON(?)",
            params=[json.dumps(record)],
        ).collect()

    monkeypatch.setattr(weather, "SOURCE_TABLE", src)
    monkeypatch.setattr(weather, "TARGET_TABLE", tgt)

    count = weather.flatten_weather(session)
    assert count == 3

    out = session.table(tgt)
    dtypes = dict(out.dtypes)
    for col in ("WIND_SPEED_KNOTS", "WAVE_HEIGHT_M"):
        assert dtypes[col].lower() in ("float", "double"), dtypes[col]

    # Spot-check a known value survives typing.
    storm = out.where(out["VOYAGE_ID"] == "V003").collect()[0]
    assert abs(storm["WAVE_HEIGHT_M"] - 5.1) < 1e-6
