"""Unit tests for transforms/ais_flatten.py."""

from __future__ import annotations

import json

from snowflake.snowpark import Session

from transforms import ais_flatten

_RAW_AIS = [
    {
        "vessel_mmsi": "111111111",
        "vessel_name": "ATLANTIC STAR",
        "latitude": 51.95,
        "longitude": 4.13,
        "position_ts": "2026-01-01 08:00:00",
        "speed_knots": 12.4,
        "nav_status": "underway",
    },
    {
        "vessel_mmsi": "222222222",
        "vessel_name": "PACIFIC DAWN",
        "latitude": 1.29,
        "longitude": 103.85,
        "position_ts": "2026-01-01 09:30:00",
        "speed_knots": 0.0,
        "nav_status": "at anchor",
    },
    {
        "vessel_mmsi": "333333333",
        "vessel_name": "NORDIC WIND",
        "latitude": 53.55,
        "longitude": 9.99,
        "position_ts": "2026-01-01 10:15:00",
        "speed_knots": 8.7,
        "nav_status": "underway",
    },
    {
        "vessel_mmsi": "444444444",
        "vessel_name": "CORAL QUEEN",
        "latitude": 22.31,
        "longitude": 114.17,
        "position_ts": "2026-01-01 11:00:00",
        "speed_knots": 15.2,
        "nav_status": "underway",
    },
    {
        "vessel_mmsi": "555555555",
        "vessel_name": "IBERIAN SUN",
        "latitude": 36.14,
        "longitude": -5.35,
        "position_ts": "2026-01-01 12:45:00",
        "speed_knots": 6.1,
        "nav_status": "restricted",
    },
]


def test_ais_flatten(session: Session, tmp_schema: str, monkeypatch) -> None:
    src = f"{tmp_schema}.AIS_POSITIONS_RAW"
    tgt = f"{tmp_schema}.AIS_POSITIONS"

    # Land 5 rows of raw VARIANT AIS JSON in a temp Bronze table.
    session.sql(f"CREATE TABLE {src} (raw_payload VARIANT)").collect()
    for record in _RAW_AIS:
        session.sql(
            f"INSERT INTO {src} SELECT PARSE_JSON(?)",
            params=[json.dumps(record)],
        ).collect()

    monkeypatch.setattr(ais_flatten, "SOURCE_TABLE", src)
    monkeypatch.setattr(ais_flatten, "TARGET_TABLE", tgt)

    count = ais_flatten.flatten_ais(session)
    assert count == 5

    out = session.table(tgt)

    # latitude / longitude are floating-point typed (Snowflake reports as double).
    dtypes = dict(out.dtypes)
    for col in ("LATITUDE", "LONGITUDE"):
        assert dtypes[col].lower() in ("float", "double"), dtypes[col]

    # No nulls in vessel_mmsi.
    null_mmsi = out.where(out["VESSEL_MMSI"].is_null()).count()
    assert null_mmsi == 0
