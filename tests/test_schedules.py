"""Unit tests for transforms/schedules.py."""

from __future__ import annotations

from snowflake.snowpark import Session

from transforms import schedules

# (voyage_id, vessel_mmsi, origin, dest, eta, ata, expected_delay_hours)
_ROWS = [
    ("V001", "111111111", "ROTTERDAM", "SINGAPORE",
     "2026-01-10 08:00:00", "2026-01-10 14:00:00", 6),
    ("V002", "222222222", "HAMBURG", "HONG KONG",
     "2026-01-12 06:00:00", "2026-01-12 06:00:00", 0),
    ("V003", "333333333", "VALENCIA", "NEW YORK",
     "2026-01-15 18:00:00", "2026-01-16 06:00:00", 12),
]


def test_schedules_delay_hours(session: Session, tmp_schema: str, monkeypatch) -> None:
    src = f"{tmp_schema}.VESSEL_SCHEDULES_RAW"
    tgt = f"{tmp_schema}.VESSEL_SCHEDULES"

    session.sql(
        f"""
        CREATE TABLE {src} (
            voyage_id VARCHAR,
            vessel_mmsi VARCHAR,
            origin_port VARCHAR,
            destination_port VARCHAR,
            estimated_arrival TIMESTAMP_NTZ,
            actual_arrival TIMESTAMP_NTZ
        )
        """
    ).collect()

    for voyage_id, mmsi, origin, dest, eta, ata, _ in _ROWS:
        session.sql(
            f"INSERT INTO {src} VALUES (?, ?, ?, ?, ?, ?)",
            params=[voyage_id, mmsi, origin, dest, eta, ata],
        ).collect()

    monkeypatch.setattr(schedules, "SOURCE_TABLE", src)
    monkeypatch.setattr(schedules, "TARGET_TABLE", tgt)

    count = schedules.transform_schedules(session)
    assert count == 3

    out = {
        row["VOYAGE_ID"]: row["DELAY_HOURS"]
        for row in session.table(tgt).collect()
    }
    expected = {voyage_id: delay for voyage_id, *_, delay in _ROWS}
    assert out == expected
