"""Shared pytest fixtures for the DataFusion transform tests.

Tests run against a live Snowflake account (credentials from .env) inside an
isolated, uniquely-named schema that is dropped after each test.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator

import pytest
from snowflake.snowpark import Session

from config.settings import get_session


@pytest.fixture(scope="session")
def session() -> Iterator[Session]:
    """Provide a single Snowpark session for the whole test session."""
    sess = get_session()
    yield sess
    sess.close()


@pytest.fixture()
def tmp_schema(session: Session) -> Iterator[str]:
    """Create a unique schema for a test and drop it afterwards.

    Yields:
        The fully-qualified schema name (DATABASE.SCHEMA).
    """
    database = session.get_current_database().strip('"')
    schema_name = f"TEST_{uuid.uuid4().hex[:12].upper()}"
    fq_schema = f"{database}.{schema_name}"

    session.sql(f"CREATE SCHEMA {fq_schema}").collect()
    try:
        yield fq_schema
    finally:
        session.sql(f"DROP SCHEMA IF EXISTS {fq_schema}").collect()
