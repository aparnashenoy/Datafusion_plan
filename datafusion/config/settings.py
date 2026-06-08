"""Snowflake session management for DataFusion.

Loads credentials from a local ``.env`` file (via python-dotenv) and builds a
configured Snowpark :class:`~snowflake.snowpark.Session`. Credentials are never
hardcoded — fill in ``.env`` based on ``.env.example``.
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger
from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSessionException

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_PROJECT_ROOT / ".env")

_REQUIRED_KEYS = (
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_ROLE",
)


def _connection_parameters() -> dict[str, str]:
    """Read Snowflake connection parameters from the environment.

    Raises:
        RuntimeError: if any required credential is missing.
    """
    missing = [key for key in _REQUIRED_KEYS if not os.getenv(key)]
    if missing:
        raise RuntimeError(
            "Missing Snowflake credentials in environment: "
            + ", ".join(missing)
            + ". Copy .env.example to .env and fill in the values."
        )

    return {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "database": os.environ["SNOWFLAKE_DATABASE"],
        "role": os.environ["SNOWFLAKE_ROLE"],
    }


def get_session() -> Session:
    """Build and return a configured Snowpark Session.

    Returns:
        A live Snowpark :class:`Session` connected to Snowflake.
    """
    params = _connection_parameters()
    logger.info(
        "Building Snowpark session for account={} user={} warehouse={} database={} role={}",
        params["account"],
        params["user"],
        params["warehouse"],
        params["database"],
        params["role"],
    )
    session = Session.builder.configs(params).create()
    logger.success("Snowpark session created.")
    return session


def test_connection() -> str:
    """Verify connectivity by running ``SELECT CURRENT_VERSION()``.

    Returns:
        The Snowflake version string.
    """
    session = get_session()
    try:
        version = session.sql("SELECT CURRENT_VERSION()").collect()[0][0]
        logger.success("Connected to Snowflake. CURRENT_VERSION() = {}", version)
        return version
    except SnowparkSessionException as exc:  # pragma: no cover - network dependent
        logger.error("Failed to query Snowflake version: {}", exc)
        raise
    finally:
        session.close()
        logger.info("Snowpark session closed.")


if __name__ == "__main__":
    test_connection()
