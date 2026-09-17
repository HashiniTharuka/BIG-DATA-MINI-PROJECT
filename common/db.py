"""Shared Postgres connection helper used by every Python component
(producer, batch source, Airflow DAG, API, dashboard). Spark uses its own
JDBC writer instead of this module.
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator

import psycopg2
import psycopg2.extras


def get_conn_params() -> dict:
    return {
        "host": os.environ.get("POSTGRES_HOST", "localhost"),
        "port": int(os.environ.get("POSTGRES_PORT", "5432")),
        "dbname": os.environ.get("POSTGRES_DB", "hospital"),
        "user": os.environ.get("POSTGRES_USER", "hospital"),
        "password": os.environ.get("POSTGRES_PASSWORD", "hospital_pw"),
    }


@contextmanager
def get_connection() -> Iterator[psycopg2.extensions.connection]:
    conn = psycopg2.connect(**get_conn_params())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def upsert_health(stage: str, status: str = "ok", detail: str | None = None) -> None:
    """Record a heartbeat for a pipeline stage. Used by every component so
    the /health endpoint and Prometheus alert rules can detect staleness."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_health (stage, last_success_at, last_status, detail)
                VALUES (%s, now(), %s, %s)
                ON CONFLICT (stage) DO UPDATE SET
                    last_success_at = EXCLUDED.last_success_at,
                    last_status = EXCLUDED.last_status,
                    detail = EXCLUDED.detail
                """,
                (stage, status, detail),
            )


def dict_cursor(conn: psycopg2.extensions.connection):
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
