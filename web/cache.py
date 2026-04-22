import json
import os
import sqlite3
import logging
from typing import Any, Callable, Dict

import redis

REDIS_TTL_SECONDS = 3600
DATASET_VERSION_KEY = "dataset_version"

cache_logger = logging.getLogger("cache")
if not cache_logger.handlers:
    cache_log_file = os.path.join(os.path.dirname(__file__), "..", "logs", "cache.log")
    cache_log_dir = os.path.dirname(cache_log_file)
    if cache_log_dir and not os.path.exists(cache_log_dir):
        os.makedirs(cache_log_dir, exist_ok=True)

    cache_handler = logging.FileHandler(cache_log_file)
    cache_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    cache_logger.addHandler(cache_handler)
    cache_logger.setLevel(logging.INFO)
    cache_logger.propagate = False

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "analytics_cache.db")


def _ensure_db_dir_exists() -> None:
    db_dir = os.path.dirname(DB_PATH)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)


def _get_connection() -> sqlite3.Connection:
    _ensure_db_dir_exists()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_sqlite() -> None:
    with _get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS query_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                endpoint TEXT,
                filters TEXT,
                response TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO settings (key, value)
            VALUES (?, ?)
            """,
            (DATASET_VERSION_KEY, "1"),
        )
        conn.commit()


def _read_dataset_version_int() -> int:
    init_sqlite()
    with _get_connection() as conn:
        row = conn.execute(
            "SELECT value FROM settings WHERE key = ?",
            (DATASET_VERSION_KEY,),
        ).fetchone()
        if not row:
            conn.execute(
                "INSERT INTO settings (key, value) VALUES (?, ?)",
                (DATASET_VERSION_KEY, "1"),
            )
            conn.commit()
            return 1
        try:
            return int(row["value"])
        except (TypeError, ValueError):
            conn.execute(
                "UPDATE settings SET value = ? WHERE key = ?",
                ("1", DATASET_VERSION_KEY),
            )
            conn.commit()
            return 1


def get_dataset_version() -> str:
    value = _read_dataset_version_int()
    return f"v{value}"


def increment_dataset_version() -> str:
    current = _read_dataset_version_int()
    updated = current + 1
    with _get_connection() as conn:
        conn.execute(
            "UPDATE settings SET value = ? WHERE key = ?",
            (str(updated), DATASET_VERSION_KEY),
        )
        conn.commit()
    return f"v{updated}"


def _safe_json_dumps(data: Any) -> str:
    return json.dumps(data, sort_keys=True, default=str)


def _build_cache_key(endpoint: str, filters: Dict[str, Any], version: str) -> str:
    return f"{endpoint}:{json.dumps(filters, sort_keys=True)}:{version}"


def store_query_log(endpoint: str, filters: Dict[str, Any], response: Any) -> None:
    with _get_connection() as conn:
        conn.execute(
            """
            INSERT INTO query_logs(endpoint, filters, response)
            VALUES (?, ?, ?)
            """,
            (endpoint, _safe_json_dumps(filters), _safe_json_dumps(response)),
        )
        conn.commit()


class _RedisClientSingleton:
    _client = None

    @classmethod
    def get_client(cls):
        if cls._client is None:
            cls._client = redis.Redis(host="localhost", port=6379, decode_responses=True)
        return cls._client


redis_client = _RedisClientSingleton.get_client()


def get_cached_response(
    endpoint: str,
    filters: Dict[str, Any],
    compute_fn: Callable[[Dict[str, Any]], Any],
):
    import time

    version = get_dataset_version()
    filters_str = json.dumps(filters, sort_keys=True)
    key = f"{endpoint}:{filters_str}:{version}"

    cache_logger.info(f"REQUEST_START | endpoint={endpoint} | filters={filters_str} | version={version}")

    try:
        cached = redis_client.get(key)
        if cached:
            cache_logger.info(f"CACHE_HIT | endpoint={endpoint} | key={key}")
            redis_client.expire(key, REDIS_TTL_SECONDS)
            cache_logger.info(f"REQUEST_END | endpoint={endpoint} | source=cache")
            return json.loads(cached)
    except Exception as e:
        cache_logger.error(f"CACHE_ERROR_READ | endpoint={endpoint} | error={str(e)}")

    cache_logger.info(f"CACHE_MISS | endpoint={endpoint} | key={key}")

    start = time.time()
    result = compute_fn(filters)
    duration = round((time.time() - start) * 1000, 2)
    cache_logger.info(f"COMPUTE_DONE | endpoint={endpoint} | time_ms={duration}")

    try:
        redis_client.set(key, _safe_json_dumps(result), ex=REDIS_TTL_SECONDS)
        cache_logger.info(f"CACHE_WRITE | endpoint={endpoint} | key={key}")
    except Exception as e:
        cache_logger.error(f"CACHE_ERROR_WRITE | endpoint={endpoint} | error={str(e)}")

    try:
        store_query_log(endpoint, filters, result)
        cache_logger.info(f"SQLITE_WRITE | endpoint={endpoint}")
    except Exception as e:
        cache_logger.error(f"SQLITE_ERROR | endpoint={endpoint} | error={str(e)}")

    cache_logger.info(f"REQUEST_END | endpoint={endpoint} | source=compute")
    return result
