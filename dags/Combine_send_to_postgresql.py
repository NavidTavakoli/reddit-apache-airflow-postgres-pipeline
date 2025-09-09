from __future__ import annotations

import os
import csv
import re
import time
import shutil
import hashlib
import datetime as dt
from typing import Dict, Any, List, Tuple

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ------------------ Paths & Config ------------------
BASE = "/home/tahmast/airflow"
DATA_DIR = os.getenv("DATA_DIR", f"{BASE}/data")
COMBINE_DIR = os.getenv("COMBINE_DIR", f"{DATA_DIR}/combined")
LOADED_DIR = os.getenv("LOADED_DIR", f"{DATA_DIR}/loaded")
CSV_GLOB_PREFIX = os.getenv("CSV_GLOB_PREFIX", "italytravel_")  # match all CSV files starting with this prefix

PG_CONN_ID = os.getenv("PG_CONN_ID", "pg_reddit")
PG_SCHEMA = os.getenv("PG_SCHEMA", "reddit_schema")
PG_TABLE = os.getenv("PG_TABLE", "redit_table")

GDPR_SALT = os.getenv("GDPR_SALT", "dev-salt-change-me")

_logger = LoggingMixin().log

# Final columns for DB and combined CSV
DB_COLUMNS = [
    "thing_key",
    "thing_type",
    "id",                 
    "created_at",
    "score",
    "num_comments",
    "title_sanitized",
    "author_hash",
    "permalink",          
    "subreddit",
    "flair_text",
]

# Normalize permalink for consistency
_re_trailing_slash = re.compile(r"/+$")

def _norm_permalink(s: str) -> str:
    if not s:
        return ""
    s = s.strip().lower()
    s = _re_trailing_slash.sub("", s)
    return s

def _sha256_hex(salt: str, value: str) -> str:
    h = hashlib.sha256()
    h.update((salt + (value or "")).encode("utf-8"))
    return h.hexdigest()

def _detect_csv_files() -> List[str]:
    files = []
    if not os.path.isdir(DATA_DIR):
        return files
    for name in sorted(os.listdir(DATA_DIR)):
        if not name.endswith(".csv"):
            continue
        if not name.startswith(CSV_GLOB_PREFIX):
            continue
        files.append(os.path.join(DATA_DIR, name))
    return files

def _read_csv(path: str) -> Tuple[List[Dict[str, Any]], List[str]]:
    with open(path, "r", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        rows = list(rdr)
        return rows, rdr.fieldnames or []

def _normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    reddit_id = row.get("post_id") or row.get("id") or ""
    thing_type = row.get("thing_type") or "t3"

    # permalink (no hashing; just normalized)
    pl = row.get("permalink") or row.get("url") or ""
    permalink_norm = _norm_permalink(pl)

    # thing_key: hash(type + (id or permalink)) to keep it unique and stable
    thing_key = row.get("thing_key")
    if not thing_key:
        base = f"{thing_type}:{reddit_id or permalink_norm}"
        thing_key = _sha256_hex(GDPR_SALT, base)

    created_at = row.get("created_at") or ""
    score = row.get("score") or 0
    num_comments = row.get("num_comments") or 0
    try: score = int(score)
    except: score = 0
    try: num_comments = int(num_comments)
    except: num_comments = 0

    title_sanitized = row.get("title_sanitized") or row.get("title") or ""
    author_hash = row.get("author_hash") or ""
    subreddit = row.get("subreddit") or "ItalyTravel"
    flair_text = row.get("flair_text") or ""

    return {
        "thing_key": thing_key,
        "thing_type": thing_type,
        "id": reddit_id,
        "created_at": created_at,
        "score": score,
        "num_comments": num_comments,
        "title_sanitized": title_sanitized,
        "author_hash": author_hash,
        "permalink": permalink_norm,
        "subreddit": subreddit,
        "flair_text": flair_text,
    }

def _combine_csvs(**context) -> str:
   
    os.makedirs(COMBINE_DIR, exist_ok=True)
    os.makedirs(LOADED_DIR, exist_ok=True)

    src_files = _detect_csv_files()
    if not src_files:
        _logger.info("No CSV files found in %s", DATA_DIR)
        raise RuntimeError("No CSV files to combine.")

    out_name = f"italytravel_combined_{dt.datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.csv"
    out_path = os.path.join(COMBINE_DIR, out_name)
    tmp_out = out_path + ".tmp"

    seen_keys: set[str] = set()
    used_files: List[str] = []
    kept_rows: List[Dict[str, Any]] = []

    for path in src_files:
        try:
            rows, cols = _read_csv(path)
        except Exception as e:
            _logger.warning("Skip %s (read error: %s)", path, e)
            continue

        if not rows:
            _logger.warning("Skip %s (empty file)", path)
            continue

        kept_any = False
        for r in rows:
            nr = _normalize_row(r)
            k = nr.get("thing_key")
            if not k:
                continue
            if k in seen_keys:
                continue
            seen_keys.add(k)
            kept_rows.append(nr)
            kept_any = True

        if kept_any:
            used_files.append(path)
        else:
            _logger.warning("No usable rows from %s", path)

    # If nothing valid was kept, still empty the data/ folder by moving sources to loaded/, then error out
    if not kept_rows:
        for p in src_files:
            dst = os.path.join(LOADED_DIR, os.path.basename(p))
            try:
                shutil.move(p, dst)
            except Exception as e:
                _logger.warning("Could not move %s to loaded when empty: %s", p, e)
        raise RuntimeError("After reading files, no valid rows left to write.")

    # Write the combined CSV atomically via .tmp
    with open(tmp_out, "w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=DB_COLUMNS)
        wr.writeheader()
        for row in kept_rows:
            wr.writerow({col: row.get(col, "") for col in DB_COLUMNS})
    os.replace(tmp_out, out_path)

    # Move consumed source files to loaded/
    for p in used_files:
        dst = os.path.join(LOADED_DIR, os.path.basename(p))
        try:
            shutil.move(p, dst)
        except Exception as e:
            _logger.warning("Could not move %s to loaded: %s", p, e)

    # Move skipped source files to loaded/ as well so data/ is fully emptied
    for p in set(src_files) - set(used_files):
        dst = os.path.join(LOADED_DIR, os.path.basename(p))
        try:
            shutil.move(p, dst)
        except Exception as e:
            _logger.warning("Could not move skipped %s to loaded: %s", p, e)

    _logger.info(
        "Combined %d rows into %s; moved %d source files to loaded/ (and %d skipped).",
        len(kept_rows), out_path, len(used_files), len(set(src_files) - set(used_files))
    )
    print(out_path)
    return out_path



def _load_to_postgres(**context):
    ti = context["ti"]
    combined_path = ti.xcom_pull(task_ids="combine_csvs", key="return_value")
    if not combined_path or not os.path.isfile(combined_path):
        raise RuntimeError("Combined CSV path not found from XCom.")

    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    tmp_table = f"{PG_TABLE}_stage_{int(time.time())}"

    def q_ident(s: str) -> str:
        return '"' + s.replace('"', '""') + '"'

    SCHEMA_Q = q_ident(PG_SCHEMA)   # e.g., "reddit_schema"
    TABLE_Q  = q_ident(PG_TABLE)    # e.g., "redit_table"
    TMP_Q    = q_ident(tmp_table)

    # Create TEMP TABLE like the target (no constraints enforced here)
    create_stage = f"""
        CREATE TEMP TABLE {TMP_Q}
        (LIKE {SCHEMA_Q}.{TABLE_Q} INCLUDING DEFAULTS);
    """

    copy_sql = f"""
        COPY {TMP_Q} ({", ".join(DB_COLUMNS)})
        FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"');
    """

    # UPSERT syntax
    insert_sql = f"""
        INSERT INTO {SCHEMA_Q}.{TABLE_Q} ({", ".join(DB_COLUMNS)})
        SELECT {", ".join(DB_COLUMNS)}
        FROM {TMP_Q}
        ON CONFLICT (thing_key) DO UPDATE SET
            score          = EXCLUDED.score,
            num_comments   = EXCLUDED.num_comments,
            title_sanitized= EXCLUDED.title_sanitized,
            subreddit      = EXCLUDED.subreddit,
            flair_text     = EXCLUDED.flair_text;
    """

    conn = hook.get_conn()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute(create_stage)
            with open(combined_path, "r", encoding="utf-8") as f:
                cur.copy_expert(copy_sql, f)
            cur.execute(insert_sql)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

# ------------------ DAG ------------------
default_args = {
    "owner": "Data Science Group",
    "depends_on_past": False,
    "start_date": dt.datetime(2025, 9, 1, 0, 0),
    "retries": 1,
}

SCHEDULE = os.getenv("COMBINE_SCHEDULE", "5 * * * *")



dag = DAG(
    dag_id="Sending_to_the_PostgreSQL",
    default_args=default_args,
    schedule=SCHEDULE,   
    catchup=False,
    max_active_runs=1,
    tags=["reddit", "csv", "postgres"],
)

combine = PythonOperator(
    task_id="combine_csvs",
    python_callable=_combine_csvs,
    dag=dag,
)

load = PythonOperator(
    task_id="Insert_to_postgreSQL",
    python_callable=_load_to_postgres,
    dag=dag,
)

done = EmptyOperator(task_id="done", dag=dag)

combine >> load >> done
