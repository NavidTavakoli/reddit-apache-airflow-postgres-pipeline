from __future__ import annotations

import os
import re
import csv
import time
import random
import hashlib
import datetime as dt
from typing import List, Dict, Any, Optional

import requests
from requests import auth
from requests.exceptions import RequestException, Timeout, ConnectionError as ReqConnectionError

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.log.logging_mixin import LoggingMixin

# ------------------ Config ------------------
SUBREDDIT = os.getenv("SUBREDDIT", "italytravel")
LIMIT = int(os.getenv("LIMIT", "10"))

# توصیه: UA معنادار بفرستید
USER_AGENT = os.getenv(
    "REDDIT_UA",
    "NavidRedditCrawler/1.0 (contact: example@example.com) PythonRequests",
)

OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/home/tahmast/airflow/data")

# در تولید، حتماً یک مقدار طولانی و تصادفی از طریق env ست کنید
GDPR_SALT = os.getenv("GDPR_SALT", "dev-salt-change-me")

# زمان‌بندی قابل تنظیم از طریق env
CRON_SCHEDULE = os.getenv("CRON_SCHEDULE", "*/10 * * * *")  # پیش‌فرض هر 10 دقیقه

# OAuth (اختیاری ولی شدیداً توصیه‌شده)
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")

_logger = LoggingMixin().log

RE_EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
RE_LONG_DIGITS = re.compile(r"[0-9]{7,}")

# کش ساده برای توکن
_token_cache: Dict[str, Any] = {"access_token": None, "expires_at": 0.0}


def _sanitize_title(title: str) -> str:
    title = (title or "").replace("\n", " ")
    title = re.sub(r"\s+", " ", title).strip()
    title = RE_EMAIL.sub("[redacted-email]", title)
    title = RE_LONG_DIGITS.sub("[redacted-number]", title)
    return title[:300]


def _hash_value(val: Optional[str], salt: str = GDPR_SALT) -> str:
    if not val:
        return ""
    h = hashlib.sha256()
    h.update((salt + str(val)).encode("utf-8"))
    return h.hexdigest()


def _get_oauth_token() -> Optional[str]:
    """در صورت تنظیم بودن client_id/secret، توکن userless می‌گیرد و کش می‌کند."""
    if not (REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET):
        return None

    now = time.time()
    token = _token_cache.get("access_token")
    exp = _token_cache.get("expires_at", 0.0)
    if token and now < (exp - 60):
        return token

    headers = {"User-Agent": USER_AGENT}
    try:
        r = requests.post(
            "https://www.reddit.com/api/v1/access_token",
            auth=auth.HTTPBasicAuth(REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET),
            data={"grant_type": "client_credentials"},
            headers=headers,
            timeout=20,
        )
        if r.status_code != 200:
            _logger.warning("OAuth token fetch failed: %s %s", r.status_code, r.text[:200])
            return None
        data = r.json()
        token = data.get("access_token")
        expires_in = data.get("expires_in", 3600)
        if token:
            _token_cache["access_token"] = token
            _token_cache["expires_at"] = now + int(expires_in)
            return token
        return None
    except (RequestException, Timeout, ReqConnectionError) as e:
        _logger.warning("OAuth token network error: %s", e)
        return None


def _fetch_last_posts(subreddit: str, limit: int) -> List[Dict[str, Any]]:
    """
    تلاش می‌کند با OAuth (oauth.reddit.com) بخواند؛
    اگر نشد، به endpoint عمومی (www.reddit.com) سوییچ می‌کند.
    دارای backoff نمایی + jitter و احترام به Retry-After.
    """
    token = _get_oauth_token()
    if token:
        base_url = f"https://oauth.reddit.com/r/{subreddit}/new"
        headers = {
            "User-Agent": USER_AGENT,
            "Authorization": f"bearer {token}",
            "Accept": "application/json",
        }
    else:
        base_url = f"https://www.reddit.com/r/{subreddit}/new.json"
        headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}

    # پارامترها (در oauth endpoint پسوند .json لازم نیست)
    params = {"limit": min(int(limit), 100), "raw_json": 1}

    backoff = 2.0
    last_status = None

    for attempt in range(8):
        try:
            resp = requests.get(base_url, headers=headers, params=params, timeout=20)
            last_status = resp.status_code
        except (RequestException, Timeout, ReqConnectionError) as e:
            _logger.warning("Network error (attempt %d): %s", attempt + 1, e)
            sleep_s = backoff + random.uniform(0, 1.7)
            time.sleep(sleep_s)
            backoff = min(backoff * 2, 64)
            continue

        if resp.status_code == 200:
            try:
                data = resp.json()
            except ValueError:
                _logger.warning("Invalid JSON received from Reddit.")
                # تلاش دوباره با backoff
                time.sleep(backoff + random.uniform(0, 1.5))
                backoff = min(backoff * 2, 64)
                continue

            # ساختار پاسخ در oauth و non-oauth مشابه است: data -> children
            items = (data or {}).get("data", {}).get("children", [])
            out: List[Dict[str, Any]] = []
            for it in items:
                d = it.get("data", {}) if isinstance(it, dict) else {}
                created_utc = d.get("created_utc")
                created_at = (
                    dt.datetime.fromtimestamp(created_utc, tz=dt.timezone.utc)
                    .isoformat()
                    .replace("+00:00", "Z")
                    if created_utc
                    else None
                )
                permalink = d.get("permalink")
                out.append(
                    {
                        "post_id": d.get("id"),
                        "created_at": created_at,
                        "score": d.get("score"),
                        "num_comments": d.get("num_comments"),
                        "title_sanitized": _sanitize_title(d.get("title")),
                        # مقادیر خام PII ذخیره نمی‌شوند؛ صرفاً جهت هش در مرحله‌ی CSV
                        "author": d.get("author"),
                        "permalink": (f"https://www.reddit.com{permalink}" if permalink else None),
                        "url": d.get("url"),
                    }
                )
                if len(out) >= limit:
                    break
            _logger.info("Fetched %d posts from r/%s (attempt %d)", len(out), subreddit, attempt + 1)
            return out

        # ریت‌لیمیت/ارورهای موقتی
        if resp.status_code in {429, 403, 500, 502, 503, 504}:
            ra = resp.headers.get("Retry-After")
            wait = backoff
            if ra:
                try:
                    wait = max(wait, float(ra))
                except ValueError:
                    pass
            # اگر OAuth در دسترس نیست و 403/429 زیاد دارید، لاگ بده
            if resp.status_code in {429, 403} and not token:
                _logger.warning(
                    "HTTP %s on public endpoint. Consider setting REDDIT_CLIENT_ID/SECRET for OAuth.",
                    resp.status_code,
                )
            sleep_s = wait + random.uniform(0, 1.7)
            _logger.warning("HTTP %s from Reddit; sleeping %.1fs (attempt %d)", resp.status_code, sleep_s, attempt + 1)
            time.sleep(sleep_s)
            backoff = min(backoff * 2, 64)
            # اگر توکن داشتیم و 401/403 گرفتیم، یک بار invalidate و دوباره تلاش می‌کنیم
            if resp.status_code in {401, 403} and token:
                _token_cache["access_token"] = None
                _token_cache["expires_at"] = 0.0
                token = _get_oauth_token()
                if token:
                    headers["Authorization"] = f"bearer {token}"
            continue

        # سایر کدها → raise
        try:
            resp.raise_for_status()
        except Exception as e:
            _logger.error("Reddit API error (attempt %d): %s", attempt + 1, e)
            raise

    raise RuntimeError(f"Reddit API failed after retries; last_status={last_status}")


def _write_csv(rows: List[Dict[str, Any]]) -> str:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    # نام فایل شامل مهر زمان جهت یکتا بودن
    filename = f"italytravel_hashed_{int(time.time())}.csv"
    out_path = os.path.join(OUTPUT_DIR, filename)
    tmp_path = out_path + ".tmp"

    fieldnames = [
        "post_id",
        "created_at",
        "score",
        "num_comments",
        "title_sanitized",
        "author_hash",
        "permalink_hash",
        # در صورت نیاز URL خارجی را هم هش کنید و ستون را اضافه نمایید:
        # "external_url_hash",
    ]

    # نوشتن اتمیک: ابتدا tmp سپس replace
    with open(tmp_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(
                {
                    "post_id": r.get("post_id"),
                    "created_at": r.get("created_at"),
                    "score": r.get("score"),
                    "num_comments": r.get("num_comments"),
                    "title_sanitized": r.get("title_sanitized"),
                    "author_hash": _hash_value(r.get("author")),
                    "permalink_hash": _hash_value(r.get("permalink")),
                    # "external_url_hash": _hash_value(r.get("url")),
                }
            )
    os.replace(tmp_path, out_path)
    return out_path


def fetch_and_save_callable(**context):
    if GDPR_SALT == "dev-salt-change-me":
        _logger.warning("GDPR_SALT is using a dev default. Set a strong secret via env for production.")
    if not (REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET):
        _logger.warning(
            "Running WITHOUT OAuth (REDDIT_CLIENT_ID/SECRET not set). Expect tighter rate limits on Reddit."
        )

    rows = _fetch_last_posts(SUBREDDIT, LIMIT)
    path = _write_csv(rows)
    _logger.info("Wrote CSV: %s (rows=%d)", path, len(rows))
    print(f"Wrote CSV: {path}")
    return path


# ------------------ DAG ------------------

default_args = {
    "owner": "Data Science Group",
    "depends_on_past": False,
    "start_date": dt.datetime(2025, 9, 1, 0, 0, tzinfo=dt.timezone.utc),
    "retries": 5,
    "retry_delay": dt.timedelta(minutes=5),
    "retry_exponential_backoff": True,     # Airflow 2.x
    "max_retry_delay": dt.timedelta(hours=1),
}

dag = DAG(
    dag_id="Reddit--CSV",
    default_args=default_args,
    schedule=CRON_SCHEDULE,                 # قابل تنظیم با env
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=dt.timedelta(minutes=20),
    tags=["reddit", "csv", "gdpr"],
)

get_italytravel_csv = PythonOperator(
    task_id="Get-Reddit-Italytravel-CSV",
    python_callable=fetch_and_save_callable,
    dag=dag,
)

done = EmptyOperator(task_id="Done", dag=dag)

get_italytravel_csv >> done
