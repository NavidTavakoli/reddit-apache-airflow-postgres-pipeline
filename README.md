# Airflow Reddit ETL: Fetch   →  Postgres (GDPR-aware)

This repo contains two Apache Airflow DAGs that fetch posts from Reddit, write them to CSVs, then combine and load them into PostgreSQL with proper de-duplication and GDPR-aware hashing.

1) **Fetch** – reads the latest posts from a subreddit, sanitizes titles, hashes sensitive fields, and writes a CSV. (`dags/Fetch_reddit.py`)
2) **Combine & Load** – finds CSVs, normalizes + de-duplicates rows, writes a single combined CSV, and **UPSERTs** into PostgreSQL (append new, update existing). (`dags/Combine_send_to_postgresql.py`)

---

## Contents

```
Airflow/
├─ airflow.cfg              # Airflow config (example)
├─ dags/
│  ├─ Fetch_reddit.py       # DAG 1: fetch & write CSV
│  └─ Combine_send_to_postgresql.py  # DAG 2: combine & load to Postgres
├─ data/                    # Incoming CSVs (DAG 1 writes here)
│  ├─ combined/             # DAG 2 writes the combined CSV here
│  └─ loaded/               # Source & combined CSVs archived here
├─ start.sh / stop.sh       # (optional) helper scripts
├─ logs/

```



## Apache Airflow Configuration

`airflow.cfg` change (under the `[database]` section)

```ini
[database]
# sql_alchemy_conn = sqlite:////home/tahmast/airflow/airflow.db
sql_alchemy_conn = postgresql+psycopg2://reddit:123456@localhost:5432/reddit_database
sql_alchemy_pool_pre_ping = True
sql_engine_encoding = utf-8
sql_alchemy_schema = public

```

### Suggested steps

1. **Stop Airflow** (use your `stop.sh`).
2. **Backup and edit**:

```bash
cp "$AIRFLOW_HOME/airflow.cfg" "$AIRFLOW_HOME/airflow.cfg.bak"

# Comment the SQLite line if it’s still active
sed -i 's|^sql_alchemy_conn = sqlite:.*|# &|' "$AIRFLOW_HOME/airflow.cfg"

# Set the Postgres URI (overwrites any existing sql_alchemy_conn line)
sed -i 's|^sql_alchemy_conn = .*|sql_alchemy_conn = postgresql+psycopg2://reddit:123456@localhost:5432/reddit_database|' "$AIRFLOW_HOME/airflow.cfg"

```

3. **Install driver/provider (if not already installed):**

```bash
pip install psycopg2-binary "apache-airflow-providers-postgres>=6.0"
```

4. **Initialize Airflow metadata DB:**

   Fresh setup:

```bash
./start.sh
```

5. **Verify the setting:**

```bash
airflow config get-value database sql_alchemy_conn

# should print the Postgres URI

```

Tip: Prefer using an env var instead of hardcoding the password:



```bash
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN='postgresql+psycopg2://USERNAME:PASSWORD@localhost:5432/DATABASE_NAME'

```

Env vars override `airflow.cfg`. Keeping Airflow **metadata** in a separate DB from your project data is also a good practice (optional, but cleaner).

## Quick Ops Notes: processes, ports, and a big red button 😉

When Airflow acts weird (stuck UI, ghost runs, “port already in use”), **check what’s actually running** and which **ports are bound** before you restart. A few one-liners you’ll use a lot:

**See running Airflow processes**

```bash
ps aux | grep -i airflow
# or, more precise:
pgrep -af "airflow|gunicorn|scheduler|triggerer|worker"

```

**Check if the API/Web UI port is taken (default :8080)**

```bash
lsof -i :8080
# or:
ss -ltnp 'sport = :8080'

```

**Other useful Airflow ports (defaults)**

- **8080** – Airflow API/Web UI (your setup runs the API server on 8080)
- **8793** – Worker log server
- **8794** – Trigger log server
- **8974** – Scheduler health check (if enabled)
- **5555** – Flower (only if you run Celery+Flower)
- **5432** – Postgres (metadata DB)

> Tip: if a port is stuck, find who owns it and stop **that** process rather than randomly `kill -9`ing stuff:

```bash
lsof -i :8080 -sTCP:LISTEN
kill <PID>            # graceful
kill -9 <PID>         # only if necessary 😅

```

### Prefer clean shutdowns

If you have `stop.sh`, use it first (it does graceful stops). Fallbacks:

```bash
# Try graceful first:
pkill -f "airflow api"
pkill -f "airflow scheduler"
pkill -f "airflow triggerer"

# If they refuse to die:
pkill -9 -f "airflow (api|scheduler|triggerer)"

```

### ⚠️ About `airflow database reset`

```bash
airflow db reset
```

- **Destructive**: drops and recreates the **metadata DB**. You will lose **DAG run history, task instances, connections, variables, pools, users**, etc.

- Use **only in development** when you truly want a clean slate.

- If you must do it in prod (really, don’t), **back up** your metadata DB first:

```bash
pg_dump --no-owner --dbname="postgresql://user:pass@host:5432/airflow_db" > airflow_meta_backup.sql

```

- After a reset, you’ll need to **recreate connections/variables** (or restore from backup). 😉

Stay tidy: verify processes with `ps`, verify ports with `lsof/ss`, and keep `db reset` as your last resort button.

## What it does

### DAG 1 — Fetch & Write CSV (`Fetch_reddit.py`)
- Pulls recent posts from `r/<SUBREDDIT>`; prefers OAuth (`oauth.reddit.com`) and falls back to the public endpoint on failure (exponential backoff, honors `Retry-After`).
- **GDPR awareness**:
  - Only `author` is hashed → `author_hash` (salted with `GDPR_SALT`).
  - `id`, `permalink`, `subreddit`, etc. remain raw (public/metadata).
  - `thing_key` is a stable SHA256 derived from Reddit “name”/type → future-proof primary key.
- Writes `italytravel_<ts>.csv` to `data/` with columns:

| thing_key | thing_type | id   | created_at | score | num_comments | title_sanitized | author_hash | permalink | subreddit | flair_text |
| --------- | ---------- | ---- | ---------- | ----- | ------------ | --------------- | ----------- | --------- | --------- | ---------- |

### DAG 2 — Combine & Load to Postgres (`reddit_csv_combine_load.py`)
- Scans `data/` for CSVs matching `CSV_GLOB_PREFIX` (default: `italytravel_`).
- Normalizes fields (e.g., permalink cleanup), computes/uses `thing_key`, and **de-duplicates** rows in memory by `thing_key`.
- Writes one `combined/italytravel_combined_<UTC>.csv` (atomic `.tmp` → final).
- **Moves** all source CSVs from `data/` to `data/loaded/` so `data/` is emptied each run.
- Loads the combined CSV into a **TEMP stage table** via `COPY`, then **UPSERTs** into the target table:

```sql
ON CONFLICT (thing_key) DO UPDATE SET
  score           = EXCLUDED.score,
  num_comments    = EXCLUDED.num_comments,
  title_sanitized = EXCLUDED.title_sanitized,
  subreddit       = EXCLUDED.subreddit,
  flair_text      = EXCLUDED.flair_text;
```

After a successful commit, the **combined CSV is also moved** to `data/loaded/` (cut, not copy).

## PostgreSQL schema (example)

Schema: `reddit_schema`
 Table: `redit_table` (PK: `thing_key`)

```sql
CREATE SCHEMA IF NOT EXISTS reddit_schema;

CREATE TABLE IF NOT EXISTS reddit_schema.redit_table (
  thing_key       TEXT PRIMARY KEY,   -- stable hashed key
  thing_type      TEXT NOT NULL,      -- e.g. 't3'
  id              TEXT,               -- raw post id
  created_at      TIMESTAMPTZ,        -- parses ISO8601 '...Z'
  score           INTEGER,
  num_comments    INTEGER,
  title_sanitized TEXT,
  author_hash     TEXT,
  permalink       TEXT,
  subreddit       TEXT,
  flair_text      TEXT
);

-- Optional: helper indexes
CREATE INDEX IF NOT EXISTS redit_table_id_idx        ON reddit_schema.redit_table (id);
CREATE INDEX IF NOT EXISTS redit_table_subreddit_idx ON reddit_schema.redit_table (subreddit);

```

## Airflow → Postgres connection

Create a connection named **`pg_reddit`** (the DAGs use this `conn_id`):

```bash
# Install the Postgres provider if needed
pip install apache-airflow-providers-postgres

# Create the connection (CLI)
airflow connections add pg_reddit \
  --conn-type postgres \
  --conn-host localhost \
  --conn-port 5432 \
  --conn-login reddit \
  --conn-password 123456 \
  --conn-schema reddit_database

```

If you run Airflow **API server** (v3.0.6) without the classic webserver UI, use the CLI as above to manage connections.

### **Environment variables**

| Var                    | Purpose                                                 | Default                      |
| ---------------------- | ------------------------------------------------------- | ---------------------------- |
| `SUBREDDIT`            | Subreddit to crawl                                      | `italytravel`                |
| `LIMIT`                | Max posts per fetch                                     | `10`                         |
| `REDDIT_UA`            | Requests User-Agent                                     | sensible default             |
| `OUTPUT_DIR`           | CSV output dir                                          | `/home/tahmast/airflow/data` |
| `GDPR_SALT`            | **Required in prod**. Salt for hashing sensitive fields | write-your-salt-here😉        |
| `CRON_SCHEDULE`        | Fetch DAG schedule (cron)                               | `*/10 * * * *`               |
| `DATA_DIR`             | Base data dir                                           | `$AIRFLOW_HOME/data`         |
| `COMBINE_DIR`          | Combined CSV dir                                        | `$DATA_DIR/combined`         |
| `LOADED_DIR`           | Archive dir                                             | `$DATA_DIR/loaded`           |
| `CSV_GLOB_PREFIX`      | Source CSV prefix                                       | `italytravel`                |
| `PG_CONN_ID`           | Airflow conn id                                         | `pg_reddit`                  |
| `PG_SCHEMA`            | Target schema                                           | `reddit_schema`              |
| `PG_TABLE`             | Target table                                            | `redit_table`                |
| `COMBINE_SCHEDULE`     | Combine/Load DAG schedule                               | `5 * * * *`                  |
| `REDDIT_CLIENT_ID`     | Reddit OAuth client id                                  | —                            |
| `REDDIT_CLIENT_SECRET` | Reddit OAuth client secret                              | —                            |

Set `REDDIT_CLIENT_ID` and `REDDIT_CLIENT_SECRET` to enable OAuth; you’ll hit fewer rate limits.

## How to run

1. **Configure** Airflow (see `airflow.cfg`) and ensure the Postgres provider is installed.
2. **Create** the Postgres connection (`pg_reddit`) via CLI (see above).
3. **Create** the schema/table (DDL above) if you haven’t already.
4. Put both DAG files into `~/airflow/dags/`.
5. Start your Airflow components (scheduler + API server), or use your `start.sh`.
6. The **Fetch** DAG writes CSVs to `data/`.
7. The **Combine & Load** DAG (default: every hour at minute 5) merges and loads into Postgres and archives files to `data/loaded/`.

### Data flow

```mermaid
flowchart TD
  A[[Reddit API]]
  B[[Fetch DAG]]
  C[/"data/italytravel_<ts>.csv"\nRaw CSVs/]
  D[[Combine & Load DAG]]
  E[/"data/combined/italytravel_combined_<utc>.csv"\nNormalized & de-duplicated/]
  F[(Postgres\nUPSERT on `thing_key`)]
  G[/"data/loaded/"\nArchive/]

  A --> B --> C --> D --> E --> F --> G

  %% Group filesystem nodes for clarity
  subgraph "Local Filesystem"
    direction TB
    C
    E
    G
  end

  classDef store fill:#eef,stroke:#99c,stroke-width:1px,color:#111;
  class C,E,G store;

```

**De-duplication** happens in memory by `thing_key` and again at DB level via `ON CONFLICT (thing_key)`.

**Updates** refresh selected fields (`score`, `num_comments`, `title_sanitized`, `subreddit`, `flair_text`).



## PostgreSQL Export — Sample Preview

> File: `redit_table_202509091827.csv` (exported from `reddit_schema.redit_table` via `\copy` / DBeaver).

| primary key | type | id_hash | created_at     | score | comments | title                                                        | author_hash | permalink_hash | subreddit   | flair          |
| ----------- | ---- | ------- | -------------- | ----- | -------- | ------------------------------------------------------------ | ----------- | -------------- | ----------- | -------------- |
| 928092      | t3   | c39634  | 9/9/2025 18:42 | 1     | 5        | Is it safe to drive in February through Dolomites Region     | 7dae71      | 8e36e0         | ItalyTravel | Transportation |
| 6944d3      | t3   | 6189a6  | 9/9/2025 18:17 | 1     | 6        | Looking for recommendations for which city to hang out for a week in late November as a mid-30s vegetarian couple | 7578b0      | 9897ee         | ItalyTravel | Other          |
| 2934cc      | t3   | 7912d5  | 9/9/2025 18:09 | 1     | 3        | Itabus is it reliable?                                       | 71e304      | ad5463         | ItalyTravel | Transportation |

<sub>*(Rows above are illustrative; actual values come from the CSV.)*</sub>

## Privacy notes

- **Hashed**: `author_hash` `id`, `permalink`,(with `GDPR_SALT`).
- **Not hashed**:  `subreddit`, `title_sanitized`, etc.
- **Titles** are sanitized to remove emails and long digit sequences before writing.

## Troubleshooting

- **`AirflowNotFoundException: conn_id 'pg_reddit' isn't defined`**
   Create the connection via CLI, then restart the scheduler.
- **No CSVs found / “no valid rows left”**
   Ensure the Fetch DAG ran and wrote files to `data/`. The Combine & Load DAG moves everything to `data/loaded/` each run.
- **Rate limits (429)**
   Set `REDDIT_CLIENT_ID/SECRET` to use OAuth; the DAG honors `Retry-After` and uses exponential backoff.
- **“empty cryptography key” warning on `airflow connections add`**
   Optional: set `FERNET_KEY` in `airflow.cfg` to encrypt connection passwords at rest.

## Notes

- Verified on **Airflow 3.0.6** with the **API server** (no classic webserver).

- DB browsing/editing via **DBeaver** on `reddit_database` / `reddit_schema`.

- Typical schedules: *fetch every 10 min*, *combine/load hourly*. Adjust via env.

  ```pgsql
  Created by Navid Tavakoli Shalmani, Data Engineer.
  Want to know more? 
  
  ```

  ###### Visit my Website:   https://navidtavakolishalmani.com

  

