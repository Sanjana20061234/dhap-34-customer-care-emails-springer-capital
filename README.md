# Customer Care Emails Pipeline (DHAP-34)

I built this pipeline to ingest customer support email data from a local CSV into PostgreSQL using Apache Airflow running in Docker.

The pipeline executes five tasks:
1. `check_manifest_status`: Verifies that `manifest.yaml` exists and has `status: done`.
2. `read_csv`: Reads `customer_care_emails.csv` as UTF-8.
3. `validate_schema`: Enforces the contract defined in `schema.yaml`. It checks for missing or extra columns, nulls in non-nullable fields, and out-of-range values.
4. `transform`: Normalizes timestamps to UTC, parses stringified list columns into Python lists, fixes text encoding issues, standardizes product casing, and generates a SHA-256 primary key.
5. `load_to_postgres`: Loads records into the target PostgreSQL database using batch upserts (`ON CONFLICT (email_key) DO UPDATE`).

## Prerequisites

- Docker Desktop (v20+ with Docker Compose v2+)
- WSL2 backend enabled if running on Windows

## Setup

1. Copy `.env.example` to `.env`:
   ```bash
   cp .env.example .env
   ```
2. Verify credentials and ports in `.env`. By default, the Airflow UI runs on port 8080, and the target PostgreSQL instance maps to host port 5433.
3. Ensure `customer_care_emails.csv`, `manifest.yaml`, and `schema.yaml` are located in this project directory.
4. Start the stack:
   ```bash
   docker compose up -d
   ```
   Docker Compose pulls the pinned `apache/airflow:2.10.5-python3.11` and `postgres:15-alpine` images, initializes the Airflow metadata database, provisions the target database, and starts the scheduler and webserver.

## Airflow UI and DAG Execution

1. Open `http://localhost:8080` in your browser.
2. Sign in with username `airflow` and password `airflow`.
3. Unpause and trigger `load_customer_care_emails_to_postgres` from the UI.

Alternatively, trigger the DAG from the command line:
```bash
docker exec airflow-scheduler airflow dags trigger load_customer_care_emails_to_postgres
```

Check run status:
```bash
docker exec airflow-scheduler airflow dags list-runs -d load_customer_care_emails_to_postgres
```

## Verifying Loaded Rows

Query the target PostgreSQL container directly using `psql`:

```bash
docker exec target-postgres-db psql -U postgres -d customer_care -c "SELECT count(*) FROM customer_care_emails;"
```
Expected output:
```text
 count 
-------
  2259
(1 row)
```

Inspect a complete record:
```bash
docker exec target-postgres-db psql -U postgres -d customer_care -x -c "SELECT * FROM customer_care_emails LIMIT 1;"
```

## Idempotency

I wrote the target loader with an idempotent upsert:
```sql
INSERT INTO customer_care_emails (...)
VALUES (...)
ON CONFLICT (email_key) DO UPDATE SET ...;
```
If you trigger the DAG a second or third time, existing rows are updated rather than duplicated. The total row count remains exactly 2,259.

## Malformed CSV Failure Test

To verify that schema validation catches bad data before it touches the database:

1. Rename a column in `customer_care_emails.csv` (for example, change `subject` to `renamed_subject`):
   ```python
   import pandas as pd
   df = pd.read_csv("customer_care_emails.csv")
   df.rename(columns={"subject": "renamed_subject"}).to_csv("customer_care_emails.csv", index=False)
   ```
2. Trigger the DAG:
   ```bash
   docker exec airflow-scheduler airflow dags trigger load_customer_care_emails_to_postgres
   ```
3. Check task statuses:
   ```bash
   docker exec airflow-scheduler airflow tasks states-for-dag-run load_customer_care_emails_to_postgres <RUN_ID>
   ```
   The `validate_schema` task marks as `failed` with `AirflowFailException: Schema mismatch: missing=['subject'], extra=['renamed_subject']`. Downstream tasks `transform` and `load_to_postgres` are marked as `upstream_failed` and never run.
4. Verify table count in Postgres remains 2,259.
5. Restore the original CSV:
   ```python
   df.to_csv("customer_care_emails.csv", index=False)
   ```

## Data Issues I Found

1. **Repeating `thread_id`**: Multiple email exchanges share the same `thread_id`. Because `thread_id` cannot serve as a unique primary key, I generated `email_key` by hashing `thread_id | utc timestamp | sender | md5(message_body)` with SHA-256. This produced 2,259 distinct keys for 2,259 rows.
2. **Mixed timestamp formats**: The source CSV contained timestamps formatted with varying offsets (`+00:00`), ISO strings (`Z`), millisecond precision, and naive timestamps. I normalized all timestamps to UTC ISO strings.
3. **List-in-string columns**: `email_types` and `product_types` were stored as string literals containing Python lists (e.g., `"['inquiry']"`). I parsed these with `ast.literal_eval` and mapped them to PostgreSQL `TEXT[]` arrays.
4. **Inconsistent product casing**: Products had mixed casing, such as `"Cloud Management"` and `"Cloud management"`, or `"IAM Service"` and `"IAM service"`. I mapped them to canonical title-case values during transformation.
5. **Mojibake**: Characters such as `â€™` appeared in message bodies due to Windows-1252 / UTF-8 decoding mismatches. I replaced these with correct apostrophes and quotation marks.

## Troubleshooting

- **Ports 8080 or 5433 in use**: If port 8080 is taken, update the webserver port mapping in `docker-compose.yaml`. If port 5432 is occupied by a local PostgreSQL installation, target Postgres is already mapped to port 5433 via `TARGET_POSTGRES_PORT` in `.env`.
- **Docker daemon not running**: On Windows, ensure Docker Desktop is running and WSL2 integration is active. Verify with `docker compose ps`.
- **File mount permissions**: Airflow containers run under UID 50000. Files mounted from Windows host are readable, but ensure file permissions allow read access.
- **Re-applying `ddl.sql`**: PostgreSQL only executes scripts in `/docker-entrypoint-initdb.d/` when the data directory is completely empty. If you modify `ddl.sql`, drop the existing database volume and restart:
  ```bash
  docker compose down -v
  docker compose up -d
  ```
