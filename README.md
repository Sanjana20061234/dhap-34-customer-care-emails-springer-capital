# Customer Care Emails Pipeline (DHAP-34)

This pipeline loads a local CSV of customer support emails into PostgreSQL. It runs as an Apache Airflow DAG inside Docker Compose, so the whole stack comes up with one command.

The DAG has five tasks:

1. `check_manifest_status`: checks that `manifest.yaml` exists and has `status: done`.
2. `read_csv`: reads `customer_care_emails.csv` as UTF-8.
3. `validate_schema`: checks the data against `schema.yaml`. It fails on missing or extra columns, nulls in non-nullable columns and out-of-range values.
4. `transform`: converts timestamps to UTC, parses the list columns, fixes encoding problems, normalizes product casing and builds the primary key.
5. `load_to_postgres`: upserts the rows into the target table with `ON CONFLICT (email_key) DO UPDATE`.

## Files

| File | Purpose |
|---|---|
| `manifest.yaml` | Dataset name, source, license, row count, checksum and `status` |
| `schema.yaml` | Columns, types, nullable flags and the primary key |
| `ddl.sql` | `CREATE TABLE` for the target database |
| `docker-compose.yaml` | Airflow, its metadata database and the target PostgreSQL |
| `.env.example` | Placeholder settings. Copy to `.env` and edit |
| `dags/load_customer_care_emails_to_postgres.py` | The DAG |
| `customer_care_emails.csv` | The data (2,259 rows, 13 columns) |

## Data source

The data is `rtweera/customer_care_emails` on Hugging Face: https://huggingface.co/datasets/rtweera/customer_care_emails (license gpl-3.0). It is a synthetic set of customer care email threads.

To get it again, open the dataset page, go to "Files and versions", download `dataset.csv` and rename it to `customer_care_emails.csv`. Put it in this folder.

## Prerequisites

- Docker Desktop with Docker Compose v2
- On Windows, the WSL2 backend (run `wsl --install` in an admin PowerShell if Docker says WSL is missing)
- Free ports 8080 (Airflow UI) and 5433 (target PostgreSQL)

## Setup

1. Copy the example settings.

   PowerShell:
   ```powershell
   Copy-Item .env.example .env
   ```
   Bash:
   ```bash
   cp .env.example .env
   ```
2. Open `.env` and replace every `change_me` with your own value. The password in `AIRFLOW_CONN_TARGET_POSTGRES` must match `TARGET_POSTGRES_PASSWORD`. `.env` is in `.gitignore` and must not be committed.
3. Check that `customer_care_emails.csv`, `manifest.yaml` and `schema.yaml` are in this folder.
4. Start the stack:
   ```bash
   docker compose up -d
   ```
   The first run downloads the Airflow and PostgreSQL images, so it takes a few minutes. The `airflow-init` container exits when it finishes. That is normal.
5. Check that the services are healthy:
   ```bash
   docker compose ps
   ```

`ddl.sql` creates the target table the first time the target database starts.

## Running the DAG

1. Open http://localhost:8080.
2. Sign in with the admin username and password from your `.env` (`_AIRFLOW_WWW_USER_USERNAME` and `_AIRFLOW_WWW_USER_PASSWORD`).
3. Find `load_customer_care_emails_to_postgres` and click the play button, then "Trigger DAG".
4. Open the Grid view and wait until all five tasks are green.

To trigger it from the command line instead:
```bash
docker exec airflow-scheduler airflow dags trigger load_customer_care_emails_to_postgres
docker exec airflow-scheduler airflow dags list-runs -d load_customer_care_emails_to_postgres
```

## Checking the rows in PostgreSQL

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

If you changed `TARGET_POSTGRES_USER` or `TARGET_POSTGRES_DB` in `.env`, use those values in the command.

To look at one full row:
```bash
docker exec target-postgres-db psql -U postgres -d customer_care -x -c "SELECT * FROM customer_care_emails LIMIT 1;"
```

## Re-running is safe

The loader upserts on `email_key`:
```sql
INSERT INTO customer_care_emails (...)
VALUES (...)
ON CONFLICT (email_key) DO UPDATE SET ...;
```
If you trigger the DAG again, existing rows are updated and no duplicates appear. The count stays at 2,259.

## Malformed CSV test

This shows that a bad file stops at validation and never reaches the database.

1. Back up the real CSV outside the project folder.

   PowerShell:
   ```powershell
   Copy-Item customer_care_emails.csv $env:TEMP\customer_care_emails.backup.csv
   ```
2. Open `customer_care_emails.csv` in a text editor. On the first line only, change `subject` to `subject_x`. Save the file.
3. Trigger the DAG (UI or command line, as above).
4. In the Grid view, `validate_schema` turns red. `transform` and `load_to_postgres` show as `upstream_failed` and do not run. The error message is:
   ```text
   Schema mismatch: missing=['subject'], extra=['subject_x']
   ```
   To read it from the log files:
   ```bash
   docker exec airflow-scheduler grep -rh "AirflowFailException: Schema mismatch" /opt/airflow/logs/dag_id=load_customer_care_emails_to_postgres
   ```
5. Run the row count again. It is still 2,259.
6. Restore the real file.

   PowerShell:
   ```powershell
   Copy-Item $env:TEMP\customer_care_emails.backup.csv customer_care_emails.csv -Force
   ```
7. Trigger the DAG once more. All five tasks should be green.

## Results I checked

I ran these on 14 October 2026 with Docker Desktop (WSL2) and Airflow 2.10.5.

- After truncating the table, the first run loaded 2,259 rows.
- Running the DAG several more times left the count at 2,259.
- With `subject` renamed to `subject_x`, `validate_schema` failed with `Schema mismatch: missing=['subject'], extra=['subject_x']`. The later tasks did not run and the table stayed at 2,259 rows.
- After restoring the CSV, the DAG ran green again.

## Data issues I found

1. **Repeating `thread_id`.** Every email in a thread shares the same `thread_id`, so it cannot be the primary key. I built `email_key` as a SHA-256 hash of `thread_id`, the UTC timestamp, the sender and an MD5 of the message body. All 2,259 rows get a distinct key.
2. **Mixed timestamp formats.** The timestamps come with `+00:00`, with a `Z`, with milliseconds and with no timezone. I parse them all as UTC.
3. **Lists stored as strings.** `email_types` and `product_types` look like `"['inquiry']"`. I parse them with `ast.literal_eval` and store them as `TEXT[]`.
4. **Inconsistent product casing.** For example `Cloud Management` and `Cloud management`. I map them to one title-case form.
5. **Encoding damage.** Some message bodies contain sequences like `â€™` where an apostrophe should be. I replace them with the right characters.

## Design notes

- The five tasks pass the rows to each other through Airflow's XCom. That is fine for a 1.5 MB file. For a much larger file I would write an intermediate file and pass its path instead.
- No credentials are in the code. Passwords live in `.env`, which is not committed. The DAG reads the database connection from the `AIRFLOW_CONN_TARGET_POSTGRES` environment variable.
- The DAG has `catchup=False` and no schedule. It runs only when triggered.

## Stopping and resetting

Stop the stack and keep the data:
```bash
docker compose down
```
Stop it and delete the databases (this also makes `ddl.sql` run again on the next start):
```bash
docker compose down -v
docker compose up -d
```

## Troubleshooting

- **Port 8080 or 5433 is already in use.** Change the port mapping in `docker-compose.yaml`, or set `TARGET_POSTGRES_PORT` in `.env`. Port 5433 is used for the target database so it does not clash with a local PostgreSQL on 5432.
- **Docker is not running.** Start Docker Desktop and wait for "Engine running". On Windows, make sure WSL2 is installed.
- **`docker compose up` complains about missing variables.** `.env` is missing or incomplete. Copy it from `.env.example` again.
- **I changed `ddl.sql` and nothing happened.** PostgreSQL only runs the script when its data folder is empty. Use `docker compose down -v` and start again.
- **The Logs tab in the Airflow UI shows `403 FORBIDDEN`.** The webserver and the scheduler do not share a secret key, so the webserver cannot fetch the logs. Read the file from the scheduler instead:
  ```bash
  docker exec airflow-scheduler cat "/opt/airflow/logs/dag_id=<dag>/run_id=<run_id>/task_id=<task>/attempt=1.log"
  ```
- **The DAG does not appear in the UI.** Wait a minute for the scheduler to scan the `dags` folder, then refresh. If it still does not show, run `docker exec airflow-scheduler airflow dags list-import-errors`.
- **Airflow login fails.** Use the username and password from your `.env`, not the placeholders in `.env.example`.