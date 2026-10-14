import ast
import hashlib
import logging
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import psycopg2.extras
import yaml
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

DATA_DIR = Path(os.getenv("PROJECT_DIR", "/opt/airflow/project"))
MANIFEST_PATH = DATA_DIR / "manifest.yaml"
SCHEMA_PATH = DATA_DIR / "schema.yaml"

PRODUCT_MAP = {
    "api development": "API Development", "api monitoring": "API Monitoring",
    "cloud management": "Cloud Management", "iam service": "IAM Service",
    "mercury language": "Mercury Language",
}

def clean_text(text: str) -> str:
    return text.replace("â€™", "’").replace("â€œ", "“").replace("â€ ", "”").replace("â€”", "—") if isinstance(text, str) else text

@dag(
    dag_id="load_customer_care_emails_to_postgres",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
)
def email_pipeline():
    @task
    def check_manifest_status() -> None:
        with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
            manifest = yaml.safe_load(f)
        status = manifest.get("status")
        if status != "done":
            raise AirflowFailException(f"Manifest status is '{status}', expected 'done'")
        log.info("Manifest status verified: %s", status)

    @task
    def read_csv(dag_run=None) -> list[dict]:
        filename = "customer_care_emails.csv"
        if dag_run and getattr(dag_run, "conf", None):
            filename = dag_run.conf.get("csv_filename", filename)
        csv_path = DATA_DIR / os.getenv("CSV_FILENAME", filename)
        df = pd.read_csv(csv_path, encoding="utf-8")
        log.info("Read %d rows from %s", len(df), csv_path.name)
        return df.to_dict(orient="records")

    @task
    def validate_schema(rows: list[dict]) -> list[dict]:
        with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
            schema = yaml.safe_load(f)

        expected_cols = [c for c in schema["columns"] if not c.get("derived")]
        expected_names = [c["name"] for c in expected_cols]
        if not rows:
            raise AirflowFailException("Dataset is empty, validation failed")

        actual_cols = list(rows[0].keys())
        missing = set(expected_names) - set(actual_cols)
        extra = set(actual_cols) - set(expected_names)
        if missing or extra:
            raise AirflowFailException(f"Schema mismatch: missing={sorted(missing)}, extra={sorted(extra)}")

        for col in expected_cols:
            name, is_null = col["name"], col.get("nullable", True)
            allowed, cmin, cmax = col.get("allowed"), col.get("min"), col.get("max")
            for i, r in enumerate(rows):
                val = r[name]
                if not is_null and (val is None or pd.isna(val)):
                    raise AirflowFailException(f"Null in non-nullable column '{name}' at row {i}")
                if allowed:
                    items = ast.literal_eval(val) if col.get("type") == "list" and isinstance(val, str) else [val]
                    if any(item not in allowed for item in items):
                        raise AirflowFailException(f"Invalid value in '{name}' at row {i}: {val}")
                if cmin is not None or cmax is not None:
                    try:
                        num = float(val)
                    except (ValueError, TypeError):
                        raise AirflowFailException(f"Wrong type: cannot cast '{name}'='{val}' to float at row {i}")
                    if (cmin is not None and num < cmin) or (cmax is not None and num > cmax):
                        raise AirflowFailException(f"Out of range in '{name}' at row {i}: {num}")

        log.info("Validated %d rows successfully against schema", len(rows))
        return rows

    @task
    def transform(rows: list[dict]) -> list[dict]:
        transformed = []
        for r in rows:
            ts = pd.to_datetime(r["timestamp"], utc=True)
            body = clean_text(r["message_body"])
            body_md5 = hashlib.md5(body.encode("utf-8")).hexdigest()
            raw_key = f"{r['thread_id']}|{ts.isoformat()}|{r['sender']}|{body_md5}"
            email_key = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()

            email_types = ast.literal_eval(r["email_types"]) if isinstance(r["email_types"], str) else r["email_types"]
            raw_prods = ast.literal_eval(r["product_types"]) if isinstance(r["product_types"], str) else r["product_types"]
            products = [PRODUCT_MAP.get(p.strip().lower(), p.strip().title()) for p in raw_prods]

            transformed.append({
                "email_key": email_key, "subject": clean_text(r["subject"]),
                "sender": r["sender"], "receiver": r["receiver"],
                "timestamp": ts.isoformat(), "message_body": body,
                "thread_id": r["thread_id"], "email_types": email_types,
                "email_status": r["email_status"], "email_criticality": r["email_criticality"],
                "product_types": products, "agent_effectivity": r["agent_effectivity"],
                "agent_efficiency": r["agent_efficiency"],
                "customer_satisfaction": float(r["customer_satisfaction"]),
            })
        log.info("Transformed %d rows", len(transformed))
        return transformed

    @task
    def load_to_postgres(rows: list[dict]) -> None:
        if not rows:
            log.info("No rows to load")
            return
        cols = list(rows[0].keys())
        tuples = [tuple(r[c] for c in cols) for r in rows]
        updates = [f"{c} = EXCLUDED.{c}" for c in cols if c != "email_key"]
        insert_sql = f"""
            INSERT INTO customer_care_emails ({', '.join(cols)})
            VALUES ({', '.join(['%s'] * len(cols))})
            ON CONFLICT (email_key) DO UPDATE SET {', '.join(updates)}
        """
        hook = PostgresHook(postgres_conn_id="target_postgres")
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, insert_sql, tuples, page_size=500)
            conn.commit()
        log.info("Loaded %d rows into target postgres", len(rows))

    manifest_ok = check_manifest_status()
    raw_data = read_csv()
    manifest_ok >> raw_data
    validated_data = validate_schema(raw_data)
    transformed_data = transform(validated_data)
    load_to_postgres(transformed_data)

pipeline = email_pipeline()
