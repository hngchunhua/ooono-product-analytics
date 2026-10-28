import os
from pathlib import Path

import requests
from dotenv import load_dotenv

import json
import duckdb

from datetime import datetime, timedelta, timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry



# 1. Load API credentials
load_dotenv(Path(__file__).parent / ".env")

BASE_URL = os.getenv("OOONO_BASE_URL", "").rstrip("/")
CLIENT_ID = os.getenv("OOONO_CLIENT_ID")
CLIENT_SECRET = os.getenv("OOONO_CLIENT_SECRET")

if not all([BASE_URL, CLIENT_ID, CLIENT_SECRET]):
    raise ValueError("Missing API credentials in .env")


# 2. Authenticate and obtain access token
auth_response = requests.post(
    f"{BASE_URL}/oauth/token",
    data={
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    },
    timeout=60,
)

auth_response.raise_for_status()

access_token = auth_response.json()["access_token"]

print("Authentication successful!")



# 3. API pagination and incremental parameters

PAGE_SIZE = 1000
INITIAL_DAYS = 35

session = requests.Session()
session.headers.update({
    "Authorization": f"Bearer {access_token}"
})

# Retry temporary server errors and rate limits
retries = Retry(
    total=2,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)
session.mount("https://", HTTPAdapter(max_retries=retries))


def parse_utc(value):
    return datetime.fromisoformat(
        str(value).replace("Z", "+00:00")
    ).astimezone(timezone.utc)


def format_utc(value):
    return value.astimezone(timezone.utc).isoformat(
        timespec="microseconds"
    ).replace("+00:00", "Z")


def fetch_pages(endpoint, received_since=None):
    cursor = None
    seen_cursors = set()

    while True:
        params = {"limit": PAGE_SIZE}

        if received_since is not None:
            params["received_since"] = received_since

        if cursor is not None:
            if cursor in seen_cursors:
                raise RuntimeError("Repeated pagination cursor")
            seen_cursors.add(cursor)
            params["cursor"] = cursor

        response = session.get(
            f"{BASE_URL}/v1/{endpoint}",
            params=params,
            timeout=60,
        )
        response.raise_for_status()

        result = response.json()
        records = result["data"]

        yield records

        cursor = result.get("next_cursor")

        if not cursor:
            break


# 4. Bronze tables and checkpoint storage

DB_PATH = Path(__file__).resolve().parent / "data" / "ooono.duckdb"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

with duckdb.connect(str(DB_PATH)) as conn:

    conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze.drives (
            received_at VARCHAR,
            raw_payload JSON NOT NULL,
            ingested_at TIMESTAMPTZ DEFAULT now()
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze.warnings (
            received_at VARCHAR,
            raw_payload JSON NOT NULL,
            ingested_at TIMESTAMPTZ DEFAULT now()
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze.users (
            raw_payload JSON NOT NULL,
            ingested_at TIMESTAMPTZ DEFAULT now()
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze.ingestion_checkpoints (
            endpoint VARCHAR PRIMARY KEY,
            last_received_at VARCHAR NOT NULL,
            updated_at TIMESTAMPTZ DEFAULT now()
        )
    """)

    sources = ["drives", "warnings", "users"]

    for endpoint in sources:

        previous_checkpoint = None
        received_since = None

        if endpoint != "users":
            row = conn.execute("""
                SELECT last_received_at
                FROM bronze.ingestion_checkpoints
                WHERE endpoint = ?
            """, [endpoint]).fetchone()

            if row:
                previous_checkpoint = parse_utc(row[0])
                start = previous_checkpoint
            else:
                start = datetime.now(timezone.utc) - timedelta(
                    days=INITIAL_DAYS
                )

            received_since = format_utc(start)

        print(f"\nIngesting: {endpoint}")

        before = conn.execute(
            f"SELECT COUNT(*) FROM bronze.{endpoint}"
        ).fetchone()[0]

        max_received = previous_checkpoint

        # All pages and the checkpoint are committed together
        conn.execute("BEGIN TRANSACTION")

        try:
            if endpoint == "users":
                conn.execute("DELETE FROM bronze.users")

            for records in fetch_pages(endpoint, received_since):

                if endpoint == "users":
                    rows = [
                        (json.dumps(r),)
                        for r in records
                    ]

                    if rows:
                        conn.executemany("""
                            INSERT INTO bronze.users (raw_payload)
                            VALUES (?)
                        """, rows)

                else:
                    rows = []

                    for r in records:
                        received_at = r["received_at"]
                        received_dt = parse_utc(received_at)

                        if (max_received is None or
                                received_dt > max_received):
                            max_received = received_dt

                        rows.append((
                            received_at,
                            json.dumps(r),
                        ))

                    if rows:
                        conn.executemany(f"""
                            INSERT INTO bronze.{endpoint}
                                (received_at, raw_payload)
                            VALUES (?, ?)
                        """, rows)

            # Update only after every page was processed
            if endpoint != "users" and max_received is not None:
                conn.execute("""
                    INSERT INTO bronze.ingestion_checkpoints
                        (endpoint, last_received_at)
                    VALUES (?, ?)
                    ON CONFLICT (endpoint) DO UPDATE SET
                        last_received_at = EXCLUDED.last_received_at,
                        updated_at = now()
                """, [endpoint, format_utc(max_received)])

            conn.execute("COMMIT")

        except Exception:
            conn.execute("ROLLBACK")
            raise

        after = conn.execute(
            f"SELECT COUNT(*) FROM bronze.{endpoint}"
        ).fetchone()[0]

        print(f"Ingesting: {endpoint} Complete")
        
        if endpoint == "users":
            print(f"Records loaded: {after}")
        else:
            print(f"New records inserted: {after - before}")
        
        print(f"Total records stored: {after}")

session.close()