"""
setup_database.py
=================
ONE-TIME SETUP SCRIPT — Run this once on the client laptop after
installing PostgreSQL.

What it does:
  1. Reads ALL config from your .env file (nothing hardcoded)
  2. Connects to PostgreSQL using those credentials
  3. Creates the database (DB_NAME_V2 from .env)
  4. Creates all 6 tables with the correct schema + indexes

.env keys required (create .env in the same folder as this script):
    DB_HOST        e.g.  localhost
    DB_PORT        e.g.  5432
    DB_USER        e.g.  postgres
    DB_PASSWORD    your postgres password
    DB_NAME_V2     e.g.  si2tech_travel_normalized

Usage:
  1. Make sure your .env file is filled in
  2. Run:  python setup_database.py

REQUIREMENTS:
  pip install psycopg2-binary python-dotenv
"""

import os
import sys
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv

# ── Load .env from the same directory as this script ─────────────────────────
_ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(dotenv_path=_ENV_PATH)

# ── All config from .env — nothing hardcoded ─────────────────────────────────
DB_HOST     = os.getenv("DB_HOST",     "localhost")
DB_PORT     = os.getenv("DB_PORT",     "5432")
DB_USER     = os.getenv("DB_USER",     "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME     = os.getenv("DB_NAME_V2",  "si2tech_travel_normalized")

# ─────────────────────────────────────────────────────────────────────────────


SQL_CREATE_TABLES = """
-- ── employees ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS employees (
    employee_id     SERIAL PRIMARY KEY,
    employee_name   VARCHAR(255) NOT NULL UNIQUE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ── bookings ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bookings (
    booking_id          SERIAL PRIMARY KEY,
    booking_timestamp   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    project_no          VARCHAR(100),
    reason              TEXT,
    booking_vendor      VARCHAR(100),
    total_amount        NUMERIC(12, 2),
    booking_date        DATE
);

-- ── booking_passengers ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS booking_passengers (
    booking_id      INTEGER NOT NULL REFERENCES bookings(booking_id)   ON DELETE CASCADE,
    employee_id     INTEGER NOT NULL REFERENCES employees(employee_id) ON DELETE CASCADE,
    PRIMARY KEY (booking_id, employee_id)
);

-- ── flight_segments ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS flight_segments (
    segment_id      SERIAL PRIMARY KEY,
    booking_id      INTEGER NOT NULL REFERENCES bookings(booking_id) ON DELETE CASCADE,
    segment_order   INTEGER NOT NULL DEFAULT 1,
    airline         VARCHAR(100),
    flight_number   VARCHAR(50),
    origin          VARCHAR(100),
    destination     VARCHAR(100),
    travel_date     DATE,
    departure_time  TIME,
    arrival_time    TIME,
    amount          NUMERIC(12, 2),
    pnr             VARCHAR(50)
);

-- ── hotel_stays ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS hotel_stays (
    stay_id         SERIAL PRIMARY KEY,
    booking_id      INTEGER NOT NULL REFERENCES bookings(booking_id) ON DELETE CASCADE,
    hotel_name      VARCHAR(255),
    check_in_date   DATE,
    checkout_date   DATE,
    amount          NUMERIC(12, 2),
    booking_id_ext  VARCHAR(100)
);

-- ── meeting_details ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS meeting_details (
    meeting_id          SERIAL PRIMARY KEY,
    booking_id          INTEGER NOT NULL REFERENCES bookings(booking_id) ON DELETE CASCADE,
    meeting_date        DATE,
    meeting_time        TIME,
    meeting_location    TEXT
);

-- ── indexes ────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_bookings_project_no      ON bookings(project_no);
CREATE INDEX IF NOT EXISTS idx_bookings_booking_date    ON bookings(booking_date);
CREATE INDEX IF NOT EXISTS idx_flight_segments_booking  ON flight_segments(booking_id);
CREATE INDEX IF NOT EXISTS idx_hotel_stays_booking      ON hotel_stays(booking_id);
CREATE INDEX IF NOT EXISTS idx_meeting_details_booking  ON meeting_details(booking_id);
CREATE INDEX IF NOT EXISTS idx_bp_employee              ON booking_passengers(employee_id);
CREATE INDEX IF NOT EXISTS idx_employees_name           ON employees(employee_name);
"""


def _check_env():
    """Warn if any required .env keys are missing."""
    missing = []
    for key in ("DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME_V2"):
        if not os.getenv(key):
            missing.append(key)
    if missing:
        print(f"  ⚠️  These keys are missing or empty in your .env: {', '.join(missing)}")
        print(f"  .env path checked: {_ENV_PATH}")
        if "DB_PASSWORD" in missing:
            print("  ℹ️  DB_PASSWORD can be empty if your PostgreSQL has no password set.")


def main():
    print("=" * 60)
    print("  SI2TECH TRAVEL — Database Setup")
    print("=" * 60)
    print(f"\n  Reading config from: {_ENV_PATH}")
    print(f"  DB_HOST    : {DB_HOST}")
    print(f"  DB_PORT    : {DB_PORT}")
    print(f"  DB_USER    : {DB_USER}")
    print(f"  DB_NAME_V2 : {DB_NAME}")
    print(f"  DB_PASSWORD: {'(set)' if DB_PASSWORD else '(empty)'}")

    _check_env()

    # ── Step 1: Connect to default 'postgres' db to create the new db ───────
    print(f"\n[1/3] Connecting to PostgreSQL at {DB_HOST}:{DB_PORT}...")
    try:
        conn = psycopg2.connect(
            host     = DB_HOST,
            port     = int(DB_PORT),
            dbname   = "postgres",
            user     = DB_USER,
            password = DB_PASSWORD,
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        print("  ✅ Connected.")
    except Exception as e:
        print(f"  ❌ Connection failed: {e}")
        print("\n  Troubleshooting:")
        print("  • Make sure PostgreSQL is running")
        print("  • Check DB_HOST, DB_PORT, DB_USER, DB_PASSWORD in your .env")
        sys.exit(1)

    # ── Step 2: Create the database ──────────────────────────────────────────
    print(f"\n[2/3] Creating database '{DB_NAME}'...")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME,))
            if cur.fetchone():
                print(f"  ⚠️  Database '{DB_NAME}' already exists — skipping creation.")
            else:
                cur.execute(f'CREATE DATABASE "{DB_NAME}"')
                print(f"  ✅ Database '{DB_NAME}' created.")
    except Exception as e:
        print(f"  ❌ Failed to create database: {e}")
        sys.exit(1)
    finally:
        conn.close()

    # ── Step 3: Connect to new db and create tables ──────────────────────────
    print(f"\n[3/3] Creating tables and indexes...")
    try:
        conn2 = psycopg2.connect(
            host     = DB_HOST,
            port     = int(DB_PORT),
            dbname   = DB_NAME,
            user     = DB_USER,
            password = DB_PASSWORD,
        )
        with conn2:
            with conn2.cursor() as cur:
                cur.execute(SQL_CREATE_TABLES)
        print("  ✅ Tables created:")
        print("     • employees")
        print("     • bookings")
        print("     • booking_passengers")
        print("     • flight_segments")
        print("     • hotel_stays")
        print("     • meeting_details")
        print("  ✅ Indexes created.")
    except Exception as e:
        print(f"  ❌ Table creation failed: {e}")
        sys.exit(1)
    finally:
        conn2.close()

    print("\n" + "=" * 60)
    print("  ✅ Setup complete! Database is ready.")
    print("  You can now run the main application.")
    print("=" * 60)


if __name__ == "__main__":
    main()
