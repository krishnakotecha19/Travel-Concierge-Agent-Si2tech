"""
weekly_backup.py
================
WEEKLY BACKUP SCRIPT — Scheduled automatically every Sunday at 11 PM.

What it does every run:
  1. Reads ALL config from your .env file
  2. Takes a snapshot (pg_dump) of the live database → saves as .sql in db_backups/
  3. Deletes backup files older than BACKUP_KEEP_DAYS (default: 60 days = ~8 weeks)
  4. Logs everything to db_backups/backup_log.txt

That's it. The live database is never touched.

.env keys used (same file as your main app):
    DB_HOST            e.g.  localhost
    DB_PORT            e.g.  5432
    DB_USER            e.g.  postgres
    DB_PASSWORD        your postgres password
    DB_NAME_V2         e.g.  si2tech_travel_normalized
    BACKUP_DIR         (optional) custom folder path for backups
    BACKUP_KEEP_DAYS   (optional) days to keep old backups, default 60

TO RESTORE A BACKUP MANUALLY (if ever needed):
  python weekly_backup.py restore db_backups/backup_..._20260310.sql

REQUIREMENTS:
  pip install psycopg2-binary python-dotenv
  PostgreSQL must be installed (pg_dump auto-detected)
"""

import os
import sys
import subprocess
import datetime
import glob
import logging
from dotenv import load_dotenv

# ── Load .env from the same directory as this script ─────────────────────────
_ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(dotenv_path=_ENV_PATH)

# ── All config from .env ──────────────────────────────────────────────────────
DB_HOST     = os.getenv("DB_HOST",     "localhost")
DB_PORT     = os.getenv("DB_PORT",     "5432")
DB_USER     = os.getenv("DB_USER",     "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME     = os.getenv("DB_NAME_V2",  "si2tech_travel_normalized")

BACKUP_DIR  = os.getenv(
    "BACKUP_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "db_backups"),
)
KEEP_DAYS   = int(os.getenv("BACKUP_KEEP_DAYS", "60"))

# ─────────────────────────────────────────────────────────────────────────────


def _find_pg_dump() -> str:
    """Auto-locate pg_dump — tries PostgreSQL versions 17→13 on Windows, then PATH."""
    for ver in ["17", "16", "15", "14", "13"]:
        path = rf"C:\Program Files\PostgreSQL\{ver}\bin\pg_dump.exe"
        if os.path.isfile(path):
            return path
    return "pg_dump"  # Linux / Mac / Windows with PATH set


def _setup_logging() -> logging.Logger:
    os.makedirs(BACKUP_DIR, exist_ok=True)
    log_file = os.path.join(BACKUP_DIR, "backup_log.txt")
    logging.basicConfig(
        level    = logging.INFO,
        format   = "%(asctime)s  %(levelname)s  %(message)s",
        datefmt  = "%Y-%m-%d %H:%M:%S",
        handlers = [
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    return logging.getLogger("weekly_backup")


# ─────────────────────────────────────────────────────────────────────────────
#  STEP 1 — Snapshot the database
# ─────────────────────────────────────────────────────────────────────────────

def take_backup(logger) -> str | None:
    """Dumps the live database to a timestamped .sql file. Live DB is never touched."""
    pg_dump   = _find_pg_dump()
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename  = f"backup_{DB_NAME}_{timestamp}.sql"
    filepath  = os.path.join(BACKUP_DIR, filename)

    os.makedirs(BACKUP_DIR, exist_ok=True)
    logger.info(f"Taking snapshot → {filepath}")

    env               = os.environ.copy()
    env["PGPASSWORD"] = DB_PASSWORD

    cmd = [
        pg_dump,
        "--host",        DB_HOST,
        "--port",        DB_PORT,
        "--username",    DB_USER,
        "--dbname",      DB_NAME,
        "--format",      "plain",   # plain .sql — human-readable, easy to restore
        "--no-password",
        "--file",        filepath,
    ]

    try:
        result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=300)

        if result.returncode != 0:
            logger.error(f"pg_dump failed (exit {result.returncode}): {result.stderr.strip()}")
            if os.path.exists(filepath) and os.path.getsize(filepath) == 0:
                os.remove(filepath)
            return None

        size_kb = os.path.getsize(filepath) / 1024
        logger.info(f"✅ Snapshot saved: {filename}  ({size_kb:.1f} KB)")
        return filepath

    except FileNotFoundError:
        logger.error(
            f"pg_dump not found (tried: '{pg_dump}').\n"
            "  Ensure PostgreSQL is installed and its bin folder is in your PATH."
        )
        return None
    except subprocess.TimeoutExpired:
        logger.error("pg_dump timed out (5 min limit).")
        return None
    except Exception as exc:
        logger.error(f"Unexpected error during backup: {exc}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
#  STEP 2 — Delete old backup files
# ─────────────────────────────────────────────────────────────────────────────

def delete_old_backups(logger):
    """Removes .sql backup files older than KEEP_DAYS from BACKUP_DIR."""
    cutoff    = datetime.datetime.now() - datetime.timedelta(days=KEEP_DAYS)
    pattern   = os.path.join(BACKUP_DIR, f"backup_{DB_NAME}_*.sql")
    all_files = sorted(glob.glob(pattern))

    deleted = kept = 0
    for fp in all_files:
        mtime = datetime.datetime.fromtimestamp(os.path.getmtime(fp))
        if mtime < cutoff:
            try:
                os.remove(fp)
                age = (datetime.datetime.now() - mtime).days
                logger.info(f"🗑️  Deleted old snapshot: {os.path.basename(fp)}  ({age} days old)")
                deleted += 1
            except Exception as exc:
                logger.warning(f"Could not delete {fp}: {exc}")
        else:
            kept += 1

    logger.info(f"Cleanup done — kept {kept} snapshot(s), deleted {deleted}  (policy: >{KEEP_DAYS} days).")


def _list_backups(logger):
    pattern   = os.path.join(BACKUP_DIR, f"backup_{DB_NAME}_*.sql")
    all_files = sorted(glob.glob(pattern))
    if not all_files:
        logger.info("No snapshots on disk yet.")
        return
    logger.info(f"Snapshots on disk ({len(all_files)}):")
    for fp in all_files:
        size_kb = os.path.getsize(fp) / 1024
        mtime   = datetime.datetime.fromtimestamp(os.path.getmtime(fp)).strftime("%Y-%m-%d %H:%M")
        logger.info(f"  {os.path.basename(fp):62s} {size_kb:8.1f} KB   {mtime}")


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    logger = _setup_logging()
    logger.info("=" * 62)
    logger.info("  SI2TECH TRAVEL — Weekly Backup")
    logger.info(f"  DB   : {DB_NAME}  |  {DB_HOST}:{DB_PORT}  |  user: {DB_USER}")
    logger.info(f"  Dir  : {BACKUP_DIR}  |  Keep: {KEEP_DAYS} days")
    logger.info("=" * 62)

    # ── 1. Snapshot ──────────────────────────────────────────────────────────
    logger.info("[1/2] Snapshotting live database...")
    backup_path = take_backup(logger)

    if not backup_path:
        logger.error("Snapshot failed — live database is untouched. Check errors above.")
        sys.exit(1)

    # ── 2. Clean up old snapshots ────────────────────────────────────────────
    logger.info(f"[2/2] Removing snapshots older than {KEEP_DAYS} days...")
    delete_old_backups(logger)
    _list_backups(logger)

    logger.info("=" * 62)
    logger.info("  ✅ Weekly backup completed successfully.")
    logger.info("=" * 62)
    sys.exit(0)


# ─────────────────────────────────────────────────────────────────────────────
#  MANUAL RESTORE — python weekly_backup.py restore db_backups/backup_....sql
# ─────────────────────────────────────────────────────────────────────────────

def manual_restore(backup_file: str):
    logger = _setup_logging()

    if not os.path.isfile(backup_file):
        logger.error(f"File not found: {backup_file}")
        sys.exit(1)

    psql = _find_pg_dump().replace("pg_dump", "psql")
    env  = os.environ.copy()
    env["PGPASSWORD"] = DB_PASSWORD

    logger.info(f"Restoring from: {backup_file}")

    cmd = [
        psql,
        "--host",        DB_HOST,
        "--port",        DB_PORT,
        "--username",    DB_USER,
        "--dbname",      DB_NAME,
        "--no-password",
        "--quiet",
        "--file",        backup_file,
    ]

    result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=300)
    if result.returncode == 0:
        logger.info("✅ Restore complete.")
    else:
        logger.error(f"❌ Restore failed: {result.stderr.strip()}")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "restore":
        if len(sys.argv) < 3:
            print("Usage: python weekly_backup.py restore <path/to/backup.sql>")
            sys.exit(1)
        manual_restore(sys.argv[2])
    else:
        main()