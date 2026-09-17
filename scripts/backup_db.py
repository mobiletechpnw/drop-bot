#!/usr/bin/env python3
"""Back up the Drop Bot database — every table, to files you can restore from.

The bot and the dashboard share one PostgreSQL database, and it holds the only
copy of drop history, per-buyer claims, payments, tracking numbers, raffles and
server settings. Nothing in the repo recreates that, so take a backup before
anything risky (a deploy, a migration, a merge).

    DATABASE_URL=postgres://user:pass@host:5432/db python scripts/backup_db.py

On Railway: copy DATABASE_URL from the Postgres service's Variables tab, or run
it through the CLI with `railway run python scripts/backup_db.py`.

Writes a timestamped folder:

    backups/dropbot-20260917-014233/
        manifest.json   what was captured, row counts, server version
        <table>.json    every row, full fidelity (timestamps as ISO strings)
        <table>.csv     the same rows for Excel / eyeballing
        restore.sql     INSERTs to load it all back

Restoring, in two steps — restore.sql carries rows, not tables:

    1. Make sure the tables exist. Starting the bot (or the dashboard) against
       the database creates them; a brand-new empty database needs that first.
    2. psql "$DATABASE_URL" -f backups/<folder>/restore.sql

restore.sql is additive — every INSERT carries ON CONFLICT DO NOTHING, so it
never overwrites rows that are already there. To roll a table all the way back
to the snapshot, delete its rows first, in the same transaction:

    psql "$DATABASE_URL" -c 'BEGIN; DELETE FROM user_claims; COMMIT;'

⚠️  The backup contains secrets — `server_settings.web_access_key` is a login
credential for the dashboard, and payment handles are personal data. Keep the
folder private; it is git-ignored by default.

Options:
    --out DIR          where to write (default: ./backups)
    --tables a,b,c     only these tables (default: every table in the database)
    --quiet            only print the final folder path
"""
import argparse
import asyncio
import csv
import datetime
import decimal
import json
import os
import pathlib
import sys

import asyncpg

# Tables the bot and dashboard own. Anything else found in the database is
# backed up too — this list only fixes a sensible, dependency-friendly order.
KNOWN_ORDER = [
    "server_admins", "server_managers", "server_settings", "bot_guilds",
    "drop_history", "user_claims", "payment_boards",
    "raffle_hosts", "raffles", "raffle_slots",
    "live_drops", "live_orders", "pending_actions", "pending_notifications",
]


def _jsonable(value):
    """Postgres types → JSON, without losing anything we can't get back."""
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, (bytes, memoryview)):
        return bytes(value).hex()
    return value


def _sql_literal(value):
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float, decimal.Decimal)):
        return str(value)
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return "'" + value.isoformat() + "'"
    if isinstance(value, (bytes, memoryview)):
        return "'\\x" + bytes(value).hex() + "'"
    if isinstance(value, (dict, list)):
        value = json.dumps(value)
    return "'" + str(value).replace("'", "''") + "'"


async def list_tables(conn):
    rows = await conn.fetch("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """)
    found = [r["table_name"] for r in rows]
    ordered = [t for t in KNOWN_ORDER if t in found]
    return ordered + [t for t in found if t not in ordered]


async def serial_sequences(conn, table):
    """(column, sequence) pairs so restore.sql can fast-forward SERIAL ids."""
    rows = await conn.fetch("""
        SELECT column_name, pg_get_serial_sequence($1, column_name) AS seq
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = $1
    """, table)
    return [(r["column_name"], r["seq"]) for r in rows if r["seq"]]


async def backup(database_url, out_dir, only_tables=None, quiet=False):
    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    folder = pathlib.Path(out_dir) / f"dropbot-{stamp}"
    folder.mkdir(parents=True, exist_ok=True)

    conn = await asyncpg.connect(database_url)
    try:
        version = await conn.fetchval("SELECT version()")
        tables = await list_tables(conn)
        if only_tables:
            wanted = [t.strip() for t in only_tables.split(",") if t.strip()]
            missing = [t for t in wanted if t not in tables]
            if missing:
                raise SystemExit(f"no such table(s): {', '.join(missing)}")
            tables = [t for t in tables if t in wanted]

        manifest = {
            "taken_at": datetime.datetime.now().astimezone().isoformat(),
            "server": version,
            "database": await conn.fetchval("SELECT current_database()"),
            "tables": {},
        }
        sql_parts = [
            "-- Drop Bot restore script\n"
            f"-- Snapshot taken {manifest['taken_at']}\n"
            "-- Additive: every INSERT is ON CONFLICT DO NOTHING, so existing\n"
            "-- rows are left alone. Delete a table's rows first if you want a\n"
            "-- true roll-back to this snapshot.\n"
            "BEGIN;\n"
        ]

        for table in tables:
            rows = await conn.fetch(f'SELECT * FROM "{table}"')
            columns = list(rows[0].keys()) if rows else [
                r["column_name"] for r in await conn.fetch("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema='public' AND table_name=$1
                    ORDER BY ordinal_position""", table)
            ]
            data = [{c: _jsonable(r[c]) for c in columns} for r in rows]

            (folder / f"{table}.json").write_text(
                json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

            with (folder / f"{table}.csv").open("w", newline="", encoding="utf-8-sig") as fh:
                writer = csv.writer(fh)
                writer.writerow(columns)
                for row in data:
                    writer.writerow([row[c] for c in columns])

            if rows:
                cols_sql = ", ".join(f'"{c}"' for c in columns)
                sql_parts.append(f"\n-- {table}: {len(rows)} row(s)\n")
                for r in rows:
                    values = ", ".join(_sql_literal(r[c]) for c in columns)
                    sql_parts.append(
                        f'INSERT INTO "{table}" ({cols_sql}) VALUES ({values}) '
                        f"ON CONFLICT DO NOTHING;\n")
            for column, seq in await serial_sequences(conn, table):
                sql_parts.append(
                    f"SELECT setval('{seq}', "
                    f'COALESCE((SELECT MAX("{column}") FROM "{table}"), 1));\n')

            manifest["tables"][table] = {"rows": len(rows), "columns": columns}
            if not quiet:
                print(f"  {table:24} {len(rows):6} row(s)")

        sql_parts.append("\nCOMMIT;\n")
        (folder / "restore.sql").write_text("".join(sql_parts), encoding="utf-8")
        (folder / "manifest.json").write_text(
            json.dumps(manifest, indent=2), encoding="utf-8")
    finally:
        await conn.close()

    total = sum(t["rows"] for t in manifest["tables"].values())
    if quiet:
        print(folder)
    else:
        size = sum(f.stat().st_size for f in folder.iterdir()) / 1024
        print(f"\n✅  {total} row(s) across {len(manifest['tables'])} table(s) "
              f"→ {folder}  ({size:.0f} KB)")
        print(f"    Restore with:  psql \"$DATABASE_URL\" -f {folder}/restore.sql")
        print("    ⚠️  Contains dashboard access keys — keep this folder private.")
    return folder


def main():
    parser = argparse.ArgumentParser(description="Back up the Drop Bot database.")
    parser.add_argument("--out", default="backups", help="output directory")
    parser.add_argument("--tables", default="", help="comma-separated subset")
    parser.add_argument("--quiet", action="store_true", help="print only the folder")
    args = parser.parse_args()

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        sys.exit("DATABASE_URL is not set — copy it from your Postgres service "
                 "and re-run:  DATABASE_URL=postgres://... python scripts/backup_db.py")
    asyncio.run(backup(database_url, args.out, args.tables or None, args.quiet))


if __name__ == "__main__":
    main()
