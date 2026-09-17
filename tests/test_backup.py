"""Tests for scripts/backup_db.py — the value conversions a restore depends on.

The round-trip itself (dump → restore into a fresh database → compare every
table) was verified against a real PostgreSQL 16 server; what's pinned here is
the per-value encoding, because a single mis-escaped string silently corrupts
restore.sql, and nothing would notice until a restore is actually needed.
"""
import datetime
import decimal
import importlib.util
import json
import pathlib

SPEC = importlib.util.spec_from_file_location(
    "backup_db", pathlib.Path(__file__).resolve().parent.parent / "scripts" / "backup_db.py")
backup_db = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(backup_db)


def test_sql_literal_escapes_quotes():
    """A tracking number or store name with an apostrophe must not end the string."""
    assert backup_db._sql_literal("1Z333'quote") == "'1Z333''quote'"
    assert backup_db._sql_literal("O'Brien's") == "'O''Brien''s'"


def test_sql_literal_types():
    assert backup_db._sql_literal(None) == "NULL"
    assert backup_db._sql_literal(True) == "TRUE"
    assert backup_db._sql_literal(False) == "FALSE"
    assert backup_db._sql_literal(42) == "42"
    assert backup_db._sql_literal(decimal.Decimal("50.00")) == "50.00"
    assert backup_db._sql_literal(datetime.datetime(2026, 9, 1, 12)) == "'2026-09-01T12:00:00'"
    assert backup_db._sql_literal(b"\x01\x02") == "'\\x0102'"


def test_sql_literal_keeps_unicode_intact():
    assert backup_db._sql_literal("Cara — ünïcode ✨") == "'Cara — ünïcode ✨'"


def test_sql_literal_serialises_json_columns():
    assert backup_db._sql_literal({"HOODIE": 1}) == '\'{"HOODIE": 1}\''


def test_booleans_are_not_written_as_numbers():
    """TRUE must not degrade to 1 — `confirmed` is a boolean column."""
    assert backup_db._sql_literal(True) not in ("1", "'1'")


def test_jsonable_round_trips_through_json():
    row = {
        "closed_at": datetime.datetime(2026, 9, 1, 12),
        "subtotal": decimal.Decimal("100.00"),
        "confirmed": True,
        "tracking": None,
        "blob": b"\xff\x00",
    }
    encoded = {k: backup_db._jsonable(v) for k, v in row.items()}
    decoded = json.loads(json.dumps(encoded))          # must survive a JSON file
    assert decoded["closed_at"] == "2026-09-01T12:00:00"
    assert decoded["subtotal"] == "100.00"             # exact, not a lossy float
    assert decoded["confirmed"] is True
    assert decoded["tracking"] is None
    assert decoded["blob"] == "ff00"


def test_known_table_order_covers_what_the_apps_create():
    """Guards against a new table being added without a place in the order."""
    for table in ("server_settings", "drop_history", "user_claims", "raffles",
                  "raffle_slots", "live_orders", "pending_actions",
                  "pending_notifications", "bot_guilds", "payment_boards"):
        assert table in backup_db.KNOWN_ORDER
