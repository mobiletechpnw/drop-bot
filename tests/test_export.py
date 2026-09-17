"""Tests for the export paths: Excel/CSV/printable claim list.

Each of these covers a way the export silently produced the wrong file rather
than an error:

  * `!export` after the next drop session started paired the new drop's stock
    with the previous drop's claims, so every row was skipped and the workbook
    came out empty,
  * ...and read payments from the live dict that `!drop` had just reset, so
    buyers who had paid exported as unpaid,
  * the dashboard's download 500'd for any store whose Discord name has an
    emoji, because HTTP headers are latin-1 only.
"""
import asyncio
import csv
import io
import os
from contextlib import asynccontextmanager

import openpyxl
import pytest

import drop_bot

os.environ.setdefault("DATABASE_URL", "postgres://unused/for-import")
import webapp  # noqa: E402  (needs the env var above)


class FakeUser:
    def __init__(self, uid, name):
        self.id, self.display_name = uid, name


ALICE, BOB = FakeUser(111, "alice"), FakeUser(222, "bob")
STOCK = {"hoodie": {"display": "HOODIE", "price": 50.0, "qty": 0, "limit": None}}
CLAIMS = {"hoodie": [{"user": ALICE, "qty": 2, "time": None},
                     {"user": BOB, "qty": 1, "time": None}]}
PAID = {111: [{"method": "venmo", "amount": 100.0, "confirmed": True, "time": None}]}


def _closed_drop_in_memory(gid):
    """State right after `!enddrop`: the drop is still in memory."""
    drop_bot.stock[gid] = dict(STOCK)
    drop_bot.claims[gid] = {k: list(v) for k, v in CLAIMS.items()}
    drop_bot.payments[gid] = dict(PAID)
    drop_bot.last_drop_snapshot[gid] = {"stock": dict(STOCK),
                                        "claims": {k: list(v) for k, v in CLAIMS.items()}}
    drop_bot.archived_payments.pop(gid, None)


def _next_drop_staged(gid, new_stock=True):
    """State after `!drop` starts the next session (see cmd_drop): the closed
    drop moved to the snapshot/archive and the live dicts were reset."""
    drop_bot.last_drop_snapshot[gid] = {"stock": dict(STOCK),
                                        "claims": {k: list(v) for k, v in CLAIMS.items()}}
    drop_bot.archived_payments[gid] = {"payments": dict(PAID),
                                       "claims": {k: list(v) for k, v in CLAIMS.items()},
                                       "stock": dict(STOCK)}
    drop_bot.stock[gid] = ({"jacket": {"display": "JACKET", "price": 80.0, "qty": 5,
                                       "limit": None}} if new_stock else {})
    drop_bot.claims[gid] = {}
    drop_bot.payments[gid] = {}


def _rows(gid):
    stock_ref, claims_ref, payments_ref, from_archive = drop_bot.export_refs(gid)
    return drop_bot.build_buyer_rows(stock_ref, claims_ref, payments_ref), from_archive


# ── The conversion: stock, claims and payments must describe one drop ─────────

def test_export_of_live_drop_uses_live_state():
    gid = 91001
    _closed_drop_in_memory(gid)
    rows, from_archive = _rows(gid)
    assert not from_archive
    assert [r["name"] for r in rows] == ["alice", "bob"]
    assert [r["owed"] for r in rows] == [100.0, 50.0]
    assert [r["status"] for r in rows] == ["Paid", "Unpaid"]


@pytest.mark.parametrize("new_stock", [True, False])
def test_export_after_next_drop_starts_still_has_the_orders(new_stock):
    """The regression: new stock + previous claims exported an empty sheet."""
    gid = 91002
    _next_drop_staged(gid, new_stock=new_stock)
    rows, from_archive = _rows(gid)
    assert from_archive
    assert [r["name"] for r in rows] == ["alice", "bob"]
    assert [r["owed"] for r in rows] == [100.0, 50.0]


def test_export_after_next_drop_starts_keeps_payment_status():
    """`!drop` moves payments to archived_payments — paid buyers stay paid."""
    gid = 91003
    _next_drop_staged(gid)
    rows, _ = _rows(gid)
    assert [(r["name"], r["status"], r["confirmed"]) for r in rows] == [
        ("alice", "Paid", 100.0), ("bob", "Unpaid", 0.0)]


def test_claims_with_no_matching_stock_are_skipped_not_crashed():
    gid = 91004
    drop_bot.stock[gid] = {}
    drop_bot.claims[gid] = {"ghost": [{"user": ALICE, "qty": 1, "time": None}]}
    drop_bot.payments[gid] = {}
    drop_bot.last_drop_snapshot.pop(gid, None)
    drop_bot.archived_payments.pop(gid, None)
    rows, _ = _rows(gid)
    assert rows == []


def test_partial_payment_is_not_reported_as_paid():
    gid = 91005
    _closed_drop_in_memory(gid)
    drop_bot.payments[gid] = {
        111: [{"method": "venmo", "amount": 40.0, "confirmed": True, "time": None}]}
    rows, _ = _rows(gid)
    alice = next(r for r in rows if r["user_id"] == 111)
    assert alice["status"] == "Partial"
    assert alice["outstanding"] == 60.0


# ── The three output formats ─────────────────────────────────────────────────

def test_workbook_has_every_buyer_and_no_duplicate_previous_sheet():
    gid = 91006
    _next_drop_staged(gid)
    rows, from_archive = _rows(gid)
    wb = drop_bot.build_export_workbook(gid, rows, previous_rows=[])
    values = [r for r in wb["Orders"].iter_rows(values_only=True)]
    assert values[0][0] == "Buyer"
    assert [v[0] for v in values[1:]] == ["alice", "bob"]
    # The archived drop is already the exported one — don't ship it twice.
    assert from_archive and "Previous Drop" not in wb.sheetnames


def test_csv_has_one_row_per_claim_line():
    gid = 91007
    _closed_drop_in_memory(gid)
    rows, _ = _rows(gid)
    parsed = list(csv.reader(io.StringIO(drop_bot.build_claims_csv(rows))))
    assert parsed[0] == drop_bot.CLAIM_CSV_HEADERS
    assert [r[0] for r in parsed[1:]] == ["alice", "bob"]
    assert parsed[1][9] == "Paid" and parsed[2][9] == "Unpaid"


def test_claim_list_text_lists_everyone_even_on_a_huge_drop():
    """The embed version trims at 25 buyers; this one never does."""
    gid = 91008
    drop_bot.stock[gid] = dict(STOCK)
    drop_bot.claims[gid] = {"hoodie": [
        {"user": FakeUser(2000 + i, f"buyer{i:03d}"), "qty": 1, "time": None}
        for i in range(120)]}
    drop_bot.payments[gid] = {}
    rows, _ = _rows(gid)
    text = drop_bot.build_claims_text(rows, "Test — Drop #9 claim list")
    assert len(rows) == 120
    for i in (0, 42, 119):
        assert f"buyer{i:03d}" in text
    assert "120 buyer(s), 120 item(s)" in text
    assert "Total owed $6000.00" in text


def test_claim_list_text_handles_an_empty_drop():
    assert "No claims." in drop_bot.build_claims_text([], "Nothing here")


# ── Dashboard downloads ──────────────────────────────────────────────────────

CLAIM_KEYS = ["user_id", "user_name", "item_display", "qty", "price", "subtotal",
              "confirmed", "tracking"]
CLAIM_ROWS = [(111, "alice", "HOODIE", 2, 50.0, 100.0, True, "1Z999"),
              (111, "alice", "TEE", 1, 25.0, 25.0, True, None),
              (222, "bob", "HOODIE", 1, 50.0, 50.0, False, None)]


class _FakeConn:
    async def fetch(self, query, *args):
        return [dict(zip(CLAIM_KEYS, r)) for r in CLAIM_ROWS]


class _FakePool:
    @asynccontextmanager
    async def _acquire(self):
        yield _FakeConn()

    def acquire(self):
        return self._acquire()


def _fake_request(guild_name="Test Store"):
    app = type("App", (), {"state": type("State", (), {"pool": _FakePool()})})
    return type("Request", (), {
        "app": app, "session": {"guild_id": 42, "guild_name": guild_name}})()


@pytest.mark.parametrize("guild_name", ["Test Store", "🔥 Vault Drops — PNW"])
def test_dashboard_downloads_survive_any_store_name(guild_name):
    """A store name with an emoji used to 500 the download: Starlette encodes
    headers as latin-1, and Content-Disposition carried the raw name."""
    for route in (webapp.drop_export, webapp.drop_export_csv):
        response = asyncio.run(route(_fake_request(guild_name), 13))
        assert response.status_code == 200
        assert response.body
        # Exactly what Starlette does when it builds the response.
        response.headers["content-disposition"].encode("latin-1")


def test_dashboard_xlsx_contains_the_orders():
    response = asyncio.run(webapp.drop_export(_fake_request(), 13))
    ws = openpyxl.load_workbook(io.BytesIO(response.body)).active
    values = [r for r in ws.iter_rows(values_only=True)]
    assert values[0] == tuple(webapp.EXPORT_HEADERS)
    assert values[1][:6] == ("alice", "111", "HOODIE", 2, 100, 125)
    assert values[3][:6] == ("bob", "222", "HOODIE", 1, 50, 50)


def test_dashboard_csv_matches_the_xlsx_rows():
    response = asyncio.run(webapp.drop_export_csv(_fake_request(), 13))
    parsed = list(csv.reader(io.StringIO(response.body.decode("utf-8-sig"))))
    assert parsed[0] == webapp.EXPORT_HEADERS
    assert parsed[1] == ["alice", "111", "HOODIE", "2", "100.00", "125.00", "Yes", "1Z999"]
    assert parsed[2] == ["", "", "TEE", "1", "25.00", "", "", ""]
    assert parsed[3] == ["bob", "222", "HOODIE", "1", "50.00", "50.00", "No", ""]


def test_dashboard_export_requires_a_session():
    anonymous = type("Request", (), {"app": None, "session": {}})()
    assert asyncio.run(webapp.drop_export_csv(anonymous, 13)).status_code == 303


# ── Every claim, grouped by buyer (!claims and the dashboard's Buyers page) ───

HISTORY_KEYS = ["user_id", "user_name", "drop_number", "closed_at", "item_display",
                "qty", "price", "subtotal", "confirmed", "tracking"]
CLOSED_AT = __import__("datetime").datetime(2026, 8, 1)
HISTORY = [
    (111, "alice", 11, CLOSED_AT, "HOODIE", 1, 50.0, 50.0, True, "1Z111"),
    (111, "alice", 12, CLOSED_AT, "TEE", 2, 25.0, 50.0, False, None),
    (222, "bob", 12, CLOSED_AT, "HOODIE", 1, 50.0, 50.0, True, "1Z222"),
    (333, "Cara", 12, CLOSED_AT, "CAP", 3, 15.0, 45.0, True, None),
]


def _history_rows():
    return [dict(zip(HISTORY_KEYS, r)) for r in HISTORY]


def test_ledgers_group_every_claim_under_its_buyer():
    ledgers = drop_bot.build_buyer_ledgers(_history_rows())
    assert [led["name"] for led in ledgers] == ["alice", "bob", "Cara"]
    alice = ledgers[0]
    assert [d["drop_number"] for d in alice["drops"]] == [11, 12]
    assert alice["total"] == 100.0 and alice["paid"] == 50.0
    assert alice["outstanding"] == 50.0 and alice["items"] == 3
    assert alice["drops"][0]["tracking"] == "1Z111"


def test_ledgers_include_the_drop_still_in_memory():
    """A live drop isn't in user_claims yet — "every claim" has to cover it."""
    live_rows = drop_bot.build_buyer_rows(STOCK, CLAIMS, PAID)
    ledgers = drop_bot.build_buyer_ledgers(_history_rows(), live_drop=(13, live_rows))
    alice = next(led for led in ledgers if led["user_id"] == 111)
    live = [d for d in alice["drops"] if d["live"]]
    assert [d["drop_number"] for d in live] == [13]
    assert alice["total"] == 200.0          # 100 saved + 100 claimed live
    assert alice["paid"] == 150.0           # 50 saved + 100 confirmed live
    assert any(led["user_id"] == 222 for led in ledgers)


def test_ledger_text_has_a_section_per_buyer():
    ledgers = drop_bot.build_buyer_ledgers(_history_rows())
    text = drop_bot.build_buyer_ledger_text(ledgers, "Store — every claim by buyer")
    assert "3 buyer(s)" in text
    for name, uid in (("alice", 111), ("bob", 222), ("Cara", 333)):
        assert f"{name}  (ID: {uid})" in text
    assert "Drop #11" in text and "Drop #12" in text
    assert "1Z222" in text


def test_ledger_csv_repeats_the_buyer_on_every_claim():
    ledgers = drop_bot.build_buyer_ledgers(_history_rows())
    parsed = list(csv.reader(io.StringIO(drop_bot.build_buyer_ledger_csv(ledgers))))
    assert parsed[0] == drop_bot.BUYER_CSV_HEADERS
    assert len(parsed) == 1 + len(HISTORY)
    assert [r[0] for r in parsed[1:]] == ["alice", "alice", "bob", "Cara"]
    assert all(r[1] for r in parsed[1:]), "every row carries the user ID"


@pytest.mark.parametrize("arg, expected", [
    ("", (False, None, None, None)),
    ("csv", (True, None, None, None)),
    ("<@123456789012345678>", (False, 123456789012345678, None, None)),
    ("123456789012345678 csv", (True, 123456789012345678, None, None)),
    ("13", (False, None, 13, None)),
    ("drop 13", (False, None, 13, None)),
    ("#13", (False, None, 13, None)),
    ("banana", (False, None, None, "banana")),
])
def test_claims_argument_parsing(arg, expected):
    assert drop_bot.parse_claims_filter(arg) == expected


class _RecordingConn:
    """Captures the query the dashboard builds so the filter can be asserted."""

    def __init__(self, rows):
        self.rows, self.query, self.args = rows, None, None

    async def fetch(self, query, *args):
        self.query, self.args = query, args
        return self.rows


def test_dashboard_buyer_ledgers_group_by_user():
    conn = _RecordingConn(_history_rows())
    ledgers = asyncio.run(webapp._load_buyer_ledgers(conn, 42))
    assert [led["name"] for led in ledgers] == ["alice", "bob", "Cara"]
    # Newest drop first on the page.
    assert [d["drop_number"] for d in ledgers[0]["drops"]] == [12, 11]
    assert ledgers[0]["outstanding"] == 50.0
    assert "user_name ILIKE" not in conn.query


def test_dashboard_buyer_filter_is_pushed_into_the_query():
    conn = _RecordingConn([])
    asyncio.run(webapp._load_buyer_ledgers(conn, 42, q="ali"))
    assert "user_name ILIKE $2" in conn.query and conn.args[1] == "%ali%"

    conn = _RecordingConn([])
    asyncio.run(webapp._load_buyer_ledgers(conn, 42, q="111"))
    assert "user_id = $2" in conn.query and conn.args[1] == 111


def test_dashboard_buyer_exports_carry_every_claim():
    request = _fake_request("🔥 Vault Drops")

    class _Pool(_FakePool):
        @asynccontextmanager
        async def _acquire(self):
            yield _RecordingConn(_history_rows())

    request.app.state.pool = _Pool()

    response = asyncio.run(webapp.buyers_export_csv(request))
    response.headers["content-disposition"].encode("latin-1")
    parsed = list(csv.reader(io.StringIO(response.body.decode("utf-8-sig"))))
    assert parsed[0] == webapp.BUYER_CSV_HEADERS
    assert [r[0] for r in parsed[1:]] == ["alice", "alice", "bob", "Cara"]

    response = asyncio.run(webapp.buyers_export_xlsx(request))
    wb = openpyxl.load_workbook(io.BytesIO(response.body))
    assert wb.sheetnames == ["Buyers", "Claims"]
    summary = [r for r in wb["Buyers"].iter_rows(values_only=True)]
    assert summary[1][:4] == ("alice", "111", 2, 3)
    assert len([r for r in wb["Claims"].iter_rows(values_only=True)]) == 1 + len(HISTORY)
