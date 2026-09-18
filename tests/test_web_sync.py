"""Tests for the two bot↔dashboard links that were documented but missing.

Both were silent failures rather than errors, which is why they went unnoticed:

  * `!webkey` was documented in README/WEB.md but no such command existed, and
    nothing ever wrote `server_settings.web_access_key` — so the dashboard's
    per-server login could never succeed for anyone,
  * the dashboard queued buyer DMs (tracking numbers, raffle confirmations)
    into `pending_notifications` and told the manager they'd been sent, but the
    bot never read that table, so the rows just accumulated.
"""
import asyncio
from contextlib import asynccontextmanager

import discord
import pytest

import drop_bot


# ── Fake database ─────────────────────────────────────────────────────────────

class FakeConn:
    """Enough of an asyncpg connection for the queries these paths run.

    Backed by a dict of guild_id -> settings row and a list of notification
    rows, so a test can assert on what the code actually persisted.
    """

    def __init__(self, store):
        self.store = store

    async def fetchrow(self, sql, *args):
        if "SELECT web_access_key" in sql:
            row = self.store["settings"].get(args[0])
            return {"web_access_key": row["web_access_key"]} if row else None
        raise AssertionError(f"unexpected fetchrow: {sql}")

    async def fetch(self, sql, *args):
        if "FROM pending_notifications" in sql:
            max_attempts, limit = args
            return [
                r for r in self.store["notifications"]
                if r["sent_at"] is None and r["attempts"] < max_attempts
            ][:limit]
        raise AssertionError(f"unexpected fetch: {sql}")

    async def execute(self, sql, *args):
        if "INSERT INTO server_settings" in sql:
            guild_id, key, guild_name = args
            row = self.store["settings"].setdefault(
                guild_id, {"web_access_key": None, "guild_name": None}
            )
            row["web_access_key"] = key
            row["guild_name"] = guild_name or row["guild_name"]
            return "INSERT 0 1"
        if "make_interval" in sql:
            # The staleness sweep, keyed on each row's age in hours.
            max_age, = args
            stale = [r for r in self.store["notifications"]
                     if r["sent_at"] is None and r["age_hours"] > max_age]
            for r in stale:
                r["sent_at"] = "retired"
            return f"UPDATE {len(stale)}"
        if "UPDATE pending_notifications" in sql:
            row = next(r for r in self.store["notifications"] if r["id"] == args[0])
            row["attempts"] += 1
            if "sent_at = NOW()" in sql:
                row["sent_at"] = "now"
            return "UPDATE 1"
        raise AssertionError(f"unexpected execute: {sql}")


class FakePool:
    def __init__(self, store):
        self.store = store

    @asynccontextmanager
    async def _acquire(self):
        yield FakeConn(self.store)

    def acquire(self):
        return self._acquire()


@pytest.fixture
def store(monkeypatch):
    data = {"settings": {}, "notifications": []}
    monkeypatch.setattr(drop_bot, "db_pool", FakePool(data))
    return data


# ── #1  !webkey ───────────────────────────────────────────────────────────────

def test_webkey_command_is_registered():
    """The command the README and WEB.md both tell managers to run."""
    assert drop_bot.bot.get_command("webkey") is not None


def test_webkey_mints_a_key_on_first_use(store):
    key = asyncio.run(drop_bot.db_get_or_create_web_key(42, "Vault Drops"))
    assert key
    assert store["settings"][42]["web_access_key"] == key
    # The name rides along so the dashboard shows more than a raw guild ID.
    assert store["settings"][42]["guild_name"] == "Vault Drops"


def test_webkey_is_stable_across_calls(store):
    """Plain `!webkey` re-reads the existing key rather than rotating it —
    otherwise every lookup would log out whoever was already signed in."""
    first = asyncio.run(drop_bot.db_get_or_create_web_key(42, "Vault Drops"))
    second = asyncio.run(drop_bot.db_get_or_create_web_key(42, "Vault Drops"))
    assert first == second


def test_webkey_reset_invalidates_the_old_key(store):
    first = asyncio.run(drop_bot.db_get_or_create_web_key(42, "Vault Drops"))
    second = asyncio.run(drop_bot.db_get_or_create_web_key(42, "Vault Drops", reset=True))
    assert second != first
    assert store["settings"][42]["web_access_key"] == second


def test_webkey_is_unguessable(store):
    """It's a bearer credential with full write access to a server's records."""
    keys = {asyncio.run(drop_bot.db_set_web_key(gid)) for gid in range(25)}
    assert len(keys) == 25                      # no collisions
    assert all(len(k) >= 32 for k in keys)      # token_urlsafe(32) ≈ 43 chars


def test_webkey_reset_keeps_the_stored_guild_name(store):
    """A reset from a DM has no guild object to read the name from; it must not
    blank out a name an earlier call already saved."""
    asyncio.run(drop_bot.db_set_web_key(42, "Vault Drops"))
    asyncio.run(drop_bot.db_set_web_key(42, None))
    assert store["settings"][42]["guild_name"] == "Vault Drops"


# ── #2  pending_notifications ─────────────────────────────────────────────────

class FakeTarget:
    """A user the bot can DM. `raises` makes the send fail."""

    def __init__(self, uid, raises=None):
        self.id = uid
        self.raises = raises
        self.sent = []

    async def send(self, message):
        if self.raises is not None:
            raise self.raises
        self.sent.append(message)


def _queue(store, *rows, age_hours=0):
    for i, (user_id, message) in enumerate(rows, start=1):
        store["notifications"].append({
            "id": i, "guild_id": 42, "user_id": user_id,
            "message": message, "sent_at": None, "attempts": 0,
            "age_hours": age_hours,
        })


def _drain_with(monkeypatch, target):
    monkeypatch.setattr(
        drop_bot, "_resolve_dm_target",
        lambda guild_id, user_id: _immediately(target),
    )
    asyncio.run(drop_bot._drain_pending_notifications())


async def _immediately(value):
    return value


def test_queued_dm_is_delivered(store, monkeypatch):
    """The dashboard tells the manager the buyer was notified — so a row in
    this table has to actually turn into a DM."""
    _queue(store, (111, "📦 Your Drop #13 order shipped: 1Z999"))
    target = FakeTarget(111)
    _drain_with(monkeypatch, target)

    assert target.sent == ["📦 Your Drop #13 order shipped: 1Z999"]
    assert store["notifications"][0]["sent_at"] is not None


def test_delivered_dm_is_not_sent_twice(store, monkeypatch):
    _queue(store, (111, "first"))
    target = FakeTarget(111)
    _drain_with(monkeypatch, target)
    _drain_with(monkeypatch, target)
    assert target.sent == ["first"]


def test_closed_dms_are_dropped_not_retried(store, monkeypatch):
    """Retrying a buyer whose DMs are closed can never succeed, and would keep
    the row at the front of the queue forever."""
    _queue(store, (111, "hello"))
    _drain_with(monkeypatch, FakeTarget(111, raises=discord.Forbidden.__new__(discord.Forbidden)))
    row = store["notifications"][0]
    assert row["sent_at"] is not None
    assert row["attempts"] == 1


def test_transient_failure_is_retried_then_given_up_on(store, monkeypatch):
    """A network blip should be retried; a row that never succeeds must stop
    being retried so it can't stall the queue behind it."""
    _queue(store, (111, "hello"))
    flaky = FakeTarget(111, raises=RuntimeError("connection reset"))

    for _ in range(drop_bot.NOTIFY_MAX_ATTEMPTS):
        _drain_with(monkeypatch, flaky)
        # Still unsent, so it stays eligible until the attempt cap is reached.
        assert store["notifications"][0]["sent_at"] is None

    assert store["notifications"][0]["attempts"] == drop_bot.NOTIFY_MAX_ATTEMPTS
    # Past the cap the drain skips it entirely.
    _drain_with(monkeypatch, flaky)
    assert store["notifications"][0]["attempts"] == drop_bot.NOTIFY_MAX_ATTEMPTS


def test_one_bad_row_does_not_block_the_next(store, monkeypatch):
    _queue(store, (111, "blocked"), (222, "should still arrive"))
    delivered = []

    async def resolve(guild_id, user_id):
        if user_id == 111:
            raise RuntimeError("boom")
        target = FakeTarget(user_id)
        delivered.append(target)
        return target

    monkeypatch.setattr(drop_bot, "_resolve_dm_target", resolve)
    asyncio.run(drop_bot._drain_pending_notifications())

    assert [t.sent for t in delivered] == [["should still arrive"]]
    assert store["notifications"][1]["sent_at"] is not None


def test_drain_is_batched(store, monkeypatch):
    """A 'push tracking to every buyer' click can queue hundreds of rows; one
    pass must not try to send them all."""
    _queue(store, *[(1000 + i, f"msg {i}") for i in range(drop_bot.NOTIFY_BATCH + 20)])
    sent = []

    async def resolve(guild_id, user_id):
        target = FakeTarget(user_id)
        sent.append(target)
        return target

    monkeypatch.setattr(drop_bot, "_resolve_dm_target", resolve)
    asyncio.run(drop_bot._drain_pending_notifications())
    assert len(sent) == drop_bot.NOTIFY_BATCH


def test_outbox_poller_is_wired_up():
    """The loop on_ready starts has to drain both dashboard outboxes."""
    assert callable(drop_bot.poll_web_outbox)
    assert callable(drop_bot._drain_pending_actions)
    assert callable(drop_bot._drain_pending_notifications)


def test_stale_dms_are_retired_not_delivered_late(store, monkeypatch):
    """Rows stranded while the bot had no poller at all must not turn into a
    burst of tracking DMs about drops that finished months ago."""
    _queue(store, (111, "ancient"), age_hours=drop_bot.NOTIFY_MAX_AGE_HOURS + 1)
    target = FakeTarget(111)
    _drain_with(monkeypatch, target)

    assert target.sent == []
    # Retired, not left pending — otherwise it's rescanned on every pass.
    assert store["notifications"][0]["sent_at"] is not None


def test_recent_dms_are_still_delivered(store, monkeypatch):
    _queue(store, (111, "recent"), age_hours=drop_bot.NOTIFY_MAX_AGE_HOURS - 1)
    target = FakeTarget(111)
    _drain_with(monkeypatch, target)
    assert target.sent == ["recent"]
