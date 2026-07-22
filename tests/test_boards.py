"""Unit tests for drop_bot's embed builders and command registration.

These are deliberately dependency-light: they import drop_bot (which no longer
runs the bot at import time) and exercise the pure/in-memory board builders.
They exist to catch the two regressions that actually bit this project:

  * a large accidental deletion silently removing commands, and
  * claim-list embeds blowing past Discord's 25-field / 6000-char limits on a
    busy drop, which makes the board edit fail and freeze.
"""
import discord
import drop_bot


class FakeUser:
    def __init__(self, uid, name):
        self.id = uid
        self.display_name = name


def _setup_drop(gid, n_buyers, price=100.0, confirmed=False):
    """Populate in-memory state for `gid` with n_buyers each claiming one item."""
    drop_bot.stock[gid] = {
        "ITEM": {"display": "Test Item", "price": price, "qty": 10_000, "limit": None}
    }
    drop_bot.claims[gid] = {"ITEM": []}
    drop_bot.payments[gid] = {}
    for i in range(n_buyers):
        u = FakeUser(1000 + i, f"LongishBuyerName{i:03d}")
        drop_bot.claims[gid]["ITEM"].append({"user": u, "qty": 1, "time": None})
        drop_bot.payments[gid][u.id] = [
            {"amount": price if confirmed else 0.0, "method": "venmo", "confirmed": confirmed}
        ]


# ── Discord embed limits ──────────────────────────────────────────────────────

def test_live_claimlist_stays_within_discord_limits():
    gid = 90001
    _setup_drop(gid, 60)  # far more than 25 buyers
    embed = drop_bot.build_live_claimlist_embed(gid)
    assert len(embed.fields) <= 25
    assert len(embed) <= 6000
    assert any("more buyers" in (f.name or "") for f in embed.fields), "overflow summary missing"


def test_manager_claimlist_stays_within_discord_limits():
    gid = 90002
    _setup_drop(gid, 80)
    embed = drop_bot.build_claimlist_embed(gid)
    assert len(embed.fields) <= 25
    assert len(embed) <= 6000


def test_payment_board_description_within_limits():
    gid = 90003
    _setup_drop(gid, 200)
    embed = drop_bot.build_payment_board_embed(gid)
    assert len(embed.description or "") <= 4096
    assert len(embed) <= 6000


def test_small_drop_has_no_overflow_field():
    gid = 90004
    _setup_drop(gid, 3)
    embed = drop_bot.build_live_claimlist_embed(gid)
    assert len(embed.fields) == 3
    assert all("more buyers" not in (f.name or "") for f in embed.fields)


# ── _fill_buyer_fields / _cap_description helpers ─────────────────────────────

def test_fill_buyer_fields_summarizes_overflow():
    embed = discord.Embed(title="t")
    entries = [(f"name{i}", f"value{i}") for i in range(100)]
    drop_bot._fill_buyer_fields(embed, entries, overflow_noun="buyers")
    assert len(embed.fields) <= 25
    assert embed.fields[-1].name.startswith("➕")
    # the summary should account for everyone not shown
    shown = len(embed.fields) - 1
    assert f"{100 - shown} more buyers" in embed.fields[-1].name


def test_cap_description_trims_long_text():
    out = drop_bot._cap_description("x" * 6000)
    assert len(out) <= 4096
    assert "trimmed" in out


def test_cap_description_leaves_short_text():
    out = drop_bot._cap_description("short")
    assert out == "short"


# ── Drop-number labelling ─────────────────────────────────────────────────────

def test_stock_embed_shows_drop_number():
    gid = 90005
    drop_bot.current_drop_number[gid] = 7
    drop_bot.stock[gid] = {"A": {"display": "A", "price": 10.0, "qty": 5, "limit": None}}
    drop_bot.claims[gid] = {"A": []}
    embed = drop_bot.build_stock_embed(gid)
    assert "Drop #7" in embed.title


def test_stock_embed_falls_back_without_number():
    gid = 90006
    drop_bot.current_drop_number.pop(gid, None)
    drop_bot.stock[gid] = {"A": {"display": "A", "price": 10.0, "qty": 5, "limit": None}}
    drop_bot.claims[gid] = {"A": []}
    embed = drop_bot.build_stock_embed(gid)
    assert embed.title == "🛒  Drop Stock"


# ── Command registration (guards against accidental mass deletion) ────────────

def test_core_commands_registered():
    names = {c.name for c in drop_bot.bot.commands}
    expected = {
        "drop", "release", "enddrop", "paid", "confirm", "addtracking",
        "myhistory", "export", "paymentboard", "claimlist", "stock", "announce",
    }
    missing = expected - names
    assert not missing, f"missing commands: {sorted(missing)}"
