#!/usr/bin/env python3
"""
Backfill Drop #12 for UCE'S PLUG CHAT into the dashboard database.

Why this exists
---------------
A drop that closed but the bot never captured (e.g. the bot was offline when
the drop closed) leaves no rows in `drop_history` / `user_claims`, so it never
shows up on the web dashboard. This script writes those rows by hand, using the
exact same shape the bot's `close_drop()` writes, so the drop appears on the
dashboard just like a normally-closed one.

The drop being restored (from the final closed claim list):

    Drop #12 — UCE'S PLUG CHAT
    🔴 Drop CLOSED — Final Claim List
    dealuh
      • CHAOS RISING BOOSTER BUNDLE x2 — $80.00
    Total: $80.00
    5/23/26, 3:19 PM

    → qty 2 @ $40.00 each = $80.00 subtotal (the "x{qty} — ${subtotal}" format
      the bot uses; see build_claimlist_embed in drop_bot.py).

How to run
----------
Against the SAME Postgres the bot/dashboard use:

    railway run python scripts/backfill_drop_12.py
  or
    DATABASE_URL="postgres://user:pass@host:5432/db" python scripts/backfill_drop_12.py

Optional overrides (only if auto-resolution can't find them):
    GUILD_ID=123...        # skip guild-name lookup, use this guild id
    BUYER_USER_ID=123...   # dealuh's Discord user id, if not in prior history
    PAID=1                 # mark the order paid (default: unpaid, like a fresh close)

The script is idempotent: running it twice will not create a duplicate. It also
refuses to run if inserting this drop would NOT land at position #12 (i.e. the
store doesn't currently have exactly 11 earlier drops), so it can't silently
renumber your existing drops. Pass FORCE=1 to override that guard.
"""

import asyncio
import datetime
import json
import os
import sys

import asyncpg

# ── The drop, as read off the final claim list ────────────────────────────────
GUILD_NAME = "UCE'S PLUG CHAT"
CLOSED_AT = datetime.datetime(2026, 5, 23, 15, 19, 0)  # 5/23/26, 3:19 PM
BUYER_NAME = "dealuh"
ITEM_DISPLAY = "CHAOS RISING BOOSTER BUNDLE"
QTY = 2
UNIT_PRICE = 40.00          # $80.00 subtotal / qty 2
SUBTOTAL = QTY * UNIT_PRICE  # 80.00
EXPECTED_DROP_NUMBER = 12

PAID = os.getenv("PAID", "").strip() in ("1", "true", "yes")
FORCE = os.getenv("FORCE", "").strip() in ("1", "true", "yes")


def _fail(msg):
    print(f"❌  {msg}")
    sys.exit(1)


async def resolve_guild_id(conn):
    override = os.getenv("GUILD_ID", "").strip()
    if override.isdigit():
        return int(override)
    rows = await conn.fetch(
        "SELECT guild_id, guild_name FROM server_settings "
        "WHERE guild_name ILIKE $1",
        GUILD_NAME,
    )
    if not rows:
        # Fall back to the presence table the dashboard also relies on.
        rows = await conn.fetch(
            "SELECT guild_id, guild_name FROM bot_guilds WHERE guild_name ILIKE $1",
            GUILD_NAME,
        )
    if not rows:
        _fail(f"Could not find a guild named {GUILD_NAME!r}. "
              f"Set GUILD_ID=<id> and re-run.")
    if len(rows) > 1:
        opts = ", ".join(f"{r['guild_id']} ({r['guild_name']})" for r in rows)
        _fail(f"Multiple guilds match {GUILD_NAME!r}: {opts}. "
              f"Set GUILD_ID=<id> to pick one.")
    return rows[0]["guild_id"]


async def resolve_buyer_id(conn, guild_id):
    override = os.getenv("BUYER_USER_ID", "").strip()
    if override.isdigit():
        return int(override)
    # Reuse dealuh's real Discord id from their existing order history so the
    # dashboard groups this drop under the same buyer.
    row = await conn.fetchrow(
        """SELECT user_id FROM user_claims
           WHERE guild_id = $1 AND user_name ILIKE $2
           ORDER BY drop_number DESC LIMIT 1""",
        guild_id, BUYER_NAME,
    )
    if row:
        return row["user_id"]
    _fail(f"No prior order history for buyer {BUYER_NAME!r} in this guild, so "
          f"their Discord user id is unknown. Set BUYER_USER_ID=<id> and re-run "
          f"(you can copy it from Discord with Developer Mode on).")


async def already_present(conn, guild_id, user_id):
    """Idempotency: has this exact drop already been backfilled?"""
    dh = await conn.fetchrow(
        """SELECT id FROM drop_history
           WHERE guild_id = $1 AND closed_at = $2 AND total_revenue = $3""",
        guild_id, CLOSED_AT, SUBTOTAL,
    )
    uc = await conn.fetchrow(
        """SELECT id FROM user_claims
           WHERE guild_id = $1 AND user_id = $2 AND closed_at = $3
             AND item_display = $4""",
        guild_id, user_id, CLOSED_AT, ITEM_DISPLAY,
    )
    return bool(dh or uc)


async def run():
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        _fail("DATABASE_URL is not set. Run via `railway run python "
              "scripts/backfill_drop_12.py` or export DATABASE_URL first.")

    conn = await asyncpg.connect(dsn)
    try:
        guild_id = await resolve_guild_id(conn)
        user_id = await resolve_buyer_id(conn, guild_id)
        print(f"→ Guild:  {guild_id} ({GUILD_NAME})")
        print(f"→ Buyer:  {user_id} ({BUYER_NAME})")

        if await already_present(conn, guild_id, user_id):
            print("✅  Drop #12 is already saved for this guild — nothing to do.")
            return

        # The dashboard numbers drops by closed_at order, and user_claims stores
        # that number. Work out where this drop lands and make sure it is #12.
        total_drops = await conn.fetchval(
            "SELECT COUNT(*) FROM drop_history WHERE guild_id = $1", guild_id
        )
        drops_before = await conn.fetchval(
            "SELECT COUNT(*) FROM drop_history "
            "WHERE guild_id = $1 AND closed_at <= $2",
            guild_id, CLOSED_AT,
        )
        position = drops_before + 1
        drops_after = total_drops - drops_before
        print(f"→ Existing drops: {total_drops} "
              f"({drops_before} on/before this date, {drops_after} after)")
        print(f"→ This drop would become #{position}")

        if position != EXPECTED_DROP_NUMBER and not FORCE:
            _fail(f"Refusing to insert: this would become drop #{position}, "
                  f"not #{EXPECTED_DROP_NUMBER}. Your store currently has "
                  f"{total_drops} drop(s). If that's expected, re-run with FORCE=1.")
        if drops_after and not FORCE:
            _fail(f"Refusing to insert: {drops_after} existing drop(s) close "
                  f"AFTER {CLOSED_AT}, so inserting here would renumber them. "
                  f"Re-run with FORCE=1 only if you understand this.")

        summary = {ITEM_DISPLAY: {"qty": QTY, "revenue": SUBTOTAL}}

        async with conn.transaction():
            # Mirror db_save_drop_history(): revenue, item count, buyer count.
            await conn.execute(
                """INSERT INTO drop_history
                       (guild_id, closed_at, total_revenue, total_items,
                        unique_buyers, summary)
                   VALUES ($1, $2, $3, $4, $5, $6)""",
                guild_id, CLOSED_AT, SUBTOTAL, QTY, 1, json.dumps(summary),
            )
            # Mirror db_save_user_claims(): one row per claimed item.
            await conn.execute(
                """INSERT INTO user_claims
                       (guild_id, user_id, user_name, drop_number, closed_at,
                        item_display, qty, price, subtotal, confirmed)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)""",
                guild_id, user_id, BUYER_NAME, position, CLOSED_AT,
                ITEM_DISPLAY, QTY, UNIT_PRICE, SUBTOTAL, PAID,
            )

        print(f"✅  Saved Drop #{position} for {GUILD_NAME}: "
              f"{BUYER_NAME} — {ITEM_DISPLAY} x{QTY} — ${SUBTOTAL:.2f} "
              f"({'PAID' if PAID else 'unpaid'}).")
        print("   It will show on the dashboard's Drops list immediately.")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(run())
