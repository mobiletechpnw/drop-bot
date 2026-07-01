#!/usr/bin/env python3
"""
READ-ONLY inspection of what's already saved for a guild's drops.

Changes nothing. It prints every saved drop (in the dashboard's close-date
order) with its date, totals, and the buyers/items on it, plus how the stored
drop_number values line up. Use it to see the current state before backfilling
missing drops.

Run:
    # set your PUBLIC Railway url first, then run
    #   PowerShell:  $env:DATABASE_URL = "postgresql://...proxy.rlwy.net:PORT/railway"
    #   bash:        export DATABASE_URL="postgresql://...proxy.rlwy.net:PORT/railway"
    python scripts/inspect_drops.py
"""

import asyncio
import os
import sys

import asyncpg

# UCE'S PLUG CHAT ✨ 🤙🏽 — matched by id so the curly apostrophe/emoji name
# can't trip us up. Override with the GUILD_ID env var if ever needed.
GUILD_ID = 1492850792175108149


async def main():
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("❌  DATABASE_URL is not set. Set it to your PUBLIC Railway url "
              "(host ends in .proxy.rlwy.net) and re-run.")
        sys.exit(1)

    gid = int(os.getenv("GUILD_ID") or GUILD_ID)
    conn = await asyncpg.connect(dsn, timeout=30)
    try:
        row = await conn.fetchrow(
            "SELECT guild_id, guild_name FROM server_settings WHERE guild_id = $1",
            gid)
        name = row["guild_name"] if row else "(no server_settings row)"
        print(f"Guild: {gid}  ({name})\n")

        drops = await conn.fetch(
            """SELECT ROW_NUMBER() OVER (ORDER BY closed_at) AS pos,
                      closed_at, total_revenue, total_items, unique_buyers
               FROM drop_history WHERE guild_id = $1 ORDER BY closed_at""", gid)
        print(f"=== drop_history: {len(drops)} saved drop(s), by close date ===")
        for d in drops:
            print(f"  dashboard #{d['pos']:>2}  {d['closed_at']}  "
                  f"rev=${float(d['total_revenue']):.2f}  "
                  f"items={d['total_items']}  buyers={d['unique_buyers']}")

        print("\n=== buyers/items per saved drop (from user_claims) ===")
        claims = await conn.fetch(
            """SELECT drop_number, closed_at, user_id, user_name,
                      item_display, qty, price, subtotal, confirmed
               FROM user_claims WHERE guild_id = $1
               ORDER BY drop_number, user_name, item_display""", gid)
        by_drop = {}
        for c in claims:
            by_drop.setdefault(c["drop_number"], []).append(c)
        if not by_drop:
            print("  (no user_claims rows saved for this guild)")
        for dn in sorted(by_drop):
            cs = by_drop[dn]
            dt = cs[0]["closed_at"]
            total = sum(float(c["subtotal"]) for c in cs)
            buyers = sorted({c["user_name"] for c in cs})
            print(f"\n  stored drop_number={dn}  closed_at={dt}  "
                  f"total=${total:.2f}  buyers={len(buyers)}")
            for c in cs:
                paid = "PAID" if c["confirmed"] else "unpaid"
                print(f"     {c['user_name']:<24} {c['item_display']} "
                      f"x{c['qty']} — ${float(c['subtotal']):.2f} [{paid}]")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
