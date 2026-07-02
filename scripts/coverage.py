#!/usr/bin/env python3
"""
READ-ONLY coverage map: for every saved drop, show whether its per-buyer
detail (user_claims) exists. Writes nothing.

Tells us exactly which drops still need their buyer/item lists restored.

Run:
    #   $env:DATABASE_URL = "postgresql://...proxy.rlwy.net:PORT/railway"
    python coverage.py
"""

import asyncio
import os
import sys

import asyncpg

GUILD_ID = int(os.getenv("GUILD_ID") or 1492850792175108149)  # UCE'S PLUG CHAT


async def main():
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("❌  DATABASE_URL is not set. Set your PUBLIC Railway url and re-run.")
        sys.exit(1)

    conn = await asyncpg.connect(dsn, timeout=30)
    try:
        drops = await conn.fetch(
            """SELECT ROW_NUMBER() OVER (ORDER BY closed_at) AS pos,
                      closed_at, total_revenue, total_items, unique_buyers
               FROM drop_history WHERE guild_id = $1 ORDER BY closed_at""", GUILD_ID)
        counts = {r["drop_number"]: r for r in await conn.fetch(
            """SELECT drop_number, COUNT(*) AS rows,
                      COUNT(DISTINCT user_id) AS buyers,
                      COALESCE(SUM(subtotal),0) AS rev
               FROM user_claims WHERE guild_id = $1
               GROUP BY drop_number""", GUILD_ID)}

        print(f"Guild {GUILD_ID}\n")
        print("pos | close date          | dash rev  | detail rows | detail rev | status")
        print("----+---------------------+-----------+-------------+------------+-------------------")
        missing = []
        for d in drops:
            pos = d["pos"]
            c = counts.get(pos)
            rows = c["rows"] if c else 0
            drev = float(c["rev"]) if c else 0.0
            dashrev = float(d["total_revenue"])
            if dashrev == 0 and rows == 0:
                status = "empty drop (ok)"
            elif rows == 0:
                status = "❌ MISSING detail"
                missing.append(pos)
            elif abs(drev - dashrev) < 0.01:
                status = "✅ complete"
            else:
                status = f"⚠ detail rev ${drev:.2f} != dash ${dashrev:.2f}"
            print(f"{pos:>3} | {str(d['closed_at'])[:19]} | ${dashrev:>8.2f} | "
                  f"{rows:>11} | ${drev:>9.2f} | {status}")

        # Any stored drop_number that isn't a valid position (1..N)?
        valid = {d["pos"] for d in drops}
        orphans = sorted(k for k in counts if k not in valid)
        print(f"\nDrops MISSING buyer detail: {missing or 'none'}")
        if orphans:
            print(f"⚠ user_claims rows with an unexpected drop_number "
                  f"(not 1..{len(drops)}): {orphans}")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
