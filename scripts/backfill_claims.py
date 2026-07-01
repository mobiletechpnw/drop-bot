#!/usr/bin/env python3
"""
Restore missing per-buyer order detail (user_claims) for early drops.

Context
-------
For UCE'S PLUG CHAT the drop *totals* already exist in drop_history for every
drop (numbers/dates/revenue are correct), but the per-buyer detail in
user_claims was only ever saved for the later drops. So the early drops show a
revenue total on the dashboard but no orders when you open them. This script
fills in ONLY the missing user_claims rows. It does NOT touch drop_history, so
the existing drop numbering and totals stay exactly as they are.

For each drop it:
  • finds the matching drop_history row by position (drop N = the Nth drop by
    close date = the dashboard's Drop #N),
  • VERIFIES the buyer/item data below adds up to that row's revenue, item
    count and buyer count — and skips the drop if it doesn't (guards against a
    wrong mapping),
  • reuses each buyer's real Discord user id from their orders in later drops,
  • is idempotent: a drop that already has user_claims rows is left alone.

Safety
------
Dry-run by default: it prints the full plan and verification and writes nothing.
Re-run with APPLY=1 to actually insert.

Run
---
    # PowerShell:
    #   $env:DATABASE_URL = "postgresql://...proxy.rlwy.net:PORT/railway"
    #   python backfill_claims.py            # dry-run, shows the plan
    #   $env:APPLY = "1"; python backfill_claims.py   # actually write
"""

import asyncio
import hashlib
import os
import sys

import asyncpg

GUILD_ID = int(os.getenv("GUILD_ID") or 1492850792175108149)  # UCE'S PLUG CHAT
APPLY = os.getenv("APPLY", "").strip() in ("1", "true", "yes")
# Store these restored orders as paid (per the store owner). Set PAID=0 to
# insert them as unpaid instead.
PAID = os.getenv("PAID", "1").strip() in ("1", "true", "yes")

# ── The drops to restore ──────────────────────────────────────────────────────
# DROPS[drop_number] = { buyer_name: [ (item_display, qty, subtotal), ... ] }
# price-per-unit is derived as subtotal / qty. Verified against drop_history
# totals at run time, so a typo shows up as a MISMATCH rather than bad data.
DROPS = {
    7: {
        "PokeBowlTrainer": [
            ("MEGA CHARIZARD UPC", 4, 700.00),
            ("AH PIN COLLECTION", 3, 120.00),
            ("AH BOOSTER BUNDLE", 2, 110.00),
        ],
        "Vany510": [
            ("AH PIN COLLECTION", 3, 120.00),
            ("AH BOOSTER BUNDLE", 2, 110.00),
        ],
        "Alota.soles": [("AH PIN COLLECTION", 3, 120.00)],
        "NASTii.Pulls": [
            ("AH PIN COLLECTION", 3, 120.00),
            ("AH BOOSTER BUNDLE", 2, 110.00),
        ],
        "Vault & Pine Collective": [("AH BOOSTER BUNDLE", 2, 110.00)],
        "Michael Scarn": [("AH BOOSTER BUNDLE", 2, 110.00)],
    },
    9: {
        "Vault & Pine Collective": [("CHAOS RISING ETB", 3, 210.00)],
        "Sam": [("CHAOS RISING ETB", 3, 210.00)],
        "Vany510": [("CHAOS RISING ETB", 3, 210.00)],
        "Allamas46": [("CHAOS RISING ETB", 1, 70.00)],
        "PNW_Pearce": [("CHAOS RISING ETB", 2, 140.00)],
        "dealuh": [("CHAOS RISING ETB", 1, 70.00)],
        "napua808": [("CHAOS RISING ETB", 1, 70.00)],
    },
    10: {
        "Vault & Pine Collective": [("CHAOS RISING ETB TORN SHRINK", 2, 100.00)],
        "dealuh": [("CHAOS RISING ETB TORN SHRINK", 1, 50.00)],
        "Allamas46": [("CHAOS RISING ETB TORN SHRINK", 1, 50.00)],
    },
    11: {
        "Sam": [("PITCH BLACK ETB", 3, 195.00)],
        "Vault & Pine Collective": [
            ("PITCH BLACK ETB", 3, 195.00),
            ("PITCH BLACK BOOSTER BUNDLE", 2, 90.00),
        ],
        "Vany510": [("PITCH BLACK ETB", 3, 195.00)],
        "dealuh": [("PITCH BLACK ETB", 2, 130.00)],
        "PNW_Pearce": [
            ("PITCH BLACK ETB", 2, 130.00),
            ("PITCH BLACK BOOSTER BUNDLE", 2, 90.00),
        ],
        "Ka$hMoneyyy": [("PITCH BLACK ETB", 3, 195.00)],
        "brit | sari-sari tcg": [
            ("PITCH BLACK ETB", 3, 195.00),
            ("PITCH BLACK BOOSTER BUNDLE", 2, 90.00),
        ],
        "Allamas46": [("PITCH BLACK ETB", 2, 130.00)],
        "PokeBowlTrainer": [
            ("PITCH BLACK ETB", 3, 195.00),
            ("PITCH BLACK BOOSTER BUNDLE", 2, 90.00),
        ],
        "Antz": [
            ("PITCH BLACK ETB", 3, 195.00),
            ("PITCH BLACK BOOSTER BUNDLE", 2, 90.00),
        ],
        "MAUIIIIIIIIIIIII": [
            ("PITCH BLACK ETB", 2, 130.00),
            ("PITCH BLACK BOOSTER BUNDLE", 2, 90.00),
        ],
        "TonyTone187": [
            ("PITCH BLACK ETB", 1, 65.00),
            ("PITCH BLACK BOOSTER BUNDLE", 1, 45.00),
        ],
        "@reflexsolegy": [("PITCH BLACK BOOSTER BUNDLE", 2, 90.00)],
        "Itzjusdom": [("PITCH BLACK BOOSTER BUNDLE", 2, 90.00)],
    },
    12: {
        "dealuh": [("CHAOS RISING BOOSTER BUNDLE", 2, 80.00)],
    },
}


def _synthetic_id(name: str) -> int:
    """Stable placeholder id for a buyer with no known Discord id.

    Negative so it can never collide with a real Discord snowflake, and
    deterministic so the same name maps to the same id across drops.
    """
    h = int(hashlib.sha1(name.strip().lower().encode()).hexdigest()[:15], 16)
    return -h


async def main():
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("❌  DATABASE_URL is not set. Set your PUBLIC Railway url and re-run.")
        sys.exit(1)

    conn = await asyncpg.connect(dsn, timeout=30)
    try:
        # name -> real Discord id, taken from every existing order for this guild.
        idmap = {}
        for r in await conn.fetch(
                "SELECT DISTINCT ON (lower(user_name)) user_name, user_id "
                "FROM user_claims WHERE guild_id = $1 "
                "ORDER BY lower(user_name), drop_number DESC", GUILD_ID):
            idmap[r["user_name"].strip().lower()] = r["user_id"]

        print(f"Guild {GUILD_ID} — {'APPLY (writing)' if APPLY else 'DRY-RUN (no writes)'}"
              f", orders marked {'PAID' if PAID else 'unpaid'}\n")

        planned = []          # rows to insert: (dn, closed_at, uid, name, item, qty, price, sub)
        unmatched_names = set()

        for dn in sorted(DROPS):
            drop = DROPS[dn]
            # The Nth drop by close date is the dashboard's Drop #N.
            dh = await conn.fetchrow(
                "SELECT closed_at, total_revenue, total_items, unique_buyers "
                "FROM drop_history WHERE guild_id = $1 "
                "ORDER BY closed_at OFFSET $2 LIMIT 1",
                GUILD_ID, dn - 1)
            if not dh:
                print(f"Drop {dn}: ❌ no drop_history row at position {dn} — skipping.")
                continue

            my_rev = sum(sub for lines in drop.values() for _, _, sub in lines)
            my_items = sum(q for lines in drop.values() for _, q, _ in lines)
            my_buyers = len(drop)
            ok = (abs(float(dh["total_revenue"]) - my_rev) < 0.01
                  and dh["total_items"] == my_items
                  and dh["unique_buyers"] == my_buyers)

            existing = await conn.fetchval(
                "SELECT COUNT(*) FROM user_claims "
                "WHERE guild_id = $1 AND drop_number = $2", GUILD_ID, dn)

            status = "✅ matches" if ok else "❌ MISMATCH"
            print(f"Drop {dn}  ({dh['closed_at']}):  {status}")
            print(f"    dashboard: rev=${float(dh['total_revenue']):.2f} "
                  f"items={dh['total_items']} buyers={dh['unique_buyers']}")
            print(f"    my data:   rev=${my_rev:.2f} items={my_items} buyers={my_buyers}")
            if existing:
                print(f"    ⏭  already has {existing} user_claims row(s) — skipping.")
                continue
            if not ok:
                print("    ⚠  totals don't match — skipping (won't insert bad data).")
                continue

            for name, lines in drop.items():
                key = name.strip().lower()
                uid = idmap.get(key)
                if uid is None:
                    uid = _synthetic_id(name)
                    unmatched_names.add(name)
                for item, qty, sub in lines:
                    price = round(sub / qty, 2)
                    planned.append((dn, dh["closed_at"], uid, name,
                                    item, qty, price, sub))
            print(f"    → will insert {sum(len(v) for v in drop.values())} row(s).")

        print(f"\nTotal rows to insert: {len(planned)}")
        if unmatched_names:
            print("\n⚠  These buyers had no Discord id in existing orders, so a stable "
                  "placeholder id was generated (their dashboard records are fine; the "
                  "only limit is these won't link to their Discord !myhistory):")
            for n in sorted(unmatched_names):
                print(f"     - {n}")

        if not APPLY:
            print("\nDRY-RUN only — nothing was written. "
                  "Re-run with APPLY=1 to insert.")
            return

        async with conn.transaction():
            for dn, closed_at, uid, name, item, qty, price, sub in planned:
                await conn.execute(
                    """INSERT INTO user_claims
                           (guild_id, user_id, user_name, drop_number, closed_at,
                            item_display, qty, price, subtotal, confirmed)
                       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)""",
                    GUILD_ID, uid, name, dn, closed_at, item, qty, price, sub, PAID)
        print(f"\n✅  Inserted {len(planned)} order row(s). Open the drops on the "
              f"dashboard to confirm the buyer lists now show.")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
