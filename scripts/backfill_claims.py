#!/usr/bin/env python3
"""
Restore missing per-buyer order detail (user_claims) for the early drops of
UCE'S PLUG CHAT, and blank out one duplicate drop.

Background
----------
Every drop's *totals* already exist in drop_history (numbers/dates/revenue are
correct), but the per-buyer detail in user_claims was only saved for the later
drops. A coverage check showed drops 1, 2, 3, 5, 6 are missing their detail
(drop 8 is a genuine empty drop; everything else is complete).

Drops #4 and #5 are the SAME Prismatic drop recorded twice, so total revenue is
overcounting by $900. Per the owner, we blank the duplicate (#4) to $0/empty —
just like the existing empty drop #8 — which fixes the overcount without
renumbering anything. Drop #5 keeps the real detail.

What this does
--------------
  • Inserts the missing user_claims rows for drops 1, 2, 3, 5, 6, matching each
    drop by position (drop N = the Nth drop by close date = dashboard Drop #N),
    verifying the data adds up to that drop's stored revenue/items/buyers first.
  • Blanks drop_history position 4 (total_revenue/items/buyers → 0), but only
    after confirming positions 4 AND 5 are both the $900 twins (so it can't
    zero the wrong drop).
  • Reuses each buyer's real Discord id from their later orders; unknown buyers
    get a stable placeholder id.
  • Idempotent: a drop that already has detail is skipped; an already-blanked
    #4 is left alone. drop_history is otherwise untouched, so numbering holds.

Safety
------
Dry-run by default — prints the full plan and writes nothing. Re-run with
APPLY=1 to actually make the changes (all in one transaction).

Run
---
    #   $env:DATABASE_URL = "postgresql://...proxy.rlwy.net:PORT/railway"
    #   python backfill_claims.py                      # dry-run
    #   $env:APPLY = "1"; python backfill_claims.py     # write
"""

import asyncio
import hashlib
import os
import sys

import asyncpg

GUILD_ID = int(os.getenv("GUILD_ID") or 1492850792175108149)  # UCE'S PLUG CHAT
APPLY = os.getenv("APPLY", "").strip() in ("1", "true", "yes")
# Restored orders are stored as paid (per the store owner). PAID=0 for unpaid.
PAID = os.getenv("PAID", "1").strip() in ("1", "true", "yes")

# Blank this position (the duplicate Prismatic drop, a clone of #5). Guarded:
# only blanks if positions 4 and 5 are both ~$900, so it can't hit the wrong row.
BLANK_POSITION = 4
BLANK_EXPECT = 900.00

# ── The drops to restore ──────────────────────────────────────────────────────
# DROPS[drop_number] = { buyer_name: [ (item_display, qty, subtotal), ... ] }
# unit price is derived as subtotal / qty; totals are verified at run time.
DROPS = {
    1: {
        "PokeBowlTrainer": [
            ("AH PIN COLLECTION", 3, 120.00),
            ("AH BOOSTER BUNDLE", 1, 55.00),
        ],
        "NASTii.Pulls": [
            ("AH PIN COLLECTION", 3, 120.00),
            ("AH 2 PACK BLISTER", 4, 72.00),
            ("AH BOOSTER BUNDLE", 2, 110.00),
        ],
        "jordan | sari-sari tcg": [
            ("AH PIN COLLECTION", 3, 120.00),
            ("AH 2 PACK BLISTER", 4, 72.00),
        ],
        "@reflexsolegy": [("AH PIN COLLECTION", 3, 120.00)],
        "Vany510": [
            ("AH PIN COLLECTION", 3, 120.00),
            ("AH BOOSTER BUNDLE", 2, 110.00),
        ],
        "TonyTone187": [
            ("AH PIN COLLECTION", 3, 120.00),
            ("AH BOOSTER BUNDLE", 1, 55.00),
        ],
        "Vault & Pine Collective": [("AH PIN COLLECTION", 2, 80.00)],
        "britt": [
            ("AH PIN COLLECTION", 3, 120.00),
            ("AH 2 PACK BLISTER", 4, 72.00),
        ],
        "Antz": [("AH 2 PACK BLISTER", 2, 36.00)],
    },
    2: {
        "Michael Scarn": [("LOST ORIGIN ETB", 3, 375.00)],
        "PNW_Pearce": [
            ("PHANTASMAL FLAMES ETB", 1, 90.00),
            ("DESTINED RIVALS BOOSTER BUNDLE", 2, 100.00),
        ],
        "NASTii.Pulls": [("PHANTASMAL FLAMES ETB", 2, 180.00)],
        "Antz": [("PHANTASMAL FLAMES ETB", 1, 90.00)],
        "Vault & Pine Collective": [("DESTINED RIVALS BOOSTER BUNDLE", 1, 50.00)],
        "Vany510": [("DESTINED RIVALS BOOSTER BUNDLE", 1, 50.00)],
    },
    3: {
        "Michael Scarn": [("AH POSTER COLLECTION SEALED CASE", 1, 700.00)],
        "PokeBowlTrainer": [
            ("AH PIN COLLECTION SEALED CASE", 1, 250.00),
            ("AH 2 PACK BLISTERS SEALED CASE", 1, 425.00),
        ],
        "Vault & Pine Collective": [("AH PIN COLLECTION SEALED CASE", 1, 250.00)],
    },
    5: {
        "Sam": [("PRISMATIC SPC", 2, 450.00)],
        "SneakyP": [("PRISMATIC SPC", 1, 225.00)],
        "Vault & Pine Collective": [("PRISMATIC SPC", 1, 225.00)],
    },
    6: {
        "Michael Scarn": [("PRISMATIC SPC", 4, 900.00)],
    },
}


def _synthetic_id(name: str) -> int:
    """Stable negative placeholder id for a buyer with no known Discord id."""
    return -int(hashlib.sha1(name.strip().lower().encode()).hexdigest()[:15], 16)


async def main():
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("❌  DATABASE_URL is not set. Set your PUBLIC Railway url and re-run.")
        sys.exit(1)

    conn = await asyncpg.connect(dsn, timeout=30)
    try:
        idmap = {}
        for r in await conn.fetch(
                "SELECT DISTINCT ON (lower(user_name)) user_name, user_id "
                "FROM user_claims WHERE guild_id = $1 "
                "ORDER BY lower(user_name), drop_number DESC", GUILD_ID):
            idmap[r["user_name"].strip().lower()] = r["user_id"]

        print(f"Guild {GUILD_ID} — {'APPLY (writing)' if APPLY else 'DRY-RUN (no writes)'}"
              f", orders marked {'PAID' if PAID else 'unpaid'}\n")

        planned = []
        unmatched_names = set()

        for dn in sorted(DROPS):
            drop = DROPS[dn]
            dh = await conn.fetchrow(
                "SELECT closed_at, total_revenue, total_items, unique_buyers "
                "FROM drop_history WHERE guild_id = $1 "
                "ORDER BY closed_at OFFSET $2 LIMIT 1", GUILD_ID, dn - 1)
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

            print(f"Drop {dn}  ({dh['closed_at']}):  "
                  f"{'✅ matches' if ok else '❌ MISMATCH'}")
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
                    planned.append((dn, dh["closed_at"], uid, name,
                                    item, qty, round(sub / qty, 2), sub))
            print(f"    → will insert {sum(len(v) for v in drop.values())} row(s).")

        # ── Blank the duplicate drop #4 ───────────────────────────────────────
        blank_id = None
        p4 = await conn.fetchrow(
            "SELECT id, closed_at, total_revenue FROM drop_history "
            "WHERE guild_id = $1 ORDER BY closed_at OFFSET $2 LIMIT 1",
            GUILD_ID, BLANK_POSITION - 1)
        p5 = await conn.fetchrow(
            "SELECT total_revenue FROM drop_history "
            "WHERE guild_id = $1 ORDER BY closed_at OFFSET $2 LIMIT 1",
            GUILD_ID, BLANK_POSITION)
        print(f"\nDuplicate #{BLANK_POSITION}:")
        if not p4 or not p5:
            print("    ❌ could not read positions 4/5 — skipping blank.")
        elif abs(float(p4["total_revenue"])) < 0.01:
            print("    ⏭  position 4 is already blanked — nothing to do.")
        elif not (abs(float(p4["total_revenue"]) - BLANK_EXPECT) < 0.01
                  and abs(float(p5["total_revenue"]) - BLANK_EXPECT) < 0.01):
            print(f"    ⚠  positions 4/5 are not both ${BLANK_EXPECT:.2f} "
                  f"(saw ${float(p4['total_revenue']):.2f} / "
                  f"${float(p5['total_revenue']):.2f}) — skipping blank to be safe.")
        else:
            blank_id = p4["id"]
            print(f"    → will blank position 4 ({p4['closed_at']}, "
                  f"${float(p4['total_revenue']):.2f}) to $0/empty.")

        print(f"\nTotal order rows to insert: {len(planned)}"
              f"{'   + blank 1 drop' if blank_id else ''}")
        if unmatched_names:
            print("\n⚠  No Discord id on file for these buyers → stable placeholder id "
                  "(records show fine; just won't link to their Discord !myhistory):")
            for n in sorted(unmatched_names):
                print(f"     - {n}")

        if not APPLY:
            print("\nDRY-RUN only — nothing written. Re-run with APPLY=1 to apply.")
            return

        async with conn.transaction():
            for dn, closed_at, uid, name, item, qty, price, sub in planned:
                await conn.execute(
                    """INSERT INTO user_claims
                           (guild_id, user_id, user_name, drop_number, closed_at,
                            item_display, qty, price, subtotal, confirmed)
                       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)""",
                    GUILD_ID, uid, name, dn, closed_at, item, qty, price, sub, PAID)
            if blank_id:
                await conn.execute(
                    """UPDATE drop_history
                       SET total_revenue = 0, total_items = 0, unique_buyers = 0,
                           summary = '{}'::jsonb
                       WHERE id = $1""", blank_id)
        print(f"\n✅  Inserted {len(planned)} order row(s)"
              f"{' and blanked duplicate #4' if blank_id else ''}. "
              f"Re-run coverage.py to confirm every drop is complete.")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
