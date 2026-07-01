-- Backfill Drop #12 for UCE'S PLUG CHAT into the dashboard database.
--
-- Run this in Railway → Postgres service → the "Data"/"Query" tab (or any
-- psql session against the same database). It does exactly what the bot's
-- close_drop() does: writes one drop_history row and one user_claims row so
-- the drop shows on the web dashboard like a normally-closed one.
--
--   Drop #12 — UCE'S PLUG CHAT
--   dealuh — CHAOS RISING BOOSTER BUNDLE x2 — $80.00  (5/23/26, 3:19 PM)
--   qty 2 @ $40.00 = $80.00
--
-- Safe to run twice: it detects if the drop is already saved and does nothing.
-- If dealuh already PAID, change  FALSE  to  TRUE  on the confirmed line below.

DO $$
DECLARE
    v_guild  BIGINT;
    v_user   BIGINT;
    v_pos    INT;
    v_closed TIMESTAMP := TIMESTAMP '2026-05-23 15:19:00';  -- 5/23/26, 3:19 PM
BEGIN
    -- Resolve the guild by name.
    SELECT guild_id INTO v_guild
      FROM server_settings
     WHERE guild_name ILIKE 'UCE''S PLUG CHAT'
     LIMIT 1;
    IF v_guild IS NULL THEN
        RAISE EXCEPTION 'Guild "UCE''S PLUG CHAT" not found in server_settings. '
                        'Set v_guild manually to the correct guild_id.';
    END IF;

    -- Reuse dealuh's Discord user id from their existing order history.
    SELECT user_id INTO v_user
      FROM user_claims
     WHERE guild_id = v_guild AND user_name ILIKE 'dealuh'
     ORDER BY drop_number DESC
     LIMIT 1;
    IF v_user IS NULL THEN
        RAISE EXCEPTION 'No prior orders for "dealuh"; their Discord user id is '
                        'unknown. Set v_user manually to dealuh''s Discord id.';
    END IF;

    -- Idempotency: bail if this drop is already saved.
    IF EXISTS (
        SELECT 1 FROM user_claims
         WHERE guild_id = v_guild AND user_id = v_user
           AND closed_at = v_closed
           AND item_display = 'CHAOS RISING BOOSTER BUNDLE'
    ) THEN
        RAISE NOTICE 'Drop #12 already saved — nothing to do.';
        RETURN;
    END IF;

    -- The dashboard numbers drops by closed_at order; user_claims stores that
    -- number. Compute the position this drop lands at.
    SELECT COUNT(*) + 1 INTO v_pos
      FROM drop_history
     WHERE guild_id = v_guild AND closed_at <= v_closed;

    INSERT INTO drop_history
        (guild_id, closed_at, total_revenue, total_items, unique_buyers, summary)
    VALUES
        (v_guild, v_closed, 80.00, 2, 1,
         '{"CHAOS RISING BOOSTER BUNDLE": {"qty": 2, "revenue": 80.0}}'::jsonb);

    INSERT INTO user_claims
        (guild_id, user_id, user_name, drop_number, closed_at,
         item_display, qty, price, subtotal, confirmed)
    VALUES
        (v_guild, v_user, 'dealuh', v_pos, v_closed,
         'CHAOS RISING BOOSTER BUNDLE', 2, 40.00, 80.00, FALSE);  -- FALSE = unpaid

    RAISE NOTICE 'Saved drop #% for guild % (buyer %).', v_pos, v_guild, v_user;
END $$;
