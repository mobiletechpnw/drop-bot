# Drop Bot — Web Dashboard

A browser interface for managing the **records** side of a Drop Bot server
without Discord: payment methods, managers, channels, drop history, orders, and
shipping tracking. It reads and writes the **same PostgreSQL database** as the
bot.

## What it can and can't do

**Can (all DB-backed):**
- Edit payment methods (Venmo / Zelle / Cash App / Apple Pay) and channel IDs
- Add / remove managers by Discord user ID
- Browse every closed drop with revenue, item, and buyer stats
- View each drop's per-buyer orders, payment status, and tracking
- Mark a past-drop order **paid / unpaid**, filter a drop to **unpaid only**,
  or **mark every unpaid order paid** in one click (updates saved history)
- Add / edit **tracking numbers** per buyer per drop (permanent, in history) —
  the buyer gets DM'd on Discord, same as `!addtracking` (see below)
- **Watch a live drop and mark buyers paid / unpaid on it** — the in-progress
  drop mirrors to the **Live** page while it's running (see below)
- Search all orders by buyer name or Discord user ID
- Download a per-drop Excel export

**Can't (by design):** *stage* a drop — loading stock, taking claims, going
live, and closing still happen in Discord, because that live state lives in the
bot's memory. (You *can* now mark payments on a live drop from the dashboard;
see "Marking a live drop" below.) Config you change here (payments, managers,
channels) is picked up by the running bot within ~60 seconds via its periodic
config refresh.

## Discord notifications from the web dashboard

The web dashboard and the bot are separate processes that only share the
database — the dashboard has no direct connection to Discord. So actions that
need to message a buyer (currently: **adding/changing a tracking number**)
work via a small outbox: the dashboard writes a row to a
`pending_notifications` table, and the bot polls that table every ~15 seconds
and delivers the DM. In practice a tracking number saved on the web reaches
the buyer's DMs within about 15–30 seconds — same message text as `!addtracking`.
If the buyer has DMs closed, the notification is marked delivered (attempted)
rather than retried forever; the tracking number itself is still saved and
visible in `!myhistory` and the dashboard either way. Re-saving the same
tracking value, or clearing it, does **not** send a DM — only a genuinely new
value does.

**Pushing DMs for tracking already saved:** because a single save only DMs on a
*new* value, tracking numbers entered before this notify feature existed — or
any a buyer never received — leave no per-buyer way to reach them. A drop's page
has a **📦 Push tracking DMs (N)** button that enqueues the tracking DM for every
buyer in that drop who has a saved tracking number, using the same outbox. Use
it to (re)deliver tracking for a whole drop at once; buyers get the same message
as `!addtracking`.

## Paid / unpaid status — how it syncs with Discord

Marking an order **paid** or **unpaid** on the dashboard and doing it in Discord
both read and write the **same database column** (`user_claims.confirmed`), so
the two stay in sync automatically — there is no separate copy to drift:

- Dashboard **Mark paid** / **Undo** (and **Mark all paid**) set that column
  `TRUE` / `FALSE` for the buyer's rows in that drop.
- On Discord, `!myhistory` reads the column **fresh from the database every
  time**, so a dashboard change shows up the next time it's run. Excel exports
  read the same column.
- It works the other way too: `!markpaid` / `!confirm` in Discord write the
  same column the dashboard reads.

Drop numbers line up on both sides (both are 1-based by close order), so
"Drop #13" on the dashboard is the buyer's Drop #13.

**This applies to *closed* drops only** — which is all the dashboard shows. A
drop that is **currently live** is not in the database yet: its claims and
payment status live in the bot's memory until the drop closes (`!enddrop`), at
which point the final paid status is snapshotted into `user_claims`. So while a
drop is live, confirm payments in Discord (`!markpaid` / `!confirm` / `!paid`);
the dashboard has nothing to edit for that drop until it closes, and then it
stays in sync as described above.

## Marking a live drop from the dashboard

A drop that's currently live isn't in the database — its claims and payment
status live in the bot's memory until it closes. To let the dashboard show and
act on a live drop anyway, the bot mirrors it to the database and reads back an
action outbox (the same split as the tracking notifications, in both
directions):

- **Visibility:** while a drop is live, the bot writes each buyer's items,
  total, and confirmed amount to a `live_orders` table (and a `live_drops` flag)
  every time the live boards update — roughly every couple of seconds. The
  dashboard's **Live** page reads that. When no drop is live the mirror is
  cleared, so the page shows "no drop is live".
- **Write-back:** clicking **Mark paid** / **Undo** on the Live page writes a
  row to a `pending_actions` table. The bot polls it every ~15s and applies it
  to its in-memory payments, exactly like a manager running `!confirm` /
  reversing it in Discord — the live claim list and payment board update, and
  the buyer gets the same confirmation DM. **Mark paid** confirms any payment
  the buyer already reported with `!paid` and tops up the rest so their order is
  fully covered; **Undo** removes that top-up and un-confirms the reported
  payments. So the change is reflected on Discord within ~15–30 seconds; hit
  **Refresh** on the Live page to see it land back.

When the drop **closes**, its final paid status is snapshotted into
`user_claims` (the normal history) and the live mirror is cleared — from then on
it's a closed drop you edit under **Drops**, and paid status stays in sync as
described above. An action queued a moment before the drop closed is skipped
rather than applied to the wrong drop; just mark it on the closed drop instead.

## Signing in

Auth is a **per-server access key**. In Discord, a manager runs:

```
!webkey          → DMs the current key (creates one if missing)
!webkey reset    → generates a new key and invalidates the old one
```

Paste that key on the dashboard's login page. The key maps to exactly one
server, so no server picker is needed. Treat it like a password.

## Running locally

```bash
pip install -r requirements.txt
export DATABASE_URL=postgres://user:pass@host:5432/dbname   # same DB as the bot
export WEB_SECRET=$(python -c "import secrets;print(secrets.token_urlsafe(32))")
export WEB_SECURE_COOKIES=false     # allow http during local dev
uvicorn webapp:app --host 0.0.0.0 --port 8000
```

Open http://localhost:8000 .

## Environment variables

| Variable              | Used by | Required | Notes |
|-----------------------|---------|----------|-------|
| `DATABASE_URL`        | both    | yes      | Shared Postgres connection string |
| `WEB_SECRET`          | web     | prod yes | Signs session cookies; set a stable value so logins survive restarts |
| `WEB_SECURE_COOKIES`  | web     | no       | `true` (default) sets the Secure flag — keep on for HTTPS; set `false` for local http |
| `WEB_BASE_URL`        | bot     | no       | If set, `!webkey` includes a direct login link, e.g. `https://drops.example.com` |
| `CREATOR_WEB_KEY`     | web     | no       | Master key for the bot creator to oversee **all** servers. Leave unset to disable creator login. |
| `PORT`                | web     | host     | Provided by Railway/host |

## Creator oversight (all servers)

Set `CREATOR_WEB_KEY` on the web service to a long secret of your choosing, e.g.:

```bash
export CREATOR_WEB_KEY=$(python -c "import secrets;print(secrets.token_urlsafe(32))")
```

Paste that key on the same login page. Instead of a single store you land on
**All servers** (`/admin`): totals across every store plus a per-server table
(drops, revenue, outstanding, last drop). Click **Manage** on any server to
drop into it — every normal page (settings, orders, tracking, confirm, export)
then acts on that store, with a banner and a "back to all servers" link. This
key is the master credential; keep it very private and rotate it by changing
the env var.

**Servers the bot has left:** the all-servers list shows *every* server with
any historical data, including ones the bot is no longer a member of (kicked,
removed, etc.) — their records aren't deleted. Those rows are dimmed and
tagged **"⚠️ Bot no longer in this server"** so it's clear they can be viewed
for reporting but not managed live. This is tracked in a `bot_guilds` table
that the bot updates on join/leave and reconciles on every startup (so it
also catches guilds left while the bot was offline). Note `!creator servers`
in Discord only lists guilds the bot is *currently* in, so its count can be
lower than the web dashboard's — that's expected.

## Deploying on Railway (two services, one database)

This repo runs as **two services from the same codebase** — the Discord bot and
the web dashboard. Each service is driven by its own Railway config file so their
start commands stay independent:

- `railway.toml` → the **bot** (`python -u drop_bot.py`) — your existing service,
  unchanged.
- `railway.web.toml` → the **web dashboard**
  (`uvicorn webapp:app --host 0.0.0.0 --port $PORT`).

Steps:

1. Leave your existing **bot** service as-is (it already uses `railway.toml`).
2. **Add a second service** in the same Railway project from the same repo.
3. Point it at its config: web service → **Settings → Config-as-code /
   "Railway Config File"** → set to `railway.web.toml`.
4. Give the web service the shared Postgres variable (Variables → Add Reference →
   point `DATABASE_URL` at the Postgres plugin) plus `WEB_SECRET` and, for
   creator oversight, `CREATOR_WEB_KEY`. Keep `WEB_SECURE_COOKIES=true`.
5. Expose the web service publicly (Settings → Networking → Generate Domain).
6. Optionally set `WEB_BASE_URL` on the **bot** service to the web service's
   public URL so `!webkey` links straight to the login page.

> Why two config files? A `startCommand` in `railway.toml` takes precedence over
> a service's dashboard setting, so it would force *both* services to run the same
> process. Giving the web service its own config file avoids that and leaves the
> bot service completely untouched.

Generate the secrets with:

```bash
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

Both services share the one database, so records stay in sync. The bot picks up
web config changes within ~60s; tracking edits are visible to the bot
immediately (it reads tracking straight from the database).

## Security notes

- The access key is a bearer credential — anyone with it can manage that
  server's records. Share it privately and rotate with `!webkey reset`.
- Session cookies are signed with `WEB_SECRET`; keep it secret and stable.
- The dashboard scopes every query to the signed-in server, so one server's key
  never exposes another server's data.
