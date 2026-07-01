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
- Add / edit **tracking numbers** per buyer per drop (permanent, in history)
- Search all orders by buyer name or Discord user ID
- Download a per-drop Excel export

**Can't (by design):** run a *live* drop — staging stock, taking claims, and
closing a drop still happen in Discord, because that state lives in the bot's
memory until the drop closes. Config you change here (payments, managers,
channels) is picked up by the running bot within ~60 seconds via its periodic
config refresh.

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

## Deploying on Railway (two services, one database)

The repo's `Procfile` defines two process types:

```
worker: python -u drop_bot.py                                   # the Discord bot
web:    uvicorn webapp:app --host 0.0.0.0 --port $PORT          # the dashboard
```

1. Keep your existing service running the **bot** (`worker`).
2. Add a **second service** in the same Railway project from the same repo, and
   set its start command to:
   `uvicorn webapp:app --host 0.0.0.0 --port $PORT`
3. Give the web service the shared Postgres variables (reference the same
   Postgres plugin) plus `WEB_SECRET`. Optionally set `WEB_BASE_URL` on the bot
   service to the web service's public URL so `!webkey` links straight to login.
4. Expose the web service publicly (Railway → Settings → Generate Domain).

Both services share the one database, so records stay in sync. The bot picks up
web config changes within ~60s; tracking edits are visible to the bot
immediately (it reads tracking straight from the database).

## Security notes

- The access key is a bearer credential — anyone with it can manage that
  server's records. Share it privately and rotate with `!webkey reset`.
- Session cookies are signed with `WEB_SECRET`; keep it secret and stable.
- The dashboard scopes every query to the signed-in server, so one server's key
  never exposes another server's data.
