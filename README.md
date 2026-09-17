# drop-bot

A Discord bot for running product "drops": stock, first-come claims, payments,
tracking, raffles, and per-user order history — backed by PostgreSQL.

## Components

- **`drop_bot.py`** — the Discord bot (`worker` process).
- **`webapp.py`** — an optional web dashboard for managing records (payments,
  managers, drop history, orders, shipping tracking) from a browser, with
  per-drop Excel/CSV exports. See [WEB.md](WEB.md).

## Exporting a drop

**One drop.** From Discord (managers): `!export` for an Excel workbook,
`!export csv` for a CSV, `!export list` for a printable text list of every
buyer's claims, or `!claimlist full` for that same full list during a drop —
the `!claimlist` embed trims once a drop gets busy, the file never does. From
the dashboard: **Excel** / **CSV** on any closed drop.

**Every claim, grouped by buyer.** `!claims` DMs the whole roster: one section
per buyer with every claim they have ever made, across every drop, including
the one still running. `!claims csv` for a spreadsheet, `!claims @user` for a
single buyer, `!claims 13` for a single drop. The dashboard has the same thing
under **Buyers**, with Excel and CSV exports.

## Backing up the data

The database is the only copy of drop history, claims, payments, tracking,
raffles and settings — nothing in this repo recreates it. Take a snapshot
before anything risky (a deploy, a migration, a merge):

```bash
DATABASE_URL=postgres://user:pass@host:5432/db python scripts/backup_db.py
# on Railway: copy DATABASE_URL from the Postgres service's Variables tab,
# or run it through the CLI:  railway run python scripts/backup_db.py
```

That writes `backups/dropbot-<timestamp>/` with every table as JSON (full
fidelity) and CSV (for Excel), plus `restore.sql` and a `manifest.json` of row
counts. To restore: make sure the tables exist (starting the bot creates them),
then `psql "$DATABASE_URL" -f backups/<folder>/restore.sql`. The inserts are
`ON CONFLICT DO NOTHING`, so a restore never overwrites rows that are already
there — delete a table's rows first if you want a true roll-back — and it
fast-forwards the `SERIAL` sequences so later inserts don't collide.

`backups/` is git-ignored: a snapshot contains `server_settings.web_access_key`
(the dashboard login) and buyers' payment handles. Keep it private.

## Running the bot

```bash
pip install -r requirements.txt
export BOT_TOKEN=...            # Discord bot token
export DATABASE_URL=postgres://user:pass@host:5432/dbname
export CREATOR_ID=...          # optional: your Discord user ID (super admin)
python -u drop_bot.py
```

## Web dashboard

Optional, shares the same database. In Discord run `!webkey` to get a login
key, then start the dashboard (`uvicorn webapp:app`). Full setup and Railway
deployment instructions are in [WEB.md](WEB.md).
