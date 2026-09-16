# drop-bot

A Discord bot for running product "drops": stock, first-come claims, payments,
tracking, raffles, and per-user order history — backed by PostgreSQL.

## Components

- **`drop_bot.py`** — the Discord bot (`worker` process).
- **`webapp.py`** — an optional web dashboard for managing records (payments,
  managers, drop history, orders, shipping tracking) from a browser, with
  per-drop Excel/CSV exports. See [WEB.md](WEB.md).

## Exporting a drop

From Discord (managers): `!export` for an Excel workbook, `!export csv` for a
CSV, `!export list` for a printable text list of every buyer's claims, or
`!claimlist full` for that same full list during a drop — the `!claimlist`
embed trims once a drop gets busy, the file never does. From the dashboard:
**Excel** / **CSV** on any closed drop.

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
