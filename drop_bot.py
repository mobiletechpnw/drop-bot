"""
Vault & Pine Drop Bot
=====================
Setup (first time in a new server):
  !setup                           — Register yourself as admin, set drop channel + payment info

Admin only:
  !addmanager @user                — Grant manager role
  !removemanager @user             — Revoke manager role
  !managers                        — List admin and managers
  !setpayment                      — Update payment info
  !setdropchannel #channel         — Update drop channel

Creator only (DM the bot):
  !creator servers                 — List all servers the bot is in
  !creator info <guild_id>         — See a server's settings, admin, and managers
  !creator setpayment <guild_id>   — Update payment info for a server
  !creator setdropchannel <guild_id> <#channel_id> — Update drop channel for a server
  !creator resetadmin <guild_id> @user — Reassign the admin for a server
  !creator announce <guild_id> <message> — Post an announcement in a server's drop channel

Manager/Admin commands (server or DM after !drop):
  !drop                            — Start a new drop session (must run in server)
  !addstock <item> <qty> <price> [limit <n>]
  !editstock <item> <qty> <price>
  !removestockitem <item>
  !preview
  !countdown <minutes>
  !release
  !autoclose on/off
  !claimlist
  !unpaid                          — List buyers who haven't been confirmed yet
  !confirm @user                   — Mark a buyer as fully paid
  !bump @user                      — DM a buyer a payment reminder
  !remind                          — Tag all unpaid buyers in the drop channel
  !announce <message>              — Post a formatted announcement in the drop channel
  !history                         — View last 10 drop summaries
  !enddrop

Public commands (anyone, in server only):
  !claim <item> <qty>
  !unclaim <item> <qty>
  !waitlist <item>
  !paid <method> <amount>          — works during and after drop
  !stock
  !myclaims

Raffle commands — owner only (slash commands):
  /raffle create <name> <spots> <price>  — Create a raffle with button UI
  /raffle confirm <name> @user           — Confirm a user's payment
  /raffle wheel <name> [force]           — Generate Wheel of Names link for live spin
  /raffle winner <name> @user            — Record the winner
  /raffle cancel <name>                  — Cancel and remove a raffle
  /raffle setchannel #channel            — Set the raffle channel (one-time setup)
  /raffle status <name>                  — Show current raffle state

Raffle commands — anyone (slash commands):
  /raffles                               — List all active raffles
  Spot claiming is button-based — no commands needed, just tap!
"""

import discord
from discord.ext import commands
from collections import defaultdict
import datetime
import asyncio
import json
import os
import random
from urllib.parse import quote as url_quote
import asyncpg

BOT_TOKEN    = os.environ.get("BOT_TOKEN", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
CREATOR_ID   = int(os.environ.get("CREATOR_ID", "0"))
PREFIX       = "!"

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.reactions = True
bot = commands.Bot(command_prefix=PREFIX, intents=intents, help_command=None)

# ── DATABASE ──────────────────────────────────────────────────────────────────

db_pool = None


async def init_db():
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL)
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS server_admins (
                guild_id BIGINT PRIMARY KEY,
                user_id BIGINT NOT NULL
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS server_managers (
                guild_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                PRIMARY KEY (guild_id, user_id)
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS server_settings (
                guild_id          BIGINT PRIMARY KEY,
                drop_channel_id   BIGINT,
                venmo             TEXT,
                zelle             TEXT,
                cashapp           TEXT,
                applepay          TEXT,
                raffle_channel_id BIGINT
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS drop_history (
                id            SERIAL PRIMARY KEY,
                guild_id      BIGINT NOT NULL,
                closed_at     TIMESTAMP NOT NULL,
                total_revenue NUMERIC NOT NULL,
                total_items   INT NOT NULL,
                unique_buyers INT NOT NULL,
                summary       JSONB NOT NULL
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS raffles (
                guild_id   BIGINT  NOT NULL,
                name       TEXT    NOT NULL,
                spots      INTEGER NOT NULL,
                price      TEXT    NOT NULL,
                channel_id BIGINT  NOT NULL,
                message_id BIGINT,
                status     TEXT    NOT NULL DEFAULT 'open',
                PRIMARY KEY (guild_id, name)
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS raffle_slots (
                guild_id    BIGINT  NOT NULL,
                raffle_name TEXT    NOT NULL,
                spot_num    INTEGER NOT NULL,
                user_id     BIGINT,
                username    TEXT,
                paid        BOOLEAN NOT NULL DEFAULT FALSE,
                PRIMARY KEY (guild_id, raffle_name, spot_num)
            )
        """)
        await conn.execute("""
            ALTER TABLE server_settings
            ADD COLUMN IF NOT EXISTS raffle_channel_id BIGINT
        """)
    print("✅  Database ready.")


async def db_load_all():
    global server_admins, server_managers, server_settings
    async with db_pool.acquire() as conn:
        admins = await conn.fetch("SELECT guild_id, user_id FROM server_admins")
        for row in admins:
            server_admins[row["guild_id"]] = row["user_id"]

        managers = await conn.fetch("SELECT guild_id, user_id FROM server_managers")
        for row in managers:
            server_managers[row["guild_id"]].add(row["user_id"])

        settings = await conn.fetch("SELECT * FROM server_settings")
        for row in settings:
            server_settings[row["guild_id"]] = {
                "drop_channel_id":   row["drop_channel_id"],
                "venmo":             row["venmo"],
                "zelle":             row["zelle"],
                "cashapp":           row["cashapp"],
                "applepay":          row["applepay"],
                "raffle_channel_id": row["raffle_channel_id"] if "raffle_channel_id" in row.keys() else None,
            }
            rc = row["raffle_channel_id"] if "raffle_channel_id" in row.keys() else None
            if rc:
                server_raffle_channel[row["guild_id"]] = rc

        # ── Load raffles ───────────────────────────────────────────────────────
        raffle_rows = await conn.fetch("SELECT * FROM raffles")
        for row in raffle_rows:
            gid  = row["guild_id"]
            name = row["name"]
            server_raffles[gid][name] = {
                "spots":      row["spots"],
                "price":      row["price"],
                "channel_id": row["channel_id"],
                "message_id": row["message_id"],
                "status":     row["status"],
                "slots":      {},
            }
            server_raffle_channel[gid] = row["channel_id"]

        slot_rows = await conn.fetch("SELECT * FROM raffle_slots")
        for row in slot_rows:
            gid  = row["guild_id"]
            name = row["raffle_name"]
            if gid in server_raffles and name in server_raffles[gid]:
                server_raffles[gid][name]["slots"][row["spot_num"]] = {
                    "user_id":  row["user_id"],
                    "username": row["username"],
                    "paid":     row["paid"],
                }

    print(f"✅  Loaded {len(server_admins)} server(s) from database.")


async def db_set_admin(guild_id, user_id):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO server_admins (guild_id, user_id)
            VALUES ($1, $2)
            ON CONFLICT (guild_id) DO UPDATE SET user_id = $2
        """, guild_id, user_id)


async def db_add_manager(guild_id, user_id):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO server_managers (guild_id, user_id)
            VALUES ($1, $2)
            ON CONFLICT DO NOTHING
        """, guild_id, user_id)


async def db_remove_manager(guild_id, user_id):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            DELETE FROM server_managers WHERE guild_id = $1 AND user_id = $2
        """, guild_id, user_id)


async def db_save_drop_history(guild_id, revenue, total_items, unique_buyers, summary):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO drop_history (guild_id, closed_at, total_revenue, total_items, unique_buyers, summary)
            VALUES ($1, $2, $3, $4, $5, $6)
        """, guild_id, datetime.datetime.utcnow(), revenue, total_items, unique_buyers, json.dumps(summary))


async def db_save_settings(guild_id):
    s = server_settings.get(guild_id, {})
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO server_settings (guild_id, drop_channel_id, venmo, zelle, cashapp, applepay, raffle_channel_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (guild_id) DO UPDATE SET
                drop_channel_id   = $2,
                venmo             = $3,
                zelle             = $4,
                cashapp           = $5,
                applepay          = $6,
                raffle_channel_id = $7
        """, guild_id,
            s.get("drop_channel_id"),
            s.get("venmo"),
            s.get("zelle"),
            s.get("cashapp"),
            s.get("applepay"),
            s.get("raffle_channel_id"),
        )


# ── RAFFLE DB HELPERS ─────────────────────────────────────────────────────────

async def _db_save_raffle(guild_id: int, name: str):
    r = server_raffles[guild_id][name]
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO raffles (guild_id, name, spots, price, channel_id, message_id, status)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (guild_id, name) DO UPDATE SET
                spots      = EXCLUDED.spots,
                price      = EXCLUDED.price,
                channel_id = EXCLUDED.channel_id,
                message_id = EXCLUDED.message_id,
                status     = EXCLUDED.status
        """, guild_id, name,
            r["spots"], r["price"], r["channel_id"], r["message_id"], r["status"])


async def _db_save_slot(guild_id: int, name: str, spot_num: int):
    s = server_raffles[guild_id][name]["slots"][spot_num]
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO raffle_slots (guild_id, raffle_name, spot_num, user_id, username, paid)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (guild_id, raffle_name, spot_num) DO UPDATE SET
                user_id  = EXCLUDED.user_id,
                username = EXCLUDED.username,
                paid     = EXCLUDED.paid
        """, guild_id, name, spot_num, s["user_id"], s["username"], s["paid"])


async def _db_delete_raffle(guild_id: int, name: str):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM raffle_slots WHERE guild_id = $1 AND raffle_name = $2",
            guild_id, name)
        await conn.execute(
            "DELETE FROM raffles WHERE guild_id = $1 AND name = $2",
            guild_id, name)


async def _db_save_raffle_channel(guild_id: int, channel_id: int):
    if guild_id not in server_settings:
        server_settings[guild_id] = {}
    server_settings[guild_id]["raffle_channel_id"] = channel_id
    await db_save_settings(guild_id)


# ── PER-SERVER STATE ──────────────────────────────────────────────────────────

server_admins           = {}
server_managers         = defaultdict(set)
server_settings         = {}
session_state           = defaultdict(lambda: "closed")
stock                   = defaultdict(dict)
claims                  = defaultdict(lambda: defaultdict(list))
waitlist                = defaultdict(lambda: defaultdict(list))
stock_message           = {}
pinned_message          = {}
autoclose               = defaultdict(lambda: True)
manager_session         = {}
payments                = defaultdict(lambda: defaultdict(list))
payment_board_message   = {}
pending_payment_messages = {}
last_drop_snapshot      = {}
archived_payments       = {}

# ── RAFFLE STATE ──────────────────────────────────────────────────────────────
server_raffles        = defaultdict(dict)
server_raffle_channel = {}

# ─────────────────────────────────────────────────────────────────────────────



def normalize(name):
    return name.lower().strip()


def parse_price(price_str):
    return float(price_str.lstrip("$"))


def is_manager(guild_id, user_id):
    return user_id in server_managers[guild_id]


def is_admin(guild_id, user_id):
    return server_admins.get(guild_id) == user_id


def is_creator(user_id):
    return user_id == CREATOR_ID


def all_sold_out(guild_id):
    if not stock[guild_id]:
        return False
    for key, info in stock[guild_id].items():
        claimed = sum(c["qty"] for c in claims[guild_id][key])
        if info["qty"] - claimed > 0:
            return False
    return True


def user_claimed_qty(guild_id, key, user_id):
    return sum(c["qty"] for c in claims[guild_id][key] if c["user"].id == user_id)


def get_manager_context(ctx):
    if ctx.guild:
        guild_id = ctx.guild.id
        if not is_manager(guild_id, ctx.author.id):
            return None, None
        return guild_id, ctx.channel
    else:
        session = manager_session.get(ctx.author.id)
        if not session:
            return None, None
        guild_id = session["guild_id"]
        if not is_manager(guild_id, ctx.author.id):
            return None, None
        return guild_id, session["channel"]


def get_drop_channel(guild):
    s = server_settings.get(guild.id, {})
    channel_id = s.get("drop_channel_id")
    if channel_id:
        return guild.get_channel(channel_id)
    return None


def build_payment_info(guild_id):
    s = server_settings.get(guild_id, {})
    lines = []
    if s.get("venmo"):
        lines.append(f"💜  Venmo: **{s['venmo']}**")
    if s.get("zelle"):
        lines.append(f"💙  Zelle: **{s['zelle']}**")
    if s.get("cashapp"):
        lines.append(f"💚  Cash App: **{s['cashapp']}**")
    if s.get("applepay"):
        lines.append(f"🍎  Apple Pay: **{s['applepay']}**")
    return "\n".join(lines) if lines else "No payment info configured."


def get_user_total_owed(guild_id, user_id):
    stock_ref  = stock[guild_id] if stock[guild_id] else last_drop_snapshot.get(guild_id, {}).get("stock", {})
    claims_ref = claims[guild_id] if claims[guild_id] else last_drop_snapshot.get(guild_id, {}).get("claims", {})
    return sum(
        c["qty"] * stock_ref[key]["price"]
        for key, claim_list in claims_ref.items()
        for c in claim_list
        if c["user"].id == user_id and key in stock_ref
    )


def build_stock_embed(guild_id):
    embed = discord.Embed(title="🛒  Drop Stock", color=discord.Color.gold(), timestamp=datetime.datetime.utcnow())
    for key, info in stock[guild_id].items():
        claimed  = sum(c["qty"] for c in claims[guild_id][key])
        qty_left = info["qty"] - claimed
        limit_str = f"  •  max {info['limit']} per person" if info["limit"] else ""
        status = f"**${info['price']:.2f}** each  •  **{qty_left}** of {info['qty']} remaining{limit_str}"
        if qty_left <= 0:
            status += "  🚫 **SOLD OUT**"
        embed.add_field(name=info["display"], value=status, inline=False)
    return embed


def build_claimlist_embed(guild_id, title="📋  Claim List"):
    embed      = discord.Embed(title=title, color=discord.Color.blurple(), timestamp=datetime.datetime.utcnow())
    stock_ref  = stock[guild_id] if stock[guild_id] else last_drop_snapshot.get(guild_id, {}).get("stock", {})
    claims_ref = claims[guild_id] if claims[guild_id] else last_drop_snapshot.get(guild_id, {}).get("claims", {})
    user_orders = {}
    for key, claim_list in claims_ref.items():
        if not claim_list or key not in stock_ref:
            continue
        item_display = stock_ref[key]["display"]
        item_price   = stock_ref[key]["price"]
        for c in claim_list:
            uid = c["user"].id
            if uid not in user_orders:
                user_orders[uid] = {"user": c["user"], "items": [], "total": 0.0}
            subtotal = c["qty"] * item_price
            user_orders[uid]["items"].append(f"• {item_display}  x{c['qty']}  — ${subtotal:.2f}")
            user_orders[uid]["total"] += subtotal
    if not user_orders:
        embed.description = "No claims yet."
        return embed
    for uid, order in user_orders.items():
        lines       = "\n".join(order["items"])
        field_value = f"{lines}\n**Total: ${order['total']:.2f}**"
        if len(field_value) > 1024:
            field_value = field_value[:1020] + "..."
        embed.add_field(name=f"{order['user'].display_name}", value=field_value, inline=False)
    return embed


def build_howto_embed():
    embed = discord.Embed(
        title="📖  How to Claim",
        color=discord.Color.green(),
        description="Welcome to the drop! Here's how it works:"
    )
    embed.add_field(name="!claim <item> <qty>",      value="Grab an item — e.g. `!claim PRE ETB 1`",         inline=False)
    embed.add_field(name="!stock",                   value="See what's still available",                      inline=False)
    embed.add_field(name="!myclaims",                value="See what you've claimed and your total owed",      inline=False)
    embed.add_field(name="!unclaim <item> <qty>",    value="Drop some or all of a claim",                     inline=False)
    embed.add_field(name="!waitlist <item>",         value="Join the waitlist if something is sold out",      inline=False)
    embed.add_field(name="!paid <method> <amount>",  value="Confirm your payment — e.g. `!paid venmo $125`", inline=False)
    embed.set_footer(text="First come, first served — when it's gone, it's gone!")
    return embed


def build_payment_board_embed(guild_id):
    embed = discord.Embed(title="💳  Payment Board", color=discord.Color.green(), timestamp=datetime.datetime.utcnow())
    confirmed_lines = []
    for user_id, user_payments in payments[guild_id].items():
        confirmed = [p for p in user_payments if p["confirmed"]]
        if confirmed:
            total   = sum(p["amount"] for p in confirmed)
            methods = ", ".join(f"{p['method'].title()} ${p['amount']:.2f}" for p in confirmed)
            confirmed_lines.append(f"✅  <@{user_id}> — {methods}  •  **${total:.2f} total**")
    embed.description = "\n".join(confirmed_lines) if confirmed_lines else "No payments confirmed yet."
    embed.set_footer(text="Updated automatically as payments are confirmed")
    return embed


async def update_payment_board(guild_id):
    msg = payment_board_message.get(guild_id)
    if msg:
        try:
            await msg.edit(embed=build_payment_board_embed(guild_id))
        except (discord.NotFound, discord.Forbidden):
            payment_board_message.pop(guild_id, None)


async def update_stock_embed(guild_id):
    msg = stock_message.get(guild_id)
    if not msg:
        return
    try:
        await msg.edit(embed=build_stock_embed(guild_id))
    except discord.NotFound:
        stock_message.pop(guild_id, None)
    except discord.Forbidden:
        stock_message.pop(guild_id, None)
    except discord.HTTPException as e:
        if e.status == 429:
            await asyncio.sleep(2)
            try:
                await msg.edit(embed=build_stock_embed(guild_id))
            except Exception:
                pass
        else:
            stock_message.pop(guild_id, None)


async def notify_waitlist(guild_id, key, qty_freed):
    wl = waitlist[guild_id][key]
    if not wl or qty_freed <= 0:
        return
    item_display = stock[guild_id][key]["display"] if key in stock[guild_id] else key
    for user in wl[:qty_freed]:
        try:
            await user.send(f"🔔  **{item_display}** is available again! Head back to the server and use `!claim {item_display} 1` before it's gone!")
        except discord.Forbidden:
            pass
    waitlist[guild_id][key] = wl[qty_freed:]


async def close_drop(channel, guild_id):
    session_state[guild_id] = "closed"
    pin = pinned_message.pop(guild_id, None)
    if pin:
        try:
            await pin.unpin()
        except (discord.Forbidden, discord.HTTPException):
            pass
    last_drop_snapshot[guild_id] = {
        "stock":  dict(stock[guild_id]),
        "claims": {k: list(v) for k, v in claims[guild_id].items()}
    }
    history_summary = {}
    total_revenue   = 0.0
    total_items     = 0
    buyers          = set()
    for key, claim_list in claims[guild_id].items():
        if not claim_list:
            continue
        item_display  = stock[guild_id][key]["display"] if key in stock[guild_id] else key
        item_price    = stock[guild_id][key]["price"]   if key in stock[guild_id] else 0
        item_qty      = sum(c["qty"] for c in claim_list)
        item_revenue  = item_qty * item_price
        total_revenue += item_revenue
        total_items   += item_qty
        for c in claim_list:
            buyers.add(c["user"].id)
        history_summary[item_display] = {"qty": item_qty, "revenue": item_revenue}
    await db_save_drop_history(guild_id, total_revenue, total_items, len(buyers), history_summary)
    embed     = build_claimlist_embed(guild_id, title="🔴  Drop CLOSED — Final Claim List")
    await channel.send(embed=embed)
    board_msg = await channel.send(embed=build_payment_board_embed(guild_id))
    payment_board_message[guild_id] = board_msg
    payment_info   = build_payment_info(guild_id)
    claimer_totals = defaultdict(list)
    for key, claim_list in claims[guild_id].items():
        for c in claim_list:
            subtotal = c["qty"] * stock[guild_id][key]["price"]
            claimer_totals[c["user"]].append((stock[guild_id][key]["display"], c["qty"], subtotal))
    for user, items in claimer_totals.items():
        total = sum(subtotal for _, _, subtotal in items)
        lines = "\n".join(f"• **{display}**  ×{qty}  — ${subtotal:.2f}" for display, qty, subtotal in items)
        try:
            await user.send(
                f"🧾  **Drop closed! Here's your order summary:**\n{lines}\n"
                f"**Total owed: ${total:.2f}**\n\n"
                f"**Send payment using one of these methods:**\n{payment_info}\n\n"
                f"Once you've sent payment, go back to the server and type:\n"
                f"`!paid venmo $125` (or whichever method you used)\n"
                f"You can run `!paid` multiple times if you split across methods."
            )
        except discord.Forbidden:
            pass


async def silent(ctx):
    try:
        await ctx.message.delete()
    except (discord.Forbidden, discord.NotFound):
        pass


async def dm(ctx, message):
    try:
        await ctx.author.send(message)
    except discord.Forbidden:
        pass


async def collect_payment_info(user, guild_id):
    def check(m):
        return m.author.id == user.id and isinstance(m.channel, discord.DMChannel)
    fields = [
        ("venmo",    "💜  What is your **Venmo** handle? (e.g. @yourname) — type `skip` to leave blank"),
        ("zelle",    "💙  What is your **Zelle** phone number or email? — type `skip` to leave blank"),
        ("cashapp",  "💚  What is your **Cash App** handle? (e.g. $yourname) — type `skip` to leave blank"),
        ("applepay", "🍎  What is your **Apple Pay** phone number? — type `skip` to leave blank"),
    ]
    if guild_id not in server_settings:
        server_settings[guild_id] = {}
    for field, prompt in fields:
        await user.send(prompt)
        try:
            msg   = await bot.wait_for("message", check=check, timeout=120)
            value = msg.content.strip()
            server_settings[guild_id][field] = None if value.lower() == "skip" else value
        except asyncio.TimeoutError:
            server_settings[guild_id][field] = None
    await db_save_settings(guild_id)
    await user.send(
        f"✅  Payment info saved!\n\n{build_payment_info(guild_id)}\n\n"
        f"You can update this anytime with `!setpayment` in your server."
    )


async def collect_drop_channel(user, guild):
    def check(m):
        return m.author.id == user.id and isinstance(m.channel, discord.DMChannel)
    await user.send(
        "📢  Which channel should drops be posted in?\n"
        "Please **mention the channel** in your server (e.g. `#drops`)."
    )
    try:
        msg = await bot.wait_for("message", check=check, timeout=120)
        if msg.channel_mentions:
            channel = msg.channel_mentions[0]
            if guild.id not in server_settings:
                server_settings[guild.id] = {}
            server_settings[guild.id]["drop_channel_id"] = channel.id
            await db_save_settings(guild.id)
            await user.send(f"✅  Drop channel set to **#{channel.name}**. You can change this anytime with `!setdropchannel #channel` in your server.")
            return channel
        else:
            await user.send("⚠️  No channel detected. You can set it later with `!setdropchannel #channel` in your server.")
    except asyncio.TimeoutError:
        await user.send("⚠️  Timed out. You can set the drop channel later with `!setdropchannel #channel` in your server.")
    return None


# ── EVENTS ────────────────────────────────────────────────────────────────────

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandOnCooldown):
        await ctx.send(f"⏳  Slow down! Try again in **{error.retry_after:.0f}s**.", delete_after=5)
    elif isinstance(error, commands.CommandNotFound):
        pass
    else:
        raise error


@bot.event
async def on_ready():
    await init_db()
    await db_load_all()
    await _register_persistent_views()
    await bot.tree.sync()
    print(f"✅  Logged in as {bot.user} ({bot.user.id})")


@bot.event
async def on_reaction_add(reaction, user):
    if user.bot:
        return
    if str(reaction.emoji) != "✅":
        return
    msg_id = reaction.message.id
    if msg_id not in pending_payment_messages:
        return
    data     = pending_payment_messages[msg_id]
    guild_id = data["guild_id"]
    buyer_id = data["buyer_id"]
    if not is_manager(guild_id, user.id):
        return
    user_pmts = payments[guild_id][buyer_id]
    pending   = [p for p in user_pmts if not p["confirmed"]]
    if not pending:
        return
    for p in pending:
        p["confirmed"] = True
    total_confirmed = sum(p["amount"] for p in pending)
    del pending_payment_messages[msg_id]
    await update_payment_board(guild_id)
    guild = reaction.message.guild
    buyer = guild.get_member(buyer_id)
    if buyer:
        try:
            await buyer.send(f"✅  Your payment of **${total_confirmed:.2f}** has been confirmed! Thanks so much — enjoy your order! 🎉")
        except discord.Forbidden:
            pass
    try:
        await reaction.message.edit(content=reaction.message.content + f"\n✅  Confirmed by **{user.display_name}**")
    except (discord.Forbidden, discord.NotFound):
        pass


# ── SETUP & MANAGER COMMANDS ──────────────────────────────────────────────────

@bot.command(name="setup")
async def cmd_setup(ctx):
    if not ctx.guild:
        await ctx.author.send("⚠️  Please run `!setup` in your server, not in a DM.")
        return
    guild_id = ctx.guild.id
    if guild_id in server_admins:
        await ctx.send(f"⚠️  This server already has an admin: <@{server_admins[guild_id]}>")
        return
    server_admins[guild_id] = ctx.author.id
    server_managers[guild_id].add(ctx.author.id)
    await db_set_admin(guild_id, ctx.author.id)
    await db_add_manager(guild_id, ctx.author.id)
    await ctx.send(f"✅  **{ctx.author.display_name}** is now the drop admin! Check your DMs to finish setup.")
    await collect_drop_channel(ctx.author, ctx.guild)
    await collect_payment_info(ctx.author, guild_id)


@bot.command(name="setpayment")
async def cmd_setpayment(ctx):
    if not ctx.guild:
        return
    guild_id = ctx.guild.id
    if not is_admin(guild_id, ctx.author.id):
        await ctx.send("⚠️  Only the server admin can update payment info.")
        return
    await ctx.send("Check your DMs to update payment info!")
    await collect_payment_info(ctx.author, guild_id)


@bot.command(name="setdropchannel")
async def cmd_setdropchannel(ctx):
    if not ctx.guild:
        return
    guild_id = ctx.guild.id
    if not is_admin(guild_id, ctx.author.id):
        await ctx.send("⚠️  Only the server admin can update the drop channel.")
        return
    if not ctx.message.channel_mentions:
        await ctx.send("Usage: `!setdropchannel #channel`")
        return
    channel = ctx.message.channel_mentions[0]
    if guild_id not in server_settings:
        server_settings[guild_id] = {}
    server_settings[guild_id]["drop_channel_id"] = channel.id
    await db_save_settings(guild_id)
    await ctx.send(f"✅  Drop channel updated to **#{channel.name}**.")


@bot.command(name="addmanager")
async def cmd_addmanager(ctx):
    if not ctx.guild:
        return
    guild_id = ctx.guild.id
    if not is_admin(guild_id, ctx.author.id):
        await ctx.send("⚠️  Only the server admin can add managers.")
        return
    if not ctx.message.mentions:
        await ctx.send("Usage: `!addmanager @user`")
        return
    user = ctx.message.mentions[0]
    server_managers[guild_id].add(user.id)
    await db_add_manager(guild_id, user.id)
    await ctx.send(f"✅  **{user.display_name}** has been added as a drop manager.")


@bot.command(name="removemanager")
async def cmd_removemanager(ctx):
    if not ctx.guild:
        return
    guild_id = ctx.guild.id
    if not is_admin(guild_id, ctx.author.id):
        await ctx.send("⚠️  Only the server admin can remove managers.")
        return
    if not ctx.message.mentions:
        await ctx.send("Usage: `!removemanager @user`")
        return
    user = ctx.message.mentions[0]
    if user.id == server_admins[guild_id]:
        await ctx.send("⚠️  You can't remove the admin.")
        return
    server_managers[guild_id].discard(user.id)
    await db_remove_manager(guild_id, user.id)
    await ctx.send(f"✅  **{user.display_name}** has been removed as a drop manager.")


@bot.command(name="managers")
async def cmd_managers(ctx):
    if not ctx.guild:
        return
    guild_id = ctx.guild.id
    if guild_id not in server_admins:
        await ctx.send("⚠️  No admin set up yet. Use `!setup` first.")
        return
    admin_id = server_admins[guild_id]
    managers = [uid for uid in server_managers[guild_id] if uid != admin_id]
    lines    = [f"👑  Admin: <@{admin_id}>"]
    if managers:
        lines += [f"🔧  Manager: <@{uid}>" for uid in managers]
    else:
        lines.append("No additional managers yet.")
    await ctx.send("\n".join(lines))


# ── DROP COMMANDS ─────────────────────────────────────────────────────────────

@bot.command(name="drop")
async def cmd_drop(ctx):
    if not ctx.guild:
        await ctx.author.send("⚠️  `!drop` must be run in a server channel.")
        return
    guild_id = ctx.guild.id
    if not is_manager(guild_id, ctx.author.id):
        return
    drop_ch = get_drop_channel(ctx.guild) or ctx.channel
    if claims[guild_id] or payments[guild_id]:
        archived_payments[guild_id] = {
            "payments": dict(payments[guild_id]),
            "claims":   {k: list(v) for k, v in claims[guild_id].items()},
            "stock":    dict(stock[guild_id]) if stock[guild_id] else dict(last_drop_snapshot.get(guild_id, {}).get("stock", {})),
        }
    session_state[guild_id]          = "staging"
    stock[guild_id]                  = {}
    claims[guild_id]                 = defaultdict(list)
    waitlist[guild_id]               = defaultdict(list)
    payments[guild_id]               = defaultdict(list)
    stock_message.pop(guild_id, None)
    pinned_message.pop(guild_id, None)
    payment_board_message.pop(guild_id, None)
    pending_payment_messages.clear()
    autoclose[guild_id]              = True
    manager_session[ctx.author.id]   = {"guild_id": guild_id, "channel": drop_ch}
    await silent(ctx)
    await dm(ctx, f"✅  Drop session started for **{ctx.guild.name}**! Drop will post in **#{drop_ch.name}**.\n\nCommands available now: `!addstock`, `!editstock`, `!removestockitem`, `!preview`, `!countdown`, `!release`, `!claimlist`, `!autoclose`, `!enddrop`\n\n💡  Auto-close is **ON**.")


@bot.command(name="addstock")
async def cmd_addstock(ctx, *, args=""):
    guild_id, drop_channel = get_manager_context(ctx)
    if guild_id is None:
        if not ctx.guild:
            await ctx.author.send("⚠️  No active drop session found. Run `!drop` in your server first.")
        return
    if ctx.guild:
        await silent(ctx)
    if session_state[guild_id] == "closed":
        await dm(ctx, "⚠️  No drop session active. Use `!drop` first.")
        return
    limit = None
    parts = args.split()
    if len(parts) >= 2 and parts[-2].lower() == "limit":
        try:
            limit = int(parts[-1])
            parts = parts[:-2]
        except ValueError:
            await dm(ctx, "⚠️  Limit must be a whole number.")
            return
    if len(parts) < 3:
        await dm(ctx, "Usage: `!addstock <item name> <qty> <price> [limit <n>]`")
        return
    price_str = parts[-1]
    qty_str   = parts[-2]
    item_name = " ".join(parts[:-2])
    try:
        qty   = int(qty_str)
        price = parse_price(price_str)
    except ValueError:
        await dm(ctx, f"⚠️  Couldn't read qty/price from `{qty_str}` / `{price_str}`")
        return
    key = normalize(item_name)
    stock[guild_id][key] = {"display": item_name.upper(), "qty": qty, "price": price, "limit": limit}
    limit_str = f"  •  max **{limit}** per person" if limit else "  •  no per-person limit"
    await dm(ctx, f"✅  **{item_name.upper()}** — {qty} @ ${price:.2f} each{limit_str}.")
    if ctx.author.id != CREATOR_ID and CREATOR_ID != 0:
        creator = await bot.fetch_user(CREATOR_ID)
        if creator:
            guild      = bot.get_guild(guild_id)
            guild_name = guild.name if guild else f"Guild {guild_id}"
            try:
                await creator.send(
                    f"[{guild_name}] {ctx.author.display_name} added stock: "
                    f"**{item_name.upper()}** - {qty} @ ${price:.2f} each{limit_str}."
                )
            except discord.Forbidden:
                pass


@bot.command(name="editstock")
async def cmd_editstock(ctx, *, args=""):
    guild_id, drop_channel = get_manager_context(ctx)
    if guild_id is None:
        if not ctx.guild:
            await ctx.author.send("⚠️  No active drop session found.")
        return
    if ctx.guild:
        await silent(ctx)
    if session_state[guild_id] == "closed":
        await dm(ctx, "⚠️  No drop session active.")
        return
    parts = args.split()
    if len(parts) < 3:
        await dm(ctx, "Usage: `!editstock <item name> <qty> <price>`")
        return
    price_str = parts[-1]
    qty_str   = parts[-2]
    item_name = " ".join(parts[:-2])
    try:
        qty   = int(qty_str)
        price = parse_price(price_str)
    except ValueError:
        await dm(ctx, f"⚠️  Couldn't read qty/price from `{qty_str}` / `{price_str}`")
        return
    key = normalize(item_name)
    if key not in stock[guild_id]:
        matches = [k for k in stock[guild_id] if normalize(item_name) in k or k in normalize(item_name)]
        if len(matches) == 1:
            key = matches[0]
        else:
            names = ", ".join(f"`{s['display']}`" for s in stock[guild_id].values())
            await dm(ctx, f"⚠️  Item not found. Current stock: {names}")
            return
    stock[guild_id][key]["qty"]   = qty
    stock[guild_id][key]["price"] = price
    if session_state[guild_id] == "live":
        await update_stock_embed(guild_id)
    await dm(ctx, f"✅  **{stock[guild_id][key]['display']}** updated — {qty} @ ${price:.2f} each.")


@bot.command(name="removestockitem")
async def cmd_removestockitem(ctx, *, item_name=""):
    guild_id, drop_channel = get_manager_context(ctx)
    if guild_id is None:
        if not ctx.guild:
            await ctx.author.send("⚠️  No active drop session found.")
        return
    if ctx.guild:
        await silent(ctx)
    if not item_name:
        await dm(ctx, "Usage: `!removestockitem <item name>`")
        return
    key = normalize(item_name)
    if key not in stock[guild_id]:
        matches = [k for k in stock[guild_id] if normalize(item_name) in k or k in normalize(item_name)]
        if len(matches) == 1:
            key = matches[0]
        else:
            names = ", ".join(f"`{s['display']}`" for s in stock[guild_id].values())
            await dm(ctx, f"⚠️  Item not found. Current stock: {names}")
            return
    removed = stock[guild_id].pop(key)
    await dm(ctx, f"🗑️  **{removed['display']}** removed.")


@bot.command(name="preview")
async def cmd_preview(ctx):
    guild_id, drop_channel = get_manager_context(ctx)
    if guild_id is None:
        if not ctx.guild:
            await ctx.author.send("⚠️  No active drop session found.")
        return
    if ctx.guild:
        await silent(ctx)
    if not stock[guild_id]:
        await dm(ctx, "⚠️  No stock loaded yet.")
        return
    await ctx.author.send(content="👀  **Drop preview — this is what members will see when you !release:**", embed=build_stock_embed(guild_id))


@bot.command(name="countdown")
async def cmd_countdown(ctx, minutes: str = ""):
    guild_id, drop_channel = get_manager_context(ctx)
    if guild_id is None:
        if not ctx.guild:
            await ctx.author.send("⚠️  No active drop session found.")
        return
    if ctx.guild:
        await silent(ctx)
    try:
        mins = int(minutes)
        if mins < 1 or mins > 60:
            raise ValueError
    except ValueError:
        await dm(ctx, "⚠️  Please provide a number between 1 and 60. Example: `!countdown 5`")
        return
    await drop_channel.send(f"⏳  **Drop incoming in {mins} minute{'s' if mins != 1 else ''}!** Get ready to claim — first come, first served! 🔥")
    await dm(ctx, f"✅  Countdown posted — {mins} minute{'s' if mins != 1 else ''} until drop.")

    async def auto_release():
        if mins >= 2:
            await asyncio.sleep((mins - 1) * 60)
            if session_state[guild_id] != "live":
                await drop_channel.send("⏰  **1 minute until the drop!** Stay ready!")
            else:
                return
        else:
            await asyncio.sleep(mins * 60)
        if session_state[guild_id] == "live":
            return
        if session_state[guild_id] != "staging":
            return
        if not stock[guild_id]:
            await drop_channel.send("⚠️  Countdown ended but no stock was loaded — drop not released.")
            return
        session_state[guild_id] = "live"
        msg = await drop_channel.send(embed=build_stock_embed(guild_id))
        stock_message[guild_id] = msg
        await drop_channel.send("🟢  **Drop is LIVE!**  First come, first served!")
        await drop_channel.send(embed=build_howto_embed())
        try:
            await msg.pin()
            pinned_message[guild_id] = msg
        except (discord.Forbidden, discord.HTTPException):
            pass

    asyncio.create_task(auto_release())


@bot.command(name="autoclose")
async def cmd_autoclose(ctx, toggle: str = ""):
    guild_id, drop_channel = get_manager_context(ctx)
    if guild_id is None:
        if not ctx.guild:
            await ctx.author.send("⚠️  No active drop session found.")
        return
    if ctx.guild:
        await silent(ctx)
    if toggle.lower() == "on":
        autoclose[guild_id] = True
        await dm(ctx, "✅  Auto-close is now **ON**.")
    elif toggle.lower() == "off":
        autoclose[guild_id] = False
        await dm(ctx, "✅  Auto-close is now **OFF** — use `!enddrop` to close manually.")
    else:
        status = "ON" if autoclose[guild_id] else "OFF"
        await dm(ctx, f"Auto-close is currently **{status}**.")


@bot.command(name="release")
async def cmd_release(ctx):
    guild_id, drop_channel = get_manager_context(ctx)
    if guild_id is None:
        if not ctx.guild:
            await ctx.author.send("⚠️  No active drop session found.")
        return
    if ctx.guild:
        await silent(ctx)
    if session_state[guild_id] == "closed":
        await dm(ctx, "⚠️  No drop session active.")
        return
    if not stock[guild_id]:
        await dm(ctx, "⚠️  No stock loaded.")
        return
    session_state[guild_id] = "live"
    msg = await drop_channel.send(embed=build_stock_embed(guild_id))
    stock_message[guild_id] = msg
    await drop_channel.send("🟢  **Drop is LIVE!**  First come, first served!")
    await drop_channel.send(embed=build_howto_embed())
    try:
        await msg.pin()
        pinned_message[guild_id] = msg
    except (discord.Forbidden, discord.HTTPException):
        pass
    await dm(ctx, "🟢  Drop is live!")


@bot.command(name="enddrop")
async def cmd_enddrop(ctx):
    guild_id, drop_channel = get_manager_context(ctx)
    if guild_id is None:
        if not ctx.guild:
            await ctx.author.send("⚠️  No active drop session found.")
        return
    if ctx.guild:
        await silent(ctx)
    if session_state[guild_id] != "live":
        await dm(ctx, "⚠️  No active drop to end.")
        return
    manager_session.pop(ctx.author.id, None)
    await close_drop(drop_channel, guild_id)


@bot.command(name="claimlist")
async def cmd_claimlist(ctx):
    guild_id, drop_channel = get_manager_context(ctx)
    if guild_id is None:
        if not ctx.guild:
            await ctx.author.send("⚠️  No active drop session found.")
        return
    if ctx.guild:
        await silent(ctx)
    if session_state[guild_id] == "closed":
        await dm(ctx, "No active drop.")
        return
    await ctx.author.send(embed=build_claimlist_embed(guild_id))


@bot.command(name="unpaid")
async def cmd_unpaid(ctx):
    if not ctx.guild:
        await ctx.author.send("⚠️  Please run `!unpaid` in your server channel.")
        return
    guild_id = ctx.guild.id
    if not is_manager(guild_id, ctx.author.id):
        return
    await silent(ctx)
    stock_ref  = stock[guild_id] if stock[guild_id] else last_drop_snapshot.get(guild_id, {}).get("stock", {})
    claims_ref = claims[guild_id] if claims[guild_id] else last_drop_snapshot.get(guild_id, {}).get("claims", {})
    claimer_totals = {}
    for key, claim_list in claims_ref.items():
        for c in claim_list:
            uid = c["user"].id
            if key not in stock_ref:
                continue
            subtotal = c["qty"] * stock_ref[key]["price"]
            if uid not in claimer_totals:
                claimer_totals[uid] = {"user": c["user"], "total": 0.0}
            claimer_totals[uid]["total"] += subtotal
    unpaid_lines = []
    for uid, info in claimer_totals.items():
        confirmed_total = sum(p["amount"] for p in payments[guild_id][uid] if p["confirmed"])
        owed = info["total"] - confirmed_total
        if owed > 0.01:
            unpaid_lines.append(f"• **{info['user'].display_name}** — owes **${owed:.2f}**")
    if not unpaid_lines:
        await ctx.author.send("✅  All claimers have been confirmed!")
        return
    await ctx.author.send("⏳  **Unpaid claimers:**\n" + "\n".join(unpaid_lines))


@bot.command(name="confirm")
async def cmd_confirm(ctx):
    if not ctx.guild:
        await ctx.author.send("⚠️  Please run `!confirm` in your server channel.")
        return
    guild_id = ctx.guild.id
    if not is_manager(guild_id, ctx.author.id):
        return
    await silent(ctx)
    if not ctx.message.mentions:
        await dm(ctx, "Usage: `!confirm @user`")
        return
    user      = ctx.message.mentions[0]
    user_pmts = payments[guild_id][user.id]
    pending   = [p for p in user_pmts if not p["confirmed"]]
    archived      = archived_payments.get(guild_id, {})
    archived_pmts = archived.get("payments", {})
    if isinstance(archived_pmts, dict) and user.id in archived_pmts:
        archived_pending = [p for p in archived_pmts[user.id] if not p["confirmed"]]
        pending = pending + archived_pending
    if not pending:
        await dm(ctx, f"⚠️  No pending payments found for **{user.display_name}**.")
        return
    for p in pending:
        p["confirmed"] = True
    total_confirmed = sum(p["amount"] for p in pending)
    await update_payment_board(guild_id)
    try:
        await user.send(f"✅  Your payment of **${total_confirmed:.2f}** has been confirmed! Thanks so much — enjoy your order! 🎉")
    except discord.Forbidden:
        pass
    await dm(ctx, f"✅  Confirmed **${total_confirmed:.2f}** from **{user.display_name}**.")


# ── PUBLIC COMMANDS ───────────────────────────────────────────────────────────

@bot.command(name="stock")
async def cmd_stock(ctx):
    if not ctx.guild:
        await ctx.author.send("⚠️  Please use this command in your server channel.")
        return
    guild_id = ctx.guild.id
    if session_state[guild_id] != "live":
        await ctx.send("No drop is currently active.")
        return
    await ctx.send(embed=build_stock_embed(guild_id))


@bot.command(name="paid")
@commands.cooldown(3, 60, commands.BucketType.user)
async def cmd_paid(ctx, *, args=""):
    if not ctx.guild:
        await ctx.author.send("⚠️  Please use `!paid` in your server channel.")
        return
    guild_id        = ctx.guild.id
    claims_ref      = claims[guild_id] if claims[guild_id] else last_drop_snapshot.get(guild_id, {}).get("claims", {})
    archived        = archived_payments.get(guild_id, {})
    archived_claims = archived.get("claims", {})
    has_live_claim = any(
        c["user"].id == ctx.author.id
        for claim_list in claims_ref.values()
        for c in claim_list
    )
    has_archived_claim = any(
        c["user"].id == ctx.author.id
        for claim_list in archived_claims.values()
        for c in claim_list
    )
    if not has_live_claim and not has_archived_claim:
        await ctx.author.send("⚠️  You don't have any claims to pay for.")
        await silent(ctx)
        return
    using_archive = False
    if has_live_claim and has_archived_claim:
        live_total = sum(
            c["qty"] * stock[guild_id][key]["price"]
            for key, cl in claims_ref.items()
            for c in cl
            if c["user"].id == ctx.author.id and key in stock[guild_id]
        )
        arch_stock = archived.get("stock", {})
        arch_total = sum(
            c["qty"] * arch_stock[key]["price"]
            for key, cl in archived_claims.items()
            for c in cl
            if c["user"].id == ctx.author.id and key in arch_stock
        )
        await ctx.send(
            f"You have claims in two drops, {ctx.author.display_name}!\n"
            f"Reply with **`1`** for the current drop (${live_total:.2f} owed) or "
            f"**`2`** for the previous drop (${arch_total:.2f} owed).\n"
            "Or run `!paid` twice to report both."
        )
        def check(m):
            return m.author.id == ctx.author.id and m.channel.id == ctx.channel.id and m.content.strip() in ["1", "2"]
        try:
            reply = await bot.wait_for("message", check=check, timeout=30)
            if reply.content.strip() == "2":
                claims_ref    = archived_claims
                stock_ref     = arch_stock
                payments_ref  = archived.get("payments", defaultdict(list))
                using_archive = True
            else:
                stock_ref    = stock[guild_id]
                payments_ref = payments[guild_id]
        except asyncio.TimeoutError:
            await ctx.send("⏰  Timed out — defaulting to current drop.")
            stock_ref    = stock[guild_id]
            payments_ref = payments[guild_id]
    elif not has_live_claim and has_archived_claim:
        claims_ref    = archived_claims
        stock_ref     = archived.get("stock", {})
        payments_ref  = archived.get("payments", defaultdict(list))
        using_archive = True
    else:
        stock_ref    = stock[guild_id] if stock[guild_id] else last_drop_snapshot.get(guild_id, {}).get("stock", {})
        payments_ref = payments[guild_id]
    parts = args.split()
    if len(parts) < 2:
        await ctx.author.send("Usage: `!paid <method> <amount>`  e.g. `!paid venmo $125`")
        await silent(ctx)
        return
    method        = parts[0].lower()
    valid_methods = ["venmo", "zelle", "cashapp", "applepay", "apple"]
    if method not in valid_methods:
        await ctx.author.send("⚠️  Payment method not recognized. Use: venmo, zelle, cashapp, or applepay")
        await silent(ctx)
        return
    if method == "apple":
        method = "applepay"
    try:
        amount = parse_price(parts[1])
    except ValueError:
        await ctx.author.send(f"⚠️  Couldn't read amount from `{parts[1]}`. Example: `!paid venmo $125`")
        await silent(ctx)
        return
    await silent(ctx)
    if using_archive:
        if guild_id not in archived_payments:
            archived_payments[guild_id] = {"payments": defaultdict(list), "claims": {}, "stock": {}}
        if "payments" not in archived_payments[guild_id]:
            archived_payments[guild_id]["payments"] = defaultdict(list)
        archived_payments[guild_id]["payments"][ctx.author.id].append({
            "method": method, "amount": amount,
            "time": datetime.datetime.utcnow(), "confirmed": False
        })
        payments_ref = archived_payments[guild_id]["payments"]
    else:
        payments[guild_id][ctx.author.id].append({
            "method": method, "amount": amount,
            "time": datetime.datetime.utcnow(), "confirmed": False
        })
        payments_ref = payments[guild_id]
    total_owed = sum(
        c["qty"] * stock_ref[key]["price"]
        for key, claim_list in claims_ref.items()
        for c in claim_list
        if c["user"].id == ctx.author.id and key in stock_ref
    )
    total_paid = sum(p["amount"] for p in payments_ref[ctx.author.id])
    remaining  = total_owed - total_paid
    await ctx.author.send(
        f"💳  Payment of **${amount:.2f}** via **{method.title()}** received!\n"
        f"Total owed: ${total_owed:.2f}  •  Total reported: ${total_paid:.2f}"
        + (f"  •  Still outstanding: **${remaining:.2f}**" if remaining > 0.01 else "  •  ✅ Fully reported! Waiting on confirmation.")
    )
    drop_ch          = get_drop_channel(ctx.guild) or ctx.channel
    manager_mentions = " ".join(f"<@{uid}>" for uid in server_managers[guild_id])
    ping_msg         = await drop_ch.send(
        f"💰  {manager_mentions} — **{ctx.author.display_name}** reported payment of **${amount:.2f}** via **{method.title()}**.\n"
        f"React ✅ to confirm or use `!confirm @{ctx.author.display_name}`.",
        allowed_mentions=discord.AllowedMentions(users=True)
    )
    await ping_msg.add_reaction("✅")
    pending_payment_messages[ping_msg.id] = {"guild_id": guild_id, "buyer_id": ctx.author.id}


@bot.command(name="claim")
@commands.cooldown(5, 10, commands.BucketType.user)
async def cmd_claim(ctx, *, args=""):
    if not ctx.guild:
        await ctx.author.send("⚠️  Please use `!claim` in your server channel.")
        return
    guild_id = ctx.guild.id
    if session_state[guild_id] != "live":
        await ctx.send("⚠️  No active drop right now.")
        return
    parts = args.split()
    if not parts:
        await ctx.send("Usage: `!claim <item> <qty>`  e.g. `!claim PRE ETB 1`")
        return
    if args.strip().lower() == "all":
        responses = [
            "🔴  *Oak's words echoed: 'There's a time and place for everything, but not now.'* Use `!claim <item> <qty>`.",
            "💫  You used Splash. Nothing happened. Use `!claim <item> <qty>`.",
            "😴  Your claim used Rest. It fell asleep and did nothing. Try `!claim <item> <qty>`.",
            "💀  Giovanni himself reviewed your claim and rejected it. Try `!claim <item> <qty>`.",
        ]
        await ctx.send(random.choice(responses))
        return
    if "luck" in args.lower():
        await ctx.send("🎰  Even Arceus couldn't find *luck* in this drop. Check `!stock` for what's real.")
        return
    try:
        qty       = int(parts[-1])
        item_name = " ".join(parts[:-1])
    except ValueError:
        qty       = 1
        item_name = " ".join(parts)
    if not item_name:
        await ctx.send("Usage: `!claim <item> <qty>`  e.g. `!claim PRE ETB 1`")
        return
    if qty < 1:
        await ctx.send("⚠️  Qty must be at least 1.")
        return
    key = normalize(item_name)
    if key not in stock[guild_id]:
        matches = [k for k in stock[guild_id] if normalize(item_name) in k or k in normalize(item_name)]
        if len(matches) == 1:
            key = matches[0]
        elif len(matches) > 1:
            names = ", ".join(f"`{stock[guild_id][k]['display']}`" for k in matches)
            await ctx.send(f"⚠️  Multiple matches: {names} — be more specific.")
            return
        else:
            names = ", ".join(f"`{s['display']}`" for s in stock[guild_id].values())
            await ctx.send(f"⚠️  Item not found. Available: {names}")
            return
    info            = stock[guild_id][key]
    already_claimed = sum(c["qty"] for c in claims[guild_id][key])
    remaining       = info["qty"] - already_claimed
    if remaining <= 0:
        wl            = waitlist[guild_id][key]
        already_on_wl = any(u.id == ctx.author.id for u in wl)
        if already_on_wl:
            await ctx.send(f"😔  **{info['display']}** is sold out and you're already on the waitlist.")
        else:
            await ctx.send(f"😔  **{info['display']}** is sold out! Use `!waitlist {info['display']}` to be notified if it opens up.")
        return
    if qty > remaining:
        await ctx.send(f"⚠️  Only **{remaining}** of **{info['display']}** left. Try `!claim {info['display']} {remaining}`")
        return
    if info["limit"] is not None:
        already_user = user_claimed_qty(guild_id, key, ctx.author.id)
        allowed      = info["limit"] - already_user
        if allowed <= 0:
            await ctx.send(f"⚠️  You've already claimed the max of **{info['limit']}** for **{info['display']}**.")
            return
        if qty > allowed:
            await ctx.send(f"⚠️  You can only claim **{allowed}** more of **{info['display']}** (limit: {info['limit']} per person).")
            return
    existing = next((c for c in claims[guild_id][key] if c["user"].id == ctx.author.id), None)
    if existing:
        existing["qty"] += qty
    else:
        claims[guild_id][key].append({"user": ctx.author, "qty": qty, "time": datetime.datetime.utcnow()})
    new_remaining = remaining - qty
    total_cost    = qty * info["price"]
    await ctx.send(f"✅  **{ctx.author.display_name}** claimed **{qty}x {info['display']}** — ${total_cost:.2f}  •  {new_remaining} left")
    await update_stock_embed(guild_id)
    if autoclose[guild_id] and all_sold_out(guild_id):
        await ctx.send("🎉  **Everything is claimed!** Closing the drop...")
        await close_drop(ctx.channel, guild_id)


@bot.command(name="unclaim")
async def cmd_unclaim(ctx, *, args=""):
    if not ctx.guild:
        return
    guild_id = ctx.guild.id
    if session_state[guild_id] != "live":
        await ctx.send("⚠️  No active drop right now.")
        return
    if not args:
        await ctx.send("Usage: `!unclaim <item> <qty>`  e.g. `!unclaim PRE ETB 1`")
        return
    parts = args.split()
    try:
        qty       = int(parts[-1])
        item_name = " ".join(parts[:-1])
    except ValueError:
        qty       = None
        item_name = " ".join(parts)
    if not item_name:
        await ctx.send("Usage: `!unclaim <item> <qty>`")
        return
    key = normalize(item_name)
    if key not in stock[guild_id]:
        matches = [k for k in stock[guild_id] if normalize(item_name) in k or k in normalize(item_name)]
        if len(matches) == 1:
            key = matches[0]
        else:
            names = ", ".join(f"`{s['display']}`" for s in stock[guild_id].values())
            await ctx.send(f"⚠️  Item not found. Available: {names}")
            return
    existing = next((c for c in claims[guild_id][key] if c["user"].id == ctx.author.id), None)
    if not existing:
        await ctx.send("You don't have a claim on that item.")
        return
    if qty is None or qty >= existing["qty"]:
        freed = existing["qty"]
        claims[guild_id][key].remove(existing)
        await ctx.send(f"↩️  **{ctx.author.display_name}** removed their entire claim on **{stock[guild_id][key]['display']}**.")
    else:
        if qty < 1:
            await ctx.send("⚠️  Qty must be at least 1.")
            return
        freed            = qty
        existing["qty"] -= qty
        await ctx.send(f"↩️  **{ctx.author.display_name}** removed **{qty}x {stock[guild_id][key]['display']}** from their claim. ({existing['qty']} still claimed)")
    await update_stock_embed(guild_id)
    await notify_waitlist(guild_id, key, freed)


@bot.command(name="waitlist")
async def cmd_waitlist(ctx, *, item_name=""):
    if not ctx.guild:
        await ctx.author.send("⚠️  Please use `!waitlist` in your server channel.")
        return
    guild_id = ctx.guild.id
    if session_state[guild_id] != "live":
        await ctx.send("⚠️  No active drop right now.")
        return
    if not item_name:
        await ctx.send("Usage: `!waitlist <item>`")
        return
    key = normalize(item_name)
    if key not in stock[guild_id]:
        matches = [k for k in stock[guild_id] if normalize(item_name) in k or k in normalize(item_name)]
        if len(matches) == 1:
            key = matches[0]
        else:
            names = ", ".join(f"`{s['display']}`" for s in stock[guild_id].values())
            await ctx.send(f"⚠️  Item not found. Available: {names}")
            return
    info            = stock[guild_id][key]
    already_claimed = sum(c["qty"] for c in claims[guild_id][key])
    remaining       = info["qty"] - already_claimed
    if remaining > 0:
        await ctx.send(f"**{info['display']}** is still available! Use `!claim {info['display']} 1` to grab it.")
        return
    already_on_wl = any(u.id == ctx.author.id for u in waitlist[guild_id][key])
    if already_on_wl:
        await ctx.send(f"You're already on the waitlist for **{info['display']}**!")
        return
    waitlist[guild_id][key].append(ctx.author)
    pos = len(waitlist[guild_id][key])
    await ctx.send(f"✅  **{ctx.author.display_name}** added to the waitlist for **{info['display']}** (position #{pos}).")


@bot.command(name="myclaims")
async def cmd_myclaims(ctx):
    if not ctx.guild:
        await ctx.author.send("⚠️  Please use `!myclaims` in your server channel.")
        return
    guild_id   = ctx.guild.id
    stock_ref  = stock[guild_id] if stock[guild_id] else last_drop_snapshot.get(guild_id, {}).get("stock", {})
    claims_ref = claims[guild_id] if claims[guild_id] else last_drop_snapshot.get(guild_id, {}).get("claims", {})
    if not stock_ref and not claims_ref:
        await ctx.send("No active drop.")
        return
    user_claims = []
    total       = 0.0
    for key, claim_list in claims_ref.items():
        for c in claim_list:
            if c["user"].id == ctx.author.id and key in stock_ref:
                subtotal = c["qty"] * stock_ref[key]["price"]
                total   += subtotal
                user_claims.append(f"• **{stock_ref[key]['display']}**  ×{c['qty']}  — ${subtotal:.2f}")
    if not user_claims:
        await ctx.send("You haven't claimed anything in this drop yet.")
        return
    lines = "\n".join(user_claims)
    await ctx.send(f"**{ctx.author.display_name}'s claims:**\n{lines}\n**Total owed: ${total:.2f}**")


@bot.command(name="history")
async def cmd_history(ctx):
    if not ctx.guild:
        await ctx.author.send("⚠️  Please run `!history` in your server channel.")
        return
    guild_id = ctx.guild.id
    if not is_manager(guild_id, ctx.author.id):
        return
    await silent(ctx)
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT closed_at, total_revenue, total_items, unique_buyers, summary
            FROM drop_history
            WHERE guild_id = $1
            ORDER BY closed_at DESC
            LIMIT 10
        """, guild_id)
    if not rows:
        await ctx.author.send("No drop history yet. History is saved automatically when a drop ends.")
        return
    embed = discord.Embed(title="📊  Drop History (Last 10)", color=discord.Color.blurple(), timestamp=datetime.datetime.utcnow())
    all_revenue = 0.0
    all_items   = 0
    for i, row in enumerate(rows):
        summary     = json.loads(row["summary"])
        date_str    = row["closed_at"].strftime("%b %d, %Y")
        item_lines  = "\n".join(f"- {item}: x{data['qty']} - ${float(data['revenue']):.2f}" for item, data in summary.items())
        footer_line = f"{row['total_items']} items  -  {row['unique_buyers']} buyer(s)"
        field_value = f"{item_lines}\n{footer_line}" if item_lines else footer_line
        if len(field_value) > 1024:
            field_value = field_value[:1020] + "..."
        embed.add_field(
            name=f"Drop #{len(rows) - i}  -  {date_str}  -  ${float(row['total_revenue']):.2f}",
            value=field_value, inline=False
        )
        all_revenue += float(row["total_revenue"])
        all_items   += row["total_items"]
    embed.set_footer(text=f"All-time: ${all_revenue:.2f} revenue  -  {all_items} items sold")
    try:
        await ctx.author.send(embed=embed)
    except discord.Forbidden:
        await ctx.send("Could not DM you the history — please open your DMs and try again.")
    except discord.HTTPException:
        await ctx.send("History embed was too large to send.")


@bot.command(name="bump")
async def cmd_bump(ctx):
    if not ctx.guild:
        await ctx.author.send("⚠️  Please run `!bump` in your server channel.")
        return
    guild_id = ctx.guild.id
    if not is_manager(guild_id, ctx.author.id):
        return
    await silent(ctx)
    if not ctx.message.mentions:
        await dm(ctx, "Usage: `!bump @user`")
        return
    user       = ctx.message.mentions[0]
    total_owed = get_user_total_owed(guild_id, user.id)
    if total_owed == 0:
        await dm(ctx, f"⚠️  **{user.display_name}** doesn't have any claims.")
        return
    confirmed = sum(p["amount"] for p in payments[guild_id][user.id] if p["confirmed"])
    remaining = total_owed - confirmed
    if remaining <= 0.01:
        await dm(ctx, f"✅  **{user.display_name}** is already fully paid!")
        return
    payment_info = build_payment_info(guild_id)
    try:
        await user.send(
            f"👋  Hey! Just a friendly reminder that you have an outstanding balance of **${remaining:.2f}** from the recent drop.\n\n"
            f"**Send payment using one of these methods:**\n{payment_info}\n\n"
            f"Once sent, run `!paid <method> <amount>` in the server to let us know. Thanks!"
        )
        await dm(ctx, f"✅  Bump sent to **{user.display_name}** — they owe **${remaining:.2f}**.")
    except discord.Forbidden:
        await dm(ctx, f"⚠️  Couldn't DM **{user.display_name}** — their DMs may be closed.")


@bot.command(name="remind")
async def cmd_remind(ctx):
    if not ctx.guild:
        await ctx.author.send("⚠️  Please run `!remind` in your server channel.")
        return
    guild_id = ctx.guild.id
    if not is_manager(guild_id, ctx.author.id):
        return
    await silent(ctx)
    drop_channel = get_drop_channel(ctx.guild) or ctx.channel
    claims_ref   = claims[guild_id] if claims[guild_id] else last_drop_snapshot.get(guild_id, {}).get("claims", {})
    stock_ref    = stock[guild_id]  if stock[guild_id]  else last_drop_snapshot.get(guild_id, {}).get("stock",  {})
    unpaid_mentions = []
    seen_users      = set()
    for key, claim_list in claims_ref.items():
        for c in claim_list:
            uid = c["user"].id
            if uid in seen_users or key not in stock_ref:
                continue
            seen_users.add(uid)
            total_owed = sum(
                cc["qty"] * stock_ref[k]["price"]
                for k, cl in claims_ref.items()
                for cc in cl
                if cc["user"].id == uid and k in stock_ref
            )
            confirmed = sum(p["amount"] for p in payments[guild_id][uid] if p["confirmed"])
            if total_owed - confirmed > 0.01:
                unpaid_mentions.append(f"<@{uid}>")
    if not unpaid_mentions:
        await dm(ctx, "✅  Everyone has been confirmed — no outstanding payments!")
        return
    payment_info = build_payment_info(guild_id)
    mentions_str = " ".join(unpaid_mentions)
    await drop_channel.send(
        f"⏰  **Payment Reminder** — {mentions_str}\n\n"
        f"You have an outstanding balance from the recent drop. Please send payment:\n"
        f"{payment_info}\n\n"
        f"Once sent, type `!paid <method> <amount>` to confirm. Thanks!"
    )
    await dm(ctx, f"✅  Reminder posted for {len(unpaid_mentions)} unpaid buyer(s).")


@bot.command(name="announce")
async def cmd_announce(ctx, *, message: str = ""):
    if not ctx.guild:
        await ctx.author.send("⚠️  Please run `!announce` in your server channel.")
        return
    guild_id = ctx.guild.id
    if not is_manager(guild_id, ctx.author.id):
        return
    await silent(ctx)
    if not message:
        await dm(ctx, "Usage: `!announce <message>`\nExample: `!announce Drop going live in 10 minutes!`")
        return
    if len(message) > 4000:
        await dm(ctx, "⚠️  Message too long — keep it under 4000 characters.")
        return
    drop_channel = get_drop_channel(ctx.guild) or ctx.channel
    embed = discord.Embed(description=message, color=discord.Color.gold(), timestamp=datetime.datetime.utcnow())
    embed.set_footer(text="VaultDrop")
    await drop_channel.send(embed=embed)
    if drop_channel != ctx.channel:
        await dm(ctx, f"✅  Announcement posted in **#{drop_channel.name}**.")


# ── CREATOR COMMANDS ──────────────────────────────────────────────────────────

@bot.command(name="creator")
async def cmd_creator(ctx, subcommand: str = "", *args):
    if ctx.guild:
        await ctx.message.delete()
        return
    if not is_creator(ctx.author.id):
        return
    if not subcommand:
        await ctx.author.send(
            "**Creator Commands (DM only):**\n"
            "`!creator servers` — List all servers the bot is in\n"
            "`!creator info <guild_id>` — See a server's settings, admin, and managers\n"
            "`!creator setpayment <guild_id>` — Update payment info for a server\n"
            "`!creator setdropchannel <guild_id> <channel_id>` — Update drop channel for a server\n"
            "`!creator resetadmin <guild_id> <user_id>` — Reassign the admin for a server\n"
            "`!creator announce <guild_id> <message>` — Post announcement in a server's drop channel"
        )
        return
    sub = subcommand.lower()
    if sub == "servers":
        guilds = bot.guilds
        if not guilds:
            await ctx.author.send("The bot is not in any servers.")
            return
        lines = []
        for g in guilds:
            admin_id    = server_admins.get(g.id)
            admin_str   = f"<@{admin_id}>" if admin_id else "No admin set"
            settings    = server_settings.get(g.id, {})
            ch_id       = settings.get("drop_channel_id")
            ch_str      = f"<#{ch_id}>" if ch_id else "No channel set"
            payment_set = any([settings.get("venmo"), settings.get("zelle"), settings.get("cashapp"), settings.get("applepay")])
            lines.append(f"**{g.name}** (`{g.id}`)")
            lines.append(f"  Admin: {admin_str}  |  Drop channel: {ch_str}  |  Payment info: {'✅' if payment_set else '❌'}")
        msg = "\n".join(lines)
        for i in range(0, len(msg), 1900):
            await ctx.author.send(msg[i:i+1900])
        return
    if sub == "info":
        if not args:
            await ctx.author.send("Usage: `!creator info <guild_id>`")
            return
        try:
            guild_id = int(args[0])
        except ValueError:
            await ctx.author.send("⚠️  Invalid guild ID.")
            return
        guild = bot.get_guild(guild_id)
        if not guild:
            await ctx.author.send(f"⚠️  Bot is not in a server with ID `{guild_id}`.")
            return
        admin_id = server_admins.get(guild_id)
        managers = [uid for uid in server_managers[guild_id] if uid != admin_id]
        settings = server_settings.get(guild_id, {})
        ch_id    = settings.get("drop_channel_id")
        lines    = [
            f"**Server:** {guild.name} (`{guild_id}`)",
            f"**Members:** {guild.member_count}",
            f"**Admin:** {'<@' + str(admin_id) + '>' if admin_id else 'Not set'}",
            f"**Managers:** {', '.join('<@' + str(uid) + '>' for uid in managers) if managers else 'None'}",
            f"**Drop channel:** {'<#' + str(ch_id) + '>' if ch_id else 'Not set'}",
            f"**Venmo:** {settings.get('venmo') or 'Not set'}",
            f"**Zelle:** {settings.get('zelle') or 'Not set'}",
            f"**Cash App:** {settings.get('cashapp') or 'Not set'}",
            f"**Apple Pay:** {settings.get('applepay') or 'Not set'}",
        ]
        await ctx.author.send("\n".join(lines))
        return
    if sub == "setpayment":
        if not args:
            await ctx.author.send("Usage: `!creator setpayment <guild_id>`")
            return
        try:
            guild_id = int(args[0])
        except ValueError:
            await ctx.author.send("⚠️  Invalid guild ID.")
            return
        guild = bot.get_guild(guild_id)
        if not guild:
            await ctx.author.send(f"⚠️  Bot is not in a server with ID `{guild_id}`.")
            return
        await ctx.author.send(f"Setting payment info for **{guild.name}**:")
        await collect_payment_info(ctx.author, guild_id)
        return
    if sub == "setdropchannel":
        if len(args) < 2:
            await ctx.author.send("Usage: `!creator setdropchannel <guild_id> <channel_id>`")
            return
        try:
            guild_id   = int(args[0])
            channel_id = int(args[1])
        except ValueError:
            await ctx.author.send("⚠️  Invalid guild ID or channel ID.")
            return
        guild = bot.get_guild(guild_id)
        if not guild:
            await ctx.author.send(f"⚠️  Bot is not in a server with ID `{guild_id}`.")
            return
        channel = guild.get_channel(channel_id)
        if not channel:
            await ctx.author.send(f"⚠️  Channel `{channel_id}` not found in **{guild.name}**.")
            return
        if guild_id not in server_settings:
            server_settings[guild_id] = {}
        server_settings[guild_id]["drop_channel_id"] = channel_id
        await db_save_settings(guild_id)
        await ctx.author.send(f"✅  Drop channel for **{guild.name}** updated to **#{channel.name}**.")
        return
    if sub == "resetadmin":
        if len(args) < 2:
            await ctx.author.send("Usage: `!creator resetadmin <guild_id> <user_id>`")
            return
        try:
            guild_id     = int(args[0])
            new_admin_id = int(args[1])
        except ValueError:
            await ctx.author.send("⚠️  Invalid guild ID or user ID.")
            return
        guild = bot.get_guild(guild_id)
        if not guild:
            await ctx.author.send(f"⚠️  Bot is not in a server with ID `{guild_id}`.")
            return
        member = guild.get_member(new_admin_id)
        if not member:
            await ctx.author.send(f"⚠️  User `{new_admin_id}` not found in **{guild.name}**.")
            return
        old_admin_id            = server_admins.get(guild_id)
        server_admins[guild_id] = new_admin_id
        server_managers[guild_id].add(new_admin_id)
        await db_set_admin(guild_id, new_admin_id)
        await db_add_manager(guild_id, new_admin_id)
        await ctx.author.send(
            f"✅  Admin for **{guild.name}** updated.\n"
            f"Old admin: {'<@' + str(old_admin_id) + '>' if old_admin_id else 'None'}\n"
            f"New admin: **{member.display_name}** (`{new_admin_id}`)"
        )
        return
    if sub == "announce":
        if len(args) < 2:
            await ctx.author.send("Usage: `!creator announce <guild_id> <message>`")
            return
        try:
            guild_id = int(args[0])
        except ValueError:
            await ctx.author.send("⚠️  Invalid guild ID — must be a number.")
            return
        guild = bot.get_guild(guild_id)
        if not guild:
            await ctx.author.send(f"⚠️  Bot is not in a server with ID `{guild_id}`.")
            return
        message = " ".join(args[1:])
        if not message.strip():
            await ctx.author.send("⚠️  Message cannot be empty.")
            return
        drop_channel = get_drop_channel(guild)
        if not drop_channel:
            drop_channel = next(
                (c for c in guild.text_channels if c.permissions_for(guild.me).send_messages), None
            )
            if not drop_channel:
                await ctx.author.send(f"⚠️  No drop channel set for **{guild.name}** and no accessible channel found.")
                return
            await ctx.author.send(f"⚠️  No drop channel configured — posting to **#{drop_channel.name}** instead.")
        embed = discord.Embed(description=message, color=discord.Color.gold(), timestamp=datetime.datetime.utcnow())
        embed.set_footer(text="VaultDrop")
        await drop_channel.send(embed=embed)
        await ctx.author.send(f"✅  Announcement posted in **#{drop_channel.name}** on **{guild.name}**.")
        return
    await ctx.author.send(f"⚠️  Unknown subcommand `{subcommand}`. Type `!creator` for a list of commands.")


@bot.command(name="help")
async def cmd_help(ctx):
    if not ctx.guild:
        return
    embed = discord.Embed(title="📖  VaultDrop Commands", color=discord.Color.gold(), timestamp=datetime.datetime.utcnow())
    embed.add_field(
        name="During a Drop",
        value=(
            "`!claim <item> <qty>` — Claim an item\n"
            "`!unclaim <item> <qty>` — Remove a claim\n"
            "`!stock` — See what's available\n"
            "`!myclaims` — See your claims and total\n"
            "`!waitlist <item>` — Join waitlist for sold out items"
        ),
        inline=False
    )
    embed.add_field(
        name="Payments",
        value=(
            "`!paid <method> <amount>` — Report your payment\n"
            "e.g. `!paid venmo $125`\n"
            "Methods: venmo, zelle, cashapp, applepay"
        ),
        inline=False
    )
    embed.add_field(
        name="Raffles",
        value="`/raffles` — See active raffles\n`/raffle create` — Create a raffle (owner only)\nSpots are claimed by tapping buttons — no commands needed!",
        inline=False
    )
    embed.set_footer(text="VaultDrop — First come, first served!")
    await ctx.send(embed=embed)



# ══════════════════════════════════════════════════════════════════════════════
# RAFFLE MODULE — Button UI + Slash Commands
# ══════════════════════════════════════════════════════════════════════════════
#
# USER FLOW:
#   Owner runs /raffle create → embed posts with numbered buttons (max 10)
#   User taps a button        → spot claimed instantly, bot DMs payment info
#   Owner runs /raffle confirm @user → marks paid, embed updates
#   Owner runs /raffle wheel  → Wheel of Names link posted for live spin
#   Owner runs /raffle winner @user  → winner announced
#
# PERSISTENCE:
#   RaffleView is re-registered from DB on startup so buttons survive restarts
#
# ── DB HELPERS ────────────────────────────────────────────────────────────────

async def _db_save_raffle(guild_id: int, name: str):
    r = server_raffles[guild_id][name]
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO raffles (guild_id, name, spots, price, channel_id, message_id, status)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (guild_id, name) DO UPDATE SET
                spots      = EXCLUDED.spots,
                price      = EXCLUDED.price,
                channel_id = EXCLUDED.channel_id,
                message_id = EXCLUDED.message_id,
                status     = EXCLUDED.status
        """, guild_id, name,
            r["spots"], r["price"], r["channel_id"], r["message_id"], r["status"])


async def _db_save_slot(guild_id: int, name: str, spot_num: int):
    s = server_raffles[guild_id][name]["slots"][spot_num]
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO raffle_slots (guild_id, raffle_name, spot_num, user_id, username, paid)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (guild_id, raffle_name, spot_num) DO UPDATE SET
                user_id  = EXCLUDED.user_id,
                username = EXCLUDED.username,
                paid     = EXCLUDED.paid
        """, guild_id, name, spot_num, s["user_id"], s["username"], s["paid"])


async def _db_delete_raffle(guild_id: int, name: str):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM raffle_slots WHERE guild_id = $1 AND raffle_name = $2",
            guild_id, name)
        await conn.execute(
            "DELETE FROM raffles WHERE guild_id = $1 AND name = $2",
            guild_id, name)


async def _db_save_raffle_channel(guild_id: int, channel_id: int):
    if guild_id not in server_settings:
        server_settings[guild_id] = {}
    server_settings[guild_id]["raffle_channel_id"] = channel_id
    await db_save_settings(guild_id)


# ── EMBED BUILDER ─────────────────────────────────────────────────────────────

def _raffle_embed(name: str, raffle: dict) -> discord.Embed:
    slots     = raffle["slots"]
    total     = raffle["spots"]
    claimed   = sum(1 for s in slots.values() if s["user_id"] is not None)
    paid      = sum(1 for s in slots.values() if s["paid"])
    remaining = total - claimed

    if raffle["status"] == "complete":
        color = discord.Color.gold()
    elif raffle["status"] == "closed":
        color = discord.Color.red()
    else:
        color = discord.Color.green()

    embed = discord.Embed(title=f"🎟️  Raffle — {name}", color=color)
    embed.add_field(name="Price per Spot", value=raffle["price"], inline=True)
    embed.add_field(name="Total Spots",    value=str(total),      inline=True)
    embed.add_field(name="Remaining",      value=str(remaining),  inline=True)

    # Slot list
    lines = []
    for num in sorted(slots.keys()):
        s = slots[num]
        if s["user_id"] is None:
            lines.append(f"`{num:>2}` 🟢 Open")
        elif s["paid"]:
            lines.append(f"`{num:>2}` ✅ {s['username']}")
        else:
            lines.append(f"`{num:>2}` ⏳ {s['username']} *(awaiting payment)*")

    if total <= 5:
        embed.add_field(name="Spots", value="\n".join(lines) or "—", inline=False)
    else:
        mid   = (len(lines) + 1) // 2
        embed.add_field(name="Spots",  value="\n".join(lines[:mid]) or "—", inline=True)
        embed.add_field(name="\u200b", value="\n".join(lines[mid:]) or "—", inline=True)

    status_map = {
        "open":     "🟢 Open — tap a spot button to claim!",
        "closed":   "🔴 All spots claimed — awaiting payment confirmations",
        "complete": "🏆 Raffle complete",
    }
    embed.set_footer(text=status_map.get(raffle["status"], ""))
    return embed


def _build_raffle_payment_dm(guild_id: int) -> str:
    s = server_settings.get(guild_id, {})
    lines = []
    if s.get("venmo"):    lines.append(f"💜 Venmo: {s['venmo']}")
    if s.get("zelle"):    lines.append(f"💙 Zelle: {s['zelle']}")
    if s.get("cashapp"):  lines.append(f"💚 Cash App: {s['cashapp']}")
    if s.get("applepay"): lines.append(f"🍎 Apple Pay: {s['applepay']}")
    return "\n".join(lines) if lines else "*(payment info not set — contact the server owner)*"


# ── BUTTON VIEW ───────────────────────────────────────────────────────────────

class RaffleView(discord.ui.View):
    """
    Persistent button view for a single raffle.
    custom_id format: raffle:{guild_id}:{name}:{spot_num}
    This format lets us reconstruct views from DB on restart.
    """

    def __init__(self, guild_id: int, name: str, raffle: dict):
        super().__init__(timeout=None)  # Never times out — persistent
        self.guild_id = guild_id
        self.name     = name
        self._build_buttons(raffle)

    def _build_buttons(self, raffle: dict):
        self.clear_items()
        for num in sorted(raffle["slots"].keys()):
            s       = raffle["slots"][num]
            taken   = s["user_id"] is not None
            label   = f"Spot {num}" if not taken else f"#{num} — {s['username'][:12]}"
            btn     = discord.ui.Button(
                label     = label,
                style     = discord.ButtonStyle.danger if taken else discord.ButtonStyle.success,
                disabled  = taken,
                custom_id = f"raffle:{self.guild_id}:{self.name}:{num}",
                row       = (num - 1) // 5,   # 5 buttons per row, max 2 rows = 10 spots
            )
            btn.callback = self._make_callback(num)
            self.add_item(btn)

    def _make_callback(self, spot_num: int):
        async def callback(interaction: discord.Interaction):
            await _handle_spot_claim(interaction, self.guild_id, self.name, spot_num)
        return callback

    def refresh(self, raffle: dict):
        """Rebuild buttons to reflect current slot state."""
        self._build_buttons(raffle)


async def _handle_spot_claim(
    interaction: discord.Interaction,
    guild_id: int,
    name: str,
    spot_num: int,
):
    """Shared handler called by button callbacks."""
    await interaction.response.defer(ephemeral=True)

    if guild_id not in server_raffles or name not in server_raffles[guild_id]:
        await interaction.followup.send("⚠️  This raffle no longer exists.", ephemeral=True)
        return

    raffle = server_raffles[guild_id][name]

    if raffle["status"] != "open":
        await interaction.followup.send("⚠️  This raffle is no longer accepting claims.", ephemeral=True)
        return

    slot = raffle["slots"].get(spot_num)
    if slot is None:
        await interaction.followup.send("⚠️  That spot doesn't exist.", ephemeral=True)
        return

    if slot["user_id"] is not None:
        await interaction.followup.send(
            f"⚠️  Spot **#{spot_num}** was just taken! Pick another open spot.",
            ephemeral=True,
        )
        return

    # Check user doesn't already hold a spot in this raffle
    for n, s in raffle["slots"].items():
        if s["user_id"] == interaction.user.id:
            await interaction.followup.send(
                f"⚠️  You already hold **Spot #{n}** in this raffle.",
                ephemeral=True,
            )
            return

    # Claim the spot
    username = str(interaction.user)
    raffle["slots"][spot_num] = {"user_id": interaction.user.id, "username": username, "paid": False}
    await _db_save_slot(guild_id, name, spot_num)

    # Auto-close if all spots taken
    all_claimed = all(s["user_id"] is not None for s in raffle["slots"].values())
    if all_claimed:
        raffle["status"] = "closed"
        await _db_save_raffle(guild_id, name)

    # Rebuild and update the embed + buttons
    view = RaffleView(guild_id, name, raffle)
    try:
        channel = bot.get_channel(raffle["channel_id"])
        msg     = await channel.fetch_message(raffle["message_id"])
        await msg.edit(embed=_raffle_embed(name, raffle), view=view)
    except (discord.NotFound, discord.HTTPException):
        pass

    if all_claimed:
        channel = bot.get_channel(raffle["channel_id"])
        if channel:
            await channel.send(
                f"🔒  **{name}** is fully claimed! "
                f"Waiting on payment confirmations before the spin. 🎡"
            )

    # Confirm to the user ephemerally
    await interaction.followup.send(
        f"🎟️  You claimed **Spot #{spot_num}**! Check your DMs for payment details.",
        ephemeral=True,
    )

    # DM payment instructions
    payment_info = _build_raffle_payment_dm(guild_id)
    try:
        await interaction.user.send(
            f"🎉  You claimed **Spot #{spot_num}** in the **{name}** raffle!\n\n"
            f"**Price:** {raffle['price']}\n\n"
            f"**Send payment using one of these methods:**\n{payment_info}\n\n"
            f"Once you've sent payment, reply here with your method and amount "
            f"so the organizer can confirm — e.g. `Venmo $25`.\n\nGood luck! 🤞"
        )
    except discord.Forbidden:
        channel = bot.get_channel(raffle["channel_id"])
        if channel:
            await channel.send(
                f"⚠️  {interaction.user.mention} — I couldn't DM you! "
                f"Open your DMs and contact the server owner for payment details.",
                delete_after=25,
            )


async def _register_persistent_views():
    """
    Called on startup — re-adds RaffleView for every open/closed raffle
    so buttons still work after a Railway restart.
    """
    for guild_id, raffles in server_raffles.items():
        for name, raffle in raffles.items():
            if raffle["status"] in ("open", "closed"):
                view = RaffleView(guild_id, name, raffle)
                bot.add_view(view)


# ── SLASH COMMAND TREE ────────────────────────────────────────────────────────

raffle_group = discord.app_commands.Group(
    name="raffle",
    description="Raffle commands for Vault & Pine drops"
)


@raffle_group.command(name="create", description="Create a new raffle with button-based spot claiming")
@discord.app_commands.describe(
    name="Raffle name (e.g. ScarletVault)",
    spots="Number of spots (max 10)",
    price="Price per spot (e.g. $25)",
)
async def slash_raffle_create(interaction: discord.Interaction, name: str, spots: int, price: str):
    await interaction.response.defer(ephemeral=True)

    if interaction.guild.owner_id != interaction.user.id:
        await interaction.followup.send("⚠️  Only the server owner can create raffles.", ephemeral=True)
        return

    guild_id = interaction.guild_id

    if spots < 2 or spots > 10:
        await interaction.followup.send("⚠️  Spots must be between 2 and 10.", ephemeral=True)
        return

    price = price if price.startswith("$") else f"${price}"

    if name in server_raffles[guild_id]:
        await interaction.followup.send(
            f"⚠️  A raffle named **{name}** already exists. Cancel it first or use a different name.",
            ephemeral=True,
        )
        return

    # One-time channel setup
    if guild_id not in server_raffle_channel:
        await interaction.followup.send(
            "📋  **One-time setup:** Which channel should raffles be posted in?\n"
            "Use `/raffle setchannel` first, then create your raffle.",
            ephemeral=True,
        )
        return

    raffle = {
        "spots":      spots,
        "price":      price,
        "channel_id": server_raffle_channel[guild_id],
        "message_id": None,
        "status":     "open",
        "slots":      {n: {"user_id": None, "username": None, "paid": False}
                       for n in range(1, spots + 1)},
    }
    server_raffles[guild_id][name] = raffle

    channel = bot.get_channel(server_raffle_channel[guild_id])
    if channel is None:
        await interaction.followup.send(
            "⚠️  Raffle channel not found. Use `/raffle setchannel` to set one.",
            ephemeral=True,
        )
        del server_raffles[guild_id][name]
        return

    payment_hint = _build_raffle_payment_dm(guild_id)
    embed        = _raffle_embed(name, raffle)
    embed.description = (
        f"Tap a button below to claim your spot!\n"
        f"The bot will DM you payment details instantly.\n\n"
        f"💳 **Payment accepted via:**\n{payment_hint}"
    )

    view = RaffleView(guild_id, name, raffle)
    msg  = await channel.send(embed=embed, view=view)

    raffle["message_id"] = msg.id
    await _db_save_raffle(guild_id, name)
    for spot_num in raffle["slots"]:
        await _db_save_slot(guild_id, name, spot_num)

    # Register the view for persistence
    bot.add_view(view)

    await interaction.followup.send(
        f"✅  Raffle **{name}** is live with **{spots} spots** at **{price}** each!",
        ephemeral=True,
    )


@raffle_group.command(name="confirm", description="Confirm a user's payment for a raffle spot")
@discord.app_commands.describe(name="Raffle name", user="The user to confirm")
async def slash_raffle_confirm(interaction: discord.Interaction, name: str, user: discord.Member):
    await interaction.response.defer(ephemeral=True)

    if interaction.guild.owner_id != interaction.user.id:
        await interaction.followup.send("⚠️  Only the server owner can confirm payments.", ephemeral=True)
        return

    guild_id = interaction.guild_id

    if name not in server_raffles[guild_id]:
        await interaction.followup.send(f"⚠️  No raffle named **{name}**.", ephemeral=True)
        return

    raffle     = server_raffles[guild_id][name]
    spot_found = None
    for num, s in raffle["slots"].items():
        if s["user_id"] == user.id:
            spot_found = num
            break

    if spot_found is None:
        await interaction.followup.send(
            f"⚠️  **{user.display_name}** doesn't have a spot in **{name}**.",
            ephemeral=True,
        )
        return

    if raffle["slots"][spot_found]["paid"]:
        await interaction.followup.send(
            f"⚠️  **{user.display_name}**'s payment is already confirmed.",
            ephemeral=True,
        )
        return

    raffle["slots"][spot_found]["paid"] = True
    await _db_save_slot(guild_id, name, spot_found)

    # Update embed
    channel = bot.get_channel(raffle["channel_id"])
    if channel and raffle["message_id"]:
        try:
            msg  = await channel.fetch_message(raffle["message_id"])
            view = RaffleView(guild_id, name, raffle)
            await msg.edit(embed=_raffle_embed(name, raffle), view=view)
        except (discord.NotFound, discord.HTTPException):
            pass
        await channel.send(
            f"✅  Payment confirmed — **{user.display_name}** holds Spot **#{spot_found}** in **{name}**!"
        )

    try:
        await user.send(
            f"✅  Your payment for **Spot #{spot_found}** in the **{name}** raffle is confirmed!\n"
            f"Watch for the live spin announcement. 🎡"
        )
    except discord.Forbidden:
        pass

    await interaction.followup.send(
        f"✅  Confirmed payment for **{user.display_name}** — Spot #{spot_found}.",
        ephemeral=True,
    )


@raffle_group.command(name="wheel", description="Generate Wheel of Names link for the live spin")
@discord.app_commands.describe(
    name="Raffle name",
    force="Spin even if some payments aren't confirmed yet",
)
async def slash_raffle_wheel(
    interaction: discord.Interaction,
    name: str,
    force: bool = False,
):
    await interaction.response.defer(ephemeral=True)

    if interaction.guild.owner_id != interaction.user.id:
        await interaction.followup.send("⚠️  Only the server owner can start the wheel.", ephemeral=True)
        return

    guild_id = interaction.guild_id

    if name not in server_raffles[guild_id]:
        await interaction.followup.send(f"⚠️  No raffle named **{name}**.", ephemeral=True)
        return

    raffle     = server_raffles[guild_id][name]
    paid_slots = [(num, s) for num, s in sorted(raffle["slots"].items()) if s["paid"]]

    if not paid_slots:
        await interaction.followup.send(
            "⚠️  No confirmed payments yet. Confirm payments with `/raffle confirm` first.",
            ephemeral=True,
        )
        return

    unpaid_claimed = [
        (num, s) for num, s in raffle["slots"].items()
        if s["user_id"] is not None and not s["paid"]
    ]
    if unpaid_claimed and not force:
        names_list = ", ".join(f"**{s['username']}** (#{num})" for num, s in unpaid_claimed)
        await interaction.followup.send(
            f"⚠️  Unconfirmed payments: {names_list}\n\n"
            f"Confirm them first, or use `/raffle wheel name:{name} force:True` to spin without them.",
            ephemeral=True,
        )
        return

    entries   = [f"{s['username']} - Spot {num}" for num, s in paid_slots]
    wheel_url = f"https://wheelofnames.com/?v=1&names={url_quote(','.join(entries))}"

    channel = bot.get_channel(raffle["channel_id"])
    if channel is None:
        await interaction.followup.send("⚠️  Raffle channel not found.", ephemeral=True)
        return

    embed = discord.Embed(
        title=f"🎡  SPIN TIME — {name} Raffle!",
        description=(
            f"**{len(paid_slots)} entries** loaded into the wheel!\n\n"
            f"🔗 **[Click here to open the wheel]({wheel_url})**\n\n"
            f"🎙️  Get into voice chat — the creator is spinning **LIVE** right now!"
        ),
        color=discord.Color.gold(),
    )
    embed.add_field(
        name="Entries on the Wheel",
        value="\n".join(f"• {e}" for e in entries),
        inline=False,
    )
    embed.set_footer(text=f"After the spin → /raffle winner {name} @winner")
    await channel.send(embed=embed)

    await interaction.followup.send("✅  Wheel posted! Go spin it live.", ephemeral=True)


@raffle_group.command(name="winner", description="Record the winner and post the announcement")
@discord.app_commands.describe(name="Raffle name", user="The winning user")
async def slash_raffle_winner(interaction: discord.Interaction, name: str, user: discord.Member):
    await interaction.response.defer(ephemeral=True)

    if interaction.guild.owner_id != interaction.user.id:
        await interaction.followup.send("⚠️  Only the server owner can record the winner.", ephemeral=True)
        return

    guild_id = interaction.guild_id

    if name not in server_raffles[guild_id]:
        await interaction.followup.send(f"⚠️  No raffle named **{name}**.", ephemeral=True)
        return

    raffle     = server_raffles[guild_id][name]
    spot_found = None
    for num, s in raffle["slots"].items():
        if s["user_id"] == user.id:
            spot_found = num
            break

    if spot_found is None:
        await interaction.followup.send(
            f"⚠️  **{user.display_name}** doesn't have a spot in **{name}**.",
            ephemeral=True,
        )
        return

    raffle["status"] = "complete"
    await _db_save_raffle(guild_id, name)

    # Update embed — disable all buttons
    channel = bot.get_channel(raffle["channel_id"])
    if channel and raffle["message_id"]:
        try:
            msg  = await channel.fetch_message(raffle["message_id"])
            view = RaffleView(guild_id, name, raffle)
            await msg.edit(embed=_raffle_embed(name, raffle), view=view)
        except (discord.NotFound, discord.HTTPException):
            pass

        winner_embed = discord.Embed(
            title=f"🏆  Winner — {name} Raffle!",
            description=(
                f"🎉 Congratulations to {user.mention}!\n\n"
                f"**Winner:** {raffle['slots'][spot_found]['username']}\n"
                f"**Winning Spot:** #{spot_found}\n\n"
                f"Thanks to everyone who participated! 🙌"
            ),
            color=discord.Color.gold(),
        )
        await channel.send(embed=winner_embed)

    try:
        await user.send(
            f"🏆  **You won the {name} raffle!** Congratulations!\n"
            f"The server owner will reach out shortly with your prize details."
        )
    except discord.Forbidden:
        pass

    await interaction.followup.send(
        f"🏆  Winner recorded — **{user.display_name}**, Spot #{spot_found}.",
        ephemeral=True,
    )


@raffle_group.command(name="cancel", description="Cancel and remove a raffle")
@discord.app_commands.describe(name="Raffle name")
async def slash_raffle_cancel(interaction: discord.Interaction, name: str):
    await interaction.response.defer(ephemeral=True)

    if interaction.guild.owner_id != interaction.user.id:
        await interaction.followup.send("⚠️  Only the server owner can cancel raffles.", ephemeral=True)
        return

    guild_id = interaction.guild_id

    if name not in server_raffles[guild_id]:
        await interaction.followup.send(f"⚠️  No raffle named **{name}**.", ephemeral=True)
        return

    # Disable buttons on the original message
    raffle  = server_raffles[guild_id][name]
    channel = bot.get_channel(raffle["channel_id"])
    if channel and raffle["message_id"]:
        try:
            msg = await channel.fetch_message(raffle["message_id"])
            await msg.edit(
                embed=discord.Embed(
                    title=f"🚫  Raffle Cancelled — {name}",
                    color=discord.Color.dark_gray(),
                ),
                view=None,
            )
        except (discord.NotFound, discord.HTTPException):
            pass

    await _db_delete_raffle(guild_id, name)
    del server_raffles[guild_id][name]

    await interaction.followup.send(f"🗑️  Raffle **{name}** cancelled and removed.", ephemeral=True)


@raffle_group.command(name="status", description="Show the current state of a raffle")
@discord.app_commands.describe(name="Raffle name")
async def slash_raffle_status(interaction: discord.Interaction, name: str):
    await interaction.response.defer(ephemeral=True)
    guild_id = interaction.guild_id
    if name not in server_raffles[guild_id]:
        await interaction.followup.send(f"⚠️  No raffle named **{name}**.", ephemeral=True)
        return
    await interaction.followup.send(
        embed=_raffle_embed(name, server_raffles[guild_id][name]),
        ephemeral=True,
    )


@raffle_group.command(name="setchannel", description="Set the channel where raffles are posted")
@discord.app_commands.describe(channel="The channel to post raffles in")
async def slash_raffle_setchannel(interaction: discord.Interaction, channel: discord.TextChannel):
    await interaction.response.defer(ephemeral=True)

    if interaction.guild.owner_id != interaction.user.id:
        await interaction.followup.send("⚠️  Only the server owner can set the raffle channel.", ephemeral=True)
        return

    guild_id = interaction.guild_id
    server_raffle_channel[guild_id] = channel.id
    await _db_save_raffle_channel(guild_id, channel.id)
    await interaction.followup.send(f"✅  Raffle channel set to **#{channel.name}**.", ephemeral=True)


@bot.tree.command(name="raffles", description="List all active raffles")
async def slash_raffles(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    guild_id = interaction.guild_id
    active   = {
        k: v for k, v in server_raffles.get(guild_id, {}).items()
        if v["status"] != "complete"
    }
    if not active:
        await interaction.followup.send("No active raffles right now. 🎟️", ephemeral=True)
        return

    embed = discord.Embed(title="🎟️  Active Raffles", color=discord.Color.blurple())
    for name, r in active.items():
        claimed = sum(1 for s in r["slots"].values() if s["user_id"] is not None)
        paid    = sum(1 for s in r["slots"].values() if s["paid"])
        embed.add_field(
            name=name,
            value=(
                f"**Price:** {r['price']} | "
                f"**Claimed:** {claimed}/{r['spots']} | "
                f"**Paid:** {paid} | "
                f"**Status:** {r['status'].capitalize()}"
            ),
            inline=False,
        )
    await interaction.followup.send(embed=embed, ephemeral=True)


# Register the raffle slash command group
bot.tree.add_command(raffle_group)


# ── RUN ──────────────────────────────────────────────────────────────────────
bot.run(BOT_TOKEN)
