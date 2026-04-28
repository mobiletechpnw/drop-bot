"""
Vault & Pine Drop Bot
=====================
Setup (first time in a new server):
  !setup                           — Register yourself as this server's admin

Admin only:
  !addmanager @user                — Grant manager role to a user
  !removemanager @user             — Revoke manager role
  !managers                        — List current admin and managers

Manager/Admin commands:
  Run in your server channel OR in a DM with the bot after starting a drop.
  !drop                            — Start a new drop session (must run in server)
  !addstock <item> <qty> <price> [limit <n>]
                                   — Add item, e.g.:
                                     !addstock PRE ETB 1 $100
                                     !addstock PRE ETB 3 $100 limit 1
  !editstock <item> <qty> <price>  — Edit qty/price of an existing item
  !removestockitem <item>          — Remove an item from staging
  !preview                         — DM yourself the stock embed before releasing
  !countdown <minutes>             — Post a public hype countdown, e.g. !countdown 5
  !release                         — Post the drop publicly and open claiming
  !autoclose on/off                — Toggle auto-close when sold out (default: on)
  !claimlist                       — DM yourself the current claim list
  !enddrop                         — Close drop, post final list, DM all claimers

Public commands (anyone, in server only):
  !claim <item> <qty>              — e.g. !claim PRE ETB 1
  !unclaim <item> <qty>            — Drop some or all of your claim
  !waitlist <item>                 — Join the waitlist if an item is sold out
  !stock                           — Show current inventory
  !myclaims                        — See your own claims and total
"""

import discord
from discord.ext import commands
from collections import defaultdict
import datetime
import asyncio
import os
import asyncpg

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
PREFIX = "!"

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix=PREFIX, intents=intents)

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
    print("✅  Database ready.")


async def db_load_all():
    """Load all admins and managers from DB into memory on startup."""
    async with db_pool.acquire() as conn:
        admins = await conn.fetch("SELECT guild_id, user_id FROM server_admins")
        for row in admins:
            server_admins[row["guild_id"]] = row["user_id"]

        managers = await conn.fetch("SELECT guild_id, user_id FROM server_managers")
        for row in managers:
            server_managers[row["guild_id"]].add(row["user_id"])

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

# ── PER-SERVER STATE ──────────────────────────────────────────────────────────

server_admins = {}
server_managers = defaultdict(set)
session_state = defaultdict(lambda: "closed")

# stock[guild_id][item_key] = {"display": str, "qty": int, "price": float, "limit": int or None}
stock = defaultdict(dict)

# claims[guild_id][item_key] = [{"user": member, "qty": int, "time": datetime}, ...]
claims = defaultdict(lambda: defaultdict(list))

# waitlist[guild_id][item_key] = [user, ...]
waitlist = defaultdict(lambda: defaultdict(list))

stock_message = {}
pinned_message = {}
autoclose = defaultdict(lambda: True)

# manager_session[user_id] = {"guild_id": int, "channel": TextChannel}
manager_session = {}

# ─────────────────────────────────────────────────────────────────────────────


def normalize(name):
    return name.lower().strip()


def parse_price(price_str):
    return float(price_str.lstrip("$"))


def is_manager(guild_id, user_id):
    return user_id in server_managers[guild_id]


def is_admin(guild_id, user_id):
    return server_admins.get(guild_id) == user_id


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


def build_stock_embed(guild_id):
    embed = discord.Embed(title="🛒  Drop Stock", color=discord.Color.gold(), timestamp=datetime.datetime.utcnow())
    for key, info in stock[guild_id].items():
        claimed = sum(c["qty"] for c in claims[guild_id][key])
        qty_left = info["qty"] - claimed
        limit_str = f"  •  max {info['limit']} per person" if info["limit"] else ""
        status = f"**${info['price']:.2f}** each  •  **{qty_left}** of {info['qty']} remaining{limit_str}"
        if qty_left <= 0:
            status += "  🚫 **SOLD OUT**"
        embed.add_field(name=info["display"], value=status, inline=False)
    return embed


def build_claimlist_embed(guild_id, title="📋  Claim List"):
    embed = discord.Embed(title=title, color=discord.Color.blurple(), timestamp=datetime.datetime.utcnow())
    any_claims = False
    for key, claim_list in claims[guild_id].items():
        if not claim_list:
            continue
        any_claims = True
        lines = [
            f"• **{c['user'].display_name}**  ×{c['qty']}  — ${c['qty'] * stock[guild_id][key]['price']:.2f}"
            for c in claim_list
        ]
        embed.add_field(
            name=stock[guild_id][key]["display"] if key in stock[guild_id] else key,
            value="\n".join(lines),
            inline=False
        )
    if not any_claims:
        embed.description = "No claims yet."
    return embed


def build_howto_embed():
    embed = discord.Embed(
        title="📖  How to Claim",
        color=discord.Color.green(),
        description="Welcome to the drop! Here's how it works:"
    )
    embed.add_field(name="!claim <item> <qty>", value="Grab an item — e.g. `!claim PRE ETB 1`", inline=False)
    embed.add_field(name="!stock", value="See what's still available", inline=False)
    embed.add_field(name="!myclaims", value="See what you've claimed and your total owed", inline=False)
    embed.add_field(name="!unclaim <item> <qty>", value="Drop some or all of a claim", inline=False)
    embed.add_field(name="!waitlist <item>", value="Join the waitlist if something is sold out — you'll be notified if it opens up", inline=False)
    embed.set_footer(text="First come, first served — when it's gone, it's gone!")
    return embed


async def update_stock_embed(guild_id):
    msg = stock_message.get(guild_id)
    if msg:
        try:
            await msg.edit(embed=build_stock_embed(guild_id))
        except (discord.NotFound, discord.Forbidden):
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
    embed = build_claimlist_embed(guild_id, title="🔴  Drop CLOSED — Final Claim List")
    await channel.send(embed=embed)
    claimer_totals = defaultdict(list)
    for key, claim_list in claims[guild_id].items():
        for c in claim_list:
            subtotal = c["qty"] * stock[guild_id][key]["price"]
            claimer_totals[c["user"]].append((stock[guild_id][key]["display"], c["qty"], subtotal))
    for user, items in claimer_totals.items():
        total = sum(subtotal for _, _, subtotal in items)
        lines = "\n".join(f"• **{display}**  ×{qty}  — ${subtotal:.2f}" for display, qty, subtotal in items)
        try:
            await user.send(f"🧾  **Drop closed! Here's your order summary:**\n{lines}\n**Total owed: ${total:.2f}**\n\nPlease send payment to complete your order!")
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


# ── EVENTS ────────────────────────────────────────────────────────────────────

@bot.event
async def on_ready():
    await init_db()
    await db_load_all()
    print(f"✅  Logged in as {bot.user} ({bot.user.id})")


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
    await ctx.send(f"✅  **{ctx.author.display_name}** is now the drop admin for this server!\nUse `!addmanager @user` to add additional managers.")


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
    lines = [f"👑  Admin: <@{admin_id}>"]
    if managers:
        lines += [f"🔧  Manager: <@{uid}>" for uid in managers]
    else:
        lines.append("No additional managers yet. Use `!addmanager @user` to add one.")
    await ctx.send("\n".join(lines))


# ── DROP COMMANDS ─────────────────────────────────────────────────────────────

@bot.command(name="drop")
async def cmd_drop(ctx):
    if not ctx.guild:
        await ctx.author.send("⚠️  `!drop` must be run in a server channel. After that you can use all other commands via DM.")
        return
    guild_id = ctx.guild.id
    if not is_manager(guild_id, ctx.author.id):
        return
    session_state[guild_id] = "staging"
    stock[guild_id] = {}
    claims[guild_id] = defaultdict(list)
    waitlist[guild_id] = defaultdict(list)
    stock_message.pop(guild_id, None)
    pinned_message.pop(guild_id, None)
    autoclose[guild_id] = True
    manager_session[ctx.author.id] = {"guild_id": guild_id, "channel": ctx.channel}
    await silent(ctx)
    await dm(ctx, f"✅  Drop session started for **{ctx.guild.name}**!\nYou can now use `!addstock`, `!editstock`, `!removestockitem`, `!preview`, `!countdown`, `!release`, `!claimlist`, `!autoclose`, and `!enddrop` from this DM or in the server.\n\n💡  Auto-close is **ON**.")


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
        await dm(ctx, "⚠️  No drop session active. Use `!drop` in your server first.")
        return
    limit = None
    parts = args.split()
    if len(parts) >= 2 and parts[-2].lower() == "limit":
        try:
            limit = int(parts[-1])
            parts = parts[:-2]
        except ValueError:
            await dm(ctx, "⚠️  Limit must be a whole number. Example: `!addstock PRE ETB 1 100 limit 1`")
            return
    if len(parts) < 3:
        await dm(ctx, "Usage: `!addstock <item name> <qty> <price> [limit <n>]`\nExamples:\n`!addstock PRE ETB 1 100`\n`!addstock PRE ETB 3 100 limit 1`")
        return
    price_str = parts[-1]
    qty_str = parts[-2]
    item_name = " ".join(parts[:-2])
    try:
        qty = int(qty_str)
        price = parse_price(price_str)
    except ValueError:
        await dm(ctx, f"⚠️  Couldn't read qty/price from `{qty_str}` / `{price_str}`")
        return
    key = normalize(item_name)
    stock[guild_id][key] = {"display": item_name.upper(), "qty": qty, "price": price, "limit": limit}
    limit_str = f"  •  max **{limit}** per person" if limit else "  •  no per-person limit"
    await dm(ctx, f"✅  **{item_name.upper()}** — {qty} @ ${price:.2f} each{limit_str}.")


@bot.command(name="editstock")
async def cmd_editstock(ctx, *, args=""):
    guild_id, drop_channel = get_manager_context(ctx)
    if guild_id is None:
        if not ctx.guild:
            await ctx.author.send("⚠️  No active drop session found. Run `!drop` in your server first.")
        return
    if ctx.guild:
        await silent(ctx)
    if session_state[guild_id] == "closed":
        await dm(ctx, "⚠️  No drop session active.")
        return
    parts = args.split()
    if len(parts) < 3:
        await dm(ctx, "Usage: `!editstock <item name> <qty> <price>`\nExample: `!editstock PRE ETB 2 110`")
        return
    price_str = parts[-1]
    qty_str = parts[-2]
    item_name = " ".join(parts[:-2])
    try:
        qty = int(qty_str)
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
    stock[guild_id][key]["qty"] = qty
    stock[guild_id][key]["price"] = price
    if session_state[guild_id] == "live":
        await update_stock_embed(guild_id)
    await dm(ctx, f"✅  **{stock[guild_id][key]['display']}** updated — {qty} @ ${price:.2f} each.")


@bot.command(name="removestockitem")
async def cmd_removestockitem(ctx, *, item_name=""):
    guild_id, drop_channel = get_manager_context(ctx)
    if guild_id is None:
        if not ctx.guild:
            await ctx.author.send("⚠️  No active drop session found. Run `!drop` in your server first.")
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
    await dm(ctx, f"🗑️  **{removed['display']}** removed from stock.")


@bot.command(name="preview")
async def cmd_preview(ctx):
    guild_id, drop_channel = get_manager_context(ctx)
    if guild_id is None:
        if not ctx.guild:
            await ctx.author.send("⚠️  No active drop session found. Run `!drop` in your server first.")
        return
    if ctx.guild:
        await silent(ctx)
    if not stock[guild_id]:
        await dm(ctx, "⚠️  No stock loaded yet. Use `!addstock` first.")
        return
    await ctx.author.send(content="👀  **Drop preview — this is what members will see when you !release:**", embed=build_stock_embed(guild_id))


@bot.command(name="countdown")
async def cmd_countdown(ctx, minutes: str = ""):
    guild_id, drop_channel = get_manager_context(ctx)
    if guild_id is None:
        if not ctx.guild:
            await ctx.author.send("⚠️  No active drop session found. Run `!drop` in your server first.")
        return
    if ctx.guild:
        await silent(ctx)
    try:
        mins = int(minutes)
        if mins < 1 or mins > 60:
            raise ValueError
    except ValueError:
        await dm(ctx, "⚠️  Please provide a number of minutes between 1 and 60. Example: `!countdown 5`")
        return
    await drop_channel.send(f"⏳  **Drop incoming in {mins} minute{'s' if mins != 1 else ''}!** Get ready to claim — first come, first served! 🔥")
    await dm(ctx, f"✅  Countdown posted — {mins} minute{'s' if mins != 1 else ''} until drop.")

    async def auto_remind():
        if mins >= 2:
            await asyncio.sleep((mins - 1) * 60)
            if session_state[guild_id] != "live":
                await drop_channel.send("⏰  **1 minute until the drop!** Stay ready!")
    asyncio.create_task(auto_remind())


@bot.command(name="autoclose")
async def cmd_autoclose(ctx, toggle: str = ""):
    guild_id, drop_channel = get_manager_context(ctx)
    if guild_id is None:
        if not ctx.guild:
            await ctx.author.send("⚠️  No active drop session found. Run `!drop` in your server first.")
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
        await dm(ctx, f"Auto-close is currently **{status}**. Use `!autoclose on` or `!autoclose off`.")


@bot.command(name="release")
async def cmd_release(ctx):
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
    if not stock[guild_id]:
        await dm(ctx, "⚠️  No stock loaded. Use `!addstock` first.")
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
            await ctx.author.send("⚠️  No active drop session found. Run `!drop` in your server first.")
        return
    if ctx.guild:
        await silent(ctx)
    if session_state[guild_id] != "live":
        await dm(ctx, "⚠️  No active drop to end.")
        return
    await close_drop(drop_channel, guild_id)


@bot.command(name="claimlist")
async def cmd_claimlist(ctx):
    guild_id, drop_channel = get_manager_context(ctx)
    if guild_id is None:
        if not ctx.guild:
            await ctx.author.send("⚠️  No active drop session found. Run `!drop` in your server first.")
        return
    if ctx.guild:
        await silent(ctx)
    if session_state[guild_id] == "closed":
        await dm(ctx, "No active drop.")
        return
    await ctx.author.send(embed=build_claimlist_embed(guild_id))


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


@bot.command(name="claim")
async def cmd_claim(ctx, *, args=""):
    if not ctx.guild:
        await ctx.author.send("⚠️  Please use `!claim` in your server channel, not in a DM.")
        return
    guild_id = ctx.guild.id
    if session_state[guild_id] != "live":
        await ctx.send("⚠️  No active drop right now.")
        return
    parts = args.split()
    if not parts:
        await ctx.send("Usage: `!claim <item> <qty>`  e.g. `!claim PRE ETB 1`")
        return
    try:
        qty = int(parts[-1])
        item_name = " ".join(parts[:-1])
    except ValueError:
        qty = 1
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
    info = stock[guild_id][key]
    already_claimed = sum(c["qty"] for c in claims[guild_id][key])
    remaining = info["qty"] - already_claimed
    if remaining <= 0:
        wl = waitlist[guild_id][key]
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
        allowed = info["limit"] - already_user
        if allowed <= 0:
            await ctx.send(f"⚠️  You've already claimed the max of **{info['limit']}** for **{info['display']}**.")
            return
        if qty > allowed:
            await ctx.send(f"⚠️  You can only claim **{allowed}** more of **{info['display']}** (limit: {info['limit']} per person). Try `!claim {info['display']} {allowed}`")
            return
    existing = next((c for c in claims[guild_id][key] if c["user"].id == ctx.author.id), None)
    if existing:
        existing["qty"] += qty
    else:
        claims[guild_id][key].append({"user": ctx.author, "qty": qty, "time": datetime.datetime.utcnow()})
    new_remaining = remaining - qty
    total_cost = qty * info["price"]
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
        qty = int(parts[-1])
        item_name = " ".join(parts[:-1])
    except ValueError:
        qty = None
        item_name = " ".join(parts)
    if not item_name:
        await ctx.send("Usage: `!unclaim <item> <qty>`  e.g. `!unclaim PRE ETB 1`")
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
        freed = qty
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
        await ctx.send("Usage: `!waitlist <item>`  e.g. `!waitlist PRE ETB`")
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
    info = stock[guild_id][key]
    already_claimed = sum(c["qty"] for c in claims[guild_id][key])
    remaining = info["qty"] - already_claimed
    if remaining > 0:
        await ctx.send(f"**{info['display']}** is still available! Use `!claim {info['display']} 1` to grab it.")
        return
    already_on_wl = any(u.id == ctx.author.id for u in waitlist[guild_id][key])
    if already_on_wl:
        await ctx.send(f"You're already on the waitlist for **{info['display']}**. We'll DM you if it opens up!")
        return
    waitlist[guild_id][key].append(ctx.author)
    pos = len(waitlist[guild_id][key])
    await ctx.send(f"✅  **{ctx.author.display_name}** added to the waitlist for **{info['display']}** (position #{pos}). You'll be notified if it becomes available!")


@bot.command(name="myclaims")
async def cmd_myclaims(ctx):
    if not ctx.guild:
        await ctx.author.send("⚠️  Please use `!myclaims` in your server channel.")
        return
    guild_id = ctx.guild.id
    if session_state[guild_id] != "live":
        await ctx.send("No active drop.")
        return
    user_claims = []
    total = 0.0
    for key, claim_list in claims[guild_id].items():
        for c in claim_list:
            if c["user"].id == ctx.author.id:
                subtotal = c["qty"] * stock[guild_id][key]["price"]
                total += subtotal
                user_claims.append(f"• **{stock[guild_id][key]['display']}**  ×{c['qty']}  — ${subtotal:.2f}")
    if not user_claims:
        await ctx.send("You haven't claimed anything in this drop yet.")
        return
    lines = "\n".join(user_claims)
    await ctx.send(f"**{ctx.author.display_name}'s claims:**\n{lines}\n**Total owed: ${total:.2f}**")


bot.run(BOT_TOKEN)
