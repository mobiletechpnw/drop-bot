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
  !enddrop

Public commands (anyone, in server only):
  !claim <item> <qty>
  !unclaim <item> <qty>
  !waitlist <item>
  !paid <method> <amount>          — e.g. !paid venmo $125 — works during and after drop
  !stock
  !myclaims
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
intents.reactions = True
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
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS server_settings (
                guild_id BIGINT PRIMARY KEY,
                drop_channel_id BIGINT,
                venmo TEXT,
                zelle TEXT,
                cashapp TEXT,
                applepay TEXT
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS drop_history (
                id SERIAL PRIMARY KEY,
                guild_id BIGINT NOT NULL,
                closed_at TIMESTAMP NOT NULL,
                total_revenue NUMERIC NOT NULL,
                total_items INT NOT NULL,
                unique_buyers INT NOT NULL,
                summary JSONB NOT NULL
            )
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
                "drop_channel_id": row["drop_channel_id"],
                "venmo": row["venmo"],
                "zelle": row["zelle"],
                "cashapp": row["cashapp"],
                "applepay": row["applepay"],
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
    import json
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO drop_history (guild_id, closed_at, total_revenue, total_items, unique_buyers, summary)
            VALUES ($1, $2, $3, $4, $5, $6)
        """, guild_id, datetime.datetime.utcnow(), revenue, total_items, unique_buyers, json.dumps(summary))


async def db_save_settings(guild_id):    s = server_settings.get(guild_id, {})
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO server_settings (guild_id, drop_channel_id, venmo, zelle, cashapp, applepay)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (guild_id) DO UPDATE SET
                drop_channel_id = $2, venmo = $3, zelle = $4, cashapp = $5, applepay = $6
        """, guild_id,
            s.get("drop_channel_id"),
            s.get("venmo"),
            s.get("zelle"),
            s.get("cashapp"),
            s.get("applepay"),
        )

# ── PER-SERVER STATE ──────────────────────────────────────────────────────────

server_admins = {}
server_managers = defaultdict(set)
server_settings = {}  # guild_id -> {drop_channel_id, venmo, zelle, cashapp, applepay}
session_state = defaultdict(lambda: "closed")
stock = defaultdict(dict)
claims = defaultdict(lambda: defaultdict(list))
waitlist = defaultdict(lambda: defaultdict(list))
stock_message = {}
pinned_message = {}
autoclose = defaultdict(lambda: True)
manager_session = {}

# payments[guild_id][user_id] = [{"method": str, "amount": float, "time": datetime, "confirmed": bool}, ...]
payments = defaultdict(lambda: defaultdict(list))

# payment_board_message[guild_id] = Message (the live payment board embed)
payment_board_message = {}

# pending_payment_messages[message_id] = {"guild_id": int, "buyer_id": int}
# Maps manager ping messages to the buyer so ✅ reaction confirms them
pending_payment_messages = {}

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


def get_drop_channel(guild):
    """Get the configured drop channel for a guild."""
    s = server_settings.get(guild.id, {})
    channel_id = s.get("drop_channel_id")
    if channel_id:
        return guild.get_channel(channel_id)
    return None


def build_payment_info(guild_id):
    """Build a formatted payment info string from server settings."""
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
    embed.add_field(name="!waitlist <item>", value="Join the waitlist if something is sold out", inline=False)
    embed.add_field(name="!paid <method> <amount>", value="Confirm your payment — e.g. `!paid venmo $125`", inline=False)
    embed.set_footer(text="First come, first served — when it's gone, it's gone!")
    return embed


def build_payment_board_embed(guild_id):
    """Build the live payment board showing confirmed payments."""
    embed = discord.Embed(
        title="💳  Payment Board",
        color=discord.Color.green(),
        timestamp=datetime.datetime.utcnow()
    )
    confirmed_lines = []
    for user_id, user_payments in payments[guild_id].items():
        confirmed = [p for p in user_payments if p["confirmed"]]
        if confirmed:
            total = sum(p["amount"] for p in confirmed)
            methods = ", ".join(f"{p['method'].title()} ${p['amount']:.2f}" for p in confirmed)
            confirmed_lines.append(f"✅  <@{user_id}> — {methods}  •  **${total:.2f} total**")
    if confirmed_lines:
        embed.description = "\n".join(confirmed_lines)
    else:
        embed.description = "No payments confirmed yet."
    embed.set_footer(text="Updated automatically as payments are confirmed")
    return embed


async def update_payment_board(guild_id):
    """Edit the payment board message in place."""
    msg = payment_board_message.get(guild_id)
    if msg:
        try:
            await msg.edit(embed=build_payment_board_embed(guild_id))
        except (discord.NotFound, discord.Forbidden):
            payment_board_message.pop(guild_id, None)


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

    # Post payment board in drop channel
    board_msg = await channel.send(embed=build_payment_board_embed(guild_id))
    payment_board_message[guild_id] = board_msg

    payment_info = build_payment_info(guild_id)

    # Save drop history to database
    history_summary = {}
    total_revenue = 0.0
    total_items = 0
    buyers = set()
    for key, claim_list in claims[guild_id].items():
        if not claim_list:
            continue
        item_display = stock[guild_id][key]["display"] if key in stock[guild_id] else key
        item_price = stock[guild_id][key]["price"] if key in stock[guild_id] else 0
        item_qty = sum(c["qty"] for c in claim_list)
        item_revenue = item_qty * item_price
        total_revenue += item_revenue
        total_items += item_qty
        for c in claim_list:
            buyers.add(c["user"].id)
        history_summary[item_display] = {"qty": item_qty, "revenue": item_revenue}
    await db_save_drop_history(guild_id, total_revenue, total_items, len(buyers), history_summary)

    # DM every claimer their summary + payment info
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
    """Walk the admin through setting up payment info via DM."""
    def check(m):
        return m.author.id == user.id and isinstance(m.channel, discord.DMChannel)

    fields = [
        ("venmo", "💜  What is your **Venmo** handle? (e.g. @yourname) — type `skip` to leave blank"),
        ("zelle", "💙  What is your **Zelle** phone number or email? — type `skip` to leave blank"),
        ("cashapp", "💚  What is your **Cash App** handle? (e.g. $yourname) — type `skip` to leave blank"),
        ("applepay", "🍎  What is your **Apple Pay** phone number? — type `skip` to leave blank"),
    ]

    if guild_id not in server_settings:
        server_settings[guild_id] = {}

    for field, prompt in fields:
        await user.send(prompt)
        try:
            msg = await bot.wait_for("message", check=check, timeout=120)
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
    """Ask the admin which channel drops should go in."""
    def check(m):
        return m.author.id == user.id and isinstance(m.channel, discord.DMChannel)

    await user.send(
        f"📢  Which channel should drops be posted in?\n"
        f"Please **mention the channel** in your server (e.g. `#drops`)."
    )

    try:
        msg = await bot.wait_for("message", check=check, timeout=120)
        if msg.channel_mentions:
            channel = msg.channel_mentions[0]
            if guild_id not in server_settings:
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


@bot.event
async def on_ready():
    await init_db()
    await db_load_all()
    print(f"✅  Logged in as {bot.user} ({bot.user.id})")


@bot.event
async def on_reaction_add(reaction, user):
    """Allow managers to confirm payment by reacting ✅ to the ping message."""
    if user.bot:
        return
    if str(reaction.emoji) != "✅":
        return
    msg_id = reaction.message.id
    if msg_id not in pending_payment_messages:
        return

    data = pending_payment_messages[msg_id]
    guild_id = data["guild_id"]
    buyer_id = data["buyer_id"]

    # Only managers can confirm
    if not is_manager(guild_id, user.id):
        return

    # Mark pending payments as confirmed
    user_pmts = payments[guild_id][buyer_id]
    pending = [p for p in user_pmts if not p["confirmed"]]
    if not pending:
        return

    for p in pending:
        p["confirmed"] = True

    total_confirmed = sum(p["amount"] for p in pending)

    # Remove from pending so it can't be double-confirmed
    del pending_payment_messages[msg_id]

    # Update payment board
    await update_payment_board(guild_id)

    # DM the buyer
    guild = reaction.message.guild
    buyer = guild.get_member(buyer_id)
    if buyer:
        try:
            await buyer.send(f"✅  Your payment of **${total_confirmed:.2f}** has been confirmed! Thanks so much — enjoy your order! 🎉")
        except discord.Forbidden:
            pass

    # Edit the ping message to show it's been handled
    try:
        await reaction.message.edit(content=reaction.message.content + f"\n✅  Confirmed by {user.display_name}")
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

    # Walk through drop channel + payment info setup via DM
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
    lines = [f"👑  Admin: <@{admin_id}>"]
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

    # Use configured drop channel if set, otherwise use current channel
    drop_ch = get_drop_channel(ctx.guild) or ctx.channel

    session_state[guild_id] = "staging"
    stock[guild_id] = {}
    claims[guild_id] = defaultdict(list)
    waitlist[guild_id] = defaultdict(list)
    payments[guild_id] = defaultdict(list)
    stock_message.pop(guild_id, None)
    pinned_message.pop(guild_id, None)
    payment_board_message.pop(guild_id, None)
    autoclose[guild_id] = True
    manager_session[ctx.author.id] = {"guild_id": guild_id, "channel": drop_ch}
    await silent(ctx)
    await dm(ctx, f"✅  Drop session started for **{ctx.guild.name}**! Drop will post in **#{drop_ch.name}**.\n\nYou can now use `!addstock`, `!editstock`, `!removestockitem`, `!preview`, `!countdown`, `!release`, `!claimlist`, `!autoclose`, and `!enddrop` from this DM or in the server.\n\n💡  Auto-close is **ON**.")


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
            await ctx.author.send("⚠️  No active drop session found. Run `!drop` first.")
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
    guild_id, drop_channel = get_manager_context(ctx)
    if guild_id is None:
        if not ctx.guild:
            await ctx.author.send("⚠️  No active drop session found.")
        return
    if ctx.guild:
        await silent(ctx)

    # Build list of claimers who have not been fully confirmed
    claimer_totals = {}
    for key, claim_list in claims[guild_id].items():
        for c in claim_list:
            uid = c["user"].id
            subtotal = c["qty"] * stock[guild_id][key]["price"]
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

    await ctx.author.send(f"⏳  **Unpaid claimers:**\n" + "\n".join(unpaid_lines))


@bot.command(name="confirm")
async def cmd_confirm(ctx):
    guild_id, drop_channel = get_manager_context(ctx)
    if guild_id is None:
        if not ctx.guild:
            await ctx.author.send("⚠️  No active drop session found.")
        return
    if ctx.guild:
        await silent(ctx)
    if not ctx.message.mentions:
        await dm(ctx, "Usage: `!confirm @user`")
        return

    user = ctx.message.mentions[0]

    # Mark all their pending payments as confirmed
    user_pmts = payments[guild_id][user.id]
    pending = [p for p in user_pmts if not p["confirmed"]]
    if not pending:
        await dm(ctx, f"⚠️  No pending payments found for **{user.display_name}**.")
        return

    for p in pending:
        p["confirmed"] = True

    total_confirmed = sum(p["amount"] for p in pending)

    # Update payment board
    await update_payment_board(guild_id)

    # DM the buyer
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
async def cmd_paid(ctx, *, args=""):
    if not ctx.guild:
        await ctx.author.send("⚠️  Please use `!paid` in your server channel.")
        return
    guild_id = ctx.guild.id

    # !paid works during AND after drop — just needs claims to exist
    has_claims = any(
        c["user"].id == ctx.author.id
        for claim_list in claims[guild_id].values()
        for c in claim_list
    )
    if not has_claims:
        await ctx.author.send("⚠️  You don't have any claims in the current drop.")
        await silent(ctx)
        return

    parts = args.split()
    if len(parts) < 2:
        await ctx.author.send("Usage: `!paid <method> <amount>`  e.g. `!paid venmo $125`")
        await silent(ctx)
        return

    method = parts[0].lower()
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

    # Delete buyer's message from channel to keep chat clean
    await silent(ctx)

    # Log the payment
    payments[guild_id][ctx.author.id].append({
        "method": method,
        "amount": amount,
        "time": datetime.datetime.utcnow(),
        "confirmed": False
    })

    # Calculate what they owe
    total_owed = sum(
        c["qty"] * stock[guild_id][key]["price"]
        for key, claim_list in claims[guild_id].items()
        for c in claim_list
        if c["user"].id == ctx.author.id
    )
    total_paid = sum(p["amount"] for p in payments[guild_id][ctx.author.id])
    remaining = total_owed - total_paid

    # DM the buyer their confirmation — not posted in channel
    await ctx.author.send(
        f"💳  Payment of **${amount:.2f}** via **{method.title()}** received!\n"
        f"Total owed: ${total_owed:.2f}  •  Total reported: ${total_paid:.2f}"
        + (f"  •  Still outstanding: **${remaining:.2f}**" if remaining > 0.01 else "  •  ✅ Fully reported! Waiting on confirmation.")
    )

    # Ping managers with a ✅ reaction they can click to confirm
    drop_ch = get_drop_channel(ctx.guild) or ctx.channel
    manager_mentions = " ".join(f"<@{uid}>" for uid in server_managers[guild_id])
    ping_msg = await drop_ch.send(
        f"💰  {manager_mentions} — **{ctx.author.display_name}** reported payment of **${amount:.2f}** via **{method.title()}**.\n"
        f"React ✅ to confirm or use `!confirm @{ctx.author.display_name}`.",
        allowed_mentions=discord.AllowedMentions(users=True)
    )
    await ping_msg.add_reaction("✅")
    # Store message -> buyer mapping so reaction handler knows who to confirm
    pending_payment_messages[ping_msg.id] = {"guild_id": guild_id, "buyer_id": ctx.author.id}


@bot.command(name="claim")
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
            await ctx.send(f"⚠️  You can only claim **{allowed}** more of **{info['display']}** (limit: {info['limit']} per person).")
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
    info = stock[guild_id][key]
    already_claimed = sum(c["qty"] for c in claims[guild_id][key])
    remaining = info["qty"] - already_claimed
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


@bot.command(name="history")
async def cmd_history(ctx):
    if not ctx.guild:
        await ctx.author.send("⚠️  Please run `!history` in your server channel.")
        return
    guild_id = ctx.guild.id
    if not is_manager(guild_id, ctx.author.id):
        return
    await silent(ctx)

    import json
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT closed_at, total_revenue, total_items, unique_buyers, summary
            FROM drop_history
            WHERE guild_id = $1
            ORDER BY closed_at DESC
            LIMIT 10
        """, guild_id)

    if not rows:
        await ctx.author.send("No drop history yet. History is saved automatically when you run `!enddrop`.")
        return

    embed = discord.Embed(
        title="📊  Drop History (Last 10)",
        color=discord.Color.blurple(),
        timestamp=datetime.datetime.utcnow()
    )

    all_revenue = 0.0
    all_items = 0
    for i, row in enumerate(rows):
        summary = json.loads(row["summary"])
        date_str = row["closed_at"].strftime("%b %d, %Y")
        item_lines = "\n".join(
            f"• {item}: ×{data['qty']} — ${data['revenue']:.2f}"
            for item, data in summary.items()
        )
        embed.add_field(
            name=f"Drop #{len(rows) - i}  •  {date_str}  •  ${row['total_revenue']:.2f}",
            value=f"{item_lines}\n{row['total_items']} items  •  {row['unique_buyers']} buyer(s)",
            inline=False
        )
        all_revenue += float(row["total_revenue"])
        all_items += row["total_items"]

    embed.set_footer(text=f"All-time total: ${all_revenue:.2f} across {all_items} items")
    await ctx.author.send(embed=embed)


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

    user = ctx.message.mentions[0]

    # Calculate what they owe
    total_owed = sum(
        c["qty"] * stock[guild_id][key]["price"]
        for key, claim_list in claims[guild_id].items()
        for c in claim_list
        if c["user"].id == user.id
    )
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

    # Build list of unpaid claimers
    unpaid_mentions = []
    for key, claim_list in claims[guild_id].items():
        for c in claim_list:
            uid = c["user"].id
            total_owed = sum(
                cc["qty"] * stock[guild_id][k]["price"]
                for k, cl in claims[guild_id].items()
                for cc in cl
                if cc["user"].id == uid
            )
            confirmed = sum(p["amount"] for p in payments[guild_id][uid] if p["confirmed"])
            if total_owed - confirmed > 0.01:
                mention = f"<@{uid}>"
                if mention not in unpaid_mentions:
                    unpaid_mentions.append(mention)

    if not unpaid_mentions:
        await dm(ctx, "✅  Everyone has been confirmed — no outstanding payments!")
        return

    payment_info = build_payment_info(guild_id)
    mentions_str = " ".join(unpaid_mentions)

    await drop_channel.send(
        f"⏰  **Payment Reminder** — {mentions_str}\n\n"
        f"You have an outstanding balance from the recent drop. Please send payment using one of these methods:\n"
        f"{payment_info}\n\n"
        f"Once sent, type `!paid <method> <amount>` to confirm. Thanks!"
    )
    await dm(ctx, f"✅  Reminder posted for {len(unpaid_mentions)} unpaid buyer(s).")


bot.run(BOT_TOKEN)
