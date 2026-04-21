"""
Vault & Pine Drop Bot
=====================
Owner commands (bot deletes your message silently, confirms via DM):
  !drop                            — Start a new drop session
  !addstock <item> <qty> <price>   — Add item, e.g. !addstock PRE ETB 1 $100
                                     Price can be $100 or 100
  !removestockitem <item>          — Remove an item from staging
  !release                         — Post the drop publicly and open claiming
  !claimlist                       — See who claimed what (owner only)
  !enddrop                         — Close drop, post final list, DM all claimers

Public commands (anyone):
  !claim <item> <qty>              — e.g. !claim PRE ETB 1
  !unclaim <item>                  — Drop your claim on an item
  !stock                           — Show current inventory
  !myclaims                        — See your own claims and total
"""

import discord
from discord.ext import commands
from collections import defaultdict
import datetime
import os

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
OWNER_ID = int(os.environ.get("OWNER_ID", "0"))
PREFIX = "!"

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix=PREFIX, intents=intents)

# session_state: "closed" | "staging" | "live"
session_state = "closed"
stock = {}
claims = defaultdict(list)
stock_message = None  # tracks the live stock embed message for auto-updating
pinned_message = None  # tracks the pinned message


def normalize(name):
    return name.lower().strip()


def parse_price(price_str):
    return float(price_str.lstrip("$"))


def build_stock_embed():
    embed = discord.Embed(title="🛒  Drop Stock", color=discord.Color.gold(), timestamp=datetime.datetime.utcnow())
    for key, info in stock.items():
        claimed = sum(c["qty"] for c in claims[key])
        qty_left = info["qty"] - claimed
        status = f"**${info['price']:.2f}** each  •  **{qty_left}** of {info['qty']} remaining"
        if qty_left <= 0:
            status += "  🚫 **SOLD OUT**"
        embed.add_field(name=info["display"], value=status, inline=False)
    return embed


def build_claimlist_embed(title="📋  Claim List"):
    embed = discord.Embed(title=title, color=discord.Color.blurple(), timestamp=datetime.datetime.utcnow())
    any_claims = False
    for key, claim_list in claims.items():
        if not claim_list:
            continue
        any_claims = True
        lines = [f"• **{c['user'].display_name}**  ×{c['qty']}  — ${c['qty'] * stock[key]['price']:.2f}" for c in claim_list]
        embed.add_field(name=stock[key]["display"] if key in stock else key, value="\n".join(lines), inline=False)
    if not any_claims:
        embed.description = "No claims yet."
    return embed


async def update_stock_embed():
    """Edit the live stock message in place."""
    global stock_message
    if stock_message:
        try:
            await stock_message.edit(embed=build_stock_embed())
        except (discord.NotFound, discord.Forbidden):
            stock_message = None


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


async def dm_user(user, message):
    try:
        await user.send(message)
    except discord.Forbidden:
        pass


@bot.event
async def on_ready():
    print(f"✅  Logged in as {bot.user} ({bot.user.id})")


# ── OWNER COMMANDS ────────────────────────────────────────────────────────────

@bot.command(name="drop")
async def cmd_drop(ctx):
    global session_state, stock, claims, stock_message, pinned_message
    if ctx.author.id != OWNER_ID:
        return
    session_state = "staging"
    stock = {}
    claims = defaultdict(list)
    stock_message = None
    pinned_message = None
    await silent(ctx)
    await dm(ctx, "✅  Drop session started! Load items with `!addstock <item> <qty> <price>`, then `!release` to go live.")


@bot.command(name="addstock")
async def cmd_addstock(ctx, *, args=""):
    global stock
    if ctx.author.id != OWNER_ID:
        return
    await silent(ctx)
    if session_state == "closed":
        await dm(ctx, "⚠️  No drop session active. Use `!drop` first.")
        return
    parts = args.split()
    if len(parts) < 3:
        await dm(ctx, "Usage: `!addstock <item name> <qty> <price>`\nExample: `!addstock PRE ETB 1 100`")
        return
    price_str = parts[-1]
    qty_str = parts[-2]
    item_name = " ".join(parts[:-2])
    try:
        qty = int(qty_str)
        price = parse_price(price_str)
    except ValueError:
        await dm(ctx, f"⚠️  Couldn't read qty/price from `{qty_str}` / `{price_str}`\nFormat: `!addstock PRE ETB 1 100`")
        return
    key = normalize(item_name)
    stock[key] = {"display": item_name.upper(), "qty": qty, "price": price}
    await dm(ctx, f"✅  **{item_name.upper()}** — {qty} @ ${price:.2f} each added.")


@bot.command(name="removestockitem")
async def cmd_removestockitem(ctx, *, item_name=""):
    global stock
    if ctx.author.id != OWNER_ID:
        return
    await silent(ctx)
    if not item_name:
        await dm(ctx, "Usage: `!removestockitem <item name>`")
        return
    key = normalize(item_name)
    if key not in stock:
        # fuzzy match
        matches = [k for k in stock if normalize(item_name) in k or k in normalize(item_name)]
        if len(matches) == 1:
            key = matches[0]
        else:
            names = ", ".join(f"`{s['display']}`" for s in stock.values())
            await dm(ctx, f"⚠️  Item not found. Current stock: {names}")
            return
    removed = stock.pop(key)
    await dm(ctx, f"🗑️  **{removed['display']}** removed from stock.")


@bot.command(name="release")
async def cmd_release(ctx):
    global session_state, stock_message, pinned_message
    if ctx.author.id != OWNER_ID:
        return
    await silent(ctx)
    if session_state == "closed":
        await dm(ctx, "⚠️  No drop session active. Use `!drop` first.")
        return
    if not stock:
        await dm(ctx, "⚠️  No stock loaded. Use `!addstock` first.")
        return
    session_state = "live"
    # Post stock embed and save reference for live updates
    stock_message = await ctx.send(embed=build_stock_embed())
    await ctx.send("🟢  **Drop is LIVE!**  Use `!claim <item> <qty>` to grab yours — first come, first served!")
    # Auto-pin the stock embed
    try:
        await stock_message.pin()
        pinned_message = stock_message
    except (discord.Forbidden, discord.HTTPException):
        pass


@bot.command(name="enddrop")
async def cmd_enddrop(ctx):
    global session_state, pinned_message
    if ctx.author.id != OWNER_ID:
        return
    await silent(ctx)
    if session_state != "live":
        await dm(ctx, "⚠️  No active drop to end.")
        return
    session_state = "closed"

    # Unpin the stock embed
    if pinned_message:
        try:
            await pinned_message.unpin()
        except (discord.Forbidden, discord.HTTPException):
            pass
        pinned_message = None

    # Post final claim list
    embed = build_claimlist_embed(title="🔴  Drop CLOSED — Final Claim List")
    await ctx.send(embed=embed)

    # DM every claimer their summary
    claimer_totals = defaultdict(list)
    for key, claim_list in claims.items():
        for c in claim_list:
            subtotal = c["qty"] * stock[key]["price"]
            claimer_totals[c["user"]].append(f"• **{stock[key]['display']}**  ×{c['qty']}  — ${subtotal:.2f}")

    for user, lines in claimer_totals.items():
        total = sum(
            c["qty"] * stock[key]["price"]
            for key, claim_list in claims.items()
            for c in claim_list
            if c["user"].id == user.id
        )
        summary = "\n".join(lines)
        await dm_user(user, f"🧾  **Drop closed! Here's your order summary:**\n{summary}\n**Total owed: ${total:.2f}**\n\nPlease send payment to complete your order!")


@bot.command(name="claimlist")
async def cmd_claimlist(ctx):
    if ctx.author.id != OWNER_ID:
        return
    await silent(ctx)
    if session_state == "closed":
        await dm(ctx, "No active drop.")
        return
    await ctx.send(embed=build_claimlist_embed())


# ── PUBLIC COMMANDS ───────────────────────────────────────────────────────────

@bot.command(name="stock")
async def cmd_stock(ctx):
    if session_state != "live":
        await ctx.send("No drop is currently active.")
        return
    await ctx.send(embed=build_stock_embed())


@bot.command(name="claim")
async def cmd_claim(ctx, *, args=""):
    if session_state != "live":
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
    if key not in stock:
        matches = [k for k in stock if normalize(item_name) in k or k in normalize(item_name)]
        if len(matches) == 1:
            key = matches[0]
        elif len(matches) > 1:
            names = ", ".join(f"`{stock[k]['display']}`" for k in matches)
            await ctx.send(f"⚠️  Multiple matches: {names} — be more specific.")
            return
        else:
            names = ", ".join(f"`{s['display']}`" for s in stock.values())
            await ctx.send(f"⚠️  Item not found. Available: {names}")
            return
    info = stock[key]
    already_claimed = sum(c["qty"] for c in claims[key])
    remaining = info["qty"] - already_claimed
    if remaining <= 0:
        await ctx.send(f"😔  **{info['display']}** is sold out!")
        return
    if qty > remaining:
        await ctx.send(f"⚠️  Only **{remaining}** of **{info['display']}** left. Try `!claim {info['display']} {remaining}`")
        return
    existing = next((c for c in claims[key] if c["user"].id == ctx.author.id), None)
    if existing:
        existing["qty"] += qty
    else:
        claims[key].append({"user": ctx.author, "qty": qty, "time": datetime.datetime.utcnow()})
    new_remaining = remaining - qty
    total_cost = qty * info["price"]
    await ctx.send(f"✅  **{ctx.author.display_name}** claimed **{qty}x {info['display']}** — ${total_cost:.2f}  •  {new_remaining} left")
    await update_stock_embed()


@bot.command(name="unclaim")
async def cmd_unclaim(ctx, *, item_name=""):
    if session_state != "live":
        await ctx.send("⚠️  No active drop right now.")
        return
    if not item_name:
        await ctx.send("Usage: `!unclaim <item>`  e.g. `!unclaim PRE ETB`")
        return
    key = normalize(item_name)
    if key not in stock:
        matches = [k for k in stock if normalize(item_name) in k or k in normalize(item_name)]
        if len(matches) == 1:
            key = matches[0]
        else:
            names = ", ".join(f"`{s['display']}`" for s in stock.values())
            await ctx.send(f"⚠️  Item not found. Available: {names}")
            return
    existing = next((c for c in claims[key] if c["user"].id == ctx.author.id), None)
    if not existing:
        await ctx.send("You don't have a claim on that item.")
        return
    claims[key].remove(existing)
    await ctx.send(f"↩️  **{ctx.author.display_name}** removed their claim on **{stock[key]['display']}**.")
    await update_stock_embed()


@bot.command(name="myclaims")
async def cmd_myclaims(ctx):
    if session_state != "live":
        await ctx.send("No active drop.")
        return
    user_claims = []
    total = 0.0
    for key, claim_list in claims.items():
        for c in claim_list:
            if c["user"].id == ctx.author.id:
                subtotal = c["qty"] * stock[key]["price"]
                total += subtotal
                user_claims.append(f"• **{stock[key]['display']}**  ×{c['qty']}  — ${subtotal:.2f}")
    if not user_claims:
        await ctx.send("You haven't claimed anything in this drop yet.")
        return
    lines = "\n".join(user_claims)
    await ctx.send(f"**{ctx.author.display_name}'s claims:**\n{lines}\n**Total owed: ${total:.2f}**")


bot.run(BOT_TOKEN)
