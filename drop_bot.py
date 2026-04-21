"""
Vault & Pine Drop Bot
=====================
Commands (owner only unless noted):
  !drop               — Start a new drop session
  !addstock <item> <qty> <price>  — Add item to current drop
                                    e.g. !addstock gardevoir 3 85
  !stock              — Show current inventory (anyone)
  !enddrop            — End the session and print final claim list

Commands (anyone):
  !claim <item> <qty> — Claim item(s), e.g. !claim gardevoir 1
  !myclaims           — See your own claims

Setup:
  1. pip install discord.py
  2. Replace BOT_TOKEN with your bot token from discord.dev
  3. Replace OWNER_ID with your Discord user ID (right-click yourself > Copy ID)
  4. Run: python drop_bot.py
"""

import discord
from discord.ext import commands
from collections import defaultdict
import datetime

# ── CONFIG ────────────────────────────────────────────────────────────────────
import os
BOT_TOKEN = os.environ["BOT_TOKEN"]
OWNER_ID   = int(os.environ["OWNER_ID"])   # your Discord user ID (integer)
PREFIX     = "!"
# ─────────────────────────────────────────────────────────────────────────────

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix=PREFIX, intents=intents)

# ── STATE ─────────────────────────────────────────────────────────────────────
drop_active = False

# stock[item_key] = {"display": str, "qty": int, "price": float}
stock = {}

# claims[item_key] = [ {"user": member, "qty": int, "time": datetime}, ... ]
claims = defaultdict(list)
# ─────────────────────────────────────────────────────────────────────────────


def item_key(name: str) -> str:
    """Normalize item name for dict lookups."""
    return name.lower().strip()


def build_stock_embed() -> discord.Embed:
    embed = discord.Embed(
        title="🛒  Current Drop Stock",
        color=discord.Color.gold(),
        timestamp=datetime.datetime.utcnow()
    )
    if not stock:
        embed.description = "No items added yet."
        return embed

    for key, info in stock.items():
        qty_left = info["qty"] - sum(c["qty"] for c in claims[key])
        status = f"${info['price']:.2f} each  •  **{qty_left}** of {info['qty']} left"
        if qty_left <= 0:
            status += "  🚫 SOLD OUT"
        embed.add_field(name=info["display"], value=status, inline=False)

    return embed


def build_claimlist_embed() -> discord.Embed:
    embed = discord.Embed(
        title="📋  Claim List",
        color=discord.Color.blurple(),
        timestamp=datetime.datetime.utcnow()
    )
    any_claims = False
    for key, claim_list in claims.items():
        if not claim_list:
            continue
        any_claims = True
        lines = []
        for c in claim_list:
            lines.append(f"• {c['user'].display_name}  ×{c['qty']}")
        embed.add_field(
            name=stock[key]["display"] if key in stock else key,
            value="\n".join(lines),
            inline=False
        )
    if not any_claims:
        embed.description = "No claims yet."
    return embed


# ── EVENTS ────────────────────────────────────────────────────────────────────

@bot.event
async def on_ready():
    print(f"✅  Logged in as {bot.user} ({bot.user.id})")


# ── OWNER COMMANDS ────────────────────────────────────────────────────────────

@bot.command(name="drop")
async def cmd_drop(ctx):
    if ctx.author.id != OWNER_ID:
        return
    global drop_active, stock, claims
    drop_active = True
    stock = {}
    claims = defaultdict(list)
    await ctx.send(
        "🟢  **Drop is now OPEN!**\n"
        "Use `!addstock <item> <qty> <price>` to add items.\n"
        "Buyers can `!claim <item> <qty>` once stock is added."
    )


@bot.command(name="addstock")
async def cmd_addstock(ctx, *, args: str = ""):
    if ctx.author.id != OWNER_ID:
        return
    if not drop_active:
        await ctx.send("⚠️  No active drop. Use `!drop` first.")
        return

    parts = args.rsplit(maxsplit=2)          # split from right so item name can have spaces
    if len(parts) < 3:
        await ctx.send("Usage: `!addstock <item name> <qty> <price>`\nExample: `!addstock gardevoir poster 3 85`")
        return

    item_name, qty_str, price_str = parts[0], parts[1], parts[2]

    try:
        qty   = int(qty_str)
        price = float(price_str)
    except ValueError:
        await ctx.send("⚠️  Qty must be a whole number and price a number. Example: `!addstock gardevoir 3 85`")
        return

    key = item_key(item_name)
    stock[key] = {"display": item_name.title(), "qty": qty, "price": price}

    await ctx.send(
        f"✅  Added **{item_name.title()}** — {qty} available @ ${price:.2f} each.\n"
        f"Use `!stock` to see the full listing."
    )


@bot.command(name="enddrop")
async def cmd_enddrop(ctx):
    if ctx.author.id != OWNER_ID:
        return
    global drop_active
    if not drop_active:
        await ctx.send("⚠️  No active drop.")
        return
    drop_active = False
    embed = build_claimlist_embed()
    embed.title = "🔴  Drop CLOSED — Final Claim List"
    await ctx.send(embed=embed)


# ── PUBLIC COMMANDS ───────────────────────────────────────────────────────────

@bot.command(name="stock")
async def cmd_stock(ctx):
    if not drop_active:
        await ctx.send("No drop is currently active.")
        return
    await ctx.send(embed=build_stock_embed())


@bot.command(name="claim")
async def cmd_claim(ctx, *, args: str = ""):
    if not drop_active:
        await ctx.send("⚠️  No active drop right now.")
        return

    parts = args.rsplit(maxsplit=1)
    if len(parts) == 2:
        item_name, qty_str = parts
        try:
            qty = int(qty_str)
        except ValueError:
            item_name = args
            qty = 1
    elif len(parts) == 1:
        item_name = parts[0]
        qty = 1
    else:
        await ctx.send("Usage: `!claim <item> <qty>`  e.g. `!claim gardevoir 1`")
        return

    if qty < 1:
        await ctx.send("⚠️  Qty must be at least 1.")
        return

    key = item_key(item_name)

    # fuzzy match — find closest key
    if key not in stock:
        matches = [k for k in stock if item_name.lower() in k or k in item_name.lower()]
        if len(matches) == 1:
            key = matches[0]
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
        await ctx.send(
            f"⚠️  Only **{remaining}** of **{info['display']}** left. "
            f"You asked for {qty}. Want to claim {remaining} instead? Try `!claim {info['display']} {remaining}`"
        )
        return

    # Check if this user already has a claim for this item and add to it
    existing = next((c for c in claims[key] if c["user"].id == ctx.author.id), None)
    if existing:
        existing["qty"] += qty
    else:
        claims[key].append({
            "user": ctx.author,
            "qty": qty,
            "time": datetime.datetime.utcnow()
        })

    new_remaining = remaining - qty
    total_cost = qty * info["price"]

    await ctx.send(
        f"✅  **{ctx.author.display_name}** claimed **{qty}x {info['display']}** "
        f"(${total_cost:.2f} total)  •  {new_remaining} left"
    )


@bot.command(name="myclaims")
async def cmd_myclaims(ctx):
    if not drop_active:
        await ctx.send("No active drop.")
        return

    user_claims = []
    total = 0.0
    for key, claim_list in claims.items():
        for c in claim_list:
            if c["user"].id == ctx.author.id:
                subtotal = c["qty"] * stock[key]["price"]
                total += subtotal
                user_claims.append(f"• {stock[key]['display']}  ×{c['qty']}  — ${subtotal:.2f}")

    if not user_claims:
        await ctx.send("You haven't claimed anything yet.")
        return

    lines = "\n".join(user_claims)
    await ctx.send(
        f"**Your claims, {ctx.author.display_name}:**\n{lines}\n"
        f"**Total owed: ${total:.2f}**"
    )


@bot.command(name="claimlist")
async def cmd_claimlist(ctx):
    if ctx.author.id != OWNER_ID:
        return
    if not drop_active:
        await ctx.send("No active drop.")
        return
    await ctx.send(embed=build_claimlist_embed())


bot.run(BOT_TOKEN)
