import logging
import sqlite3
import random
import asyncio
import re
import time
import math
import os
from uuid import uuid4
from telegram import (
    Update, 
    InlineKeyboardButton, 
    InlineKeyboardMarkup, 
    InputMediaPhoto, 
    InputMediaVideo,
    InlineQueryResultCachedPhoto, 
    InlineQueryResultCachedVideo,
    ChatMember
)
from telegram.constants import ParseMode, ChatMemberStatus
from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    InlineQueryHandler,
    ChatMemberHandler,
    filters,
)

# Enable logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.ERROR
)

# --- CONFIGURATION ---
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", '8229559916:AAGb35L9irAWAdn8tzrcqakaoPYNMpphUHE')
OWNER_ID = int(os.getenv("OWNER_ID", "190053552"))
DATABASE_CHANNEL_ID = -1003500704602  # Main Channel
DATABASE_CHANNEL_ID2 = "@database_test"  # Secondary Channel (SFW)

# --- FORCE SUBSCRIBE CONFIG ---
SUPPORT_GROUPS = [] 

# --- DATABASE SETUP ---
DB_NAME = "anime_catcher.db"

def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS sudos (user_id INTEGER PRIMARY KEY)''')
    c.execute('''CREATE TABLE IF NOT EXISTS uploaders (user_id INTEGER PRIMARY KEY)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS characters (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        surname TEXT,
        extra_name TEXT,
        rarity INTEGER,
        file_id TEXT,
        log_msg_id1 INTEGER,
        log_msg_id2 INTEGER,
        media_type INTEGER DEFAULT 0
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS harem (
        user_id INTEGER,
        character_id INTEGER,
        count INTEGER DEFAULT 1,
        PRIMARY KEY (user_id, character_id)
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS groups (
        chat_id INTEGER PRIMARY KEY,
        last_spawn_id INTEGER DEFAULT 0,
        spawn_threshold INTEGER DEFAULT 50,
        hentai_mode BOOLEAN DEFAULT 0,
        game_mode BOOLEAN DEFAULT 1,
        sfw_mode BOOLEAN DEFAULT 0
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        first_name TEXT,
        username TEXT,
        fav_character_id INTEGER DEFAULT 0,
        rarity_filter INTEGER DEFAULT 0,
        crystal INTEGER DEFAULT 0,
        gem REAL DEFAULT 0,
        last_daily REAL DEFAULT 0,
        last_weekly REAL DEFAULT 0
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS user_potions (
        user_id INTEGER,
        potion_id INTEGER,
        uses_left INTEGER DEFAULT 0,
        PRIMARY KEY (user_id, potion_id)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS referrals (
        referrer_id INTEGER,
        referred_id INTEGER PRIMARY KEY,
        m1_claimed BOOLEAN DEFAULT 0,
        m2_claimed BOOLEAN DEFAULT 0
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS banned_users (
        user_id INTEGER PRIMARY KEY
    )''')

    # --- NEW COLLECTION TABLES ---
    c.execute('''CREATE TABLE IF NOT EXISTS collections (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        reward_id INTEGER
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS collection_items (
        collection_id INTEGER,
        char_id INTEGER,
        PRIMARY KEY (collection_id, char_id)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS collection_completions (
        user_id INTEGER,
        collection_id INTEGER,
        PRIMARY KEY (user_id, collection_id)
    )''')

    # --- MIGRATIONS & FIXES ---
    try: c.execute("ALTER TABLE characters ADD COLUMN log_msg_id1 INTEGER")
    except: pass
    try: c.execute("ALTER TABLE characters ADD COLUMN log_msg_id2 INTEGER")
    except: pass
    try: c.execute("ALTER TABLE groups ADD COLUMN game_mode BOOLEAN DEFAULT 1")
    except: pass
    try: c.execute("ALTER TABLE groups ADD COLUMN sfw_mode BOOLEAN DEFAULT 0")
    except: pass
    try: c.execute("ALTER TABLE users ADD COLUMN first_name TEXT")
    except: pass
    try: c.execute("ALTER TABLE users ADD COLUMN username TEXT")
    except: pass
    try: c.execute("ALTER TABLE users RENAME COLUMN gold TO crystal")
    except: 
        try: c.execute("ALTER TABLE users ADD COLUMN crystal INTEGER DEFAULT 0")
        except: pass
    try: c.execute("ALTER TABLE characters ADD COLUMN media_type INTEGER DEFAULT 0")
    except: pass
    try: c.execute("ALTER TABLE users ADD COLUMN gem REAL DEFAULT 0")
    except: pass
    try: c.execute("ALTER TABLE users ADD COLUMN last_daily REAL DEFAULT 0")
    except: pass
    try: c.execute("ALTER TABLE users ADD COLUMN last_weekly REAL DEFAULT 0")
    except: pass
    
    try:
        c.execute("SELECT potion_id FROM user_potions LIMIT 1")
    except sqlite3.OperationalError:
        print("Fixing user_potions table schema...")
        c.execute("DROP TABLE IF EXISTS user_potions")
        c.execute('''CREATE TABLE user_potions (
            user_id INTEGER,
            potion_id INTEGER,
            uses_left INTEGER DEFAULT 0,
            PRIMARY KEY (user_id, potion_id)
        )''')

    try:
        c.execute("INSERT OR IGNORE INTO sudos (user_id) VALUES (?)", (OWNER_ID,))
        c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", ("event_text", "❄️ Winter\n👤 cosplay"))
    except:
        pass
    
    conn.commit()
    conn.close()

# --- CONTINUOUS ID HELPER ---
def get_next_id(c, table_name):
    """Finds the first available ID (gap) in the specified table."""
    c.execute(f"SELECT id FROM {table_name} ORDER BY id ASC LIMIT 1")
    first = c.fetchone()
    if not first or first[0] > 1:
        return 1
    
    query = f"""
        SELECT id + 1 FROM {table_name} mo
        WHERE NOT EXISTS (
            SELECT 1 FROM {table_name} mi WHERE mi.id = mo.id + 1
        )
        ORDER BY id ASC LIMIT 1
    """
    c.execute(query)
    res = c.fetchone()
    if res:
        return res[0]
    return 1 # Fallback, though usually query handles max+1

# --- GLOBAL STATE ---
current_spawns = {}
upload_cache = {}
edit_cache = {} 
try_cooldown = {} 
sell_requests = {}
reward_cache = {} 
search_sessions = {}
giveaway_data = {'id': None, 'end_time': 0, 'claimed_users': set()}
addcol_cache = {} # Stores user_id: [list of char ids] while waiting for reward id
editcol_cache = {} # Stores user_id: {collection_id, mode}
lab_cache = {} # Stores user_id: {exp_num, stage, msg_id}

# --- RARITY MAP ---
RARITY_MAP = {
    1: "🎫 Hentai Giveaway",
    2: "🎟 Giveaway",
    3: "⚫️ Common",
    4: "🟠 Rare",
    5: "🟡 Legendary",
    6: "🫧 Premium",
    7: "🔮 Event",
    8: "🔞 Hentai",
    9: "🎞 Animation",
    10: "🎗 Collector"
}

# Base chances for Hentai Mode (Used for Special Event probability)
HENTAI_MODE_BASE = {1: 0, 2: 0, 3: 15, 4: 15, 5: 25, 6: 20, 7: 15, 8: 10, 9: 7}
TOTAL_HENTAI_WEIGHT = sum(HENTAI_MODE_BASE.values())

def get_rarity_text(rarity_level):
    return RARITY_MAP.get(rarity_level, "Unknown")

def get_rarity_emoji(rarity_level):
    text = RARITY_MAP.get(rarity_level, "⚪️")
    return text.split()[0]

def get_rarity_tag(rarity_level):
    tags = {
        1: "[🔞]", 2: "[🎟]", 3: "[⚫️]", 4: "[🟠]", 
        5: "[🟡]", 6: "[🫧]", 7: "[🔮]", 8: "[🔞]",
        9: "[🎞]", 10: "[🎗]"
    }
    return tags.get(rarity_level, "[❓]")

def get_display_name(user):
    return user.first_name

def get_rank_emoji(rank):
    if rank == 1: return "🥇"
    if rank == 2: return "🥈"
    if rank == 3: return "🥉"
    if rank == 4: return "4️⃣"
    if rank == 5: return "5️⃣"
    if rank == 6: return "6️⃣"
    if rank == 7: return "7️⃣"
    if rank == 8: return "🎱"
    if rank == 9: return "9️⃣"
    if rank == 10: return "🔟"
    return str(rank)

def is_banned(user_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT user_id FROM banned_users WHERE user_id=?", (user_id,))
    result = c.fetchone()
    conn.close()
    return result is not None

# --- SAFETY CHECK HELPER ---
def is_char_allowed(rarity, is_hentai_mode, is_sfw_mode):
    if rarity == 10: return True # Collector is always allowed
    if is_sfw_mode:
        if rarity not in [2, 3, 4, 5, 9, 10]: return False
    elif not is_hentai_mode:
        if rarity in [1, 8]: return False
    return True

def get_group_settings(chat_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT hentai_mode, sfw_mode FROM groups WHERE chat_id=?", (chat_id,))
    res = c.fetchone()
    conn.close()
    is_hentai = res[0] if res else 0
    is_sfw = res[1] if res else 0
    return is_hentai, is_sfw

# --- PERMISSIONS ---
def is_sudo(user_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT user_id FROM sudos WHERE user_id=?", (user_id,))
    result = c.fetchone()
    conn.close()
    return result is not None or user_id == OWNER_ID

def is_uploader(user_id):
    if is_sudo(user_id): return True
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT user_id FROM uploaders WHERE user_id=?", (user_id,))
    result = c.fetchone()
    conn.close()
    return result is not None

def update_user_info(user):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    username = f"@{user.username}" if user.username else "No Username"
    c.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user.id,))
    c.execute("UPDATE users SET first_name=?, username=? WHERE user_id=?", (user.first_name, username, user.id))
    conn.commit()
    conn.close()

# --- ECONOMY HELPERS ---
def get_user_gems(user_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT gem FROM users WHERE user_id=?", (user_id,))
    res = c.fetchone()
    conn.close()
    return res[0] if res else 0

def update_user_gems(user_id, amount):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("UPDATE users SET gem = gem + ? WHERE user_id=?", (amount, user_id))
    conn.commit()
    conn.close()

# --- COLLECTION HELPERS ---
async def check_collection_completion(context, user_id, chat_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    # Get all collections
    c.execute("SELECT id, reward_id FROM collections")
    all_collections = c.fetchall()
    
    completed_rewards = []
    
    for col_id, reward_id in all_collections:
        # Check if already completed
        c.execute("SELECT 1 FROM collection_completions WHERE user_id=? AND collection_id=?", (user_id, col_id))
        if c.fetchone():
            continue

        # Get required chars
        c.execute("SELECT char_id FROM collection_items WHERE collection_id=?", (col_id,))
        req_chars = [r[0] for r in c.fetchall()]
        if not req_chars: continue
        
        # Check if user has all required chars
        placeholders = ','.join('?' for _ in req_chars)
        query = f"SELECT COUNT(*) FROM harem WHERE user_id=? AND character_id IN ({placeholders}) AND count > 0"
        c.execute(query, (user_id, *req_chars))
        owned_count = c.fetchone()[0]
        
        if owned_count == len(req_chars):
            # Mark as completed
            c.execute("INSERT INTO collection_completions (user_id, collection_id) VALUES (?, ?)", (user_id, col_id))
            
            # Add reward to user
            c.execute("INSERT OR IGNORE INTO harem (user_id, character_id, count) VALUES (?, ?, 0)", (user_id, reward_id))
            c.execute("UPDATE harem SET count = count + 1 WHERE user_id=? AND character_id=?", (user_id, reward_id))
            
            # Get reward details
            c.execute("SELECT name, rarity, file_id, media_type FROM characters WHERE id=?", (reward_id,))
            r_data = c.fetchone()
            if r_data:
                completed_rewards.append((reward_id, r_data[0], r_data[1], r_data[2], r_data[3], col_id))

    conn.commit()
    conn.close()
    
    # Notify user
    for rw in completed_rewards:
        rid, rname, rrarity, rfile, rmedia, cid = rw
        caption = f"🎉 **Collection Completed!**\n\nYou completed Collection #{cid} and earned:\n\n👤 {rname}\n{get_rarity_text(rrarity)}\n🆔 {rid}"
        try:
            if rmedia == 1:
                await context.bot.send_video(chat_id, rfile, caption=caption, parse_mode=ParseMode.MARKDOWN)
            else:
                await context.bot.send_photo(chat_id, rfile, caption=caption, parse_mode=ParseMode.MARKDOWN)
        except: pass

# --- POTION LOGIC ---
def get_active_potions(user_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    try:
        c.execute("SELECT potion_id, uses_left FROM user_potions WHERE user_id=? AND uses_left > 0", (user_id,))
        rows = c.fetchall()
    except:
        rows = []
    conn.close()
    
    potions = {r[0]: r[1] for r in rows}
    
    active_catch = None
    if 2 in potions: active_catch = 2
    elif 1 in potions: active_catch = 1
    
    active_rarity = None
    for p in [6, 5, 4, 3]:
        if p in potions:
            active_rarity = p
            break
            
    return active_catch, active_rarity

def consume_potions(user_id, active_ids):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    for pid in active_ids:
        if pid:
            c.execute("UPDATE user_potions SET uses_left = uses_left - 1 WHERE user_id=? AND potion_id=?", (user_id, pid))
    c.execute("DELETE FROM user_potions WHERE uses_left <= 0")
    conn.commit()
    conn.close()

# --- REFERRAL/REWARD LOGIC ---
def get_random_char_by_rarity(allowed_rarities, weights=None, exclude_id=None):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    # Try multiple times to find valid char if exclude_id is set
    for _ in range(5):
        if weights:
            try:
                chosen_rarity = random.choices(allowed_rarities, weights=weights, k=1)[0]
                c.execute("SELECT id, name, rarity, file_id, media_type FROM characters WHERE rarity=? ORDER BY RANDOM() LIMIT 1", (chosen_rarity,))
            except: return None
        else:
            placeholders = ','.join('?' for _ in allowed_rarities)
            c.execute(f"SELECT id, name, rarity, file_id, media_type FROM characters WHERE rarity IN ({placeholders}) ORDER BY RANDOM() LIMIT 1", allowed_rarities)
        
        res = c.fetchone()
        if res:
            if exclude_id and res[0] == exclude_id:
                continue # Try again
            conn.close()
            return res
            
    conn.close()
    return None

async def delete_reward_msg(context, chat_id, msg_id, delay):
    await asyncio.sleep(delay)
    try:
        if msg_id in reward_cache: del reward_cache[msg_id]
        await context.bot.delete_message(chat_id, msg_id)
    except: pass

async def send_reward_message(context, chat_id, user_id, chars, source="reward", page=0, edit_msg_id=None):
    if not chars: return
    
    total_pages = len(chars)
    current_char = chars[page]
    char_id, name, rarity, file_id, media_type = current_char
    
    is_hentai, is_sfw = get_group_settings(chat_id)
    
    show_image = True
    if rarity == 8 and not is_hentai: show_image = False
    if is_sfw and rarity != 9 and rarity != 10: show_image = False
    
    rarity_txt = get_rarity_text(rarity)
    
    title = "🎉 Reward!"
    timeout = 0
    
    if source == "referral_0": 
        title = "🎉 Referral Bonus!"
        timeout = 45
    elif source == "referral_1": 
        title = "🎉 Referral Milestone: 20 Characters!"
        timeout = 45
    elif source == "referral_2": 
        title = "🎉 Referral Milestone: 100 Characters!"
        timeout = 45
    elif source == "shop":
        title = "🛍 Shop Purchase!"
        if rarity == 8: timeout = 30
        elif rarity in [6, 7]: timeout = 60
        else: timeout = 60
    
    caption = f"{title}\n\n👤 **{name}**\n{rarity_txt}\n🆔 {char_id}\n\n(Item {page+1}/{total_pages})"
    if source.startswith("referral"):
        if source == "referral_0": caption += "\n\nThanks for playing/sharing!"
        elif source == "referral_2" and page == 0: caption += "\n\nGood luck on your journey!"

    if not show_image: caption += "\n\n⚠️ Image hidden due to group settings."

    buttons = []
    if total_pages > 1:
        row = []
        prev_page = (page - 1) % total_pages
        next_page = (page + 1) % total_pages
        row.append(InlineKeyboardButton("⬅️", callback_data=f"rew_pg_{prev_page}"))
        row.append(InlineKeyboardButton("➡️", callback_data=f"rew_pg_{next_page}"))
        buttons.append(row)
    
    markup = InlineKeyboardMarkup(buttons) if buttons else None
    
    try:
        has_spoiler = not show_image
        sent_msg_id = None
        if edit_msg_id:
            media = InputMediaVideo(file_id, caption=caption, parse_mode=ParseMode.MARKDOWN, has_spoiler=has_spoiler) if media_type == 1 else InputMediaPhoto(file_id, caption=caption, parse_mode=ParseMode.MARKDOWN, has_spoiler=has_spoiler)
            await context.bot.edit_message_media(chat_id=chat_id, message_id=edit_msg_id, media=media, reply_markup=markup)
            sent_msg_id = edit_msg_id
        else:
            if media_type == 1:
                sent_msg = await context.bot.send_video(chat_id, file_id, caption=caption, reply_markup=markup, parse_mode=ParseMode.MARKDOWN, has_spoiler=has_spoiler)
            else:
                sent_msg = await context.bot.send_photo(chat_id, file_id, caption=caption, reply_markup=markup, parse_mode=ParseMode.MARKDOWN, has_spoiler=has_spoiler)
            sent_msg_id = sent_msg.message_id
            
        if sent_msg_id:
            reward_cache[sent_msg_id] = {'user_id': user_id, 'chars': chars, 'page': page, 'chat_id': chat_id, 'source': source}
            if not edit_msg_id and timeout > 0:
                asyncio.create_task(delete_reward_msg(context, chat_id, sent_msg_id, timeout))
    except Exception as e: print(f"Reward send err: {e}")

async def give_referral_reward(context, user_id, chat_id, reward_level):
    chars_to_give = []
    if reward_level == 0: 
        c = get_random_char_by_rarity([6, 7])
        if c: chars_to_give.append(c)
    elif reward_level == 1: 
        for _ in range(2):
            c = get_random_char_by_rarity([6, 7])
            if c: chars_to_give.append(c)
    elif reward_level == 2: 
        rarities = [6, 7, 8, 9]
        weights = [35, 35, 20, 10]
        for _ in range(10):
            c = get_random_char_by_rarity(rarities, weights)
            if c: chars_to_give.append(c)
    
    if not chars_to_give: return

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    for char in chars_to_give:
        char_id = char[0]
        c.execute("INSERT OR IGNORE INTO harem (user_id, character_id, count) VALUES (?, ?, 0)", (user_id, char_id))
        c.execute("UPDATE harem SET count = count + 1 WHERE user_id=? AND character_id=?", (user_id, char_id))
    conn.commit()
    conn.close()

    source = f"referral_{reward_level}"
    await send_reward_message(context, chat_id, user_id, chars_to_give, source=source)
    await check_referral_milestones(context, user_id, chat_id)
    await check_collection_completion(context, user_id, chat_id)

async def check_referral_milestones(context, user_id, chat_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT SUM(count) FROM harem WHERE user_id=?", (user_id,))
    res = c.fetchone()
    total_count = res[0] if res and res[0] else 0
    
    # 1. As Referred
    c.execute("SELECT referrer_id, m1_claimed, m2_claimed FROM referrals WHERE referred_id=?", (user_id,))
    ref_data = c.fetchone()
    if ref_data:
        referrer_id, m1, m2 = ref_data
        if total_count >= 20 and not m1:
            c.execute("UPDATE referrals SET m1_claimed=1 WHERE referred_id=?", (user_id,))
            conn.commit()
            await give_referral_reward(context, user_id, chat_id, 1)
            await give_referral_reward(context, referrer_id, referrer_id, 1)
        
        if total_count >= 100 and not m2:
            c.execute("SELECT SUM(count) FROM harem WHERE user_id=?", (referrer_id,))
            r_res = c.fetchone()
            r_count = r_res[0] if r_res and r_res[0] else 0
            if r_count >= 100:
                c.execute("UPDATE referrals SET m2_claimed=1 WHERE referred_id=?", (user_id,))
                conn.commit()
                await give_referral_reward(context, user_id, chat_id, 2)
                await give_referral_reward(context, referrer_id, referrer_id, 2)

    # 2. As Referrer
    if total_count >= 100:
        c.execute("SELECT referred_id FROM referrals WHERE referrer_id = ? AND m2_claimed = 0", (user_id,))
        candidates = c.fetchall()
        for cand in candidates:
            referred_id = cand[0]
            c.execute("SELECT SUM(count) FROM harem WHERE user_id=?", (referred_id,))
            rr_res = c.fetchone()
            rr_count = rr_res[0] if rr_res and rr_res[0] else 0
            if rr_count >= 100:
                c.execute("UPDATE referrals SET m2_claimed=1 WHERE referred_id=?", (referred_id,))
                conn.commit()
                await give_referral_reward(context, user_id, chat_id, 2)
                await give_referral_reward(context, referred_id, referred_id, 2)
    conn.close()

async def check_membership(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not SUPPORT_GROUPS: return True
    user = update.effective_user
    if not user or user.id == OWNER_ID: return True
    
    not_joined = False
    for group in SUPPORT_GROUPS:
        try:
            # We must use the 'id' from config to check status
            chat_id = group.get('id')
            if not chat_id: continue
            
            member = await context.bot.get_chat_member(chat_id=chat_id, user_id=user.id)
            if member.status in ['left', 'kicked']:
                not_joined = True
                break
        except Exception as e:
            # If bot is not admin or can't access, ignore to prevent soft-lock
            print(f"Force Sub Error for {group.get('id')}: {e}")
            pass
            
    if not_joined:
        buttons = []
        for group in SUPPORT_GROUPS:
            name = group.get('name', 'Join Group')
            link = group.get('link', '')
            if link:
                buttons.append([InlineKeyboardButton(name, url=link)])
        
        reply_markup = InlineKeyboardMarkup(buttons)
        try:
            await update.effective_message.reply_text(
                "🔔 Please Join My Support Groups To Use Commands!",
                reply_markup=reply_markup
            )
        except: pass
        return False
        
    return True

async def check_force_start(update: Update):
    user = update.effective_user
    if not user: return True
    if is_banned(user.id): return False

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT user_id FROM users WHERE user_id=?", (user.id,))
    res = c.fetchone()
    conn.close()
    
    if not res:
        try:
            bot_username = update.get_bot().username
            await update.effective_message.reply_text(
                "⚠️ You must start the bot privately first to use this command!",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Start Bot", url=f"https://t.me/{bot_username}?start=force")]])
            )
        except:
             await update.effective_message.reply_text("⚠️ You must start the bot privately first!")
        return False
    return True

# --- ECONOMY & PRICING ---
def get_catch_reward(rarity, is_steal=False):
    base_rewards = {
        3: 10000, 4: 20000, 5: 40000,
        6: 70000, 7: 80000, 8: 100000,
        9: 90000, 10: 150000
    }
    amount = base_rewards.get(rarity, 5000)
    if is_steal: amount *= 3
    amount = int(amount * 0.35)
    variance = random.randint(-500, 500)
    final = max(100, amount + variance)
    return final

def calculate_price(rarity, catch_count):
    if rarity in [1, 2, 9, 10]: return 0
    base_prices = {
        3: 30000, 4: 60000, 5: 150000, 
        6: 300000, 7: 350000, 8: 500000,
    }
    base = base_prices.get(rarity, 99999999)
    multiplier = 1.0
    if catch_count == 0: multiplier = 2.0
    elif catch_count <= 20: multiplier = 2.0 + (catch_count * -0.0125)
    elif catch_count < 50: multiplier = 1.75 + ((catch_count - 20) * -0.025)
    else: multiplier = 0.8
    return int(base * multiplier)

def get_sell_price(rarity):
    prices = {
        3: 30000, 4: 60000, 5: 150000, 
        6: 300000, 7: 350000, 8: 500000,
    }
    return int(prices.get(rarity, 0) * 0.45)

# --- PROBABILITY HELPER ---
def get_weighted_rarity(is_hentai_mode, is_sfw_mode, is_try=False):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT rarity, COUNT(*) FROM characters GROUP BY rarity")
    counts = dict(c.fetchall()) 
    conn.close()
    
    if is_sfw_mode:
        base_chances = {1: 0, 2: 0, 3: 30, 4: 35, 5: 35, 6: 0, 7: 0, 8: 0, 9: 7, 10: 0}
    elif is_hentai_mode:
        base_chances = HENTAI_MODE_BASE.copy()
        base_chances[10] = 0
    else:
        base_chances = {1: 0, 2: 0, 3: 20, 4: 20, 5: 25, 6: 20, 7: 15, 8: 0, 9: 7, 10: 0}

    if is_try:
        base_chances[9] = 0

    population = []
    weights = []
    for r, base_chance in base_chances.items():
        count = counts.get(r, 0)
        final_weight = base_chance * count
        if final_weight > 0:
            population.append(r)
            weights.append(final_weight)
    if not population: return None
    return random.choices(population, weights=weights, k=1)[0]

# --- ASYNC HELPERS ---
async def delete_image_msg(context, chat_id, message_id, delay):
    await asyncio.sleep(delay)
    try:
        await context.bot.delete_message(chat_id, message_id)
    except: pass

async def spawn_flee_timer(context, chat_id, message_id, name):
    await asyncio.sleep(120)
    if chat_id in current_spawns and current_spawns[chat_id]['message_id'] == message_id:
        del current_spawns[chat_id]
        try:
            await context.bot.send_message(chat_id, f"⏱ The character fled away! It was {name}. Remember it for next time.")
        except: pass

async def update_channels(context, char_id, admin_name, action_type="Edited"):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT name, rarity, file_id, log_msg_id1, log_msg_id2, media_type FROM characters WHERE id=?", (char_id,))
    data = c.fetchone()
    
    # Get Collection Info
    c.execute("SELECT collection_id FROM collection_items WHERE char_id=?", (char_id,))
    col_rows = c.fetchall()
    col_ids = [r[0] for r in col_rows]
    col_text = ""
    if col_ids:
        col_ids.sort()
        col_str = ", ".join(map(str, col_ids))
        col_text = f"\n🎗Collections: {col_str}"
        
    if not data:
        conn.close()
        return

    name, rarity, file_id, msg1, msg2, media_type = data
    caption = f"👤 {name}\n{get_rarity_text(rarity)}\n🆔 {char_id}{col_text}\n\n✍️ {action_type} by: {admin_name}"
    
    media_obj = InputMediaVideo(media=file_id, caption=caption) if media_type == 1 else InputMediaPhoto(media=file_id, caption=caption)

    if msg1:
        try:
            await context.bot.edit_message_media(chat_id=DATABASE_CHANNEL_ID, message_id=msg1, media=media_obj)
        except:
            try: await context.bot.edit_message_caption(chat_id=DATABASE_CHANNEL_ID, message_id=msg1, caption=caption)
            except: pass
    
    is_safe = (rarity == 2 or 3 <= rarity <= 7 or rarity == 9 or rarity == 10)
    
    if is_safe:
        if msg2:
            try:
                await context.bot.edit_message_media(chat_id=DATABASE_CHANNEL_ID2, message_id=msg2, media=media_obj)
            except:
                try: await context.bot.edit_message_caption(chat_id=DATABASE_CHANNEL_ID2, message_id=msg2, caption=caption)
                except: pass
        else:
            try:
                m = None
                if media_type == 1:
                    m = await context.bot.send_video(chat_id=DATABASE_CHANNEL_ID2, video=file_id, caption=caption)
                else:
                    m = await context.bot.send_photo(chat_id=DATABASE_CHANNEL_ID2, photo=file_id, caption=caption)
                c.execute("UPDATE characters SET log_msg_id2=? WHERE id=?", (m.message_id, char_id))
            except: pass
    else:
        if msg2:
            try:
                await context.bot.delete_message(chat_id=DATABASE_CHANNEL_ID2, message_id=msg2)
                c.execute("UPDATE characters SET log_msg_id2=NULL WHERE id=?", (char_id,))
            except: pass
    conn.commit()
    conn.close()

# --- INLINE QUERY LOGIC ---

async def inline_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query_text = update.inline_query.query.strip().lower()
    user = update.effective_user
    if is_banned(user.id): return

    offset = int(update.inline_query.offset) if update.inline_query.offset and update.inline_query.offset.isdigit() else 0

    show_hentai = False
    only_sfw = False
    
    if "hentai" in query_text:
        show_hentai = True
        query_text = query_text.replace("hentai", "").strip()
    elif "sfw" in query_text:
        only_sfw = True
        query_text = query_text.replace("sfw", "").strip()

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    search_rarity = None
    for r_lvl, r_name in RARITY_MAP.items():
        emoji = get_rarity_emoji(r_lvl)
        if emoji in query_text:
            search_rarity = r_lvl
            break
            
    chars_list = []
    
    if query_text.isdigit() and len(query_text) < 8:
        char_id = int(query_text)
        c.execute("SELECT id, name, rarity, file_id, media_type FROM characters WHERE id=?", (char_id,))
        char = c.fetchone()
        chars_list = [char] if char else []

    elif query_text.isdigit() and len(query_text) >= 8:
        target_id = int(query_text)
        c.execute("""
            SELECT c.id, c.name, c.rarity, c.file_id, h.count, c.media_type 
            FROM harem h JOIN characters c ON h.character_id = c.id 
            WHERE h.user_id = ?
        """, (target_id,))
        rows = c.fetchall()
        chars_list = [(r[0], r[1], r[2], r[3], r[5], r[4]) for r in rows] 

    elif search_rarity:
        c.execute("SELECT id, name, rarity, file_id, media_type FROM characters WHERE rarity=?", (search_rarity,))
        chars_list = c.fetchall()

    elif query_text:
        c.execute("SELECT id, name, rarity, file_id, media_type FROM characters WHERE name LIKE ?", (f"%{query_text}%",))
        chars_list = c.fetchall()
    
    else:
        c.execute("SELECT id, name, rarity, file_id, media_type FROM characters ORDER BY RANDOM() LIMIT 50")
        chars_list = c.fetchall()

    conn.close()

    filtered_results = []
    for item in chars_list:
        rarity = item[2]
        if not is_char_allowed(rarity, show_hentai, only_sfw): continue
        filtered_results.append(item)

    paged_items = filtered_results[offset : offset + 50]
    
    results = []
    for item in paged_items:
        char_id = item[0]
        name = item[1]
        rarity = item[2]
        file_id = item[3]
        media_type = item[4]
        count_text = f" x{item[5]}" if len(item) > 5 else ""
        
        emoji = get_rarity_emoji(rarity)
        rarity_text = get_rarity_text(rarity)
        
        caption = f"👤 {name}{count_text}\n{rarity_text}\n🆔 {char_id}"
        
        if media_type == 1:
             results.append(
                InlineQueryResultCachedVideo(
                    id=str(uuid4()),
                    video_file_id=file_id,
                    title=f"{name} | {rarity_text}",
                    caption=caption,
                    description=f"{name} | {rarity_text}"
                )
            )
        else:
            results.append(
                InlineQueryResultCachedPhoto(
                    id=str(uuid4()),
                    photo_file_id=file_id,
                    caption=caption,
                    description=f"{name} | {rarity_text}"
                )
            )

    next_offset = str(offset + 50) if len(filtered_results) > offset + 50 else ""
    await update.inline_query.answer(results, cache_time=1, is_personal=True, next_offset=next_offset)

# --- INLINE MESSAGE AUTO-DELETION ---

async def check_inline_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    chat = update.effective_chat
    
    caption = msg.caption or ""
    rarity = 0
    for r_lvl, r_text in RARITY_MAP.items():
        if r_text in caption:
            rarity = r_lvl
            break
            
    if rarity == 0: return 

    is_hentai, is_sfw = get_group_settings(chat.id)
    is_allowed = is_char_allowed(rarity, is_hentai, is_sfw)
    
    if not is_allowed:
        try: await msg.delete()
        except: pass 
        return

    delay = 0
    if rarity in [6, 7, 9]: delay = 60
    elif rarity == 8: delay = 30
        
    if delay > 0:
        asyncio.create_task(delete_image_msg(context, chat.id, msg.message_id, delay))

# --- WELCOME MESSAGE ---

async def new_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message and update.message.new_chat_members:
        for member in update.message.new_chat_members:
            if member.id == context.bot.id:
                text = (
                    "Thanks for adding me!\n"
                    "Please give me **Delete Messages** permission so I can work correctly."
                )
                await update.effective_message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

# --- CUSTOM FILTER ---
class ViaBotIDFilter(filters.MessageFilter):
    def __init__(self, bot_id):
        super().__init__()
        self.bot_id = bot_id

    def filter(self, message):
        return bool(message.via_bot and message.via_bot.id == self.bot_id)

# --- COMMANDS ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_membership(update, context): return
    msg = update.effective_message
    if not msg: return
    user = update.effective_user
    if is_banned(user.id): return
    chat_id = update.effective_chat.id
    
    # Referral Logic
    if context.args and context.args[0].startswith("ref_"):
        try:
            referrer_id = int(context.args[0].split("_")[1])
            if referrer_id != user.id:
                conn = sqlite3.connect(DB_NAME)
                c = conn.cursor()
                c.execute("SELECT user_id FROM users WHERE user_id=?", (user.id,))
                exists = c.fetchone()
                c.execute("SELECT referred_id FROM referrals WHERE referred_id=?", (user.id,))
                already_referred = c.fetchone()
                
                if not exists and not already_referred:
                    c.execute("INSERT OR IGNORE INTO referrals (referrer_id, referred_id) VALUES (?, ?)", (referrer_id, user.id))
                    conn.commit()
                    # Initial Reward
                    await give_referral_reward(context, user.id, chat_id, 0)
                    await give_referral_reward(context, referrer_id, referrer_id, 0)
                    
                    expl_text = (
                         "🎉 **Welcome to Anime Catcher!**\n\n"
                         "You have been referred by a friend! Both of you received a reward.\n\n"
                         "🤝 **Referral System:**\n"
                         "1️⃣ Start the bot with a referral link -> Get a character!\n"
                         "2️⃣ Collect 20 Characters -> Both get 2 Rare/Event Characters!\n"
                         "3️⃣ Collect 100 Characters -> Both get 10 High Rarity Characters!\n\n"
                         "Good luck on your journey!"
                     )
                    await update.effective_message.reply_text(expl_text, parse_mode=ParseMode.MARKDOWN)
                conn.close()
        except: pass

    update_user_info(user)
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO groups (chat_id, last_spawn_id) VALUES (?, ?)", (chat_id, msg.message_id))
    c.execute("UPDATE groups SET last_spawn_id = ? WHERE chat_id=?", (msg.message_id, chat_id))
    conn.commit()
    conn.close()
    await update.effective_message.reply_text("👋 Hello! I am the Ravan Anime Catcher Bot.")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_membership(update, context): return
    user = update.effective_user
    if is_banned(user.id): return
    
    text = "📚 **Welcome to the Guide!**\nPlease select a category below:"
    keyboard = [
        [InlineKeyboardButton("👤 Members", callback_data="help_member")],
        [InlineKeyboardButton("👮 Admins", callback_data="help_admin")],
        [InlineKeyboardButton("📦 Uploaders", callback_data="help_uploader")],
        [InlineKeyboardButton("👑 Sudo", callback_data="help_sudo")],
        [InlineKeyboardButton("🔑 Owner", callback_data="help_owner")]
    ]
    await update.effective_message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)

async def help_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    
    text_member = (
        "👤 **User Commands:**\n\n"
        "/card - Show player card\n"
        "/steal [name] - Catch character\n"
        "/try - Try your luck (5s CD)\n"
        "/dice [amt] [odd/even/num] - Gamble with Dice\n"
        "/coin [amt] [head/tail] - Gamble with Coin\n"
        "/myharem - Show your collection\n"
        "/list - Show global list\n"
        "/uncollected - Show uncollected characters\n"
        "/cfind [name] - Search character\n"
        "/see [id] - Character info\n"
        "/giftc [id] - Gift character (Reply)\n"
        "/fav [id] - Set favorite character\n"
        "/type - Filter harem rarity\n"
        "/topg - Global Rank\n"
        "/topc - Richest Users\n"
        "/event - Show Events\n"
        "/col - Show Collections\n"
        "/lab - Laboratory Experiments\n"
        "/mymoney - Check Balance\n"
        "/ctg [amount] - Convert Crystal to Gem\n"
        "/shop - Open Shop\n"
        "/pinv - Potion Inventory\n"
        "/buy [id] - Buy character\n"
        "/sell [id] - Sell character\n"
        "/payc [amount] - Transfer Crystals (Reply)\n"
        "/claim - Claim Giveaway\n"
        "/cdaily - Daily Reward\n"
        "/cweekly - Weekly Reward\n"
        "/referral - Referral Link\n"
    )
    text_admin = "👮 **Admin Commands:**\n\n/time [number] - Set spawn threshold\n/hentai - Toggle Hentai Mode\n/sfw - Toggle SFW Mode\n/game - Toggle Mini Games\n"
    text_uploader = "📦 **Uploader Commands:**\n\n/upload [name] - Upload new character (Reply to photo/video)\n/edit [id] - Edit character\n"
    text_sudo = "👑 **Sudo Commands:**\n\n/del [id] - Delete character\n/donate [id] - Give char to user (Reply)\n/donatec [amount] - Give crystals (Reply)\n/removec [amount] - Remove crystals (Reply)\n/adduploader - Add Uploader (Reply)\n/remuploader - Remove Uploader (Reply)\n/editevent - Edit Event Text\n/broadcast - Broadcast message (Reply)\n"
    text_owner = "🔑 **Owner Commands:**\n\n/addsudo - Add Sudo Admin\n/remsudo - Remove Sudo Admin\n/remc [id] [user_id] - Remove char from user\n/delharem [user_id] - Delete user harem\n/banplayer [id] - Ban User\n/unbanplayer [id] - Unban User\n/giveaway [id] - Start Giveaway\n/cgiveaway - Cancel Giveaway\n/addcol - Add Collection\n/editcol - Edit Collection\n/delcol - Delete Collection\n"

    back_btn = [[InlineKeyboardButton("⬅️ Back", callback_data="help_main")]]
    content = "Error."
    
    if data == "help_main":
        text = "📚 **Welcome to the Guide!**\nPlease select a category below:"
        keyboard = [
            [InlineKeyboardButton("👤 Members", callback_data="help_member")],
            [InlineKeyboardButton("👮 Admins", callback_data="help_admin")],
            [InlineKeyboardButton("📦 Uploaders", callback_data="help_uploader")],
            [InlineKeyboardButton("👑 Sudo", callback_data="help_sudo")],
            [InlineKeyboardButton("🔑 Owner", callback_data="help_owner")]
        ]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
        return
    elif data == "help_member": content = text_member
    elif data == "help_admin": content = text_admin
    elif data == "help_uploader": content = text_uploader
    elif data == "help_sudo": content = text_sudo
    elif data == "help_owner": content = text_owner

    await query.edit_message_text(content, reply_markup=InlineKeyboardMarkup(back_btn), parse_mode=ParseMode.MARKDOWN)

async def upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_membership(update, context): return
    user = update.effective_user
    if is_banned(user.id): return
    if not is_uploader(user.id): return
    
    reply = update.effective_message.reply_to_message
    if not reply:
        await update.effective_message.reply_text("⚠️ Reply to an image or video with: `/upload Character Name`", parse_mode=ParseMode.MARKDOWN)
        return

    media_type = 0
    file_id = None
    
    if reply.photo:
        file_id = reply.photo[-1].file_id
    elif reply.video:
        file_id = reply.video.file_id
        media_type = 1
    elif reply.animation:
        file_id = reply.animation.file_id
        media_type = 1
    else:
        await update.effective_message.reply_text("⚠️ Supported media: Photo, Video, GIF.")
        return

    if not context.args:
        await update.effective_message.reply_text("⚠️ Name required.", parse_mode=ParseMode.MARKDOWN)
        return

    full_name = " ".join(context.args).title()

    if media_type == 1:
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        new_id = get_next_id(c, "characters")
        c.execute("INSERT INTO characters (id, name, surname, extra_name, rarity, file_id, media_type) VALUES (?, ?, '', '', 9, ?, 1)",
                  (new_id, full_name, file_id))
        char_id = new_id
        conn.commit()
        conn.close()
        
        caption = f"👤 {full_name}\n🎞 Animation\n🆔 {char_id}\n\n✍️ Added by: {user.first_name}"
        try: await context.bot.send_video(chat_id=DATABASE_CHANNEL_ID, video=file_id, caption=caption)
        except: pass
        try: await context.bot.send_video(chat_id=DATABASE_CHANNEL_ID2, video=file_id, caption=caption)
        except: pass
        
        await update.effective_message.reply_text(f"✅ Video/Animation Uploaded! ID: {char_id}")
        return

    upload_cache[user.id] = {'file_id': file_id, 'name': full_name}
    keyboard = [
        [InlineKeyboardButton("🎫 HG (1)", callback_data="rarity_1"), InlineKeyboardButton("🎟 Give (2)", callback_data="rarity_2")],
        [InlineKeyboardButton("⚫️ Com (3)", callback_data="rarity_3"), InlineKeyboardButton("🟠 Rare (4)", callback_data="rarity_4")],
        [InlineKeyboardButton("🟡 Leg (5)", callback_data="rarity_5"), InlineKeyboardButton("🫧 Prem (6)", callback_data="rarity_6")],
        [InlineKeyboardButton("🔮 Evt (7)", callback_data="rarity_7"), InlineKeyboardButton("🔞 Hentai (8)", callback_data="rarity_8")],
        [InlineKeyboardButton("🎗 Col (10)", callback_data="rarity_10"), InlineKeyboardButton("❌ Cancel", callback_data="rarity_cancel")]
    ]
    await update.effective_message.reply_text(f"Name: {full_name}\nSelect Rarity:", reply_markup=InlineKeyboardMarkup(keyboard))

async def rarity_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user
    await query.answer()
    if query.data == "rarity_cancel":
        if user.id in upload_cache: del upload_cache[user.id]
        await query.edit_message_text("❌ Cancelled.")
        return
    if user.id not in upload_cache:
        await query.edit_message_text("⚠️ Expired.")
        return

    rarity = int(query.data.split("_")[1])
    data = upload_cache[user.id]
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    new_id = get_next_id(c, "characters")
    c.execute("INSERT INTO characters (id, name, surname, extra_name, rarity, file_id, media_type) VALUES (?, ?, '', '', ?, ?, 0)",
              (new_id, data['name'], rarity, data['file_id']))
    char_id = new_id
    conn.commit()
    conn.close()

    caption = f"👤 {data['name']}\n{get_rarity_text(rarity)}\n🆔 {char_id}\n\n✍️ Added by: {user.first_name}"
    msg1_id, msg2_id = None, None
    try:
        m1 = await context.bot.send_photo(chat_id=DATABASE_CHANNEL_ID, photo=data['file_id'], caption=caption)
        msg1_id = m1.message_id
    except: pass
    if rarity == 2 or (3 <= rarity <= 7) or rarity == 10:
        try:
            m2 = await context.bot.send_photo(chat_id=DATABASE_CHANNEL_ID2, photo=data['file_id'], caption=caption)
            msg2_id = m2.message_id
        except: pass
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("UPDATE characters SET log_msg_id1=?, log_msg_id2=? WHERE id=?", (msg1_id, msg2_id, char_id))
    conn.commit()
    conn.close()
    del upload_cache[user.id]
    await query.edit_message_text(f"✅ Saved! ID: {char_id}")

async def edit_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_membership(update, context): return
    user = update.effective_user
    if is_banned(user.id) or not is_uploader(user.id): return
    if not context.args:
        await update.effective_message.reply_text("Usage: /edit [id]")
        return
    try:
        char_id = int(context.args[0])
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("SELECT name, rarity, file_id, media_type FROM characters WHERE id=?", (char_id,))
        row = c.fetchone()
        conn.close()
        if not row:
            await update.effective_message.reply_text("Character not found.")
            return
        edit_cache[user.id] = {'id': char_id, 'data': row, 'admin_name': user.first_name, 'media_type': row[3]}
        text = f"Editing ID: {char_id}\n👤 Name: {row[0]}\n{RARITY_MAP.get(row[1])}"
        keyboard = [
            [InlineKeyboardButton("Name", callback_data="edit_name"), InlineKeyboardButton("Rarity", callback_data="edit_rarity")],
            [InlineKeyboardButton("Photo/Video", callback_data="edit_photo"), InlineKeyboardButton("❌ Cancel", callback_data="edit_cancel")]
        ]
        
        msg = None
        if row[3] == 1:
            msg = await update.effective_message.reply_video(video=row[2], caption=text, reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            msg = await update.effective_message.reply_photo(photo=row[2], caption=text, reply_markup=InlineKeyboardMarkup(keyboard))
            
        asyncio.create_task(delete_image_msg(context, update.effective_chat.id, msg.message_id, 40))
    except ValueError: await update.effective_message.reply_text("ID must be a number.")

async def edit_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user
    await query.answer()
    if user.id not in edit_cache and not query.data.startswith("editcol_"):
        await query.edit_message_caption("Session expired.")
        return
    data = query.data
    
    # --- Edit Collection Logic ---
    if data.startswith("editcol_"):
        if not is_sudo(user.id) and user.id != OWNER_ID: return
        parts = data.split("_")
        mode = parts[1] # reward or chars
        col_id = int(parts[2])
        
        editcol_cache[user.id] = {'col_id': col_id, 'mode': mode}
        if mode == 'reward':
            await query.edit_message_text(f"Collection {col_id}: Send New Reward ID.")
        elif mode == 'chars':
            await query.edit_message_text(f"Collection {col_id}: Send New Character IDs (space separated).")
        return

    cache = edit_cache[user.id]
    char_id = cache['id']
    admin_name = cache['admin_name']
    
    if data == "edit_cancel":
        del edit_cache[user.id]
        try: await query.message.delete()
        except: pass
        return
    if data == "edit_rarity":
        keyboard = [
            [InlineKeyboardButton("🎫 1", callback_data="setrarity_1"), InlineKeyboardButton("🎟 2", callback_data="setrarity_2")],
            [InlineKeyboardButton("⚫️ 3", callback_data="setrarity_3"), InlineKeyboardButton("🟠 4", callback_data="setrarity_4")],
            [InlineKeyboardButton("🟡 5", callback_data="setrarity_5"), InlineKeyboardButton("🫧 6", callback_data="setrarity_6")],
            [InlineKeyboardButton("🔮 7", callback_data="setrarity_7"), InlineKeyboardButton("🔞 8", callback_data="setrarity_8")],
            [InlineKeyboardButton("🎗 10", callback_data="setrarity_10"), InlineKeyboardButton("🎞 9", callback_data="setrarity_9")]
        ]
        await query.edit_message_caption("Select new rarity:", reply_markup=InlineKeyboardMarkup(keyboard))
        return
    if data.startswith("setrarity_"):
        new_rarity = int(data.split("_")[1])
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        
        # Check old rarity to handle channel 2 logic
        c.execute("SELECT rarity, file_id, log_msg_id2, media_type, name FROM characters WHERE id=?", (char_id,))
        row = c.fetchone()
        
        old_rarity, file_id, msg2_id, media_type, name = row
        
        # Safe rarities: 2,3,4,5,6,7,9,10
        safe_list = [2,3,4,5,6,7,9,10]
        is_old_safe = old_rarity in safe_list
        is_new_safe = new_rarity in safe_list

        c.execute("UPDATE characters SET rarity=? WHERE id=?", (new_rarity, char_id))
        
        # Logic for moving between Safe and Unsafe
        if is_old_safe and not is_new_safe:
            # Was safe, now unsafe -> Delete from Channel 2
            if msg2_id:
                try: await context.bot.delete_message(DATABASE_CHANNEL_ID2, msg2_id)
                except: pass
                c.execute("UPDATE characters SET log_msg_id2=NULL WHERE id=?", (char_id,))
        
        elif not is_old_safe and is_new_safe:
            # Was unsafe, now safe -> Send to Channel 2
            caption = f"👤 {name}\n{get_rarity_text(new_rarity)}\n🆔 {char_id}\n\n✍️ Edited by: {admin_name}"
            try:
                m = None
                if media_type == 1:
                    m = await context.bot.send_video(chat_id=DATABASE_CHANNEL_ID2, video=file_id, caption=caption)
                else:
                    m = await context.bot.send_photo(chat_id=DATABASE_CHANNEL_ID2, photo=file_id, caption=caption)
                c.execute("UPDATE characters SET log_msg_id2=? WHERE id=?", (m.message_id, char_id))
            except: pass
            
        conn.commit()
        conn.close()
        
        await update_channels(context, char_id, admin_name)
        del edit_cache[user.id]
        try: await query.message.delete()
        except: pass
        await context.bot.send_message(query.message.chat_id, f"✅ Rarity updated to {get_rarity_text(new_rarity)}")
        return
    mode = data.split("_")[1]
    edit_cache[user.id]['mode'] = mode
    edit_cache[user.id]['msg_id'] = query.message.message_id 
    await query.edit_message_caption(f"Send the new {mode}:")

async def card_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_membership(update, context): return
    # Determine target
    user = update.effective_user
    target_id = user.id
    target_user = user 

    if update.effective_message.reply_to_message:
        target_user = update.effective_message.reply_to_message.from_user
        target_id = target_user.id
    elif context.args:
        query = context.args[0]
        if query.isdigit():
            target_id = int(query)
            try:
                chat_info = await context.bot.get_chat(target_id)
                target_user = chat_info
            except:
                target_user = None
        elif query.startswith("@"):
            username = query[1:]
            conn = sqlite3.connect(DB_NAME)
            c = conn.cursor()
            c.execute("SELECT user_id FROM users WHERE username = ? COLLATE NOCASE", (f"@{username}",))
            row = c.fetchone()
            conn.close()
            if row:
                target_id = row[0]
                try:
                    chat_info = await context.bot.get_chat(target_id)
                    target_user = chat_info
                except:
                    target_user = None
            else:
                await update.effective_message.reply_text("❌ User not found in database.")
                return

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    c.execute("SELECT first_name FROM users WHERE user_id=?", (target_id,))
    user_db = c.fetchone()
    
    if not user_db:
         conn.close()
         await update.effective_message.reply_text("❌ User not found or hasn't started the bot.")
         return
    
    db_first_name = user_db[0]
    name = target_user.first_name if target_user else db_first_name
    surname = ""
    if target_user and hasattr(target_user, "last_name") and target_user.last_name:
        surname = " " + target_user.last_name
    
    full_name = f"{name}{surname}"

    # Stats
    c.execute("SELECT SUM(count) FROM harem WHERE user_id=?", (target_id,))
    total_waifu = c.fetchone()[0] or 0

    c.execute("SELECT COUNT(*) FROM harem WHERE user_id=?", (target_id,))
    unique_waifu = c.fetchone()[0] or 0

    c.execute("SELECT COUNT(*) FROM characters")
    total_bot = c.fetchone()[0] or 0

    c.execute("""
        SELECT c.rarity, COUNT(*) 
        FROM harem h 
        JOIN characters c ON h.character_id = c.id 
        WHERE h.user_id = ? 
        GROUP BY c.rarity
    """, (target_id,))
    user_rarity_counts = dict(c.fetchall())

    c.execute("SELECT rarity, COUNT(*) FROM characters GROUP BY rarity")
    bot_rarity_counts = dict(c.fetchall())
    conn.close()

    percentage = (unique_waifu / total_bot * 100) if total_bot > 0 else 0
    filled_len = int(percentage / 10)
    bar = "■" * filled_len + "□" * (10 - filled_len)

    rarity_conf = [
        (1, "🎫 Hentai Giveaway"),
        (2, "🎟️ Giveaway"),
        (3, "⚫️ Common"),
        (4, "🟠 Rare"),
        (5, "🟡 Legendary"),
        (6, "🫧 Premium"),
        (7, "🔮 Event"),
        (8, "🔞 Hentai"),
        (9, "🧬 Animation"),
        (10, "🎗 Collector")
    ]
    
    chat_id = update.effective_chat.id
    is_hentai, is_sfw = get_group_settings(chat_id)
    visible_rarities = []
    
    if is_sfw:
        visible_rarities = [2, 3, 4, 5, 9, 10]
    elif not is_hentai:
        visible_rarities = [2, 3, 4, 5, 6, 7, 9, 10]
    else:
        visible_rarities = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    breakdown_text = ""
    for rid, title in rarity_conf:
        if rid not in visible_rarities: continue
        u_count = user_rarity_counts.get(rid, 0)
        t_count = bot_rarity_counts.get(rid, 0)
        perc = (u_count / t_count * 100) if t_count > 0 else 0
        breakdown_text += f"{title}: {u_count}/{t_count} ({int(perc)}%)\n"

    caption = (
        f"🪪 {full_name} 🆔`{target_id}`\n"
        f"⛩️ Total Waifu: {total_waifu}\n"
        f"🏆 Unique Waifu: {unique_waifu}/{total_bot}\n"
        f"📊 Progress Bar: {bar} {int(percentage)}%\n\n"
        f"{breakdown_text}"
    )

    try:
        photos = await context.bot.get_user_profile_photos(target_id, limit=1)
        if photos.total_count > 0:
            await update.effective_message.reply_photo(photos.photos[0][-1].file_id, caption=caption)
        else:
            await update.effective_message.reply_text(caption)
    except:
        await update.effective_message.reply_text(caption)

async def spawn_character(chat_id, context):
    is_hentai, is_sfw = get_group_settings(chat_id)
    rarity = get_weighted_rarity(is_hentai, is_sfw)
    if not rarity: return
    
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT id, name, file_id, media_type FROM characters WHERE rarity=?", (rarity,))
    chars = c.fetchall()
    conn.close()

    if not chars: return
    char = random.choice(chars)
    rarity_txt = get_rarity_text(rarity)
    caption = f"A wild character appeared!\nRate: {rarity_txt}\nUse `/steal` [name] to catch it!"
    
    msg = None
    if char[3] == 1:
        msg = await context.bot.send_video(chat_id=chat_id, video=char[2], caption=caption, parse_mode=ParseMode.MARKDOWN)
    else:
        msg = await context.bot.send_photo(chat_id=chat_id, photo=char[2], caption=caption, parse_mode=ParseMode.MARKDOWN)
    
    # Image Deletion Timer (Only high rarities)
    delay = 0
    if rarity == 8: delay = 30
    elif rarity in [6, 7]: delay = 60
    
    if delay > 0:
        asyncio.create_task(delete_image_msg(context, chat_id, msg.message_id, delay))

    # Flee Timer (All spawns, 120s)
    asyncio.create_task(spawn_flee_timer(context, chat_id, msg.message_id, char[1]))

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("UPDATE groups SET last_spawn_id = ? WHERE chat_id=?", (msg.message_id, chat_id))
    conn.commit()
    conn.close()

    current_spawns[chat_id] = {
        'char_id': char[0], 'full_name': char[1], 'message_id': msg.message_id, 'rarity': rarity
    }

async def try_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_membership(update, context): return
    if not await check_force_start(update): return
    user = update.effective_user
    if is_banned(user.id): return
    chat_id = update.effective_chat.id
    update_user_info(user)
    
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO groups (chat_id, game_mode) VALUES (?, 1)", (chat_id,))
    c.execute("SELECT game_mode, hentai_mode, sfw_mode FROM groups WHERE chat_id=?", (chat_id,))
    row = c.fetchone()
    game_enabled = row[0] if row else 1
    is_hentai = row[1] if row else 0
    is_sfw = row[2] if row else 0
    conn.close()

    if not game_enabled: return
    current_time = time.time()
    last_try = try_cooldown.get(user.id, 0)
    if current_time - last_try < 15:
        remaining = int(15 - (current_time - last_try))
        await update.effective_message.reply_text(f"⏳ Please wait {remaining}s.")
        return
    try_cooldown[user.id] = current_time
    
    # Check Active Potions
    active_catch, active_rarity = get_active_potions(user.id)
    
    rarity = None
    # Apply Rarity Potion Logic (Force specific rarity chance)
    if active_rarity:
        threshold = 0.6 # 60% default
        target_rarity = None
        
        if active_rarity == 6: # Hentai
            if is_hentai and not is_sfw: target_rarity = 8
        elif active_rarity == 5: # Event
            target_rarity = 7
        elif active_rarity == 4: # Premium
            target_rarity = 6
        elif active_rarity == 3: # Legendary
            target_rarity = 5
            threshold = 0.6 if is_sfw else 0.4
            
        if target_rarity and random.random() < threshold:
            rarity = target_rarity

    if not rarity:
        rarity = get_weighted_rarity(is_hentai, is_sfw, is_try=True)
    
    if not rarity: return
    
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT id, name, file_id, media_type FROM characters WHERE rarity=?", (rarity,))
    chars = c.fetchall()
    if not chars:
        conn.close()
        return
    char = random.choice(chars)
    char_id, name, file_id, media_type = char
    emoji = get_rarity_emoji(rarity)
    user_display = get_display_name(user)

    # Apply Catch Chance Potion Logic
    catch_chance = 0.26
    if active_catch == 1: catch_chance *= 2 # Double
    elif active_catch == 2: catch_chance *= 3 # Triple

    if random.random() < catch_chance:
        update_user_info(user)
        c.execute("INSERT OR IGNORE INTO harem (user_id, character_id, count) VALUES (?, ?, 0)", (user.id, char_id))
        c.execute("UPDATE harem SET count = count + 1 WHERE user_id=? AND character_id=?", (user.id, char_id))
        reward = get_catch_reward(rarity, is_steal=False)
        c.execute("UPDATE users SET crystal = crystal + ? WHERE user_id=?", (reward, user.id))
        conn.commit()
        caption = f"{emoji} Congrats {user_display}. You successfully smashed {name} 🆔{char_id}\nAdded {reward} Crystals."
        
        msg = None
        if media_type == 1:
            msg = await update.effective_message.reply_video(video=file_id, caption=caption)
        else:
            msg = await update.effective_message.reply_photo(photo=file_id, caption=caption)
            
        delay = 0
        if rarity == 8: delay = 30
        elif rarity in [6, 7]: delay = 60
        if delay > 0: asyncio.create_task(delete_image_msg(context, chat_id, msg.message_id, delay))
        
        # --- EVENT LOGIC (Try) ---
        user_id = user.id
        event_chance = HENTAI_MODE_BASE.get(rarity, 0) / TOTAL_HENTAI_WEIGHT
        if random.random() < event_chance:
            if random.random() < 0.5:
                # Heaven Event
                if random.random() < 0.5:
                    bonus_char = get_random_char_by_rarity([rarity], exclude_id=char_id)
                    if bonus_char:
                        b_id, b_name, _, _, _ = bonus_char
                        c.execute("INSERT OR IGNORE INTO harem (user_id, character_id, count) VALUES (?, ?, 0)", (user_id, b_id))
                        c.execute("UPDATE harem SET count = count + 1 WHERE user_id=? AND character_id=?", (user_id, b_id))
                        conn.commit()
                        await update.effective_message.reply_text(f"👼 **HEAVEN EVENT!**\nThe gods smiled upon you! You also received **{b_name}** 🆔{b_id}!", parse_mode=ParseMode.MARKDOWN)
                    else:
                        await update.effective_message.reply_text(f"👼 **HEAVEN EVENT!**\nYou entered heaven, but found no extra companion.", parse_mode=ParseMode.MARKDOWN)
                else:
                    await update.effective_message.reply_text(f"👼 **HEAVEN EVENT!**\nYou basked in glory, but received nothing extra this time.", parse_mode=ParseMode.MARKDOWN)
            else:
                # Hell Event
                if random.random() < 0.5:
                    c.execute("SELECT count FROM harem WHERE user_id=? AND character_id=?", (user_id, char_id))
                    res = c.fetchone()
                    if res and res[0] > 0:
                        new_c = res[0] - 1
                        if new_c == 0: c.execute("DELETE FROM harem WHERE user_id=? AND character_id=?", (user_id, char_id))
                        else: c.execute("UPDATE harem SET count=? WHERE user_id=? AND character_id=?", (new_c, user_id, char_id))
                        conn.commit()
                        await update.effective_message.reply_text(f"😈 **HELL EVENT!**\nDisaster! **{name}** was dragged into the abyss and lost!", parse_mode=ParseMode.MARKDOWN)
                else:
                    await update.effective_message.reply_text(f"😈 **HELL EVENT!**\nYou stared into the abyss... and survived with your character.", parse_mode=ParseMode.MARKDOWN)
        # --- END EVENT LOGIC ---

        await check_collection_completion(context, user.id, chat_id)
    else:
        await update.effective_message.reply_text(f"{emoji} Unfortunately, {name} 🆔{char_id} rejected you {user_display}.")
    
    conn.close()
    
    # Consume potions
    consume_potions(user.id, [active_catch, active_rarity])

async def coin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_membership(update, context): return
    if not await check_force_start(update): return
    user = update.effective_user
    if is_banned(user.id): return
    chat_id = update.effective_chat.id
    
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO groups (chat_id, game_mode) VALUES (?, 1)", (chat_id,))
    c.execute("SELECT game_mode FROM groups WHERE chat_id=?", (chat_id,))
    res = c.fetchone()
    game_enabled = res[0] if res else 1
    if not game_enabled:
        conn.close()
        return

    current_time = time.time()
    last_try = try_cooldown.get(user.id, 0)
    if current_time - last_try < 15:
        conn.close()
        remaining = int(15 - (current_time - last_try))
        await update.effective_message.reply_text(f"⏳ Please wait {remaining}s.")
        return

    if len(context.args) < 2:
        conn.close()
        await update.effective_message.reply_text("Usage: `/coin [amount] [tail/head]`", parse_mode=ParseMode.MARKDOWN)
        return
    try:
        amount = int(context.args[0])
        choice = context.args[1].lower()
    except:
        conn.close()
        await update.effective_message.reply_text("Invalid format.")
        return
    if amount <= 0:
        conn.close()
        await update.effective_message.reply_text("Amount must be positive.")
        return
    if choice not in ['head', 'tail']:
        conn.close()
        await update.effective_message.reply_text("Choose 'head' or 'tail'.")
        return
    c.execute("SELECT crystal FROM users WHERE user_id=?", (user.id,))
    res = c.fetchone()
    balance = res[0] if res else 0
    if balance < amount:
        conn.close()
        await update.effective_message.reply_text("❌ Not enough crystals.")
        return
    try_cooldown[user.id] = current_time
    c.execute("UPDATE users SET crystal = crystal - ? WHERE user_id=?", (amount, user.id))
    outcome = random.choice(['head', 'tail'])
    
    await update.effective_message.reply_text(f"🪙 Coin Flipped: {outcome.title()}")
    await asyncio.sleep(1)
    
    if choice == outcome:
        payout = int(amount * 2.5)
        c.execute("UPDATE users SET crystal = crystal + ? WHERE user_id=?", (payout, user.id))
        msg = f"🎉 Congrats! You won {payout} crystals. Time for bigger bets 😎"
    else:
        msg = f"📉 You lost {amount} crystals. Better luck next time 🫂"
    conn.commit()
    conn.close()
    await update.effective_message.reply_text(msg)

async def dice_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_membership(update, context): return
    if not await check_force_start(update): return
    user = update.effective_user
    if is_banned(user.id): return
    chat_id = update.effective_chat.id
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO groups (chat_id, game_mode) VALUES (?, 1)", (chat_id,))
    c.execute("SELECT game_mode FROM groups WHERE chat_id=?", (chat_id,))
    res = c.fetchone()
    game_enabled = res[0] if res else 1
    if not game_enabled:
        conn.close()
        return
    current_time = time.time()
    last_try = try_cooldown.get(user.id, 0)
    if current_time - last_try < 5:
        conn.close()
        remaining = int(5 - (current_time - last_try))
        await update.effective_message.reply_text(f"⏳ Please wait {remaining}s.")
        return
    if len(context.args) < 2:
        conn.close()
        await update.effective_message.reply_text("Usage:\n`/dice [amount] [even/odd]`\n`/dice [amount] [1-6]`", parse_mode=ParseMode.MARKDOWN)
        return
    try:
        amount = int(context.args[0])
        prediction = context.args[1].lower()
    except:
        conn.close()
        await update.effective_message.reply_text("Invalid format.")
        return
    if amount <= 0:
        conn.close()
        await update.effective_message.reply_text("Amount must be positive.")
        return
    c.execute("SELECT crystal FROM users WHERE user_id=?", (user.id,))
    res = c.fetchone()
    balance = res[0] if res else 0
    if balance < amount:
        conn.close()
        await update.effective_message.reply_text("❌ Not enough crystals.")
        return
    mode = None
    target_num = 0
    if prediction in ['even', 'odd']: mode = 'parity'
    elif prediction.isdigit() and 1 <= int(prediction) <= 6:
        mode = 'exact'
        target_num = int(prediction)
    else:
        conn.close()
        await update.effective_message.reply_text("Invalid choice. Use even/odd or 1-6.")
        return
    try_cooldown[user.id] = current_time
    c.execute("UPDATE users SET crystal = crystal - ? WHERE user_id=?", (amount, user.id))
    dice_val = random.randint(1, 6)
    await update.effective_message.reply_text(f"🎲 Dice rolled: {dice_val}")
    won = False
    multiplier = 0
    if mode == 'parity':
        is_even = (dice_val % 2 == 0)
        if (prediction == 'even' and is_even) or (prediction == 'odd' and not is_even):
            won = True
            multiplier = 2.5
    elif mode == 'exact':
        if dice_val == target_num:
            won = True
            multiplier = 7
    if won:
        payout = int(amount * multiplier)
        c.execute("UPDATE users SET crystal = crystal + ? WHERE user_id=?", (payout, user.id))
        msg = f"🎉 Congrats! You won {payout} crystals. Time for bigger bets 😎"
    else:
        msg = f"📉 You lost {amount} crystals. Better luck next time 🫂"
    conn.commit()
    conn.close()
    await update.effective_message.reply_text(msg)

async def pinv_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_membership(update, context): return
    if not await check_force_start(update): return
    user_id = update.effective_user.id
    
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT potion_id, uses_left FROM user_potions WHERE user_id=?", (user_id,))
    rows = c.fetchall()
    conn.close()
    
    if not rows:
        await update.effective_message.reply_text("🎒 Your potion inventory is empty.")
        return
    
    potions = {r[0]: r[1] for r in rows if r[1] > 0}
    if not potions:
        await update.effective_message.reply_text("🎒 Your potion inventory is empty.")
        return

    text = "🎒 **Your Potions:**\n\n"
    
    p_names = {
        1: "💊 Double Luck (2x Catch)", 
        2: "🧪 Triple Luck (3x Catch)",
        3: "💊 Legendary Boost",
        4: "🧪 Premium Boost",
        5: "🧪 Event Boost",
        6: "💉 Hentai Boost"
    }
    
    # Determine Active
    active_catch, active_rarity = get_active_potions(user_id)
    
    for pid, count in potions.items():
        name = p_names.get(pid, "Unknown Potion")
        status = ""
        if pid == active_catch or pid == active_rarity:
            status = " ✅ ACTIVE"
        text += f"• {name}: {count} uses{status}\n"
        
    text += "\n_Only the highest rarity potion of each type is active._"
    await update.effective_message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

async def game_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if is_banned(update.effective_user.id): return
    chat_member = await context.bot.get_chat_member(update.effective_chat.id, update.effective_user.id)
    if update.effective_user.id == OWNER_ID or chat_member.status in ['creator', 'administrator']:
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO groups (chat_id, game_mode) VALUES (?, 1)", (update.effective_chat.id,))
        c.execute("SELECT game_mode FROM groups WHERE chat_id=?", (update.effective_chat.id,))
        res = c.fetchone()
        new_val = 0 if res and res[0] else 1
        c.execute("UPDATE groups SET game_mode = ? WHERE chat_id=?", (new_val, update.effective_chat.id))
        conn.commit()
        conn.close()
        await update.effective_message.reply_text(f"🎮 Game Mode (/try, /dice, /coin): {'ON' if new_val else 'OFF'}")

# --- LAB LOGIC ---

def get_lab_materials(user_id, required_rarities, count):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    placeholders = ','.join('?' for _ in required_rarities)
    
    # Get user owned characters of specific rarity along with their count
    query = f"""
        SELECT h.character_id, h.count 
        FROM harem h 
        JOIN characters c ON h.character_id = c.id 
        WHERE h.user_id = ? AND c.rarity IN ({placeholders})
    """
    c.execute(query, (user_id, *required_rarities))
    owned = c.fetchall()
    conn.close()
    
    # Expand to a list of available IDs (if count > 1, add ID multiple times)
    available_ids = []
    for char_id, cnt in owned:
        available_ids.extend([char_id] * cnt)
        
    if len(available_ids) < count:
        return None
        
    return random.sample(available_ids, count)

async def execute_lab(context, chat_id, user_id, cost_ids, reward_char, is_hentai=False, is_giveaway=False):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    # Remove Cost Chars
    used_chars_text = ""
    # Group by ID for efficient DB update and reporting
    from collections import Counter
    cost_counts = Counter(cost_ids)
    
    # Fetch details for report
    ids_placeholder = ','.join('?' for _ in cost_counts.keys())
    c.execute(f"SELECT id, name, rarity FROM characters WHERE id IN ({ids_placeholder})", tuple(cost_counts.keys()))
    char_details = {row[0]: (row[1], row[2]) for row in c.fetchall()}
    
    for char_id, qty in cost_counts.items():
        c.execute("SELECT count FROM harem WHERE user_id=? AND character_id=?", (user_id, char_id))
        res = c.fetchone()
        current = res[0] if res else 0
        new_count = max(0, current - qty)
        
        if new_count == 0:
            c.execute("DELETE FROM harem WHERE user_id=? AND character_id=?", (user_id, char_id))
        else:
            c.execute("UPDATE harem SET count=? WHERE user_id=? AND character_id=?", (new_count, user_id, char_id))
            
        details = char_details.get(char_id)
        if details:
            name, rarity = details
            used_chars_text += f"{get_rarity_emoji(rarity)} {name} 🆔 {char_id} (x{qty})\n"

    # Add Reward
    # reward_char is tuple (id, name, rarity, file_id, media_type)
    # OR list of tuples if multiple rewards
    rewards = reward_char if isinstance(reward_char, list) else [reward_char]
    
    reward_text_top = ""
    last_msg = None
    
    for r_char in rewards:
        rid, rname, rrarity, rfile, rmedia = r_char
        c.execute("INSERT OR IGNORE INTO harem (user_id, character_id, count) VALUES (?, ?, 0)", (user_id, rid))
        c.execute("UPDATE harem SET count = count + 1 WHERE user_id=? AND character_id=?", (user_id, rid))
        reward_text_top += f"{get_rarity_emoji(rrarity)} {rname} 🆔 {rid}\n"
    
    conn.commit()
    conn.close()
    
    final_caption = f"✅ **Experiment Successful!**\n\nYou received:\n{reward_text_top}\n🧪 **Characters Used:**\n{used_chars_text}"
    
    # Send Image of the LAST reward char (simplified for single reward focus, or loop if multiple)
    # Usually lab gives 1, exp 6 gives 2. Let's send media for the first one or a summary.
    # Requirement: "send image and detail"
    
    rid, rname, rrarity, rfile, rmedia = rewards[0]
    
    try:
        if rmedia == 1:
            last_msg = await context.bot.send_video(chat_id, rfile, caption=final_caption, parse_mode=ParseMode.MARKDOWN)
        else:
            last_msg = await context.bot.send_photo(chat_id, rfile, caption=final_caption, parse_mode=ParseMode.MARKDOWN)
            
        # Timers
        # Premium(6)/Event(7) -> 60s
        # Hentai(8)/HG(1) -> 30s
        delay = 0
        if rrarity in [6, 7]: delay = 60
        elif rrarity in [1, 8]: delay = 30
        
        if delay > 0:
            asyncio.create_task(delete_image_msg(context, chat_id, last_msg.message_id, delay))
            
    except Exception as e:
        await context.bot.send_message(chat_id, final_caption, parse_mode=ParseMode.MARKDOWN)

    await check_collection_completion(context, user_id, chat_id)

async def lab_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_membership(update, context): return
    if not await check_force_start(update): return
    user = update.effective_user
    if is_banned(user.id): return
    
    chat_id = update.effective_chat.id
    
    # Check Game Mode
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT game_mode, hentai_mode, sfw_mode FROM groups WHERE chat_id=?", (chat_id,))
    res = c.fetchone()
    conn.close()
    
    game_mode = res[0] if res else 1
    is_hentai = res[1] if res else 0
    is_sfw = res[2] if res else 0
    
    if not game_mode:
        await update.effective_message.reply_text("❌ Laboratory is closed (Game Mode OFF).")
        return

    # Help Text Construction
    help_text = "🧪 **Laboratory Experiments**\nExchange characters for higher rarities!\nUsage: `/lab {number}`\n\n"
    
    exps = []
    # 1 & 2 Always shown if not pure SFW restriction (actually logic: sfw shows 1&2, hentai off shows 1-5, hentai on 1-8)
    # The prompt says: "ingroups that have sfw mode on only experiment 1 and 2 are shown"
    
    exps.append("1️⃣ **Common -> Rare**\nCost: 3x Common | Reward: 1x Rare")
    exps.append("2️⃣ **Rare -> Legendary**\nCost: 3x Rare | Reward: 1x Legendary")
    
    if not is_sfw:
        exps.append("3️⃣ **Legendary -> Premium**\nCost: 5x Legendary | Reward: 1x Premium")
        exps.append("4️⃣ **Premium -> Event**\nCost: 2x Premium | Reward: 1x Event")
        exps.append("5️⃣ **Event -> Premium**\nCost: 1x Event | Reward: 1x Premium")
        
        if is_hentai:
            exps.append("6️⃣ **Random Hentai**\nCost: 10x (Event/Prem) | Reward: 2x Hentai")
            exps.append("7️⃣ **Random Giveaway**\nCost: 20x Hentai | Reward: Choice (HG/Giveaway)")
            exps.append("8️⃣ **Chosen Giveaway**\nCost: 40x Hentai | Reward: Specific ID (HG/Giveaway)")

    if not context.args or context.args[0] == '0':
        await update.effective_message.reply_text(help_text + "\n\n".join(exps), parse_mode=ParseMode.MARKDOWN)
        return

    try:
        exp_num = int(context.args[0])
    except:
        await update.effective_message.reply_text("Invalid number.")
        return

    # Validate allowed experiment
    if is_sfw and exp_num > 2:
        await update.effective_message.reply_text("❌ Experiment not available in SFW mode.")
        return
    if not is_sfw and not is_hentai and exp_num > 5:
        await update.effective_message.reply_text("❌ Enable Hentai Mode for this experiment.")
        return
    if exp_num < 1 or exp_num > 8:
        await update.effective_message.reply_text("❌ Invalid experiment number.")
        return

    # Check Materials
    req_rarities = []
    req_count = 0
    desc = ""
    
    if exp_num == 1:
        req_rarities = [3]; req_count = 3
        desc = "3 random Common characters will be removed, 1 Rare added."
    elif exp_num == 2:
        req_rarities = [4]; req_count = 3
        desc = "3 random Rare characters will be removed, 1 Legendary added."
    elif exp_num == 3:
        req_rarities = [5]; req_count = 5
        desc = "5 random Legendary characters will be removed, 1 Premium added."
    elif exp_num == 4:
        req_rarities = [6]; req_count = 2
        desc = "2 random Premium characters will be removed, 1 Event added."
    elif exp_num == 5:
        req_rarities = [7]; req_count = 1
        desc = "1 random Event character will be removed, 1 Premium added."
    elif exp_num == 6:
        req_rarities = [6, 7]; req_count = 10
        desc = "10 random Event/Premium characters will be removed, 2 Hentai added."
    elif exp_num == 7:
        req_rarities = [8]; req_count = 20
        desc = "20 random Hentai characters will be removed. You choose between Giveaway/HG."
    elif exp_num == 8:
        req_rarities = [8]; req_count = 40
        desc = "40 random Hentai characters will be removed. You choose a specific ID (Giveaway/HG)."

    materials = get_lab_materials(user.id, req_rarities, req_count)
    if not materials:
        await update.effective_message.reply_text(f"❌ You don't have enough characters for this experiment!\nRequired: {req_count}x Rarity {req_rarities}")
        return

    # Confirm
    keyboard = [
        [InlineKeyboardButton("✅ Yes", callback_data="lab_confirm"), InlineKeyboardButton("❌ Cancel", callback_data="lab_cancel")]
    ]
    msg = await update.effective_message.reply_text(f"🧪 **Experiment #{exp_num}**\n{desc}\n\nAre you sure you want to continue?", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
    
    lab_cache[user.id] = {
        'exp': exp_num,
        'msg_id': msg.message_id,
        'chat_id': chat_id,
        'req_rarity': req_rarities,
        'req_count': req_count
    }

async def lab_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user
    await query.answer()
    
    if user.id not in lab_cache:
        await query.edit_message_text("❌ Session expired.")
        return
        
    session = lab_cache[user.id]
    if query.data == "lab_cancel":
        del lab_cache[user.id]
        await query.edit_message_text("❌ Experiment cancelled.")
        return

    exp_num = session['exp']
    chat_id = session['chat_id']
    
    # Re-verify materials just in case
    materials = get_lab_materials(user.id, session['req_rarity'], session['req_count'])
    if not materials:
        del lab_cache[user.id]
        await query.edit_message_text("❌ Material check failed. Items missing.")
        return

    if query.data == "lab_confirm":
        if exp_num <= 6:
            # Execute Immediate
            target_rarity = 0
            count_add = 1
            if exp_num == 1: target_rarity = 4
            elif exp_num == 2: target_rarity = 5
            elif exp_num == 3: target_rarity = 6
            elif exp_num == 4: target_rarity = 7
            elif exp_num == 5: target_rarity = 6
            elif exp_num == 6: 
                target_rarity = 8
                count_add = 2
            
            rewards = []
            for _ in range(count_add):
                r = get_random_char_by_rarity([target_rarity])
                if r: rewards.append(r)
            
            if not rewards:
                await query.edit_message_text("❌ Error generating reward.")
                return
                
            await query.delete_message()
            await execute_lab(context, chat_id, user.id, materials, rewards)
            del lab_cache[user.id]
            
        elif exp_num == 7:
            # Ask for choice
            keyboard = [
                [InlineKeyboardButton("🎟 Giveaway", callback_data="lab_c7_2"), InlineKeyboardButton("🎫 Hentai Giveaway", callback_data="lab_c7_1")],
                [InlineKeyboardButton("❌ Cancel", callback_data="lab_cancel")]
            ]
            await query.edit_message_text("Select reward rarity:", reply_markup=InlineKeyboardMarkup(keyboard))
            # Don't delete cache yet
            
        elif exp_num == 8:
            session['state'] = "waiting_id"
            await query.edit_message_text("🔢 Please send the ID of the Giveaway/HG character you want.")
            # Don't delete cache
            
    elif query.data.startswith("lab_c7_"):
        target_rarity = int(query.data.split("_")[2])
        reward = get_random_char_by_rarity([target_rarity])
        if not reward:
            await query.edit_message_text("❌ Error generating reward.")
            return
            
        await query.delete_message()
        await execute_lab(context, chat_id, user.id, materials, [reward])
        del lab_cache[user.id]

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_message: return
    user_id = update.effective_user.id
    if is_banned(user_id): return
    text = update.effective_message.text
    
    # --- LAB EXP 8 WAITING ID ---
    if user_id in lab_cache and lab_cache[user_id].get('state') == 'waiting_id':
        if not text.isdigit():
            await update.effective_message.reply_text("❌ Please send a numeric ID.")
            return
            
        target_id = int(text)
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("SELECT id, name, rarity, file_id, media_type FROM characters WHERE id=?", (target_id,))
        char = c.fetchone()
        conn.close()
        
        if not char:
            await update.effective_message.reply_text("❌ Character not found.")
            return
            
        if char[2] not in [1, 2]:
            await update.effective_message.reply_text("❌ Character must be Giveaway or Hentai Giveaway rarity.")
            return
            
        # Execute
        session = lab_cache[user_id]
        materials = get_lab_materials(user_id, session['req_rarity'], session['req_count'])
        if not materials:
            await update.effective_message.reply_text("❌ Materials missing.")
            del lab_cache[user_id]
            return
            
        await execute_lab(context, session['chat_id'], user_id, materials, [char])
        del lab_cache[user_id]
        return

    # --- ADDCOL WAITING FOR REWARD ---
    if user_id in addcol_cache:
        if not text: return
        if not text.isdigit():
            await update.effective_message.reply_text("❌ Please send a valid Character ID for reward.")
            return
        
        reward_id = int(text)
        char_ids = addcol_cache[user_id]
        
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        
        # Verify chars exist
        c.execute("SELECT id FROM characters WHERE id=?", (reward_id,))
        if not c.fetchone():
            await update.effective_message.reply_text("❌ Reward Character ID not found.")
            conn.close()
            return
        
        new_id = get_next_id(c, "collections")
        c.execute("INSERT INTO collections (id, reward_id) VALUES (?, ?)", (new_id, reward_id))
        col_id = new_id
        
        for cid in char_ids:
            c.execute("INSERT OR IGNORE INTO collection_items (collection_id, char_id) VALUES (?, ?)", (col_id, cid))
            
        conn.commit()
        conn.close()
        
        # Update captions for involved characters
        for cid in char_ids:
            await update_channels(context, cid, update.effective_user.first_name, "Edited")
            
        del addcol_cache[user_id]
        await update.effective_message.reply_text(f"✅ Collection {col_id} created with Reward {reward_id}!")
        return
        
    # --- EDITCOL WAITING ---
    if user_id in editcol_cache:
        if not text: return
        data = editcol_cache[user_id]
        col_id = data['col_id']
        mode = data['mode']
        
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        
        if mode == 'reward':
            if not text.isdigit():
                await update.effective_message.reply_text("Invalid ID.")
                return
            new_rew = int(text)
            c.execute("UPDATE collections SET reward_id=? WHERE id=?", (new_rew, col_id))
            conn.commit()
            await update.effective_message.reply_text(f"✅ Reward updated for Col {col_id}.")
            
        elif mode == 'chars':
            try:
                new_ids = [int(x) for x in text.split()]
            except:
                await update.effective_message.reply_text("Invalid IDs.")
                return
                
            # Get old chars to update captions
            c.execute("SELECT char_id FROM collection_items WHERE collection_id=?", (col_id,))
            old_chars = [r[0] for r in c.fetchall()]
            
            c.execute("DELETE FROM collection_items WHERE collection_id=?", (col_id,))
            for cid in new_ids:
                c.execute("INSERT INTO collection_items (collection_id, char_id) VALUES (?, ?)", (col_id, cid))
            conn.commit()
            
            # Update captions
            all_affected = set(old_chars + new_ids)
            for cid in all_affected:
                 await update_channels(context, cid, update.effective_user.first_name, "Edited")
            
            await update.effective_message.reply_text(f"✅ Requirements updated for Col {col_id}.")
            
        conn.close()
        del editcol_cache[user_id]
        return

    if user_id in edit_cache and 'mode' in edit_cache[user_id]:
        data = edit_cache[user_id]
        mode = data['mode']
        if mode == 'event_text':
            if not text: return
            conn = sqlite3.connect(DB_NAME)
            c = conn.cursor()
            c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", ("event_text", text))
            conn.commit()
            conn.close()
            del edit_cache[user_id]
            await update.effective_message.reply_text("✅ Event text updated.")
            return
        char_id = data['id']
        admin_name = data['admin_name']
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        if mode == 'photo':
            new_file_id = None
            if update.effective_message.photo: 
                new_file_id = update.effective_message.photo[-1].file_id
                c.execute("UPDATE characters SET file_id=?, media_type=0 WHERE id=?", (new_file_id, char_id))
            elif update.effective_message.video:
                new_file_id = update.effective_message.video.file_id
                c.execute("UPDATE characters SET file_id=?, media_type=1 WHERE id=?", (new_file_id, char_id))
            elif update.effective_message.animation:
                new_file_id = update.effective_message.animation.file_id
                c.execute("UPDATE characters SET file_id=?, media_type=1 WHERE id=?", (new_file_id, char_id))
            else: return

            await update.effective_message.reply_text("✅ Media updated.")
        elif mode == 'name':
            if not text: return
            c.execute(f"UPDATE characters SET name=? WHERE id=?", (text.title(), char_id))
            await update.effective_message.reply_text(f"✅ Name updated.")
        conn.commit()
        conn.close()
        if 'msg_id' in data:
            try: await context.bot.delete_message(update.effective_chat.id, data['msg_id'])
            except: pass
        await update_channels(context, char_id, admin_name)
        del edit_cache[user_id]
        return
    
    if update.effective_chat.type in ['group', 'supergroup']:
        chat_id = update.effective_chat.id
        current_msg_id = update.effective_message.message_id
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO groups (chat_id, last_spawn_id) VALUES (?, ?)", (chat_id, current_msg_id))
        c.execute("SELECT last_spawn_id, spawn_threshold FROM groups WHERE chat_id=?", (chat_id,))
        row = c.fetchone()
        last_spawn_id = row[0]
        threshold = row[1]
        if not last_spawn_id:
            c.execute("UPDATE groups SET last_spawn_id = ? WHERE chat_id=?", (current_msg_id, chat_id))
            conn.commit()
            conn.close()
            return
        target_id = last_spawn_id + threshold
        if current_msg_id >= target_id:
            conn.close()
            await spawn_character(chat_id, context)
        else:
            conn.close()

async def steal_character(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_membership(update, context): return
    if not await check_force_start(update): return
    if is_banned(update.effective_user.id): return
    chat_id = update.effective_chat.id
    if chat_id not in current_spawns:
        await update.effective_message.reply_text("❌ No character currently spawned.")
        return
    if not context.args:
        await update.effective_message.reply_text("⚠️ Usage: `/steal [name]`", parse_mode=ParseMode.MARKDOWN)
        return
    guess_raw = " ".join(context.args).lower()
    invalid_guesses = ['&', 'and', '+', '-', 'with']
    if guess_raw in invalid_guesses or re.search(r'^[\W_]+$', guess_raw):
        await update.effective_message.reply_text("⚠️ Please enter the correct name.")
        return
    spawn = current_spawns[chat_id]
    full_name = spawn['full_name'].lower()
    valid_tokens = [word for word in re.split(r'[\s&()+\[\]💞-]+', full_name) if word and word not in invalid_guesses]
    guess_tokens = [word for word in re.split(r'[\s&()+\[\]💞-]+', guess_raw) if word]
    success = False
    for token in guess_tokens:
        if token in valid_tokens:
            success = True
            break
    if success:
        user_id = update.effective_user.id
        update_user_info(update.effective_user)
        user_display = get_display_name(update.effective_user)
        
        char_id = spawn['char_id']
        rarity = spawn['rarity']
        
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO harem (user_id, character_id, count) VALUES (?, ?, 0)", (user_id, char_id))
        c.execute("UPDATE harem SET count = count + 1 WHERE user_id=? AND character_id=?", (user_id, char_id))
        reward = get_catch_reward(rarity, is_steal=True)
        c.execute("UPDATE users SET crystal = crystal + ? WHERE user_id=?", (reward, user_id))
        conn.commit()
        conn.close()
        
        await update.effective_message.reply_text(f"🎉 **Congrats!**\n{user_display} caught **{spawn['full_name']}** 🆔{char_id}!\n💰 Earned {reward} Crystals.", parse_mode=ParseMode.MARKDOWN)
        del current_spawns[chat_id]
        
        # --- SPECIAL EVENT LOGIC ---
        event_chance = HENTAI_MODE_BASE.get(rarity, 0) / TOTAL_HENTAI_WEIGHT
        if random.random() < event_chance:
            # Event Triggered
            # 50% Heaven, 50% Hell
            if random.random() < 0.5:
                # Heaven Event
                # 50% Chance for bonus character of same rarity (not same ID)
                if random.random() < 0.5:
                    bonus_char = get_random_char_by_rarity([rarity], exclude_id=char_id)
                    if bonus_char:
                        b_id, b_name, _, _, _ = bonus_char
                        conn = sqlite3.connect(DB_NAME)
                        c = conn.cursor()
                        c.execute("INSERT OR IGNORE INTO harem (user_id, character_id, count) VALUES (?, ?, 0)", (user_id, b_id))
                        c.execute("UPDATE harem SET count = count + 1 WHERE user_id=? AND character_id=?", (user_id, b_id))
                        conn.commit()
                        conn.close()
                        await update.effective_message.reply_text(f"👼 **HEAVEN EVENT!**\nThe gods smiled upon you! You also received **{b_name}** 🆔{b_id}!", parse_mode=ParseMode.MARKDOWN)
                    else:
                        await update.effective_message.reply_text(f"👼 **HEAVEN EVENT!**\nYou entered heaven, but found no extra companion.", parse_mode=ParseMode.MARKDOWN)
                else:
                    await update.effective_message.reply_text(f"👼 **HEAVEN EVENT!**\nYou basked in glory, but received nothing extra this time.", parse_mode=ParseMode.MARKDOWN)
            else:
                # Hell Event
                # 50% Chance to lose the character just caught
                if random.random() < 0.5:
                    conn = sqlite3.connect(DB_NAME)
                    c = conn.cursor()
                    # Check current count to be safe (should be >= 1 since we just added)
                    c.execute("SELECT count FROM harem WHERE user_id=? AND character_id=?", (user_id, char_id))
                    res = c.fetchone()
                    if res and res[0] > 0:
                        new_c = res[0] - 1
                        if new_c == 0: c.execute("DELETE FROM harem WHERE user_id=? AND character_id=?", (user_id, char_id))
                        else: c.execute("UPDATE harem SET count=? WHERE user_id=? AND character_id=?", (new_c, user_id, char_id))
                        conn.commit()
                        conn.close()
                        await update.effective_message.reply_text(f"😈 **HELL EVENT!**\nDisaster! **{spawn['full_name']}** was dragged into the abyss and lost!", parse_mode=ParseMode.MARKDOWN)
                    else:
                        conn.close() # Should not happen logically
                else:
                    await update.effective_message.reply_text(f"😈 **HELL EVENT!**\nYou stared into the abyss... and survived with your character.", parse_mode=ParseMode.MARKDOWN)

        await check_referral_milestones(context, user_id, chat_id)
        await check_collection_completion(context, user_id, chat_id)
    else:
        spawn_msg_id = spawn['message_id']
        chat_id_str = str(chat_id)
        reply_markup = None
        if chat_id_str.startswith("-100"):
            clean_id = chat_id_str[4:]
            link = f"https://t.me/c/{clean_id}/{spawn_msg_id}"
            reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("↗️ Go to Character", url=link)]])
        await update.effective_message.reply_text("❌ Wrong name! Try again.", reply_markup=reply_markup)

def get_harem_page_data(user_id, chat_id, user_first_name, page=1, mode="harem"):
    is_hentai, is_sfw = get_group_settings(chat_id) if chat_id else (0, 0)
    
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    chars_data = []
    if mode == "list":
        c.execute("SELECT name, rarity, 0 as count, id, file_id, media_type FROM characters ORDER BY name ASC")
        chars_data = c.fetchall()
        show_char = None
        filter_rarity = 0
    elif mode == "uncollected":
        c.execute("SELECT name, rarity, 0 as count, id, file_id, media_type FROM characters WHERE id NOT IN (SELECT character_id FROM harem WHERE user_id=?) ORDER BY name ASC", (user_id,))
        chars_data = c.fetchall()
        show_char = None
        filter_rarity = 0 
    else:
        c.execute("SELECT fav_character_id, rarity_filter FROM users WHERE user_id=?", (user_id,))
        user_settings = c.fetchone()
        fav_id = user_settings[0] if user_settings else 0
        filter_rarity = user_settings[1] if user_settings else 0
        c.execute("SELECT c.name, c.rarity, h.count, c.id, c.file_id, c.media_type FROM harem h JOIN characters c ON h.character_id = c.id WHERE h.user_id = ?", (user_id,))
        chars_data = c.fetchall()
        show_char = None
        if fav_id:
            for char in chars_data:
                if char[3] == fav_id:
                    show_char = char
                    break
        if not show_char and chars_data: show_char = random.choice(chars_data)
    conn.close()

    if not chars_data: return None, "List is empty!", None
    
    filtered_list = []
    for char in chars_data:
        rarity = char[1]
        if is_char_allowed(rarity, is_hentai, is_sfw):
             if filter_rarity == 0 or rarity == filter_rarity: filtered_list.append(char)
    
    if not filtered_list: return None, "No characters available to show in this mode.", None
    filtered_list.sort(key=lambda x: x[0].lower())

    if mode == "harem" and show_char:
        if not is_char_allowed(show_char[1], is_hentai, is_sfw):
            if filtered_list: show_char = random.choice(filtered_list)
            else: return None, "No safe character to display.", None
    elif mode == "list" or mode == "uncollected": show_char = random.choice(filtered_list)

    ITEMS_PER_PAGE = 20
    total_pages = math.ceil(len(filtered_list) / ITEMS_PER_PAGE) or 1
    if page < 1: page = 1
    if page > total_pages: page = total_pages
    current_page_chars = filtered_list[(page - 1) * ITEMS_PER_PAGE : page * ITEMS_PER_PAGE]

    if mode == "harem":
        caption = f"🌹 {user_first_name}'s Harem 🌹\n"
        emoji = get_rarity_emoji(show_char[1])
        caption += f"👑💎 🆔{show_char[3]} {emoji} {show_char[0]} x{show_char[2]} 💎👑\n\n"
    elif mode == "uncollected":
        caption = f"📜 Uncollected Characters 📜\n\n"
    else: caption = f"📜 Character List 📜\n\n"

    for char in current_page_chars:
        emoji = get_rarity_emoji(char[1])
        count_str = f" x{char[2]}" if mode == "harem" else ""
        caption += f"🆔{char[3]} {emoji} {char[0]}{count_str}\n"
    caption += f"\nPage {page}/{total_pages}"
    
    buttons = []
    nav_row = []
    
    cb_prefix = "harem"
    if mode == "list": cb_prefix = "list"
    elif mode == "uncollected": cb_prefix = "uncol"
        
    target_id = user_id if mode == "harem" or mode == "uncollected" else 0
    if page > 1: nav_row.append(InlineKeyboardButton("⬅️", callback_data=f"{cb_prefix}_{page-1}_{target_id}"))
    if page < total_pages: nav_row.append(InlineKeyboardButton("➡️", callback_data=f"{cb_prefix}_{page+1}_{target_id}"))
    if nav_row: buttons.append(nav_row)

    if mode == "harem":
        inline_prefix = ""
        if is_hentai: inline_prefix = "hentai "
        elif is_sfw: inline_prefix = "sfw "
        buttons.append([InlineKeyboardButton("👁 See Harem", switch_inline_query_current_chat=f"{inline_prefix}{user_id}")])

    return (show_char[4], show_char[5]), caption, InlineKeyboardMarkup(buttons) if buttons else None

async def myharem(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_membership(update, context): return
    user_id = update.effective_user.id
    if is_banned(user_id): return
    chat_id = update.effective_chat.id
    page = 1
    if context.args and context.args[0].isdigit(): page = int(context.args[0])
    
    file_data, caption, markup = get_harem_page_data(user_id, chat_id, update.effective_user.first_name, page, "harem")
    
    if not file_data: await update.effective_message.reply_text(caption)
    else: 
        file_id, media_type = file_data
        if media_type == 1: await update.effective_message.reply_video(video=file_id, caption=caption, reply_markup=markup)
        else: await update.effective_message.reply_photo(photo=file_id, caption=caption, reply_markup=markup)

async def list_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_membership(update, context): return
    if is_banned(update.effective_user.id): return
    page = 1
    if context.args and context.args[0].isdigit(): page = int(context.args[0])
    file_data, caption, markup = get_harem_page_data(0, update.effective_chat.id, "Global", page, "list")
    
    if not file_data: await update.effective_message.reply_text(caption)
    else: 
        file_id, media_type = file_data
        if media_type == 1: await update.effective_message.reply_video(video=file_id, caption=caption, reply_markup=markup)
        else: await update.effective_message.reply_photo(photo=file_id, caption=caption, reply_markup=markup)

async def uncollected_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_membership(update, context): return
    user_id = update.effective_user.id
    if is_banned(user_id): return
    chat_id = update.effective_chat.id
    page = 1
    if context.args and context.args[0].isdigit(): page = int(context.args[0])
    
    file_data, caption, markup = get_harem_page_data(user_id, chat_id, update.effective_user.first_name, page, "uncollected")
    
    if not file_data: await update.effective_message.reply_text(caption)
    else: 
        file_id, media_type = file_data
        if media_type == 1: await update.effective_message.reply_video(video=file_id, caption=caption, reply_markup=markup)
        else: await update.effective_message.reply_photo(photo=file_id, caption=caption, reply_markup=markup)

async def harem_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if is_banned(query.from_user.id): return
    data = query.data.split("_")
    mode, page, owner_id = data[0], int(data[1]), int(data[2])
    
    if (mode == "harem" or mode == "uncol") and query.from_user.id != owner_id:
        await query.answer("❌ This is not your session!", show_alert=True)
        return
        
    mode_str = "harem"
    if mode == "list": mode_str = "list"
    elif mode == "uncol": mode_str = "uncollected"

    file_data, caption, markup = get_harem_page_data(owner_id, query.message.chat_id, query.from_user.first_name, page, mode_str)
    if not file_data: await query.answer(caption, show_alert=True)
    else:
        file_id, media_type = file_data
        media = InputMediaVideo(media=file_id, caption=caption) if media_type == 1 else InputMediaPhoto(media=file_id, caption=caption)
        try: await query.edit_message_media(media=media, reply_markup=markup)
        except: 
            try: await query.edit_message_caption(caption=caption, reply_markup=markup)
            except: pass

def get_search_page_data(chat_id, query_string, page=1):
    is_hentai, is_sfw = get_group_settings(chat_id)
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT id, name, rarity, file_id, media_type FROM characters WHERE name LIKE ? ORDER BY name ASC", (f"%{query_string}%",))
    all_chars = c.fetchall()
    conn.close()
    
    filtered = []
    for char in all_chars:
        if is_char_allowed(char[2], is_hentai, is_sfw):
            filtered.append(char)
            
    if not filtered: return None, "❌ No characters found.", None
    
    ITEMS_PER_PAGE = 20
    total_pages = math.ceil(len(filtered) / ITEMS_PER_PAGE)
    if page < 1: page = 1
    if page > total_pages: page = total_pages
    
    start = (page - 1) * ITEMS_PER_PAGE
    end = start + ITEMS_PER_PAGE
    page_items = filtered[start:end]
    
    # Pick random image from current page
    show_char = random.choice(page_items)
    
    caption = f"🔎 **Results for:** `{query_string}`\n\n"
    for char in page_items:
        emoji = get_rarity_emoji(char[2])
        caption += f"🆔{char[0]} {emoji} {char[1]}\n"
        
    caption += f"\nPage {page}/{total_pages}"
    
    buttons = []
    if total_pages > 1:
        row = []
        if page > 1: row.append(InlineKeyboardButton("⬅️", callback_data=f"cfind_{page-1}"))
        if page < total_pages: row.append(InlineKeyboardButton("➡️", callback_data=f"cfind_{page+1}"))
        buttons.append(row)
        
    return (show_char[3], show_char[4]), caption, InlineKeyboardMarkup(buttons) if buttons else None

async def cfind(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_membership(update, context): return
    if is_banned(update.effective_user.id): return
    if not context.args:
        await update.effective_message.reply_text("Usage: /cfind [name]")
        return
    
    query_name = " ".join(context.args)
    media_data, caption, markup = get_search_page_data(update.effective_chat.id, query_name, 1)
    
    if not media_data:
        await update.effective_message.reply_text(caption)
        return
        
    file_id, media_type = media_data
    msg = None
    if media_type == 1:
        msg = await update.effective_message.reply_video(file_id, caption=caption, reply_markup=markup, parse_mode=ParseMode.MARKDOWN)
    else:
        msg = await update.effective_message.reply_photo(file_id, caption=caption, reply_markup=markup, parse_mode=ParseMode.MARKDOWN)
        
    if msg:
        search_sessions[msg.message_id] = query_name

async def cfind_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    msg_id = query.message.message_id
    if msg_id not in search_sessions:
        await query.answer("Search expired.", show_alert=True)
        return
        
    query_string = search_sessions[msg_id]
    page = int(query.data.split("_")[1])
    
    media_data, caption, markup = get_search_page_data(query.message.chat_id, query_string, page)
    if not media_data:
        await query.edit_message_caption("❌ Error.")
        return
        
    file_id, media_type = media_data
    media = InputMediaVideo(file_id, caption=caption, parse_mode=ParseMode.MARKDOWN) if media_type == 1 else InputMediaPhoto(file_id, caption=caption, parse_mode=ParseMode.MARKDOWN)
    
    try: await query.edit_message_media(media=media, reply_markup=markup)
    except: pass

async def see(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_membership(update, context): return
    if is_banned(update.effective_user.id): return
    if not context.args: return
    try:
        char_id = int(context.args[0])
        is_hentai, is_sfw = get_group_settings(update.effective_chat.id)
        
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("SELECT name, rarity, file_id, media_type FROM characters WHERE id=?", (char_id,))
        res = c.fetchone()
        
        if res:
            name, rarity_level, file_id, media_type = res
            if not is_char_allowed(rarity_level, is_hentai, is_sfw):
                await update.effective_message.reply_text("SFW/NSFW settings prevent viewing this.")
                conn.close()
                return
            
            c.execute("SELECT SUM(count) FROM harem WHERE character_id=? AND user_id NOT IN (SELECT user_id FROM sudos) AND user_id NOT IN (SELECT user_id FROM uploaders)", (char_id,))
            global_count = c.fetchone()[0] or 0
            
            c.execute("""SELECT u.first_name, h.count FROM harem h JOIN users u ON h.user_id = u.user_id WHERE h.character_id = ? AND h.user_id NOT IN (SELECT user_id FROM sudos) AND h.user_id NOT IN (SELECT user_id FROM uploaders) ORDER BY h.count DESC, RANDOM() LIMIT 10""", (char_id,))
            top_10 = c.fetchall()
            
            # Get Collection Info for /see
            c.execute("SELECT collection_id FROM collection_items WHERE char_id=?", (char_id,))
            col_rows = c.fetchall()
            col_text = ""
            if col_rows:
                ids = sorted([r[0] for r in col_rows])
                col_text = f"\n🎗Collections: {', '.join(map(str, ids))}"
                
            conn.close()
            
            caption = f"👤 {name}\n{get_rarity_text(rarity_level)}\n🆔 {char_id}{col_text}\n\n🌏 ᴄᴀᴜɢʜᴛ ɢʟᴏʙᴀʟʟʏ: {global_count}\n\n🎖 ᴛᴏᴘ 10:\n"
            for i, (fname, count) in enumerate(top_10, 1):
                caption += f"{get_rank_emoji(i)} {fname} ➡️ {count}\n"
            
            msg = None
            if media_type == 1: msg = await update.effective_message.reply_video(file_id, caption=caption)
            else: msg = await update.effective_message.reply_photo(file_id, caption=caption)
                
            delay = 0
            if rarity_level in [1, 8]: delay = 30
            elif rarity_level in [6, 7, 9]: delay = 60
            if delay > 0: asyncio.create_task(delete_image_msg(context, update.effective_chat.id, msg.message_id, delay))
        else:
            conn.close()
            await update.effective_message.reply_text("Not found.")
    except: pass

async def buy_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_membership(update, context): return
    if not await check_force_start(update): return
    if is_banned(update.effective_user.id): return
    if not context.args:
        await update.effective_message.reply_text("Usage: /buy [id]")
        return
    try:
        char_id = int(context.args[0])
        is_hentai, is_sfw = get_group_settings(update.effective_chat.id)
        
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("SELECT name, rarity, file_id, media_type FROM characters WHERE id=?", (char_id,))
        char_data = c.fetchone()
        
        if not char_data:
            await update.effective_message.reply_text("Character not found.")
            conn.close()
            return
            
        name, rarity, file_id, media_type = char_data
        
        if not is_char_allowed(rarity, is_hentai, is_sfw):
            await update.effective_message.reply_text("SFW/NSFW settings prevent viewing this.")
            conn.close()
            return
            
        if rarity in [1, 2]:
            await update.effective_message.reply_text("❌ Cannot buy this type.")
            conn.close()
            return
        
        if rarity == 10:
             await update.effective_message.reply_text("❌ Collector rarity cannot be purchased.")
             conn.close()
             return
        
        c.execute("SELECT SUM(count) FROM harem WHERE character_id=? AND user_id NOT IN (SELECT user_id FROM sudos) AND user_id NOT IN (SELECT user_id FROM uploaders)", (char_id,))
        global_count = c.fetchone()[0] or 0
        conn.close()
        price = calculate_price(rarity, global_count)
        caption = f"👤 {name}\n{get_rarity_text(rarity)}\n🆔 {char_id}\n\n💰 Price: {price} Crystals\n\nDo you want to buy?"
        keyboard = [[InlineKeyboardButton(f"Buy ({price})", callback_data=f"buy_confirm_{char_id}_{price}")], [InlineKeyboardButton("Cancel", callback_data="buy_cancel")]]
        
        msg = None
        if media_type == 1: msg = await update.effective_message.reply_video(file_id, caption=caption, reply_markup=InlineKeyboardMarkup(keyboard))
        else: msg = await update.effective_message.reply_photo(file_id, caption=caption, reply_markup=InlineKeyboardMarkup(keyboard))
            
        asyncio.create_task(delete_image_msg(context, update.effective_chat.id, msg.message_id, 40))
    except ValueError: await update.effective_message.reply_text("ID must be number.")

async def buy_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user
    if is_banned(user.id): return
    await query.answer()
    data = query.data.split("_")
    action = data[1]
    if action == "cancel":
        try: await query.message.delete()
        except: pass
        return
    if action == "confirm":
        char_id, price = int(data[2]), int(data[3])
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("SELECT crystal FROM users WHERE user_id=?", (user.id,))
        res = c.fetchone()
        if not res or res[0] < price:
            await query.edit_message_caption("❌ Not enough crystals!")
            conn.close()
            return
        c.execute("UPDATE users SET crystal = crystal - ? WHERE user_id=?", (price, user.id))
        c.execute("INSERT OR IGNORE INTO harem (user_id, character_id, count) VALUES (?, ?, 0)", (user.id, char_id))
        c.execute("UPDATE harem SET count = count + 1 WHERE user_id=? AND character_id=?", (user.id, char_id))
        conn.commit()
        conn.close()
        await query.edit_message_caption(f"✅ Purchase successful! ID: {char_id}")
        await check_referral_milestones(context, user.id, query.message.chat_id)
        await check_collection_completion(context, user.id, query.message.chat_id)

async def giftc_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_membership(update, context): return
    if not await check_force_start(update): return
    if is_banned(update.effective_user.id): return
    if not update.effective_message.reply_to_message:
        await update.effective_message.reply_text("⚠️ Reply to user.")
        return
    if update.effective_message.reply_to_message.from_user.id == update.effective_user.id:
        await update.effective_message.reply_text("⚠️ Self gift?")
        return
    if not context.args:
        await update.effective_message.reply_text("⚠️ Usage: `/giftc [char_id]`")
        return
    try:
        char_id = int(context.args[0])
        is_hentai, is_sfw = get_group_settings(update.effective_chat.id)
        
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        
        c.execute("SELECT name, rarity, file_id, media_type FROM characters WHERE id=?", (char_id,))
        char_data = c.fetchone()
        if not char_data:
            await update.effective_message.reply_text("Character not found.")
            conn.close()
            return
            
        name, rarity, file_id, media_type = char_data
        
        if not is_char_allowed(rarity, is_hentai, is_sfw):
            await update.effective_message.reply_text("SFW/NSFW settings prevent viewing this.")
            conn.close()
            return

        sender_id = update.effective_user.id
        receiver_id = update.effective_message.reply_to_message.from_user.id
        receiver_name = update.effective_message.reply_to_message.from_user.first_name

        c.execute("SELECT count FROM harem WHERE user_id=? AND character_id=?", (sender_id, char_id))
        if not c.fetchone():
            await update.effective_message.reply_text("❌ You don't have this character.")
            conn.close()
            return
        
        conn.close()
        caption = f"👤 {name} (ID: {char_id})\n\nAre you sure you want to gift this to {receiver_name}?"
        keyboard = [
            [InlineKeyboardButton("✅ Yes", callback_data=f"gift_confirm_{receiver_id}_{char_id}"), 
             InlineKeyboardButton("❌ No", callback_data="gift_cancel")]
        ]
        
        if media_type == 1: await update.effective_message.reply_video(file_id, caption=caption, reply_markup=InlineKeyboardMarkup(keyboard))
        else: await update.effective_message.reply_photo(file_id, caption=caption, reply_markup=InlineKeyboardMarkup(keyboard))
            
    except:
        await update.effective_message.reply_text("ID error.")

async def gift_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    sender = query.from_user
    if is_banned(sender.id): return
    await query.answer()
    data = query.data.split("_")
    action = data[1]
    if action == "cancel":
        await query.message.delete()
        return
    if action == "confirm":
        receiver_id = int(data[2])
        char_id = int(data[3])
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("SELECT count FROM harem WHERE user_id=? AND character_id=?", (sender.id, char_id))
        res = c.fetchone()
        if not res or res[0] < 1:
            await query.edit_message_caption("❌ You no longer have this character.")
            conn.close()
            return
        new_count = res[0] - 1
        if new_count == 0: c.execute("DELETE FROM harem WHERE user_id=? AND character_id=?", (sender.id, char_id))
        else: c.execute("UPDATE harem SET count = ? WHERE user_id=? AND character_id=?", (new_count, sender.id, char_id))
        c.execute("INSERT OR IGNORE INTO harem (user_id, character_id, count) VALUES (?, ?, 0)", (receiver_id, char_id))
        c.execute("UPDATE harem SET count = count + 1 WHERE user_id=? AND character_id=?", (receiver_id, char_id))
        conn.commit()
        conn.close()
        await query.edit_message_caption(f"🎁 Successfully gifted character {char_id}!")
        await check_referral_milestones(context, receiver_id, query.message.chat_id)
        await check_collection_completion(context, receiver_id, query.message.chat_id)

async def sell_single_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_membership(update, context): return
    if not await check_force_start(update): return
    if is_banned(update.effective_user.id): return
    if not context.args: return
    try:
        char_id = int(context.args[0])
        is_hentai, is_sfw = get_group_settings(update.effective_chat.id)
        
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("SELECT name, rarity, file_id, media_type FROM characters WHERE id=?", (char_id,))
        char_data = c.fetchone()
        
        if not char_data:
            conn.close()
            return
            
        name, rarity, file_id, media_type = char_data
        
        if not is_char_allowed(rarity, is_hentai, is_sfw):
            await update.effective_message.reply_text("SFW/NSFW settings prevent viewing this.")
            conn.close()
            return
            
        c.execute("SELECT count FROM harem WHERE user_id=? AND character_id=?", (update.effective_user.id, char_id))
        if not c.fetchone():
            await update.effective_message.reply_text("❌ You don't have this.")
            conn.close()
            return
            
        conn.close()
        if rarity in [1, 2]:
             await update.effective_message.reply_text("❌ Cannot sell this type.")
             return
             
        if rarity == 10:
             await update.effective_message.reply_text("❌ Collector rarity cannot be sold.")
             return

        price = get_sell_price(rarity)
        caption = f"👤 {name}\n{get_rarity_text(rarity)}\n🆔 {char_id}\n\n💰 Sell Price: {price} Crystals\n\nSell 1 count?"
        keyboard = [[InlineKeyboardButton(f"Sell for {price}", callback_data=f"sellchar_{char_id}_{price}")], [InlineKeyboardButton("Cancel", callback_data="buy_cancel")]]
        
        msg = None
        if media_type == 1: msg = await update.effective_message.reply_video(file_id, caption=caption, reply_markup=InlineKeyboardMarkup(keyboard))
        else: msg = await update.effective_message.reply_photo(file_id, caption=caption, reply_markup=InlineKeyboardMarkup(keyboard))
            
        asyncio.create_task(delete_image_msg(context, update.effective_chat.id, msg.message_id, 40))
    except: pass

async def sell_single_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user
    if is_banned(user.id): return
    await query.answer()
    data = query.data.split("_")
    char_id, price = int(data[1]), int(data[2])
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT count FROM harem WHERE user_id=? AND character_id=?", (user.id, char_id))
    res = c.fetchone()
    if not res or res[0] < 1:
        await query.edit_message_caption("❌ Error.")
        conn.close()
        return
    new_count = res[0] - 1
    if new_count == 0: c.execute("DELETE FROM harem WHERE user_id=? AND character_id=?", (user.id, char_id))
    else: c.execute("UPDATE harem SET count=? WHERE user_id=? AND character_id=?", (new_count, user.id, char_id))
    c.execute("UPDATE users SET crystal = crystal + ? WHERE user_id=?", (price, user.id))
    conn.commit()
    conn.close()
    await query.edit_message_caption(f"✅ Sold ID {char_id} for {price} Crystals.")

async def transfer_harem_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_membership(update, context): return
    if not await check_force_start(update): return
    if is_banned(update.effective_user.id): return
    if not update.effective_message.reply_to_message:
        await update.effective_message.reply_text("Reply to user.")
        return
    sender = update.effective_user
    target = update.effective_message.reply_to_message.from_user
    if sender.id == target.id: return
    text = f"⚠️ Are you sure you want to transfer ALL your characters to {target.first_name}?"
    keyboard = [[InlineKeyboardButton("Yes, Transfer", callback_data=f"sell_confirm_{target.id}")], [InlineKeyboardButton("Cancel", callback_data="sell_cancel")]]
    msg = await update.effective_message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    sell_requests[sender.id] = {'target_id': target.id, 'msg_id': msg.message_id}

async def sell_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user
    if is_banned(user.id): return
    await query.answer()
    if query.data == "sell_cancel":
        if user.id in sell_requests: del sell_requests[user.id]
        await query.edit_message_text("❌ Cancelled.")
        return
    if user.id not in sell_requests: return
    target_id_req = sell_requests[user.id]['target_id']
    target_id_cb = int(query.data.split("_")[2])
    if target_id_req != target_id_cb: return
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT character_id, count FROM harem WHERE user_id=?", (user.id,))
    items = c.fetchall()
    if not items:
        await query.edit_message_text("❌ Your harem is empty.")
        conn.close()
        return
    for char_id, count in items:
        c.execute("INSERT OR IGNORE INTO harem (user_id, character_id, count) VALUES (?, ?, 0)", (target_id_req, char_id))
        c.execute("UPDATE harem SET count = count + ? WHERE user_id=? AND character_id=?", (count, target_id_req, char_id))
    c.execute("DELETE FROM harem WHERE user_id=?", (user.id,))
    conn.commit()
    conn.close()
    del sell_requests[user.id]
    await query.edit_message_text("✅ Transfer complete.")
    await check_collection_completion(context, target_id_req, query.message.chat_id)

async def delete_character(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if is_banned(user.id) or not is_sudo(user.id): return
    if not context.args: return
    try:
        char_id = int(context.args[0])
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("SELECT name, rarity, file_id, media_type FROM characters WHERE id=?", (char_id,))
        row = c.fetchone()
        conn.close()
        if row:
            name, rarity, file_id, media_type = row
            caption = f"👤 {name}\n{get_rarity_text(rarity)}\n🆔 {char_id}\n\n⚠️ **ARE YOU SURE YOU WANT TO DELETE THIS?**"
            keyboard = [[InlineKeyboardButton("🗑 Delete", callback_data=f"del_confirm_{char_id}"), InlineKeyboardButton("❌ Cancel", callback_data="del_cancel")]]
            
            if media_type == 1:
                await update.effective_message.reply_video(video=file_id, caption=caption, reply_markup=InlineKeyboardMarkup(keyboard))
            else:
                await update.effective_message.reply_photo(photo=file_id, caption=caption, reply_markup=InlineKeyboardMarkup(keyboard))
        else: await update.effective_message.reply_text("Not found")
    except: pass

async def delete_character_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user
    if not is_sudo(user.id): return
    await query.answer()
    
    data = query.data
    if data == "del_cancel":
        try: await query.message.delete()
        except: pass
        return
        
    char_id = int(data.split("_")[2])
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT log_msg_id1, log_msg_id2 FROM characters WHERE id=?", (char_id,))
    row = c.fetchone()
    
    if row:
        try:
            if row[0]: await context.bot.delete_message(DATABASE_CHANNEL_ID, row[0])
            if row[1]: await context.bot.delete_message(DATABASE_CHANNEL_ID2, row[1])
        except: pass
        
    c.execute("DELETE FROM characters WHERE id=?", (char_id,))
    c.execute("DELETE FROM harem WHERE character_id=?", (char_id,))
    c.execute("DELETE FROM collection_items WHERE char_id=?", (char_id,))
    conn.commit()
    conn.close()
    
    await query.edit_message_caption("✅ Deleted.")

async def event_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_membership(update, context): return
    if is_banned(update.effective_user.id): return
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT value FROM settings WHERE key='event_text'")
    res = c.fetchone()
    conn.close()
    event_text = res[0] if res else "No active events."
    await update.effective_message.reply_text(f"Current events:\n{event_text}")

async def editevent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if is_banned(update.effective_user.id) or not is_sudo(update.effective_user.id): return
    edit_cache[update.effective_user.id] = {'mode': 'event_text'}
    await update.effective_message.reply_text("Send new event text.")

async def addsudo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id == OWNER_ID and update.effective_message.reply_to_message:
        target = update.effective_message.reply_to_message.from_user.id
        conn = sqlite3.connect(DB_NAME)
        conn.execute("INSERT OR IGNORE INTO sudos (user_id) VALUES (?)", (target,))
        conn.commit()
        conn.close()
        await update.effective_message.reply_text("Added Sudo.")

async def remsudo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id == OWNER_ID:
        target = int(context.args[0]) if context.args else (update.effective_message.reply_to_message.from_user.id if update.effective_message.reply_to_message else None)
        if target:
            conn = sqlite3.connect(DB_NAME)
            conn.execute("DELETE FROM sudos WHERE user_id=?", (target,))
            conn.commit()
            conn.close()
            await update.effective_message.reply_text("Removed Sudo.")

async def adduploader(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if is_sudo(update.effective_user.id):
        target = int(context.args[0]) if context.args else (update.effective_message.reply_to_message.from_user.id if update.effective_message.reply_to_message else None)
        if target:
            conn = sqlite3.connect(DB_NAME)
            conn.execute("INSERT OR IGNORE INTO uploaders (user_id) VALUES (?)", (target,))
            conn.commit()
            conn.close()
            await update.effective_message.reply_text("Added Uploader.")

async def remuploader(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if is_sudo(update.effective_user.id):
        target = int(context.args[0]) if context.args else (update.effective_message.reply_to_message.from_user.id if update.effective_message.reply_to_message else None)
        if target:
            conn = sqlite3.connect(DB_NAME)
            conn.execute("DELETE FROM uploaders WHERE user_id=?", (target,))
            conn.commit()
            conn.close()
            await update.effective_message.reply_text("Removed Uploader.")

async def uploadlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if is_sudo(update.effective_user.id):
        conn = sqlite3.connect(DB_NAME)
        rows = conn.execute("SELECT u.username, u.first_name, t.user_id FROM uploaders t LEFT JOIN users u ON t.user_id = u.user_id").fetchall()
        conn.close()
        names = []
        for r in rows:
            username, first_name, uid = r
            if username: names.append(f"{username}")
            elif first_name: names.append(f"{first_name}")
            else: names.append(str(uid))
        await update.effective_message.reply_text(f"Uploaders: {', '.join(names)}")

async def sudolist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id == OWNER_ID:
        conn = sqlite3.connect(DB_NAME)
        rows = conn.execute("SELECT u.username, u.first_name, s.user_id FROM sudos s LEFT JOIN users u ON s.user_id = u.user_id").fetchall()
        conn.close()
        names = []
        for r in rows:
            username, first_name, uid = r
            if username: names.append(f"{username}")
            elif first_name: names.append(f"{first_name}")
            else: names.append(str(uid))
        await update.effective_message.reply_text(f"Sudos: {', '.join(names)}")

async def donate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not (is_uploader(user_id) or is_sudo(user_id)): return

    if update.effective_message.reply_to_message and context.args:
        try:
            char_id = int(context.args[0])
            target = update.effective_message.reply_to_message.from_user.id
            
            if not is_sudo(user_id) and target != user_id:
                await update.effective_message.reply_text("❌ You can only donate to yourself.")
                return

            conn = sqlite3.connect(DB_NAME)
            conn.execute("INSERT OR IGNORE INTO harem (user_id, character_id, count) VALUES (?, ?, 0)", (target, char_id))
            conn.execute("UPDATE harem SET count = count + 1 WHERE user_id=? AND character_id=?", (target, char_id))
            conn.commit()
            conn.close()
            await update.effective_message.reply_text("Donated.")
            await check_collection_completion(context, target, update.effective_chat.id)
        except: pass

async def donatec(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if is_sudo(update.effective_user.id) and update.effective_message.reply_to_message and context.args:
        try:
            amt = int(context.args[0])
            target = update.effective_message.reply_to_message.from_user.id
            conn = sqlite3.connect(DB_NAME)
            conn.execute("UPDATE users SET crystal = crystal + ? WHERE user_id=?", (amt, target))
            conn.commit()
            conn.close()
            await update.effective_message.reply_text(f"Added {amt}.")
        except: pass

async def removec(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if is_sudo(update.effective_user.id) and update.effective_message.reply_to_message and context.args:
        try:
            amt = int(context.args[0])
            target = update.effective_message.reply_to_message.from_user.id
            conn = sqlite3.connect(DB_NAME)
            conn.execute("UPDATE users SET crystal = MAX(0, crystal - ?) WHERE user_id=?", (amt, target))
            conn.commit()
            conn.close()
            await update.effective_message.reply_text(f"Removed {amt}.")
        except: pass

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID: return
    if not update.effective_message.reply_to_message:
        await update.effective_message.reply_text("Reply to a message to broadcast.")
        return
    
    conn = sqlite3.connect(DB_NAME)
    users = [r[0] for r in conn.execute("SELECT user_id FROM users").fetchall()]
    groups = [r[0] for r in conn.execute("SELECT chat_id FROM groups").fetchall()]
    conn.close()
    count = 0
    for tid in set(users + groups):
        try:
            await context.bot.copy_message(tid, update.effective_chat.id, update.effective_message.reply_to_message.message_id)
            count += 1
            await asyncio.sleep(0.05)
        except: pass
    await update.effective_message.reply_text(f"Broadcast to {count}.")

async def hentai_ok(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if is_banned(update.effective_user.id): return
    chat_member = await context.bot.get_chat_member(update.effective_chat.id, update.effective_user.id)
    if update.effective_user.id == OWNER_ID or chat_member.status in ['creator', 'administrator']:
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO groups (chat_id) VALUES (?)", (update.effective_chat.id,))
        c.execute("SELECT sfw_mode, hentai_mode FROM groups WHERE chat_id=?", (update.effective_chat.id,))
        res = c.fetchone()
        sfw = res[0] if res else 0
        current = res[1] if res else 0
        if sfw: 
            await update.effective_message.reply_text("Disable SFW first.")
            conn.close()
            return
        new = 0 if current else 1
        c.execute("UPDATE groups SET hentai_mode=? WHERE chat_id=?", (new, update.effective_chat.id))
        conn.commit()
        conn.close()
        await update.effective_message.reply_text(f"Hentai: {'ON' if new else 'OFF'}")

async def sfw_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if is_banned(update.effective_user.id): return
    chat_member = await context.bot.get_chat_member(update.effective_chat.id, update.effective_user.id)
    if update.effective_user.id == OWNER_ID or chat_member.status in ['creator', 'administrator']:
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO groups (chat_id) VALUES (?)", (update.effective_chat.id,))
        c.execute("SELECT sfw_mode FROM groups WHERE chat_id=?", (update.effective_chat.id,))
        res = c.fetchone()
        new = 0 if res and res[0] else 1
        if new: c.execute("UPDATE groups SET sfw_mode=1, hentai_mode=0 WHERE chat_id=?", (update.effective_chat.id,))
        else: c.execute("UPDATE groups SET sfw_mode=0 WHERE chat_id=?", (update.effective_chat.id,))
        conn.commit()
        conn.close()
        await update.effective_message.reply_text(f"SFW: {'ON' if new else 'OFF'}")

async def set_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if is_banned(update.effective_user.id): return
    chat_member = await context.bot.get_chat_member(update.effective_chat.id, update.effective_user.id)
    if (update.effective_user.id == OWNER_ID or chat_member.status in ['creator', 'administrator']) and context.args:
        try:
            val = int(context.args[0])
            # Clamp threshold between 50 and 1000 unless sudo
            if not is_sudo(update.effective_user.id):
                if val < 50 or val > 1000:
                    await update.effective_message.reply_text("⚠️ Threshold must be between 50 and 1000.")
                    return
                
            conn = sqlite3.connect(DB_NAME)
            conn.execute("INSERT OR IGNORE INTO groups (chat_id) VALUES (?)", (update.effective_chat.id,))
            conn.execute("UPDATE groups SET spawn_threshold=?, last_spawn_id=? WHERE chat_id=?", (val, update.effective_message.message_id, update.effective_chat.id))
            conn.commit()
            conn.close()
            await update.effective_message.reply_text(f"Threshold: {val}")
        except: pass

async def topg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_membership(update, context): return
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    # Get top 10 regular users (excluding sudos/uploaders)
    c.execute("""
        SELECT u.first_name, SUM(h.count) as total 
        FROM harem h 
        JOIN users u ON h.user_id = u.user_id 
        WHERE h.user_id NOT IN (SELECT user_id FROM sudos) 
          AND h.user_id NOT IN (SELECT user_id FROM uploaders) 
        GROUP BY h.user_id 
        ORDER BY total DESC 
        LIMIT 10
    """)
    top_10 = c.fetchall()

    # Calculate current user's total
    user_id = update.effective_user.id
    c.execute("SELECT SUM(count) FROM harem WHERE user_id=?", (user_id,))
    res = c.fetchone()
    my_total = res[0] if res and res[0] else 0

    # Calculate rank based on regular users pool
    # Count how many regular users have more total than current user
    c.execute("""
        SELECT COUNT(*) 
        FROM (
            SELECT user_id, SUM(count) as total 
            FROM harem 
            WHERE user_id NOT IN (SELECT user_id FROM sudos) 
              AND user_id NOT IN (SELECT user_id FROM uploaders) 
            GROUP BY user_id
        ) sub 
        WHERE sub.total > ?
    """, (my_total,))
    rank_res = c.fetchone()
    my_rank = (rank_res[0] if rank_res else 0) + 1
    
    conn.close()

    caption = "🌏 Top 10 Global\n\n"
    for i, (fname, count) in enumerate(top_10, 1): caption += f"{get_rank_emoji(i)} {fname} ➡️ {count}\n"
    
    caption += f"\n🔢 You: {my_total} (Rank: {my_rank})"
    await update.effective_message.reply_text(caption)

async def topc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_membership(update, context): return
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    # Get top 10 regular users
    c.execute("""
        SELECT first_name, crystal 
        FROM users 
        WHERE user_id NOT IN (SELECT user_id FROM sudos) 
          AND user_id NOT IN (SELECT user_id FROM uploaders) 
        ORDER BY crystal DESC 
        LIMIT 10
    """)
    top_10 = c.fetchall()

    # Calculate current user's crystals
    user_id = update.effective_user.id
    c.execute("SELECT crystal FROM users WHERE user_id=?", (user_id,))
    res = c.fetchone()
    my_crystal = res[0] if res else 0

    # Calculate rank based on regular users pool
    c.execute("""
        SELECT COUNT(*) 
        FROM users 
        WHERE user_id NOT IN (SELECT user_id FROM sudos) 
          AND user_id NOT IN (SELECT user_id FROM uploaders) 
          AND crystal > ?
    """, (my_crystal,))
    rank_res = c.fetchone()
    my_rank = (rank_res[0] if rank_res else 0) + 1

    conn.close()

    caption = "💰 Top 10 Richest\n\n"
    for i, (fname, cry) in enumerate(top_10, 1): caption += f"{get_rank_emoji(i)} {fname} ➡️ {cry}\n"
    
    caption += f"\n🔢 You: {my_crystal} (Rank: {my_rank})"
    await update.effective_message.reply_text(caption)

async def fav_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_membership(update, context): return
    if not await check_force_start(update): return
    if not context.args: return
    try:
        char_id = int(context.args[0])
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        if char_id == 0:
            c.execute("UPDATE users SET fav_character_id=0 WHERE user_id=?", (update.effective_user.id,))
            await update.effective_message.reply_text("✅ Removed fav.")
        else:
            c.execute("SELECT count FROM harem WHERE user_id=? AND character_id=?", (update.effective_user.id, char_id))
            if not c.fetchone():
                await update.effective_message.reply_text("❌ You don't have this.")
            else:
                c.execute("UPDATE users SET fav_character_id=? WHERE user_id=?", (char_id, update.effective_user.id))
                await update.effective_message.reply_text(f"✅ Fav set: {char_id}")
        conn.commit()
        conn.close()
    except: pass

async def type_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_membership(update, context): return
    if not await check_force_start(update): return
    keyboard = [
        [InlineKeyboardButton("All", callback_data="filter_0")],
        [InlineKeyboardButton("🎫 HG", callback_data="filter_1"), InlineKeyboardButton("🎟 Give", callback_data="filter_2")],
        [InlineKeyboardButton("⚫️ Com", callback_data="filter_3"), InlineKeyboardButton("🟠 Rare", callback_data="filter_4")],
        [InlineKeyboardButton("🟡 Leg", callback_data="filter_5"), InlineKeyboardButton("🫧 Prem", callback_data="filter_6")],
        [InlineKeyboardButton("🔮 Evt", callback_data="filter_7"), InlineKeyboardButton("🔞 Hentai", callback_data="filter_8")],
        [InlineKeyboardButton("🎗 Col", callback_data="filter_10"), InlineKeyboardButton("🎞 Ani", callback_data="filter_9")]
    ]
    await update.effective_message.reply_text("Select rarity filter:", reply_markup=InlineKeyboardMarkup(keyboard))

async def filter_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    rarity = int(query.data.split("_")[1])
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("UPDATE users SET rarity_filter=? WHERE user_id=?", (rarity, query.from_user.id))
    conn.commit()
    conn.close()
    await query.edit_message_text(f"✅ Filter set.")

async def giveaway_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID: return
    if not context.args: return
    char_id = int(context.args[0])
    giveaway_data['id'] = char_id
    giveaway_data['end_time'] = time.time() + 3600
    giveaway_data['claimed_users'] = set()
    await update.effective_message.reply_text(f"🎁 Giveaway set for ID: {char_id} for 1 hour!")

async def cgiveaway_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID: return
    giveaway_data['id'] = None
    giveaway_data['end_time'] = 0
    giveaway_data['claimed_users'] = set()
    await update.effective_message.reply_text(f"❌ Giveaway Cancelled.")

async def claim_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_membership(update, context): return
    if not await check_force_start(update): return
    if is_banned(update.effective_user.id): return
    
    char_id = giveaway_data['id']
    end_time = giveaway_data['end_time']
    
    if not char_id or time.time() > end_time:
        await update.effective_message.reply_text("❌ No active giveaway.")
        return
    
    user_id = update.effective_user.id
    if user_id in giveaway_data['claimed_users']:
        await update.effective_message.reply_text("❌ You already claimed this!")
        return

    update_user_info(update.effective_user)
    
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT file_id, name, rarity, media_type FROM characters WHERE id=?", (char_id,))
    char_data = c.fetchone()
    
    if char_data:
        file_id, name, rarity, media_type = char_data
        
        is_hentai, is_sfw = get_group_settings(update.effective_chat.id)
        if not is_char_allowed(rarity, is_hentai, is_sfw):
             await update.effective_message.reply_text("❌ You claimed the character, but cannot view it here due to SFW settings.")
        else:
            if media_type == 1: await update.effective_message.reply_video(video=file_id, caption=f"🎉 You claimed {name} (ID: {char_id})!")
            else: await update.effective_message.reply_photo(photo=file_id, caption=f"🎉 You claimed {name} (ID: {char_id})!")

        c.execute("INSERT OR IGNORE INTO harem (user_id, character_id, count) VALUES (?, ?, 0)", (user_id, char_id))
        c.execute("UPDATE harem SET count = count + 1 WHERE user_id=? AND character_id=?", (user_id, char_id))
        conn.commit()
        giveaway_data['claimed_users'].add(user_id)
        await check_referral_milestones(context, user_id, update.effective_chat.id)
        await check_collection_completion(context, user_id, update.effective_chat.id)
    else:
        await update.effective_message.reply_text("Error finding character.")
    
    conn.close()

async def payc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_membership(update, context): return
    if not await check_force_start(update): return
    if not update.effective_message.reply_to_message: return
    sender = update.effective_user
    receiver = update.effective_message.reply_to_message.from_user
    if sender.id == receiver.id: return
    if not context.args: return
    try:
        amount = int(context.args[0])
        if amount <= 0: return
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("SELECT crystal FROM users WHERE user_id=?", (sender.id,))
        res = c.fetchone()
        if not res or res[0] < amount:
            await update.effective_message.reply_text("❌ Not enough crystals.")
        else:
            c.execute("UPDATE users SET crystal = crystal - ? WHERE user_id=?", (amount, sender.id))
            c.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (receiver.id,))
            c.execute("UPDATE users SET crystal = crystal + ? WHERE user_id=?", (amount, receiver.id))
            conn.commit()
            await update.effective_message.reply_text(f"✅ Paid {amount} Crystals.")
        conn.close()
    except: pass

async def remove_char(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_membership(update, context): return
    if not await check_force_start(update): return
    if not context.args: return
    char_id = int(context.args[0])
    user_id = update.effective_user.id
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT count FROM harem WHERE user_id=? AND character_id=?", (user_id, char_id))
    res = c.fetchone()
    if res and res[0] > 0:
        new = res[0] - 1
        if new == 0: c.execute("DELETE FROM harem WHERE user_id=? AND character_id=?", (user_id, char_id))
        else: c.execute("UPDATE harem SET count=? WHERE user_id=? AND character_id=?", (new, user_id, char_id))
        conn.commit()
        await update.effective_message.reply_text(f"✅ Removed 1 count of ID {char_id}.")
    else:
        await update.effective_message.reply_text("❌ Not found in harem.")
    conn.close()

async def mymoney(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_membership(update, context): return
    if not await check_force_start(update): return
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT crystal, gem FROM users WHERE user_id=?", (update.effective_user.id,))
    res = c.fetchone()
    conn.close()
    crys, gems = res if res else (0, 0)
    await update.effective_message.reply_text(f"💰 Balance:\n💎 Gems: {gems}\n💸 Crystals: {crys}")

async def ctg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_membership(update, context): return
    if not await check_force_start(update): return
    if not context.args:
        await update.effective_message.reply_text("Usage: /ctg [crystal_amount]")
        return
    try:
        amount = int(context.args[0])
        if amount <= 0: return
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("SELECT crystal FROM users WHERE user_id=?", (update.effective_user.id,))
        res = c.fetchone()
        balance = res[0] if res else 0
        
        if balance < amount:
            await update.effective_message.reply_text("❌ Not enough crystals.")
        else:
            gems_to_add = amount / 100000.0
            c.execute("UPDATE users SET crystal = crystal - ?, gem = gem + ? WHERE user_id=?", (amount, gems_to_add, update.effective_user.id))
            conn.commit()
            await update.effective_message.reply_text(f"✅ Converted {amount} Crystals to {gems_to_add} Gems.")
        conn.close()
    except ValueError:
        await update.effective_message.reply_text("Invalid amount.")

async def cdaily(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_membership(update, context): return
    if not await check_force_start(update): return
    user_id = update.effective_user.id
    if is_banned(user_id): return
    
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT last_daily FROM users WHERE user_id=?", (user_id,))
    last = c.fetchone()[0] or 0
    now = time.time()
    
    if now - last < 86400:
        remaining = int(86400 - (now - last))
        hours, remainder = divmod(remaining, 3600)
        minutes, _ = divmod(remainder, 60)
        conn.close()
        await update.effective_message.reply_text(f"⏳ Daily claim available in {hours}h {minutes}m.")
        return

    # Reward: Leg(5)=22%, Evt(7)=25%, Prem(6)=30%, Hentai(8)=20%, Ani(9)=3%
    rarities = [5, 7, 6, 8, 9]
    weights = [22, 25, 30, 20, 3]
    char = get_random_char_by_rarity(rarities, weights)
    
    crystals = random.randint(50000, 150000)
    gems = 1
    
    c.execute("UPDATE users SET last_daily=?, crystal=crystal+?, gem=gem+? WHERE user_id=?", (now, crystals, gems, user_id))
    
    msg_text = f"🗓 **Daily Reward**\n💰 +{crystals} Crystals\n💎 +{gems} Gem"
    
    if char:
        c.execute("INSERT OR IGNORE INTO harem (user_id, character_id, count) VALUES (?, ?, 0)", (user_id, char[0]))
        c.execute("UPDATE harem SET count = count + 1 WHERE user_id=? AND character_id=?", (user_id, char[0]))
        emoji = get_rarity_emoji(char[2])
        msg_text += f"\n\n🎁 You received:\n{emoji} {char[1]} 🆔{char[0]}"
    
    conn.commit()
    conn.close()
    await update.effective_message.reply_text(msg_text, parse_mode=ParseMode.MARKDOWN)
    if char: 
        await check_referral_milestones(context, user_id, update.effective_chat.id)
        await check_collection_completion(context, user_id, update.effective_chat.id)

async def cweekly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_membership(update, context): return
    if not await check_force_start(update): return
    user_id = update.effective_user.id
    if is_banned(user_id): return
    
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT last_weekly FROM users WHERE user_id=?", (user_id,))
    last = c.fetchone()[0] or 0
    now = time.time()
    
    if now - last < 604800:
        remaining = int(604800 - (now - last))
        days, remainder = divmod(remaining, 86400)
        hours, _ = divmod(remainder, 3600)
        conn.close()
        await update.effective_message.reply_text(f"⏳ Weekly claim available in {days}d {hours}h.")
        return

    # Reward: Leg(5)=20%, Evt(7)=20%, Prem(6)=25%, Hentai(8)=25%, Ani(9)=10%
    rarities = [5, 7, 6, 8, 9]
    weights = [20, 20, 25, 25, 10]
    
    chars_won = []
    for _ in range(4):
        c_char = get_random_char_by_rarity(rarities, weights)
        if c_char: chars_won.append(c_char)
    
    crystals = random.randint(200000, 600000)
    gems = 5
    
    c.execute("UPDATE users SET last_weekly=?, crystal=crystal+?, gem=gem+? WHERE user_id=?", (now, crystals, gems, user_id))
    
    msg_text = f"🗓 **Weekly Reward**\n💰 +{crystals} Crystals\n💎 +{gems} Gems"
    
    if chars_won:
        msg_text += "\n\n🎁 **You received:**"
        for char in chars_won:
            c.execute("INSERT OR IGNORE INTO harem (user_id, character_id, count) VALUES (?, ?, 0)", (user_id, char[0]))
            c.execute("UPDATE harem SET count = count + 1 WHERE user_id=? AND character_id=?", (user_id, char[0]))
            emoji = get_rarity_emoji(char[2])
            msg_text += f"\n{emoji} {char[1]} 🆔{char[0]}"
    
    conn.commit()
    conn.close()
    await update.effective_message.reply_text(msg_text, parse_mode=ParseMode.MARKDOWN)
    if chars_won: 
        await check_referral_milestones(context, user_id, update.effective_chat.id)
        await check_collection_completion(context, user_id, update.effective_chat.id)

async def referral_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_membership(update, context): return
    if not await check_force_start(update): return
    bot_username = context.bot.username
    link = f"https://t.me/{bot_username}?start=ref_{update.effective_user.id}"
    await update.effective_message.reply_text(f"🔗 **Your Referral Link:**\n`{link}`\n\nShare this link to earn rewards!", parse_mode=ParseMode.MARKDOWN)

# Shop Handler
async def shop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_membership(update, context): return
    # Check game mode
    chat_id = update.effective_chat.id
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT game_mode FROM groups WHERE chat_id=?", (chat_id,))
    res = c.fetchone()
    game_mode = res[0] if res else 1
    conn.close()
    
    if not game_mode:
        await update.effective_message.reply_text("❌ Shop is unavailable because Game Mode is OFF.")
        return

    # Show Main Menu
    user_gems = get_user_gems(update.effective_user.id)
    text = f"🏪 **Welcome to the Shop!**\n\n💎 Your Gems: `{user_gems:.4f}`\n\nPlease select a section:"
    # Encode user_id for security
    uid = update.effective_user.id
    keyboard = [
        [InlineKeyboardButton("📦🎁💝 Loot Boxes", callback_data=f"shop_sec_loot_{uid}")],
        [InlineKeyboardButton("💊🧪💉 Potions", callback_data=f"shop_sec_pot_{uid}")]
    ]
    await update.effective_message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)

# Shop Callbacks
async def shop_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user
    await query.answer()
    data = query.data
    
    # Parse data to get owner_id
    try:
        parts = data.split("_")
        owner_id = int(parts[-1])
        if user.id != owner_id:
            await query.answer("❌ This is not your shop session. Open your own with /shop.", show_alert=True)
            return
    except: return # Invalid format
    
    action = "_".join(parts[:-1]) # Reconstruct action without ID
    chat_id = query.message.chat_id
    is_hentai, is_sfw = get_group_settings(chat_id)
    
    if action == "shop_main":
        user_gems = get_user_gems(user.id)
        text = f"🏪 **Welcome to the Shop!**\n\n💎 Your Gems: `{user_gems:.4f}`\n\nPlease select a section:"
        keyboard = [
            [InlineKeyboardButton("📦🎁💝 Loot Boxes", callback_data=f"shop_sec_loot_{owner_id}")],
            [InlineKeyboardButton("💊🧪💉 Potions", callback_data=f"shop_sec_pot_{owner_id}")]
        ]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
        
    elif action == "shop_sec_loot":
        user_gems = get_user_gems(user.id)
        text = f"📦 **Loot Boxes**\nTry your luck!\n💎 Your Gems: `{user_gems:.4f}`\n\n"
        
        items = [
            (1, "📦 Common Box", 1, 3), (2, "📦 Rare Box", 3, 4), (3, "📦 Legendary Box", 5, 5),
            (4, "🎁 Premium Box", 10, 6), (5, "🎁 Event Box", 10, 7), (6, "💝 Hentai Box", 15, 8),
        ]
        
        visible_items = []
        for item in items:
            iid, name, price, rarity = item
            if is_sfw:
                if iid in [1, 2, 3]: visible_items.append(item)
            elif not is_hentai:
                if iid in [1, 2, 3, 4, 5]: visible_items.append(item)
            else: visible_items.append(item)
                
        buttons = []
        row = []
        for item in visible_items:
            iid, name, price, _ = item
            text += f"{iid}. {name} ➡️ {price} 💎\n"
            row.append(InlineKeyboardButton(f"{iid} ({price}💎)", callback_data=f"shop_buy_{iid}_{owner_id}"))
            if len(row) == 3:
                buttons.append(row)
                row = []
        if row: buttons.append(row)
        
        buttons.append([InlineKeyboardButton("⬅️ Back", callback_data=f"shop_main_{owner_id}")])
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(buttons), parse_mode=ParseMode.MARKDOWN)

    elif action == "shop_sec_pot":
        user_gems = get_user_gems(user.id)
        text = f"💊 **Potion Shop**\nBoost your /try luck!\n💎 Your Gems: `{user_gems:.4f}`\n\n"
        
        # ID, Name, Price, Uses, Description
        potions = [
            (1, "💊 Double Luck", 1, 30, "2x Catch Chance (30 tries)"),
            (2, "🧪 Triple Luck", 3, 20, "3x Catch Chance (20 tries)"),
            (3, "💊 Legendary Boost", 2, 30, "+50% Legendary Chance (30 tries)"),
            (4, "🧪 Premium Boost", 6, 15, "+60% Premium Chance (15 tries)"),
            (5, "🧪 Event Boost", 6, 15, "+60% Event Chance (15 tries)"),
            (6, "💉 Hentai Boost", 10, 10, "+60% Hentai Chance (10 tries)"),
        ]
        
        visible_pots = []
        for p in potions:
            pid = p[0]
            if is_sfw:
                if pid in [1, 2, 3]: visible_pots.append(p)
            elif not is_hentai:
                if pid in [1, 2, 3, 4, 5]: visible_pots.append(p)
            else: visible_pots.append(p)
            
        buttons = []
        row = []
        for p in visible_pots:
            pid, name, price, uses, desc = p
            text += f"{pid}. {name} ➡️ {price} 💎\nℹ️ {desc}\n"
            row.append(InlineKeyboardButton(f"{pid} ({price}💎)", callback_data=f"shop_pbuy_{pid}_{owner_id}"))
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row: buttons.append(row)
        
        buttons.append([InlineKeyboardButton("⬅️ Back", callback_data=f"shop_main_{owner_id}")])
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(buttons), parse_mode=ParseMode.MARKDOWN)

    elif action.startswith("shop_buy_"): # Lootbox Buy
        item_id = int(action.split("_")[2])
        items_map = {
            1: {"price": 1, "rarity": 3, "name": "Common Box"}, 2: {"price": 3, "rarity": 4, "name": "Rare Box"},
            3: {"price": 5, "rarity": 5, "name": "Legendary Box"}, 4: {"price": 10, "rarity": 6, "name": "Premium Box"},
            5: {"price": 10, "rarity": 7, "name": "Event Box"}, 6: {"price": 15, "rarity": 8, "name": "Hentai Box"}
        }
        item = items_map.get(item_id)
        if not item: return
        
        if is_sfw and item_id > 3: return
        if not is_hentai and item_id == 6: return
            
        gems = get_user_gems(user.id)
        price = item['price']
        if gems < price:
            await query.answer("❌ Not enough Gems!", show_alert=True)
            await query.edit_message_text(
                f"❌ **Transaction Failed**\n\nYou do not have enough Gems.\n\n"
                f"💎 Required: `{price}`\n💎 You have: `{gems}`",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=f"shop_sec_loot_{owner_id}")]])
            )
            return
            
        update_user_gems(user.id, -price)
        count = random.randint(3, 5)
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("SELECT id, name, rarity, file_id, media_type FROM characters WHERE rarity=?", (item['rarity'],))
        available_chars = c.fetchall()
        rewards = []
        if available_chars:
            if len(available_chars) < count: rewards = available_chars
            else: rewards = random.sample(available_chars, count)
            for char in rewards:
                cid = char[0]
                c.execute("INSERT OR IGNORE INTO harem (user_id, character_id, count) VALUES (?, ?, 0)", (user.id, cid))
                c.execute("UPDATE harem SET count = count + 1 WHERE user_id=? AND character_id=?", (user.id, cid))
            conn.commit()
        conn.close()
        
        keyboard = [[InlineKeyboardButton("⬅️ Back to Loot Boxes", callback_data=f"shop_sec_loot_{owner_id}")]]
        await query.edit_message_text(f"✅ **Thanks for shopping!**\nYou bought {item['name']} for {price} 💎.\nReceived {len(rewards)} characters.", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
        await send_reward_message(context, chat_id, user.id, rewards, source="shop")
        await check_referral_milestones(context, user.id, chat_id)
        await check_collection_completion(context, user.id, chat_id)

    elif action.startswith("shop_pbuy_"): # Potion Buy
        pid = int(action.split("_")[2])
        pot_map = {
            1: {"price": 1, "uses": 30, "name": "Double Luck"}, 2: {"price": 3, "uses": 20, "name": "Triple Luck"},
            3: {"price": 2, "uses": 30, "name": "Legendary Boost"}, 4: {"price": 6, "uses": 15, "name": "Premium Boost"},
            5: {"price": 6, "uses": 15, "name": "Event Boost"}, 6: {"price": 10, "uses": 10, "name": "Hentai Boost"}
        }
        pot = pot_map.get(pid)
        if not pot: return
        
        gems = get_user_gems(user.id)
        if gems < pot['price']:
            await query.answer("❌ Not enough Gems!", show_alert=True)
            await query.edit_message_text(
                f"❌ **Transaction Failed**\n\nYou do not have enough Gems.\n\n"
                f"💎 Required: `{pot['price']}`\n💎 You have: `{gems}`",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=f"shop_sec_pot_{owner_id}")]])
            )
            return
            
        update_user_gems(user.id, -pot['price'])
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO user_potions (user_id, potion_id, uses_left) VALUES (?, ?, 0)", (user.id, pid))
        c.execute("UPDATE user_potions SET uses_left = uses_left + ? WHERE user_id=? AND potion_id=?", (pot['uses'], user.id, pid))
        conn.commit()
        conn.close()
        
        keyboard = [[InlineKeyboardButton("⬅️ Back to Potions", callback_data=f"shop_sec_pot_{owner_id}")]]
        await query.edit_message_text(f"✅ Bought {pot['name']}!\nAdded {pot['uses']} uses.", reply_markup=InlineKeyboardMarkup(keyboard))

async def reward_pagination(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    msg_id = query.message.message_id
    
    if msg_id not in reward_cache:
        await query.answer("Expired.", show_alert=True)
        return
        
    data_cache = reward_cache[msg_id]
    if query.from_user.id != data_cache['user_id']:
        await query.answer("Not for you.", show_alert=True)
        return
        
    page = int(query.data.split("_")[2])
    await send_reward_message(context, data_cache['chat_id'], data_cache['user_id'], data_cache['chars'], data_cache['source'], page, msg_id)

# --- OWNER COLLECTION COMMANDS ---

async def addcol_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID: return
    if not context.args or len(context.args) < 2:
        await update.effective_message.reply_text("Usage: `/addcol {id} {id} ...` (at least 2 IDs)")
        return
    
    try:
        char_ids = [int(x) for x in context.args]
    except:
        await update.effective_message.reply_text("Invalid IDs.")
        return
        
    addcol_cache[update.effective_user.id] = char_ids
    await update.effective_message.reply_text("Okay, now reply with the Reward Character ID (single ID).")

async def editcol_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID: return
    if not context.args:
        await update.effective_message.reply_text("Usage: `/editcol {collection_id}`")
        return
    try:
        col_id = int(context.args[0])
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("SELECT reward_id FROM collections WHERE id=?", (col_id,))
        res = c.fetchone()
        if not res:
            await update.effective_message.reply_text("Collection not found.")
            conn.close()
            return
        
        reward_id = res[0]
        c.execute("SELECT char_id FROM collection_items WHERE collection_id=?", (col_id,))
        char_ids = [r[0] for r in c.fetchall()]
        conn.close()
        
        text = f"**Collection #{col_id}**\n\nReward: 🆔{reward_id}\nCharacters: {', '.join([f'🆔{i}' for i in char_ids])}"
        
        keyboard = [
            [InlineKeyboardButton("Edit Reward", callback_data=f"editcol_reward_{col_id}")],
            [InlineKeyboardButton("Edit Characters", callback_data=f"editcol_chars_{col_id}")],
            [InlineKeyboardButton("Cancel", callback_data="edit_cancel")]
        ]
        await update.effective_message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
    except: pass

async def delcol_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID: return
    if not context.args:
        await update.effective_message.reply_text("Usage: `/delcol {collection_id}`")
        return
    try:
        col_id = int(context.args[0])
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        
        # Check if exists
        c.execute("SELECT reward_id FROM collections WHERE id=?", (col_id,))
        res = c.fetchone()
        if not res:
            await update.effective_message.reply_text("Collection not found.")
            conn.close()
            return
            
        reward_id = res[0]
        c.execute("SELECT char_id FROM collection_items WHERE collection_id=?", (col_id,))
        char_ids = [r[0] for r in c.fetchall()]
        conn.close()
        
        text = f"⚠️ **ARE YOU SURE YOU WANT TO DELETE THIS?**\n\n**Collection #{col_id}**\nReward: 🆔{reward_id}\nCharacters: {', '.join([f'🆔{i}' for i in char_ids])}"
        
        keyboard = [
            [InlineKeyboardButton("✅ Confirm Delete", callback_data=f"delcol_confirm_{col_id}")],
            [InlineKeyboardButton("❌ Cancel", callback_data="del_cancel")]
        ]
        await update.effective_message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
    except: pass

async def delcol_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user
    if user.id != OWNER_ID: return
    await query.answer()
    
    col_id = int(query.data.split("_")[2])
    
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    # Get chars to update captions
    c.execute("SELECT char_id FROM collection_items WHERE collection_id=?", (col_id,))
    char_ids = [r[0] for r in c.fetchall()]
    
    c.execute("DELETE FROM collections WHERE id=?", (col_id,))
    c.execute("DELETE FROM collection_items WHERE collection_id=?", (col_id,))
    # Optionally delete completion records
    c.execute("DELETE FROM collection_completions WHERE collection_id=?", (col_id,))
    
    conn.commit()
    conn.close()
    
    for cid in char_ids:
        await update_channels(context, cid, user.first_name, "Edited")
        
    await query.edit_message_text(f"✅ Collection {col_id} deleted.")

async def col_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_membership(update, context): return
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT id, reward_id FROM collections")
    cols = c.fetchall()
    
    if not cols:
        await update.effective_message.reply_text("No active collections.")
        conn.close()
        return
        
    text = "📚 **Collections List**\n\n"
    for col in cols:
        cid, rid = col
        c.execute("SELECT char_id FROM collection_items WHERE collection_id=?", (cid,))
        items = [f"🆔{r[0]}" for r in c.fetchall()]
        text += f"**{cid}-**\nReward: 🆔{rid}\nCharacters: {', '.join(items)}\n\n"
        
    conn.close()
    await update.effective_message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

# --- LAB LOGIC ---

def get_lab_materials(user_id, required_rarities, count):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    placeholders = ','.join('?' for _ in required_rarities)
    
    # Get user owned characters of specific rarity along with their count
    query = f"""
        SELECT h.character_id, h.count 
        FROM harem h 
        JOIN characters c ON h.character_id = c.id 
        WHERE h.user_id = ? AND c.rarity IN ({placeholders})
    """
    c.execute(query, (user_id, *required_rarities))
    owned = c.fetchall()
    conn.close()
    
    # Expand to a list of available IDs (if count > 1, add ID multiple times)
    available_ids = []
    for char_id, cnt in owned:
        available_ids.extend([char_id] * cnt)
        
    if len(available_ids) < count:
        return None
        
    return random.sample(available_ids, count)

async def execute_lab(context, chat_id, user_id, cost_ids, reward_char, is_hentai=False, is_giveaway=False):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    # Remove Cost Chars
    used_chars_text = ""
    # Group by ID for efficient DB update and reporting
    from collections import Counter
    cost_counts = Counter(cost_ids)
    
    # Fetch details for report
    ids_placeholder = ','.join('?' for _ in cost_counts.keys())
    c.execute(f"SELECT id, name, rarity FROM characters WHERE id IN ({ids_placeholder})", tuple(cost_counts.keys()))
    char_details = {row[0]: (row[1], row[2]) for row in c.fetchall()}
    
    for char_id, qty in cost_counts.items():
        c.execute("SELECT count FROM harem WHERE user_id=? AND character_id=?", (user_id, char_id))
        res = c.fetchone()
        current = res[0] if res else 0
        new_count = max(0, current - qty)
        
        if new_count == 0:
            c.execute("DELETE FROM harem WHERE user_id=? AND character_id=?", (user_id, char_id))
        else:
            c.execute("UPDATE harem SET count=? WHERE user_id=? AND character_id=?", (new_count, user_id, char_id))
            
        details = char_details.get(char_id)
        if details:
            name, rarity = details
            used_chars_text += f"{get_rarity_emoji(rarity)} {name} 🆔 {char_id} (x{qty})\n"

    # Add Reward
    # reward_char is tuple (id, name, rarity, file_id, media_type)
    # OR list of tuples if multiple rewards
    rewards = reward_char if isinstance(reward_char, list) else [reward_char]
    
    reward_text_top = ""
    last_msg = None
    
    for r_char in rewards:
        rid, rname, rrarity, rfile, rmedia = r_char
        c.execute("INSERT OR IGNORE INTO harem (user_id, character_id, count) VALUES (?, ?, 0)", (user_id, rid))
        c.execute("UPDATE harem SET count = count + 1 WHERE user_id=? AND character_id=?", (user_id, rid))
        reward_text_top += f"{get_rarity_emoji(rrarity)} {rname} 🆔 {rid}\n"
    
    conn.commit()
    conn.close()
    
    final_caption = f"✅ **Experiment Successful!**\n\nYou received:\n{reward_text_top}\n🧪 **Characters Used:**\n{used_chars_text}"
    
    # Send Image of the LAST reward char (simplified for single reward focus, or loop if multiple)
    # Usually lab gives 1, exp 6 gives 2. Let's send media for the first one or a summary.
    # Requirement: "send image and detail"
    
    rid, rname, rrarity, rfile, rmedia = rewards[0]
    
    try:
        if rmedia == 1:
            last_msg = await context.bot.send_video(chat_id, rfile, caption=final_caption, parse_mode=ParseMode.MARKDOWN)
        else:
            last_msg = await context.bot.send_photo(chat_id, rfile, caption=final_caption, parse_mode=ParseMode.MARKDOWN)
            
        # Timers
        # Premium(6)/Event(7) -> 60s
        # Hentai(8)/HG(1) -> 30s
        delay = 0
        if rrarity in [6, 7]: delay = 60
        elif rrarity in [1, 8]: delay = 30
        
        if delay > 0:
            asyncio.create_task(delete_image_msg(context, chat_id, last_msg.message_id, delay))
            
    except Exception as e:
        await context.bot.send_message(chat_id, final_caption, parse_mode=ParseMode.MARKDOWN)

    await check_collection_completion(context, user_id, chat_id)

async def lab_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_membership(update, context): return
    if not await check_force_start(update): return
    user = update.effective_user
    if is_banned(user.id): return
    
    chat_id = update.effective_chat.id
    
    # Check Game Mode
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT game_mode, hentai_mode, sfw_mode FROM groups WHERE chat_id=?", (chat_id,))
    res = c.fetchone()
    conn.close()
    
    game_mode = res[0] if res else 1
    is_hentai = res[1] if res else 0
    is_sfw = res[2] if res else 0
    
    if not game_mode:
        await update.effective_message.reply_text("❌ Laboratory is closed (Game Mode OFF).")
        return

    # Help Text Construction
    help_text = "🧪 **Laboratory Experiments**\nExchange characters for higher rarities!\nUsage: `/lab {number}`\n\n"
    
    exps = []
    # 1 & 2 Always shown if not pure SFW restriction (actually logic: sfw shows 1&2, hentai off shows 1-5, hentai on 1-8)
    # The prompt says: "ingroups that have sfw mode on only experiment 1 and 2 are shown"
    
    exps.append("1️⃣ **Common -> Rare**\nCost: 3x Common | Reward: 1x Rare")
    exps.append("2️⃣ **Rare -> Legendary**\nCost: 3x Rare | Reward: 1x Legendary")
    
    if not is_sfw:
        exps.append("3️⃣ **Legendary -> Premium**\nCost: 5x Legendary | Reward: 1x Premium")
        exps.append("4️⃣ **Premium -> Event**\nCost: 2x Premium | Reward: 1x Event")
        exps.append("5️⃣ **Event -> Premium**\nCost: 1x Event | Reward: 1x Premium")
        
        if is_hentai:
            exps.append("6️⃣ **Random Hentai**\nCost: 10x (Event/Prem) | Reward: 2x Hentai")
            exps.append("7️⃣ **Random Giveaway**\nCost: 20x Hentai | Reward: Choice (HG/Giveaway)")
            exps.append("8️⃣ **Chosen Giveaway**\nCost: 40x Hentai | Reward: Specific ID (HG/Giveaway)")

    if not context.args or context.args[0] == '0':
        await update.effective_message.reply_text(help_text + "\n\n".join(exps), parse_mode=ParseMode.MARKDOWN)
        return

    try:
        exp_num = int(context.args[0])
    except:
        await update.effective_message.reply_text("Invalid number.")
        return

    # Validate allowed experiment
    if is_sfw and exp_num > 2:
        await update.effective_message.reply_text("❌ Experiment not available in SFW mode.")
        return
    if not is_sfw and not is_hentai and exp_num > 5:
        await update.effective_message.reply_text("❌ Enable Hentai Mode for this experiment.")
        return
    if exp_num < 1 or exp_num > 8:
        await update.effective_message.reply_text("❌ Invalid experiment number.")
        return

    # Check Materials
    req_rarities = []
    req_count = 0
    desc = ""
    
    if exp_num == 1:
        req_rarities = [3]; req_count = 3
        desc = "3 random Common characters will be removed, 1 Rare added."
    elif exp_num == 2:
        req_rarities = [4]; req_count = 3
        desc = "3 random Rare characters will be removed, 1 Legendary added."
    elif exp_num == 3:
        req_rarities = [5]; req_count = 5
        desc = "5 random Legendary characters will be removed, 1 Premium added."
    elif exp_num == 4:
        req_rarities = [6]; req_count = 2
        desc = "2 random Premium characters will be removed, 1 Event added."
    elif exp_num == 5:
        req_rarities = [7]; req_count = 1
        desc = "1 random Event character will be removed, 1 Premium added."
    elif exp_num == 6:
        req_rarities = [6, 7]; req_count = 10
        desc = "10 random Event/Premium characters will be removed, 2 Hentai added."
    elif exp_num == 7:
        req_rarities = [8]; req_count = 20
        desc = "20 random Hentai characters will be removed. You choose between Giveaway/HG."
    elif exp_num == 8:
        req_rarities = [8]; req_count = 40
        desc = "40 random Hentai characters will be removed. You choose a specific ID (Giveaway/HG)."

    materials = get_lab_materials(user.id, req_rarities, req_count)
    if not materials:
        await update.effective_message.reply_text(f"❌ You don't have enough characters for this experiment!\nRequired: {req_count}x Rarity {req_rarities}")
        return

    # Confirm
    keyboard = [
        [InlineKeyboardButton("✅ Yes", callback_data="lab_confirm"), InlineKeyboardButton("❌ Cancel", callback_data="lab_cancel")]
    ]
    msg = await update.effective_message.reply_text(f"🧪 **Experiment #{exp_num}**\n{desc}\n\nAre you sure you want to continue?", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
    
    lab_cache[user.id] = {
        'exp': exp_num,
        'msg_id': msg.message_id,
        'chat_id': chat_id,
        'req_rarity': req_rarities,
        'req_count': req_count
    }

async def lab_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user
    await query.answer()
    
    if user.id not in lab_cache:
        await query.edit_message_text("❌ Session expired.")
        return
        
    session = lab_cache[user.id]
    if query.data == "lab_cancel":
        del lab_cache[user.id]
        await query.edit_message_text("❌ Experiment cancelled.")
        return

    exp_num = session['exp']
    chat_id = session['chat_id']
    
    # Re-verify materials just in case
    materials = get_lab_materials(user.id, session['req_rarity'], session['req_count'])
    if not materials:
        del lab_cache[user.id]
        await query.edit_message_text("❌ Material check failed. Items missing.")
        return

    if query.data == "lab_confirm":
        if exp_num <= 6:
            # Execute Immediate
            target_rarity = 0
            count_add = 1
            if exp_num == 1: target_rarity = 4
            elif exp_num == 2: target_rarity = 5
            elif exp_num == 3: target_rarity = 6
            elif exp_num == 4: target_rarity = 7
            elif exp_num == 5: target_rarity = 6
            elif exp_num == 6: 
                target_rarity = 8
                count_add = 2
            
            rewards = []
            for _ in range(count_add):
                r = get_random_char_by_rarity([target_rarity])
                if r: rewards.append(r)
            
            if not rewards:
                await query.edit_message_text("❌ Error generating reward.")
                return
                
            await query.delete_message()
            await execute_lab(context, chat_id, user.id, materials, rewards)
            del lab_cache[user.id]
            
        elif exp_num == 7:
            # Ask for choice
            keyboard = [
                [InlineKeyboardButton("🎟 Giveaway", callback_data="lab_c7_2"), InlineKeyboardButton("🎫 Hentai Giveaway", callback_data="lab_c7_1")],
                [InlineKeyboardButton("❌ Cancel", callback_data="lab_cancel")]
            ]
            await query.edit_message_text("Select reward rarity:", reply_markup=InlineKeyboardMarkup(keyboard))
            # Don't delete cache yet
            
        elif exp_num == 8:
            session['state'] = "waiting_id"
            await query.edit_message_text("🔢 Please send the ID of the Giveaway/HG character you want.")
            # Don't delete cache
            
    elif query.data.startswith("lab_c7_"):
        target_rarity = int(query.data.split("_")[2])
        reward = get_random_char_by_rarity([target_rarity])
        if not reward:
            await query.edit_message_text("❌ Error generating reward.")
            return
            
        await query.delete_message()
        await execute_lab(context, chat_id, user.id, materials, [reward])
        del lab_cache[user.id]

async def owner_cmds(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID: return
    cmd = update.effective_message.text.split()[0][1:]
    args = context.args
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    if cmd == "banplayer" and args:
        c.execute("INSERT OR IGNORE INTO banned_users (user_id) VALUES (?)", (int(args[0]),))
        await update.effective_message.reply_text("Banned.")
    elif cmd == "unbanplayer" and args:
        c.execute("DELETE FROM banned_users WHERE user_id=?", (int(args[0]),))
        await update.effective_message.reply_text("Unbanned.")
    elif cmd == "remc" and len(args) >= 2:
        c.execute("DELETE FROM harem WHERE user_id=? AND character_id=?", (int(args[1]), int(args[0])))
        await update.effective_message.reply_text("Removed.")
    elif cmd == "delharem" and args:
        c.execute("DELETE FROM harem WHERE user_id=?", (int(args[0]),))
        await update.effective_message.reply_text("Harem deleted.")
    conn.commit()
    conn.close()

if __name__ == '__main__':
    init_db()
    app = ApplicationBuilder().token(TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    
    app.add_handler(CommandHandler("upload", upload))
    app.add_handler(CommandHandler("edit", edit_command))
    
    app.add_handler(CommandHandler("card", card_command))
    
    app.add_handler(CommandHandler("steal", steal_character))
    app.add_handler(CommandHandler("try", try_command))
    app.add_handler(CommandHandler("coin", coin_command))
    app.add_handler(CommandHandler("dice", dice_command))
    
    app.add_handler(CommandHandler("myharem", myharem))
    app.add_handler(CommandHandler("list", list_command))
    app.add_handler(CommandHandler("uncollected", uncollected_command))
    app.add_handler(CommandHandler("see", see))
    app.add_handler(CommandHandler("cfind", cfind))
    app.add_handler(CommandHandler("fav", fav_command))
    app.add_handler(CommandHandler("type", type_command))
    
    app.add_handler(CommandHandler("giftc", giftc_cmd))
    app.add_handler(CommandHandler("buy", buy_cmd))
    app.add_handler(CommandHandler("sell", sell_single_cmd))
    app.add_handler(CommandHandler("payc", payc))
    app.add_handler(CommandHandler("transferharem", transfer_harem_cmd))
    app.add_handler(CommandHandler("remove", remove_char))
    app.add_handler(CommandHandler("mymoney", mymoney))
    app.add_handler(CommandHandler("ctg", ctg))
    app.add_handler(CommandHandler("cdaily", cdaily))
    app.add_handler(CommandHandler("cweekly", cweekly))
    app.add_handler(CommandHandler("referral", referral_command))
    app.add_handler(CommandHandler("shop", shop_command))
    
    app.add_handler(CommandHandler("topg", topg))
    app.add_handler(CommandHandler("topc", topc))
    app.add_handler(CommandHandler("event", event_command))
    app.add_handler(CommandHandler("col", col_cmd))
    
    app.add_handler(CommandHandler("claim", claim_cmd))
    
    app.add_handler(CommandHandler("del", delete_character))
    app.add_handler(CommandHandler("addsudo", addsudo))
    app.add_handler(CommandHandler("remsudo", remsudo))
    app.add_handler(CommandHandler("adduploader", adduploader))
    app.add_handler(CommandHandler("remuploader", remuploader))
    app.add_handler(CommandHandler("uploadlist", uploadlist))
    app.add_handler(CommandHandler("sudolist", sudolist))
    app.add_handler(CommandHandler("donate", donate))
    app.add_handler(CommandHandler("donatec", donatec))
    app.add_handler(CommandHandler("removec", removec))
    app.add_handler(CommandHandler("editevent", editevent))
    app.add_handler(CommandHandler("broadcast", broadcast))
    
    app.add_handler(CommandHandler("hentai", hentai_ok))
    app.add_handler(CommandHandler("sfw", sfw_command))
    app.add_handler(CommandHandler("time", set_time))
    app.add_handler(CommandHandler("game", game_toggle))
    
    app.add_handler(CommandHandler("giveaway", giveaway_cmd))
    app.add_handler(CommandHandler("cgiveaway", cgiveaway_cmd))
    app.add_handler(CommandHandler("pinv", pinv_command))
    
    app.add_handler(CommandHandler("addcol", addcol_cmd))
    app.add_handler(CommandHandler("editcol", editcol_cmd))
    app.add_handler(CommandHandler("delcol", delcol_cmd))
    app.add_handler(CommandHandler("lab", lab_command))
    
    app.add_handler(CommandHandler(["banplayer", "unbanplayer", "remc", "delharem"], owner_cmds))

    # --- HANDLERS FOR INLINE, MODERATION & WELCOME ---
    app.add_handler(InlineQueryHandler(inline_query))
    app.add_handler(ChatMemberHandler(new_member, ChatMemberHandler.MY_CHAT_MEMBER))
    app.add_handler(ChatMemberHandler(new_member, ChatMemberHandler.CHAT_MEMBER))
    
    # --- CUSTOM FILTER FIX ---
    bot_id = int(TOKEN.split(":")[0])
    
    class ViaBotIDFilter(filters.MessageFilter):
        def __init__(self, bot_id):
            super().__init__()
            self.bot_id = bot_id

        def filter(self, message):
            return bool(message.via_bot and message.via_bot.id == self.bot_id)
    
    my_via_bot_filter = ViaBotIDFilter(bot_id)
    
    app.add_handler(MessageHandler(my_via_bot_filter, check_inline_message))

    app.add_handler(CallbackQueryHandler(rarity_handler, pattern="^rarity_"))
    app.add_handler(CallbackQueryHandler(edit_callback, pattern="^edit_|^setrarity_|^editcol_"))
    app.add_handler(CallbackQueryHandler(filter_callback, pattern="^filter_"))
    app.add_handler(CallbackQueryHandler(harem_callback, pattern="^harem_|^list_|^uncol_"))
    app.add_handler(CallbackQueryHandler(buy_callback, pattern="^buy_"))
    app.add_handler(CallbackQueryHandler(sell_single_callback, pattern="^sellchar_"))
    app.add_handler(CallbackQueryHandler(sell_callback, pattern="^sell_"))
    app.add_handler(CallbackQueryHandler(cfind_callback, pattern="^cfind_"))
    app.add_handler(CallbackQueryHandler(gift_callback, pattern="^gift_"))
    app.add_handler(CallbackQueryHandler(help_callback, pattern="^help_"))
    app.add_handler(CallbackQueryHandler(shop_callback, pattern="^shop_"))
    app.add_handler(CallbackQueryHandler(reward_pagination, pattern="^rew_pg_"))
    app.add_handler(CallbackQueryHandler(delete_character_callback, pattern="^del_confirm_|^del_cancel"))
    app.add_handler(CallbackQueryHandler(delcol_callback, pattern="^delcol_confirm_"))
    app.add_handler(CallbackQueryHandler(lab_callback, pattern="^lab_"))
    
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), message_handler))
    app.add_handler(MessageHandler(filters.PHOTO | filters.VIDEO | filters.ANIMATION, message_handler))
    
    print("Bot Running...")
    app.run_polling()