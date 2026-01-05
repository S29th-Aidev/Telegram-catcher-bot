import logging
import sqlite3
import random
import asyncio
import re
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
)

# --- CONFIGURATION ---
TOKEN = 'YOUR_BOT_TOKEN'
OWNER_ID = YOUR_OWN_ID
DATABASE_CHANNEL_ID = -100123456789  # Main Channel (All characters)[I recommend using private channel specialy if you are uploading hentai art]
DATABASE_CHANNEL_ID2 = "@YOUR_PUBLIC_DATABASE"  # Secondary Channel (SFW only)[The arts from hentai rarity wont send here so it's ok for this one to be public]

# --- DATABASE SETUP ---
DB_NAME = "anime_catcher.db"

def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS sudos (user_id INTEGER PRIMARY KEY)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS characters (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        surname TEXT,
        extra_name TEXT,
        rarity INTEGER,
        file_id TEXT,
        log_msg_id1 INTEGER,
        log_msg_id2 INTEGER
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
        hentai_mode BOOLEAN DEFAULT 0
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        fav_character_id INTEGER DEFAULT 0,
        rarity_filter INTEGER DEFAULT 0
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
    )''')

    # Migrations
    try: c.execute("ALTER TABLE characters ADD COLUMN log_msg_id1 INTEGER")
    except: pass
    try: c.execute("ALTER TABLE characters ADD COLUMN log_msg_id2 INTEGER")
    except: pass

    try:
        c.execute("INSERT OR IGNORE INTO sudos (user_id) VALUES (?)", (OWNER_ID,))
        c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", ("event_text", "❄️ Winter\n👤 cosplay"))
    except:
        pass
    
    conn.commit()
    conn.close()

# --- GLOBAL STATE ---
current_spawns = {}
upload_cache = {}
edit_cache = {} 
# edit_cache structure: 
# {user_id: {'id': char_id, 'data': row, 'mode': 'name'/'photo'/..., 'admin_name': name}}

# --- RARITY MAP ---
RARITY_MAP = {
    1: "⚫️ Common",
    2: "🟠 Rare",
    3: "🟡 Legendary",
    4: "🫧 Premium",
    5: "🔮 Event",
    6: "🔞 Hentai"
}

def get_rarity_text(rarity_level):
    return RARITY_MAP.get(rarity_level, "Unknown")

def get_rarity_emoji(rarity_level):
    text = RARITY_MAP.get(rarity_level, "⚪️")
    return text.split()[0]

def is_sudo(user_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT user_id FROM sudos WHERE user_id=?", (user_id,))
    result = c.fetchone()
    conn.close()
    return result is not None or user_id == OWNER_ID

# --- HELPER: CHANNEL UPDATER ---
async def update_channels(context, char_id, admin_name, action_type="Edited"):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT name, surname, rarity, file_id, log_msg_id1, log_msg_id2 FROM characters WHERE id=?", (char_id,))
    data = c.fetchone()
    
    if not data:
        conn.close()
        return

    name, surname, rarity, file_id, msg1, msg2 = data
    caption = f"🆔 ID: {char_id}\n👤 Name: {name} {surname}\n{get_rarity_text(rarity)}\n\n✍️ {action_type} by: {admin_name}"
    
    # 1. Update Channel 1 (Always exists)
    if msg1:
        try:
            await context.bot.edit_message_media(
                chat_id=DATABASE_CHANNEL_ID,
                message_id=msg1,
                media=InputMediaPhoto(media=file_id, caption=caption)
            )
        except Exception as e:
            try:
                await context.bot.edit_message_caption(chat_id=DATABASE_CHANNEL_ID, message_id=msg1, caption=caption)
            except: pass
    
    # 2. Update Channel 2 (Logic based on Rarity)
    is_sfw = (1 <= rarity <= 5)
    
    if is_sfw:
        if msg2:
            try:
                await context.bot.edit_message_media(
                    chat_id=DATABASE_CHANNEL_ID2,
                    message_id=msg2,
                    media=InputMediaPhoto(media=file_id, caption=caption)
                )
            except:
                try:
                    await context.bot.edit_message_caption(chat_id=DATABASE_CHANNEL_ID2, message_id=msg2, caption=caption)
                except: pass
        else:
            try:
                m = await context.bot.send_photo(chat_id=DATABASE_CHANNEL_ID2, photo=file_id, caption=caption)
                c.execute("UPDATE characters SET log_msg_id2=? WHERE id=?", (m.message_id, char_id))
            except Exception as e:
                print(f"Error sending to Ch2: {e}")
    else:
        if msg2:
            try:
                await context.bot.delete_message(chat_id=DATABASE_CHANNEL_ID2, message_id=msg2)
                c.execute("UPDATE characters SET log_msg_id2=NULL WHERE id=?", (char_id,))
            except: pass
            
    conn.commit()
    conn.close()

# --- ASYNC HELPERS ---
async def delete_after_delay(context, chat_id, message_id, delay):
    await asyncio.sleep(delay)
    try:
        await context.bot.delete_message(chat_id, message_id)
        if chat_id in current_spawns and current_spawns[chat_id]['message_id'] == message_id:
            await context.bot.send_message(chat_id, "⏱ The hentai character fled away!")
            del current_spawns[chat_id]
    except: pass

# --- COMMANDS ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg: return
    chat_id = update.effective_chat.id
    
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO groups (chat_id, last_spawn_id) VALUES (?, ?)", (chat_id, msg.message_id))
    c.execute("UPDATE groups SET last_spawn_id = ? WHERE chat_id=?", (msg.message_id, chat_id))
    conn.commit()
    conn.close()
    
    await update.effective_message.reply_text("👋 Hello! I am the Ravan Anime Catcher Bot.\nMessage counting started from here!")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "📚 **Commands List:**\n"
        "/steal [name] - Catch a spawned character\n"
        "/myharem - View your collection\n"
        "/see [id] - View a specific character\n"
        "/gift [id] - Gift a character\n"
        "/fav [id] - Set favorite\n"
        "/type - Filter harem\n"
        "/event - Check events\n\n"
        "**Admins:**\n"
        "/time [number] - Change spawn threshold\n"
        "/hentai - Toggle NSFW\n\n"
        "**Sudo:**\n"
        "/upload - Add character\n"
        "/edit [id] - Edit character\n"
        "/del [id] - Delete character\n"
        "/donate [id] - Give character\n"
        "/addsudo - Add admin\n"
        "/remsudo - Remove admin\n"
        "/editevent - Edit event text"
    )
    await update.effective_message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

# --- UPLOAD ---
async def upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_sudo(user.id): return

    if not update.effective_message.reply_to_message or not update.effective_message.reply_to_message.photo:
        await update.effective_message.reply_text("⚠️ Reply to an image with: `/upload Name Surname`", parse_mode=ParseMode.MARKDOWN)
        return

    args = context.args
    if not args:
        await update.effective_message.reply_text("⚠️ Name required. Example: `/upload Zero Two`", parse_mode=ParseMode.MARKDOWN)
        return

    file_id = update.effective_message.reply_to_message.photo[-1].file_id
    
    upload_cache[user.id] = {
        'file_id': file_id,
        'name': args[0],
        'surname': args[1] if len(args) > 1 else "",
        'extra_name': " ".join(args[2:]) if len(args) > 2 else ""
    }

    keyboard = [
        [InlineKeyboardButton("⚫️ Common (1)", callback_data="rarity_1"), InlineKeyboardButton("🟠 Rare (2)", callback_data="rarity_2")],
        [InlineKeyboardButton("🟡 Legendary (3)", callback_data="rarity_3"), InlineKeyboardButton("🫧 Premium (4)", callback_data="rarity_4")],
        [InlineKeyboardButton("🔮 Event (5)", callback_data="rarity_5"), InlineKeyboardButton("🔞 Hentai (6)", callback_data="rarity_6")],
        [InlineKeyboardButton("❌ Cancel", callback_data="rarity_cancel")]
    ]
    await update.effective_message.reply_text(f"Name: {args[0]}\nSelect Rarity:", reply_markup=InlineKeyboardMarkup(keyboard))

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
    c.execute("INSERT INTO characters (name, surname, extra_name, rarity, file_id) VALUES (?, ?, ?, ?, ?)",
              (data['name'], data['surname'], data['extra_name'], rarity, data['file_id']))
    char_id = c.lastrowid
    conn.commit()
    conn.close()

    caption = f"🆔 ID: {char_id}\n👤 Name: {data['name']} {data['surname']}\n{get_rarity_text(rarity)}\n\n✍️ Added by: {user.first_name}"
    
    msg1_id = None
    msg2_id = None
    
    try:
        m1 = await context.bot.send_photo(chat_id=DATABASE_CHANNEL_ID, photo=data['file_id'], caption=caption)
        msg1_id = m1.message_id
    except Exception as e:
        print(f"Error Ch1: {e}")

    if 1 <= rarity <= 5:
        try:
            m2 = await context.bot.send_photo(chat_id=DATABASE_CHANNEL_ID2, photo=data['file_id'], caption=caption)
            msg2_id = m2.message_id
        except Exception as e:
            print(f"Error Ch2: {e}")

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("UPDATE characters SET log_msg_id1=?, log_msg_id2=? WHERE id=?", (msg1_id, msg2_id, char_id))
    conn.commit()
    conn.close()

    del upload_cache[user.id]
    await query.edit_message_text(f"✅ Saved! ID: {char_id}")

# --- EDIT LOGIC ---
async def edit_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_sudo(user.id): return
    
    if not context.args:
        await update.effective_message.reply_text("Usage: /edit [id]")
        return
        
    try:
        char_id = int(context.args[0])
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("SELECT name, surname, rarity, file_id FROM characters WHERE id=?", (char_id,))
        row = c.fetchone()
        conn.close()
        
        if not row:
            await update.effective_message.reply_text("Character not found.")
            return

        edit_cache[user.id] = {'id': char_id, 'data': row, 'admin_name': user.first_name}
        
        text = f"Editing ID: {char_id}\nName: {row[0]}\nSurname: {row[1]}\nRarity: {RARITY_MAP.get(row[2])}"
        keyboard = [
            [InlineKeyboardButton("Name", callback_data="edit_name"), InlineKeyboardButton("Surname", callback_data="edit_surname")],
            [InlineKeyboardButton("Rarity", callback_data="edit_rarity"), InlineKeyboardButton("Photo", callback_data="edit_photo")],
            [InlineKeyboardButton("❌ Cancel", callback_data="edit_cancel")]
        ]
        
        await update.effective_message.reply_photo(photo=row[3], caption=text, reply_markup=InlineKeyboardMarkup(keyboard))
        
    except ValueError:
        await update.effective_message.reply_text("ID must be a number.")

async def edit_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user
    await query.answer()
    
    if user.id not in edit_cache:
        await query.edit_message_caption("Session expired.")
        return
        
    data = query.data
    cache = edit_cache[user.id]
    char_id = cache['id']
    admin_name = cache['admin_name']
    
    if data == "edit_cancel":
        del edit_cache[user.id]
        await query.edit_message_caption("❌ Edit cancelled.")
        return
        
    if data == "edit_rarity":
        keyboard = [
            [InlineKeyboardButton("⚫️ 1", callback_data="setrarity_1"), InlineKeyboardButton("🟠 2", callback_data="setrarity_2")],
            [InlineKeyboardButton("🟡 3", callback_data="setrarity_3"), InlineKeyboardButton("🫧 4", callback_data="setrarity_4")],
            [InlineKeyboardButton("🔮 5", callback_data="setrarity_5"), InlineKeyboardButton("🔞 6", callback_data="setrarity_6")]
        ]
        await query.edit_message_caption("Select new rarity:", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data.startswith("setrarity_"):
        new_rarity = int(data.split("_")[1])
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("UPDATE characters SET rarity=? WHERE id=?", (new_rarity, char_id))
        conn.commit()
        conn.close()
        
        await update_channels(context, char_id, admin_name)
        
        del edit_cache[user.id]
        await query.edit_message_caption(f"✅ Rarity updated to {get_rarity_text(new_rarity)}")
        return

    mode = data.split("_")[1]
    edit_cache[user.id]['mode'] = mode
    await query.edit_message_caption(f"Send the new {mode}:")

# --- DELETE LOGIC ---
async def delete_character(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_sudo(user.id): return

    if not context.args:
        await update.effective_message.reply_text("Usage: /del [id]")
        return

    try:
        char_id = int(context.args[0])
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("SELECT name, surname, rarity, file_id, log_msg_id1, log_msg_id2 FROM characters WHERE id=?", (char_id,))
        row = c.fetchone()
        
        if not row:
            await update.effective_message.reply_text("Character not found.")
            conn.close()
            return
            
        info = f"🗑 **Character Deleted**\nID: {char_id}\nName: {row[0]} {row[1]}\nRarity: {RARITY_MAP.get(row[2])}\nDeleted by: {user.first_name} ({user.id})"
        try:
            await context.bot.send_photo(chat_id=OWNER_ID, photo=row[3], caption=info, parse_mode=ParseMode.MARKDOWN)
        except: pass

        if row[4]: 
            try: await context.bot.delete_message(chat_id=DATABASE_CHANNEL_ID, message_id=row[4])
            except: pass
        if row[5]:
            try: await context.bot.delete_message(chat_id=DATABASE_CHANNEL_ID2, message_id=row[5])
            except: pass

        del_text = f"🗑 Character **{row[0]} {row[1]}** (ID: {char_id}) was deleted by {user.first_name}."
        try: await context.bot.send_message(chat_id=DATABASE_CHANNEL_ID, text=del_text, parse_mode=ParseMode.MARKDOWN)
        except: pass
        if 1 <= row[2] <= 5:
            try: await context.bot.send_message(chat_id=DATABASE_CHANNEL_ID2, text=del_text, parse_mode=ParseMode.MARKDOWN)
            except: pass
            
        c.execute("DELETE FROM characters WHERE id=?", (char_id,))
        c.execute("DELETE FROM harem WHERE character_id=?", (char_id,))
        conn.commit()
        conn.close()
        
        await update.effective_message.reply_text(f"✅ Character {char_id} deleted successfully.")
        
    except ValueError:
        await update.effective_message.reply_text("ID must be a number.")

# --- EVENT EDIT ---
async def editevent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID: return
    edit_cache[update.effective_user.id] = {'mode': 'event_text'}
    await update.effective_message.reply_text("Ok, send me the new event text.")

# --- SPAWN ---
async def spawn_character(chat_id, context):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    c.execute("SELECT hentai_mode FROM groups WHERE chat_id=?", (chat_id,))
    res = c.fetchone()
    is_hentai = res[0] if res else 0

    weights = [10, 15, 25, 20, 15, 15] if is_hentai else [15, 15, 25, 25, 20, 0]
    rarity = random.choices([1, 2, 3, 4, 5, 6], weights=weights, k=1)[0]

    c.execute("SELECT id, name, surname, extra_name, file_id FROM characters WHERE rarity=?", (rarity,))
    chars = c.fetchall()
    conn.close()

    if not chars: return

    char = random.choice(chars)
    valid_names = [n.lower().strip() for n in [char[1], char[2], char[3]] if n]
    
    rarity_txt = get_rarity_text(rarity)
    caption = f"A wild character appeared!\nRate: {rarity_txt}\nUse `/steal [name]` to catch it!"
    
    msg = await context.bot.send_photo(chat_id=chat_id, photo=char[4], caption=caption, parse_mode=ParseMode.MARKDOWN)

    if "🔞 Hentai" in caption:
        asyncio.create_task(delete_after_delay(context, chat_id, msg.message_id, 40))

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("UPDATE groups SET last_spawn_id = ? WHERE chat_id=?", (msg.message_id, chat_id))
    conn.commit()
    conn.close()

    current_spawns[chat_id] = {
        'char_id': char[0],
        'full_name': f"{char[1]} {char[2]}",
        'valid_names': valid_names,
        'message_id': msg.message_id
    }

# --- MESSAGE HANDLER ---
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_message: return
    user_id = update.effective_user.id
    text = update.effective_message.text
    
    if user_id in edit_cache and 'mode' in edit_cache[user_id]:
        data = edit_cache[user_id]
        mode = data['mode']
        
        if mode == 'event_text':
            if not text:
                await update.effective_message.reply_text("Please send text.")
                return
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
            if not update.effective_message.photo:
                await update.effective_message.reply_text("Please send a photo.")
                conn.close()
                return
            new_file_id = update.effective_message.photo[-1].file_id
            c.execute("UPDATE characters SET file_id=? WHERE id=?", (new_file_id, char_id))
            await update.effective_message.reply_text("✅ Photo updated.")
            
        elif mode in ['name', 'surname']:
            if not text:
                conn.close()
                return
            c.execute(f"UPDATE characters SET {mode}=? WHERE id=?", (text, char_id))
            await update.effective_message.reply_text(f"✅ {mode.capitalize()} updated.")
            
        conn.commit()
        conn.close()
        
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

# --- ACTIONS ---
async def steal_character(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id not in current_spawns:
        await update.effective_message.reply_text("❌ No character currently spawned.")
        return

    if not context.args:
        await update.effective_message.reply_text("⚠️ Usage: `/steal [name]`", parse_mode=ParseMode.MARKDOWN)
        return

    guess_raw = " ".join(context.args)
    if re.search(r'[^\w\s]', guess_raw):
        await update.effective_message.reply_text("⚠️ Please send the correct name without emojis!")
        return

    guess = guess_raw.lower().strip()
    spawn = current_spawns[chat_id]

    if any(guess in name for name in spawn['valid_names']):
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO harem (user_id, character_id, count) VALUES (?, ?, 0)", (update.effective_user.id, spawn['char_id']))
        c.execute("UPDATE harem SET count = count + 1 WHERE user_id=? AND character_id=?", (update.effective_user.id, spawn['char_id']))
        conn.commit()
        conn.close()

        await update.effective_message.reply_text(f"🎉 **Congrats!**\nYou caught **{spawn['full_name']}**!", parse_mode=ParseMode.MARKDOWN)
        del current_spawns[chat_id]
    else:
        await update.effective_message.reply_text("❌ Wrong name! Try again.")

async def myharem(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    c.execute("SELECT fav_character_id, rarity_filter FROM users WHERE user_id=?", (user_id,))
    user_settings = c.fetchone()
    fav_id = user_settings[0] if user_settings else 0
    filter_rarity = user_settings[1] if user_settings else 0

    # Added file_id (index 5)
    c.execute("SELECT c.name, c.surname, c.rarity, h.count, c.id, c.file_id FROM harem h JOIN characters c ON h.character_id = c.id WHERE h.user_id = ?", (user_id,))
    data = c.fetchall()
    conn.close()

    if not data:
        await update.effective_message.reply_text("Your harem is empty!")
        return

    fav_char = None
    
    # 1. Find Specific Fav
    if fav_id:
        for char in data:
            if char[4] == fav_id:
                fav_char = char
                break
    
    # 2. Or Random
    if not fav_char:
        fav_char = random.choice(data)

    # 3. List Logic
    other_chars = []
    for char in data:
        if char[4] == fav_char[4]:
            continue 
        if filter_rarity == 0 or char[2] == filter_rarity:
            other_chars.append(char)

    other_chars.sort(key=lambda x: x[0].lower())

    caption = f"🌹 {update.effective_user.first_name}'s Harem 🌹\n"
    
    # Main Header
    emoji = get_rarity_emoji(fav_char[2])
    full_name = f"{fav_char[0]} {fav_char[1] if fav_char[1] else ''}".strip()
    caption += f"👑💎 🆔{fav_char[4]} {emoji} {full_name} x{fav_char[3]} 💎👑\n\n"
    
    # List Body
    limit = 10
    for char in other_chars[:limit]:
        emoji = get_rarity_emoji(char[2])
        full_name = f"{char[0]} {char[1] if char[1] else ''}".strip()
        caption += f"🆔{char[4]} {emoji} {full_name} x{char[3]}\n"
        
    if len(other_chars) > limit:
        caption += f"\n... and {len(other_chars)-limit} more."

    try:
        await update.effective_message.reply_photo(photo=fav_char[5], caption=caption)
    except Exception as e:
        await update.effective_message.reply_text(f"Error sending photo: {e}\n\n{caption}")

async def see(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args: return
    try:
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("SELECT name, surname, rarity, file_id FROM characters WHERE id=?", (context.args[0],))
        res = c.fetchone()
        conn.close()
        if res:
            caption = f"👤 {res[0]} {res[1]}\n{get_rarity_text(res[2])}"
            msg = await update.effective_message.reply_photo(res[3], caption=caption)
            if "🔞 Hentai" in caption:
                asyncio.create_task(delete_after_delay(context, update.effective_chat.id, msg.message_id, 40))
        else:
            await update.effective_message.reply_text("Not found.")
    except: pass

async def event_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT value FROM settings WHERE key='event_text'")
    res = c.fetchone()
    conn.close()
    
    event_text = res[0] if res else "No active events."
    await update.effective_message.reply_text(f"Current events:\n{event_text}")

async def fav_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.effective_message.reply_text("Usage: /fav [id]")
        return
    try:
        char_id = int(context.args[0])
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("SELECT count FROM harem WHERE user_id=? AND character_id=?", (update.effective_user.id, char_id))
        if not c.fetchone():
            await update.effective_message.reply_text("❌ You don't own this character!")
            conn.close()
            return
        c.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (update.effective_user.id,))
        c.execute("UPDATE users SET fav_character_id=? WHERE user_id=?", (char_id, update.effective_user.id))
        conn.commit()
        conn.close()
        await update.effective_message.reply_text(f"✅ Favorite set to ID: {char_id}")
    except ValueError:
        await update.effective_message.reply_text("ID must be a number.")

async def type_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("All", callback_data="filter_0")],
        [InlineKeyboardButton("⚫️ Common", callback_data="filter_1"), InlineKeyboardButton("🟠 Rare", callback_data="filter_2")],
        [InlineKeyboardButton("🟡 Legendary", callback_data="filter_3"), InlineKeyboardButton("🫧 Premium", callback_data="filter_4")],
        [InlineKeyboardButton("🔮 Event", callback_data="filter_5"), InlineKeyboardButton("🔞 Hentai", callback_data="filter_6")]
    ]
    await update.effective_message.reply_text("Select rarity to filter in /myharem:", reply_markup=InlineKeyboardMarkup(keyboard))

async def filter_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    await query.answer()
    rarity = int(query.data.split("_")[1])
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
    c.execute("UPDATE users SET rarity_filter=? WHERE user_id=?", (rarity, user_id))
    conn.commit()
    conn.close()
    label = "All" if rarity == 0 else RARITY_MAP.get(rarity)
    await query.edit_message_text(f"✅ Filter set to: {label}")

async def addsudo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id == OWNER_ID and update.effective_message.reply_to_message:
        target_id = update.effective_message.reply_to_message.from_user.id
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO sudos (user_id) VALUES (?)", (target_id,))
        conn.commit()
        conn.close()
        await update.effective_message.reply_text("✅ Added Sudo.")

async def remsudo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID: return
    target_id = None
    if update.effective_message.reply_to_message:
        target_id = update.effective_message.reply_to_message.from_user.id
    elif context.args:
        try: target_id = int(context.args[0])
        except: pass
    if target_id:
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("DELETE FROM sudos WHERE user_id=?", (target_id,))
        conn.commit()
        conn.close()
        await update.effective_message.reply_text(f"🗑 Removed sudo privileges from {target_id}.")
    else:
        await update.effective_message.reply_text("Reply to a user or provide ID.")

async def hentai_ok(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_member = await context.bot.get_chat_member(update.effective_chat.id, update.effective_user.id)
    if update.effective_user.id == OWNER_ID or chat_member.status in ['creator', 'administrator']:
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO groups (chat_id) VALUES (?)", (update.effective_chat.id,))
        c.execute("SELECT hentai_mode FROM groups WHERE chat_id=?", (update.effective_chat.id,))
        res = c.fetchone()
        new = 0 if res and res[0] else 1
        c.execute("UPDATE groups SET hentai_mode = ? WHERE chat_id=?", (new, update.effective_chat.id))
        conn.commit()
        conn.close()
        await update.effective_message.reply_text(f"🔞 Hentai: {'ON' if new else 'OFF'}")

async def set_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    is_admin = False
    try:
        chat_member = await context.bot.get_chat_member(update.effective_chat.id, update.effective_user.id)
        if chat_member.status in ['creator', 'administrator'] or is_sudo(update.effective_user.id):
            is_admin = True
    except: pass

    if context.args and is_admin:
        try:
            val = int(context.args[0])
            if not is_sudo(update.effective_user.id) and (val < 50 or val > 1000):
                 await update.effective_message.reply_text("⚠️ Admins can only set between 50-1000.")
                 return
            conn = sqlite3.connect(DB_NAME)
            c = conn.cursor()
            c.execute("INSERT OR IGNORE INTO groups (chat_id) VALUES (?)", (update.effective_chat.id,))
            current_msg_id = update.effective_message.message_id
            c.execute("UPDATE groups SET spawn_threshold = ?, last_spawn_id = ? WHERE chat_id=?", (val, current_msg_id, update.effective_chat.id))
            conn.commit()
            conn.close()
            await update.effective_message.reply_text(f"✅ Threshold set to: {val}. Counter reset to this message.")
        except: pass

async def donate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_sudo(update.effective_user.id): return
    if not update.effective_message.reply_to_message:
        await update.effective_message.reply_text("⚠️ Reply to a user.")
        return
    if not context.args:
        await update.effective_message.reply_text("⚠️ Usage: `/donate [char_id]`")
        return
    target_user_id = update.effective_message.reply_to_message.from_user.id
    try:
        char_id = int(context.args[0])
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO harem (user_id, character_id, count) VALUES (?, ?, 0)", (target_user_id, char_id))
        c.execute("UPDATE harem SET count = count + 1 WHERE user_id=? AND character_id=?", (target_user_id, char_id))
        conn.commit()
        conn.close()
        await update.effective_message.reply_text(f"✅ Character {char_id} donated.")
    except Exception as e:
        await update.effective_message.reply_text(f"❌ Error: {e}")

async def gift(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_message.reply_to_message:
        await update.effective_message.reply_text("⚠️ Reply to the user you want to gift to.")
        return
    if update.effective_message.reply_to_message.from_user.id == update.effective_user.id:
        await update.effective_message.reply_text("⚠️ You cannot gift yourself.")
        return
    if not context.args:
        await update.effective_message.reply_text("⚠️ Usage: `/gift [char_id]`")
        return
    sender_id = update.effective_user.id
    receiver_id = update.effective_message.reply_to_message.from_user.id
    try:
        char_id = int(context.args[0])
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("SELECT count FROM harem WHERE user_id=? AND character_id=?", (sender_id, char_id))
        sender_data = c.fetchone()
        if not sender_data or sender_data[0] < 1:
            await update.effective_message.reply_text("❌ You don't have this character to gift.")
            conn.close()
            return
        new_sender_count = sender_data[0] - 1
        if new_sender_count == 0:
            c.execute("DELETE FROM harem WHERE user_id=? AND character_id=?", (sender_id, char_id))
        else:
            c.execute("UPDATE harem SET count = ? WHERE user_id=? AND character_id=?", (new_sender_count, sender_id, char_id))
        c.execute("INSERT OR IGNORE INTO harem (user_id, character_id, count) VALUES (?, ?, 0)", (receiver_id, char_id))
        c.execute("UPDATE harem SET count = count + 1 WHERE user_id=? AND character_id=?", (receiver_id, char_id))
        conn.commit()
        conn.close()
        await update.effective_message.reply_text(f"🎁 Successfully gifted character {char_id} to {update.effective_message.reply_to_message.from_user.first_name}!")
    except ValueError:
        await update.effective_message.reply_text("⚠️ ID must be a number.")

if __name__ == '__main__':
    init_db()
    app = ApplicationBuilder().token(TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("upload", upload))
    app.add_handler(CommandHandler("edit", edit_command))
    app.add_handler(CommandHandler("del", delete_character))
    app.add_handler(CommandHandler("addsudo", addsudo))
    app.add_handler(CommandHandler("remsudo", remsudo))
    app.add_handler(CommandHandler("donate", donate))
    app.add_handler(CommandHandler("editevent", editevent))
    app.add_handler(CommandHandler("hentai", hentai_ok))
    app.add_handler(CommandHandler("time", set_time))
    app.add_handler(CommandHandler("steal", steal_character))
    app.add_handler(CommandHandler("myharem", myharem))
    app.add_handler(CommandHandler("see", see))
    app.add_handler(CommandHandler("gift", gift))
    app.add_handler(CommandHandler("event", event_command))
    app.add_handler(CommandHandler("fav", fav_command))
    app.add_handler(CommandHandler("type", type_command))
    
    app.add_handler(CallbackQueryHandler(rarity_handler, pattern="^rarity_"))
    app.add_handler(CallbackQueryHandler(edit_callback, pattern="^edit_|^setrarity_"))
    app.add_handler(CallbackQueryHandler(filter_callback, pattern="^filter_"))
    
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), message_handler))
    app.add_handler(MessageHandler(filters.PHOTO, message_handler))
    
    print("Bot Running...")
    app.run_polling()