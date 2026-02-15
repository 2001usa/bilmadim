import sqlite3
import os

# Database faylini yaratish
db_path = 'app/database/database.db'

# Agar database papkasi bo'lmasa, yaratish
os.makedirs(os.path.dirname(db_path), exist_ok=True)

# Database'ga ulanish (fayl yaratiladi)
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Users jadvali
cursor.execute('''
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    lang TEXT DEFAULT 'uz',
    is_admin INTEGER DEFAULT 0,
    is_staff INTEGER DEFAULT 0,
    is_anipass TEXT DEFAULT '0',
    is_lux TEXT DEFAULT '0'
)
''')

# Media jadvali
cursor.execute('''
CREATE TABLE IF NOT EXISTS media (
    media_id INTEGER PRIMARY KEY AUTOINCREMENT,
    trailer_id TEXT,
    name TEXT,
    genre TEXT,
    tag TEXT,
    dub TEXT,
    series INTEGER DEFAULT 0,
    status TEXT DEFAULT 'loading',
    views INTEGER DEFAULT 0,
    msg_id INTEGER DEFAULT 0,
    type TEXT DEFAULT 'anime',
    is_vip INTEGER DEFAULT 0
)
''')

# Episodes jadvali
cursor.execute('''
CREATE TABLE IF NOT EXISTS episodes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    which_media INTEGER,
    episode_id TEXT,
    episode_num INTEGER,
    msg_id INTEGER DEFAULT 0,
    FOREIGN KEY (which_media) REFERENCES media (media_id)
)
''')

# Sponsors jadvali
cursor.execute('''
CREATE TABLE IF NOT EXISTS sponsors (
    channel_id INTEGER PRIMARY KEY,
    channel_name TEXT,
    channel_link TEXT,
    type TEXT,
    user_limit INTEGER DEFAULT 0
)
''')

# Sponsor requests jadvali
cursor.execute('''
CREATE TABLE IF NOT EXISTS sponsor_request (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id INTEGER,
    user_id INTEGER
)
''')

# Statistics jadvali
cursor.execute('''
CREATE TABLE IF NOT EXISTS statistics (
    bot TEXT PRIMARY KEY DEFAULT 'bot',
    users_count INTEGER DEFAULT 0,
    anime_count INTEGER DEFAULT 0,
    drama_count INTEGER DEFAULT 0
)
''')

# Statistika uchun boshlang'ich qiymat
cursor.execute('''
INSERT OR IGNORE INTO statistics (bot, users_count, anime_count, drama_count) 
VALUES ('bot', 0, 0, 0)
''')

conn.commit()
conn.close()

print("Database muvaffaqiyatli yaratildi!")
print(f"Fayl: {db_path}")
