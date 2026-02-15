import sqlite3
from datetime import *
from difflib import SequenceMatcher
import json

conn = sqlite3.connect('app/database/database.db',check_same_thread=False)
cursor = conn.cursor()

def add_user_base(user_id , username , lang = "uz" , is_admin = False , is_staff = False):
    # user_id , username , lang , is_admin , is_staff
    cursor.execute('INSERT INTO users (user_id , username , lang , is_admin , is_staff) VALUES (?, ?, ?, ?, ?);', (user_id , username , lang , is_admin , is_staff))
    update_statistics_user_count_base()
    conn.commit()

def add_media_base(trailer_id , name , genre , tag , dub , series = 0 , status = "loading", views = 0, msg_id = 0, type = "anime"):
    #trailer_id , name , genre , tag , dub , series , status , views , msg_id , type
    cursor.execute('INSERT INTO media (trailer_id , name , genre , tag , dub , series , status , views , msg_id , type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', (trailer_id , name , genre , tag , dub , series , status , views , msg_id , type))
    
    if type == "anime":
        cursor.execute(f"""UPDATE statistics SET anime_count = anime_count + 1 WHERE bot = "bot" """)
    else:
        cursor.execute(f"""UPDATE statistics SET drama_count = drama_count + 1 WHERE bot = "bot" """)

    conn.commit()
    return cursor.lastrowid

def add_episode_base(which_media , episode_id , episode_num, msg_id):
    # which_media , episode_id , episode_num, msg_id
    cursor.execute('INSERT INTO episodes (which_media , episode_id , episode_num, msg_id) VALUES (?, ?, ?, ?);', (which_media , episode_id , episode_num, msg_id))
    conn.commit()
    return cursor.lastrowid

def add_sponsor_base(channel_id , channel_name , channel_link, type, user_limit):
    # channel_id , channel_name , channel_link , type , user_limit
    cursor.execute('INSERT INTO sponsors (channel_id , channel_name , channel_link , type , user_limit) VALUES (?, ?, ?, ?, ?);', (channel_id , channel_name , channel_link , type , user_limit))
    conn.commit()
    return cursor.lastrowid

def add_sponsor_request_base(channel_id, user_id):
    # channel_id, user_id
    data = get_sponsor_request_base(channel_id,user_id)
    if not data:
        cursor.execute('INSERT INTO sponsor_request (chat_id,user_id) VALUES (?, ?);', (channel_id, user_id))
        conn.commit()

        sponsor = get_single_sponsors_base(channel_id)
        sponsor_limit = sponsor['user_limit'] - 1

        if sponsor_limit == 0:
            delete_sponsor_base(channel_id)
        else:
            update_sponsor_limit_count_minus_base(channel_id)

# ==============================================================================

def get_sponsor_request_base(channel_id, user_id):
    cursor.execute("SELECT * FROM sponsor_request WHERE user_id = ? AND chat_id = ?", (user_id,channel_id))
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchone()

    if not data:
        return None

    return dict(zip(columns, data))

def get_user_base(user_id):
    cursor.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchone()

    if not data:
        return None

    return dict(zip(columns, data))

def get_all_user_id_base():
    cursor.execute("SELECT user_id FROM users")
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()

    if not data:
        return []
    
    return [dict(zip(columns, row)) for row in data]

def get_all_ongoing_media_base():
    cursor.execute("SELECT * FROM media WHERE status = 'loading' ")
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()

    if not data:
        return []
    
    return [dict(zip(columns, row)) for row in data]

def get_all_media_base(type):
    cursor.execute(f"SELECT * FROM media WHERE type = '{type}' ")
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()

    if not data:
        return []
    
    return [dict(zip(columns, row)) for row in data]

def search_media_base(name,type):
    if type == "any":
        cursor.execute(f"""SELECT * FROM media WHERE name LIKE "%{name}%" """)
    else:
        cursor.execute(f"""SELECT * FROM media WHERE name LIKE "%{name}%" AND type = "{type}" """)
        
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    
    if not data:
        if type == "any":
            cursor.execute("SELECT * FROM media ")
        else:
            cursor.execute(f"""SELECT * FROM media WHERE type = "{type}" """)

        all_data = cursor.fetchall()
        conn.commit()

        # Ma'lumotlarni lug'at formatiga o'tkazish
        media = [dict(zip(columns, row)) for row in all_data]

        def similar(a, b):
            return SequenceMatcher(None, a, b).ratio()

        new_data = []

        for i in media:
            similarity = similar(i["name"], name)
            if similarity >= 0.4:
                new_data.append([similarity, i])
            else:
                try:
                    tags = i["tag"].split(",")
                    for tag in tags:
                        tag_similarity = similar(tag, name)
                        if tag_similarity >= 0.5:
                            new_data.append([tag_similarity, i])
                            break
                except KeyError:
                    pass
        
        new_data.sort(reverse=True, key=lambda x: x[0])

        return [i[1] for i in new_data]

    else:
        return [dict(zip(columns, row)) for row in data]
    

def get_media_base(media_id):
    cursor.execute("SELECT * FROM media WHERE media_id = ?", (media_id,))
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchone()

    if not data:
        return []
    
    return dict(zip(columns, data))

def get_media_episodes_base(media_id):
    cursor.execute("SELECT * FROM episodes WHERE which_media = ? ORDER BY episode_num ASC", (media_id,))
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()

    if not data:
        return []
    
    return [dict(zip(columns, row)) for row in data]

def get_statistics_base():
    cursor.execute("SELECT * FROM statistics WHERE bot = 'bot'")
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchone()
    
    return dict(zip(columns, data))

def get_all_sponsors_base():
    cursor.execute("SELECT * FROM sponsors")
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()

    if not data:
        return []
    
    return [dict(zip(columns, row)) for row in data]

def get_single_sponsors_base(channel_id):
    cursor.execute(f"SELECT * FROM sponsors WHERE channel_id = {channel_id} ")
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchone()

    if not data:
        return []
    
    return dict(zip(columns, data))

def get_all_staff_base():
    cursor.execute("SELECT * FROM users WHERE is_staff = 1")
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()

    if not data:
        return []
    
    return [dict(zip(columns, row)) for row in data]

# ==============================================================================

def update_statistics_user_count_base():
    cursor.execute(f"""UPDATE statistics SET users_count = users_count + 1 WHERE bot = "bot" """)
    conn.commit()

def update_media_episodes_count_plus_base(media_id):
    cursor.execute(f"""UPDATE media SET series = series + 1 WHERE media_id = {media_id} """)
    conn.commit()

def update_media_episodes_count_minus_base(media_id):
    cursor.execute(f"""UPDATE media SET series = series - 1 WHERE media_id = {media_id} """)
    conn.commit()

def update_media_name_base(media_id,name):
    cursor.execute(f"""UPDATE media SET name = "{name}" WHERE media_id = {media_id} """)
    conn.commit()

def update_media_genre_base(media_id,genre):
    cursor.execute(f"""UPDATE media SET genre = "{genre}" WHERE media_id = {media_id} """)
    conn.commit()

def update_media_tag_base(media_id,tag):
    cursor.execute(f"""UPDATE media SET tag = "{tag}" WHERE media_id = {media_id} """)
    conn.commit()

def update_media_dub_base(media_id,dub):
    cursor.execute(f"""UPDATE media SET dub = "{dub}" WHERE media_id = {media_id} """)
    conn.commit()

def update_media_vip_base(media_id,is_vip):
    cursor.execute(f"""UPDATE media SET is_vip = {is_vip} WHERE media_id = {media_id} """)
    conn.commit()

def update_media_status_base(media_id,status):
    cursor.execute(f"""UPDATE media SET status = "{status}" WHERE media_id = {media_id} """)
    conn.commit()

def update_episode_base(media_id,episode_num,episode_id):
    cursor.execute(f"""UPDATE episodes SET episode_id = "{episode_id}" WHERE which_media = {media_id} AND episode_num = {episode_num} """)
    conn.commit()

def update_user_staff_base(user_id,value):
    cursor.execute(f"""UPDATE users SET is_staff = {value} WHERE user_id = {user_id} """)
    conn.commit()

def update_user_admin_base(user_id,value):
    cursor.execute(f"""UPDATE users SET is_admin = {value} WHERE user_id = {user_id} """)
    conn.commit()

def update_anipass_data_base():

    current_date = datetime.now().strftime("%Y-%m-%d")

    cursor.execute(f"""
        SELECT user_id 
        FROM users 
        WHERE is_anipass < '{current_date}'
        AND is_anipass != 0
    """)

    data = cursor.fetchall()

    cursor.execute(""" 
        UPDATE users 
        SET is_anipass = 0 
        WHERE is_anipass < '{current_date}'
        AND is_anipass != 0
    """)

    conn.commit()
    return data

def update_lux_data_base():

    current_date = datetime.now().strftime("%Y-%m-%d")

    cursor.execute(f"""
        SELECT user_id 
        FROM users 
        WHERE is_lux < '{current_date}'
        AND is_lux != 0
    """)

    data = cursor.fetchall()

    cursor.execute(""" 
        UPDATE users 
        SET is_lux = 0 
        WHERE is_lux < '{current_date}'
        AND is_lux != 0
    """)

    conn.commit()
    return data

def update_sponsor_limit_count_minus_base(channel_id):
    cursor.execute(f"""UPDATE sponsors SET user_limit = user_limit - 1 WHERE channel_id = {channel_id} """)
    conn.commit()

# ==============================================================================

def delete_episode_base(media_id,episode_num):
    cursor.execute(f"""DELETE FROM episodes WHERE which_media = {media_id} AND episode_num = {episode_num}""")
    conn.commit()

def delete_sponsor_base(channel_id):
    cursor.execute(f"""DELETE FROM sponsors WHERE channel_id = {channel_id} """)
    cursor.execute(f"""DELETE FROM sponsor_request WHERE chat_id = {channel_id} """)
    conn.commit()

def delete_media_base(media_id):
    """Delete media and all its episodes"""
    cursor.execute(f"""DELETE FROM episodes WHERE which_media = {media_id}""")
    cursor.execute(f"""DELETE FROM media WHERE media_id = {media_id}""")
    conn.commit()

# ==============================================================================
