import os
import json
from datetime import datetime
from difflib import SequenceMatcher
from dotenv import load_dotenv
from supabase import create_client, Client

# Load environment variables
load_dotenv()

url: str = os.getenv("SUPABASE_URL")
key: str = os.getenv("SUPABASE_KEY")

if not url or not key:
    raise ValueError("Supabase URL or Key is missing in .env file")

supabase: Client = create_client(url, key)

# ==================== ADD FUNCTIONS ====================

def add_user_base(user_id, username, lang="uz", is_admin=False, is_staff=False):
    data = {
        "user_id": user_id,
        "username": username,
        "lang": lang,
        "is_admin": is_admin,
        "is_staff": is_staff,
        "is_anipass": None,
        "is_lux": None
    }
    # Using upsert to handle potential duplicates gracefully or just insert
    # SQLite code was INSERT, which fails on duplicate. Supabase insert also fails on duplicate by default.
    try:
        supabase.table("users").insert(data).execute()
        update_statistics_user_count_base()
    except Exception as e:
        print(f"Error adding user: {e}")

def add_media_base(trailer_id, name, genre, tag, dub, series=0, status="loading", views=0, msg_id=0, type="anime"):
    data = {
        "trailer_id": trailer_id,
        "name": name,
        "genre": genre,
        "tag": tag,
        "dub": dub,
        "series": series,
        "status": status,
        "views": views,
        "msg_id": msg_id,
        "type": type,
        "is_vip": False
    }
    
    response = supabase.table("media").insert(data).execute()
    
    # Update statistics
    if type == "anime":
        # We need to increment manually or via RPC. 
        # For simplicity, fetching current stats and updating is risky for concurrency but matches SQLite logic.
        # Better: create a stored procedure in Supabase. 
        # But for now, let's keep it simple python-side logic or just separate calls.
        # Supabase doesn't support "UPDATE ... SET count = count + 1" directly via simple library call easily without RPC.
        # We will implement update_statistics... separately.
        current_stats = get_statistics_base()
        new_count = current_stats.get("anime_count", 0) + 1
        supabase.table("statistics").update({"anime_count": new_count}).eq("bot_name", "bot").execute()
    else:
        current_stats = get_statistics_base()
        new_count = current_stats.get("drama_count", 0) + 1
        supabase.table("statistics").update({"drama_count": new_count}).eq("bot_name", "bot").execute()

    if response.data:
        return response.data[0]['media_id']
    return None

def add_episode_base(which_media, episode_id, episode_num, msg_id):
    data = {
        "which_media": which_media,
        "episode_id": episode_id,
        "episode_num": episode_num,
        "msg_id": msg_id
    }
    response = supabase.table("episodes").insert(data).execute()
    if response.data:
        return response.data[0]['id']
    return None

def add_sponsor_base(channel_id, channel_name, channel_link, type, user_limit):
    data = {
        "channel_id": channel_id,
        "channel_name": channel_name,
        "channel_link": channel_link,
        "type": type,
        "user_limit": user_limit
    }
    response = supabase.table("sponsors").insert(data).execute()
    if response.data:
        return response.data[0]['id']
    return None

def add_sponsor_request_base(channel_id, user_id):
    data_exists = get_sponsor_request_base(channel_id, user_id)
    if not data_exists:
        supabase.table("sponsor_request").insert({"chat_id": channel_id, "user_id": user_id}).execute()
        
        sponsor = get_single_sponsors_base(channel_id)
        if sponsor:
            sponsor_limit = sponsor['user_limit'] - 1
            
            if sponsor_limit == 0:
                delete_sponsor_base(channel_id)
            else:
                update_sponsor_limit_count_minus_base(channel_id)

# ==================== GET FUNCTIONS ====================

def get_sponsor_request_base(channel_id, user_id):
    response = supabase.table("sponsor_request").select("*").eq("user_id", user_id).eq("chat_id", channel_id).execute()
    if response.data:
        return response.data[0]
    return None

def get_user_base(user_id):
    response = supabase.table("users").select("*").eq("user_id", user_id).execute()
    if response.data:
        return response.data[0]
    return None

def get_all_user_id_base():
    # Warning: Fetching all users might be heavy. Supabase limits rows (default 1000).
    # You might need pagination, but adhering to original logic:
    response = supabase.table("users").select("user_id").execute()
    return response.data # Returns list of dicts [{'user_id': 123}, ...]

def get_all_ongoing_media_base():
    response = supabase.table("media").select("*").eq("status", "loading").execute()
    return response.data

def get_all_media_base(type):
    response = supabase.table("media").select("*").eq("type", type).execute()
    return response.data

def search_media_base(name, type):
    # Implementing search logic
    if type == "any":
        response = supabase.table("media").select("*").ilike("name", f"%{name}%").execute()
    else:
        response = supabase.table("media").select("*").ilike("name", f"%{name}%").eq("type", type).execute()
    
    data = response.data
    
    if not data:
        # Fallback to fuzzy search logic from original code
        if type == "any":
            all_media = supabase.table("media").select("*").execute().data
        else:
            all_media = supabase.table("media").select("*").eq("type", type).execute().data
            
        def similar(a, b):
            return SequenceMatcher(None, a, b).ratio()

        new_data = []
        for i in all_media:
            similarity = similar(i["name"], name)
            if similarity >= 0.4:
                new_data.append([similarity, i])
            else:
                try:
                    if i["tag"]:
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
        return data

def get_media_base(media_id):
    response = supabase.table("media").select("*").eq("media_id", media_id).execute()
    if response.data:
        return response.data[0]
    return [] # Original code returned [] if not found (lines 160-161 of original)

def get_media_episodes_base(media_id):
    response = supabase.table("episodes").select("*").eq("which_media", media_id).order("episode_num", desc=False).execute()
    return response.data

def get_statistics_base():
    response = supabase.table("statistics").select("*").eq("bot_name", "bot").execute()
    if response.data:
        return response.data[0]
    return {}

def get_all_sponsors_base():
    response = supabase.table("sponsors").select("*").execute()
    return response.data

def get_single_sponsors_base(channel_id):
    response = supabase.table("sponsors").select("*").eq("channel_id", channel_id).execute()
    if response.data:
        return response.data[0]
    return []

def get_all_staff_base():
    response = supabase.table("users").select("*").eq("is_staff", True).execute()
    return response.data

# ==================== UPDATE FUNCTIONS ====================

def update_statistics_user_count_base():
    # Fetch current count first (not atomic, but acceptable for this migration)
    stats = get_statistics_base()
    if stats:
        new_count = stats.get('users_count', 0) + 1
        supabase.table("statistics").update({"users_count": new_count}).eq("bot_name", "bot").execute()

def update_media_episodes_count_plus_base(media_id):
    media = get_media_base(media_id)
    if media:
        new_series = media.get('series', 0) + 1
        supabase.table("media").update({"series": new_series}).eq("media_id", media_id).execute()

def update_media_episodes_count_minus_base(media_id):
    media = get_media_base(media_id)
    if media:
        new_series = media.get('series', 0) - 1
        supabase.table("media").update({"series": new_series}).eq("media_id", media_id).execute()

def update_media_name_base(media_id, name):
    supabase.table("media").update({"name": name}).eq("media_id", media_id).execute()

def update_media_genre_base(media_id, genre):
    supabase.table("media").update({"genre": genre}).eq("media_id", media_id).execute()

def update_media_tag_base(media_id, tag):
    supabase.table("media").update({"tag": tag}).eq("media_id", media_id).execute()

def update_media_dub_base(media_id, dub):
    supabase.table("media").update({"dub": dub}).eq("media_id", media_id).execute()

def update_media_vip_base(media_id, is_vip):
    supabase.table("media").update({"is_vip": is_vip}).eq("media_id", media_id).execute()

def update_media_status_base(media_id, status):
    supabase.table("media").update({"status": status}).eq("media_id", media_id).execute()

def update_episode_base(media_id, episode_num, episode_id):
    # Note: original code only updated episode_id, we might need msg_id too? 
    # Current request is just migration.
    supabase.table("episodes").update({"episode_id": episode_id}).eq("which_media", media_id).eq("episode_num", episode_num).execute()

def update_user_staff_base(user_id, value):
    # Value is 1 or 0 (likely), or boolean. Supabase uses boolean.
    is_staff = True if value else False
    supabase.table("users").update({"is_staff": is_staff}).eq("user_id", user_id).execute()

def update_user_admin_base(user_id, value):
    is_admin = True if value else False
    supabase.table("users").update({"is_admin": is_admin}).eq("user_id", user_id).execute()

def update_anipass_data_base():
    current_date = datetime.now().isoformat()
    # Supabase timestamp comparison
    response = supabase.table("users").select("user_id").lt("is_anipass", current_date).not_.is_("is_anipass", "null").execute()
    data = response.data
    
    if data:
        supabase.table("users").update({"is_anipass": None}).lt("is_anipass", current_date).execute()
        
    return data # Returns list of dicts [{'user_id': ...}] matching original signature intent

def update_lux_data_base():
    current_date = datetime.now().isoformat()
    response = supabase.table("users").select("user_id").lt("is_lux", current_date).not_.is_("is_lux", "null").execute()
    data = response.data
    
    if data:
        supabase.table("users").update({"is_lux": None}).lt("is_lux", current_date).execute()
        
    return data

def update_sponsor_limit_count_minus_base(channel_id):
    sponsor = get_single_sponsors_base(channel_id)
    if sponsor:
        new_limit = sponsor.get('user_limit', 0) - 1
        supabase.table("sponsors").update({"user_limit": new_limit}).eq("channel_id", channel_id).execute()

# ==================== DELETE FUNCTIONS ====================

def delete_episode_base(media_id, episode_num):
    supabase.table("episodes").delete().eq("which_media", media_id).eq("episode_num", episode_num).execute()

def delete_sponsor_base(channel_id):
    supabase.table("sponsors").delete().eq("channel_id", channel_id).execute()
    supabase.table("sponsor_request").delete().eq("chat_id", channel_id).execute()

def delete_media_base(media_id):
    """Delete media and all its episodes"""
    # Cascade delete is set in SQL (on delete cascade), so deleting media should delete episodes automatically.
    # But sticking to original explicit logic just in case:
    supabase.table("episodes").delete().eq("which_media", media_id).execute()
    supabase.table("media").delete().eq("media_id", media_id).execute()
