from aiogram.types import InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from app.funcs.languages import *
from config import bot_username

def user_act_1_clbtn():
    button = InlineKeyboardBuilder()
    button.row(InlineKeyboardButton(text="🆔Kod orqali qidiruv",callback_data="c,search_id"))
    button.row(
        InlineKeyboardButton(text="🔍Anime Qidiruv",callback_data="c,search_anime"),
        InlineKeyboardButton(text="🔍Drama Qidiruv",callback_data="c,search_drama")
    )
    button.row(InlineKeyboardButton(text="🏙Rasm Orqali Anime Qidiruv",callback_data="c,image"))
    button.row(
        InlineKeyboardButton(text="📚Qo'llanma",callback_data="c,manual"),
        InlineKeyboardButton(text="💸Reklama",callback_data="c,sponsorship")
    )
    button.row(InlineKeyboardButton(text="📓Ro'yxat",callback_data="c,list"))

    return button.as_markup()

def user_act_2_clbtn():
    button = InlineKeyboardBuilder()
    button.add(
        InlineKeyboardButton(text="🔙Ortga",callback_data="c,back")
    )
    button.adjust(1)

    return button.as_markup()

def user_act_3_clbtn(medias):
    button = InlineKeyboardBuilder()
    for i in medias:
        button.add(InlineKeyboardButton(text=f"{i['name']}",callback_data=f"s,media,{i['media_id']}"))
    button.add(InlineKeyboardButton(text="🔙Chiqish",callback_data="s,back"))
    button.adjust(1)

    return button.as_markup()

def user_act_4_clbtn():
    button = InlineKeyboardBuilder()
    button.add(InlineKeyboardButton(text="🔙Ortga",callback_data="c,back"))
    button.adjust(1)

    return button.as_markup()

def user_act_5_clbtn(series_count,anime_id):
    button = InlineKeyboardBuilder()
    if series_count != 0:
        button.add(InlineKeyboardButton(text="▶️Tomosha qilish",callback_data=f"c,watch,{anime_id}"))
    button.add(InlineKeyboardButton(text="🔙Ortga",callback_data="c,back"))
    button.adjust(1)

    return button.as_markup()

def user_act_6_clbtn(episodes,page,now_episode,anime_id):
    button = InlineKeyboardBuilder()

    start = page * 20 - 20
    end = page * 20 + 20

    if start < 0:
        start = 0

    episodes_cutted = episodes[start:end]
    
    for i in episodes_cutted:
        if now_episode == i['episode_num']:
            button.add(InlineKeyboardButton(text=f"[ {i['episode_num']} ]",callback_data="c,now"))
        else:
            button.add(InlineKeyboardButton(text=f"{i['episode_num']}",callback_data=f"c,episode,{i['episode_num']},{page},{anime_id}"))
    
    button.adjust(5)
    
    if episodes[end + 1:] and page > 0:
        button.row(
            InlineKeyboardButton(text=f"⏪Oldingi",callback_data=f"c,previous,{page-1},{anime_id},{now_episode}"),
            InlineKeyboardButton(text=f"Keyingi⏩",callback_data=f"c,next,{page+1},{anime_id},{now_episode}")
        )

    elif episodes[end + 1:]:
        button.row(InlineKeyboardButton(text=f"Keyingi⏩",callback_data=f"c,next,{page+1},{anime_id},{now_episode}"))

    elif page > 0:
        button.row(InlineKeyboardButton(text=f"⏪Oldingi",callback_data=f"c,previous,{page-1},{anime_id},{now_episode}"))

    button.row(InlineKeyboardButton(text="🔙Ortga",callback_data="c,back"))

    return button.as_markup()

def user_act_7_clbtn(sponsors):
    button = InlineKeyboardBuilder()

    for i in sponsors:
        button.add(InlineKeyboardButton(text=f"{i['channel_name']}",url=f"{i['channel_link']}"))

    button.add(InlineKeyboardButton(text=f"♻️Tekshirish",url=f"https://t.me/{bot_username}?start=check"))

    button.adjust(1)

    return button.as_markup()

def user_act_8_clbtn():
    button = InlineKeyboardBuilder()

    button.add(InlineKeyboardButton(text=f"🔙Ortga",callback_data="s,back"))

    button.adjust(1)

    return button.as_markup()

def user_act_9_clbtn():
    button = InlineKeyboardBuilder()

    button.add(InlineKeyboardButton(text=f"⚡️AniPass",callback_data="c,anipass"))
    button.add(InlineKeyboardButton(text=f"💎Lux kanal",callback_data="c,lux"))
    button.add(InlineKeyboardButton(text=f"🔙Ortga",callback_data="s,back"))

    button.adjust(1)

    return button.as_markup()