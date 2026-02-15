from aiogram.types import InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from app.funcs.languages import *
from config import *

def act_1_clbtn():
    button = InlineKeyboardBuilder()
    button.add(InlineKeyboardButton(text="🔙Bekor qilish",callback_data="c,back"))
    button.adjust(1)

    return button.as_markup()

def act_2_clbtn(medias):
    button = InlineKeyboardBuilder()
    for i in medias:
        if i["type"] == "anime":
            button.add(InlineKeyboardButton(text=f"🔸Anime: {i['name']}",callback_data=f"s,media,{i['media_id']}"))
        else:
            button.add(InlineKeyboardButton(text=f"🔹Drama: {i['name']}",callback_data=f"s,media,{i['media_id']}"))

    button.add(InlineKeyboardButton(text="🔙Chiqish",callback_data="s,back"))
    button.adjust(1)

    return button.as_markup()

def act_3_clbtn(status):
    button = InlineKeyboardBuilder()

    button.add(InlineKeyboardButton(text="🏷Nomini",callback_data="s,edit,name"))
    button.add(InlineKeyboardButton(text="📚Janrini",callback_data="s,edit,genre"))
    button.add(InlineKeyboardButton(text="🔍Tegini",callback_data="s,edit,tag"))
    button.add(InlineKeyboardButton(text="🎙Ovoz beruvchini",callback_data="s,edit,dub"))

    if status == "loading":
        button.add(InlineKeyboardButton(text="Tugallash✅",callback_data="s,edit,status,finished"))
    else:
        button.add(InlineKeyboardButton(text="🔸OnGoing",callback_data="s,edit,status,loading"))

    button.add(InlineKeyboardButton(text="🗑O'chirish",callback_data="s,delete"))
    button.add(InlineKeyboardButton(text="🔙Chiqish",callback_data="s,back"))
    button.adjust(2)

    return button.as_markup()

def act_4_clbtn():
    button = InlineKeyboardBuilder()
    button.add(InlineKeyboardButton(text="🔙Bekor qilish",callback_data="s,back"))
    button.adjust(1)

    return button.as_markup()

def act_5_clbtn(episodes,selected_episode):
    button = InlineKeyboardBuilder()

    for i in episodes:
        if i['episode_num'] == selected_episode:
            button.add(InlineKeyboardButton(text=f"[ {i['episode_num']} - qism ]",callback_data=f"s,edit,{i['which_media']}-{i['episode_num']}"))
        else:
            button.add(InlineKeyboardButton(text=f"{i['episode_num']} - qism",callback_data=f"s,select,{i['which_media']}-{i['episode_num']}"))
    button.adjust(5)

    button.row(InlineKeyboardButton(text="🔙Bekor qilish",callback_data=f"s,back,{i['which_media']}-{i['episode_num']}"))

    return button.as_markup()

def act_6_clbtn(is_last_episode,episode):
    button = InlineKeyboardBuilder()

    button.add(InlineKeyboardButton(text="♻️Alishtirish",callback_data=f"s,replace,{episode['which_media']}-{episode['episode_num']}"))
    button.add(InlineKeyboardButton(text="🗑O'chirish",callback_data=f"s,delete,{episode['which_media']}-{episode['episode_num']}"))

    button.add(InlineKeyboardButton(text="🔙Bekor qilish",callback_data=f"s,back,{episode['which_media']}"))

    button.adjust(2)

    return button.as_markup()

def act_7_clbtn():
    button = InlineKeyboardBuilder()
    button.add(InlineKeyboardButton(text="⚡️Bot nomidan ( tez )",callback_data="s,type1"))
    button.add(InlineKeyboardButton(text="🔁Forward qilib ( sekin )",callback_data="s,type2"))
    button.add(InlineKeyboardButton(text="🔙Bekor qilish",callback_data="c,back"))
    button.adjust(1)

    return button.as_markup()

def act_8_clbtn(sponsors):
    button = InlineKeyboardBuilder()
    for i in sponsors:
        if i["type"] in ["simple","request"]:
            button.add(
                InlineKeyboardButton(text=f"{i['channel_name']}",callback_data=f"c,channel,{i['channel_id']}"),
                InlineKeyboardButton(text=f"{i['user_limit']}👤",callback_data=f"c,limit"),
                InlineKeyboardButton(text=f"🔗Ko'rish",url=i["channel_link"])
            )

        else: 
            button.add(
                InlineKeyboardButton(text=f"{i['channel_name']}",callback_data=f"c,channel,{i['channel_id']}"),
                InlineKeyboardButton(text=f"⚠️",callback_data=f"c,notlimit"),
                InlineKeyboardButton(text=f"🔗Ko'rish",url=i["channel_link"])
            )

    button.adjust(3)
    button.row(InlineKeyboardButton(text="➕",callback_data="c,add"),InlineKeyboardButton(text="🔙Chiqish",callback_data="d,back"))

    return button.as_markup()

def act_9_clbtn(staff_list):
    button = InlineKeyboardBuilder()
    for i in staff_list:
        button.add(InlineKeyboardButton(text=f"{i['user_id']} | {i['username']}",callback_data=f"c,staff,{i['user_id']}"))

    button.adjust(1)
    button.row(InlineKeyboardButton(text="➕",callback_data="c,add"),InlineKeyboardButton(text="🔙Chiqish",callback_data="c,back"))

    return button.as_markup()

def act_10_clbtn():
    button = InlineKeyboardBuilder()
    button.add(InlineKeyboardButton(text="✅Ha",callback_data="c,yeah"),InlineKeyboardButton(text="❌Yo'q",callback_data="c,nope"))

    return button.as_markup()

def act_11_clbtn(media_id):
    button = InlineKeyboardBuilder()
    button.add(InlineKeyboardButton(text="✨Tomosha qilish✨",url=f"https://t.me/{bot_username}?start={media_id}"))

    return button.as_markup()

def act_12_clbtn():
    button = InlineKeyboardBuilder()
    button.add(InlineKeyboardButton(text="🔸Anime",callback_data="s,anime"))
    button.add(InlineKeyboardButton(text="🔹Drama",callback_data="s,drama"))
    button.add(InlineKeyboardButton(text="🔙Bekor qilish",callback_data="c,back"))
    button.adjust(1)

    return button.as_markup()

def act_13_clbtn():
    button = InlineKeyboardBuilder()
    button.add(InlineKeyboardButton(text="✅Ha",callback_data="s,yeah"),InlineKeyboardButton(text="❌Yo'q",callback_data="s,nope"))

    return button.as_markup()

def act_14_clbtn(media_id,episode):
    button = InlineKeyboardBuilder()
    button.add(InlineKeyboardButton(text=f"✨{episode} - qism✨",url=f"https://t.me/{bot_username}?start=serie{media_id}"))

    return button.as_markup()

def act_15_clbtn():
    button = InlineKeyboardBuilder()
    button.add(InlineKeyboardButton(text="📢Oddiy/Yopiq kanal",callback_data="s,type,simple"))
    button.add(InlineKeyboardButton(text="🔒Zayafkali kanal",callback_data="s,type,request"))
    button.add(InlineKeyboardButton(text="🌐Global link",callback_data="s,type,link"))
    button.add(InlineKeyboardButton(text="🔙Chiqish",callback_data="s,back"))
    button.adjust(2)

    return button.as_markup()