from aiogram.types import KeyboardButton,ReplyKeyboardMarkup


def act_1_btn():
    kb_list = [
        [KeyboardButton(text="🔍Anime Qidirish"), KeyboardButton(text="⚡️AniPass / 💎Lux")],
        [KeyboardButton(text="🏙Rasm orqali qidiruv")],
        [KeyboardButton(text="📚Qo'llanma"), KeyboardButton(text="💸Reklama va homiylik")],
        [KeyboardButton(text="Animelar ro'yxati📓"), KeyboardButton(text="OnGoing animelar🧧")],
    ]
    keyboard = ReplyKeyboardMarkup(keyboard=kb_list, resize_keyboard=True, one_time_keyboard=True)

    return keyboard

def act_2_btn():
    kb_list = [
        [KeyboardButton(text="➕Media Qo'shish"), KeyboardButton(text="➕Qism Qo'shish")],
        [KeyboardButton(text="✏️Media Tahrirlash"), KeyboardButton(text="✏️Qismni Tahrirlash")],
        [KeyboardButton(text="📊Statistika"), KeyboardButton(text="💬Xabar Yuborish")],
        [KeyboardButton(text="🔐Majburiy A'zo"), KeyboardButton(text="👔Admin Qo'shish")],
        [KeyboardButton(text="📤Post Qilish"), KeyboardButton(text="📤Qismni Post Qilish")],
        [KeyboardButton(text="🔙Chiqish")]
    ]
    keyboard = ReplyKeyboardMarkup(keyboard=kb_list, resize_keyboard=True, one_time_keyboard=True)

    return keyboard