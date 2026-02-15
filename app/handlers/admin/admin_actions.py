from aiogram import F,Router
from aiogram.types import Message,CallbackQuery,InlineQuery,InlineQueryResultArticle
from aiogram.types.input_text_message_content import InputTextMessageContent
from aiogram.types.link_preview_options import LinkPreviewOptions
from aiogram.filters import CommandStart,Command
from aiogram.fsm.state import StatesGroup,State
from aiogram.fsm.context import FSMContext
from app.database.bot_base import *
from app.funcs.languages import *
from app.keyboards.admin.inline_buttons import *
from aiogram.enums import ParseMode,InlineQueryResultType
import asyncio
from aiogram.types.input_media_video import InputMediaVideo
from aiogram.types.input_media_photo import InputMediaPhoto
from app.funcs.functions import *
from datetime import *
from app.keyboards.admin.keyboard_buttons import *
from aiogram.types import ReplyKeyboardRemove
from app.funcs.filters.chat_filter import ChatTypeFilter
import random
from app.keyboards.admin.keyboard_buttons import *
from aiogram.types import ChatJoinRequest
import io
from aiogram.types import InputFile

admin_router = Router()

class User(StatesGroup):
    menu = State()

class Admin(StatesGroup):
    menu = State()

class Add_media(StatesGroup):
    name = State()

class Add_episode(StatesGroup):
    search = State()

class Edit_media(StatesGroup):
    search = State()

class Edit_episode(StatesGroup):
    search = State()

class Send_message(StatesGroup):
    type = State()

class Sponsor(StatesGroup):
    menu = State()

class Staff(StatesGroup):
    menu = State()

class Posting(StatesGroup):
    search = State()

class PostingEpisode(StatesGroup):
    search = State()

@admin_router.message(ChatTypeFilter(chat_type=["group", "supergroup"]))
async def action(msg: Message, state: FSMContext):
    pass

@admin_router.message(Command("start"))
async def action(msg: Message, state: FSMContext):

    await state.clear()
    await state.set_state(User.menu)
    await msg.answer(act_1_lang(),reply_markup=act_1_btn(),parse_mode=ParseMode.HTML)

@admin_router.message(Admin.menu)
async def action(msg: Message, state: FSMContext):

    command = msg.text
    
    is_admin = False
    user = get_user_base(msg.from_user.id)
    if user['is_admin'] == True:
        is_admin = True

    if command == "➕Media Qo'shish":
        await state.clear()
        await state.set_state(Add_media.name)

        a = await msg.answer("<b>✨Yangi media nomini yuboring</b>",reply_markup=act_1_clbtn(),parse_mode=ParseMode.HTML)
        await state.update_data(message_id = a.message_id)

    elif command == "➕Qism Qo'shish":
        await state.clear()
        await state.set_state(Add_episode.search)

        a = await msg.answer("<b>✨Qism qo'shilishi kerak bo'lgan media nomini yuboring</b>",reply_markup=act_1_clbtn(),parse_mode=ParseMode.HTML)
        await state.update_data(message_id = a.message_id)

    elif command == "✏️Media Tahrirlash":
        await state.clear()
        await state.set_state(Edit_media.search)

        a = await msg.answer("<b>✏️Tahrirlanishi kerak bo'lgan media nomini yuboring</b>",reply_markup=act_1_clbtn(),parse_mode=ParseMode.HTML)
        await state.update_data(message_id = a.message_id)

    elif command == "✏️Qismni Tahrirlash":
        await state.clear()
        await state.set_state(Edit_episode.search)

        a = await msg.answer("<b>✏️Qismlari tahrirlanishi kerak bo'lgan media nomini yuboring</b>",reply_markup=act_1_clbtn(),parse_mode=ParseMode.HTML)
        await state.update_data(message_id = a.message_id)

    elif command == "📊Statistika":

        statistics = get_statistics_base()
        users_count = statistics['users_count']
        anime_count = statistics['anime_count']
        drama_count = statistics['drama_count']

        text = f"""
<b>📊Botining statistikasi :</b>

<i>👥Foydalanuvchilar soni: {users_count} ta</i>
<i>🔸Animelar soni: {anime_count} ta</i>
<i>🔹Dramalar soni: {drama_count} ta</i>
"""
        
        await msg.answer(text,parse_mode=ParseMode.HTML)
    
    elif command == "📤Post Qilish":
        await state.clear()
        await state.set_state(Posting.search)

        a = await msg.answer("<b>✨Post qilinishi kerak bo'lgan media nomini kiriting</b>",reply_markup=act_1_clbtn(),parse_mode=ParseMode.HTML)
        await state.update_data(message_id = a.message_id)

    elif command == "📤Qismni Post Qilish":
        await state.clear()
        await state.set_state(PostingEpisode.search)

        a = await msg.answer("<b>✨QIsmi post qilinishi kerak bo'lgan media nomini kiriting</b>",reply_markup=act_1_clbtn(),parse_mode=ParseMode.HTML)
        await state.update_data(message_id = a.message_id)

    elif command == "🔙Chiqish":
        await state.clear()
        await state.set_state(User.menu)
        await msg.answer("<b>🏠Bosh menyu</b>",reply_markup=act_1_btn(),parse_mode=ParseMode.HTML)

    if command in [
        "💬Xabar Yuborish",
        "🔐Majburiy A'zo",
        "👔Admin Qo'shish"
    ] and not is_admin:
        await msg.answer("<b>❌Bu funksiyadan foydalana olmaysiz</b>",parse_mode=ParseMode.HTML)

    else:
        if command == "💬Xabar Yuborish":
            await state.clear()
            await state.set_state(Send_message.type)

            a = await msg.answer("<b>💬Habar yuborish turini tanlang:</b>",reply_markup=act_7_clbtn(),parse_mode=ParseMode.HTML)
            await state.update_data(message_id = a.message_id)

        elif command == "🔐Majburiy A'zo":
            await state.clear()
            await state.set_state(Sponsor.menu)

            sponsors = get_all_sponsors_base()

            a = await msg.answer("<b>🔐Homiy kanallar</b>",reply_markup=act_8_clbtn(sponsors),parse_mode=ParseMode.HTML)
            await state.update_data(message_id = a.message_id)

        elif command == "👔Admin Qo'shish":
            await state.clear()
            await state.set_state(Staff.menu)

            staff_list = get_all_staff_base()

            a = await msg.answer("<b>👔Adminlar</b>",reply_markup=act_9_clbtn(staff_list),parse_mode=ParseMode.HTML)
            await state.update_data(message_id = a.message_id)

    