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
from config import *

add_media_router = Router()

class Admin(StatesGroup):
    menu = State()

class Add_media(StatesGroup):
    name = State()
    trailer = State()
    genre = State()
    dub = State()
    tag = State()
    type = State()

@add_media_router.message(ChatTypeFilter(chat_type=["group", "supergroup"]))
async def action(msg: Message, state: FSMContext):
    pass

@add_media_router.callback_query(F.data.startswith('c'),Add_media())
async def action(call: CallbackQuery, state: FSMContext):

    await state.clear()
    await state.set_state(Admin.menu)
    await call.message.answer("✅<b>Bekor qilindi</b>",parse_mode=ParseMode.HTML,reply_markup=act_2_btn())
    await call.message.delete()

@add_media_router.message(F.text, Add_media.name)
async def action(msg: Message, state: FSMContext):

    data = await state.get_data()
    message_id = data.get("message_id")
    await msg.bot.delete_message(chat_id=msg.chat.id,message_id=message_id)

    name = msg.text
    await state.update_data(name = name)
    await state.set_state(Add_media.type)

    a = await msg.answer("<b>✨Yangi media tipini tanlang</b>",reply_markup=act_12_clbtn(),parse_mode=ParseMode.HTML)
    await state.update_data(message_id = a.message_id)

@add_media_router.callback_query(F.data.startswith('s'),Add_media.type)
async def action(call: CallbackQuery, state: FSMContext):

    command = call.data.split(",")[1]

    await state.update_data(type=command)
    await state.set_state(Add_media.trailer)
    await call.message.edit_text("<b>✨Yangi media trellerini yuboring</b>",parse_mode=ParseMode.HTML,reply_markup=act_1_clbtn())
    
@add_media_router.message(F.video,Add_media.trailer)
async def action(msg: Message, state: FSMContext):

    data = await state.get_data()
    message_id = data.get("message_id")
    await msg.bot.delete_message(chat_id=msg.chat.id,message_id=message_id)

    trailer = msg.message_id
    await state.update_data(trailer = trailer)
    await state.set_state(Add_media.genre)

    a = await msg.answer("<b>✨Yangi media Janrlarini yuboring\n🔸<i>Na'muna: Jangari, Fantastika, Sarguzasht ...</i>\n\n/skip - o'tkazib yuborish</b>",reply_markup=act_1_clbtn(),parse_mode=ParseMode.HTML)
    await state.update_data(message_id = a.message_id)

@add_media_router.message(F.text,Add_media.genre)
async def action(msg: Message, state: FSMContext):

    data = await state.get_data()
    message_id = data.get("message_id")

    # Check for /skip command
    if msg.text == "/skip":
        await msg.bot.delete_message(chat_id=msg.chat.id,message_id=message_id)
        await state.update_data(genre = "Noma'lum")
        await state.set_state(Add_media.dub)
        a = await msg.answer("<b>✨Yangi media Ovoz beruvchisini yuboring\n\n/skip - o'tkazib yuborish</b>",reply_markup=act_1_clbtn(),parse_mode=ParseMode.HTML)
        await state.update_data(message_id = a.message_id)
        return

    genre = msg.text.replace(", ",",").replace(" ,",",").replace(" ","_").replace("'","").replace("’","")
    if len(genre.split(",")) >= 3:
        await msg.bot.delete_message(chat_id=msg.chat.id,message_id=message_id)

        await state.update_data(genre = genre)
        await state.set_state(Add_media.dub)

        a = await msg.answer("<b>✨Yangi media Ovoz beruvchisini yuboring\n\n/skip - o'tkazib yuborish</b>",reply_markup=act_1_clbtn(),parse_mode=ParseMode.HTML)
        await state.update_data(message_id = a.message_id)
    else:
        a = await msg.answer("⚠️<b>Minimum 3 ta janr kiriting\n🔸<i>Na'muna: Jangari, Fantastika, Sarguzasht ...</i></b>",parse_mode=ParseMode.HTML)
        await asyncio.sleep(3)
        await a.delete()

@add_media_router.message(F.text,Add_media.dub)
async def action(msg: Message, state: FSMContext):

    data = await state.get_data()
    message_id = data.get("message_id")
    await msg.bot.delete_message(chat_id=msg.chat.id,message_id=message_id)

    # Check for /skip command
    if msg.text == "/skip":
        dub = "Noma'lum"
    else:
        dub = msg.text
    
    await state.update_data(dub = dub)
    await state.set_state(Add_media.tag)

    a = await msg.answer("<b>✨Yangi media Teglarini yuboring\n🔸<i>Na'muna: Spidermen, O'rgimchak odam, Marvel ...</i>\n\n/skip - o'tkazib yuborish</b>",reply_markup=act_1_clbtn(),parse_mode=ParseMode.HTML)
    await state.update_data(message_id = a.message_id)

@add_media_router.message(F.text,Add_media.tag)
async def action(msg: Message, state: FSMContext):

    data = await state.get_data()
    message_id = data.get("message_id")
    user_id = msg.from_user.id

    # Check for /skip command
    if msg.text == "/skip":
        tag = "Noma'lum"
    else:
        tag = msg.text.replace(", ",",").replace(" ,",",")
    
    if msg.text == "/skip" or len(tag.split(",")) >= 3:
        
        await msg.bot.delete_message(chat_id=msg.chat.id,message_id=message_id)
        a = await msg.answer("<b>♻️Yangi media bazaga qo'shilmoqda . . .</b>",parse_mode=ParseMode.HTML)

        trailer = data.get("trailer")
        name = data.get("name")
        genre = data.get("genre")
        dub = data.get("dub")
        type = data.get("type")

        trailer1 = await msg.bot.forward_message(from_chat_id=user_id,chat_id=trailers_base_chat,message_id=trailer)
        trailer = trailer1.video.file_id
        trailer_msg_id = trailer1.message_id

        media_id = add_media_base(trailer,name,genre,tag,dub,msg_id=trailer_msg_id,type=type)
        await state.clear()
        await state.set_state(Admin.menu)

        await a.delete()
        await msg.answer(f"<b>✅Yangi media bazaga qo'shildi. \n🔗Media uchun link :</b> <code>https://t.me/{bot_username}?start={media_id}</code>",parse_mode=ParseMode.HTML,reply_markup=act_2_btn())

    else:
        a = await msg.answer("⚠️<b>Minimum 3 ta teg kiriting\n🔸<i>Na'muna: Spidermen, O'rgimchak odam, Marvel ...</i></b>",parse_mode=ParseMode.HTML)
        await asyncio.sleep(3)
        await a.delete()