from aiogram import F,Router
from aiogram.types import Message,CallbackQuery,InlineQuery,InlineQueryResultArticle
from aiogram.types.input_text_message_content import InputTextMessageContent
from aiogram.types.link_preview_options import LinkPreviewOptions
from aiogram.filters import CommandStart,Command
from aiogram.fsm.state import StatesGroup,State
from aiogram.fsm.context import FSMContext
from app.database.bot_base import *
from app.funcs.languages import *
from app.keyboards.user.inline_buttons import *
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

from config import trailers_base_chat, series_base_chat

user_search_drama_router = Router()

class User(StatesGroup):
    menu = State()

class Drama(StatesGroup):
    search = State()
    menu = State()

class Media(StatesGroup):
    menu = State()

@user_search_drama_router.message(ChatTypeFilter(chat_type=["group", "supergroup"]))
async def action(msg: Message, state: FSMContext):
    pass

@user_search_drama_router.callback_query(F.data.startswith('c'),Drama.search)
async def action(call: CallbackQuery, state: FSMContext):

    await state.set_state(User.menu)
    try:
        await call.message.edit_caption(caption=act_1_lang(),reply_markup=user_act_1_clbtn(),parse_mode=ParseMode.HTML)
    except:
        await call.message.answer_photo(photo="https://img1.teletype.in/files/8e/72/8e72aa35-c0c3-4c8a-a0e3-c3a56e81073f.jpeg",caption=act_1_lang(),reply_markup=user_act_1_clbtn(),parse_mode=ParseMode.HTML)
        await call.message.delete()
            
@user_search_drama_router.callback_query(F.data.startswith('s'),Drama.search)
async def action(call: CallbackQuery, state: FSMContext):

    command = call.data.split(",")[1]
    user_id = call.from_user.id

    if command == "media":
        media_id = int(call.data.split(",")[2])
        media = get_media_base(media_id)
        
        trailer_id = media["trailer_id"]
        msg_id = media.get("msg_id")

        name = media['name']
        genre = media['genre']
        dub = media['dub']
        status = media['status']
        series = int(media['series'])

        if status == "loading":
            status_text = "🔸OnGoing"
        elif status == "finished":
            status_text = "🔹Tugallangan"

        text = f"""
<b>{name}</b>
╭──────────────────────
├‣  <b>Qism:</b> {series} ta
├‣  <b>Holati:</b> {status_text}
├‣  <b>Ovoz:</b> {dub}
├‣  <b>Janrlari:</b> {genre.replace(","," , ")}
╰─────────────
"""
        await state.set_state(Media.menu)
        await call.message.delete()

        if msg_id:
            try:
                await call.message.bot.copy_message(
                    chat_id=user_id,
                    from_chat_id=trailers_base_chat,
                    message_id=msg_id,
                    caption=text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=user_act_5_clbtn(series, media_id)
                )
            except Exception as e:
                # Fallback if copy_message fails (e.g. message deleted in channel)
                try:
                    await call.message.answer_video(video=trailer_id,caption=text,parse_mode=ParseMode.HTML,reply_markup=user_act_5_clbtn(series,media_id))
                except:
                     await call.message.answer_photo(photo=trailer_id,caption=text,parse_mode=ParseMode.HTML,reply_markup=user_act_5_clbtn(series,media_id))
        else:
            try:
                await call.message.answer_video(video=trailer_id,caption=text,parse_mode=ParseMode.HTML,reply_markup=user_act_5_clbtn(series,media_id))
            except:
                await call.message.answer_photo(photo=trailer_id,caption=text,parse_mode=ParseMode.HTML,reply_markup=user_act_5_clbtn(series,media_id))

    else:
        await state.clear()
        await state.set_state(User.menu)
        await call.message.answer_photo(photo="https://img1.teletype.in/files/8e/72/8e72aa35-c0c3-4c8a-a0e3-c3a56e81073f.jpeg",caption=act_1_lang(),reply_markup=user_act_1_clbtn(),parse_mode=ParseMode.HTML)
        await call.message.delete()

@user_search_drama_router.message(F.text, Drama.search)
async def action(msg: Message, state: FSMContext):

    data = await state.get_data()
    message_id = data.get("message_id")
    try:
        await msg.bot.delete_message(chat_id=msg.chat.id,message_id=message_id)
    except:
        pass

    name = msg.text
    medias = search_media_base(name,"drama")

    if medias:
        await msg.answer("<b>📚Kerakli animeni tanlang :</b>",parse_mode=ParseMode.HTML,reply_markup=user_act_3_clbtn(medias))
    else:
        a =await msg.answer(f"<i>'{name}'</i> <b>nomli drama topilmadi☹️</b>",parse_mode=ParseMode.HTML,reply_markup=user_act_4_clbtn())
        await state.update_data(message_id = a.message_id)
        