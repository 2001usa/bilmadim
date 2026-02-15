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

trailers_base_chat = -1001990975355
series_base_chat = -1002076256295

user_search_by_id_router = Router()

class User(StatesGroup):
    menu = State()

class SeatchById(StatesGroup):
    search = State()
    menu = State()

class Media(StatesGroup):
    menu = State()

@user_search_by_id_router.message(ChatTypeFilter(chat_type=["group", "supergroup"]))
async def action(msg: Message, state: FSMContext):
    pass

@user_search_by_id_router.callback_query(F.data.startswith('c'),SeatchById.search)
async def action(call: CallbackQuery, state: FSMContext):

    await state.set_state(User.menu)
    try:
        await call.message.edit_caption(caption=act_1_lang(),reply_markup=user_act_1_clbtn(),parse_mode=ParseMode.HTML)
    except:
        await call.message.answer_photo(photo="https://img1.teletype.in/files/8e/72/8e72aa35-c0c3-4c8a-a0e3-c3a56e81073f.jpeg",caption=act_1_lang(),reply_markup=user_act_1_clbtn(),parse_mode=ParseMode.HTML)
        await call.message.delete()

@user_search_by_id_router.message(F.text, SeatchById.search)
async def action(msg: Message, state: FSMContext):

    user_id = msg.from_user.id
    media_id = msg.text

    if media_id.isdigit():
        media = get_media_base(media_id)

        if media:
        
            trailer_id = media["trailer_id"]

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

            try:
                await msg.answer_video(video=trailer_id,caption=text,parse_mode=ParseMode.HTML,reply_markup=user_act_5_clbtn(series,media_id))
            except:
                await msg.answer_photo(photo=trailer_id,caption=text,parse_mode=ParseMode.HTML,reply_markup=user_act_5_clbtn(series,media_id))

        else:
            a = await msg.answer("<b>⚠️Siz yuborgan kodga tegishli hech narsa topilmadi. Qayta urinib ko'ring</b>",parse_mode=ParseMode.HTML)
            await asyncio.sleep(3)
            await a.delete()
            await msg.delete()

    else:
        a = await msg.answer("<b>⚠️Iltimos kod yuboring</b>",parse_mode=ParseMode.HTML)
        await asyncio.sleep(3)
        await a.delete()
        await msg.delete()

        