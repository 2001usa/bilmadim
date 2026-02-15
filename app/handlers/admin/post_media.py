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
from aiogram.types import FSInputFile
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

post_media_router = Router()

class Admin(StatesGroup):
    menu = State()

class Posting(StatesGroup):
    search = State()
    channel = State()
    check = State()

@post_media_router.message(ChatTypeFilter(chat_type=["group", "supergroup"]))
async def action(msg: Message, state: FSMContext):
    pass

@post_media_router.callback_query(F.data.startswith('c'),Posting())
async def action(call: CallbackQuery, state: FSMContext):

    await state.clear()
    await state.set_state(Admin.menu)
    await call.message.answer("✅<b>Bekor qilindi</b>",parse_mode=ParseMode.HTML,reply_markup=act_2_btn())
    await call.message.delete()

@post_media_router.message(F.text, Posting.search)
async def action(msg: Message, state: FSMContext):

    data = await state.get_data()
    message_id = data.get("message_id")
    await msg.bot.delete_message(chat_id=msg.chat.id,message_id=message_id)

    name = msg.text
    medias = search_media_base(name,"any")

    if medias:
        await msg.answer("<b>📚Kerakli mediani tanlang :</b>",parse_mode=ParseMode.HTML,reply_markup=act_2_clbtn(medias))
    else:
        a = await msg.answer(f"<i>'{name}'</i> <b>nomli media topilmadi☹️</b>",parse_mode=ParseMode.HTML,reply_markup=act_1_clbtn())
        await state.update_data(message_id = a.message_id)

@post_media_router.callback_query(F.data.startswith('s'),Posting.search)
async def action(call: CallbackQuery, state: FSMContext):

    command = call.data.split(",")[1]
    if command == "media":
        media_id = int(call.data.split(",")[2])
        media = get_media_base(media_id)

        trailer_id = media['trailer_id']
        name = media['name']
        status = media['status']
        series = int(media['series'])

        if status == "loading":
            status_text = "🔸OnGoing"
        elif status == "finished":
            status_text = "🔹Tugallangan"

        text = f"""
<i>🏷Nom:</i> <b>{name}</b>
-
<i>🎞Qismlar soni:</i> <b>{series} ta</b>
<i>🧮Status:</i> <b>{status_text}</b>
-
<i>⚠️Ushbu media post qilinishi kerak bo'lgan kanal nomidan istalgan habarni botga ulashing</i>
"""     
        await call.message.delete()
        await state.set_state(Posting.channel)
        a = await call.message.answer_video(video=trailer_id,caption=text,parse_mode=ParseMode.HTML,reply_markup=act_1_clbtn())
        await state.update_data(
            message_id = a.message_id,
            media_id = media_id,
            title = name
        )
    else:
        await state.clear()
        await state.set_state(Admin.menu)
        await call.message.answer("✅<b>Bekor qilindi</b>",parse_mode=ParseMode.HTML,reply_markup=act_2_btn())
        await call.message.delete()

@post_media_router.message(F.text, Posting.channel)
async def action(msg: Message, state: FSMContext):

    data = await state.get_data()
    message_id = data.get("message_id")
    title = data.get("title")

    if msg.forward_from_chat:
        chat_id = msg.forward_from_chat.id
        name = msg.forward_from_chat.title

        channel_data = await msg.bot.get_chat(chat_id)
        url = channel_data.invite_link

        if url:
            
            await state.set_state(Posting.check)
            text = f"<b>⚠️{title}</b> ni <a href='{url}'><b>{name}</b></a> kanaliga post qilishni tasdiqlayszmi ?"
            await msg.bot.edit_message_caption(
                chat_id=msg.from_user.id,
                message_id=message_id,
                caption=text,
                parse_mode=ParseMode.HTML,
                reply_markup=act_13_clbtn()
            )
            await state.update_data(channel_id = chat_id)
            await msg.delete()

        else:
            a = await msg.answer("<b>❌Bot kanalda admin qilinmagan</b>",parse_mode=ParseMode.HTML)
            await asyncio.sleep(3)
            await a.delete()

    else:
        a = await msg.answer("<b>❌Habar kanal nomidan yuborilmagan</b>",parse_mode=ParseMode.HTML)
        await asyncio.sleep(3)
        await a.delete()

@post_media_router.callback_query(F.data.startswith('s'),Posting.check)
async def action(call: CallbackQuery, state: FSMContext):

    command = call.data.split(",")[1]

    data = await state.get_data()
    media_id = data.get("media_id")
    channel_id = data.get("channel_id")

    if command == "yeah":
        media = get_media_base(media_id)

        trailer_id = media['trailer_id']
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
<i>🏷Nomi:</i> <b>{name}</b>
┌──────────
├<i>📚 Janr:</i>  <b>#{genre.replace(","," #")}</b>
├<i>🎙 Ovoz:</i>  <b>{dub}</b>
├<i>🎞 Qismlar:</i>  <b>{series} ta</b>
└──────────┐
<i>🧮Status:</i> <b>{status_text}</b>
"""     
        await call.bot.send_video(chat_id=channel_id,video=trailer_id,caption=text,parse_mode=ParseMode.HTML,reply_markup=act_11_clbtn(media_id))
        
        text = "✅<b>Muvaffaqiyatli post qilindi</b>"
        await state.clear()
        await state.set_state(Admin.menu)
        await call.message.answer(text,parse_mode=ParseMode.HTML,reply_markup=act_2_btn())
        await call.message.delete()

    elif command == "nope":
        await state.clear()
        await state.set_state(Admin.menu)
        await call.message.answer("✅<b>Bekor qilindi</b>",parse_mode=ParseMode.HTML,reply_markup=act_2_btn())
        await call.message.delete()