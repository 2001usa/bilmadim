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

add_episode_router = Router()

class Admin(StatesGroup):
    menu = State()

class Add_episode(StatesGroup):
    search = State()
    send_serie = State()

@add_episode_router.message(ChatTypeFilter(chat_type=["group", "supergroup"]))
async def action(msg: Message, state: FSMContext):
    pass

@add_episode_router.callback_query(F.data.startswith('c'),Add_episode())
async def action(call: CallbackQuery, state: FSMContext):

    await state.clear()
    await state.set_state(Admin.menu)
    await call.message.answer("✅<b>Bekor qilindi</b>",parse_mode=ParseMode.HTML,reply_markup=act_2_btn())
    await call.message.delete()

@add_episode_router.callback_query(F.data.startswith('s'),Add_episode.search)
async def action(call: CallbackQuery, state: FSMContext):

    command = call.data.split(",")[1]
    if command == "media":
        media_id = int(call.data.split(",")[2])
        media = get_media_base(media_id)

        trailer_id = media['trailer_id']
        name = media['name']
        series = int(media['series'])

        text = f"""
<i>{name}</i> 
⚠️mediasi uchun {series + 1} - qismni yuboring
"""     
        await state.set_state(Add_episode.send_serie)
        await call.message.delete()
        a = await call.message.answer_video(video=trailer_id,caption=text,parse_mode=ParseMode.HTML,reply_markup=act_1_clbtn())
        await state.update_data(
            message_id = a.message_id,
            media_id = media_id,
            episode_num = series + 1,
            name = name,
            trailer_id = trailer_id
        )

    else:
        await state.clear()
        await state.set_state(Admin.menu)
        await call.message.answer("✅<b>Bekor qilindi</b>",parse_mode=ParseMode.HTML,reply_markup=act_2_btn())
        await call.message.delete()

@add_episode_router.message(F.text, Add_episode.search)
async def action(msg: Message, state: FSMContext):

    data = await state.get_data()
    message_id = data.get("message_id")
    await msg.bot.delete_message(chat_id=msg.chat.id,message_id=message_id)

    name = msg.text
    medias = search_media_base(name,"any")

    if medias:
        await msg.answer("<b>📚Kerakli mediani tanlang :</b>",parse_mode=ParseMode.HTML,reply_markup=act_2_clbtn(medias))
    else:
        a =await msg.answer(f"<i>'{name}'</i> <b>nomli media topilmadi☹️</b>",parse_mode=ParseMode.HTML,reply_markup=act_1_clbtn())
        await state.update_data(message_id = a.message_id)
        
@add_episode_router.message(F.video, Add_episode.send_serie)
async def action(msg: Message, state: FSMContext):

    data = await state.get_data()
    message_id = data.get("message_id")
    media_id = data.get("media_id")
    episode_num = data.get("episode_num")
    name = data.get("name")
    trailer_id = data.get("trailer_id")

    # Update state FIRST to prevent race condition with multiple uploads
    await state.update_data(
        episode_num = episode_num + 1
    )

    episode = await msg.bot.forward_message(chat_id=series_base_chat,message_id=msg.message_id,from_chat_id=msg.chat.id)
    episode_id = episode.video.file_id
    msg_id = episode.message_id

    add_episode_base(media_id,episode_id,episode_num,msg_id)
    update_media_episodes_count_plus_base(media_id)

    # Send simple confirmation
    await msg.reply(f"<b>✅ {episode_num}-qism saqlandi.</b>\n👇KEYINGI qismni yuboring yoki /theend buyrug'ini yuboring.", parse_mode=ParseMode.HTML)

@add_episode_router.message(F.text == "/theend", Add_episode.send_serie)
async def finish_upload(msg: Message, state: FSMContext):
    """Handler for /theend command to finish episode upload"""
    
    data = await state.get_data()
    media_id = data.get("media_id")
    name = data.get("name")
    episode_num = data.get("episode_num")
    
    # episode_num is already incremented, so actual last episode is episode_num - 1
    total_episodes = episode_num - 1
    
    await state.clear()
    await state.set_state(Admin.menu)
    
    text = f"""
<b>✅ Yuklash tugallandi!</b>

<i>{name}</i> uchun jami <b>{total_episodes} ta qism</b> yuklandi.
"""
    
    await msg.answer(text, parse_mode=ParseMode.HTML, reply_markup=act_2_btn())
