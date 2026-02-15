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

user_media_router = Router()

class User(StatesGroup):
    menu = State()
    search = State()

class Media(StatesGroup):
    menu = State()

@user_media_router.message(ChatTypeFilter(chat_type=["group", "supergroup"]))
async def action(msg: Message, state: FSMContext):
    pass

@user_media_router.callback_query(F.data.startswith('c'),Media.menu)
async def action(call: CallbackQuery, state: FSMContext):

    command = call.data.split(",")[1]
    user_id = call.from_user.id

    if command == "watch":
        media_id = int(call.data.split(",")[2])
        media_episodes = get_media_episodes_base(media_id)
        media = get_media_base(media_id)

        name = media["name"]
        first_episode = media_episodes[0]

        caption = f"""
<b>{name}</b>
<i>1 - qism</i>
"""

        await call.message.edit_media(
            media=InputMediaVideo(
                media=first_episode["episode_id"],
                caption=caption,
                parse_mode=ParseMode.HTML
            ),
            reply_markup=user_act_6_clbtn(media_episodes,0,first_episode["episode_num"],media_id)
        )

    elif command == "episode":
        episode_num = int(call.data.split(",")[2])
        page = int(call.data.split(",")[3])
        media_id = int(call.data.split(",")[4])

        media = get_media_base(media_id)
        name = media['name']

        media_episodes = get_media_episodes_base(media_id)
        episode = media_episodes[episode_num-1]

        caption = f"""
<b>{name}</b>
<i>{episode_num} - qism</i>
"""
        
        await call.message.answer_video(
            video=episode["episode_id"],
            caption=caption,
            parse_mode=ParseMode.HTML,
            reply_markup=user_act_6_clbtn(media_episodes,page,episode["episode_num"],media_id)
        )

        await call.message.edit_reply_markup(reply_markup=None)

    elif command == "next" or command == "previous":
        page = int(call.data.split(",")[2])
        episode_num = int(call.data.split(",")[4])
        media_id = int(call.data.split(",")[3])

        media_episodes = get_media_episodes_base(media_id)
        episode = media_episodes[episode_num-1]

        await call.message.edit_reply_markup(
            reply_markup=user_act_6_clbtn(media_episodes,page,episode["episode_num"],media_id)
        )

    elif command == "now":
        await call.answer(
            "📌Siz hozir shu qismni tomosha qilmoqdasiz !",
            show_alert=True
        )

    else:
        await state.set_state(User.menu)
        await call.message.edit_media(
            media=InputMediaPhoto(
                media="https://img1.teletype.in/files/8e/72/8e72aa35-c0c3-4c8a-a0e3-c3a56e81073f.jpeg",
                caption=act_1_lang(),parse_mode=ParseMode.HTML
            ),
            reply_markup=user_act_1_clbtn()
        )