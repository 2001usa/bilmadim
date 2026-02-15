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

edit_episode_router = Router()

class Admin(StatesGroup):
    menu = State()

class Edit_episode(StatesGroup):
    search = State()
    menu =State()
    edit = State()
    replace = State()

@edit_episode_router.message(ChatTypeFilter(chat_type=["group", "supergroup"]))
async def action(msg: Message, state: FSMContext):
    pass

@edit_episode_router.callback_query(F.data.startswith('c'),Edit_episode())
async def action(call: CallbackQuery, state: FSMContext):

    await state.clear()
    await state.set_state(Admin.menu)
    await call.message.answer("✅<b>Bekor qilindi</b>",parse_mode=ParseMode.HTML,reply_markup=act_2_btn())
    await call.message.delete()

@edit_episode_router.callback_query(F.data.startswith('s'),Edit_episode.search)
async def action(call: CallbackQuery, state: FSMContext):

    command = call.data.split(",")[1]
    if command == "media":
        media_id = int(call.data.split(",")[2])
        episodes = get_media_episodes_base(media_id=media_id)
        if episodes:
            await state.set_state(Edit_episode.menu)
            await call.message.delete()

            name = get_media_base(media_id)['name']
            episode_1 = episodes[0]
            episode_id = episode_1['episode_id']

            text = f"""
✏️<b>{name}</b>

<i>1 - qism</i>
"""
            a = await call.message.answer_video(video=episode_id,caption=text,parse_mode=ParseMode.HTML,reply_markup=act_5_clbtn(episodes,1))
            await state.update_data(
                message_id = a.message_id,
                name = name,
                media_id = media_id
            )

        else:
            text = "☹️Ushbu mediada hali qismlar mavjud emas"
            await call.answer(text,show_alert=True)

    else:
        await state.clear()
        await state.set_state(Admin.menu)
        await call.message.answer("✅<b>Bekor qilindi</b>",parse_mode=ParseMode.HTML,reply_markup=act_2_btn())
        await call.message.delete()

@edit_episode_router.message(F.text, Edit_episode.search)
async def action(msg: Message, state: FSMContext):

    data = await state.get_data()
    message_id = data.get("message_id")
    await msg.bot.delete_message(chat_id=msg.chat.id,message_id=message_id)

    name = msg.text
    medias = search_media_base(name,"media")

    if medias:
        await msg.answer("<b>📚Kerakli mediani tanlang :</b>",parse_mode=ParseMode.HTML,reply_markup=act_2_clbtn(medias))
    else:
        a = await msg.answer(f"<i>'{name}'</i> <b>nomli media topilmadi☹️</b>",parse_mode=ParseMode.HTML,reply_markup=act_1_clbtn())
        await state.update_data(message_id = a.message_id)

@edit_episode_router.callback_query(F.data.startswith('s'),Edit_episode.menu)
async def action(call: CallbackQuery, state: FSMContext):

    command = call.data.split(",")[1]

    data = await state.get_data()
    name = data.get("name")

    if command == "select":

        media_id = int(call.data.split(",")[2].split("-")[0])
        episode_num = int(call.data.split(",")[2].split("-")[1])

        episodes = get_media_episodes_base(media_id)
        episode = episodes[episode_num-1]
        episode_id = episode['episode_id']

        text = f"""
✏️<b>{name}</b>

<i>{episode_num} - qism</i>
"""

        a = await call.message.edit_media(media=InputMediaVideo(media=episode_id,caption=text,parse_mode=ParseMode.HTML),reply_markup=act_5_clbtn(episodes,episode_num))
        await state.update_data(message_id = a.message_id)

    elif command == "edit":
        media_id = int(call.data.split(",")[2].split("-")[0])
        episode_num = int(call.data.split(",")[2].split("-")[1])

        await state.set_state(Edit_episode.edit)

        episodes = get_media_episodes_base(media_id)
        episode = episodes[episode_num-1]
        episode_id = episode['episode_id']

        text = f"""
✏️<b>{name}</b>

<i>{episode_num} - qism</i>
"""

        if episodes[-1]['episode_num'] == episode_num:
            is_last_episode = True
        else:
            is_last_episode = False

        a = await call.message.edit_media(media=InputMediaVideo(media=episode_id,caption=text,parse_mode=ParseMode.HTML),reply_markup=act_6_clbtn(is_last_episode,episode))
        await state.update_data(message_id = a.message_id)

    elif command == "back":
        await state.clear()
        await state.set_state(Admin.menu)
        await call.message.answer("✅<b>Bekor qilindi</b>",parse_mode=ParseMode.HTML,reply_markup=act_2_btn())
        await call.message.delete()

@edit_episode_router.callback_query(F.data.startswith('s'),Edit_episode.edit)
async def action(call: CallbackQuery, state: FSMContext):

    command = call.data.split(",")[1]
    
    media_id = int(call.data.split(",")[2].split("-")[0])

    data = await state.get_data()
    name = data.get("name")
    
    if command == "back":

        episodes = get_media_episodes_base(media_id)
        episode_id = episodes[0]['episode_id']

        text = f"""
✏️<b>{name}</b>

<i>1 - qism</i>
"""     
        await state.set_state(Edit_episode.menu)
        a = await call.message.edit_media(media=InputMediaVideo(media=episode_id,caption=text,parse_mode=ParseMode.HTML),reply_markup=act_5_clbtn(episodes,1))
        await state.update_data(message_id = a.message_id)

    elif command == "delete":
        # Show confirmation dialog
        episode_num = int(call.data.split(",")[2].split("-")[1])
        
        text = f"""
<b>⚠️ DIQQAT!</b>

<i>{name}</i> mediasining <b>{episode_num} - qismini</b> o'chirmoqchimisiz?

Bu amalni bekor qilib bo'lmaydi!
"""
        # Create confirmation keyboard
        from aiogram.utils.keyboard import InlineKeyboardBuilder
        confirm_btn = InlineKeyboardBuilder()
        confirm_btn.add(InlineKeyboardButton(text="✅Ha, o'chirish",callback_data=f"s,confirm_delete,{media_id}-{episode_num}"))
        confirm_btn.add(InlineKeyboardButton(text="❌Yo'q, bekor qilish",callback_data=f"s,cancel_delete,{media_id}-{episode_num}"))
        confirm_btn.adjust(1)
        
        await call.message.edit_caption(caption=text,parse_mode=ParseMode.HTML,reply_markup=confirm_btn.as_markup())

    elif command == "confirm_delete":
        episode_num = int(call.data.split(",")[2].split("-")[1])
        episodes = get_media_episodes_base(media_id)
        
        # Delete the episode
        delete_episode_base(media_id,episode_num)
        update_media_episodes_count_minus_base(media_id)
        
        # Renumber remaining episodes that come after the deleted one
        for ep in episodes:
            if ep['episode_num'] > episode_num:
                cursor.execute(f"""UPDATE episodes SET episode_num = {ep['episode_num'] - 1} WHERE which_media = {media_id} AND episode_num = {ep['episode_num']}""")
        conn.commit()
        
        await call.answer(f"{episode_num} - qism o'chirildi✅",show_alert=True)

        # Get updated episodes list
        episodes = get_media_episodes_base(media_id)
        
        if len(episodes) != 0:
            # Show the episode at the same position, or the last one if we deleted the last episode
            if episode_num <= len(episodes):
                episode = episodes[episode_num - 1]
            else:
                episode = episodes[-1]
            
            episode_id = episode['episode_id']

            await state.set_state(Edit_episode.menu)
            text = f"""
✏️<b>{name}</b>

<i>{episode['episode_num']} - qism</i>
"""
            a = await call.message.edit_media(media=InputMediaVideo(media=episode_id,caption=text,parse_mode=ParseMode.HTML),reply_markup=act_5_clbtn(episodes,episode['episode_num']))
            await state.update_data(message_id = a.message_id, name = name, media_id = media_id)

        else:
            await call.message.delete()
            await state.clear()
            await state.set_state(Admin.menu)
            await call.message.answer("<b>✅Barcha qismlar o'chirildi. Media hali mavjud.</b>",parse_mode=ParseMode.HTML,reply_markup=act_2_btn())

    elif command == "cancel_delete":
        # Return to episode edit view
        episode_num = int(call.data.split(",")[2].split("-")[1])
        episodes = get_media_episodes_base(media_id)
        episode = episodes[episode_num-1]
        episode_id = episode['episode_id']

        text = f"""
✏️<b>{name}</b>

<i>{episode_num} - qism</i>
"""
        if episodes[-1]['episode_num'] == episode_num:
            is_last_episode = True
        else:
            is_last_episode = False

        await call.message.edit_media(media=InputMediaVideo(media=episode_id,caption=text,parse_mode=ParseMode.HTML),reply_markup=act_6_clbtn(is_last_episode,episode))

    elif command == "replace":
        episode_num = int(call.data.split(",")[2].split("-")[1])
        episodes = get_media_episodes_base(media_id)

        episode = episodes[episode_num-1]
        episode_id = episode['episode_id']

        await state.set_state(Edit_episode.replace)

        text = f"""
✏️<b>{name}</b>

<i>♻️{episode_num} - qism uchun boshqa qism yuboring</i>
"""
        a = await call.message.edit_caption(caption=text,parse_mode=ParseMode.HTML,reply_markup=act_4_clbtn())
        await state.update_data(
            message_id = a.message_id,
            media_id = media_id,
            episode_num = episode_num
        )

@edit_episode_router.callback_query(F.data.startswith('s'),Edit_episode.replace)
async def action(call: CallbackQuery, state: FSMContext):

    data = await state.get_data()
    name = data.get("name")
    media_id = data.get("media_id")

    episodes = get_media_episodes_base(media_id)
    episode_id = episodes[0]['episode_id']

    text = f"""
✏️<b>{name}</b>

<i>1 - qism</i>
""" 
    await state.clear()
    await state.set_state(Edit_episode.menu)
    a = await call.message.edit_media(media=InputMediaVideo(media=episode_id,caption=text,parse_mode=ParseMode.HTML),reply_markup=act_5_clbtn(episodes,1))
    await state.update_data(message_id = a.message_id, name = name, media_id = media_id)

@edit_episode_router.message(F.video, Edit_episode.replace)
async def action(msg: Message, state: FSMContext):

    data = await state.get_data()
    name = data.get("name")
    media_id = data.get("media_id")
    message_id = data.get("message_id")
    episode_num = data.get("episode_num")

    new_episode = await msg.bot.forward_message(chat_id=series_base_chat,from_chat_id=msg.chat.id,message_id=msg.message_id)
    new_episode_id = new_episode.video.file_id

    update_episode_base(media_id,episode_num,new_episode_id)

    await msg.delete()

    episodes = get_media_episodes_base(media_id)
    # episodes index starts at 0, episode_num starts at 1
    # Check if episode_num corresponds to a valid index
    if 0 < episode_num <= len(episodes):
        episode = episodes[episode_num - 1]
    else:
        episode = episodes[0] # Fallback
    
    episode_id = episode['episode_id']

    text = f"""
✏️<b>{name}</b>

<i>{episode['episode_num']} - qism</i>
"""
    await state.set_state(Edit_episode.menu)
    a = await msg.bot.edit_message_media(media=InputMediaVideo(media=episode_id,caption=text,parse_mode=ParseMode.HTML),reply_markup=act_5_clbtn(episodes,episode['episode_num']),chat_id=msg.chat.id,message_id=message_id)
    await state.update_data(message_id = a.message_id, name = name)
   
