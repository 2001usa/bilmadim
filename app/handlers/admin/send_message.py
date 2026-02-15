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
from aiogram.types.input_media_document import InputMediaDocument
from aiogram.types.input_media_animation import InputMediaAnimation
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

send_message_router = Router()

class Admin(StatesGroup):
    menu = State()

class Send_message(StatesGroup):
    type = State()
    type1 = State()
    type2 = State()


@send_message_router.message(ChatTypeFilter(chat_type=["group", "supergroup"]))
async def action(msg: Message, state: FSMContext):
    pass

@send_message_router.callback_query(F.data.startswith('c'),Send_message())
async def action(call: CallbackQuery, state: FSMContext):

    await state.clear()
    await state.set_state(Admin.menu)
    await call.message.answer("✅<b>Bekor qilindi</b>",parse_mode=ParseMode.HTML,reply_markup=act_2_btn())
    await call.message.delete()

@send_message_router.callback_query(F.data.startswith('s'),Send_message.type)
async def action(call: CallbackQuery, state: FSMContext):

    command = call.data.split(",")[1]

    if command == "type1":
        await state.set_state(Send_message.type1)
        a = await call.message.edit_text("<b>⚠️Yuborilishi kerak bo'lgan habarni kiriting:</b>",parse_mode=ParseMode.HTML,reply_markup=act_1_clbtn())
        await state.update_data(message_id = a.message_id)

    if command == "type2":
        await state.set_state(Send_message.type2)
        a = await call.message.edit_text("<b>⚠️Uztilishi kerak bo'lgan habarni kiriting:</b>",parse_mode=ParseMode.HTML,reply_markup=act_1_clbtn())
        await state.update_data(message_id = a.message_id)
        
@send_message_router.message(Send_message.type1)
async def action(msg: Message, state: FSMContext):

    data = await state.get_data()
    message_id = data.get("message_id")

    users = get_all_user_id_base()
    sended = 0
    not_sended = 0

    text = f"""
<b>✅Habar yuborilishi boshlandi</b>
<i>{len(users)} ta foydalanuvchiga yuboriladi⚠️</i>
"""

    await state.clear()
    await state.set_state(Admin.menu)
    await msg.answer(text,parse_mode=ParseMode.HTML,reply_markup=act_2_btn())

    try:
        await msg.bot.delete_message(chat_id=msg.from_user.id,message_id=message_id)
    except:
        pass

    if msg.media_group_id:
        media_group = []
        for media in msg.photo or msg.video or msg.document or msg.animation:
            if msg.photo:
                media_group.append(InputMediaPhoto(media.file_id, caption=msg.caption, caption_entities=msg.caption_entities))
            elif msg.video:
                media_group.append(InputMediaVideo(media.file_id, caption=msg.caption, caption_entities=msg.caption_entities))
            elif msg.document:
                media_group.append(InputMediaDocument(media.file_id, caption=msg.caption, caption_entities=msg.caption_entities))
            elif msg.animation:
                media_group.append(InputMediaAnimation(media.file_id, caption=msg.caption, caption_entities=msg.caption_entities))


    for i in users:
        user_id = i['user_id']
        random_sleep = random.randint(1, 3)

        try:
            if msg.text:
                await msg.bot.send_message(
                    chat_id=user_id,
                    text=msg.text,
                    entities=msg.entities,
                    reply_markup=msg.reply_markup
                )

            elif msg.photo:
                await msg.bot.send_photo(
                    chat_id=user_id,
                    photo=msg.photo[-1].file_id,  # Sends the highest resolution
                    caption=msg.caption,
                    caption_entities=msg.caption_entities,
                    reply_markup=msg.reply_markup
                )

            elif msg.video:
                await msg.bot.send_video(
                    chat_id=user_id,
                    video=msg.video.file_id,
                    caption=msg.caption,
                    caption_entities=msg.caption_entities,
                    reply_markup=msg.reply_markup
                )

            elif msg.audio:
                await msg.bot.send_audio(
                    chat_id=user_id,
                    audio=msg.audio.file_id,
                    caption=msg.caption,
                    caption_entities=msg.caption_entities,
                    reply_markup=msg.reply_markup
                )

            elif msg.voice:
                await msg.bot.send_voice(
                    chat_id=user_id,
                    voice=msg.voice.file_id,
                    caption=msg.caption,
                    caption_entities=msg.caption_entities,
                    reply_markup=msg.reply_markup
                )

            elif msg.document:
                await msg.bot.send_document(
                    chat_id=user_id,
                    document=msg.document.file_id,
                    caption=msg.caption,
                    caption_entities=msg.caption_entities,
                    reply_markup=msg.reply_markup
                )

            elif msg.animation:
                await msg.bot.send_animation(
                    chat_id=user_id,
                    animation=msg.animation.file_id,
                    caption=msg.caption,
                    caption_entities=msg.caption_entities,
                    reply_markup=msg.reply_markup
                )

            elif msg.sticker:
                await msg.bot.send_sticker(
                    chat_id=user_id,
                    sticker=msg.sticker.file_id
                )

            elif msg.poll:
                await msg.bot.send_poll(
                    chat_id=user_id,
                    question=msg.poll.question,
                    options=[option.text for option in msg.poll.options],
                    is_anonymous=msg.poll.is_anonymous
                )

            elif msg.media_group_id:
                await msg.bot.send_media_group(chat_id=user_id, media=media_group)


            sended += 1

        except:
            not_sended += 1

        await asyncio.sleep(random_sleep)


    text = f"""
<b>♻️Habar yuborib bo'lindi !</b>
<i>✅Yuborildi: {sended} ta</i>
<i>❌Yuborilmadi: {not_sended} ta</i>
"""
    await msg.answer(text,parse_mode=ParseMode.HTML)

@send_message_router.message(Send_message.type2)
async def action(msg: Message, state: FSMContext):

    data = await state.get_data()
    message_id = data.get("message_id")

    users = get_all_user_id_base()
    sended = 0
    not_sended = 0

    text = f"""
<b>✅Habar uzatilishi boshlandi</b>
<i>{len(users)} ta foydalanuvchiga yuboriladi⚠️</i>
"""
    
    try:
        await msg.bot.delete_message(chat_id=msg.from_user.id,message_id=message_id)
    except:
        pass

    await state.clear()
    await state.set_state(Admin.menu)
    await msg.answer(text,parse_mode=ParseMode.HTML,reply_markup=act_2_btn())

    for i in users:
        user_id = i['user_id']
        random_sleep = random.randint(3, 5)

        try:
            await msg.bot.forward_message(chat_id=user_id,from_chat_id=msg.from_user.id,message_id=msg.message_id)
            sended += 1
        except:
            not_sended += 1
        
        await asyncio.sleep(random_sleep)

    text = f"""
<b>♻️Habar yuborib bo'lindi !</b>
<i>✅Yuborildi: {sended} ta</i>
<i>❌Yuborilmadi: {not_sended} ta</i>
"""
    await msg.answer(text,parse_mode=ParseMode.HTML)