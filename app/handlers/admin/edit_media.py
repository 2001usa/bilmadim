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

edit_media_router = Router()

class Admin(StatesGroup):
    menu = State()

class Edit_media(StatesGroup):
    search = State()
    menu = State()

    name = State()
    genre = State()
    tag = State()
    dub = State()

@edit_media_router.message(ChatTypeFilter(chat_type=["group", "supergroup"]))
async def action(msg: Message, state: FSMContext):
    pass

@edit_media_router.callback_query(F.data.startswith('c'),Edit_media())
async def action(call: CallbackQuery, state: FSMContext):

    await state.clear()
    await state.set_state(Admin.menu)
    await call.message.answer("✅<b>Bekor qilindi</b>",parse_mode=ParseMode.HTML,reply_markup=act_2_btn())
    await call.message.delete()

@edit_media_router.callback_query(F.data.startswith('s'),Edit_media.search)
async def action(call: CallbackQuery, state: FSMContext):

    command = call.data.split(",")[1]
    if command == "media":
        media_id = int(call.data.split(",")[2])
        media = get_media_base(media_id)

        trailer_id = media['trailer_id']
        name = media['name']
        genre = media['genre']
        tag = media['tag']
        dub = media['dub']
        status = media['status']
        series = int(media['series'])

        if status == "loading":
            status_text = "🔸OnGoing"
        elif status == "finished":
            status_text = "🔹Tugallangan"

        text = f"""
<i>🏷Anime:</i> <b>{name}</b>
-
<i>📚Janri:</i> <b>#{genre.replace(","," #")}</b>
-
<i>🔍Teg:</i> <b>{tag.replace(","," ,")}</b>
-
<i>🎙Ovoz berdi:</i> <b>{dub}</b>
-
<i>🎞Qismlar soni:</i> <b>{series} ta</b>
-
<i>🧮Status:</i> <b>{status_text}</b>
"""     
        await call.message.delete()
        await state.set_state(Edit_media.menu)
        a = await call.message.answer_video(video=trailer_id,caption=text,parse_mode=ParseMode.HTML,reply_markup=act_3_clbtn(status))
        await state.update_data(
            message_id = a.message_id,
            media_id = media_id
        )
    else:
        await state.clear()
        await state.set_state(Admin.menu)
        await call.message.answer("✅<b>Bekor qilindi</b>",parse_mode=ParseMode.HTML,reply_markup=act_2_btn())
        await call.message.delete()

@edit_media_router.message(F.text, Edit_media.search)
async def action(msg: Message, state: FSMContext):

    data = await state.get_data()
    message_id = data.get("message_id")
    await msg.bot.delete_message(chat_id=msg.chat.id,message_id=message_id)

    name = msg.text.strip()  # Remove extra spaces
    medias = search_media_base(name,"any")

    if medias:
        await msg.answer("<b>📚Kerakli mediani tanlang :</b>",parse_mode=ParseMode.HTML,reply_markup=act_2_clbtn(medias))
    else:
        a = await msg.answer(f"<i>'{name}'</i> <b>nomli media topilmadi☹️</b>",parse_mode=ParseMode.HTML,reply_markup=act_1_clbtn())
        await state.update_data(message_id = a.message_id)

@edit_media_router.callback_query(F.data.startswith('s'),Edit_media.menu)
async def action(call: CallbackQuery, state: FSMContext):

    command = call.data.split(",")[1]

    data = await state.get_data()
    media_id = data.get("media_id")

    if command == "back":
        await state.clear()
        await state.set_state(Admin.menu)
        await call.message.answer("✅<b>Bekor qilindi</b>",parse_mode=ParseMode.HTML,reply_markup=act_2_btn())
        await call.message.delete()
    
    else:
        command = call.data.split(",")[2]
        if command == "name":
            text = "<b>✏️Media uchun yangi nom yuboring</b>"
            await state.set_state(Edit_media.name)
            a = await call.message.edit_caption(caption=text,parse_mode=ParseMode.HTML,reply_markup=act_4_clbtn())
            await state.update_data(message_id = a.message_id)
        
        elif command == "genre":
            text = "<b>✏️Media uchun yangi janr yuboring</b>"
            await state.set_state(Edit_media.genre)
            a = await call.message.edit_caption(caption=text,parse_mode=ParseMode.HTML,reply_markup=act_4_clbtn())
            await state.update_data(message_id = a.message_id)

        elif command == "tag":
            text = "<b>✏️Media uchun yangi teg yuboring</b>"
            await state.set_state(Edit_media.tag)
            a = await call.message.edit_caption(caption=text,parse_mode=ParseMode.HTML,reply_markup=act_4_clbtn())
            await state.update_data(message_id = a.message_id)

        elif command == "dub":
            text = "<b>✏️Media uchun yangi ovoz beruvchini yuboring</b>"
            await state.set_state(Edit_media.dub)
            a = await call.message.edit_caption(caption=text,parse_mode=ParseMode.HTML,reply_markup=act_4_clbtn())
            await state.update_data(message_id = a.message_id)

        elif command == "delete":
            # Confirm deletion
            media = get_media_base(media_id)
            name = media['name']
            
            text = f"""
<b>⚠️ DIQQAT!</b>

<i>{name}</i> mediani va uning barcha qismlarini o'chirmoqchimisiz?

Bu amalni bekor qilib bo'lmaydi!
"""
            # Create confirmation keyboard
            from aiogram.utils.keyboard import InlineKeyboardBuilder
            confirm_btn = InlineKeyboardBuilder()
            confirm_btn.add(InlineKeyboardButton(text="✅Ha, o'chirish",callback_data=f"s,confirm_delete,{media_id}"))
            confirm_btn.add(InlineKeyboardButton(text="❌Yo'q, bekor qilish",callback_data="s,cancel_delete"))
            confirm_btn.adjust(1)
            
            await call.message.edit_caption(caption=text,parse_mode=ParseMode.HTML,reply_markup=confirm_btn.as_markup())

        elif command == "confirm_delete":
            media_id = int(call.data.split(",")[2])
            media = get_media_base(media_id)
            name = media['name']
            
            # Delete media and all episodes
            delete_media_base(media_id)
            
            await call.message.delete()
            await state.clear()
            await state.set_state(Admin.menu)
            await call.message.answer(f"<b>✅ '{name}' mediasi va barcha qismlari o'chirildi.</b>",parse_mode=ParseMode.HTML,reply_markup=act_2_btn())

        elif command == "cancel_delete":
            # Return to media view
            media = get_media_base(media_id)
            
            trailer_id = media['trailer_id']
            name = media['name']
            genre = media['genre']
            tag = media['tag']
            dub = media['dub']
            status = media['status']
            series = int(media['series'])

            if status == "loading":
                status_text = "🔸OnGoing"
            elif status == "finished":
                status_text = "🔹Tugallangan"

            text = f"""
<i>🏷Nom:</i> <b>{name}</b>
-
<i>📚Janri:</i> <b>#{genre.replace(","," #")}</b>
-
<i>🔍Teg:</i> <b>{tag.replace(","," ,")}</b>
-
<i>🎙Ovoz berdi:</i> <b>{dub}</b>
-
<i>🎞Qismlar soni:</i> <b>{series} ta</b>
-
<i>🧮Status:</i> <b>{status_text}</b>
"""
            await call.message.edit_caption(caption=text,parse_mode=ParseMode.HTML,reply_markup=act_3_clbtn(status))

        elif command == "status":
            command = call.data.split(",")[3]

            update_media_status_base(media_id,command)
            media = get_media_base(media_id)

            name = media['name']
            genre = media['genre']
            tag = media['tag']
            dub = media['dub']
            status = media['status']
            series = int(media['series'])

            if status == "loading":
                status_text = "🔸OnGoing"
            elif status == "finished":
                status_text = "🔹Tugallangan"

            text = f"""
<i>🏷Nom:</i> <b>{name}</b>
-
<i>📚Janri:</i> <b>#{genre.replace(","," #")}</b>
-
<i>🔍Teg:</i> <b>{tag.replace(","," ,")}</b>
-
<i>🎙Ovoz berdi:</i> <b>{dub}</b>
-
<i>🎞Qismlar soni:</i> <b>{series} ta</b>
-
<i>🧮Status:</i> <b>{status_text}</b>
        """     
            await state.set_state(Edit_media.menu)
            a = await call.message.edit_caption(caption=text,parse_mode=ParseMode.HTML,reply_markup=act_3_clbtn(status))
            await state.update_data(
                message_id = a.message_id,
                media_id = media_id
            )

@edit_media_router.callback_query(F.data.startswith('s'),Edit_media.dub)
async def action(call: CallbackQuery, state: FSMContext):

    data = await state.get_data()
    media_id = data.get("media_id")
    await state.clear()

    media = get_media_base(media_id)

    name = media['name']
    genre = media['genre']
    tag = media['tag']
    dub = media['dub']
    status = media['status']
    series = int(media['series'])

    if status == "loading":
        status_text = "🔸OnGoing"
    elif status == "finished":
        status_text = "🔹Tugallangan"

    text = f"""
<i>🏷Nom:</i> <b>{name}</b>
-
<i>📚Janri:</i> <b>#{genre.replace(","," #")}</b>
-
<i>🔍Teg:</i> <b>{tag.replace(","," ,")}</b>
-
<i>🎙Ovoz berdi:</i> <b>{dub}</b>
-
<i>🎞Qismlar soni:</i> <b>{series} ta</b>
-
<i>🧮Status:</i> <b>{status_text}</b>
"""     
    
    await state.set_state(Edit_media.menu)
    a = await call.message.edit_caption(caption=text,parse_mode=ParseMode.HTML,reply_markup=act_3_clbtn(status))
    await state.update_data(
        message_id = a.message_id,
        media_id = media_id
    )

@edit_media_router.message(F.text, Edit_media.dub)
async def action(msg: Message, state: FSMContext):

    dub = msg.text

    data = await state.get_data()
    media_id = data.get("media_id")
    message_id = data.get("message_id")

    update_media_dub_base(media_id,dub)

    await state.clear()

    media = get_media_base(media_id)

    name = media['name']
    genre = media['genre']
    tag = media['tag']
    dub = media['dub']
    status = media['status']
    series = int(media['series'])

    if status == "loading":
        status_text = "🔸OnGoing"
    elif status == "finished":
        status_text = "🔹Tugallangan"

    text = f"""
<i>🏷Nom:</i> <b>{name}</b>
-
<i>📚Janri:</i> <b>#{genre.replace(","," #")}</b>
-
<i>🔍Teg:</i> <b>{tag.replace(","," ,")}</b>
-
<i>🎙Ovoz berdi:</i> <b>{dub}</b>
-
<i>🎞Qismlar soni:</i> <b>{series} ta</b>
-
<i>🧮Status:</i> <b>{status_text}</b>
"""     
    await msg.delete()
    await state.set_state(Edit_media.menu)
    a = await msg.bot.edit_message_caption(caption=text,parse_mode=ParseMode.HTML,reply_markup=act_3_clbtn(status),message_id = message_id,chat_id=msg.chat.id)
    await state.update_data(
        message_id = a.message_id,
        media_id = media_id
    )

@edit_media_router.callback_query(F.data.startswith('s'),Edit_media.tag)
async def action(call: CallbackQuery, state: FSMContext):

    data = await state.get_data()
    media_id = data.get("media_id")
    await state.clear()

    media = get_media_base(media_id)

    name = media['name']
    genre = media['genre']
    tag = media['tag']
    dub = media['dub']
    status = media['status']
    series = int(media['series'])

    if status == "loading":
        status_text = "🔸OnGoing"
    elif status == "finished":
        status_text = "🔹Tugallangan"

    text = f"""
<i>🏷Nom:</i> <b>{name}</b>
-
<i>📚Janri:</i> <b>#{genre.replace(","," #")}</b>
-
<i>🔍Teg:</i> <b>{tag.replace(","," ,")}</b>
-
<i>🎙Ovoz berdi:</i> <b>{dub}</b>
-
<i>🎞Qismlar soni:</i> <b>{series} ta</b>
-
<i>🧮Status:</i> <b>{status_text}</b>
"""     
    
    await state.set_state(Edit_media.menu)
    a = await call.message.edit_caption(caption=text,parse_mode=ParseMode.HTML,reply_markup=act_3_clbtn(status))
    await state.update_data(
        message_id = a.message_id,
        media_id = media_id
    )

@edit_media_router.message(F.text, Edit_media.tag)
async def action(msg: Message, state: FSMContext):

    tag = msg.text
    if len(tag.split(",")) >= 3:

        data = await state.get_data()
        media_id = data.get("media_id")
        message_id = data.get("message_id")

        update_media_tag_base(media_id,tag)

        await state.clear()

        media = get_media_base(media_id)

        name = media['name']
        genre = media['genre']
        tag = media['tag']
        dub = media['dub']
        status = media['status']
        series = int(media['series'])

        if status == "loading":
            status_text = "🔸OnGoing"
        elif status == "finished":
            status_text = "🔹Tugallangan"

        text = f"""
<i>🏷Nom:</i> <b>{name}</b>
-
<i>📚Janri:</i> <b>#{genre.replace(","," #")}</b>
-
<i>🔍Teg:</i> <b>{tag.replace(","," ,")}</b>
-
<i>🎙Ovoz berdi:</i> <b>{dub}</b>
-
<i>🎞Qismlar soni:</i> <b>{series} ta</b>
-
<i>🧮Status:</i> <b>{status_text}</b>
"""     
        await msg.delete()
        await state.set_state(Edit_media.menu)
        a = await msg.bot.edit_message_caption(caption=text,parse_mode=ParseMode.HTML,reply_markup=act_3_clbtn(status),message_id = message_id,chat_id=msg.chat.id)
        await state.update_data(
            message_id = a.message_id,
            media_id = media_id
        )
    
    else:
        a = await msg.answer("⚠️<b>Minimum 3 ta teg kiriting</b>",parse_mode=ParseMode.HTML)
        await asyncio.sleep(3)
        await msg.delete()
        await a.delete()


@edit_media_router.callback_query(F.data.startswith('s'),Edit_media.genre)
async def action(call: CallbackQuery, state: FSMContext):

    data = await state.get_data()
    media_id = data.get("media_id")
    await state.clear()

    media = get_media_base(media_id)

    name = media['name']
    genre = media['genre']
    tag = media['tag']
    dub = media['dub']
    status = media['status']
    series = int(media['series'])

    if status == "loading":
        status_text = "🔸OnGoing"
    elif status == "finished":
        status_text = "🔹Tugallangan"

    text = f"""
<i>🏷Nom:</i> <b>{name}</b>
-
<i>📚Janri:</i> <b>#{genre.replace(","," #")}</b>
-
<i>🔍Teg:</i> <b>{tag.replace(","," ,")}</b>
-
<i>🎙Ovoz berdi:</i> <b>{dub}</b>
-
<i>🎞Qismlar soni:</i> <b>{series} ta</b>
-
<i>🧮Status:</i> <b>{status_text}</b>
"""     
    
    await state.set_state(Edit_media.menu)
    a = await call.message.edit_caption(caption=text,parse_mode=ParseMode.HTML,reply_markup=act_3_clbtn(status))
    await state.update_data(
        message_id = a.message_id,
        media_id = media_id
    )

@edit_media_router.message(F.text, Edit_media.genre)
async def action(msg: Message, state: FSMContext):

    genre = msg.text
    if len(genre.split(",")) >= 3:

        data = await state.get_data()
        media_id = data.get("media_id")
        message_id = data.get("message_id")

        update_media_genre_base(media_id,genre)

        await state.clear()

        media = get_media_base(media_id)

        name = media['name']
        genre = media['genre']
        tag = media['tag']
        dub = media['dub']
        status = media['status']
        series = int(media['series'])

        if status == "loading":
            status_text = "🔸OnGoing"
        elif status == "finished":
            status_text = "🔹Tugallangan"

        text = f"""
<i>🏷Nom:</i> <b>{name}</b>
-
<i>📚Janri:</i> <b>#{genre.replace(","," #")}</b>
-
<i>🔍Teg:</i> <b>{tag.replace(","," ,")}</b>
-
<i>🎙Ovoz berdi:</i> <b>{dub}</b>
-
<i>🎞Qismlar soni:</i> <b>{series} ta</b>
-
<i>🧮Status:</i> <b>{status_text}</b>
"""     
        await msg.delete()
        await state.set_state(Edit_media.menu)
        a = await msg.bot.edit_message_caption(caption=text,parse_mode=ParseMode.HTML,reply_markup=act_3_clbtn(status),message_id = message_id,chat_id=msg.chat.id)
        await state.update_data(
            message_id = a.message_id,
            media_id = media_id
        )
    
    else:
        a = await msg.answer("⚠️<b>Minimum 3 ta janr kiriting</b>",parse_mode=ParseMode.HTML)
        await asyncio.sleep(3)
        await msg.delete()
        await a.delete()

@edit_media_router.callback_query(F.data.startswith('s'),Edit_media.name)
async def action(call: CallbackQuery, state: FSMContext):

    data = await state.get_data()
    media_id = data.get("media_id")
    await state.clear()

    media = get_media_base(media_id)

    name = media['name']
    genre = media['genre']
    tag = media['tag']
    dub = media['dub']
    status = media['status']
    series = int(media['series'])

    if status == "loading":
        status_text = "🔸OnGoing"
    elif status == "finished":
        status_text = "🔹Tugallangan"

    text = f"""
<i>🏷Nom:</i> <b>{name}</b>
-
<i>📚Janri:</i> <b>#{genre.replace(","," #")}</b>
-
<i>🔍Teg:</i> <b>{tag.replace(","," ,")}</b>
-
<i>🎙Ovoz berdi:</i> <b>{dub}</b>
-
<i>🎞Qismlar soni:</i> <b>{series} ta</b>
-
<i>🧮Status:</i> <b>{status_text}</b>
"""     
    
    await state.set_state(Edit_media.menu)
    a = await call.message.edit_caption(caption=text,parse_mode=ParseMode.HTML,reply_markup=act_3_clbtn(status))
    await state.update_data(
        message_id = a.message_id,
        media_id = media_id
    )

@edit_media_router.message(F.text, Edit_media.name)
async def action(msg: Message, state: FSMContext):

    name = msg.text

    data = await state.get_data()
    media_id = data.get("media_id")
    message_id = data.get("message_id")

    update_media_name_base(media_id,name)

    await state.clear()

    media = get_media_base(media_id)

    name = media['name']
    genre = media['genre']
    tag = media['tag']
    dub = media['dub']
    status = media['status']
    series = int(media['series'])

    if status == "loading":
        status_text = "🔸OnGoing"
    elif status == "finished":
        status_text = "🔹Tugallangan"

    text = f"""
<i>🏷Nom:</i> <b>{name}</b>
-
<i>📚Janri:</i> <b>#{genre.replace(","," #")}</b>
-
<i>🔍Teg:</i> <b>{tag.replace(","," ,")}</b>
-
<i>🎙Ovoz berdi:</i> <b>{dub}</b>
-
<i>🎞Qismlar soni:</i> <b>{series} ta</b>
-
<i>🧮Status:</i> <b>{status_text}</b>
"""     
    await msg.delete()
    await state.set_state(Edit_media.menu)
    a = await msg.bot.edit_message_caption(caption=text,parse_mode=ParseMode.HTML,reply_markup=act_3_clbtn(status),message_id = message_id,chat_id=msg.chat.id)
    await state.update_data(
        message_id = a.message_id,
        media_id = media_id
    )