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
from aiogram.types import FSInputFile
from app.keyboards.admin.keyboard_buttons import *
from aiogram.types import ChatJoinRequest
import io
from aiogram.types import InputFile
import re
import aiohttp
from config import ads_manager_username, admin_ids

router = Router()

class User(StatesGroup):
    menu = State()
    search_by_image = State()

class Admin(StatesGroup):
    menu = State()

class Drama(StatesGroup):
    search = State()

class Anime(StatesGroup):
    search = State()

class Media(StatesGroup):
    menu = State()

class SeatchById(StatesGroup):
    search = State()

@router.message(ChatTypeFilter(chat_type=["group", "supergroup"]))
async def action(msg: Message, state: FSMContext):
    pass

@router.message(Command("admin"))
async def admin_command(msg: Message, state: FSMContext):

    user_id = msg.from_user.id
    user = get_user_base(user_id)

    if user_id in admin_ids or user["is_admin"] == True or user["is_staff"] == True:
        await state.clear()
        
        if user_id in admin_ids and user["is_admin"] == False:
             update_user_admin_base(user_id,True)

        await state.set_state(Admin.menu)
        await msg.answer(act_2_lang(),reply_markup=act_2_btn(),parse_mode=ParseMode.HTML)
    else:
        await state.clear()
        await state.set_state(Admin.menu)
        await msg.answer("❌",reply_markup=act_1_btn())

@router.message(Command("panel"))
async def panel_command(msg: Message, state: FSMContext):

    user_id = msg.from_user.id
    user = get_user_base(user_id)

    if user_id in admin_ids or user["is_admin"] == True or user["is_staff"] == True:
        await state.clear()
        
        if user_id in admin_ids and user["is_admin"] == False:
             update_user_admin_base(user_id,True)

        await state.set_state(Admin.menu)
        await msg.answer(act_2_lang(),reply_markup=act_2_btn(),parse_mode=ParseMode.HTML)
    else:
        await state.clear()
        await state.set_state(Admin.menu)
        await msg.answer("❌",reply_markup=act_1_btn())

@router.message(Command("start"))
async def action(msg: Message, state: FSMContext):

    await state.clear()
    await state.set_state(User.menu)

    user_id = msg.from_user.id
    username = msg.from_user.username

    user = get_user_base(user_id)
    if not user:
        # Agar foydalanuvchi admin_ids ro'yxatida bo'lsa, avtomatik admin qilamiz
        is_admin = user_id in admin_ids
        add_user_base(user_id, username, is_admin=is_admin)
    else:
        # Update existing user if they are in config admin_ids but not admin in DB
        if user_id in admin_ids and user["is_admin"] == False:
             update_user_admin_base(user_id,True)

    sponsors = get_all_sponsors_base()
    not_sub_channels = await check_user_subscribes(sponsors,msg)

    if not_sub_channels:
        await msg.answer(
            "<b>⚠️Avval homiy kanallarga a'zo bo'ling</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=user_act_7_clbtn(not_sub_channels)
        )
    
    else:
        command = msg.text.replace(" ","").split("/start")[1]

        if command.isdigit():
            media_id = int(command)

            media = get_media_base(media_id)
        
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
            await msg.delete()

            try:
                await msg.answer_video(video=trailer_id,caption=text,parse_mode=ParseMode.HTML,reply_markup=user_act_5_clbtn(series,media_id))
            except:
                await msg.answer_photo(photo=trailer_id,caption=text,parse_mode=ParseMode.HTML,reply_markup=user_act_5_clbtn(series,media_id))


        elif "serie" in command.lower():
            media_id = int(command.replace("serie",""))

            media_episodes = get_media_episodes_base(media_id)
            media = get_media_base(media_id)

            name = media["name"]
            last_episode = media_episodes[-1]

            page = (last_episode['episode_num'] - 1) // 20 

            caption = f"""
<b>{name}</b>
<i>{last_episode['episode_num']} - qism</i>
"""         
            await state.set_state(Media.menu)
            await msg.answer_video(
                video=last_episode["episode_id"],
                caption=caption,
                parse_mode=ParseMode.HTML,
                reply_markup=user_act_6_clbtn(media_episodes,page,last_episode["episode_num"],media_id)
            )

        else:
            await msg.answer_photo(photo="https://img1.teletype.in/files/8e/72/8e72aa35-c0c3-4c8a-a0e3-c3a56e81073f.jpeg",caption=act_1_lang(),reply_markup=user_act_1_clbtn(),parse_mode=ParseMode.HTML)

@router.callback_query(F.data.startswith('s'),User.menu)
async def action(call: CallbackQuery, state: FSMContext):

    await call.message.edit_caption(caption=act_1_lang(),reply_markup=user_act_1_clbtn(),parse_mode=ParseMode.HTML)

@router.callback_query(F.data.startswith('c'),User.menu)
async def action(call: CallbackQuery, state: FSMContext):

    command = call.data.split(",")[1]

    if command == "search_id":
        text = "<b>🔍Qidirilishi kerak bo'lgan anime yoki drama kodini yuboring</b>"

        await state.set_state(SeatchById.search)

        a = await call.message.edit_caption(
            caption=text,
            parse_mode=ParseMode.HTML,
            reply_markup=user_act_2_clbtn()
        )
        await state.update_data(
            message_id = a.message_id
        )

    if command == "search_anime":
        text = "<b>🔍Qidirilishi kerak bo'lgan anime nomini yuboring</b>"

        await state.set_state(Anime.search)

        a = await call.message.edit_caption(
            caption=text,
            parse_mode=ParseMode.HTML,
            reply_markup=user_act_2_clbtn()
        )
        await state.update_data(
            message_id = a.message_id
        )

    elif command == "search_drama":
        text = "<b>🔍Qidirilishi kerak bo'lgan drama nomini yuboring</b>"

        await state.set_state(Drama.search)

        a = await call.message.edit_caption(
            caption=text,
            parse_mode=ParseMode.HTML,
            reply_markup=user_act_2_clbtn()
        )
        await state.update_data(
            message_id = a.message_id
        )

    elif command == "image":
        
        text = """
<b>🔍Qidirilishi kerak bo'gan anime rasmni kiriting</b>
-
<b>‼️DIQQAT</b>
<i>Yaxshi natija olish uchun animeni videosidan skrinshot olib aynan anime tasvirlangan joyini qirqib keyin rasmni yuboring. Animening Posteri yoki Bannerlarini yubormang !</i>
"""

        await state.set_state(User.search_by_image)

        a = await call.message.edit_caption(
            caption=text,
            parse_mode=ParseMode.HTML,
            reply_markup=user_act_2_clbtn()
        )
        await state.update_data(message_id = a.message_id)

    elif command == "manual":
        text = f"""
📚<b>Botini ishlatish bo'yicha qo'llanma : </b>
-
🔍<b>Anime Qidirish</b> - Botda mavjud bo'lgan animelarni qidirish uchun ishlatiladi. 
🔍<b>Drama Qidirish</b> - Botda mavjud bo'lgan dramalarni qidirish uchun ishlatiladi. 
💸<b>Reklama</b>  - bot adminlari bilan reklama yoki homiylik yuzasidan aloqaga chiqish.
🏙<b>Rasm Orqali Anime Qidiruv</b>  - Nomini topa olmayotgan animeingizni rasm orqali topib beradi.
📓<b>Ro'yxat</b>  - Botga joylangan Anime va Dramalar ro'yhati.
-
🆔Botdagi ID ingiz : <code>{call.from_user.id}</code>
"""
        await call.message.edit_caption(
            caption=text,
            parse_mode=ParseMode.HTML,
            reply_markup=user_act_8_clbtn()
        )

    elif command == "sponsorship":
        text = f"""
<b>📌Reklama va homiylik masalasida admin bilan bog'laning</b>
💬<i>Batafsil: @{ads_manager_username}</i>
"""
        await call.message.edit_caption(
            caption=text,
            parse_mode=ParseMode.HTML,
            reply_markup=user_act_8_clbtn()
        )

    elif command == "list":

        medias = get_all_media_base("anime")
        with open("Animelar_royxati.txt", "w", encoding="utf-8") as file:
            file.write("Botidagi barcha Animelar ro'yxati:\n\n")
            num = 0
            for i in medias:
                num += 1
                text = f"""
----  {num}  ----
Nomi : [ {i['name']} ]
Janri : {i['genre'].replace(","," ")}"""
                file.write(text)

        document = FSInputFile("Animelar_royxati.txt")
        await call.message.answer_document(document,caption="🔸<b>Animelar ro'yxati</b>",parse_mode=ParseMode.HTML)
        os.remove("Animelar_royxati.txt")

        medias = get_all_media_base("drama")
        with open("Dramalar_royxati.txt", "w", encoding="utf-8") as file:
            file.write("Botidagi barcha Dramalar ro'yxati:\n\n")
            num = 0
            for i in medias:
                num += 1
                text = f"""
----  {num}  ----
Nomi : [ {i['name']} ]
Janri : {i['genre'].replace(","," ")}"""
                file.write(text)

        document = FSInputFile("Dramalar_royxati.txt")
        await call.message.answer_document(document,caption="🔹<b>Dramalar ro'yxati</b>",parse_mode=ParseMode.HTML)

        os.remove("Dramalar_royxati.txt")

    elif command == "anipass":
        text = """
<b>⚡️AniPass Premium</b>

AniPass - bu premium obuna xizmati.

<b>Imkoniyatlar:</b>
• Reklamasiz tomosha
• VIP kontentlarga kirish
• Premium qo'llab-quvvatlash

<b>Narx va obuna:</b>
Admin bilan bog'laning: @{ads_manager_username}
"""
        await call.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=user_act_8_clbtn())
    
    elif command == "lux":
        text = """
<b>💎Lux Kanal</b>

Lux - bu maxsus premium kanal obunasi.

<b>Imkoniyatlar:</b>
• Eksklyuziv kontent
• Reklamasiz tajriba
• Maxsus qo'llab-quvvatlash

<b>Narx va obuna:</b>
Admin bilan bog'laning: @{ads_manager_username}
"""
        await call.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=user_act_8_clbtn())

        return True

@router.message(F.text, User.menu)
async def handle_keyboard_buttons(msg: Message, state: FSMContext):
    """Handler for user keyboard buttons"""
    
    command = msg.text
    
    if command == "🔍Anime Qidirish":
        text = "<b>🔍Qidirilishi kerak bo'lgan anime nomini yuboring</b>"
        await state.set_state(Anime.search)
        a = await msg.answer(text, parse_mode=ParseMode.HTML, reply_markup=user_act_2_clbtn())
        await state.update_data(message_id = a.message_id)
    
    elif command == "⚡️AniPass / 💎Lux":
        text = """
<b>⚡️AniPass / 💎Lux Premium Xizmatlar</b>

Tanlang:
"""
        await msg.answer(text, parse_mode=ParseMode.HTML, reply_markup=user_act_9_clbtn())
    
    elif command == "🏙Rasm orqali qidiruv":
        text = """
<b>🔍Qidirilishi kerak bo'gan anime rasmni kiriting</b>
-
<b>‼️DIQQAT</b>
<i>Yaxshi natija olish uchun animeni videosidan skrinshot olib aynan anime tasvirlangan joyini qirqib keyin rasmni yuboring. Animening Posteri yoki Bannerlarini yubormang !</i>
"""
        await state.set_state(User.search_by_image)
        a = await msg.answer(text, parse_mode=ParseMode.HTML, reply_markup=user_act_2_clbtn())
        await state.update_data(message_id = a.message_id)
    
    elif command == "📚Qo'llanma":
        text = f"""
📚<b>Botini ishlatish bo'yicha qo'llanma : </b>
-
🔍<b>Anime Qidirish</b> - Botda mavjud bo'lgan animelarni qidirish uchun ishlatiladi. 
🔍<b>Drama Qidirish</b> - Botda mavjud bo'lgan dramalarni qidirish uchun ishlatiladi. 
💸<b>Reklama</b>  - bot adminlari bilan reklama yoki homiylik yuzasidan aloqaga chiqish.
🏙<b>Rasm Orqali Anime Qidiruv</b>  - Nomini topa olmayotgan animeingizni rasm orqali topib beradi.
📓<b>Ro'yxat</b>  - Botga joylangan Anime va Dramalar ro'yhati.
-
🆔Botdagi ID ingiz : <code>{msg.from_user.id}</code>
"""
        await msg.answer(text, parse_mode=ParseMode.HTML)
    
    elif command == "💸Reklama va homiylik":
        text = f"""
<b>📌Reklama va homiylik masalasida admin bilan bog'laning</b>
💬<i>Batafsil: @{ads_manager_username}</i>
"""
        await msg.answer(text, parse_mode=ParseMode.HTML)
    
    elif command == "Animelar ro'yxati📓":
        medias = get_all_media_base("anime")
        with open("Animelar_royxati.txt", "w", encoding="utf-8") as file:
            file.write("Botidagi barcha Animelar ro'yxati:\\n\\n")
            num = 0
            for i in medias:
                num += 1
                text = f"""
----  {num}  ----
Nomi : [ {i['name']} ]
Janri : {i['genre'].replace(","," ")}"""
                file.write(text)

        document = FSInputFile("Animelar_royxati.txt")
        await msg.answer_document(document,caption="🔸<b>Animelar ro'yxati</b>",parse_mode=ParseMode.HTML)
        os.remove("Animelar_royxati.txt")

        medias = get_all_media_base("drama")
        with open("Dramalar_royxati.txt", "w", encoding="utf-8") as file:
            file.write("Botidagi barcha Dramalar ro'yxati:\\n\\n")
            num = 0
            for i in medias:
                num += 1
                text = f"""
----  {num}  ----
Nomi : [ {i['name']} ]
Janri : {i['genre'].replace(","," ")}"""
                file.write(text)

        document = FSInputFile("Dramalar_royxati.txt")
        await msg.answer_document(document,caption="🔹<b>Dramalar ro'yxati</b>",parse_mode=ParseMode.HTML)
        os.remove("Dramalar_royxati.txt")
    
    elif command == "OnGoing animelar🧧":
        medias = get_all_ongoing_media_base()
        if medias:
            text = "<b>🧧OnGoing Animelar:</b>"
            await msg.answer(text, parse_mode=ParseMode.HTML, reply_markup=user_act_3_clbtn(medias))
        else:
            await msg.answer("<b>Hozirda OnGoing animelar yo'q</b>", parse_mode=ParseMode.HTML)