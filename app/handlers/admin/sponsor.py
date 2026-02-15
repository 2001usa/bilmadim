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
import re

sponsor_router = Router()

class Admin(StatesGroup):
    menu = State()

class Sponsor(StatesGroup):
    menu = State()
    type = State()
    add = State()
    limit = State()

    link = State()
    name = State()

@sponsor_router.message(ChatTypeFilter(chat_type=["group", "supergroup"]))
async def action(msg: Message, state: FSMContext):
    pass

@sponsor_router.callback_query(F.data.startswith('d'),Sponsor())
async def action(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(Admin.menu)
    await call.message.answer("✅<b>Bekor qilindi</b>",parse_mode=ParseMode.HTML,reply_markup=act_2_btn())
    await call.message.delete()

@sponsor_router.callback_query(F.data.startswith('c'),Sponsor.add)
async def action(call: CallbackQuery, state: FSMContext):

    sponsors = get_all_sponsors_base()
    await state.set_state(Sponsor.menu)
    
    await call.message.edit_text(
        "<b>🔐Homiy kanallar</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=act_8_clbtn(sponsors)
    )

@sponsor_router.callback_query(F.data.startswith('c'),Sponsor.menu)
async def action(call: CallbackQuery, state: FSMContext):

    command = call.data.split(",")[1]
    
    if command == "add":
        await state.set_state(Sponsor.type)

        text = "<b>📌Ulamoqchi bo'lgan homiy kanalning tipini tanlang :</b>"
        await call.message.edit_text(text,reply_markup=act_15_clbtn(),parse_mode=ParseMode.HTML)

    elif command == "channel":
        channel_id = int(call.data.split(",")[2])
        channel = get_single_sponsors_base(channel_id)
        delete_sponsor_base(channel_id)

        sponsors = get_all_sponsors_base()

        await state.set_state(Sponsor.menu)

        await call.answer(f"{channel['channel_name']} homiylikdan olib tashlandi✅")
        await call.message.edit_reply_markup(reply_markup=act_8_clbtn(sponsors))

    elif command == "limit":
        text = "⚠️Shuncha foydalanuvchi bo'tingiz orqali o'sha kanalga qo'shilganidan so'ng kanal avtomatik majburiy a'zodan olib tashlanadi"
        await call.answer(text,show_alert=True)

    elif command == "notlimit":
        text = "⚠️Global havolalarga limit qo'yib bo'lmaydi"
        await call.answer(text,show_alert=True)

@sponsor_router.callback_query(F.data.startswith('s'),Sponsor.type)
async def action(call: CallbackQuery, state: FSMContext):
    
    command = call.data.split(",")[1]

    if command == "type":
        type = call.data.split(",")[2]

        if type in ["request","simple"]:
            await state.set_state(Sponsor.add)
            text = "<b>⚠️Yangi homiy kanal nomidan istalgan postni botga forward qiling</b>"
            a = await call.message.edit_text(text,reply_markup=act_4_clbtn(),parse_mode=ParseMode.HTML)
            
        elif type == "link":
            await state.set_state(Sponsor.link)
            text = "<b>🌐Istalgan website uchun link yuboring</b>"
            a = await call.message.edit_text(text,reply_markup=act_4_clbtn(),parse_mode=ParseMode.HTML)

        await state.update_data(
            message_id = a.message_id,
            type = type
        )

    else:
        await state.clear()
        await state.set_state(Admin.menu)
        await call.message.answer("✅<b>Bekor qilindi</b>",parse_mode=ParseMode.HTML,reply_markup=act_2_btn())
        await call.message.delete()

@sponsor_router.message(Sponsor.link)
async def action(msg: Message, state: FSMContext):

    data = await state.get_data()
    message_id = data.get("message_id")

    url = msg.text

    def is_link(string):
        pattern = r'^(https?:\/\/)([\w\-]+\.)+[\w\-]+(\/[\w\-._~:/?#[\]@!$&\'()*+,;=]*)?$'
        return re.match(pattern, string) is not None
    
    is_link = is_link(url)

    if is_link:
        await state.set_state(Sponsor.name)
        await state.update_data(url = url)

        await msg.delete()
        text = f"🔸<code>{url}</code> <b>linki uchun nom yuboring</b>"
        await msg.bot.edit_message_text(
            chat_id=msg.from_user.id,
            message_id=message_id,
            text=text,
            parse_mode=ParseMode.HTML,
            reply_markup= act_4_clbtn()
        )

    else:
        a = await msg.answer("<b>❌Iltimos to'g'ri link kiriting</b>",parse_mode=ParseMode.HTML)
        await asyncio.sleep(3)
        await a.delete()
        await msg.delete()

@sponsor_router.message(Sponsor.name)
async def action(msg: Message, state: FSMContext):

    data = await state.get_data()
    message_id = data.get("message_id")
    url = data.get("url")

    name = msg.text

    add_sponsor_base(0,name,url,"link",0)
    sponsors = get_all_sponsors_base()

    await state.clear()
    await state.set_state(Sponsor.menu)
    
    await msg.bot.edit_message_text(
        chat_id=msg.from_user.id,
        message_id=message_id,
        text="<b>🔐Homiy kanallar</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=act_8_clbtn(sponsors)
    )
    await msg.delete()

@sponsor_router.message(Sponsor.add)
async def action(msg: Message, state: FSMContext):

    data = await state.get_data()
    message_id = data.get("message_id")
    type = data.get("type")

    if msg.forward_from_chat:
        chat_id = msg.forward_from_chat.id
        name = msg.forward_from_chat.title

        if type == "simple":
            channel_data = await msg.bot.get_chat(chat_id)
            url = channel_data.invite_link
        else:
            try:
                channel_data = await msg.bot.create_chat_invite_link(chat_id=chat_id,creates_join_request=True)
                url = channel_data.invite_link
            except:
                url = None

        if url:

            await state.set_state(Sponsor.limit)
            await msg.delete()

            try:
                await msg.bot.delete_message(chat_id=msg.from_user.id, message_id=message_id)
            except:
                pass
            
            text = "<b>👤Ushbu homiy kanalga qo'shilishi kerak bo'lgan obunachilar soni uchun limit kiriting</b>"
            a = await msg.answer(text,reply_markup=act_4_clbtn(),parse_mode=ParseMode.HTML)
            await state.update_data(
                url = url,
                name = name,
                channel_id = chat_id,
                message_id = a.message_id
            )

        else:
            a = await msg.answer("<b>❌Bot kanalda admin qilinmagan</b>",parse_mode=ParseMode.HTML)
            await asyncio.sleep(3)
            await a.delete()
            await msg.delete()

    else:
        a = await msg.answer("<b>❌Habar kanal nomidan yuborilmagan</b>",parse_mode=ParseMode.HTML)
        await asyncio.sleep(3)
        await a.delete()
        await msg.delete()


@sponsor_router.message(Sponsor.limit)
async def action(msg: Message, state: FSMContext):

    data = await state.get_data()
    message_id = data.get("message_id")
    type = data.get("type")
    url = data.get("url")
    channel_id = data.get("channel_id")
    name = data.get("name")

    limit = msg.text

    if limit.isdigit():

        add_sponsor_base(channel_id,name,url,type,limit)
        sponsors = get_all_sponsors_base()

        await state.clear()
        await state.set_state(Sponsor.menu)
        
        await msg.bot.edit_message_text(
            chat_id=msg.from_user.id,
            message_id=message_id,
            text="<b>🔐Homiy kanallar</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=act_8_clbtn(sponsors)
        )
        await msg.delete()

    else:
        a = await msg.answer("<b>❌Iltimos Son yuboring</b>",parse_mode=ParseMode.HTML)
        await asyncio.sleep(3)
        await a.delete()
        await msg.delete()

