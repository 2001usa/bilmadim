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

staff_router = Router()

class Admin(StatesGroup):
    menu = State()

class Staff(StatesGroup):
    menu = State()
    add = State()

@staff_router.message(ChatTypeFilter(chat_type=["group", "supergroup"]))
async def action(msg: Message, state: FSMContext):
    pass

@staff_router.callback_query(F.data.startswith('c'),Staff.add)
async def action(call: CallbackQuery, state: FSMContext):
    
    staff_list = get_all_staff_base()
    await state.set_state(Staff.menu)
    
    await call.message.edit_text(
        "<b>👔Adminlar</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=act_9_clbtn(staff_list)
    )

@staff_router.callback_query(F.data.startswith('c'),Staff.menu)
async def action(call: CallbackQuery, state: FSMContext):

    command = call.data.split(",")[1]
    
    if command == "back":
        await state.clear()
        await state.set_state(Admin.menu)
        await call.message.answer("✅<b>Bekor qilindi</b>",parse_mode=ParseMode.HTML,reply_markup=act_2_btn())
        await call.message.delete()

    elif command == "add":
        await state.set_state(Staff.add)

        text = "<b>⚠️Yangi Adminning ID sini yuboring</b>"
        await call.message.edit_text(text,reply_markup=act_4_clbtn(),parse_mode=ParseMode.HTML)

    elif command == "staff":
        user_id = int(call.data.split(",")[2])
        user = get_user_base(user_id)
        update_user_staff_base(user_id,0)

        staff_list = get_all_staff_base()

        await state.set_state(Staff.menu)

        await call.answer(f"{user['user_id']} adminlikdan olib tashlandi✅")
        await call.message.edit_reply_markup(reply_markup=act_9_clbtn(staff_list))

@staff_router.message(Staff.add)
async def action(msg: Message, state: FSMContext):

    data = await state.get_data()
    message_id = data.get("message_id")

    if msg.text.isdigit():
        user_id = int(msg.text)
        user = get_user_base(user_id)

        if user:
            update_user_staff_base(user_id,1)
            await msg.delete()

            staff_list = get_all_staff_base()
            await state.set_state(Staff.menu)
            
            await msg.bot.edit_message_text(
                chat_id=msg.from_user.id,
                message_id=message_id,
                text="<b>👔Adminlar</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=act_9_clbtn(staff_list)
            )

        else:
            a = await msg.answer("<b>❌Foydalanuvchi botdan ro'yxatdan o'tmagan</b>",parse_mode=ParseMode.HTML)
            await asyncio.sleep(3)
            await a.delete()
            await msg.delete()

    else:
        a = await msg.answer("<b>❌ID yuboring</b>",parse_mode=ParseMode.HTML)
        await asyncio.sleep(3)
        await a.delete()
        await msg.delete()