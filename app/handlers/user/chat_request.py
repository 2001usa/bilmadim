from aiogram import Router
from app.database.bot_base import *
from datetime import *
from app.database.bot_base import add_sponsor_request_base
from aiogram.types import ChatJoinRequest
from aiogram.types import ChatMemberUpdated
from aiogram.filters import ChatMemberUpdatedFilter

request_router = Router()

@request_router.chat_join_request()
async def handle_join_request(request: ChatJoinRequest):
    
    user_id = request.from_user.id
    chat_id = request.chat.id

    add_sponsor_request_base(chat_id,user_id)