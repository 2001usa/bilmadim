from aiogram import Bot,Dispatcher
import logging
import asyncio
from app.handlers.admin.admin_actions import admin_router
from app.handlers.admin.add_media import add_media_router
from app.handlers.admin.add_episode import add_episode_router
from app.handlers.admin.edit_media import edit_media_router
from app.handlers.admin.edit_episode import edit_episode_router
from app.handlers.admin.send_message import send_message_router
from app.handlers.admin.sponsor import sponsor_router
from app.handlers.admin.staff import staff_router
from app.handlers.admin.post_media import post_media_router
from app.handlers.admin.post_episode import post_media_episode_router

from app.handlers.user.user_actions import router
from app.handlers.user.search_anime import user_search_router
from app.handlers.user.search_drama import user_search_drama_router
from app.handlers.user.media import user_media_router
from app.handlers.user.search_by_image import user_search_by_image_router
from app.handlers.user.chat_request import request_router
from app.handlers.user.search_by_id import user_search_by_id_router
from config import token

async def main():

    bot = Bot(token=token)

    dp = Dispatcher()

    dp.include_router(router=request_router)
    dp.include_router(router=router)
    dp.include_router(router=admin_router)
    dp.include_router(router=add_episode_router)
    dp.include_router(router=add_media_router)
    dp.include_router(router=edit_media_router)
    dp.include_router(router=edit_episode_router)
    dp.include_router(router=send_message_router)
    dp.include_router(router=post_media_router)
    dp.include_router(router=post_media_episode_router)
    dp.include_router(router=sponsor_router)
    dp.include_router(router=staff_router)
    dp.include_router(router=user_search_router)
    dp.include_router(router=user_search_by_id_router)
    dp.include_router(router=user_search_drama_router)
    dp.include_router(router=user_media_router)
    dp.include_router(router=user_search_by_image_router)

    await dp.start_polling(bot)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(main())
    except:
        print("Bot to'xtatildi")
