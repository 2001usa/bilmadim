import os
from PIL import Image,ImageFilter
import requests
import tracemoepy
from app.database.bot_base import *
from aiogram.enums import ParseMode

async def searching_anime_by_image(image_path):
    try:
        tracemoe = tracemoepy.tracemoe.TraceMoe()
        resp = tracemoe.search(image_path,upload_file=True)
        os.remove(image_path)

        return resp
    except:
        os.remove(image_path)
        return "Error"

async def check_user_subscribes(sponsors, msg):
    not_sub_channels = []

    for i in sponsors:
        try:
            if i['type'] == "simple":
                chat_member = await msg.bot.get_chat_member(
                    chat_id=i["channel_id"],
                    user_id=msg.from_user.id
                )

                if chat_member.status in ["restricted", "left", "kicked"]:
                    not_sub_channels.append(i)
                else:
                    add_sponsor_request_base(i["channel_id"],msg.from_user.id)

            elif i["type"] == "request":
                
                user = get_sponsor_request_base(i['channel_id'],msg.from_user.id)
                if not user:
                    chat_member = await msg.bot.get_chat_member(
                        chat_id=i["channel_id"],
                        user_id=msg.from_user.id
                    )
                    if chat_member.status in ["restricted", "left", "kicked"]:
                        not_sub_channels.append(i)

        except:
            delete_sponsor_base(i['channel_id'])

    if len(not_sub_channels) > 0:
        for i in sponsors:
            if i["type"] == "link":
                not_sub_channels.append(i)

    return not_sub_channels