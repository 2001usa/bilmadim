import os
from dotenv import load_dotenv

# .env faylini yuklash
load_dotenv()

# Serveringizga python yordamida requirements.txt faylini install qiling
# .env faylida quyidagi o'zgaruvchilarni sozlang

#-----------------------------------------------
# Telegramda yopiq kanal oching. 
# Kanal nomini "Trailers" nomiga o'zgartiring. Botingizni yopiq kanalga admin qiling va 
# o'sha yopiq kanalingiz ID sini .env fayliga yozing
trailers_base_chat = os.getenv('TRAILERS_BASE_CHAT')
if trailers_base_chat:
    trailers_base_chat = int(trailers_base_chat)

#-----------------------------------------------
# Telegramda yopiq kanal oching. 
# Kanal nomini "Series" nomiga o'zgartiring. Botingizni yopiq kanalga admin qiling va 
# o'sha yopiq kanalingiz ID sini .env fayliga yozing
series_base_chat = os.getenv('SERIES_BASE_CHAT')
if series_base_chat:
    series_base_chat = int(series_base_chat)

#----------------------------------------------- 
# @botfather orqali bot yaratib botingizni tokenini .env fayliga yozib qo'ying
token = os.getenv('BOT_TOKEN')

#-----------------------------------------------
# Botingizni Usernamesini "@" belgisini qo'ymagan holda .env fayliga yozib qo'ying
# Misol uchun : kawaii_uz_bot
bot_username = os.getenv('BOT_USERNAME')

#-----------------------------------------------
# Foydalanuvchilar reklama yuzasidan bo'g'lana olishlari uchun 
# telegram akkuntingizni usernamesini "@" belgisini qo'ymagan holda .env fayliga yozib qo'ying
ads_manager_username = os.getenv('ADS_MANAGER_USERNAME')

#-----------------------------------------------
# O'zingizni Telegram ID raqamingizni .env fayliga yozing
# Bir nechta admin bo'lishi mumkin: 123456789,987654321
# O'z ID raqamingizni bilish uchun @userinfobot ga /start yuboring
admin_ids_str = os.getenv('ADMIN_IDS')
admin_ids = [int(id.strip()) for id in admin_ids_str.split(',')]
