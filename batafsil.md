# 🤖 AniLegend Bot - Batafsil Qo'llanma

Ushbu hujjat **AniLegend** botining barcha imkoniyatlari, adminlar uchun qo'llanma va texnik tafsilotlarlarni o'z ichiga oladi.

## 👥 Foydalanuvchilar Uchun Imkoniyatlar

Bot oddiy foydalanuvchilar uchun quyidagi qulayliklarni taqdim etadi:

1.  **🔍 Qidiruv Tizimi**:
    *   **Anime Qidirish**: Animelarni nomi bo'yicha qidirish.
    *   **Drama Qidirish**: Dramalarni nomi bo'yicha qidirish.
    *   **Rasm Orqali Qidiruv**: Animening videosidan olingan skrinshot orqali qaysi anime ekanligini aniqlash (trace.moe integratsiyasi).
    *   **Kod Orqali Qidiruv**: Har bir media uchun maxsus kod orqali to'g'ridan-to'g'ri kirish.

2.  **📺 Tomosha Qilish**:
    *   Filmlar va seriallarni to'g'ridan-to'g'ri bot ichida ko'rish.
    *   Treylerlarni ko'rish va seriallar uchun qismlarni tanlash menyusi.
    *   **OnGoing** (davom etayotgan) va **Tugallangan** medialarni ajratib ko'rsatish.

3.  **💎 Premium Xizmatlar (AniPass / Lux)**:
    *   Reklamasiz foydalanish.
    *   VIP kontentlarga kirish (agar mavjud bo'lsa).

---

## 👔 Adminlar Uchun Imkoniyatlar (/admin yoki /panel)

Admin paneliga kirish uchun `config.py` faylida ID raqamingiz bo'lishi kerak.

### 1. ➕ Media va Qism Qo'shish
*   **Media Qo'shish**: Yangi Anime yoki Drama qo'shish. Nomi, janri, teglari, dublyaj tili, rasmi va treylerini kiritishingiz kerak.
*   **Qism Qo'shish**: Mavjud mediaga yangi qismlarni yuklash.
    *   **🆕 YANGILIK**: Endi qismlarni **ketma-ket (bulk upload)** yuklash mumkin. Birinchi qismni yuborganingizdan so'ng, bot "Saqlandi" deydi va darhol keyingisini yuborishingiz mumkin. Har safar tugma bosish shart emas.

### 2. ✏️ Tahrirlash
*   **Media Tahrirlash**: Nomi, janri, teglari, holati (Ongoing/Finished) va boshqa ma'lumotlarni o'zgartirish.
*   **Qismni Tahrirlash**: Yuklangan qismni almashtirish yoki o'chirish.
    *   **🆕 TUZATILDI**: Qismni almashtirgandan so'ng, bot endi 1-qismga tashlab yubormaydi, balki o'sha tahrirlangan qismga qaytadi.

### 3. 📊 Statistika va Foydalanuvchilar
*   Umumiy foydalanuvchilar, anime va dramalar sonini ko'rish.
*   Foydalanuvchilarga xabar yuborish (hammaga tarqatish).
*   Yangi adminlarni tayinlash.

### 4. 🔐 Majburiy Obuna (Sponsorship)
*   Botdan foydalanish uchun majburiy kanallarni sozlash.
*   "Zayavkali" kanallar yoki oddiy ochiq kanallarni qo'shish.

---

## 🛠 Texnik Ma'lumotlar

Ushbu bo'lim dasturchilar uchun mo'ljallangan.

### 📁 Fayl Tuzilishi
*   `main.py`: Botni ishga tushiruvchi asosiy fayl.
*   `config.py`: Sozlamalar (Token, Admin IDlar, Kanallar).
*   `app/database/database.db`: SQLite ma'lumotlar bazasi.
*   `app/database/bot_base.py`: Bazaga ulanish va so'rovlar funksiyalari.
*   `app/handlers/`: Botning logikasi (admin va user uchun alohida papkalar).
*   `app/keyboards/`: Tugmalar (Inline va Reply).

### 🗄 Ma'lumotlar Bazasi (SQLite)
Asosiy jadvallar:
1.  **users**: Foydalanuvchilar, adminlar, premium statuslari.
2.  **media**: Anime/Drama ma'lumotlari (nomi, treyleri, janri).
3.  **episodes**: Har bir qismning fayl IDsi va raqami.
4.  **sponsors**: Majburiy kanallar ro'yxati.
5.  **statistics**: Bot statistikasi.

### ⚙️ Yangi qo'shilgan funksiyalar (So'nggi yangilanish)
1.  **Admin Exit**: Admin paneldan chiqish uchun "🔙Chiqish" tugmasi qo'shildi.
2.  **Bulk Upload**: `add_episode.py` optimallashtirildi.
3.  **Edit Loop Fix**: `edit_episode.py` dagi navigatsiya xatosi tuzatildi.
