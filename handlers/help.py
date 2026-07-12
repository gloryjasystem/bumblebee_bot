"""
handlers/help.py — Команда /help и кнопка «Как пользоваться».

Один и тот же текст показывается:
  • при вводе /help (доступно через кнопку «Меню» слева, см. set_my_commands в bot.py);
  • при нажатии «ℹ️ Как пользоваться» в Личном кабинете (callback "menu:help").

Текст — единственный источник правды (HELP_TEXT). Кнопка «Назад» контекстная:
из кабинета → menu:settings, из набранного /help → menu:main.
"""
from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
)

from utils.nav import navigate, set_active_msg, consume_command

router = Router()


HELP_TEXT = (
    "🐝 <b>Bumblebee — как всё устроено</b>\n\n"
    "Подключаете <b>своего</b> бота — и через него управляете каналами и группами: "
    "защита, рассылки, ссылки и аналитика. Всё настраивается кнопками, без кода.\n\n"
    "🚀 <b>С чего начать</b>\n"
    "Создайте бота в @BotFather, подключите его к Bumblebee — и настраивайте всё "
    "прямо здесь.\n\n"
    "Инструменты ниже работают <b>в ваших каналах и группах</b> — сразу после "
    "подключения бота. Чтобы их настроить, откройте «🤖 Мой список ботов» → "
    "выберите бота → его меню. Один бот обслуживает несколько каналов и групп.\n\n"
    "<b>Что внутри:</b>\n\n"
    "✅ <b>Обработка заявок</b> — принимает вступающих сам и проверяет каждого по "
    "чёрному списку.\n\n"
    "🛡 <b>Защита</b> — капча и фильтры по языку, именам и фото отсекают спам до "
    "входа.\n\n"
    "⛔️ <b>Чёрный список</b> — банит спамеров на входе во все каналы и вычищает тех, "
    "кто уже внутри.\n\n"
    "💬 <b>Сообщения</b> — приветствие и прощание по имени, автоответы и реакции.\n\n"
    "📨 <b>Рассылка</b> — сообщение по всей базе подписчиков: медиа, кнопки, "
    "расписание.\n\n"
    "🔗 <b>Ссылки</b> — считают переходы и стоимость подписчика: видно, какой трафик "
    "окупается.\n\n"
    "📣 <b>Обратная связь</b> — подписчики пишут вам через бота, вы отвечаете в один "
    "тап.\n\n"
    "⚙️ <b>Управление и команда</b> — доступ, часовой пояс и приглашение "
    "администраторов.\n\n"
    "📊 <b>Статистика</b> — открываете своего бота или канал и сразу видите: сколько "
    "людей пришло за сегодня, вчера и всего, заявки в очереди, активные и ушедшие "
    "подписчики.\n\n"
    "💎 <b>Тарифы</b> — Free для старта; Старт, Про и Бизнес дают больше "
    "возможностей и лимитов. Цены — в разделе «💎 Тарифы».\n\n"
    "💬 Вопросы — «Служба поддержки»."
)


def _kb_back(callback_data: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад", callback_data=callback_data)],
    ])


# ── /help (через кнопку «Меню» слева или ввод команды) ────────
@router.message(Command("help"))
async def cmd_help(message: Message):
    # Убираем командный «пузырь» и предыдущий экран, шлём справку свежей внизу
    await consume_command(message)
    sent = await message.answer(HELP_TEXT, reply_markup=_kb_back("menu:main"))
    await set_active_msg(message.from_user.id, sent.message_id)


# ── «ℹ️ Как пользоваться» из Личного кабинета ─────────────────
@router.callback_query(F.data == "menu:help")
async def on_help_menu(callback: CallbackQuery):
    await navigate(callback, HELP_TEXT, reply_markup=_kb_back("menu:settings"))
