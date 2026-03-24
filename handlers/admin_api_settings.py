"""
handlers/admin_api_settings.py — Admin UI для управления настройками RapidAPI.

Доступ: только Главный Администратор платформы (проверка через is_main_admin()).

Путь в боте: ⚙️ Настройки платформы → 🔑 Настройки RapidAPI

Функциональность:
  - Показ текущих настроек (Host, замаскированный Key, остаток квоты).
  - Изменение Key и Host через FSM-ввод.
  - Немедленная инвалидация TTL-кэша после сохранения.
  - Удаление сообщения с ключом из истории чата (безопасность).
"""
import logging

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from services.settings import (
    get_quota,
    get_setting,
    invalidate_api_cache,
    set_setting,
)

logger = logging.getLogger(__name__)

router = Router()


# ── FSM-состояния ─────────────────────────────────────────────────────────────

class AdminApiSettingsFSM(StatesGroup):
    """Состояния ожидания нового значения от администратора."""
    waiting_key  = State()   # Ждём новый API Key
    waiting_host = State()   # Ждём новый API Host


# ── Клавиатуры ────────────────────────────────────────────────────────────────

def _kb_api_settings(owner_id: int) -> InlineKeyboardMarkup:
    """Главная клавиатура панели настроек RapidAPI."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔑 Изменить Key",  callback_data="api_set_key"),
            InlineKeyboardButton(text="✏️ Изменить Host", callback_data="api_set_host"),
        ],
        [InlineKeyboardButton(text="◀️ Назад", callback_data=f"ga_bl:{owner_id}")],
    ])


def _kb_cancel_api(back_cb: str = "rapidapi_settings") -> InlineKeyboardMarkup:
    """Клавиатура отмены ввода нового значения."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚫 Отменить", callback_data=back_cb)],
    ])


# ── Вспомогательные функции ───────────────────────────────────────────────────

def _mask_key(key: str | None) -> str:
    """
    Маскирует API-ключ для безопасного отображения в чате.
    Показывает первые 6 символов, остальное заменяет точками.

    Примеры:
      "sk-a1b2c3d4e5f6"  → "sk-a1b•••••••••••••••••"
      ""                  → "❌ не задан"
      "YOUR_KEY_HERE"     → "❌ не задан"
    """
    if not key or key in ("YOUR_KEY_HERE", "", "None"):
        return "❌ не задан"
    visible = key[:6]
    dots = "•" * min(len(key) - 6, 20)
    return f"{visible}{dots}"


def _format_quota(quota: int) -> str:
    """Форматирует остаток квоты с индикатором критического уровня."""
    if quota < 0:
        return "<i>неизвестно — запустите обработку</i>"
    if quota < 50:
        return f"<b>🔴 {quota:,}</b> (критически мало!)"
    if quota < 200:
        return f"<b>🟡 {quota:,}</b> (пополните скоро)"
    return f"<b>🟢 {quota:,}</b>"


async def _render_settings_text() -> str:
    """Строит текст панели настроек, считывая актуальные данные из БД."""
    host  = await get_setting("rapidapi_host", "—")
    key   = await get_setting("rapidapi_key",  "")
    quota = await get_quota()

    return (
        "🔑 <b>Настройки RapidAPI</b>\n\n"
        f"<b>Host:</b> <code>{host}</code>\n"
        f"<b>Key:</b>  <code>{_mask_key(key)}</code>\n"
        f"📊 Остаток запросов: {_format_quota(quota)}\n\n"
        "<i>Изменения применяются мгновенно — перезапуск бота не нужен.</i>"
    )


# ── Показ панели настроек ─────────────────────────────────────────────────────

@router.callback_query(F.data == "rapidapi_settings")
async def on_rapidapi_settings(call: CallbackQuery, platform_user: dict | None):
    """
    Отображает текущие настройки RapidAPI:
    Host, замаскированный Key и остаток квоты из БД.
    """
    if not platform_user:
        return

    # Защита: только главный администратор платформы
    from config import settings as cfg
    if call.from_user.id not in (cfg.owner_telegram_id, cfg.co_owner_telegram_id):
        return await call.answer("⛔️ Доступ запрещён.", show_alert=True)

    text = await _render_settings_text()
    owner_id = platform_user["user_id"]
    try:
        await call.message.edit_text(
            text,
            parse_mode="HTML",
            reply_markup=_kb_api_settings(owner_id),
        )
    except Exception:
        await call.message.answer(
            text,
            parse_mode="HTML",
            reply_markup=_kb_api_settings(owner_id),
        )


# ── Нажатие «Изменить Key» ────────────────────────────────────────────────────

@router.callback_query(F.data == "api_set_key")
async def on_api_set_key(call: CallbackQuery, state: FSMContext, platform_user: dict | None):
    """Переводит в режим ожидания нового API Key."""
    if not platform_user:
        return

    from config import settings as cfg
    if call.from_user.id not in (cfg.owner_telegram_id, cfg.co_owner_telegram_id):
        return await call.answer("⛔️ Доступ запрещён.", show_alert=True)

    await state.set_state(AdminApiSettingsFSM.waiting_key)
    prompt_msg = await call.message.edit_text(
        "🔑 <b>Введите новый API Key</b>\n\n"
        "Скопируйте ключ из вашего аккаунта RapidAPI и отправьте его сюда.\n\n"
        "⚠️ <i>Сообщение с ключом будет автоматически удалено из чата после сохранения.</i>",
        parse_mode="HTML",
        reply_markup=_kb_cancel_api(),
    )
    await state.update_data(prompt_msg_id=prompt_msg.message_id)


# ── Нажатие «Изменить Host» ───────────────────────────────────────────────────

@router.callback_query(F.data == "api_set_host")
async def on_api_set_host(call: CallbackQuery, state: FSMContext, platform_user: dict | None):
    """Переводит в режим ожидания нового API Host."""
    if not platform_user:
        return

    from config import settings as cfg
    if call.from_user.id not in (cfg.owner_telegram_id, cfg.co_owner_telegram_id):
        return await call.answer("⛔️ Доступ запрещён.", show_alert=True)

    await state.set_state(AdminApiSettingsFSM.waiting_host)
    prompt_msg = await call.message.edit_text(
        "✏️ <b>Введите новый API Host</b>\n\n"
        "Пример: <code>telegram124.p.rapidapi.com</code>\n\n"
        "<i>Вводите только хост, без https:// и слешей.</i>",
        parse_mode="HTML",
        reply_markup=_kb_cancel_api(),
    )
    await state.update_data(prompt_msg_id=prompt_msg.message_id)


# ── Приём нового Key ──────────────────────────────────────────────────────────

@router.message(AdminApiSettingsFSM.waiting_key)
async def on_save_key(msg: Message, state: FSMContext, platform_user: dict | None):
    """
    Сохраняет новый API Key в platform_settings и немедленно
    инвалидирует TTL-кэш, чтобы пайплайн подхватил ключ без ожидания.
    Удаляет сообщение с ключом из истории чата для безопасности.
    """
    if not platform_user:
        return

    new_key = (msg.text or "").strip()
    if not new_key:
        return await msg.answer("⚠️ Значение не может быть пустым.")

    # 1. Сохраняем в БД
    await set_setting("rapidapi_key", new_key)

    # 2. КРИТИЧНО: инвалидируем кэш — новый ключ подхватится немедленно
    invalidate_api_cache()

    # 3. Удаляем сообщение-запрос "Введите ключ" (если записали его)
    data = await state.get_data()
    prompt_msg_id = data.get("prompt_msg_id")
    if prompt_msg_id:
        try:
            await msg.bot.delete_message(chat_id=msg.chat.id, message_id=prompt_msg_id)
        except Exception:
            pass

    # 4. Очищаем состояние
    await state.clear()

    # 5. Удаляем ответ пользователя с секретным ключом из истории чата
    try:
        await msg.delete()
    except Exception:
        pass

    logger.info("[ADMIN API] API Key updated by user=%d", msg.from_user.id)

    # 5. Показываем обновлённую панель настроек
    owner_id = platform_user["user_id"]
    text = await _render_settings_text()
    await msg.answer(
        f"✅ <b>API Key сохранён</b>\n"
        f"<i>Сообщение с ключом удалено из чата.</i>\n\n"
        + text,
        parse_mode="HTML",
        reply_markup=_kb_api_settings(owner_id),
    )


# ── Приём нового Host ─────────────────────────────────────────────────────────

@router.message(AdminApiSettingsFSM.waiting_host)
async def on_save_host(msg: Message, state: FSMContext, platform_user: dict | None):
    """
    Сохраняет новый API Host и автоматически обновляет URL + query-параметр.

    Логика:
      - Если хост известен (есть в KNOWN_PROVIDERS) — берём готовые URL и param.
      - Если хост неизвестен — сохраняем путь из старого URL, меняем только хост.
    """
    if not platform_user:
        return

    from urllib.parse import urlparse, urlunparse
    from services.settings import resolve_provider

    new_host = (msg.text or "").strip().lower()
    new_host = new_host.removeprefix("https://").removeprefix("http://").rstrip("/")

    if not new_host or "." not in new_host:
        return await msg.answer(
            "⚠️ Некорректный хост. Введите только доменное имя, без https://.\n"
            "Пример: <code>telegram124.p.rapidapi.com</code>",
            parse_mode="HTML",
        )

    # 1. Сохраняем Host
    await set_setting("rapidapi_host", new_host)

    # 2. Подбираем URL и param
    provider = resolve_provider(new_host)
    if provider:
        # Известный провайдер — берём готовые значения из реестра
        new_url   = provider.url
        new_param = provider.param
        provider_note = f"✅ Провайдер распознан автоматически (параметр: <code>{new_param}</code>)"
    else:
        # Неизвестный провайдер — подменяем хост в URL, сохраняем путь
        old_url = await get_setting("rapidapi_url", f"https://{new_host}/")
        try:
            parsed  = urlparse(old_url)
            new_url = urlunparse(parsed._replace(netloc=new_host))
        except Exception:
            new_url = f"https://{new_host}/"
        new_param = "username"  # дефолт для неизвестных
        provider_note = "⚠️ Провайдер не распознан — URL обновлён, параметр: <code>username</code>"

    await set_setting("rapidapi_url",   new_url)
    await set_setting("rapidapi_param", new_param)

    # 3. Инвалидируем кэш — новые настройки сразу подхватятся пайплайном
    invalidate_api_cache()

    # 4. Удаляем сообщение-запрос «Введите хост»
    data = await state.get_data()
    prompt_msg_id = data.get("prompt_msg_id")
    if prompt_msg_id:
        try:
            await msg.bot.delete_message(chat_id=msg.chat.id, message_id=prompt_msg_id)
        except Exception:
            pass

    # 5. Сбрасываем FSM
    await state.clear()

    logger.info("[ADMIN API] Host=%s URL=%s param=%s (user=%d)",
                new_host, new_url, new_param, msg.from_user.id)

    # 6. Показываем обновлённую панель
    owner_id = platform_user["user_id"]
    text = await _render_settings_text()
    await msg.answer(
        f"✅ <b>API Host сохранён:</b> <code>{new_host}</code>\n"
        f"{provider_note}\n\n" + text,
        parse_mode="HTML",
        reply_markup=_kb_api_settings(owner_id),
    )
