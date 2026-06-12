"""
utils/admin_guard.py — Авторизация админ-панели платформы.

Router-level фильтр: пускает к админ-обработчикам ТОЛЬКО владельца, совладельца
или нанятого менеджера (запись в global_admins). Для всех остальных роутер
возвращает UNHANDLED → апдейт идёт дальше к обычным пользовательским роутерам
(обычные юзеры НЕ блокируются).

Закрывает класс уязвимости «broken access control»: раньше опасные ga_*-callback'и
выполнялись без проверки, кто их вызвал (callback_data полностью контролируется
клиентом и может быть отправлен напрямую через MTProto, минуя меню).
"""
from aiogram.filters import BaseFilter
from aiogram.types import TelegramObject


class IsAdmin(BaseFilter):
    """True — если событие от владельца / совладельца / менеджера платформы."""

    async def __call__(self, event: TelegramObject) -> bool:
        # Ленивый импорт — избегаем циклической зависимости с handlers.global_admin.
        from handlers.global_admin import get_admin_context

        user = getattr(event, "from_user", None)
        if user is None:
            return False
        try:
            role, _ = await get_admin_context(user.id, user.username)
        except Exception:
            # Фильтр НИКОГДА не должен бросать исключение наружу — иначе он мог бы
            # сорвать обработку обычного апдейта. Fail-closed: при любой ошибке — отказ.
            return False
        return role is not None
