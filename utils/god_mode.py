"""
utils/god_mode.py — Сессии «Режима управления».

In-memory словарь: admin_id -> target_user_id.
Не зависит от FSM — state.clear() в channel_settings.py
не влияет на активные сессии управления.
"""

_sessions: dict[int, int] = {}  # admin_id -> target_user_id


def enter(admin_id: int, target_uid: int) -> None:
    """Активировать режим управления ботом пользователя."""
    _sessions[admin_id] = target_uid


def exit_mode(admin_id: int) -> None:
    """Завершить режим управления."""
    _sessions.pop(admin_id, None)


def get_target(admin_id: int) -> int | None:
    """Вернуть ID целевого пользователя, или None если режим не активен."""
    return _sessions.get(admin_id)
