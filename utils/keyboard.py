import json
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo

def build_inline_keyboard(buttons_raw) -> InlineKeyboardMarkup | None:
    """
    Парсит сырой JSON-список кнопок из БД в InlineKeyboardMarkup.
    Поддерживает:
      - Цветные кнопки через emoji: 🟥 (danger), 🟩 (success), 🟦 (primary)
      - WebApp-кнопки через суффикс (webapp) в URL
    """
    if not buttons_raw:
        return None
        
    btns = buttons_raw if isinstance(buttons_raw, list) else json.loads(buttons_raw)
    inline_rows = []
    
    # Нормализуем к 2D списку
    if btns and isinstance(btns[0], dict):
        rows = [[b] for b in btns]
    else:
        rows = btns
    
    for row in rows:
        inline_row = []
        for btn in row:
            text = btn.get("text", "")
            url = btn.get("url", "")
            
            btn_style = None
            if text.startswith("🟦"):
                btn_style = "primary"
                text = text[len("🟦"):].strip()
            elif text.startswith("🟩"):
                btn_style = "success"
                text = text[len("🟩"):].strip()
            elif text.startswith("🟥"):
                btn_style = "danger"
                text = text[len("🟥"):].strip()

            if url.endswith("(webapp)"):
                clean_url = url[:-8].strip()
                kwargs = dict(text=text, web_app=WebAppInfo(url=clean_url))
                if btn_style:
                    kwargs["style"] = btn_style
                inline_row.append(InlineKeyboardButton(**kwargs))
            else:
                kwargs = dict(text=text, url=url)
                if btn_style:
                    kwargs["style"] = btn_style
                inline_row.append(InlineKeyboardButton(**kwargs))
        if inline_row:
            inline_rows.append(inline_row)
            
    return InlineKeyboardMarkup(inline_keyboard=inline_rows) if inline_rows else None
