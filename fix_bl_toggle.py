path = r'c:\Users\secvency\Desktop\bumblebee_bot\handlers\global_admin.py'
with open(path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Find line numbers for the ga_bl function
start_line = None
end_line = None
for i, line in enumerate(lines):
    if '@router.callback_query(F.data.startswith("ga_bl:"))' in line:
        start_line = i
    if start_line and i > start_line + 5:
        if line.startswith('@router.') and i > start_line + 10:
            end_line = i
            break

print(f"on_ga_bl: lines {start_line+1} to {end_line}")
print("First line:", lines[start_line].rstrip())
print("Last line before end:", lines[end_line-1].rstrip())

# Build new function
new_func_lines = [
    '\n',
    '@router.callback_query(F.data.startswith("ga_bl:"))\n',
    'async def on_ga_bl(callback: CallbackQuery):\n',
    '    role, owner_id = await get_admin_context(callback.from_user.id, callback.from_user.username)\n',
    '    if not role:\n',
    '        return await callback.answer("\u274c \u041d\u0435\u0442 \u043f\u0440\u0430\u0432", show_alert=True)\n',
    '\n',
    '    async with get_pool().acquire() as conn:\n',
    '        bl_count = await conn.fetchval("SELECT COUNT(*) FROM blacklist WHERE owner_id=$1", owner_id) or 0\n',
    '        bl_active = await conn.fetchval(\n',
    '            "SELECT blacklist_active FROM platform_users WHERE user_id=$1", owner_id\n',
    '        )\n',
    '        if bl_active is None:\n',
    '            bl_active = True\n',
    '        active_bots = await conn.fetch("""\n',
    '            SELECT bot_username FROM child_bots\n',
    '            WHERE owner_id=$1 AND in_global_network=true\n',
    '            ORDER BY created_at ASC\n',
    '        """, owner_id)\n',
    '\n',
    '    bots_list = ("\\n".join(f"\u2022 @{r[\'bot_username\']}" for r in active_bots)\n',
    '                 if active_bots else\n',
    '                 "\u274e \u041d\u0435\u0442 \u0430\u043a\u0442\u0438\u0432\u043d\u044b\u0445 \u0431\u043e\u0442\u043e\u0432. \u041f\u0435\u0440\u0435\u0439\u0434\u0438\u0442\u0435 \u0432 \'\U0001f5c4\ufe0f \u0423\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u0438\u0435 \u043e\u0431\u0449\u0435\u0439 \u0431\u0430\u0437\u043e\u0439\'")\n',
    '\n',
    '    if bl_active:\n',
    '        shield = "\U0001f6e1\ufe0f <b>\u0417\u0430\u0449\u0438\u0442\u0430 \u0410\u041a\u0422\u0418\u0412\u041d\u0410</b> \u2014 \u0437\u0430\u043f\u0438\u0441\u0438 \u0427\u0421 \u0431\u043b\u043e\u043a\u0438\u0440\u0443\u044e\u0442 \u0432\u0445\u043e\u0434"\n',
    '        toggle_text = "\u2705 \u0427\u0421: \u0412\u043a\u043b\u044e\u0447\u0451\u043d \U0001f7e2  \u2014  \u043d\u0430\u0436\u0430\u0442\u044c \u0447\u0442\u043e\u0431\u044b \u0432\u044b\u043a\u043b\u044e\u0447\u0438\u0442\u044c"\n',
    '    else:\n',
    '        shield = "\u26a0\ufe0f <b>\u0417\u0430\u0449\u0438\u0442\u0430 \u0412\u042b\u041a\u041b\u042e\u0427\u0415\u041d\u0410</b> \u2014 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u0438 \u0438\u0437 \u0427\u0421 \u043c\u043e\u0433\u0443\u0442 \u0432\u0445\u043e\u0434\u0438\u0442\u044c"\n',
    '        toggle_text = "\u26d4 \u0427\u0421: \u0412\u044b\u043a\u043b\u044e\u0447\u0435\u043d \U0001f534  \u2014  \u043d\u0430\u0436\u0430\u0442\u044c \u0447\u0442\u043e\u0431\u044b \u0432\u043a\u043b\u044e\u0447\u0438\u0442\u044c"\n',
    '\n',
    '    text = (\n',
    '        "\U0001f6ab <b>\u0413\u043b\u043e\u0431\u0430\u043b\u044c\u043d\u044b\u0439 \u0427\u0451\u0440\u043d\u044b\u0439 \u0421\u043f\u0438\u0441\u043e\u043a</b>\\n"\n',
    '        "\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\\n"\n',
    '        f"{shield}\\n\\n"\n',
    '        f"\U0001f4c2 \u0417\u0430\u043f\u0438\u0441\u0435\u0439 \u0432 \u0431\u0430\u0437\u0435: <b>{bl_count}</b>\\n\\n"\n',
    '        f"\U0001f916 <b>\u0420\u0430\u0441\u043f\u0440\u043e\u0441\u0442\u0440\u0430\u043d\u044f\u0435\u0442\u0441\u044f \u043d\u0430 \u0431\u043e\u0442\u043e\u0432:</b>\\n{bots_list}\\n\\n"\n',
    '        "<i>\u0423\u043f\u0440\u0430\u0432\u043b\u044f\u0442\u044c \u0431\u043e\u0442\u0430\u043c\u0438 \u2014 \'\U0001f5c4\ufe0f \u0423\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u0438\u0435 \u043e\u0431\u0449\u0435\u0439 \u0431\u0430\u0437\u043e\u0439\'</i>"\n',
    '    )\n',
    '\n',
    '    kb = [\n',
    '        [InlineKeyboardButton(text=toggle_text, callback_data=f"ga_bl_master:{owner_id}")],\n',
    '        [\n',
    '            InlineKeyboardButton(text="\u2795 \u0414\u043e\u0431\u0430\u0432\u0438\u0442\u044c \u0432 \u0427\u0421", callback_data=f"ga_bl_add:{owner_id}"),\n',
    '            InlineKeyboardButton(text="\u2796 \u0423\u0434\u0430\u043b\u0438\u0442\u044c \u0438\u0437 \u0427\u0421", callback_data=f"ga_bl_del:{owner_id}")\n',
    '        ],\n',
    '        [InlineKeyboardButton(text="\U0001f4e5 \u0421\u043a\u0430\u0447\u0430\u0442\u044c \u0427\u0421 (CSV)", callback_data=f"ga_bl_export_csv:{owner_id}")],\n',
    '        [InlineKeyboardButton(text="\u25c0\ufe0f \u041d\u0430\u0437\u0430\u0434", callback_data=f"ga_main:{owner_id}")]\n',
    '    ]\n',
    '\n',
    '    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))\n',
    '    await callback.answer()\n',
    '\n',
    '\n',
    '@router.callback_query(F.data.startswith("ga_bl_master:"))\n',
    'async def on_ga_bl_master_toggle(callback: CallbackQuery):\n',
    '    """Master toggle: enable/disable global blacklist enforcement."""\n',
    '    owner_id = int(callback.data.split(":")[1])\n',
    '    role, _ = await get_admin_context(callback.from_user.id, callback.from_user.username)\n',
    '    if not role:\n',
    '        return await callback.answer("\u274c \u041d\u0435\u0442 \u043f\u0440\u0430\u0432", show_alert=True)\n',
    '\n',
    '    async with get_pool().acquire() as conn:\n',
    '        current = await conn.fetchval(\n',
    '            "SELECT blacklist_active FROM platform_users WHERE user_id=$1", owner_id\n',
    '        )\n',
    '        new_val = not (current if current is not None else True)\n',
    '        await conn.execute(\n',
    '            "UPDATE platform_users SET blacklist_active=$1 WHERE user_id=$2",\n',
    '            new_val, owner_id\n',
    '        )\n',
    '        await conn.execute("""\n',
    '            INSERT INTO audit_log (owner_id, user_id, action, details)\n',
    '            VALUES ($1, $2, \'bl_toggle\', $3)\n',
    '        """, owner_id, callback.from_user.id,\n',
    '            "Blacklist ENABLED" if new_val else "Blacklist DISABLED")\n',
    '\n',
    '    alert = ("\u2705 \u0427\u0421 \u0432\u043a\u043b\u044e\u0447\u0451\u043d \u2014 \u0437\u0430\u0449\u0438\u0442\u0430 \u0430\u043a\u0442\u0438\u0432\u043d\u0430, \u0437\u0430\u043f\u0438\u0441\u0438 \u0431\u043b\u043e\u043a\u0438\u0440\u0443\u044e\u0442 \u0432\u0445\u043e\u0434" if new_val\n',
    '            else "\u26d4 \u0427\u0421 \u0432\u044b\u043a\u043b\u044e\u0447\u0435\u043d \u2014 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u0438 \u0438\u0437 \u0427\u0421 \u043c\u043e\u0433\u0443\u0442 \u0432\u043e\u0439\u0442\u0438")\n',
    '    await callback.answer(alert, show_alert=True)\n',
    '    callback.data = f"ga_bl:{owner_id}"\n',
    '    await on_ga_bl(callback)\n',
    '\n',
    '\n',
]

new_lines = lines[:start_line] + new_func_lines + lines[end_line:]
with open(path, 'w', encoding='utf-8') as f:
    f.writelines(new_lines)
print("SUCCESS: on_ga_bl replaced with master toggle version!")
print(f"File now has {len(new_lines)} lines")
