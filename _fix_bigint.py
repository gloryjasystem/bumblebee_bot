import re, sys

files = [
    "handlers/channel_settings.py",
    "handlers/blacklist.py",
]

for path in files:
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    # Replace "chat_id=$N" (without existing ::bigint) with "chat_id=$N::bigint"
    fixed = re.sub(r"chat_id=\$(\d+)(?!::bigint)", r"chat_id=$\1::bigint", content)
    count = content.count("chat_id=$") - fixed.count("chat_id=$")
    with open(path, "w", encoding="utf-8") as f:
        f.write(fixed)
    print(f"{path}: fixed {count} occurrences")

print("Done!")
