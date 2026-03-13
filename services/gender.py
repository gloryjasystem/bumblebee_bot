"""
services/gender.py — Определение пола по имени пользователя.

Использует библиотеку gender-guesser (40k+ имён, мультиязычная).
Если имя не в базе — fallback по русским суффиксам.
"""
import gender_guesser.detector as _gg

_detector = _gg.Detector(case_sensitive=False)

# Типичные русские суффиксы женских/мужских имён (проверяем без ударений)
_FEMALE_ENDS = ("а", "я", "ия", "ья", "ия")
_MALE_ENDS = ("й", "ий", "ей", "ыи", "ых", "ев", "ов")


def guess_gender(first_name: str) -> str | None:
    """
    Определяет пол по имени.
    Возвращает 'M', 'F', или None если не удалось определить.
    """
    if not first_name or not first_name.strip():
        return None

    name = first_name.strip()

    # Сначала пробуем gender-guesser (40k+ имён EN/DE/RU и др.)
    result = _detector.get_gender(name)
    if result == "male":
        return "M"
    if result == "female":
        return "F"
    if result == "mostly_male":
        return "M"
    if result == "mostly_female":
        return "F"

    # Fallback: анализ по суффиксам (хорошо работает для русских имён)
    n = name.lower()
    if n.endswith(_FEMALE_ENDS):
        return "F"
    if n.endswith(_MALE_ENDS):
        return "M"

    return None
