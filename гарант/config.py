"""
Настройки Telegram-магазина подписок.

Перед запуском заполните BOT_TOKEN и ADMIN_IDS.
"""

# Токен бота, который выдаёт @BotFather
BOT_TOKEN = "8842537708:AAE8LY5GFq4kTXG4ffDq8zWCMntohhop5l4"

# Telegram ID администраторов (целые числа)
ADMIN_IDS = [6358456805]

# Файл базы данных SQLite (создаётся автоматически при первом запуске)
DB_PATH = "shop.db"

# Язык по умолчанию для новых пользователей
DEFAULT_LANGUAGE = "ru"

# Доступные языки интерфейса
LANGUAGES = {
    "ru": "🇷🇺 Русский",
    "en": "🇬🇧 English",
}

# Валюта, отображаемая в ценах
CURRENCY = "₽"

# Номер заказа для покупателя = внутренний id + это смещение (заказ №1042 и т.п.)
ORDER_NUMBER_OFFSET = 1000

# Максимальный размер чека в мегабайтах
MAX_CHECK_SIZE_MB = 20

# Сколько заказов показывать в списках админ-панели
ORDERS_LIST_LIMIT = 30

# Пауза между сообщениями при рассылке (секунды), защищает от лимитов Telegram
BROADCAST_DELAY = 0.05

# Реквизиты по умолчанию (редактируются в админ-панели)
DEFAULT_REQUISITES = "2200 **** **** 1234"

# Категории каталога: внутренний ключ -> отображаемое название
CATEGORIES = {
    "chatgpt": "ChatGPT",
    "claude": "Claude",
    "grok": "Grok",
    "gemini": "Gemini",
    "spotify": "Spotify",
    "youtube": "YouTube",
    "capcut": "CapCut",
    "cursor": "Cursor",
}

# Расположение кнопок категорий в каталоге (по две в ряд)
CATALOG_LAYOUT = [
    ["chatgpt", "claude"],
    ["grok", "gemini"],
    ["spotify", "youtube"],
    ["capcut", "cursor"],
]