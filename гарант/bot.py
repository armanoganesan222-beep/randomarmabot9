"""
Telegram-магазин ИИ-подписок и цифровых подписок.
aiogram 3 + SQLite (aiosqlite). Запуск: python bot.py
"""

import asyncio
import json
import logging
import re
import sys
from datetime import datetime, timezone
from typing import Any

import aiosqlite
from aiogram import Bot, Dispatcher, F, Router
from aiogram.dispatcher.middlewares.base import BaseMiddleware
from aiogram.exceptions import (
    TelegramAPIError,
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramRetryAfter,
)
from aiogram.filters import BaseFilter, Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    MessageEntity,
    InputMediaPhoto,
    User,
)

from config import (
    ADMIN_IDS,
    BOT_TOKEN,
    BROADCAST_DELAY,
    CATALOG_LAYOUT,
    CATEGORIES,
    CURRENCY,
    DB_PATH,
    DEFAULT_LANGUAGE,
    DEFAULT_REQUISITES,
    LANGUAGES,
    MAX_CHECK_SIZE_MB,
    ORDER_NUMBER_OFFSET,
    ORDERS_LIST_LIMIT,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("shop")

db: aiosqlite.Connection | None = None
WRITE_LOCK = asyncio.Lock()
BROADCAST_RUNNING = False
BACKGROUND_TASKS: set[asyncio.Task] = set()


# ============================================================================
# ТЕКСТЫ ПО УМОЛЧАНИЮ
# ============================================================================

# Редактируемые тексты: ключ -> {язык: текст}. Форматирование хранится в БД.
DEFAULT_TEXTS: dict[str, dict[str, str]] = {
    "welcome": {
        "ru": "✨ Добро пожаловать, {name}!\n\nЗдесь вы можете оформить подписки на ИИ-сервисы и цифровые сервисы.\n\nВыберите раздел ниже 👇",
        "en": "✨ Welcome, {name}!\n\nHere you can order AI and digital service subscriptions.\n\nChoose a section below 👇",
    },
    "catalog": {
        "ru": "🛍 Каталог\n\nВыберите категорию:",
        "en": "🛍 Catalog\n\nChoose a category:",
    },
    "category_title": {
        "ru": "💎 {category}\n\nВыберите подписку:",
        "en": "💎 {category}\n\nChoose a subscription:",
    },
    "category_empty": {
        "ru": "😔 В этой категории пока нет товаров.",
        "en": "😔 There are no products in this category yet.",
    },
    "product_card": {
        "ru": "💎 {product_name}\n\n💰 Цена: {price} ₽\n📦 Наличие: {stock}\n\n📝 Описание:\n{description}",
        "en": "💎 {product_name}\n\n💰 Price: {price} ₽\n📦 Availability: {stock}\n\n📝 Description:\n{description}",
    },
    "stock_in": {"ru": "В наличии", "en": "In stock"},
    "stock_out": {"ru": "Нет в наличии", "en": "Out of stock"},
    "payment": {
        "ru": "💳 Оплата заказа №{order_id}\n\n📦 Товар: {product}\n💰 Сумма: {price} ₽\n\n🏦 Реквизиты:\n{requisites}\n\n📎 После оплаты отправьте чек фотографией или документом (PDF).",
        "en": "💳 Payment for order №{order_id}\n\n📦 Product: {product}\n💰 Amount: {price} ₽\n\n🏦 Payment details:\n{requisites}\n\n📎 After paying, send the receipt as a photo or document (PDF).",
    },
    "check_sent": {
        "ru": "✅ Чек получен!\n\nЗаказ ожидает проверки администратором.",
        "en": "✅ Receipt received!\n\nThe order is waiting for administrator review.",
    },
    "paid_notice": {
        "ru": "✅ Оплата заказа №{order_id} подтверждена.\n\nОжидайте выдачу товара.",
        "en": "✅ Payment for order №{order_id} confirmed.\n\nPlease wait for the product delivery.",
    },
    "rejected_notice": {
        "ru": "❌ Заказ №{order_id} отклонён.\n\n📝 Причина: {reason}",
        "en": "❌ Order №{order_id} was rejected.\n\n📝 Reason: {reason}",
    },
    "delivered_notice": {
        "ru": "🎉 Заказ №{order_id} выполнен. Спасибо за покупку!",
        "en": "🎉 Order №{order_id} is completed. Thank you for your purchase!",
    },
    "profile": {
        "ru": "👤 Ваш профиль\n\n🆔 ID пользователя: {user_id}\n📅 Дата регистрации: {created}\n🛍 Количество покупок: {count}\n💰 Сумма покупок: {sum} ₽",
        "en": "👤 Your profile\n\n🆔 User ID: {user_id}\n📅 Registered: {created}\n🛍 Purchases: {count}\n💰 Total spent: {sum} ₽",
    },
    "orders_title": {
        "ru": "📋 Мои заказы\n\nВыберите заказ:",
        "en": "📋 My orders\n\nChoose an order:",
    },
    "orders_empty": {
        "ru": "📭 У вас пока нет заказов.",
        "en": "📭 You have no orders yet.",
    },
    "order_detail": {
        "ru": "🧾 Заказ №{order_id}\n\n📦 Товар: {product}\n💰 Цена: {price} ₽\n📅 Дата: {date}\n📌 Статус: {status}\n📝 Комментарий: {reason}",
        "en": "🧾 Order №{order_id}\n\n📦 Product: {product}\n💰 Price: {price} ₽\n📅 Date: {date}\n📌 Status: {status}\n📝 Comment: {reason}",
    },
    "support": {
        "ru": "🆘 Поддержка\n\nНажмите кнопку ниже, чтобы написать нам.",
        "en": "🆘 Support\n\nPress the button below to contact us.",
    },
    "info": {
        "ru": "ℹ️ Информация о магазине\n\nТекст раздела можно изменить в админ-панели.",
        "en": "ℹ️ Store information\n\nThis text can be edited in the admin panel.",
    },
    "language": {
        "ru": "🌐 Выберите язык интерфейса:",
        "en": "🌐 Choose interface language:",
    },
    "requisites": {
        "ru": DEFAULT_REQUISITES,
        "en": DEFAULT_REQUISITES,
    },
}

# Кнопки: id -> (текст ru, текст en, эмодзи)
DEFAULT_BUTTONS: dict[str, tuple[str, str, str]] = {
    "main_products": ("Товары", "Products", "🛍"),
    "main_profile": ("Профиль", "Profile", "👤"),
    "main_support": ("Поддержка", "Support", "🆘"),
    "main_info": ("Информация", "Information", "ℹ️"),
    "main_language": ("Язык", "Language", "🌐"),
    "nav_back": ("Назад", "Back", "◀️"),
    "nav_main": ("Главное меню", "Main menu", "🏠"),
    "profile_orders": ("Мои заказы", "My orders", "📋"),
    "prod_buy": ("Купить", "Buy", "💳"),
    "pay_cancel": ("Отменить заказ", "Cancel order", "❌"),
    "support_contact": ("Написать в поддержку", "Contact support", "💬"),
    "lang_ru": ("🇷🇺 Русский", "🇷🇺 Русский", ""),
    "lang_en": ("🇬🇧 English", "🇬🇧 English", ""),
    "cat_chatgpt": ("ChatGPT", "ChatGPT", ""),
    "cat_claude": ("Claude", "Claude", ""),
    "cat_grok": ("Grok", "Grok", ""),
    "cat_gemini": ("Gemini", "Gemini", ""),
    "cat_spotify": ("Spotify", "Spotify", ""),
    "cat_youtube": ("YouTube", "YouTube", ""),
    "cat_capcut": ("CapCut", "CapCut", ""),
    "cat_cursor": ("Cursor", "Cursor", ""),
    "adm_products": ("Управление товарами", "Manage products", "📦"),
    "adm_orders": ("Заявки", "Orders", "🧾"),
    "adm_requisites": ("Реквизиты", "Requisites", "🏦"),
    "adm_texts": ("Редактор текстов", "Text editor", "📝"),
    "adm_buttons": ("Редактор кнопок", "Button editor", "🔘"),
    "adm_stats": ("Статистика", "Statistics", "📊"),
    "adm_broadcast": ("Рассылка", "Broadcast", "📢"),
    "adm_settings": ("Настройки", "Settings", "⚙️"),
    "adm_photos": ("Фото разделов", "Section photos", "🖼"),
    "adm_add_product": ("Добавить товар", "Add product", "➕"),
    "adm_f_new": ("Новые", "New", "🆕"),
    "adm_f_paid": ("Оплаченные", "Paid", "💰"),
    "adm_f_done": ("Выполненные", "Completed", "✅"),
    "adm_f_rejected": ("Отклонённые", "Rejected", "🚫"),
    "adm_f_all": ("Все заявки", "All orders", "📚"),
    "order_ok": ("Подтвердить оплату", "Confirm payment", "✅"),
    "order_reject": ("Отклонить", "Reject", "🚫"),
    "order_deliver": ("Выдать товар", "Deliver product", "📤"),
    "order_write": ("Написать покупателю", "Message buyer", "✉️"),
    "order_skip": ("Пропустить", "Skip", "⏭"),
}

# Интерфейсные строки покупателя (не редактируются через админку)
L: dict[str, dict[str, str]] = {
    "ru": {
        "unavailable": "⛔ Товар сейчас недоступен.",
        "out_of_stock": "⛔ Товара нет в наличии. Покупка недоступна.",
        "not_found": "❗ Заказ не найден.",
        "not_pending": "❗ Заказ уже не ожидает оплаты.",
        "check_wrong_type": "❗ Поддерживаются фото, изображения и PDF-файлы.",
        "check_too_big": f"❗ Файл слишком большой (максимум {MAX_CHECK_SIZE_MB} МБ).",
        "check_dup": "❗ Этот чек уже прикреплён к другому заказу.",
        "no_pending_hint": "Чтобы отправить чек, откройте «Мои заказы», выберите заказ и нажмите «Оплатить».",
        "cancel_ok": "❌ Заказ отменён.",
        "cancel_fail": "❗ Этот заказ нельзя отменить.",
        "lang_changed": "✅ Язык изменён.",
        "pay_btn": "💳 Оплатить",
    },
    "en": {
        "unavailable": "⛔ This product is currently unavailable.",
        "out_of_stock": "⛔ Out of stock. Purchase is not available.",
        "not_found": "❗ Order not found.",
        "not_pending": "❗ The order is no longer awaiting payment.",
        "check_wrong_type": "❗ Only photos, images and PDF files are accepted.",
        "check_too_big": f"❗ The file is too large (maximum {MAX_CHECK_SIZE_MB} MB).",
        "check_dup": "❗ This receipt is already attached to another order.",
        "no_pending_hint": "To send a receipt, open «My orders», choose the order and press «Pay».",
        "cancel_ok": "❌ Order cancelled.",
        "cancel_fail": "❗ This order cannot be cancelled.",
        "lang_changed": "✅ Language changed.",
        "pay_btn": "💳 Pay",
    },
}

STATUS_NAMES: dict[str, dict[str, str]] = {
    "ru": {
        "pending_payment": "Ожидает оплаты",
        "checking": "Проверка чека",
        "paid": "Оплачен",
        "delivering": "Выдаётся",
        "done": "Выполнен",
        "rejected": "Отклонён",
        "cancelled": "Отменён",
    },
    "en": {
        "pending_payment": "Awaiting payment",
        "checking": "Receipt under review",
        "paid": "Paid",
        "delivering": "Delivering",
        "done": "Completed",
        "rejected": "Rejected",
        "cancelled": "Cancelled",
    },
}

# Список редактируемых текстов в админ-панели
TEXT_EDITOR_KEYS: list[tuple[str, str]] = [
    ("welcome", "Приветствие"),
    ("catalog", "Текст каталога"),
    ("category_title", "Текст категории"),
    ("category_empty", "Пустая категория"),
    ("product_card", "Карточка товара"),
    ("stock_in", "Статус: в наличии"),
    ("stock_out", "Статус: нет в наличии"),
    ("payment", "Текст оплаты"),
    ("check_sent", "После отправки чека"),
    ("paid_notice", "Уведомление об оплате"),
    ("rejected_notice", "Уведомление об отклонении"),
    ("delivered_notice", "Уведомление о выдаче"),
    ("profile", "Профиль"),
    ("orders_title", "Заголовок «Мои заказы»"),
    ("orders_empty", "Нет заказов"),
    ("order_detail", "Карточка заказа"),
    ("support", "Поддержка"),
    ("info", "Информация"),
    ("language", "Выбор языка"),
]

# Значения переменных для предпросмотра в админ-панели
SAMPLE_VARS: dict[str, str] = {
    "name": "Иван",
    "category": "Gemini",
    "product_name": "Gemini Pro 18",
    "price": "49",
    "stock": "В наличии",
    "description": "Пример описания товара",
    "user_id": "123456789",
    "created": "2026-01-01",
    "count": "3",
    "sum": "147",
    "order_id": "1042",
    "product": "Gemini Pro 18",
    "date": "2026-10-20",
    "status": "Оплачен",
    "reason": "Пример причины",
    "requisites": "2200 **** **** 1234",
}


# ============================================================================
# БАЗА ДАННЫХ
# ============================================================================

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    language TEXT NOT NULL DEFAULT 'ru',
    created_at TEXT NOT NULL,
    last_seen TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    category TEXT NOT NULL,
    name TEXT NOT NULL,
    price INTEGER NOT NULL CHECK (price > 0),
    description TEXT NOT NULL DEFAULT '',
    description_entities TEXT NOT NULL DEFAULT '[]',
    is_active INTEGER NOT NULL DEFAULT 1,
    in_stock INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    username TEXT,
    product_id INTEGER,
    product_name TEXT NOT NULL,
    price INTEGER NOT NULL CHECK (price > 0),
    requisites_text TEXT NOT NULL,
    requisites_entities TEXT NOT NULL DEFAULT '[]',
    status TEXT NOT NULL,
    check_file_id TEXT,
    check_unique_id TEXT UNIQUE,
    check_kind TEXT,
    reject_reason TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

CREATE INDEX IF NOT EXISTS idx_orders_user ON orders(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);

CREATE TABLE IF NOT EXISTS order_status_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER NOT NULL,
    status TEXT NOT NULL,
    actor TEXT NOT NULL,
    note TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(id)
);

CREATE TABLE IF NOT EXISTS texts (
    key TEXT NOT NULL,
    lang TEXT NOT NULL,
    text TEXT NOT NULL,
    entities TEXT NOT NULL DEFAULT '[]',
    PRIMARY KEY (key, lang)
);

CREATE TABLE IF NOT EXISTS buttons (
    btn_id TEXT NOT NULL,
    lang TEXT NOT NULL,
    text TEXT NOT NULL,
    emoji TEXT NOT NULL DEFAULT '',
    icon_custom_emoji_id TEXT,
    PRIMARY KEY (btn_id, lang)
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS photos (
    key TEXT PRIMARY KEY,
    file_id TEXT NOT NULL
);
"""


async def init_db() -> None:
    """Создаёт соединение и таблицы, заполняет значения по умолчанию."""
    global db
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA foreign_keys = ON")
    await db.execute("PRAGMA journal_mode = WAL")
    await db.executescript(SCHEMA)
    await seed_defaults()


async def seed_defaults() -> None:
    async with WRITE_LOCK:
        for key, langs in DEFAULT_TEXTS.items():
            for lang, text in langs.items():
                await db.execute(
                    "INSERT OR IGNORE INTO texts (key, lang, text, entities) VALUES (?, ?, ?, '[]')",
                    (key, lang, text),
                )
        for btn_id, (ru, en, emoji) in DEFAULT_BUTTONS.items():
            for lang, label in (("ru", ru), ("en", en)):
                await db.execute(
                    "INSERT OR IGNORE INTO buttons (btn_id, lang, text, emoji, icon_custom_emoji_id) "
                    "VALUES (?, ?, ?, ?, NULL)",
                    (btn_id, lang, label, emoji),
                )
        await db.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('support_username', '')")
        await db.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('support_url', '')")
        await db.commit()


async def fetchone(sql: str, params: tuple = ()) -> aiosqlite.Row | None:
    async with db.execute(sql, params) as cur:
        return await cur.fetchone()


async def fetchall(sql: str, params: tuple = ()) -> list[aiosqlite.Row]:
    async with db.execute(sql, params) as cur:
        return await cur.fetchall()


async def scalar(sql: str, params: tuple = ()) -> Any:
    row = await fetchone(sql, params)
    return row[0] if row and row[0] is not None else 0


async def run(sql: str, params: tuple = ()) -> int:
    """Выполняет запись под блокировкой, возвращает количество изменённых строк."""
    async with WRITE_LOCK:
        cur = await db.execute(sql, params)
        await db.commit()
        return cur.rowcount


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def today_prefix() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def fmt_money(value: int) -> str:
    return f"{int(value):,}".replace(",", " ")


def order_number(order_id: int) -> int:
    return order_id + ORDER_NUMBER_OFFSET


def buyer_link(user_id: int) -> str:
    return f"tg://user?id={user_id}"


# --- пользователи -----------------------------------------------------------

async def upsert_user(user: User) -> None:
    now = now_iso()
    await run(
        """INSERT INTO users (user_id, username, first_name, language, created_at, last_seen)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(user_id) DO UPDATE SET
               username = excluded.username,
               first_name = excluded.first_name,
               last_seen = excluded.last_seen""",
        (user.id, user.username, user.first_name, DEFAULT_LANGUAGE, now, now),
    )


async def get_lang(user_id: int) -> str:
    row = await fetchone("SELECT language FROM users WHERE user_id = ?", (user_id,))
    return row["language"] if row else DEFAULT_LANGUAGE


async def set_lang(user_id: int, lang: str) -> None:
    await run("UPDATE users SET language = ? WHERE user_id = ?", (lang, user_id))


# --- настройки --------------------------------------------------------------

async def get_setting(key: str, default: str = "") -> str:
    row = await fetchone("SELECT value FROM settings WHERE key = ?", (key,))
    return row["value"] if row else default


async def set_setting(key: str, value: str) -> None:
    await run(
        "INSERT INTO settings (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )


# --- тексты -----------------------------------------------------------------

PHOTO_SECTIONS: list[tuple[str, str]] = [
    ("main", "Главное меню"),
    ("catalog", "Каталог"),
    *[(f"category:{key}", f"Категория: {name}") for key, name in CATEGORIES.items()],
    ("profile", "Профиль"),
    ("orders", "Мои заказы"),
    ("payment", "Оплата"),
    ("support", "Поддержка"),
    ("info", "Информация"),
    ("language", "Язык"),
]


async def get_photo(key: str) -> str | None:
    row = await fetchone("SELECT file_id FROM photos WHERE key = ?", (key,))
    return row["file_id"] if row else None


async def set_photo(key: str, file_id: str) -> None:
    await run(
        "INSERT INTO photos (key, file_id) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET file_id = excluded.file_id",
        (key, file_id),
    )


async def delete_photo(key: str) -> None:
    await run("DELETE FROM photos WHERE key = ?", (key,))


async def get_text(key: str, lang: str) -> tuple[str, list[dict]]:
    row = await fetchone("SELECT text, entities FROM texts WHERE key = ? AND lang = ?", (key, lang))
    if row:
        return row["text"], json.loads(row["entities"] or "[]")
    fallback = DEFAULT_TEXTS.get(key, {}).get(lang, "")
    return fallback, []


async def plain_text(key: str, lang: str) -> str:
    return (await get_text(key, lang))[0]


async def save_text(key: str, lang: str, text: str, entities: list[dict]) -> None:
    await run(
        """INSERT INTO texts (key, lang, text, entities) VALUES (?, ?, ?, ?)
           ON CONFLICT(key, lang) DO UPDATE SET
               text = excluded.text, entities = excluded.entities""",
        (key, lang, text, json.dumps(entities, ensure_ascii=False)),
    )


async def render_text(key: str, lang: str, **variables: Any) -> tuple[str, list[dict]]:
    text, entities = await get_text(key, lang)
    return render(text, entities, variables)


# --- кнопки -----------------------------------------------------------------

async def get_button(btn_id: str, lang: str) -> dict:
    row = await fetchone(
        "SELECT text, emoji, icon_custom_emoji_id FROM buttons WHERE btn_id = ? AND lang = ?",
        (btn_id, lang),
    )
    if row:
        return {"text": row["text"], "emoji": row["emoji"] or "", "icon": row["icon_custom_emoji_id"]}
    ru, en, emoji = DEFAULT_BUTTONS.get(btn_id, (btn_id, btn_id, ""))
    return {"text": en if lang == "en" else ru, "emoji": emoji, "icon": None}


async def keyboard(lang: str, rows: list[list[dict]]) -> InlineKeyboardMarkup:
    """
    Строит inline-клавиатуру.
    Элемент строки: {"id": btn_id, "cb": callback_data} — кнопка из редактора;
                    {"id": btn_id, "url": link}           — ссылка из редактора;
                    {"text": label, "cb": callback_data}  — динамическая кнопка;
                    {"text": label, "url": link}          — динамическая ссылка.
    """
    markup_rows: list[list[InlineKeyboardButton]] = []
    for row in rows:
        line: list[InlineKeyboardButton] = []
        for item in row:
            if "id" in item:
                btn = await get_button(item["id"], lang)
                label, emoji, icon = btn["text"], btn["emoji"], btn["icon"]
            else:
                label, emoji, icon = item["text"], "", None
            if emoji and not icon:
                label = f"{emoji} {label}"
            kwargs: dict[str, Any] = {"text": label}
            if icon:
                kwargs["icon_custom_emoji_id"] = icon
            if "url" in item:
                kwargs["url"] = item["url"]
            else:
                kwargs["callback_data"] = item["cb"]
            line.append(InlineKeyboardButton(**kwargs))
        markup_rows.append(line)
    return InlineKeyboardMarkup(inline_keyboard=markup_rows)


# --- товары -----------------------------------------------------------------

async def get_product(product_id: int) -> aiosqlite.Row | None:
    return await fetchone("SELECT * FROM products WHERE id = ?", (product_id,))


async def render_product_card(product: aiosqlite.Row, lang: str) -> tuple[str, list[dict]]:
    stock_key = "stock_in" if product["in_stock"] else "stock_out"
    return await render_text(
        "product_card",
        lang,
        product_name=product["name"],
        price=fmt_money(product["price"]),
        stock=await plain_text(stock_key, lang),
        description=(product["description"], json.loads(product["description_entities"] or "[]")),
    )


async def insert_product(data: dict) -> int:
    async with WRITE_LOCK:
        cur = await db.execute(
            """INSERT INTO products (category, name, price, description, description_entities,
                                     is_active, in_stock, created_at)
               VALUES (?, ?, ?, ?, ?, 1, ?, ?)""",
            (
                data["category"],
                data["name"],
                int(data["price"]),
                data["description"],
                json.dumps(data["description_entities"], ensure_ascii=False),
                1 if data["in_stock"] else 0,
                now_iso(),
            ),
        )
        product_id = cur.lastrowid
        await db.commit()
    return product_id


# --- заказы -----------------------------------------------------------------

async def get_order(order_id: int) -> aiosqlite.Row | None:
    return await fetchone("SELECT * FROM orders WHERE id = ?", (order_id,))


async def create_order(user: User, product: aiosqlite.Row) -> int:
    """Создаёт заказ со снимком цены и реквизитов."""
    req_text, req_entities = await get_text("requisites", DEFAULT_LANGUAGE)
    now = now_iso()
    async with WRITE_LOCK:
        cur = await db.execute(
            """INSERT INTO orders (user_id, username, product_id, product_name, price,
                                   requisites_text, requisites_entities, status, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'pending_payment', ?, ?)""",
            (
                user.id,
                user.username,
                product["id"],
                product["name"],
                product["price"],
                req_text,
                json.dumps(req_entities, ensure_ascii=False),
                now,
                now,
            ),
        )
        order_id = cur.lastrowid
        await db.execute(
            "INSERT INTO order_status_history (order_id, status, actor, note, created_at) "
            "VALUES (?, 'pending_payment', ?, NULL, ?)",
            (order_id, f"user:{user.id}", now),
        )
        await db.commit()
    return order_id


async def change_status(
    order_id: int,
    from_statuses: tuple[str, ...],
    to_status: str,
    actor: str,
    note: str | None = None,
    reason: str | None = None,
) -> bool:
    """
    Атомарно меняет статус, только если заказ сейчас в одном из from_statuses.
    Защищает от двойного подтверждения и гонок между администраторами.
    """
    now = now_iso()
    placeholders = ",".join("?" * len(from_statuses))
    async with WRITE_LOCK:
        cur = await db.execute(
            f"""UPDATE orders SET
                    status = ?,
                    updated_at = ?,
                    reject_reason = COALESCE(?, reject_reason),
                    completed_at = CASE WHEN ? = 'done' THEN ? ELSE completed_at END
                WHERE id = ? AND status IN ({placeholders})""",
            (to_status, now, reason, to_status, now, order_id, *from_statuses),
        )
        if cur.rowcount != 1:
            await db.rollback()
            return False
        await db.execute(
            "INSERT INTO order_status_history (order_id, status, actor, note, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (order_id, to_status, actor, note, now),
        )
        await db.commit()
        return True


async def attach_check(order_id: int, user_id: int, file_id: str, unique_id: str, kind: str) -> str:
    """Прикрепляет чек к заказу. Возвращает 'ok', 'not_pending' или 'dup'."""
    now = now_iso()
    async with WRITE_LOCK:
        try:
            cur = await db.execute(
                """UPDATE orders SET status = 'checking', check_file_id = ?, check_unique_id = ?,
                       check_kind = ?, updated_at = ?
                   WHERE id = ? AND user_id = ? AND status = 'pending_payment'""",
                (file_id, unique_id, kind, now, order_id, user_id),
            )
        except aiosqlite.IntegrityError:
            await db.rollback()
            return "dup"
        if cur.rowcount != 1:
            await db.rollback()
            return "not_pending"
        await db.execute(
            "INSERT INTO order_status_history (order_id, status, actor, note, created_at) "
            "VALUES (?, 'checking', ?, NULL, ?)",
            (order_id, f"user:{user_id}", now),
        )
        await db.commit()
        return "ok"


# ============================================================================
# ENTITIES: СОХРАНЕНИЕ И ПОДСТАНОВКА ПЕРЕМЕННЫХ (UTF-16)
# ============================================================================

PLACEHOLDER_RE = re.compile(r"\{([a-z_]+)\}")


def utf16_len(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


def entities_to_dicts(entities: list[MessageEntity] | None) -> list[dict]:
    """Сохраняет все атрибуты entities (включая custom_emoji_id, url, user) в обычные dict."""
    return [entity.model_dump(exclude_none=True) for entity in (entities or [])]


def to_entities(entities: list[dict] | None) -> list[MessageEntity] | None:
    if not entities:
        return None
    return [MessageEntity(**entity) for entity in entities]


def render(template: str, entities: list[dict], variables: dict[str, Any] | None = None) -> tuple[str, list[dict]]:
    """
    Подставляет переменные вида {name} и пересчитывает смещения entities в UTF-16.

    Значение переменной может быть строкой или кортежем (text, entities) —
    тогда его собственные entities (например, Premium Emoji в описании товара)
    сохраняются и смещаются вместе с текстом.
    """
    variables = variables or {}
    template16 = template.encode("utf-16-le")

    replacements: list[tuple[int, int, bytes, int, list[dict]]] = []
    for match in PLACEHOLDER_RE.finditer(template):
        name = match.group(1)
        if name not in variables:
            continue
        value = variables[name]
        if isinstance(value, tuple):
            value_text, value_entities = value[0], list(value[1] or [])
        else:
            value_text, value_entities = str(value), []
        start = utf16_len(template[: match.start()])
        end = utf16_len(template[: match.end()])
        value_bytes = value_text.encode("utf-16-le")
        replacements.append((start, end, value_bytes, len(value_bytes) // 2, value_entities))

    out = b""
    prev = 0
    placed: list[tuple[int, int, int, int, list[dict]]] = []  # (start, end, new_start, new_len, entities)
    for start, end, value_bytes, value_len, value_entities in replacements:
        out += template16[prev * 2 : start * 2]
        new_start = len(out) // 2
        out += value_bytes
        placed.append((start, end, new_start, value_len, value_entities))
        prev = end
    out += template16[prev * 2 :]

    def map_pos(pos: int, is_end: bool) -> int:
        delta = 0
        for start, end, new_start, new_len, _ in placed:
            if pos < start:
                return pos + delta
            if pos == start:
                return new_start
            if pos < end:
                return new_start + new_len if is_end else new_start
            delta += new_len - (end - start)
        return pos + delta

    new_entities: list[dict] = []
    for entity in entities or []:
        ent_start = entity["offset"]
        ent_end = ent_start + entity["length"]
        mapped_start = map_pos(ent_start, False)
        mapped_end = map_pos(ent_end, True)
        if mapped_end > mapped_start:
            copied = dict(entity)
            copied["offset"] = mapped_start
            copied["length"] = mapped_end - mapped_start
            new_entities.append(copied)

    for _, _, new_start, _, value_entities in placed:
        for value_entity in value_entities:
            copied = dict(value_entity)
            copied["offset"] = value_entity["offset"] + new_start
            new_entities.append(copied)

    new_entities.sort(key=lambda item: item["offset"])
    return out.decode("utf-16-le"), new_entities


def extract_custom_emoji(text: str, entities: list[dict]) -> tuple[str, str | None]:
    """Возвращает (символ-заменитель, custom_emoji_id) из первого custom emoji в сообщении."""
    units = text.encode("utf-16-le")
    for entity in entities:
        if entity.get("type") == "custom_emoji":
            piece = units[entity["offset"] * 2 : (entity["offset"] + entity["length"]) * 2]
            return piece.decode("utf-16-le"), entity.get("custom_emoji_id")
    return text.strip()[:8], None


# ============================================================================
# ОТПРАВКА И РЕДАКТИРОВАНИЕ ЭКРАНОВ
# ============================================================================

async def _delete_quiet(message: Message) -> None:
    try:
        await message.delete()
    except TelegramAPIError:
        pass


async def screen(target: CallbackQuery | Message, text: str, entities: list[dict] | None = None,
                 markup: InlineKeyboardMarkup | None = None, photo_key: str | None = None) -> None:
    """Показывает экран. Если для раздела задано фото, оно выводится с подписью."""
    file_id = await get_photo(photo_key) if photo_key else None
    if not file_id:
        await _screen_plain(target, text, entities, markup)
        return
    is_callback = isinstance(target, CallbackQuery)
    message = target.message if is_callback else target
    if message is None:
        return
    entities_obj = to_entities(entities)
    if len(text) > 1024:
        if is_callback:
            await _delete_quiet(message)
        await message.answer_photo(file_id)
        await message.answer(text, entities=entities_obj, reply_markup=markup)
        return
    if is_callback and message.photo:
        try:
            await message.edit_media(
                InputMediaPhoto(media=file_id, caption=text, caption_entities=entities_obj),
                reply_markup=markup,
            )
            return
        except TelegramBadRequest as exc:
            if "not modified" in str(exc).lower():
                return
    if is_callback:
        await _delete_quiet(message)
    await message.answer_photo(file_id, caption=text, caption_entities=entities_obj, reply_markup=markup)


async def _screen_plain(target: CallbackQuery | Message, text: str, entities: list[dict] | None = None,
                        markup: InlineKeyboardMarkup | None = None) -> None:
    """Редактирует сообщение, если это возможно, иначе отправляет новое."""
    if isinstance(target, CallbackQuery):
        message = target.message
        if message is None:
            return
        try:
            await message.edit_text(text, entities=to_entities(entities), reply_markup=markup)
            return
        except TelegramBadRequest as exc:
            if "not modified" in str(exc).lower():
                return
        try:
            await message.delete()
        except TelegramAPIError:
            pass
        await message.answer(text, entities=to_entities(entities), reply_markup=markup)
    else:
        await target.answer(text, entities=to_entities(entities), reply_markup=markup)


async def show_key(target: CallbackQuery | Message, key: str, lang: str, rows: list[list[dict]],
                   photo_key: str | None = None, **variables: Any) -> None:
    text, entities = await render_text(key, lang, **variables)
    markup = await keyboard(lang, rows)
    await screen(target, text, entities, markup, photo_key=photo_key)


async def send_payload(bot: Bot, chat_id: int, data: dict, markup: InlineKeyboardMarkup | None = None) -> None:
    """Отправляет рассылочное сообщение (текст, фото или видео) с сохранением entities."""
    entities = to_entities(data.get("entities"))
    text = data.get("text") or None
    if data["kind"] == "photo":
        await bot.send_photo(chat_id, photo=data["file_id"], caption=text,
                             caption_entities=entities, reply_markup=markup)
    elif data["kind"] == "video":
        await bot.send_video(chat_id, video=data["file_id"], caption=text,
                             caption_entities=entities, reply_markup=markup)
    else:
        await bot.send_message(chat_id, text or " ", entities=entities, reply_markup=markup)


async def notify_user(bot: Bot, user_id: int, key: str, **variables: Any) -> None:
    lang = await get_lang(user_id)
    text, entities = await render_text(key, lang, **variables)
    try:
        await bot.send_message(user_id, text, entities=to_entities(entities))
    except TelegramAPIError as exc:
        log.warning("Не удалось отправить уведомление %s пользователю %s: %s", key, user_id, exc)


# ============================================================================
# FSM И ФИЛЬТРЫ
# ============================================================================

class UserStates(StatesGroup):
    awaiting_check = State()


class AdminStates(StatesGroup):
    add_category = State()
    add_name = State()
    add_price = State()
    add_desc = State()
    product_edit = State()
    reject_reason = State()
    deliver = State()
    requisites_edit = State()
    requisites_preview = State()
    text_edit = State()
    text_preview = State()
    button_text = State()
    button_emoji = State()
    support_username = State()
    support_url = State()
    broadcast_content = State()
    broadcast_preview = State()
    photo_set = State()


class IsAdmin(BaseFilter):
    async def __call__(self, event: Message | CallbackQuery) -> bool:
        user = event.from_user
        return bool(user and user.id in ADMIN_IDS)


class UserMiddleware(BaseMiddleware):
    """Регистрирует пользователя и обновляет время последнего визита."""

    async def __call__(self, handler, event, data):
        user = data.get("event_from_user")
        if user is not None and not user.is_bot:
            await upsert_user(user)
        return await handler(event, data)


# ============================================================================
# ПОЛЬЗОВАТЕЛЬСКАЯ ЧАСТЬ
# ============================================================================

user_router = Router(name="user")


def main_rows() -> list[list[dict]]:
    return [
        [{"id": "main_products", "cb": "nav:catalog"}],
        [{"id": "main_profile", "cb": "profile"}, {"id": "main_support", "cb": "support"}],
        [{"id": "main_info", "cb": "info"}],
        [{"id": "main_language", "cb": "lang"}],
    ]


async def main_view(lang: str, name: str) -> tuple[str, list[dict], InlineKeyboardMarkup]:
    text, entities = await render_text("welcome", lang, name=name)
    markup = await keyboard(lang, main_rows())
    return text, entities, markup


async def payment_screen(target: CallbackQuery | Message, order: aiosqlite.Row, lang: str) -> None:
    requisites = (order["requisites_text"], json.loads(order["requisites_entities"] or "[]"))
    rows = [
        [{"id": "pay_cancel", "cb": f"cancel:{order['id']}"}],
        [{"id": "nav_main", "cb": "nav:main"}],
    ]
    await show_key(
        target,
        "payment",
        lang,
        rows,
        photo_key="payment",
        order_id=order_number(order["id"]),
        product=order["product_name"],
        price=fmt_money(order["price"]),
        requisites=requisites,
    )


async def orders_screen(target: CallbackQuery | Message, lang: str, user_id: int) -> None:
    rows_db = await fetchall(
        "SELECT id, product_name, status FROM orders WHERE user_id = ? ORDER BY id DESC LIMIT 30",
        (user_id,),
    )
    if not rows_db:
        await show_key(target, "orders_empty", lang, [[{"id": "nav_back", "cb": "profile"}]],
                       photo_key="orders")
        return
    rows = [
        [{
            "text": f"№{order_number(r['id'])} · {r['product_name']} · {STATUS_NAMES[lang][r['status']]}",
            "cb": f"ord:{r['id']}",
        }]
        for r in rows_db
    ]
    rows.append([{"id": "nav_back", "cb": "profile"}])
    await show_key(target, "orders_title", lang, rows, photo_key="orders")


@user_router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext) -> None:
    await state.clear()
    lang = await get_lang(message.from_user.id)
    text, entities, markup = await main_view(lang, message.from_user.first_name or "")
    await screen(message, text, entities, markup, photo_key="main")


@user_router.callback_query(F.data == "nav:main")
async def cb_main(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    lang = await get_lang(callback.from_user.id)
    text, entities, markup = await main_view(lang, callback.from_user.first_name or "")
    await screen(callback, text, entities, markup, photo_key="main")
    await callback.answer()


@user_router.callback_query(F.data == "nav:catalog")
async def cb_catalog(callback: CallbackQuery) -> None:
    lang = await get_lang(callback.from_user.id)
    rows = [[{"id": f"cat_{key}", "cb": f"cat:{key}"} for key in row] for row in CATALOG_LAYOUT]
    rows.append([{"id": "nav_back", "cb": "nav:main"}])
    await show_key(callback, "catalog", lang, rows, photo_key="catalog")
    await callback.answer()


@user_router.callback_query(F.data.startswith("cat:"))
async def cb_category(callback: CallbackQuery) -> None:
    key = callback.data.split(":", 1)[1]
    lang = await get_lang(callback.from_user.id)
    if key not in CATEGORIES:
        await callback.answer()
        return
    back = [{"id": "nav_back", "cb": "nav:catalog"}]
    products = await fetchall(
        "SELECT id, name, price FROM products WHERE category = ? AND is_active = 1 ORDER BY id",
        (key,),
    )
    if not products:
        await show_key(callback, "category_empty", lang, [back], photo_key=f"category:{key}")
    else:
        rows = [
            [{"text": f"{p['name']} — {fmt_money(p['price'])} {CURRENCY}", "cb": f"prod:{p['id']}"}]
            for p in products
        ]
        rows.append(back)
        await show_key(callback, "category_title", lang, rows, photo_key=f"category:{key}",
                       category=CATEGORIES[key])
    await callback.answer()


@user_router.callback_query(F.data.startswith("prod:"))
async def cb_product(callback: CallbackQuery) -> None:
    product_id = int(callback.data.split(":")[1])
    lang = await get_lang(callback.from_user.id)
    product = await get_product(product_id)
    if not product or not product["is_active"]:
        await callback.answer(L[lang]["unavailable"], show_alert=True)
        return
    text, entities = await render_product_card(product, lang)
    rows: list[list[dict]] = []
    if product["in_stock"]:
        rows.append([{"id": "prod_buy", "cb": f"buy:{product_id}"}])
    rows.append([{"id": "nav_back", "cb": f"cat:{product['category']}"}])
    markup = await keyboard(lang, rows)
    await screen(callback, text, entities, markup, photo_key=f"product:{product_id}")
    await callback.answer()


@user_router.callback_query(F.data.startswith("buy:"))
async def cb_buy(callback: CallbackQuery, state: FSMContext) -> None:
    product_id = int(callback.data.split(":")[1])
    lang = await get_lang(callback.from_user.id)
    product = await get_product(product_id)
    if not product or not product["is_active"]:
        await callback.answer(L[lang]["unavailable"], show_alert=True)
        return
    if not product["in_stock"]:
        await callback.answer(L[lang]["out_of_stock"], show_alert=True)
        return
    order_id = await create_order(callback.from_user, product)
    await state.set_state(UserStates.awaiting_check)
    await state.update_data(order_id=order_id)
    order = await get_order(order_id)
    await payment_screen(callback, order, lang)
    await callback.answer()


@user_router.callback_query(F.data.startswith("pay:"))
async def cb_resume_payment(callback: CallbackQuery, state: FSMContext) -> None:
    order_id = int(callback.data.split(":")[1])
    lang = await get_lang(callback.from_user.id)
    order = await get_order(order_id)
    if not order or order["user_id"] != callback.from_user.id:
        await callback.answer(L[lang]["not_found"], show_alert=True)
        return
    if order["status"] != "pending_payment":
        await callback.answer(L[lang]["not_pending"], show_alert=True)
        return
    await state.set_state(UserStates.awaiting_check)
    await state.update_data(order_id=order_id)
    await payment_screen(callback, order, lang)
    await callback.answer()


@user_router.callback_query(F.data.startswith("cancel:"))
async def cb_cancel(callback: CallbackQuery, state: FSMContext) -> None:
    order_id = int(callback.data.split(":")[1])
    lang = await get_lang(callback.from_user.id)
    order = await get_order(order_id)
    if not order or order["user_id"] != callback.from_user.id:
        await callback.answer(L[lang]["not_found"], show_alert=True)
        return
    ok = await change_status(order_id, ("pending_payment",), "cancelled",
                             actor=f"user:{callback.from_user.id}")
    if not ok:
        await callback.answer(L[lang]["cancel_fail"], show_alert=True)
        return
    data = await state.get_data()
    if data.get("order_id") == order_id:
        await state.clear()
    await callback.answer(L[lang]["cancel_ok"])
    await orders_screen(callback, lang, callback.from_user.id)


@user_router.callback_query(F.data == "profile")
async def cb_profile(callback: CallbackQuery) -> None:
    lang = await get_lang(callback.from_user.id)
    user_id = callback.from_user.id
    user_row = await fetchone("SELECT created_at FROM users WHERE user_id = ?", (user_id,))
    count = await scalar("SELECT COUNT(*) FROM orders WHERE user_id = ? AND status = 'done'", (user_id,))
    total = await scalar(
        "SELECT COALESCE(SUM(price), 0) FROM orders WHERE user_id = ? AND status = 'done'", (user_id,)
    )
    rows = [
        [{"id": "profile_orders", "cb": "orders"}],
        [{"id": "nav_back", "cb": "nav:main"}],
    ]
    await show_key(
        callback,
        "profile",
        lang,
        rows,
        photo_key="profile",
        user_id=user_id,
        created=user_row["created_at"][:10] if user_row else "—",
        count=count,
        sum=fmt_money(total),
    )
    await callback.answer()


@user_router.callback_query(F.data == "orders")
async def cb_orders(callback: CallbackQuery) -> None:
    lang = await get_lang(callback.from_user.id)
    await orders_screen(callback, lang, callback.from_user.id)
    await callback.answer()


@user_router.callback_query(F.data.startswith("ord:"))
async def cb_order_detail(callback: CallbackQuery) -> None:
    order_id = int(callback.data.split(":")[1])
    lang = await get_lang(callback.from_user.id)
    order = await get_order(order_id)
    if not order or order["user_id"] != callback.from_user.id:
        await callback.answer(L[lang]["not_found"], show_alert=True)
        return
    rows: list[list[dict]] = []
    if order["status"] == "pending_payment":
        rows.append([{"text": L[lang]["pay_btn"], "cb": f"pay:{order_id}"}])
        rows.append([{"id": "pay_cancel", "cb": f"cancel:{order_id}"}])
    rows.append([{"id": "nav_back", "cb": "orders"}])
    await show_key(
        callback,
        "order_detail",
        lang,
        rows,
        photo_key="orders",
        order_id=order_number(order_id),
        product=order["product_name"],
        price=fmt_money(order["price"]),
        date=order["created_at"][:10],
        status=STATUS_NAMES[lang][order["status"]],
        reason=order["reject_reason"] or "—",
    )
    await callback.answer()


async def _attach_check_common(message: Message, state: FSMContext, order_id: int, lang: str,
                               file_id: str, unique_id: str, kind: str, size: int) -> None:
    if size > MAX_CHECK_SIZE_MB * 1024 * 1024:
        await message.answer(L[lang]["check_too_big"])
        return
    result = await attach_check(order_id, message.from_user.id, file_id, unique_id, kind)
    if result == "dup":
        await message.answer(L[lang]["check_dup"])
        return
    if result == "not_pending":
        await state.clear()
        await message.answer(L[lang]["not_pending"])
        return
    await state.clear()
    text, entities = await render_text("check_sent", lang)
    await message.answer(text, entities=to_entities(entities))
    await notify_admins_new_order(message.bot, order_id)


@user_router.message(UserStates.awaiting_check, F.photo)
async def on_check_photo(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    lang = await get_lang(message.from_user.id)
    photo = message.photo[-1]
    await _attach_check_common(message, state, data["order_id"], lang,
                               photo.file_id, photo.file_unique_id, "photo", photo.file_size or 0)


@user_router.message(UserStates.awaiting_check, F.document)
async def on_check_document(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    lang = await get_lang(message.from_user.id)
    document = message.document
    mime = document.mime_type or ""
    if not (mime.startswith("image/") or mime == "application/pdf"):
        await message.answer(L[lang]["check_wrong_type"])
        return
    await _attach_check_common(message, state, data["order_id"], lang,
                               document.file_id, document.file_unique_id, "document",
                               document.file_size or 0)


@user_router.message(StateFilter(None), F.photo | F.document)
async def on_check_without_state(message: Message) -> None:
    lang = await get_lang(message.from_user.id)
    await message.answer(L[lang]["no_pending_hint"])


@user_router.callback_query(F.data == "support")
async def cb_support(callback: CallbackQuery) -> None:
    lang = await get_lang(callback.from_user.id)
    username = (await get_setting("support_username")).lstrip("@")
    url = await get_setting("support_url")
    if not url and username:
        url = f"https://t.me/{username}"
    rows: list[list[dict]] = []
    if url:
        rows.append([{"id": "support_contact", "url": url}])
    rows.append([{"id": "nav_back", "cb": "nav:main"}])
    await show_key(callback, "support", lang, rows, photo_key="support")
    await callback.answer()


@user_router.callback_query(F.data == "info")
async def cb_info(callback: CallbackQuery) -> None:
    lang = await get_lang(callback.from_user.id)
    await show_key(callback, "info", lang, [[{"id": "nav_back", "cb": "nav:main"}]], photo_key="info")
    await callback.answer()


@user_router.callback_query(F.data == "lang")
async def cb_language(callback: CallbackQuery) -> None:
    lang = await get_lang(callback.from_user.id)
    rows = [
        [{"id": "lang_ru", "cb": "lang:ru"}],
        [{"id": "lang_en", "cb": "lang:en"}],
        [{"id": "nav_back", "cb": "nav:main"}],
    ]
    await show_key(callback, "language", lang, rows, photo_key="language")
    await callback.answer()


@user_router.callback_query(F.data.startswith("lang:"))
async def cb_set_language(callback: CallbackQuery, state: FSMContext) -> None:
    code = callback.data.split(":", 1)[1]
    if code not in LANGUAGES:
        await callback.answer()
        return
    await set_lang(callback.from_user.id, code)
    await state.clear()
    text, entities, markup = await main_view(code, callback.from_user.first_name or "")
    await screen(callback, text, entities, markup, photo_key="main")
    await callback.answer(L[code]["lang_changed"])


# ============================================================================
# АДМИН-ПАНЕЛЬ
# ============================================================================

admin_router = Router(name="admin")
admin_router.message.filter(IsAdmin())
admin_router.callback_query.filter(IsAdmin())

ADMIN_LANG = "ru"


def admin_menu_rows() -> list[list[dict]]:
    return [
        [{"id": "adm_products", "cb": "adm:products"}],
        [{"id": "adm_orders", "cb": "adm:orders"}],
        [{"id": "adm_requisites", "cb": "adm:req"}],
        [{"id": "adm_texts", "cb": "adm:texts"}],
        [{"id": "adm_buttons", "cb": "adm:buttons"}],
        [{"id": "adm_stats", "cb": "adm:stats"}],
        [{"id": "adm_broadcast", "cb": "adm:bc"}],
        [{"id": "adm_photos", "cb": "adm:photos"}],
        [{"id": "adm_settings", "cb": "adm:settings"}],
        [{"id": "nav_main", "cb": "nav:main"}],
    ]


async def admin_menu_screen(target: CallbackQuery | Message) -> None:
    markup = await keyboard(ADMIN_LANG, admin_menu_rows())
    await screen(target, "🛠 Админ-панель\n\nВыберите раздел:", None, markup)


@admin_router.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext) -> None:
    await state.clear()
    await admin_menu_screen(message)


@admin_router.callback_query(F.data == "adm:menu")
async def adm_menu(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await admin_menu_screen(callback)
    await callback.answer()


# --- товары ----------------------------------------------------------------

async def admin_products_screen(target: CallbackQuery | Message) -> None:
    products = await fetchall("SELECT id, name, price, is_active FROM products ORDER BY id DESC LIMIT 60")
    rows: list[list[dict]] = [
        [{
            "text": f"{'⛔ ' if not p['is_active'] else ''}#{p['id']} {p['name']} — {fmt_money(p['price'])} ₽",
            "cb": f"adm:prod:{p['id']}",
        }]
        for p in products
    ]
    rows.append([{"id": "adm_add_product", "cb": "adm:padd"}])
    rows.append([{"id": "nav_back", "cb": "adm:menu"}])
    text = "📦 Товары\n\nВыберите товар или добавьте новый:" if products else "📦 Товаров пока нет. Добавьте первый:"
    await screen(target, text, None, await keyboard(ADMIN_LANG, rows))


async def admin_product_view(target: CallbackQuery | Message, product_id: int) -> None:
    product = await get_product(product_id)
    if not product:
        await screen(target, "Товар не найден.", None, await keyboard(ADMIN_LANG, [[{"id": "nav_back", "cb": "adm:products"}]]))
        return
    text, entities = await render_product_card(product, ADMIN_LANG)
    toggle_label = "👁 Скрыть" if product["is_active"] else "👁 Включить"
    stock_label = "📦 Сделать: нет в наличии" if product["in_stock"] else "📦 Сделать: в наличии"
    rows = [
        [{"text": "✏️ Название", "cb": f"adm:pedit:{product_id}:name"},
         {"text": "💰 Цена", "cb": f"adm:pedit:{product_id}:price"}],
        [{"text": "📝 Описание", "cb": f"adm:pedit:{product_id}:desc"},
         {"text": "📂 Категория", "cb": f"adm:pcat:{product_id}"}],
        [{"text": stock_label, "cb": f"adm:pstock:{product_id}"}],
        [{"text": toggle_label, "cb": f"adm:pact:{product_id}"}],
        [{"text": "🖼 Фото", "cb": f"adm:photo:product:{product_id}"}],
        [{"text": "🗑 Удалить", "cb": f"adm:pdel:{product_id}"}],
        [{"id": "nav_back", "cb": "adm:products"}],
    ]
    await screen(target, text, entities, await keyboard(ADMIN_LANG, rows))


@admin_router.callback_query(F.data == "adm:products")
async def adm_products(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await admin_products_screen(callback)
    await callback.answer()


@admin_router.callback_query(F.data.startswith("adm:prod:"))
async def adm_product(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await admin_product_view(callback, int(callback.data.split(":")[2]))
    await callback.answer()


@admin_router.callback_query(F.data.startswith("adm:pedit:"))
async def adm_product_edit_prompt(callback: CallbackQuery, state: FSMContext) -> None:
    _, _, pid, field = callback.data.split(":")
    prompts = {
        "name": "Введите новое название товара (до 120 символов):",
        "price": "Введите новую цену в рублях (целое число больше 0):",
        "desc": "Отправьте новое описание. Форматирование и Premium Emoji сохранятся.",
    }
    if field not in prompts:
        await callback.answer()
        return
    await state.set_state(AdminStates.product_edit)
    await state.update_data(product_id=int(pid), field=field)
    rows = [[{"id": "nav_back", "cb": f"adm:prod:{pid}"}]]
    await screen(callback, prompts[field], None, await keyboard(ADMIN_LANG, rows))
    await callback.answer()


@admin_router.message(AdminStates.product_edit, F.text)
async def adm_product_edit_value(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    product_id = data["product_id"]
    field = data["field"]
    if field == "name":
        value = message.text.strip()
        if not 1 <= len(value) <= 120:
            await message.answer("❗ Название должно быть от 1 до 120 символов.")
            return
        await run("UPDATE products SET name = ? WHERE id = ?", (value, product_id))
    elif field == "price":
        raw = message.text.replace(" ", "")
        if not raw.isdigit() or int(raw) <= 0:
            await message.answer("❗ Введите целую цену в рублях больше 0.")
            return
        await run("UPDATE products SET price = ? WHERE id = ?", (int(raw), product_id))
    else:
        await run(
            "UPDATE products SET description = ?, description_entities = ? WHERE id = ?",
            (message.text, json.dumps(entities_to_dicts(message.entities), ensure_ascii=False), product_id),
        )
    await state.clear()
    await message.answer("✅ Сохранено.")
    await admin_product_view(message, product_id)


@admin_router.callback_query(F.data.startswith("adm:pcat:"))
async def adm_product_category(callback: CallbackQuery) -> None:
    pid = callback.data.split(":")[2]
    items = list(CATEGORIES.items())
    rows = [[{"text": name, "cb": f"adm:pcatset:{pid}:{key}"} for key, name in items[i:i + 2]]
            for i in range(0, len(items), 2)]
    rows.append([{"id": "nav_back", "cb": f"adm:prod:{pid}"}])
    await screen(callback, "📂 Выберите категорию:", None, await keyboard(ADMIN_LANG, rows))
    await callback.answer()


@admin_router.callback_query(F.data.startswith("adm:pcatset:"))
async def adm_product_category_set(callback: CallbackQuery) -> None:
    _, _, pid, key = callback.data.split(":")
    if key in CATEGORIES:
        await run("UPDATE products SET category = ? WHERE id = ?", (key, int(pid)))
    await admin_product_view(callback, int(pid))
    await callback.answer("Категория изменена")


@admin_router.callback_query(F.data.startswith("adm:pstock:"))
async def adm_product_stock(callback: CallbackQuery) -> None:
    pid = int(callback.data.split(":")[2])
    await run("UPDATE products SET in_stock = 1 - in_stock WHERE id = ?", (pid,))
    await admin_product_view(callback, pid)
    await callback.answer()


@admin_router.callback_query(F.data.startswith("adm:pact:"))
async def adm_product_active(callback: CallbackQuery) -> None:
    pid = int(callback.data.split(":")[2])
    await run("UPDATE products SET is_active = 1 - is_active WHERE id = ?", (pid,))
    await admin_product_view(callback, pid)
    await callback.answer()


@admin_router.callback_query(F.data.startswith("adm:pdel:"))
async def adm_product_delete_confirm(callback: CallbackQuery) -> None:
    pid = callback.data.split(":")[2]
    rows = [
        [{"text": "✅ Да, удалить", "cb": f"adm:pdelok:{pid}"}],
        [{"text": "✖️ Отмена", "cb": f"adm:prod:{pid}"}],
    ]
    await screen(callback, "🗑 Удалить товар?\n\nИстория старых заказов сохранится.", None,
                 await keyboard(ADMIN_LANG, rows))
    await callback.answer()


@admin_router.callback_query(F.data.startswith("adm:pdelok:"))
async def adm_product_delete(callback: CallbackQuery) -> None:
    pid = int(callback.data.split(":")[2])
    await run("DELETE FROM products WHERE id = ?", (pid,))
    await admin_products_screen(callback)
    await callback.answer("Товар удалён")


# --- добавление товара (FSM) -----------------------------------------------

@admin_router.callback_query(F.data == "adm:padd")
async def adm_add_start(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(AdminStates.add_category)
    items = list(CATEGORIES.items())
    rows = [[{"text": name, "cb": f"adm:paddcat:{key}"} for key, name in items[i:i + 2]]
            for i in range(0, len(items), 2)]
    rows.append([{"id": "nav_back", "cb": "adm:products"}])
    await screen(callback, "➕ Новый товар\n\nШаг 1. Выберите категорию:", None,
                 await keyboard(ADMIN_LANG, rows))
    await callback.answer()


@admin_router.callback_query(F.data.startswith("adm:paddcat:"))
async def adm_add_category(callback: CallbackQuery, state: FSMContext) -> None:
    key = callback.data.split(":")[2]
    if key not in CATEGORIES:
        await callback.answer()
        return
    await state.update_data(category=key)
    await state.set_state(AdminStates.add_name)
    rows = [[{"id": "nav_back", "cb": "adm:products"}]]
    await screen(callback, "Шаг 2. Введите название товара:", None, await keyboard(ADMIN_LANG, rows))
    await callback.answer()


@admin_router.message(AdminStates.add_name, F.text)
async def adm_add_name(message: Message, state: FSMContext) -> None:
    name = message.text.strip()
    if not 1 <= len(name) <= 120:
        await message.answer("❗ Название должно быть от 1 до 120 символов.")
        return
    await state.update_data(name=name)
    await state.set_state(AdminStates.add_price)
    await message.answer("Шаг 3. Введите цену в рублях (целое число):")


@admin_router.message(AdminStates.add_price, F.text)
async def adm_add_price(message: Message, state: FSMContext) -> None:
    raw = message.text.replace(" ", "")
    if not raw.isdigit() or int(raw) <= 0:
        await message.answer("❗ Введите целую цену в рублях больше 0.")
        return
    await state.update_data(price=int(raw))
    await state.set_state(AdminStates.add_desc)
    await message.answer("Шаг 4. Отправьте описание с форматированием. Premium Emoji сохранятся.")


@admin_router.message(AdminStates.add_desc, F.text)
async def adm_add_description(message: Message, state: FSMContext) -> None:
    await state.update_data(
        description=message.text,
        description_entities=entities_to_dicts(message.entities),
    )
    rows = [
        [{"text": "В наличии", "cb": "adm:paddstock:in"}],
        [{"text": "Нет в наличии", "cb": "adm:paddstock:out"}],
    ]
    await message.answer("Шаг 5. Выберите наличие:", reply_markup=await keyboard(ADMIN_LANG, rows))


@admin_router.callback_query(F.data.startswith("adm:paddstock:"))
async def adm_add_stock(callback: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    if "name" not in data:
        await callback.answer("Начните добавление заново", show_alert=True)
        return
    in_stock = callback.data.split(":")[2] == "in"
    await state.update_data(in_stock=in_stock)
    preview_data = {**data, "in_stock": in_stock}
    stock_key = "stock_in" if in_stock else "stock_out"
    text, entities = await render_text(
        "product_card",
        ADMIN_LANG,
        product_name=preview_data["name"],
        price=fmt_money(preview_data["price"]),
        stock=await plain_text(stock_key, ADMIN_LANG),
        description=(preview_data["description"], preview_data["description_entities"]),
    )
    rows = [
        [{"text": "✅ Подтвердить создание", "cb": "adm:paddok"}],
        [{"text": "✖️ Отмена", "cb": "adm:products"}],
    ]
    await screen(callback, "Шаг 6. Предпросмотр карточки:\n\n" + text, None, None)
    await callback.message.answer(text, entities=to_entities(entities),
                                  reply_markup=await keyboard(ADMIN_LANG, rows))
    await callback.answer()


@admin_router.callback_query(F.data == "adm:paddok")
async def adm_add_confirm(callback: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    if "name" not in data or "in_stock" not in data:
        await callback.answer("Данные товара утеряны, начните заново", show_alert=True)
        return
    product_id = await insert_product(data)
    await state.clear()
    await callback.answer("✅ Товар добавлен")
    await admin_product_view(callback.message, product_id)


# --- заявки -----------------------------------------------------------------

ORDER_FILTERS: dict[str, tuple[str, ...] | None] = {
    "new": ("checking",),
    "paid": ("paid", "delivering"),
    "done": ("done",),
    "rejected": ("rejected",),
    "all": None,
}


def admin_order_text(order: aiosqlite.Row, header: str | None = None) -> str:
    number = order_number(order["id"])
    username = f"@{order['username']}" if order["username"] else "—"
    title = f"🔔 {header} №{number}" if header else f"📋 Заказ №{number}"
    lines = [
        title,
        "",
        f"👤 Покупатель: {username}",
        f"🆔 Telegram ID: {order['user_id']}",
        f"📦 Товар: {order['product_name']}",
        f"💰 Сумма: {fmt_money(order['price'])} {CURRENCY}",
        f"📅 Дата оформления: {order['created_at'][:16].replace('T', ' ')}",
        f"📌 Статус: {STATUS_NAMES['ru'][order['status']]}",
    ]
    if order["reject_reason"]:
        lines.append(f"📝 Причина отклонения: {order['reject_reason']}")
    return "\n".join(lines)


async def notify_admins_new_order(bot: Bot, order_id: int) -> None:
    order = await get_order(order_id)
    if not order:
        return
    text = admin_order_text(order, header="НОВЫЙ ЗАКАЗ")
    rows = [
        [{"id": "order_ok", "cb": f"adm:ok:{order_id}"}, {"id": "order_reject", "cb": f"adm:no:{order_id}"}],
        [{"id": "order_write", "url": buyer_link(order["user_id"])}],
    ]
    markup = await keyboard(ADMIN_LANG, rows)
    for admin_id in ADMIN_IDS:
        try:
            if order["check_kind"] == "photo":
                await bot.send_photo(admin_id, photo=order["check_file_id"], caption=text, reply_markup=markup)
            else:
                await bot.send_document(admin_id, document=order["check_file_id"], caption=text,
                                        reply_markup=markup)
        except TelegramAPIError as exc:
            log.error("Не удалось отправить заявку администратору %s: %s", admin_id, exc)


async def admin_order_screen(target: CallbackQuery | Message, order_id: int) -> None:
    order = await get_order(order_id)
    if not order:
        await screen(target, "Заказ не найден.", None, await keyboard(ADMIN_LANG, [[{"id": "nav_back", "cb": "adm:orders"}]]))
        return
    rows: list[list[dict]] = []
    if order["status"] == "checking":
        rows.append([{"id": "order_ok", "cb": f"adm:ok:{order_id}"},
                     {"id": "order_reject", "cb": f"adm:no:{order_id}"}])
    elif order["status"] == "paid":
        rows.append([{"id": "order_deliver", "cb": f"adm:deliver:{order_id}"}])
    rows.append([{"id": "order_write", "url": buyer_link(order["user_id"])}])
    rows.append([{"id": "nav_back", "cb": "adm:orders"}])
    await screen(target, admin_order_text(order), None, await keyboard(ADMIN_LANG, rows))


@admin_router.callback_query(F.data == "adm:orders")
async def adm_orders(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    rows = [
        [{"id": "adm_f_new", "cb": "adm:olist:new"}],
        [{"id": "adm_f_paid", "cb": "adm:olist:paid"}],
        [{"id": "adm_f_done", "cb": "adm:olist:done"}],
        [{"id": "adm_f_rejected", "cb": "adm:olist:rejected"}],
        [{"id": "adm_f_all", "cb": "adm:olist:all"}],
        [{"id": "nav_back", "cb": "adm:menu"}],
    ]
    await screen(callback, "🧾 Заявки\n\nВыберите раздел:", None, await keyboard(ADMIN_LANG, rows))
    await callback.answer()


@admin_router.callback_query(F.data.startswith("adm:olist:"))
async def adm_order_list(callback: CallbackQuery) -> None:
    flt = callback.data.split(":")[2]
    statuses = ORDER_FILTERS.get(flt)
    if statuses is None:
        orders = await fetchall(
            "SELECT id, product_name, price, status FROM orders ORDER BY id DESC LIMIT ?",
            (ORDERS_LIST_LIMIT,),
        )
    else:
        placeholders = ",".join("?" * len(statuses))
        orders = await fetchall(
            f"SELECT id, product_name, price, status FROM orders WHERE status IN ({placeholders}) "
            f"ORDER BY id DESC LIMIT ?",
            (*statuses, ORDERS_LIST_LIMIT),
        )
    rows: list[list[dict]] = [
        [{"text": f"№{order_number(o['id'])} · {o['product_name']} · {STATUS_NAMES['ru'][o['status']]}",
          "cb": f"adm:order:{o['id']}"}]
        for o in orders
    ]
    rows.append([{"id": "nav_back", "cb": "adm:orders"}])
    text = "🧾 Заявок в этом разделе нет." if not orders else "🧾 Выберите заявку:"
    await screen(callback, text, None, await keyboard(ADMIN_LANG, rows))
    await callback.answer()


@admin_router.callback_query(F.data.startswith("adm:order:"))
async def adm_order_view(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await admin_order_screen(callback, int(callback.data.split(":")[2]))
    await callback.answer()


@admin_router.callback_query(F.data.startswith("adm:ok:"))
async def adm_order_confirm(callback: CallbackQuery) -> None:
    order_id = int(callback.data.split(":")[2])
    ok = await change_status(order_id, ("checking",), "paid", actor=f"admin:{callback.from_user.id}")
    if not ok:
        await callback.answer("⚠️ Заявка уже обработана", show_alert=True)
        return
    order = await get_order(order_id)
    await notify_user(callback.bot, order["user_id"], "paid_notice", order_id=order_number(order_id))
    await callback.answer("✅ Оплата подтверждена")
    await admin_order_screen(callback, order_id)


@admin_router.callback_query(F.data.startswith("adm:no:"))
async def adm_order_reject_start(callback: CallbackQuery, state: FSMContext) -> None:
    order_id = int(callback.data.split(":")[2])
    order = await get_order(order_id)
    if not order or order["status"] != "checking":
        await callback.answer("⚠️ Заявка уже обработана", show_alert=True)
        return
    await state.set_state(AdminStates.reject_reason)
    await state.update_data(order_id=order_id)
    rows = [[{"id": "order_skip", "cb": "adm:skipreason"}]]
    await screen(callback, "Укажите причину отклонения или нажмите «Пропустить».", None,
                 await keyboard(ADMIN_LANG, rows))
    await callback.answer()


async def finish_reject(bot: Bot, state: FSMContext, reason: str, reply) -> None:
    data = await state.get_data()
    order_id = data.get("order_id")
    await state.clear()
    if not order_id:
        await reply("Нет активной заявки.")
        return
    ok = await change_status(order_id, ("checking",), "rejected", actor="admin", reason=reason)
    if not ok:
        await reply("⚠️ Заявка уже обработана.")
        return
    order = await get_order(order_id)
    await notify_user(bot, order["user_id"], "rejected_notice",
                      order_id=order_number(order_id), reason=reason or "—")
    await reply("🚫 Заказ отклонён, покупатель уведомлён.")


@admin_router.message(AdminStates.reject_reason, F.text)
async def adm_order_reject_reason(message: Message, state: FSMContext) -> None:
    await finish_reject(message.bot, state, message.text.strip()[:500], message.answer)


@admin_router.callback_query(F.data == "adm:skipreason", StateFilter(AdminStates.reject_reason))
async def adm_order_reject_skip(callback: CallbackQuery, state: FSMContext) -> None:
    await finish_reject(callback.bot, state, "", callback.message.answer)
    await callback.answer()


@admin_router.callback_query(F.data.startswith("adm:deliver:"))
async def adm_order_deliver_start(callback: CallbackQuery, state: FSMContext) -> None:
    order_id = int(callback.data.split(":")[2])
    order = await get_order(order_id)
    if not order or order["status"] != "paid":
        await callback.answer("⚠️ Выдача недоступна для этого статуса", show_alert=True)
        return
    await state.set_state(AdminStates.deliver)
    await state.update_data(order_id=order_id)
    rows = [[{"id": "nav_back", "cb": f"adm:order:{order_id}"}]]
    await screen(callback, "📤 Отправьте данные для выдачи. Поддерживаются форматирование и Premium Emoji.",
                 None, await keyboard(ADMIN_LANG, rows))
    await callback.answer()


@admin_router.message(AdminStates.deliver, F.text)
async def adm_order_deliver(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    order_id = data.get("order_id")
    await state.clear()
    if not order_id:
        await message.answer("Нет активной заявки.")
        return
    claimed = await change_status(order_id, ("paid",), "delivering", actor=f"admin:{message.from_user.id}")
    if not claimed:
        await message.answer("⚠️ Заказ уже выдаётся или выдан.")
        return
    order = await get_order(order_id)
    try:
        await message.bot.send_message(
            order["user_id"],
            message.text,
            entities=to_entities(entities_to_dicts(message.entities)),
        )
    except TelegramAPIError as exc:
        await change_status(order_id, ("delivering",), "paid", actor="system",
                            note=f"Ошибка отправки: {exc}")
        await message.answer(
            f"❌ Не удалось отправить покупателю: {exc}\nЗаказ остался в статусе «Оплачен»."
        )
        return
    await change_status(order_id, ("delivering",), "done", actor=f"admin:{message.from_user.id}")
    await notify_user(message.bot, order["user_id"], "delivered_notice", order_id=order_number(order_id))
    await message.answer(f"✅ Товар выдан, заказ №{order_number(order_id)} выполнен.")


# --- реквизиты --------------------------------------------------------------

@admin_router.callback_query(F.data == "adm:req")
async def adm_requisites(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    text, entities = await get_text("requisites", ADMIN_LANG)
    rows = [
        [{"text": "✏️ Изменить реквизиты", "cb": "adm:reqedit"}],
        [{"id": "nav_back", "cb": "adm:menu"}],
    ]
    await screen(callback, "🏦 Текущие реквизиты:\n\n" + text if not entities else text,
                 entities if not entities else entities, await keyboard(ADMIN_LANG, rows))
    await callback.answer()


@admin_router.callback_query(F.data == "adm:reqedit")
async def adm_requisites_edit(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(AdminStates.requisites_edit)
    rows = [[{"id": "nav_back", "cb": "adm:req"}]]
    await screen(callback, "Отправьте новые реквизиты с форматированием и Premium Emoji.", None,
                 await keyboard(ADMIN_LANG, rows))
    await callback.answer()


@admin_router.message(AdminStates.requisites_edit, F.text)
async def adm_requisites_preview(message: Message, state: FSMContext) -> None:
    entities = entities_to_dicts(message.entities)
    await state.set_state(AdminStates.requisites_preview)
    await state.update_data(text=message.text, entities=entities)
    rows = [
        [{"text": "💾 Сохранить", "cb": "adm:reqsave"}],
        [{"text": "✖️ Отмена", "cb": "adm:menu"}],
    ]
    await message.answer("👁 Предпросмотр реквизитов ↓")
    await message.answer(message.text, entities=to_entities(entities),
                         reply_markup=await keyboard(ADMIN_LANG, rows))


@admin_router.callback_query(F.data == "adm:reqsave", StateFilter(AdminStates.requisites_preview))
async def adm_requisites_save(callback: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    await save_text("requisites", "ru", data["text"], data["entities"])
    await save_text("requisites", "en", data["text"], data["entities"])
    await state.clear()
    await callback.answer("✅ Сохранено")
    await admin_menu_screen(callback.message)


# --- редактор текстов -------------------------------------------------------

def _variables_hint() -> str:
    return ", ".join("{" + key + "}" for key in SAMPLE_VARS)


@admin_router.callback_query(F.data == "adm:texts")
async def adm_texts(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    rows = [[{"text": label, "cb": f"adm:txt:{key}"}] for key, label in TEXT_EDITOR_KEYS]
    rows.append([{"id": "nav_back", "cb": "adm:menu"}])
    await screen(callback, "📝 Редактор текстов\n\nВыберите текст:", None, await keyboard(ADMIN_LANG, rows))
    await callback.answer()


@admin_router.callback_query(F.data.startswith("adm:txt:") & ~F.data.startswith("adm:txtl:")
                             & ~F.data.startswith("adm:txtedit:") & ~F.data.startswith("adm:txtreset:"))
async def adm_text_languages(callback: CallbackQuery) -> None:
    key = callback.data.split(":", 2)[2]
    rows = [
        [{"text": "🇷🇺 Русский", "cb": f"adm:txtl:{key}:ru"}, {"text": "🇬🇧 English", "cb": f"adm:txtl:{key}:en"}],
        [{"id": "nav_back", "cb": "adm:texts"}],
    ]
    await screen(callback, "Выберите язык текста:", None, await keyboard(ADMIN_LANG, rows))
    await callback.answer()


async def admin_text_view(target: CallbackQuery | Message, key: str, lang: str) -> None:
    text, entities = await get_text(key, lang)
    rendered, rendered_entities = render(text, entities, SAMPLE_VARS)
    rows = [
        [{"text": "✏️ Изменить", "cb": f"adm:txtedit:{key}:{lang}"},
         {"text": "♻️ Сбросить", "cb": f"adm:txtreset:{key}:{lang}"}],
        [{"id": "nav_back", "cb": f"adm:txt:{key}"}],
    ]
    await screen(target, rendered, rendered_entities, await keyboard(ADMIN_LANG, rows))


@admin_router.callback_query(F.data.startswith("adm:txtl:"))
async def adm_text_view(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    _, _, key, lang = callback.data.split(":")
    await admin_text_view(callback, key, lang)
    await callback.answer()


@admin_router.callback_query(F.data.startswith("adm:txtedit:"))
async def adm_text_edit_start(callback: CallbackQuery, state: FSMContext) -> None:
    _, _, key, lang = callback.data.split(":")
    await state.set_state(AdminStates.text_edit)
    await state.update_data(key=key, lang=lang)
    rows = [[{"id": "nav_back", "cb": f"adm:txtl:{key}:{lang}"}]]
    await screen(
        callback,
        "Отправьте новый текст. Форматирование и Premium Emoji сохранятся.\n\n"
        f"Доступные переменные: {_variables_hint()}",
        None,
        await keyboard(ADMIN_LANG, rows),
    )
    await callback.answer()


@admin_router.message(AdminStates.text_edit, F.text)
async def adm_text_preview(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    entities = entities_to_dicts(message.entities)
    await state.set_state(AdminStates.text_preview)
    await state.update_data(text=message.text, entities=entities)
    rendered, rendered_entities = render(message.text, entities, SAMPLE_VARS)
    rows = [
        [{"text": "💾 Сохранить", "cb": "adm:txtsave"}],
        [{"text": "✖️ Отмена", "cb": f"adm:txtl:{data['key']}:{data['lang']}"}],
    ]
    await message.answer("👁 Предпросмотр ↓")
    await message.answer(rendered, entities=to_entities(rendered_entities),
                         reply_markup=await keyboard(ADMIN_LANG, rows))


@admin_router.callback_query(F.data == "adm:txtsave", StateFilter(AdminStates.text_preview))
async def adm_text_save(callback: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    await save_text(data["key"], data["lang"], data["text"], data["entities"])
    await state.clear()
    await callback.answer("✅ Сохранено")
    await admin_text_view(callback.message, data["key"], data["lang"])


@admin_router.callback_query(F.data.startswith("adm:txtreset:"))
async def adm_text_reset(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    _, _, key, lang = callback.data.split(":")
    default = DEFAULT_TEXTS.get(key, {}).get(lang, "")
    await save_text(key, lang, default, [])
    await callback.answer("♻️ Текст сброшен")
    await admin_text_view(callback, key, lang)


# --- редактор кнопок --------------------------------------------------------

async def admin_button_view(target: CallbackQuery | Message, btn_id: str) -> None:
    ru = await get_button(btn_id, "ru")
    en = await get_button(btn_id, "en")
    icon_note = " (Premium Emoji)" if ru["icon"] else ""
    text = (
        f"🔘 Кнопка: {btn_id}\n\n"
        f"🇷🇺 Текст: {ru['text']}\n"
        f"🇬🇧 Текст: {en['text']}\n"
        f"😀 Эмодзи: {ru['emoji'] or '—'}{icon_note}\n\n"
        f"Пример: {ru['emoji'] + ' ' if ru['emoji'] and not ru['icon'] else ''}{ru['text']}"
    )
    rows = [
        [{"text": "✏️ Текст RU", "cb": f"adm:btntxt:{btn_id}:ru"},
         {"text": "✏️ Текст EN", "cb": f"adm:btntxt:{btn_id}:en"}],
        [{"text": "😀 Эмодзи", "cb": f"adm:btnemoji:{btn_id}"},
         {"text": "♻️ Сбросить", "cb": f"adm:btnreset:{btn_id}"}],
        [{"id": "nav_back", "cb": "adm:buttons"}],
    ]
    await screen(target, text, None, await keyboard(ADMIN_LANG, rows))


@admin_router.callback_query(F.data == "adm:buttons")
async def adm_buttons(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    rows: list[list[dict]] = []
    for btn_id in DEFAULT_BUTTONS:
        current = await get_button(btn_id, "ru")
        rows.append([{"text": f"{btn_id}: {current['text']}", "cb": f"adm:btn:{btn_id}"}])
    rows.append([{"id": "nav_back", "cb": "adm:menu"}])
    await screen(callback, "🔘 Редактор кнопок\n\nВыберите кнопку:", None, await keyboard(ADMIN_LANG, rows))
    await callback.answer()


@admin_router.callback_query(F.data.startswith("adm:btn:"))
async def adm_button_view(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await admin_button_view(callback, callback.data.split(":", 2)[2])
    await callback.answer()


@admin_router.callback_query(F.data.startswith("adm:btntxt:"))
async def adm_button_text_start(callback: CallbackQuery, state: FSMContext) -> None:
    _, _, btn_id, lang = callback.data.split(":")
    await state.set_state(AdminStates.button_text)
    await state.update_data(btn_id=btn_id, lang=lang)
    rows = [[{"id": "nav_back", "cb": f"adm:btn:{btn_id}"}]]
    await screen(callback, "Отправьте новый текст кнопки (до 64 символов).", None,
                 await keyboard(ADMIN_LANG, rows))
    await callback.answer()


@admin_router.message(AdminStates.button_text, F.text)
async def adm_button_text_save(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    value = message.text.strip()
    if not 1 <= len(value) <= 64:
        await message.answer("❗ Текст кнопки должен быть от 1 до 64 символов.")
        return
    await run("UPDATE buttons SET text = ? WHERE btn_id = ? AND lang = ?", (value, data["btn_id"], data["lang"]))
    await state.clear()
    await message.answer("✅ Текст кнопки сохранён.")
    await admin_button_view(message, data["btn_id"])


@admin_router.callback_query(F.data.startswith("adm:btnemoji:"))
async def adm_button_emoji_start(callback: CallbackQuery, state: FSMContext) -> None:
    btn_id = callback.data.split(":", 2)[2]
    await state.set_state(AdminStates.button_emoji)
    await state.update_data(btn_id=btn_id)
    rows = [[{"id": "nav_back", "cb": f"adm:btn:{btn_id}"}]]
    await screen(
        callback,
        "Отправьте эмодзи: обычный или Premium Emoji (отправьте сообщение с ним).\n"
        "Отправьте «-», чтобы убрать эмодзи.",
        None,
        await keyboard(ADMIN_LANG, rows),
    )
    await callback.answer()


@admin_router.message(AdminStates.button_emoji, F.text)
async def adm_button_emoji_save(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    btn_id = data["btn_id"]
    raw = message.text
    if raw.strip() == "-":
        emoji, icon = "", None
    else:
        emoji, icon = extract_custom_emoji(raw, entities_to_dicts(message.entities))
    await run("UPDATE buttons SET emoji = ?, icon_custom_emoji_id = ? WHERE btn_id = ?", (emoji, icon, btn_id))
    await state.clear()
    if icon:
        await message.answer(
            "✅ Premium Emoji сохранён. Если Telegram не принимает его на кнопке, "
            "проверьте версию Bot API и aiogram в логах."
        )
    else:
        await message.answer("✅ Эмодзи сохранено.")
    await admin_button_view(message, btn_id)


@admin_router.callback_query(F.data.startswith("adm:btnreset:"))
async def adm_button_reset(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    btn_id = callback.data.split(":", 2)[2]
    ru, en, emoji = DEFAULT_BUTTONS.get(btn_id, (btn_id, btn_id, ""))
    await run("UPDATE buttons SET text = ?, emoji = ?, icon_custom_emoji_id = NULL WHERE btn_id = ? AND lang = 'ru'",
              (ru, emoji, btn_id))
    await run("UPDATE buttons SET text = ?, emoji = ?, icon_custom_emoji_id = NULL WHERE btn_id = ? AND lang = 'en'",
              (en, emoji, btn_id))
    await callback.answer("♻️ Кнопка сброшена")
    await admin_button_view(callback, btn_id)


# --- статистика -------------------------------------------------------------

async def stats_text() -> str:
    today = today_prefix()
    month = today[:7]
    total_users = await scalar("SELECT COUNT(*) FROM users")
    new_today = await scalar("SELECT COUNT(*) FROM users WHERE created_at LIKE ?", (f"{today}%",))
    total_orders = await scalar("SELECT COUNT(*) FROM orders")
    done = await scalar("SELECT COUNT(*) FROM orders WHERE status = 'done'")
    pending = await scalar(
        "SELECT COUNT(*) FROM orders WHERE status IN ('pending_payment', 'checking', 'paid', 'delivering')"
    )
    revenue = await scalar("SELECT COALESCE(SUM(price), 0) FROM orders WHERE status = 'done'")
    revenue_today = await scalar(
        "SELECT COALESCE(SUM(price), 0) FROM orders WHERE status = 'done' AND completed_at LIKE ?",
        (f"{today}%",),
    )
    revenue_month = await scalar(
        "SELECT COALESCE(SUM(price), 0) FROM orders WHERE status = 'done' AND completed_at LIKE ?",
        (f"{month}%",),
    )
    top = await fetchall(
        "SELECT product_name, COUNT(*) AS cnt FROM orders WHERE status = 'done' "
        "GROUP BY product_name ORDER BY cnt DESC LIMIT 5"
    )
    top_lines = [f"  {i}. {row['product_name']} — {row['cnt']} шт." for i, row in enumerate(top, 1)]
    return "\n".join([
        "📊 Статистика",
        "",
        f"👥 Всего пользователей: {total_users}",
        f"🆕 Новых за сегодня: {new_today}",
        "",
        f"🧾 Всего заказов: {total_orders}",
        f"✅ Выполнено: {done}",
        f"⏳ Ожидают обработки: {pending}",
        "",
        f"💰 Выручка: {fmt_money(revenue)} {CURRENCY}",
        f"📅 Выручка за сегодня: {fmt_money(revenue_today)} {CURRENCY}",
        f"🗓 Выручка за месяц: {fmt_money(revenue_month)} {CURRENCY}",
        "",
        "🏆 Популярные товары:",
        *(top_lines or ["  пока нет выполненных заказов"]),
    ])


@admin_router.callback_query(F.data == "adm:stats")
async def adm_stats(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    rows = [[{"id": "nav_back", "cb": "adm:menu"}]]
    await screen(callback, await stats_text(), None, await keyboard(ADMIN_LANG, rows))
    await callback.answer()


# --- рассылка ---------------------------------------------------------------

async def deliver_broadcast_to(bot: Bot, user_id: int, data: dict) -> str:
    for _ in range(3):
        try:
            await send_payload(bot, user_id, data)
            return "sent"
        except TelegramRetryAfter as exc:
            await asyncio.sleep(exc.retry_after + 1)
        except TelegramForbiddenError:
            return "blocked"
        except TelegramAPIError:
            return "failed"
    return "failed"


async def run_broadcast(bot: Bot, admin_id: int, data: dict) -> None:
    global BROADCAST_RUNNING
    counters = {"sent": 0, "blocked": 0, "failed": 0}
    try:
        users = await fetchall("SELECT user_id FROM users")
        for row in users:
            status = await deliver_broadcast_to(bot, row["user_id"], data)
            counters[status] += 1
            await asyncio.sleep(BROADCAST_DELAY)
        report = (
            "📢 Рассылка завершена\n\n"
            f"✅ Доставлено: {counters['sent']}\n"
            f"🚫 Заблокировали бота: {counters['blocked']}\n"
            f"❌ Ошибки: {counters['failed']}\n"
            f"👥 Всего получателей: {len(users)}"
        )
        await bot.send_message(admin_id, report)
    except Exception:
        log.exception("Ошибка во время рассылки")
    finally:
        BROADCAST_RUNNING = False


@admin_router.callback_query(F.data == "adm:bc")
async def adm_broadcast_start(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(AdminStates.broadcast_content)
    rows = [[{"id": "nav_back", "cb": "adm:menu"}]]
    await screen(callback, "📢 Отправьте сообщение для рассылки: текст, фото или видео с подписью.",
                 None, await keyboard(ADMIN_LANG, rows))
    await callback.answer()


@admin_router.message(AdminStates.broadcast_content, F.text | F.photo | F.video)
async def adm_broadcast_preview(message: Message, state: FSMContext) -> None:
    if message.photo:
        data = {"kind": "photo", "file_id": message.photo[-1].file_id, "text": message.caption or "",
                "entities": entities_to_dicts(message.caption_entities)}
    elif message.video:
        data = {"kind": "video", "file_id": message.video.file_id, "text": message.caption or "",
                "entities": entities_to_dicts(message.caption_entities)}
    else:
        data = {"kind": "text", "file_id": None, "text": message.text,
                "entities": entities_to_dicts(message.entities)}
    await state.set_state(AdminStates.broadcast_preview)
    await state.update_data(**data)
    rows = [
        [{"text": "🚀 Начать рассылку", "cb": "adm:bc_start"}],
        [{"text": "✖️ Отменить", "cb": "adm:menu"}],
    ]
    await message.answer("👁 Предпросмотр рассылки ↓")
    await send_payload(message.bot, message.chat.id, data, await keyboard(ADMIN_LANG, rows))


@admin_router.callback_query(F.data == "adm:bc_start", StateFilter(AdminStates.broadcast_preview))
async def adm_broadcast_launch(callback: CallbackQuery, state: FSMContext) -> None:
    global BROADCAST_RUNNING
    if BROADCAST_RUNNING:
        await callback.answer("⏳ Рассылка уже идёт", show_alert=True)
        return
    data = await state.get_data()
    await state.clear()
    BROADCAST_RUNNING = True
    task = asyncio.create_task(run_broadcast(callback.bot, callback.from_user.id, data))
    BACKGROUND_TASKS.add(task)
    task.add_done_callback(BACKGROUND_TASKS.discard)
    await callback.message.answer("🚀 Рассылка запущена. Отчёт придёт по окончании.")
    await callback.answer()


# --- настройки --------------------------------------------------------------

async def admin_settings_screen(target: CallbackQuery | Message) -> None:
    username = await get_setting("support_username")
    url = await get_setting("support_url")
    text = (
        "⚙️ Настройки\n\n"
        f"👤 Username поддержки: @{username or '—'}\n"
        f"🔗 Ссылка поддержки: {url or '—'}\n\n"
        "Тексты «Поддержка» и «Информация» меняются в редакторе текстов."
    )
    rows = [
        [{"text": "👤 Username поддержки", "cb": "adm:setuser"}],
        [{"text": "🔗 Ссылка поддержки", "cb": "adm:seturl"}],
        [{"id": "nav_back", "cb": "adm:menu"}],
    ]
    await screen(target, text, None, await keyboard(ADMIN_LANG, rows))


@admin_router.callback_query(F.data == "adm:settings")
async def adm_settings(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await admin_settings_screen(callback)
    await callback.answer()


@admin_router.callback_query(F.data == "adm:setuser")
async def adm_set_username_start(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(AdminStates.support_username)
    rows = [[{"id": "nav_back", "cb": "adm:settings"}]]
    await screen(callback, "Отправьте username поддержки без @ (например, support).\nОтправьте «-», чтобы очистить.",
                 None, await keyboard(ADMIN_LANG, rows))
    await callback.answer()


@admin_router.message(AdminStates.support_username, F.text)
async def adm_set_username_save(message: Message, state: FSMContext) -> None:
    value = message.text.strip().lstrip("@")
    if value == "-":
        value = ""
    if value and not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{4,31}", value):
        await message.answer("❗ Некорректный username. Допустимы латинские буквы, цифры и _ (5–32 символа).")
        return
    await set_setting("support_username", value)
    await state.clear()
    await message.answer("✅ Сохранено.")
    await admin_settings_screen(message)


@admin_router.callback_query(F.data == "adm:seturl")
async def adm_set_url_start(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(AdminStates.support_url)
    rows = [[{"id": "nav_back", "cb": "adm:settings"}]]
    await screen(callback, "Отправьте ссылку вида https://t.me/... или «-», чтобы очистить.",
                 None, await keyboard(ADMIN_LANG, rows))
    await callback.answer()


@admin_router.message(AdminStates.support_url, F.text)
async def adm_set_url_save(message: Message, state: FSMContext) -> None:
    value = message.text.strip()
    if value == "-":
        value = ""
    elif not value.startswith("https://"):
        await message.answer("❗ Ссылка должна начинаться с https://")
        return
    await set_setting("support_url", value)
    await state.clear()
    await message.answer("✅ Сохранено.")
    await admin_settings_screen(message)


# ============================================================================
# ЗАПУСК
# ============================================================================

# --- фото разделов и товаров ----------------------------------------------

def photo_label(key: str) -> str:
    if key.startswith("product:"):
        return f"Товар #{key.split(':', 1)[1]}"
    return dict(PHOTO_SECTIONS).get(key, key)


async def admin_photo_view(target: CallbackQuery | Message, key: str) -> None:
    has_photo = await get_photo(key) is not None
    status = "✅ Фото установлено" if has_photo else "— Фото не установлено"
    text = f"🖼 {photo_label(key)}\n\n{status}\n\nОтправьте фото, чтобы заменить его, или удалите текущее."
    rows = [
        [{"text": "✏️ Изменить фото", "cb": f"adm:photoset:{key}"}],
        [{"text": "🗑 Удалить фото", "cb": f"adm:photodel:{key}"}],
        [{"id": "nav_back", "cb": "adm:photos"}],
    ]
    await screen(target, text, None, await keyboard(ADMIN_LANG, rows))


@admin_router.callback_query(F.data == "adm:photos")
async def adm_photos(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    rows = [[{"text": label, "cb": f"adm:photo:{key}"}] for key, label in PHOTO_SECTIONS]
    rows.append([{"id": "nav_back", "cb": "adm:menu"}])
    await screen(callback, "🖼 Фото разделов\n\nВыберите раздел:", None, await keyboard(ADMIN_LANG, rows))
    await callback.answer()


@admin_router.callback_query(F.data.startswith("adm:photo:"))
async def adm_photo_view_cb(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await admin_photo_view(callback, callback.data.split(":", 2)[2])
    await callback.answer()


@admin_router.callback_query(F.data.startswith("adm:photoset:"))
async def adm_photo_set_start(callback: CallbackQuery, state: FSMContext) -> None:
    key = callback.data.split(":", 2)[2]
    await state.set_state(AdminStates.photo_set)
    await state.update_data(photo_key=key)
    rows = [[{"id": "nav_back", "cb": f"adm:photo:{key}"}]]
    await screen(callback, "Отправьте фото для этого раздела.\nОтправьте «-», чтобы удалить фото.",
                 None, await keyboard(ADMIN_LANG, rows))
    await callback.answer()


@admin_router.callback_query(F.data.startswith("adm:photodel:"))
async def adm_photo_delete(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    key = callback.data.split(":", 2)[2]
    await delete_photo(key)
    await callback.answer("🗑 Фото удалено")
    await admin_photo_view(callback, key)


@admin_router.message(AdminStates.photo_set, F.photo)
async def adm_photo_save(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    key = data["photo_key"]
    await set_photo(key, message.photo[-1].file_id)
    await state.clear()
    await message.answer("✅ Фото сохранено.")
    await admin_photo_view(message, key)


@admin_router.message(AdminStates.photo_set, F.text)
async def adm_photo_text(message: Message, state: FSMContext) -> None:
    if message.text.strip() != "-":
        await message.answer("❗ Отправьте фото или «-» для удаления.")
        return
    data = await state.get_data()
    key = data["photo_key"]
    await delete_photo(key)
    await state.clear()
    await message.answer("🗑 Фото удалено.")
    await admin_photo_view(message, key)


async def main() -> None:
    if not BOT_TOKEN or BOT_TOKEN.startswith("PUT_YOUR"):
        sys.exit("Укажите BOT_TOKEN в config.py")
    if not ADMIN_IDS or ADMIN_IDS == [123456789]:
        sys.exit("Укажите свои Telegram ID в ADMIN_IDS в config.py")

    await init_db()

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.update.outer_middleware(UserMiddleware())
    dp.include_router(user_router)
    dp.include_router(admin_router)

    await bot.delete_webhook(drop_pending_updates=True)
    log.info("Бот запущен")
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        await bot.session.close()
        if db is not None:
            await db.close()



asyncio.run(main())
