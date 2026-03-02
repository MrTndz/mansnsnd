#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram Business Message Monitor Bot
Version: 7.0.0
Author: Business Monitor Team
Date: 2026-03-01

╔══════════════════════════════════════════════════════════════╗
║          МОИ 20 НОВЫХ ИДЕЙ ДЛЯ ВЕРСИИ 7.0.0                ║
╠══════════════════════════════════════════════════════════════╣
║  1. ВЕРИФИКАЦИЯ ТЕЛЕФОНА — после принятия условий            ║
║     пользователь отправляет номер, защита от мульти          ║
║  2. РЕФЕРАЛЬНАЯ СИСТЕМА — уникальные ссылки,                 ║
║     10% от платежей реферала идет пригласившему              ║
║  3. РЕАЛЬНЫЕ STARS НА ВЫВОД — 10 Stars при регистрации,      ║
║     накопление через рефералы, вывод через запрос            ║
║  4. АВТОЗАЧИСЛЕНИЕ — при получении Stars/подарков            ║
║     баланс пополняется мгновенно автоматически               ║
║  5. HTML ЭКСПОРТ ПРИ УДАЛЕНИИ ЧАТА — красивый HTML           ║
║     со всеми сообщениями, фото в base64 inline               ║
║  6. АВТООЧИСТКА МЕДИА — через 6ч после отправки              ║
║     экспорта медиафайлы удаляются с диска                    ║
║  7. ЗАХВАТ ТАЙМЕР-МЕДИА — мгновенная пересылка               ║
║     фото/видео/кружков/гс с таймером при исчезновении        ║
║  8. ПРОСМОТР ЧАТОВ В АДМИНКЕ — список бизнес-чатов           ║
║     пользователя с детальной статистикой                     ║
║  9. УМНЫЙ КАЛЬКУЛЯТОР ЦЕН — автоконвертация                  ║
║     подарки↔Stars↔рубли при выборе подписки                  ║
║ 10. СИСТЕМА ВЫВОДА STARS — запрос пользователя               ║
║     → уведомление админу → ручная выплата                    ║
║ 11. ЛЬГОТНЫЙ ПЕРИОД 24Ч — ограниченный доступ                ║
║     после истечения подписки, мягкое напоминание             ║
║ 12. ЗАЩИТА ОТ МУЛЬТИАККАУНТОВ — блокировка при               ║
║     повторной регистрации с тем же номером                   ║
║ 13. АНАЛИТИКА РЕФЕРАЛОВ — кто привёл, сколько               ║
║     заработал, история всех рефералов                        ║
║ 14. УМНЫЕ УВЕДОМЛЕНИЯ — удалённый текст как цитата/код,      ║
║     фото с описанием, полный контекст                        ║
║ 15. СТАТИСТИКА ПО ЧАТАМ — по каждому чату отдельно:         ║
║     сообщения, удаления, медиа, активность                   ║
║ 16. VIP СТАТУС — автоматически при тратах > 5000 Stars,      ║
║     особые привилегии и бонусы                               ║
║ 17. ИСТОРИЯ ТРАНЗАКЦИЙ — все пополнения/списания/            ║
║     рефералы с фильтрами и пагинацией                        ║
║ 18. КУРС STARS В РЕАЛЬНОМ ВРЕМЕНИ — отображение              ║
║     1⭐ = 1.79₽ в разделе подписок и оплаты                   ║
║ 19. МАССОВЫЕ ДЕЙСТВИЯ АДМИНА — рассылка/выдача Stars         ║
║     группе пользователей с фильтрами                         ║
║ 20. УВЕДОМЛЕНИЯ ОБ ИСТЕЧЕНИИ — за 3 дня, 1 день              ║
║     и в момент истечения подписки                            ║
╚══════════════════════════════════════════════════════════════╝
"""

import asyncio
import logging
import os
import sys
import json
import sqlite3
import hashlib
import base64
import html as html_module
import re
import random
import string
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple
from pathlib import Path
import aiofiles

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    BusinessConnection,
    BusinessMessagesDeleted,
    FSInputFile,
    BufferedInputFile,
    LabeledPrice,
    PreCheckoutQuery,
    SuccessfulPayment,
    Contact,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
    InputMediaPhoto,
    InputMediaVideo,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

# ═══════════════════════════════════════════════════════════════
# КОНФИГУРАЦИЯ
# ═══════════════════════════════════════════════════════════════

BOT_TOKEN = "8296802832:AAEU4oF4v5bjKP3KTb1rRx1Oxf-Z1dng9QQ"
ADMIN_ID = 7785371505
ADMIN_USERNAME = "mrztn"

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("bot.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# Директории
MEDIA_DIR = Path("media")
EXPORTS_DIR = Path("exports")
DB_DIR = Path("database")

for _d in [MEDIA_DIR, EXPORTS_DIR, DB_DIR]:
    _d.mkdir(exist_ok=True)

# Константы пагинации
USERS_PER_PAGE = 10
MAX_MEDIA_SIZE = 50 * 1024 * 1024  # 50 MB
MEDIA_CLEANUP_HOURS = 6            # Удалять медиа через 6 часов после экспорта
TRIAL_DAYS = 3                     # Пробный период — 3 дня
GRACE_PERIOD_HOURS = 24            # Льготный период после истечения
REFERRAL_PERCENT = 10              # % от платежа рефераладля пригласившего
REGISTRATION_STARS = 10            # Stars при регистрации
VIP_THRESHOLD_STARS = 5000         # Порог для VIP статуса

# ═══════════════════════════════════════════════════════════════
# ПРАЙС-ЛИСТ ПОДПИСОК (в Telegram Stars)
# Расчёт: 1 Star = 1.79 RUB (цена покупки для пользователя)
# После комиссии Telegram 30% → бот получает ~70%
# Цены установлены с запасом чтобы всегда быть в плюсе
# ═══════════════════════════════════════════════════════════════

SUBSCRIPTION_PRICES = {
    "week": {
        "stars": 150,
        "rub_display": 269,
        "days": 7,
        "name": "7 дней",
        "emoji": "📅",
        "net_rub": 188,      # Чистая прибыль после комиссии TG
    },
    "month": {
        "stars": 500,
        "rub_display": 895,
        "days": 30,
        "name": "1 месяц",
        "emoji": "💎",
        "net_rub": 627,
    },
    "month_3": {
        "stars": 1200,
        "rub_display": 2148,
        "days": 90,
        "name": "3 месяца",
        "emoji": "💎",
        "net_rub": 1503,
    },
    "month_6": {
        "stars": 2000,
        "rub_display": 3580,
        "days": 180,
        "name": "6 месяцев",
        "emoji": "👑",
        "net_rub": 2506,
    },
    "year": {
        "stars": 3500,
        "rub_display": 6265,
        "days": 365,
        "name": "1 год",
        "emoji": "👑",
        "net_rub": 4386,
    },
    "lifetime": {
        "stars": 15000,
        "rub_display": 26850,
        "days": None,
        "name": "Навсегда",
        "emoji": "♾️",
        "net_rub": 18795,
    },
}

STAR_TO_RUB = 1.79  # Актуальный курс покупки Stars

# ═══════════════════════════════════════════════════════════════
# FSM СОСТОЯНИЯ
# ═══════════════════════════════════════════════════════════════


class RegistrationStates(StatesGroup):
    waiting_phone = State()


class AdminStates(StatesGroup):
    main_menu = State()
    user_number_input = State()
    send_message = State()
    gift_subscription = State()
    send_stars = State()
    broadcast_message = State()
    search_user = State()
    bulk_stars = State()


class WithdrawStates(StatesGroup):
    enter_amount = State()
    confirm = State()


class SubscriptionStates(StatesGroup):
    choosing_plan = State()


# ═══════════════════════════════════════════════════════════════
# БАЗА ДАННЫХ
# ═══════════════════════════════════════════════════════════════


class Database:
    """Полный класс для работы с SQLite базой данных."""

    def __init__(self, db_path: str = "database/bot.db"):
        self.db_path = db_path
        self.init_database()

    def get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def init_database(self):
        """Инициализация всех таблиц БД версии 7.0.0."""
        conn = self.get_connection()
        cur = conn.cursor()

        # ── Пользователи ──────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id             INTEGER PRIMARY KEY,
                username            TEXT,
                first_name          TEXT,
                last_name           TEXT,
                phone               TEXT UNIQUE,
                registered_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_activity       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                accepted_terms      BOOLEAN DEFAULT 0,
                phone_verified      BOOLEAN DEFAULT 0,
                is_active           BOOLEAN DEFAULT 1,
                is_blocked          BOOLEAN DEFAULT 0,
                subscription_type   TEXT DEFAULT 'free',
                subscription_expires TIMESTAMP,
                trial_used          BOOLEAN DEFAULT 0,
                grace_period_ends   TIMESTAMP,
                vip_status          BOOLEAN DEFAULT 0,
                referral_code       TEXT UNIQUE,
                referred_by         INTEGER,
                stars_balance       INTEGER DEFAULT 0,
                stars_earned_total  INTEGER DEFAULT 0,
                stars_withdrawn     INTEGER DEFAULT 0,
                total_spent_stars   INTEGER DEFAULT 0,
                total_messages_saved INTEGER DEFAULT 0,
                total_deletions_tracked INTEGER DEFAULT 0,
                total_edits_tracked INTEGER DEFAULT 0,
                total_media_saved   INTEGER DEFAULT 0,
                total_photo         INTEGER DEFAULT 0,
                total_video         INTEGER DEFAULT 0,
                total_video_note    INTEGER DEFAULT 0,
                total_voice         INTEGER DEFAULT 0,
                total_audio         INTEGER DEFAULT 0,
                total_document      INTEGER DEFAULT 0,
                total_sticker       INTEGER DEFAULT 0,
                FOREIGN KEY (referred_by) REFERENCES users(user_id)
            )
        """)

        # ── Бизнес-подключения ────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS business_connections (
                connection_id       TEXT PRIMARY KEY,
                user_id             INTEGER,
                connected_user_id   INTEGER,
                is_enabled          BOOLEAN DEFAULT 1,
                can_reply           BOOLEAN DEFAULT 0,
                created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                disabled_at         TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        """)

        # ── Сохранённые сообщения ─────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS saved_messages (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id             INTEGER,
                connection_id       TEXT,
                chat_id             INTEGER,
                chat_title          TEXT,
                message_id          INTEGER,
                sender_id           INTEGER,
                sender_username     TEXT,
                sender_name         TEXT,
                message_text        TEXT,
                media_type          TEXT,
                media_file_id       TEXT,
                media_file_path     TEXT,
                media_thumbnail_path TEXT,
                caption             TEXT,
                has_timer           BOOLEAN DEFAULT 0,
                timer_seconds       INTEGER,
                timer_expires       TIMESTAMP,
                is_view_once        BOOLEAN DEFAULT 0,
                has_spoiler         BOOLEAN DEFAULT 0,
                media_width         INTEGER,
                media_height        INTEGER,
                media_duration      INTEGER,
                media_file_size     INTEGER,
                created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_deleted          BOOLEAN DEFAULT 0,
                deleted_at          TIMESTAMP,
                is_edited           BOOLEAN DEFAULT 0,
                edited_at           TIMESTAMP,
                original_text       TEXT,
                media_sent_on_delete BOOLEAN DEFAULT 0,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        """)

        # ── Экспорты чатов ────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS chat_exports (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id             INTEGER,
                connection_id       TEXT,
                chat_id             INTEGER,
                chat_title          TEXT,
                file_path           TEXT,
                messages_count      INTEGER DEFAULT 0,
                media_count         INTEGER DEFAULT 0,
                cleanup_after       TIMESTAMP,
                cleaned_up          BOOLEAN DEFAULT 0,
                exported_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        """)

        # ── Платежи ───────────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS payments (
                id                          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id                     INTEGER,
                amount_stars                INTEGER,
                plan_type                   TEXT,
                payment_method              TEXT DEFAULT 'stars',
                telegram_payment_charge_id  TEXT,
                provider_payment_charge_id  TEXT,
                invoice_payload             TEXT,
                status                      TEXT DEFAULT 'pending',
                referral_bonus_paid         BOOLEAN DEFAULT 0,
                created_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                confirmed_at                TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        """)

        # ── Транзакции Stars ──────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stars_transactions (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id             INTEGER,
                amount              INTEGER,
                transaction_type    TEXT,
                description         TEXT,
                related_user_id     INTEGER,
                created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        """)

        # ── Запросы на вывод ──────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS withdrawal_requests (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id             INTEGER,
                amount_stars        INTEGER,
                status              TEXT DEFAULT 'pending',
                admin_comment       TEXT,
                created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                processed_at        TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        """)

        # ── Действия администратора ───────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS admin_actions (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id            INTEGER,
                target_user_id      INTEGER,
                action_type         TEXT,
                action_details      TEXT,
                created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # ── Медиа с таймерами (быстрый доступ) ───────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS timer_media (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id             INTEGER,
                connection_id       TEXT,
                chat_id             INTEGER,
                message_id          INTEGER,
                media_type          TEXT,
                media_file_path     TEXT,
                sender_username     TEXT,
                captured_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                sent_to_user        BOOLEAN DEFAULT 0,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        """)

        # ── Индексы ───────────────────────────────────────────
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_msg_lookup
            ON saved_messages(user_id, chat_id, message_id)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_msg_deleted
            ON saved_messages(user_id, is_deleted)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_conn_user
            ON business_connections(user_id, is_enabled)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_timer_media
            ON timer_media(user_id, chat_id, message_id)
        """)

        conn.commit()
        conn.close()
        logger.info("База данных инициализирована (v7.0.0)")

    # ─── ПОЛЬЗОВАТЕЛИ ─────────────────────────────────────────

    def add_user(
        self,
        user_id: int,
        username: str = None,
        first_name: str = None,
        last_name: str = None,
        referral_code: str = None,
        referred_by: int = None,
    ) -> bool:
        conn = self.get_connection()
        cur = conn.cursor()
        try:
            if not referral_code:
                referral_code = self._generate_referral_code(user_id)
            cur.execute(
                """
                INSERT OR IGNORE INTO users
                    (user_id, username, first_name, last_name,
                     referral_code, referred_by)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                (user_id, username, first_name, last_name, referral_code, referred_by),
            )
            conn.commit()
            return cur.rowcount > 0
        except Exception as e:
            logger.error(f"add_user error: {e}")
            return False
        finally:
            conn.close()

    def _generate_referral_code(self, user_id: int) -> str:
        """Генерация уникального реферального кода."""
        base = hashlib.md5(f"{user_id}{datetime.now().timestamp()}".encode()).hexdigest()[:8].upper()
        return f"REF{base}"

    def get_user(self, user_id: int) -> Optional[Dict]:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        conn.close()
        return dict(row) if row else None

    def get_user_by_phone(self, phone: str) -> Optional[Dict]:
        phone_clean = re.sub(r"\D", "", phone)
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM users WHERE REPLACE(REPLACE(phone,'+',''),'-','') LIKE ?",
            (f"%{phone_clean[-10:]}",),
        )
        row = cur.fetchone()
        conn.close()
        return dict(row) if row else None

    def get_user_by_referral_code(self, code: str) -> Optional[Dict]:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE referral_code = ?", (code.upper(),))
        row = cur.fetchone()
        conn.close()
        return dict(row) if row else None

    def update_user_phone(self, user_id: int, phone: str) -> bool:
        conn = self.get_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "UPDATE users SET phone = ?, phone_verified = 1 WHERE user_id = ?",
                (phone, user_id),
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False  # Номер уже используется
        finally:
            conn.close()

    def accept_terms(self, user_id: int):
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            "UPDATE users SET accepted_terms = 1 WHERE user_id = ?", (user_id,)
        )
        conn.commit()
        conn.close()

    def update_user_activity(self, user_id: int):
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            "UPDATE users SET last_activity = CURRENT_TIMESTAMP WHERE user_id = ?",
            (user_id,),
        )
        conn.commit()
        conn.close()

    def block_user(self, user_id: int, reason: str = ""):
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("UPDATE users SET is_blocked = 1 WHERE user_id = ?", (user_id,))
        conn.commit()
        conn.close()
        self.log_admin_action(ADMIN_ID, user_id, "block", reason or "Заблокирован")

    def unblock_user(self, user_id: int):
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("UPDATE users SET is_blocked = 0 WHERE user_id = ?", (user_id,))
        conn.commit()
        conn.close()
        self.log_admin_action(ADMIN_ID, user_id, "unblock", "Разблокирован")

    # ─── ПОДПИСКИ ─────────────────────────────────────────────

    def activate_trial(self, user_id: int) -> bool:
        user = self.get_user(user_id)
        if not user or user["trial_used"]:
            return False
        expires = datetime.now() + timedelta(days=TRIAL_DAYS)
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE users
            SET subscription_type = 'trial',
                subscription_expires = ?,
                trial_used = 1
            WHERE user_id = ?
        """,
            (expires, user_id),
        )
        conn.commit()
        conn.close()
        return True

    def activate_subscription(self, user_id: int, plan_key: str, days: int = None):
        plan = SUBSCRIPTION_PRICES[plan_key]
        if plan["days"] is None:
            expires = None
            sub_type = "lifetime"
        else:
            d = days if days else plan["days"]
            # Если уже есть активная подписка — добавляем дни
            user = self.get_user(user_id)
            if user and user["subscription_expires"]:
                try:
                    current_expires = datetime.fromisoformat(user["subscription_expires"])
                    if current_expires > datetime.now():
                        expires = current_expires + timedelta(days=d)
                    else:
                        expires = datetime.now() + timedelta(days=d)
                except Exception:
                    expires = datetime.now() + timedelta(days=d)
            else:
                expires = datetime.now() + timedelta(days=d)
            sub_type = plan_key

        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE users
            SET subscription_type = ?,
                subscription_expires = ?,
                grace_period_ends = NULL
            WHERE user_id = ?
        """,
            (sub_type, expires, user_id),
        )
        conn.commit()
        conn.close()
        self.log_admin_action(
            ADMIN_ID, user_id, "subscription",
            f"Plan={plan_key}, expires={expires}"
        )

    def check_subscription(self, user_id: int) -> bool:
        """Проверяет доступ с учётом льготного периода."""
        user = self.get_user(user_id)
        if not user or user["is_blocked"]:
            return False
        if user["subscription_type"] == "free":
            return False
        if user["subscription_type"] == "lifetime":
            return True
        if user["subscription_expires"]:
            try:
                expires = datetime.fromisoformat(user["subscription_expires"])
                if datetime.now() <= expires:
                    return True
                # Проверяем льготный период
                if user["grace_period_ends"]:
                    grace = datetime.fromisoformat(user["grace_period_ends"])
                    if datetime.now() <= grace:
                        return True  # В льготном периоде
            except Exception:
                pass
        return False

    def deactivate_subscription(self, user_id: int):
        grace = datetime.now() + timedelta(hours=GRACE_PERIOD_HOURS)
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE users
            SET subscription_type = 'free',
                subscription_expires = NULL,
                grace_period_ends = ?
            WHERE user_id = ?
        """,
            (grace, user_id),
        )
        conn.commit()
        conn.close()

    def check_and_update_vip(self, user_id: int):
        """Обновляет VIP статус на основе суммарных трат."""
        user = self.get_user(user_id)
        if not user:
            return
        if user["total_spent_stars"] >= VIP_THRESHOLD_STARS and not user["vip_status"]:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(
                "UPDATE users SET vip_status = 1 WHERE user_id = ?", (user_id,)
            )
            conn.commit()
            conn.close()

    # ─── STARS ────────────────────────────────────────────────

    def add_stars(self, user_id: int, amount: int, description: str = "", related_user: int = None):
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE users
            SET stars_balance = stars_balance + ?,
                stars_earned_total = stars_earned_total + ?
            WHERE user_id = ?
        """,
            (amount, amount, user_id),
        )
        cur.execute(
            """
            INSERT INTO stars_transactions
                (user_id, amount, transaction_type, description, related_user_id)
            VALUES (?, ?, 'add', ?, ?)
        """,
            (user_id, amount, description, related_user),
        )
        conn.commit()
        conn.close()

    def remove_stars(self, user_id: int, amount: int, description: str = "") -> bool:
        user = self.get_user(user_id)
        if not user or user["stars_balance"] < amount:
            return False
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE users
            SET stars_balance = stars_balance - ?,
                total_spent_stars = total_spent_stars + ?
            WHERE user_id = ?
        """,
            (amount, amount, user_id),
        )
        cur.execute(
            """
            INSERT INTO stars_transactions
                (user_id, amount, transaction_type, description)
            VALUES (?, ?, 'remove', ?)
        """,
            (user_id, -amount, description),
        )
        conn.commit()
        conn.close()
        self.check_and_update_vip(user_id)
        return True

    def get_stars_balance(self, user_id: int) -> int:
        user = self.get_user(user_id)
        return user["stars_balance"] if user else 0

    def get_stars_history(self, user_id: int, limit: int = 20, offset: int = 0) -> List[Dict]:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT * FROM stars_transactions
            WHERE user_id = ?
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
        """,
            (user_id, limit, offset),
        )
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    # ─── ВЫВОД STARS ──────────────────────────────────────────

    def create_withdrawal_request(self, user_id: int, amount: int) -> int:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO withdrawal_requests (user_id, amount_stars)
            VALUES (?, ?)
        """,
            (user_id, amount),
        )
        req_id = cur.lastrowid
        conn.commit()
        conn.close()
        # Резервируем Stars
        self.remove_stars(user_id, amount, f"Запрос на вывод #{req_id}")
        return req_id

    def approve_withdrawal(self, req_id: int):
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE withdrawal_requests
            SET status = 'approved', processed_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """,
            (req_id,),
        )
        cur.execute(
            "SELECT user_id, amount_stars FROM withdrawal_requests WHERE id = ?",
            (req_id,),
        )
        row = cur.fetchone()
        conn.commit()
        conn.close()
        if row:
            conn2 = self.get_connection()
            cur2 = conn2.cursor()
            cur2.execute(
                "UPDATE users SET stars_withdrawn = stars_withdrawn + ? WHERE user_id = ?",
                (row["amount_stars"], row["user_id"]),
            )
            conn2.commit()
            conn2.close()
        return dict(row) if row else None

    def reject_withdrawal(self, req_id: int, comment: str = ""):
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT user_id, amount_stars FROM withdrawal_requests WHERE id = ?",
            (req_id,),
        )
        row = cur.fetchone()
        if row:
            # Возвращаем Stars
            self.add_stars(row["user_id"], row["amount_stars"], f"Возврат: отклонён вывод #{req_id}")
        cur.execute(
            """
            UPDATE withdrawal_requests
            SET status = 'rejected', admin_comment = ?, processed_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """,
            (comment, req_id),
        )
        conn.commit()
        conn.close()
        return dict(row) if row else None

    def get_pending_withdrawals(self) -> List[Dict]:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT w.*, u.username, u.first_name
            FROM withdrawal_requests w
            JOIN users u ON w.user_id = u.user_id
            WHERE w.status = 'pending'
            ORDER BY w.created_at ASC
        """
        )
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    # ─── РЕФЕРАЛЫ ─────────────────────────────────────────────

    def get_referral_stats(self, user_id: int) -> Dict:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) as total FROM users WHERE referred_by = ?", (user_id,)
        )
        total = cur.fetchone()["total"]
        cur.execute(
            """
            SELECT COALESCE(SUM(ABS(amount)), 0) as earned
            FROM stars_transactions
            WHERE user_id = ? AND transaction_type = 'add'
              AND description LIKE '%реферал%'
        """,
            (user_id,),
        )
        earned = cur.fetchone()["earned"]
        cur.execute(
            """
            SELECT u.user_id, u.username, u.first_name, u.subscription_type,
                   u.registered_at
            FROM users u WHERE u.referred_by = ?
            ORDER BY u.registered_at DESC LIMIT 20
        """,
            (user_id,),
        )
        refs = [dict(r) for r in cur.fetchall()]
        conn.close()
        return {"total": total, "earned_stars": earned, "referrals": refs}

    def pay_referral_bonus(self, referred_user_id: int, payment_stars: int):
        """Начисляет бонус пригласившему при первой оплате реферала."""
        user = self.get_user(referred_user_id)
        if not user or not user["referred_by"]:
            return
        inviter_id = user["referred_by"]
        bonus = int(payment_stars * REFERRAL_PERCENT / 100)
        if bonus < 1:
            bonus = 1
        self.add_stars(
            inviter_id,
            bonus,
            f"Реферальный бонус {REFERRAL_PERCENT}% от оплаты реферала {referred_user_id}",
            related_user=referred_user_id,
        )
        return bonus

    # ─── БИЗНЕС-ПОДКЛЮЧЕНИЯ ───────────────────────────────────

    def add_business_connection(
        self, connection_id: str, user_id: int, connected_user_id: int, can_reply: bool = False
    ) -> bool:
        conn = self.get_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT OR REPLACE INTO business_connections
                    (connection_id, user_id, connected_user_id, can_reply, is_enabled)
                VALUES (?, ?, ?, ?, 1)
            """,
                (connection_id, user_id, connected_user_id, can_reply),
            )
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"add_business_connection error: {e}")
            return False
        finally:
            conn.close()

    def disable_business_connection(self, connection_id: str):
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE business_connections
            SET is_enabled = 0, disabled_at = CURRENT_TIMESTAMP
            WHERE connection_id = ?
        """,
            (connection_id,),
        )
        conn.commit()
        conn.close()

    def get_business_connection(self, connection_id: str) -> Optional[Dict]:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM business_connections WHERE connection_id = ?",
            (connection_id,),
        )
        row = cur.fetchone()
        conn.close()
        return dict(row) if row else None

    def get_user_connections(self, user_id: int) -> List[Dict]:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM business_connections WHERE user_id = ? ORDER BY created_at DESC",
            (user_id,),
        )
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_user_chats(self, user_id: int) -> List[Dict]:
        """Уникальные чаты пользователя со статистикой."""
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT chat_id, chat_title,
                   COUNT(*) as total_messages,
                   SUM(CASE WHEN is_deleted=1 THEN 1 ELSE 0 END) as deleted_count,
                   SUM(CASE WHEN media_type IS NOT NULL THEN 1 ELSE 0 END) as media_count,
                   MAX(created_at) as last_message_at
            FROM saved_messages
            WHERE user_id = ?
            GROUP BY chat_id
            ORDER BY last_message_at DESC
        """,
            (user_id,),
        )
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    # ─── СООБЩЕНИЯ ────────────────────────────────────────────

    def save_message(
        self,
        user_id: int,
        connection_id: str,
        chat_id: int,
        message_id: int,
        sender_id: int,
        sender_username: str = None,
        sender_name: str = None,
        message_text: str = None,
        media_type: str = None,
        media_file_id: str = None,
        media_file_path: str = None,
        media_thumbnail_path: str = None,
        caption: str = None,
        has_timer: bool = False,
        timer_seconds: int = None,
        is_view_once: bool = False,
        has_spoiler: bool = False,
        media_width: int = None,
        media_height: int = None,
        media_duration: int = None,
        media_file_size: int = None,
        chat_title: str = None,
    ) -> Optional[int]:
        conn = self.get_connection()
        cur = conn.cursor()
        try:
            timer_expires = None
            if has_timer and timer_seconds:
                timer_expires = datetime.now() + timedelta(seconds=timer_seconds)

            cur.execute(
                """
                INSERT INTO saved_messages
                    (user_id, connection_id, chat_id, chat_title, message_id,
                     sender_id, sender_username, sender_name, message_text,
                     media_type, media_file_id, media_file_path, media_thumbnail_path,
                     caption, has_timer, timer_seconds, timer_expires, is_view_once,
                     has_spoiler, media_width, media_height, media_duration, media_file_size)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
                (
                    user_id, connection_id, chat_id, chat_title, message_id,
                    sender_id, sender_username, sender_name, message_text,
                    media_type, media_file_id, media_file_path, media_thumbnail_path,
                    caption, has_timer, timer_seconds, timer_expires, is_view_once,
                    has_spoiler, media_width, media_height, media_duration, media_file_size,
                ),
            )
            msg_db_id = cur.lastrowid

            # Обновляем статистику пользователя
            cur.execute(
                "UPDATE users SET total_messages_saved = total_messages_saved + 1 WHERE user_id = ?",
                (user_id,),
            )
            if media_type:
                cur.execute(
                    "UPDATE users SET total_media_saved = total_media_saved + 1 WHERE user_id = ?",
                    (user_id,),
                )
                col = f"total_{media_type}"
                if col in [
                    "total_photo", "total_video", "total_video_note",
                    "total_voice", "total_audio", "total_document", "total_sticker"
                ]:
                    cur.execute(
                        f"UPDATE users SET {col} = {col} + 1 WHERE user_id = ?",
                        (user_id,),
                    )
            conn.commit()
            return msg_db_id
        except Exception as e:
            logger.error(f"save_message error: {e}")
            return None
        finally:
            conn.close()

    def get_message(self, user_id: int, chat_id: int, message_id: int) -> Optional[Dict]:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT * FROM saved_messages
            WHERE user_id = ? AND chat_id = ? AND message_id = ?
            ORDER BY created_at DESC LIMIT 1
        """,
            (user_id, chat_id, message_id),
        )
        row = cur.fetchone()
        conn.close()
        return dict(row) if row else None

    def mark_message_deleted(self, user_id: int, chat_id: int, message_id: int) -> bool:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE saved_messages
            SET is_deleted = 1, deleted_at = CURRENT_TIMESTAMP
            WHERE user_id = ? AND chat_id = ? AND message_id = ? AND is_deleted = 0
        """,
            (user_id, chat_id, message_id),
        )
        affected = cur.rowcount
        if affected > 0:
            cur.execute(
                "UPDATE users SET total_deletions_tracked = total_deletions_tracked + 1 WHERE user_id = ?",
                (user_id,),
            )
        conn.commit()
        conn.close()
        return affected > 0

    def mark_message_edited(self, user_id: int, chat_id: int, message_id: int, original_text: str):
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE saved_messages
            SET is_edited = 1, edited_at = CURRENT_TIMESTAMP, original_text = ?
            WHERE user_id = ? AND chat_id = ? AND message_id = ?
        """,
            (original_text, user_id, chat_id, message_id),
        )
        affected = cur.rowcount
        if affected > 0:
            cur.execute(
                "UPDATE users SET total_edits_tracked = total_edits_tracked + 1 WHERE user_id = ?",
                (user_id,),
            )
        conn.commit()
        conn.close()

    def get_chat_messages(self, user_id: int, chat_id: int) -> List[Dict]:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT * FROM saved_messages
            WHERE user_id = ? AND chat_id = ?
            ORDER BY created_at ASC
        """,
            (user_id, chat_id),
        )
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_connection_messages(self, user_id: int, connection_id: str) -> List[Dict]:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT * FROM saved_messages
            WHERE user_id = ? AND connection_id = ?
            ORDER BY created_at ASC
        """,
            (user_id, connection_id),
        )
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    # ─── ТАЙМЕР-МЕДИА ─────────────────────────────────────────

    def save_timer_media(
        self,
        user_id: int,
        connection_id: str,
        chat_id: int,
        message_id: int,
        media_type: str,
        media_file_path: str,
        sender_username: str = None,
    ):
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT OR REPLACE INTO timer_media
                (user_id, connection_id, chat_id, message_id, media_type,
                 media_file_path, sender_username)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
            (user_id, connection_id, chat_id, message_id, media_type, media_file_path, sender_username),
        )
        conn.commit()
        conn.close()

    def get_timer_media(self, user_id: int, chat_id: int, message_id: int) -> Optional[Dict]:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT * FROM timer_media
            WHERE user_id = ? AND chat_id = ? AND message_id = ?
        """,
            (user_id, chat_id, message_id),
        )
        row = cur.fetchone()
        conn.close()
        return dict(row) if row else None

    def mark_timer_media_sent(self, timer_media_id: int):
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            "UPDATE timer_media SET sent_to_user = 1 WHERE id = ?", (timer_media_id,)
        )
        conn.commit()
        conn.close()

    # ─── АДМИНИСТРАТИВНЫЕ МЕТОДЫ ──────────────────────────────

    def get_all_users(self, limit: int = None, offset: int = 0) -> List[Dict]:
        conn = self.get_connection()
        cur = conn.cursor()
        if limit:
            cur.execute(
                "SELECT * FROM users ORDER BY registered_at DESC LIMIT ? OFFSET ?",
                (limit, offset),
            )
        else:
            cur.execute("SELECT * FROM users ORDER BY registered_at DESC")
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def search_users(self, query: str) -> List[Dict]:
        conn = self.get_connection()
        cur = conn.cursor()
        q = f"%{query}%"
        cur.execute(
            """
            SELECT * FROM users
            WHERE CAST(user_id AS TEXT) LIKE ?
               OR username LIKE ?
               OR first_name LIKE ?
               OR last_name LIKE ?
               OR phone LIKE ?
            ORDER BY registered_at DESC LIMIT 20
        """,
            (q, q, q, q, q),
        )
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_user_count(self) -> int:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) as c FROM users")
        result = cur.fetchone()
        conn.close()
        return result["c"] if result else 0

    def get_active_subscriptions_count(self) -> int:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*) as c FROM users
            WHERE subscription_type NOT IN ('free')
              AND (subscription_expires IS NULL OR subscription_expires > CURRENT_TIMESTAMP)
              AND is_blocked = 0
        """
        )
        result = cur.fetchone()
        conn.close()
        return result["c"] if result else 0

    def get_total_messages(self) -> int:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) as c FROM saved_messages")
        r = cur.fetchone()
        conn.close()
        return r["c"] if r else 0

    def get_total_deletions(self) -> int:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) as c FROM saved_messages WHERE is_deleted=1")
        r = cur.fetchone()
        conn.close()
        return r["c"] if r else 0

    def get_today_new_users(self) -> int:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) as c FROM users WHERE DATE(registered_at) = DATE('now')"
        )
        r = cur.fetchone()
        conn.close()
        return r["c"] if r else 0

    def get_vip_users_count(self) -> int:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) as c FROM users WHERE vip_status = 1")
        r = cur.fetchone()
        conn.close()
        return r["c"] if r else 0

    def get_expiring_subscriptions(self, days: int = 3) -> List[Dict]:
        """Подписки, истекающие в течение N дней."""
        conn = self.get_connection()
        cur = conn.cursor()
        deadline = datetime.now() + timedelta(days=days)
        cur.execute(
            """
            SELECT * FROM users
            WHERE subscription_expires IS NOT NULL
              AND subscription_expires > CURRENT_TIMESTAMP
              AND subscription_expires <= ?
              AND subscription_type != 'free'
              AND is_blocked = 0
        """,
            (deadline,),
        )
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def save_payment(
        self,
        user_id: int,
        amount_stars: int,
        plan_type: str,
        tg_charge_id: str,
        provider_charge_id: str,
        payload: str,
    ):
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO payments
                (user_id, amount_stars, plan_type, telegram_payment_charge_id,
                 provider_payment_charge_id, invoice_payload, status, confirmed_at)
            VALUES (?, ?, ?, ?, ?, ?, 'completed', CURRENT_TIMESTAMP)
        """,
            (user_id, amount_stars, plan_type, tg_charge_id, provider_charge_id, payload),
        )
        conn.commit()
        conn.close()

    def log_admin_action(self, admin_id: int, target_id: int, action: str, details: str = ""):
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO admin_actions (admin_id, target_user_id, action_type, action_details)
            VALUES (?, ?, ?, ?)
        """,
            (admin_id, target_id, action, details),
        )
        conn.commit()
        conn.close()

    def get_admin_history(self, user_id: int) -> List[Dict]:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT * FROM admin_actions
            WHERE target_user_id = ?
            ORDER BY created_at DESC LIMIT 20
        """,
            (user_id,),
        )
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    # ─── ЭКСПОРТЫ ─────────────────────────────────────────────

    def save_export_record(
        self,
        user_id: int,
        connection_id: str,
        chat_id: int,
        chat_title: str,
        file_path: str,
        messages_count: int,
        media_count: int,
    ) -> int:
        cleanup_after = datetime.now() + timedelta(hours=MEDIA_CLEANUP_HOURS)
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO chat_exports
                (user_id, connection_id, chat_id, chat_title, file_path,
                 messages_count, media_count, cleanup_after)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (user_id, connection_id, chat_id, chat_title, file_path,
             messages_count, media_count, cleanup_after),
        )
        export_id = cur.lastrowid
        conn.commit()
        conn.close()
        return export_id

    def get_pending_cleanups(self) -> List[Dict]:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT * FROM chat_exports
            WHERE cleaned_up = 0 AND cleanup_after <= CURRENT_TIMESTAMP
        """
        )
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def mark_cleanup_done(self, export_id: int):
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            "UPDATE chat_exports SET cleaned_up = 1 WHERE id = ?", (export_id,)
        )
        conn.commit()
        conn.close()


# ─── Создание экземпляра БД ────────────────────────────────────
db = Database()


# ═══════════════════════════════════════════════════════════════
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ═══════════════════════════════════════════════════════════════


async def download_media(
    bot: Bot,
    file_id: str,
    media_type: str,
    user_id: int,
    prefix: str = "",
) -> Optional[str]:
    """Скачивает медиафайл и возвращает путь на диске."""
    try:
        file = await bot.get_file(file_id)
        if not file or not file.file_path:
            return None
        ext = file.file_path.rsplit(".", 1)[-1] if "." in file.file_path else "bin"
        user_dir = MEDIA_DIR / str(user_id)
        user_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        fhash = hashlib.md5(file_id.encode()).hexdigest()[:8]
        filename = f"{prefix}{media_type}_{ts}_{fhash}.{ext}"
        dest = user_dir / filename
        await bot.download_file(file.file_path, destination=str(dest))
        logger.info(f"Media saved: {dest}")
        return str(dest)
    except Exception as e:
        logger.error(f"download_media error: {e}")
        return None


def format_subscription_info(user: Dict) -> str:
    """Возвращает форматированный статус подписки."""
    if user["is_blocked"]:
        return "🚫 Заблокирован"
    sub = user["subscription_type"]
    if sub == "free":
        return "🆓 Бесплатный"
    if sub == "trial":
        if user["subscription_expires"]:
            try:
                exp = datetime.fromisoformat(user["subscription_expires"])
                days = (exp - datetime.now()).days
                return f"🎁 Пробный ({max(0,days)}д)"
            except Exception:
                pass
        return "🎁 Пробный"
    if sub == "lifetime":
        return "♾️ Навсегда"
    if sub in SUBSCRIPTION_PRICES and user["subscription_expires"]:
        try:
            exp = datetime.fromisoformat(user["subscription_expires"])
            days = (exp - datetime.now()).days
            name = SUBSCRIPTION_PRICES[sub]["name"]
            emoji = SUBSCRIPTION_PRICES[sub]["emoji"]
            if days < 0:
                grace = user.get("grace_period_ends")
                if grace:
                    try:
                        g = datetime.fromisoformat(grace)
                        if datetime.now() <= g:
                            gh = int((g - datetime.now()).total_seconds() / 3600)
                            return f"⏳ Льготный период ({gh}ч)"
                    except Exception:
                        pass
                return "⏰ Истекла"
            return f"{emoji} {name} ({max(0,days)}д)"
        except Exception:
            pass
    return "💎 Активна"


def format_user_line(user: Dict, index: int) -> str:
    status = "🚫" if user["is_blocked"] else "✅"
    vip = "👑" if user.get("vip_status") else ""
    uname = f"@{user['username']}" if user["username"] else "без username"
    name = user["first_name"] or "?"
    sub_short = {
        "free": "🆓", "trial": "🎁", "week": "📅",
        "month": "💎", "month_3": "💎", "month_6": "👑",
        "year": "👑", "lifetime": "♾️",
    }.get(user["subscription_type"], "❓")
    return f"{index}. {status}{vip}{sub_short} {name} ({uname})\n   ID: {user['user_id']}"


def stars_to_rub(stars: int) -> float:
    return round(stars * STAR_TO_RUB, 2)


async def generate_chat_html(
    user: Dict,
    messages: List[Dict],
    chat_title: str,
    connection_id: str,
) -> str:
    """Генерирует красивый HTML файл с историей чата, включая медиа в base64."""

    def escape(text: str) -> str:
        return html_module.escape(str(text)) if text else ""

    rows_html = ""
    for msg in messages:
        ts = msg.get("created_at", "")[:19]
        sender = msg.get("sender_username") or msg.get("sender_name") or str(msg.get("sender_id", "?"))
        text = msg.get("message_text") or msg.get("caption") or ""
        media_type = msg.get("media_type") or ""
        is_deleted = msg.get("is_deleted", False)
        is_edited = msg.get("is_edited", False)
        has_timer = msg.get("has_timer", False)
        is_view_once = msg.get("is_view_once", False)

        tags = []
        if is_deleted:
            tags.append('<span class="tag del">🗑 УДАЛЕНО</span>')
        if is_edited:
            tags.append('<span class="tag edit">✏️ ИЗМЕНЕНО</span>')
        if has_timer:
            tags.append('<span class="tag timer">⏱ ТАЙМЕР</span>')
        if is_view_once:
            tags.append('<span class="tag once">👁 ОДНОРАЗОВО</span>')

        tags_html = " ".join(tags)
        row_class = "deleted" if is_deleted else "normal"

        # Медиа
        media_html = ""
        media_path = msg.get("media_file_path")
        if media_path and Path(media_path).exists():
            try:
                async with aiofiles.open(media_path, "rb") as f:
                    data = await f.read()
                b64 = base64.b64encode(data).decode()
                if media_type == "photo":
                    media_html = f'<img src="data:image/jpeg;base64,{b64}" class="media-img" />'
                elif media_type in ("video", "video_note"):
                    media_html = (
                        f'<video controls class="media-video">'
                        f'<source src="data:video/mp4;base64,{b64}" type="video/mp4"></video>'
                    )
                elif media_type in ("voice", "audio"):
                    media_html = (
                        f'<audio controls class="media-audio">'
                        f'<source src="data:audio/ogg;base64,{b64}"></audio>'
                    )
                elif media_type == "document":
                    fname = Path(media_path).name
                    media_html = (
                        f'<a href="data:application/octet-stream;base64,{b64}" '
                        f'download="{escape(fname)}" class="doc-link">📎 {escape(fname)}</a>'
                    )
                else:
                    media_html = f'<div class="media-placeholder">📎 {escape(media_type.upper())}</div>'
            except Exception as e:
                media_html = f'<div class="media-placeholder">❌ Ошибка загрузки медиа: {escape(str(e))}</div>'
        elif media_type:
            media_html = f'<div class="media-placeholder">📎 {escape(media_type.upper())} (файл недоступен)</div>'

        # Текст как цитата
        text_html = ""
        if text:
            text_html = f'<blockquote class="msg-text">{escape(text)}</blockquote>'

        # Оригинальный текст при редактировании
        orig_html = ""
        if is_edited and msg.get("original_text"):
            orig_html = (
                f'<div class="original-text">'
                f'<span class="orig-label">Оригинал:</span> '
                f'<code>{escape(msg["original_text"])}</code>'
                f'</div>'
            )

        rows_html += f"""
        <div class="message {row_class}">
            <div class="msg-header">
                <span class="sender">@{escape(sender)}</span>
                <span class="time">{escape(ts)}</span>
                <span class="tags">{tags_html}</span>
            </div>
            {media_html}
            {text_html}
            {orig_html}
        </div>
        """

    user_name = f"{user.get('first_name','')} ({user.get('username','')})"
    export_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg_count = len(messages)

    html_content = f"""<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Экспорт чата — {escape(chat_title)}</title>
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            background: #0d1117; color: #e6edf3;
            padding: 20px; line-height: 1.5;
        }}
        .header {{
            background: linear-gradient(135deg, #1f2937, #374151);
            border-radius: 12px; padding: 20px; margin-bottom: 20px;
            border: 1px solid #374151;
        }}
        .header h1 {{ font-size: 1.5em; color: #60a5fa; margin-bottom: 8px; }}
        .header .meta {{ color: #9ca3af; font-size: 0.9em; }}
        .header .meta span {{ margin-right: 15px; }}
        .message {{
            background: #161b22; border-radius: 8px; padding: 12px 16px;
            margin-bottom: 10px; border-left: 3px solid #30363d;
            transition: border-color 0.2s;
        }}
        .message.deleted {{ border-left-color: #ef4444; background: #1c0a0a; }}
        .message.normal {{ border-left-color: #3b82f6; }}
        .msg-header {{
            display: flex; align-items: center; gap: 10px;
            margin-bottom: 8px; flex-wrap: wrap;
        }}
        .sender {{ font-weight: bold; color: #60a5fa; }}
        .time {{ color: #6b7280; font-size: 0.85em; }}
        .tags {{ display: flex; gap: 5px; flex-wrap: wrap; }}
        .tag {{
            padding: 2px 8px; border-radius: 12px; font-size: 0.75em;
            font-weight: bold;
        }}
        .tag.del {{ background: #7f1d1d; color: #fca5a5; }}
        .tag.edit {{ background: #1c3a5e; color: #93c5fd; }}
        .tag.timer {{ background: #713f12; color: #fde68a; }}
        .tag.once {{ background: #312e81; color: #a5b4fc; }}
        .msg-text {{
            border-left: 3px solid #4b5563; padding: 8px 12px;
            margin: 8px 0; color: #d1d5db; white-space: pre-wrap;
            border-radius: 0 4px 4px 0; background: #1f2937;
        }}
        .original-text {{
            font-size: 0.85em; color: #9ca3af; margin-top: 6px;
            padding: 6px 10px; background: #111827; border-radius: 4px;
        }}
        .orig-label {{ color: #6b7280; }}
        code {{ font-family: 'Courier New', monospace; color: #f87171; }}
        .media-img {{ max-width: 100%; max-height: 300px; border-radius: 6px; margin: 8px 0; }}
        .media-video {{ max-width: 100%; border-radius: 6px; margin: 8px 0; }}
        .media-audio {{ width: 100%; margin: 8px 0; }}
        .doc-link {{
            display: inline-block; padding: 6px 12px;
            background: #1f2937; border-radius: 6px; color: #60a5fa;
            text-decoration: none; margin: 8px 0;
        }}
        .doc-link:hover {{ background: #374151; }}
        .media-placeholder {{
            padding: 10px; background: #1f2937; border-radius: 6px;
            color: #9ca3af; margin: 8px 0;
        }}
        .footer {{
            text-align: center; color: #6b7280; font-size: 0.85em;
            margin-top: 20px; padding-top: 15px; border-top: 1px solid #30363d;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>💬 {escape(chat_title)}</h1>
        <div class="meta">
            <span>👤 {escape(user_name)}</span>
            <span>📅 {export_time}</span>
            <span>💬 {msg_count} сообщений</span>
        </div>
    </div>

    <div class="messages">
        {rows_html}
    </div>

    <div class="footer">
        Экспортировано Business Message Monitor Bot v7.0.0 • {export_time}
    </div>
</body>
</html>"""
    return html_content


# ═══════════════════════════════════════════════════════════════
# КЛАВИАТУРЫ
# ═══════════════════════════════════════════════════════════════


def kb_start() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="📄 Условия использования", callback_data="show_terms")
    b.button(text="✅ Принять и продолжить", callback_data="accept_terms")
    b.adjust(1)
    return b.as_markup()


def kb_phone() -> ReplyKeyboardMarkup:
    b = ReplyKeyboardBuilder()
    b.button(text="📱 Поделиться номером", request_contact=True)
    b.adjust(1)
    return b.as_markup(resize_keyboard=True, one_time_keyboard=True)


def kb_main(user_id: int, vip: bool = False) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="📊 Моя статистика", callback_data="my_stats")
    b.button(text="💎 Подписка", callback_data="subscription_menu")
    b.button(text="⭐ Баланс Stars", callback_data="my_stars")
    b.button(text="👥 Рефералы", callback_data="referrals")
    b.button(text="📤 История транзакций", callback_data="tx_history_0")
    b.button(text="💸 Вывод Stars", callback_data="withdraw_menu")
    if user_id == ADMIN_ID:
        b.button(text="🛡 Админ-панель", callback_data="admin_panel")
    b.adjust(2)
    return b.as_markup()


def kb_back_main() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="◀️ Главное меню", callback_data="main_menu")
    return b.as_markup()


def kb_subscription_plans(user: Dict) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    stars_bal = user["stars_balance"]
    for key, plan in SUBSCRIPTION_PRICES.items():
        affordable = "✅ " if stars_bal >= plan["stars"] else ""
        b.button(
            text=f"{plan['emoji']} {plan['name']}: {plan['stars']}⭐ (~{plan['rub_display']}₽) {affordable}",
            callback_data=f"sub_detail_{key}",
        )
    b.button(text="📊 Курс Stars", callback_data="stars_rate")
    b.button(text="◀️ Назад", callback_data="main_menu")
    b.adjust(1)
    return b.as_markup()


def kb_sub_payment(plan_key: str, user_stars: int) -> InlineKeyboardMarkup:
    plan = SUBSCRIPTION_PRICES[plan_key]
    b = InlineKeyboardBuilder()
    b.button(text=f"⭐ Оплатить {plan['stars']} Stars", callback_data=f"pay_stars_{plan_key}")
    if user_stars >= plan["stars"]:
        b.button(
            text=f"💰 Списать со счёта ({user_stars}⭐)",
            callback_data=f"pay_balance_{plan_key}",
        )
    b.button(text=f"👤 Написать @{ADMIN_USERNAME}", url=f"https://t.me/{ADMIN_USERNAME}")
    b.button(text="◀️ Назад", callback_data="subscription_menu")
    b.adjust(1)
    return b.as_markup()


def kb_admin_main() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="👥 Пользователи", callback_data="admin_users_0")
    b.button(text="📊 Статистика", callback_data="admin_stats")
    b.button(text="🔍 Поиск", callback_data="admin_search")
    b.button(text="📢 Рассылка", callback_data="admin_broadcast")
    b.button(text="💸 Запросы вывода", callback_data="admin_withdrawals")
    b.button(text="⭐ Массово Stars", callback_data="admin_bulk_stars")
    b.button(text="◀️ Главное меню", callback_data="main_menu")
    b.adjust(2)
    return b.as_markup()


def kb_users_page(page: int, total_pages: int) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"admin_users_{page-1}"))
    nav.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="noop"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"admin_users_{page+1}"))
    for btn in nav:
        b.add(btn)
    b.row(InlineKeyboardButton(text="🔢 Выбрать по номеру", callback_data="admin_pick_num"))
    b.row(InlineKeyboardButton(text="◀️ Назад", callback_data="admin_panel"))
    return b.as_markup()


def kb_user_management(target_id: int) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    user = db.get_user(target_id)
    b.button(text="💬 Написать", callback_data=f"adm_msg_{target_id}")
    b.button(text="🎁 Дать подписку", callback_data=f"adm_sub_{target_id}")
    b.button(text="⭐ Дать Stars", callback_data=f"adm_stars_{target_id}")
    b.button(text="📋 Чаты пользователя", callback_data=f"adm_chats_{target_id}")
    b.button(text="📜 История действий", callback_data=f"adm_hist_{target_id}")
    if user and user["is_blocked"]:
        b.button(text="✅ Разблокировать", callback_data=f"adm_unblock_{target_id}")
    else:
        b.button(text="🚫 Заблокировать", callback_data=f"adm_block_{target_id}")
    b.button(text="◀️ Список", callback_data="admin_users_0")
    b.adjust(2)
    return b.as_markup()


def kb_gift_sub(target_id: int) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for key, plan in SUBSCRIPTION_PRICES.items():
        b.button(
            text=f"{plan['emoji']} {plan['name']}",
            callback_data=f"adm_giveplan_{target_id}_{key}",
        )
    b.button(text="◀️ Назад", callback_data=f"adm_view_{target_id}")
    b.adjust(2)
    return b.as_markup()


def kb_withdrawal_admin(req_id: int) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="✅ Одобрить", callback_data=f"adm_with_approve_{req_id}")
    b.button(text="❌ Отклонить", callback_data=f"adm_with_reject_{req_id}")
    b.adjust(2)
    return b.as_markup()


# ═══════════════════════════════════════════════════════════════
# ОБРАБОТЧИКИ КОМАНД И РЕГИСТРАЦИИ
# ═══════════════════════════════════════════════════════════════

router = Router()


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    """Старт бота с поддержкой реферальных ссылок."""
    await state.clear()
    user_id = message.from_user.id
    args = message.text.split()
    referred_by = None

    # Реферальный код из ссылки
    if len(args) > 1:
        ref_code = args[1]
        if ref_code.startswith("REF"):
            ref_user = db.get_user_by_referral_code(ref_code)
            if ref_user and ref_user["user_id"] != user_id:
                referred_by = ref_user["user_id"]

    is_new = db.add_user(
        user_id,
        message.from_user.username,
        message.from_user.first_name,
        message.from_user.last_name,
        referred_by=referred_by,
    )

    user = db.get_user(user_id)
    if not user:
        await message.answer("❌ Ошибка. Попробуйте снова /start")
        return

    if user["is_blocked"]:
        await message.answer(
            f"🚫 Ваш аккаунт заблокирован.\n\nСвяжитесь с @{ADMIN_USERNAME}"
        )
        return

    db.update_user_activity(user_id)

    # Если не принял условия — показываем их
    if not user["accepted_terms"]:
        await message.answer(
            "👋 <b>Business Message Monitor Bot v7.0</b>\n\n"
            "🔍 Мониторинг бизнес-сообщений, перехват удалённых медиа,\n"
            "реферальная система и многое другое.\n\n"
            "📋 Перед началом ознакомьтесь с условиями использования\n"
            "и нажмите <b>«Принять и продолжить»</b>",
            reply_markup=kb_start(),
        )
        return

    # Если не верифицирован телефон
    if not user["phone_verified"]:
        await message.answer(
            "📱 <b>Верификация аккаунта</b>\n\n"
            "Для защиты от мультиаккаунтов и обеспечения безопасности\n"
            "необходимо подтвердить ваш номер телефона.\n\n"
            "Нажмите кнопку ниже 👇",
            reply_markup=kb_phone(),
        )
        await state.set_state(RegistrationStates.waiting_phone)
        return

    # Полностью зарегистрирован
    stars = db.get_stars_balance(user_id)
    vip = "👑 VIP • " if user.get("vip_status") else ""
    ref_code = user.get("referral_code", "")

    await message.answer(
        f"🏠 <b>Главное меню</b>\n\n"
        f"{vip}{format_subscription_info(user)}\n"
        f"⭐ Баланс: <b>{stars}</b> Stars\n\n"
        f"🔗 Ваша реф. ссылка:\n"
        f"<code>https://t.me/mrztnbot?start={ref_code}</code>",
        reply_markup=kb_main(user_id, vip=bool(user.get("vip_status"))),
    )


@router.callback_query(F.data == "show_terms")
async def show_terms(callback: CallbackQuery):
    text = (
        "📄 <b>УСЛОВИЯ ИСПОЛЬЗОВАНИЯ v7.0</b>\n\n"
        "<b>1. Бот работает ТОЛЬКО с Telegram Business API</b>\n"
        "Сохраняет сообщения только из подключённых бизнес-чатов.\n\n"
        "<b>2. Ответственность пользователя</b>\n"
        "Вы самостоятельно несёте ответственность за законность\n"
        "использования полученных данных.\n\n"
        "<b>3. Подписка и оплата</b>\n"
        "3 дня пробного периода бесплатно. Далее — платная подписка.\n"
        "Платежи через Telegram Stars (XTR).\n\n"
        "<b>4. Данные</b>\n"
        "Хранятся на защищённых серверах. Можно запросить удаление.\n\n"
        "<b>5. Звёзды и вывод</b>\n"
        "10 Stars за регистрацию + % от платежей рефералов.\n"
        "Вывод — через запрос администратору.\n\n"
        "<b>НАЖИМАЯ «ПРИНЯТЬ», ВЫ СОГЛАШАЕТЕСЬ СО ВСЕМИ УСЛОВИЯМИ.</b>"
    )
    await callback.message.edit_text(text, reply_markup=kb_start())


@router.callback_query(F.data == "accept_terms")
async def accept_terms(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    db.accept_terms(user_id)

    await callback.message.edit_text(
        "✅ Условия приняты!\n\n"
        "📱 <b>Шаг 2: Верификация телефона</b>\n\n"
        "Для защиты от мультиаккаунтов подтвердите\n"
        "ваш номер телефона. Нажмите кнопку ниже 👇",
    )
    await callback.message.answer(
        "👇 Нажмите кнопку для отправки номера:",
        reply_markup=kb_phone(),
    )
    await state.set_state(RegistrationStates.waiting_phone)


@router.message(RegistrationStates.waiting_phone, F.contact)
async def handle_phone(message: Message, state: FSMContext):
    user_id = message.from_user.id
    contact = message.contact

    # Проверяем что это номер именно этого пользователя
    if contact.user_id and contact.user_id != user_id:
        await message.answer(
            "❌ Пожалуйста, поделитесь СВОИМ номером телефона.",
            reply_markup=kb_phone(),
        )
        return

    phone = contact.phone_number

    # Проверка на дублирование номера
    existing = db.get_user_by_phone(phone)
    if existing and existing["user_id"] != user_id:
        await message.answer(
            "🚫 <b>Этот номер уже зарегистрирован в системе.</b>\n\n"
            f"Если считаете это ошибкой, свяжитесь с @{ADMIN_USERNAME}",
            reply_markup=ReplyKeyboardRemove(),
        )
        db.block_user(user_id, f"Дубликат номера {phone}")
        await state.clear()
        return

    # Сохраняем номер
    ok = db.update_user_phone(user_id, phone)
    if not ok:
        await message.answer(
            "🚫 Этот номер уже используется другим аккаунтом.",
            reply_markup=ReplyKeyboardRemove(),
        )
        await state.clear()
        return

    # Начисляем бонусные Stars
    db.add_stars(user_id, REGISTRATION_STARS, "Бонус при регистрации")

    # Пробный период
    trial_ok = db.activate_trial(user_id)

    user = db.get_user(user_id)
    ref_code = user.get("referral_code", "")

    # Уведомляем реферера
    if user.get("referred_by"):
        try:
            ref_info = db.get_user(user.get("referred_by"))
            if ref_info:
                await message.bot.send_message(
                    user.get("referred_by"),
                    f"🎉 По вашей реферальной ссылке зарегистрировался новый пользователь!\n"
                    f"Вы получите {REFERRAL_PERCENT}% от каждой его оплаты.",
                )
        except Exception:
            pass

    # Уведомляем администратора
    try:
        await message.bot.send_message(
            ADMIN_ID,
            f"🆕 Новый пользователь!\n"
            f"ID: {user_id}\n"
            f"Имя: {message.from_user.first_name}\n"
            f"Username: @{message.from_user.username or '?'}\n"
            f"Телефон: {phone}\n"
            f"Реферер: {user.get('referred_by') or 'нет'}",
        )
    except Exception:
        pass

    trial_text = (
        f"\n\n🎁 Пробный период <b>{TRIAL_DAYS} дня</b> активирован!"
        if trial_ok
        else ""
    )

    await message.answer(
        f"✅ <b>Верификация пройдена!</b>\n\n"
        f"⭐ Вам начислено <b>{REGISTRATION_STARS} Stars</b> за регистрацию{trial_text}\n\n"
        f"🔗 Ваша реф. ссылка:\n"
        f"<code>https://t.me/mrztnbot?start={ref_code}</code>\n\n"
        f"Теперь подключите бота через Telegram Business:\n"
        f"<b>Настройки → Business → Чат-боты → @mrztnbot</b>",
        reply_markup=ReplyKeyboardRemove(),
    )

    await state.clear()

    # Показываем главное меню
    await message.answer(
        "🏠 <b>Главное меню</b>",
        reply_markup=kb_main(user_id),
    )


@router.message(RegistrationStates.waiting_phone)
async def wrong_phone_input(message: Message):
    await message.answer(
        "❌ Пожалуйста, используйте кнопку «Поделиться номером»:",
        reply_markup=kb_phone(),
    )


# ═══════════════════════════════════════════════════════════════
# ГЛАВНОЕ МЕНЮ И НАВИГАЦИЯ
# ═══════════════════════════════════════════════════════════════


@router.callback_query(F.data == "main_menu")
async def main_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = callback.from_user.id
    user = db.get_user(user_id)
    if not user:
        await callback.answer("Ошибка. Отправьте /start")
        return
    stars = db.get_stars_balance(user_id)
    vip_tag = "👑 VIP • " if user.get("vip_status") else ""
    ref_code = user.get("referral_code", "")
    await callback.message.edit_text(
        f"🏠 <b>Главное меню</b>\n\n"
        f"{vip_tag}{format_subscription_info(user)}\n"
        f"⭐ Баланс: <b>{stars}</b> Stars\n\n"
        f"🔗 Реф. ссылка:\n"
        f"<code>https://t.me/mrztnbot?start={ref_code}</code>",
        reply_markup=kb_main(user_id, vip=bool(user.get("vip_status"))),
    )


@router.callback_query(F.data == "my_stats")
async def my_stats(callback: CallbackQuery):
    user_id = callback.from_user.id
    user = db.get_user(user_id)
    if not user:
        await callback.answer()
        return
    stars = db.get_stars_balance(user_id)
    chats = db.get_user_chats(user_id)
    ref_stats = db.get_referral_stats(user_id)

    text = (
        f"📊 <b>Ваша статистика</b>\n\n"
        f"<b>Аккаунт:</b>\n"
        f"├ {format_subscription_info(user)}\n"
        f"{'├ 👑 VIP статус\n' if user.get('vip_status') else ''}"
        f"└ 📅 Зарегистрирован: {str(user['registered_at'])[:10]}\n\n"
        f"<b>Stars:</b>\n"
        f"├ ⭐ Баланс: {stars}\n"
        f"├ 📈 Всего заработано: {user.get('stars_earned_total', 0)}\n"
        f"├ 💸 Потрачено: {user.get('total_spent_stars', 0)}\n"
        f"└ 🏦 Выведено: {user.get('stars_withdrawn', 0)}\n\n"
        f"<b>Сообщения:</b>\n"
        f"├ 💬 Сохранено: {user.get('total_messages_saved', 0)}\n"
        f"├ 🗑 Удалений: {user.get('total_deletions_tracked', 0)}\n"
        f"├ ✏️ Изменений: {user.get('total_edits_tracked', 0)}\n"
        f"└ 📸 Медиа: {user.get('total_media_saved', 0)}\n\n"
        f"<b>Медиа по типам:</b>\n"
        f"📸 Фото: {user.get('total_photo',0)} | "
        f"🎥 Видео: {user.get('total_video',0)} | "
        f"⭕ Кружки: {user.get('total_video_note',0)}\n"
        f"🎤 Гс: {user.get('total_voice',0)} | "
        f"🎵 Аудио: {user.get('total_audio',0)} | "
        f"📄 Доки: {user.get('total_document',0)}\n\n"
        f"<b>Рефералы:</b>\n"
        f"├ 👥 Приглашено: {ref_stats['total']}\n"
        f"└ ⭐ Заработано: {ref_stats['earned_stars']}\n\n"
        f"<b>Чаты:</b> {len(chats)} подключённых"
    )
    b = InlineKeyboardBuilder()
    b.button(text="💬 Мои чаты", callback_data="my_chats")
    b.button(text="◀️ Назад", callback_data="main_menu")
    b.adjust(1)
    await callback.message.edit_text(text, reply_markup=b.as_markup())


@router.callback_query(F.data == "my_chats")
async def my_chats(callback: CallbackQuery):
    user_id = callback.from_user.id
    chats = db.get_user_chats(user_id)
    if not chats:
        await callback.answer("Нет сохранённых чатов", show_alert=True)
        return
    text = "💬 <b>Ваши бизнес-чаты:</b>\n\n"
    for c in chats[:20]:
        title = c.get("chat_title") or f"Чат {c['chat_id']}"
        text += (
            f"📁 <b>{html_module.escape(str(title))}</b>\n"
            f"  💬 Сообщений: {c['total_messages']}\n"
            f"  🗑 Удалений: {c['deleted_count']}\n"
            f"  📸 Медиа: {c['media_count']}\n"
            f"  🕐 Последнее: {str(c.get('last_message_at',''))[:16]}\n\n"
        )
    await callback.message.edit_text(text, reply_markup=kb_back_main())


# ═══════════════════════════════════════════════════════════════
# STARS И ВЫВОД
# ═══════════════════════════════════════════════════════════════


@router.callback_query(F.data == "my_stars")
async def my_stars(callback: CallbackQuery):
    user_id = callback.from_user.id
    stars = db.get_stars_balance(user_id)
    user = db.get_user(user_id)
    rub_equiv = stars_to_rub(stars)
    b = InlineKeyboardBuilder()
    b.button(text="💸 Вывести Stars", callback_data="withdraw_menu")
    b.button(text="📤 История", callback_data="tx_history_0")
    b.button(text="◀️ Назад", callback_data="main_menu")
    b.adjust(1)
    await callback.message.edit_text(
        f"⭐ <b>Баланс Stars</b>\n\n"
        f"💰 Текущий баланс: <b>{stars} Stars</b>\n"
        f"💵 В рублях: ~{rub_equiv}₽\n\n"
        f"📈 Всего заработано: {user.get('stars_earned_total',0)} Stars\n"
        f"💸 Потрачено: {user.get('total_spent_stars',0)} Stars\n"
        f"🏦 Выведено: {user.get('stars_withdrawn',0)} Stars\n\n"
        f"<b>Как получить Stars:</b>\n"
        f"• 🎁 10 Stars за регистрацию\n"
        f"• 👥 {REFERRAL_PERCENT}% от платежей рефералов\n"
        f"• 🎉 Бонусы от администратора\n\n"
        f"<b>Курс: 1⭐ = {STAR_TO_RUB}₽</b>",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data.startswith("tx_history_"))
async def tx_history(callback: CallbackQuery):
    user_id = callback.from_user.id
    page = int(callback.data.split("_")[-1])
    limit = 10
    offset = page * limit
    txs = db.get_stars_history(user_id, limit, offset)
    if not txs and page == 0:
        await callback.answer("История транзакций пуста", show_alert=True)
        return

    text = f"📤 <b>История транзакций</b> (стр. {page+1})\n\n"
    for tx in txs:
        sign = "+" if tx["amount"] > 0 else ""
        emoji = "📈" if tx["amount"] > 0 else "📉"
        text += (
            f"{emoji} <b>{sign}{tx['amount']} Stars</b>\n"
            f"   {html_module.escape(str(tx.get('description',''))[:50])}\n"
            f"   🕐 {str(tx.get('created_at',''))[:16]}\n\n"
        )

    b = InlineKeyboardBuilder()
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"tx_history_{page-1}"))
    if len(txs) == limit:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"tx_history_{page+1}"))
    for btn in nav:
        b.add(btn)
    b.row(InlineKeyboardButton(text="◀️ Назад", callback_data="my_stars"))
    await callback.message.edit_text(text, reply_markup=b.as_markup())


@router.callback_query(F.data == "withdraw_menu")
async def withdraw_menu(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    stars = db.get_stars_balance(user_id)
    if stars < 10:
        await callback.answer(
            "Минимум для вывода: 10 Stars. У вас недостаточно.",
            show_alert=True,
        )
        return
    await callback.message.edit_text(
        f"💸 <b>Вывод Stars</b>\n\n"
        f"⭐ Доступно: <b>{stars} Stars</b>\n"
        f"💵 Эквивалент: ~{stars_to_rub(stars)}₽\n\n"
        f"Введите количество Stars для вывода (минимум 10):\n\n"
        f"Обработка — вручную администратором.\n"
        f"Срок: 24-48 часов.",
        reply_markup=kb_back_main(),
    )
    await state.set_state(WithdrawStates.enter_amount)


@router.message(WithdrawStates.enter_amount)
async def withdraw_enter_amount(message: Message, state: FSMContext):
    user_id = message.from_user.id
    try:
        amount = int(message.text.strip())
        if amount < 10:
            await message.answer("❌ Минимум 10 Stars.")
            return
        stars = db.get_stars_balance(user_id)
        if amount > stars:
            await message.answer(f"❌ У вас только {stars} Stars.")
            return
        await state.update_data(amount=amount)
        b = InlineKeyboardBuilder()
        b.button(text="✅ Подтвердить вывод", callback_data="withdraw_confirm")
        b.button(text="❌ Отмена", callback_data="main_menu")
        b.adjust(1)
        await message.answer(
            f"⭐ Вывести: <b>{amount} Stars</b>\n"
            f"💵 Эквивалент: ~{stars_to_rub(amount)}₽\n\n"
            f"Подтвердите запрос:",
            reply_markup=b.as_markup(),
        )
        await state.set_state(WithdrawStates.confirm)
    except ValueError:
        await message.answer("❌ Введите число.")


@router.callback_query(WithdrawStates.confirm, F.data == "withdraw_confirm")
async def withdraw_confirm(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    data = await state.get_data()
    amount = data.get("amount", 0)
    if amount < 10:
        await callback.answer("Ошибка суммы", show_alert=True)
        await state.clear()
        return

    req_id = db.create_withdrawal_request(user_id, amount)
    await state.clear()

    user = db.get_user(user_id)
    uname = f"@{user['username']}" if user and user.get("username") else f"ID:{user_id}"

    # Уведомляем администратора
    try:
        await callback.bot.send_message(
            ADMIN_ID,
            f"💸 <b>Запрос на вывод Stars #{req_id}</b>\n\n"
            f"👤 Пользователь: {uname} ({user_id})\n"
            f"⭐ Сумма: {amount} Stars (~{stars_to_rub(amount)}₽)\n\n"
            f"Обработайте запрос в /admin → Запросы вывода",
            reply_markup=kb_withdrawal_admin(req_id),
        )
    except Exception:
        pass

    await callback.message.edit_text(
        f"✅ <b>Запрос на вывод создан! (#{req_id})</b>\n\n"
        f"⭐ Сумма: {amount} Stars\n"
        f"Stars зарезервированы до обработки.\n\n"
        f"Срок обработки: 24-48 часов.",
        reply_markup=kb_back_main(),
    )


# ═══════════════════════════════════════════════════════════════
# РЕФЕРАЛЫ
# ═══════════════════════════════════════════════════════════════


@router.callback_query(F.data == "referrals")
async def referrals_menu(callback: CallbackQuery):
    user_id = callback.from_user.id
    user = db.get_user(user_id)
    stats = db.get_referral_stats(user_id)
    ref_code = user.get("referral_code", "")
    ref_link = f"https://t.me/mrztnbot?start={ref_code}"

    text = (
        f"👥 <b>Реферальная программа</b>\n\n"
        f"🔗 Ваша ссылка:\n"
        f"<code>{ref_link}</code>\n\n"
        f"📊 Статистика:\n"
        f"├ Приглашено: <b>{stats['total']}</b> чел.\n"
        f"└ Заработано: <b>{stats['earned_stars']}</b> Stars\n\n"
        f"<b>Условия:</b>\n"
        f"• {REFERRAL_PERCENT}% от каждой оплаты реферала → вам\n"
        f"• Начисляется мгновенно\n"
        f"• Можно вывести или потратить на подписку\n\n"
        f"<b>Ваши рефералы:</b>"
    )
    if stats["referrals"]:
        for r in stats["referrals"][:10]:
            name = r.get("first_name") or r.get("username") or str(r["user_id"])
            sub = r.get("subscription_type", "free")
            text += f"\n• {html_module.escape(name[:20])} — {sub}"
    else:
        text += "\nПока никого нет. Поделитесь ссылкой!"

    await callback.message.edit_text(text, reply_markup=kb_back_main())


# ═══════════════════════════════════════════════════════════════
# ПОДПИСКИ И ПЛАТЕЖИ
# ═══════════════════════════════════════════════════════════════


@router.callback_query(F.data == "stars_rate")
async def stars_rate(callback: CallbackQuery):
    await callback.answer(
        f"📊 Курс Telegram Stars:\n"
        f"1 ⭐ = {STAR_TO_RUB} ₽\n"
        f"100 ⭐ = {stars_to_rub(100)} ₽",
        show_alert=True,
    )


@router.callback_query(F.data == "subscription_menu")
async def subscription_menu(callback: CallbackQuery):
    user_id = callback.from_user.id
    user = db.get_user(user_id)
    if not user:
        await callback.answer()
        return
    stars = db.get_stars_balance(user_id)
    text = (
        f"💎 <b>Подписка</b>\n\n"
        f"Статус: {format_subscription_info(user)}\n"
        f"⭐ Ваш баланс: <b>{stars} Stars</b>\n"
        f"💵 Эквивалент: ~{stars_to_rub(stars)}₽\n\n"
        f"<b>Доступные планы:</b>\n"
        f"(✅ = можно купить с вашего баланса)\n\n"
        f"📊 Курс: 1⭐ = {STAR_TO_RUB}₽"
    )
    await callback.message.edit_text(
        text, reply_markup=kb_subscription_plans(user)
    )


@router.callback_query(F.data.startswith("sub_detail_"))
async def sub_detail(callback: CallbackQuery):
    plan_key = callback.data.split("_", 2)[-1]
    if plan_key not in SUBSCRIPTION_PRICES:
        await callback.answer("Неверный план", show_alert=True)
        return
    plan = SUBSCRIPTION_PRICES[plan_key]
    user_id = callback.from_user.id
    user_stars = db.get_stars_balance(user_id)

    # Умный калькулятор
    can_from_balance = user_stars >= plan["stars"]
    needed = max(0, plan["stars"] - user_stars)

    text = (
        f"{plan['emoji']} <b>Подписка: {plan['name']}</b>\n\n"
        f"💰 Стоимость: <b>{plan['stars']}⭐</b> (~{plan['rub_display']}₽)\n"
        f"📅 Срок: {'Навсегда' if plan['days'] is None else str(plan['days']) + ' дней'}\n\n"
        f"<b>Ваш баланс:</b> {user_stars}⭐\n"
        f"{'✅ Можете купить с баланса' if can_from_balance else f'⚠️ Нужно ещё {needed}⭐ (~{stars_to_rub(needed)}₽)'}\n\n"
        f"<b>💡 Умный калькулятор:</b>\n"
        f"• {plan['stars']}⭐ ≈ {stars_to_rub(plan['stars'])}₽\n"
        f"• Выгода за год vs месяц: "
        + (f"~{round((SUBSCRIPTION_PRICES['month']['stars']*12 - plan['stars'])/SUBSCRIPTION_PRICES['month']['stars']*100)}%\n" if plan_key == 'year' else "—\n")
        + f"\n<b>Способ оплаты:</b>"
    )
    await callback.message.edit_text(
        text, reply_markup=kb_sub_payment(plan_key, user_stars)
    )


@router.callback_query(F.data.startswith("pay_stars_"))
async def pay_with_stars(callback: CallbackQuery):
    plan_key = callback.data.split("_", 2)[-1]
    if plan_key not in SUBSCRIPTION_PRICES:
        await callback.answer("Неверный план", show_alert=True)
        return
    plan = SUBSCRIPTION_PRICES[plan_key]
    try:
        await callback.bot.send_invoice(
            chat_id=callback.from_user.id,
            title=f"Подписка: {plan['name']}",
            description=f"Business Monitor — подписка «{plan['name']}» ({plan['days'] or 'навсегда'} дней)",
            payload=f"sub_{plan_key}_{callback.from_user.id}",
            currency="XTR",
            prices=[LabeledPrice(label=plan["name"], amount=plan["stars"])],
        )
        await callback.answer("Инвойс отправлен 👆")
    except Exception as e:
        await callback.answer(f"Ошибка: {e}", show_alert=True)


@router.callback_query(F.data.startswith("pay_balance_"))
async def pay_from_balance(callback: CallbackQuery):
    plan_key = callback.data.split("_", 2)[-1]
    if plan_key not in SUBSCRIPTION_PRICES:
        await callback.answer("Неверный план", show_alert=True)
        return
    plan = SUBSCRIPTION_PRICES[plan_key]
    user_id = callback.from_user.id
    stars = db.get_stars_balance(user_id)
    if stars < plan["stars"]:
        await callback.answer("Недостаточно Stars на балансе", show_alert=True)
        return

    ok = db.remove_stars(user_id, plan["stars"], f"Покупка подписки: {plan['name']}")
    if not ok:
        await callback.answer("Ошибка списания", show_alert=True)
        return

    db.activate_subscription(user_id, plan_key, plan["days"])

    # Реферальный бонус
    bonus = db.pay_referral_bonus(user_id, plan["stars"])
    if bonus:
        user = db.get_user(user_id)
        if user and user.get("referred_by"):
            try:
                await callback.bot.send_message(
                    user["referred_by"],
                    f"⭐ Реферальный бонус: +{bonus} Stars\n"
                    f"Ваш реферал продлил подписку!",
                )
            except Exception:
                pass

    await callback.message.edit_text(
        f"✅ <b>Подписка «{plan['name']}» активирована!</b>\n\n"
        f"💰 Списано: {plan['stars']} Stars\n"
        f"📅 Срок: {'Навсегда' if plan['days'] is None else str(plan['days']) + ' дней'}\n\n"
        f"Бот начнёт мониторинг ваших бизнес-чатов.",
        reply_markup=kb_back_main(),
    )


@router.pre_checkout_query()
async def pre_checkout(query: PreCheckoutQuery):
    await query.answer(ok=True)


@router.message(F.successful_payment)
async def successful_payment_handler(message: Message):
    payment: SuccessfulPayment = message.successful_payment
    user_id = message.from_user.id
    stars_paid = payment.total_amount
    payload = payment.invoice_payload
    # payload format: sub_{plan_key}_{user_id}
    parts = payload.split("_")
    plan_key = parts[1] if len(parts) >= 2 else "month"

    if plan_key not in SUBSCRIPTION_PRICES:
        plan_key = "month"

    plan = SUBSCRIPTION_PRICES[plan_key]
    db.activate_subscription(user_id, plan_key, plan["days"])
    db.save_payment(
        user_id,
        stars_paid,
        plan_key,
        payment.telegram_payment_charge_id,
        payment.provider_payment_charge_id,
        payload,
    )

    # Реферальный бонус пригласившему
    bonus = db.pay_referral_bonus(user_id, stars_paid)
    if bonus:
        user = db.get_user(user_id)
        if user and user.get("referred_by"):
            try:
                await message.bot.send_message(
                    user["referred_by"],
                    f"⭐ <b>Реферальный бонус: +{bonus} Stars!</b>\n\n"
                    f"Ваш реферал оплатил подписку «{plan['name']}»\n"
                    f"Вы получили {REFERRAL_PERCENT}% = {bonus} Stars",
                )
            except Exception:
                pass

    # Уведомляем администратора
    try:
        u = db.get_user(user_id)
        uname = f"@{u['username']}" if u and u.get("username") else f"ID:{user_id}"
        await message.bot.send_message(
            ADMIN_ID,
            f"💰 <b>Новый платёж!</b>\n\n"
            f"👤 {uname}\n"
            f"💎 План: {plan['name']}\n"
            f"⭐ Оплачено: {stars_paid} Stars (~{stars_to_rub(stars_paid)}₽)",
        )
    except Exception:
        pass

    await message.answer(
        f"✅ <b>Оплата прошла успешно!</b>\n\n"
        f"💎 Подписка «{plan['name']}» активирована!\n"
        f"⭐ Оплачено: {stars_paid} Stars",
        reply_markup=kb_back_main(),
    )


# ═══════════════════════════════════════════════════════════════
# ОБРАБОТКА ПОДАРКОВ TELEGRAM (Gifts → автозачисление Stars)
# ═══════════════════════════════════════════════════════════════


@router.message(F.gift)
async def handle_gift(message: Message):
    """Автозачисление Stars при получении Telegram подарка."""
    try:
        user_id = message.from_user.id
        gift = message.gift
        stars_value = 0

        if hasattr(gift, "star_count"):
            stars_value = gift.star_count
        elif hasattr(gift, "gift") and hasattr(gift.gift, "star_count"):
            stars_value = gift.gift.star_count

        if stars_value > 0:
            db.add_stars(user_id, stars_value, f"Получен Telegram подарок ({stars_value} Stars)")
            await message.answer(
                f"🎁 <b>Подарок получен!</b>\n\n"
                f"⭐ Вам зачислено <b>{stars_value} Stars</b>\n"
                f"💰 Новый баланс: {db.get_stars_balance(user_id)} Stars",
                reply_markup=kb_back_main(),
            )
        else:
            await message.answer(
                "🎁 Спасибо за подарок!\n"
                "Свяжитесь с администратором для зачисления Stars.",
            )
    except Exception as e:
        logger.error(f"handle_gift error: {e}")


# ═══════════════════════════════════════════════════════════════
# ADMIN ПАНЕЛЬ
# ═══════════════════════════════════════════════════════════════


def require_admin(func):
    """Декоратор: проверяет что caller = ADMIN_ID."""
    import functools

    @functools.wraps(func)
    async def wrapper(callback: CallbackQuery, *args, **kwargs):
        if callback.from_user.id != ADMIN_ID:
            await callback.answer("⛔ Недостаточно прав", show_alert=True)
            return
        return await func(callback, *args, **kwargs)

    return wrapper


@router.callback_query(F.data == "admin_panel")
@require_admin
async def admin_panel(callback: CallbackQuery):
    total = db.get_user_count()
    active = db.get_active_subscriptions_count()
    today = db.get_today_new_users()
    vip = db.get_vip_users_count()
    msgs = db.get_total_messages()
    dels = db.get_total_deletions()
    pending = db.get_pending_withdrawals()

    await callback.message.edit_text(
        f"🛡 <b>Админ-панель</b>\n\n"
        f"👥 Пользователей: {total}\n"
        f"💎 Активных подписок: {active}\n"
        f"🆕 Сегодня: +{today}\n"
        f"👑 VIP: {vip}\n\n"
        f"💬 Сообщений: {msgs}\n"
        f"🗑 Удалений: {dels}\n\n"
        f"⏳ Ожидают вывода: {len(pending)}",
        reply_markup=kb_admin_main(),
    )


@router.callback_query(F.data == "admin_stats")
@require_admin
async def admin_stats(callback: CallbackQuery):
    total = db.get_user_count()
    active = db.get_active_subscriptions_count()
    today = db.get_today_new_users()
    msgs = db.get_total_messages()
    dels = db.get_total_deletions()
    vip = db.get_vip_users_count()
    expiring = db.get_expiring_subscriptions(3)

    text = (
        f"📊 <b>Полная статистика</b>\n\n"
        f"<b>Пользователи:</b>\n"
        f"├ Всего: {total}\n"
        f"├ С подпиской: {active}\n"
        f"├ VIP: {vip}\n"
        f"└ Сегодня новых: {today}\n\n"
        f"<b>Данные:</b>\n"
        f"├ Сообщений: {msgs}\n"
        f"└ Удалений: {dels}\n\n"
        f"<b>Истекает в 3 дня:</b> {len(expiring)} подписок\n"
    )
    b = InlineKeyboardBuilder()
    b.button(text="◀️ Назад", callback_data="admin_panel")
    await callback.message.edit_text(text, reply_markup=b.as_markup())


@router.callback_query(F.data.startswith("admin_users_"))
@require_admin
async def admin_users(callback: CallbackQuery):
    page = int(callback.data.split("_")[-1])
    total = db.get_user_count()
    total_pages = max(1, (total + USERS_PER_PAGE - 1) // USERS_PER_PAGE)
    page = max(0, min(page, total_pages - 1))
    users = db.get_all_users(USERS_PER_PAGE, page * USERS_PER_PAGE)

    text = f"👥 <b>Пользователи ({page+1}/{total_pages})</b>\n\n"
    for i, u in enumerate(users, page * USERS_PER_PAGE + 1):
        text += format_user_line(u, i) + "\n"

    try:
        await callback.message.edit_text(
            text, reply_markup=kb_users_page(page, total_pages)
        )
    except Exception:
        await callback.answer()


@router.callback_query(F.data == "admin_pick_num")
@require_admin
async def admin_pick_num(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "🔢 Отправьте <b>номер</b> пользователя из списка:"
    )
    await state.set_state(AdminStates.user_number_input)


@router.message(AdminStates.user_number_input)
async def admin_process_num(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        num = int(message.text.strip())
        users = db.get_all_users(1, num - 1)
        if not users:
            await message.answer("❌ Пользователь не найден.")
            await state.clear()
            return
        user = users[0]
        await state.clear()
        await show_user_card(message.bot, message.chat.id, user)
    except ValueError:
        await message.answer("❌ Введите целое число.")


async def show_user_card(bot, chat_id: int, user: Dict):
    """Показывает карточку пользователя."""
    stars = db.get_stars_balance(user["user_id"])
    ref_stats = db.get_referral_stats(user["user_id"])
    conns = db.get_user_connections(user["user_id"])

    text = (
        f"👤 <b>Пользователь</b>\n\n"
        f"ID: <code>{user['user_id']}</code>\n"
        f"Имя: {html_module.escape(str(user.get('first_name','?')))}\n"
        f"Username: @{user.get('username') or '?'}\n"
        f"Телефон: {user.get('phone') or '?'}\n"
        f"{'👑 VIP\n' if user.get('vip_status') else ''}"
        f"🔒 Статус: {'🚫 Заблокирован' if user['is_blocked'] else '✅ Активен'}\n\n"
        f"<b>Подписка:</b> {format_subscription_info(user)}\n\n"
        f"<b>Stars:</b>\n"
        f"├ Баланс: {stars} ⭐\n"
        f"├ Потрачено: {user.get('total_spent_stars',0)} ⭐\n"
        f"└ Рефералов: {ref_stats['total']}\n\n"
        f"<b>Статистика:</b>\n"
        f"├ Сообщений: {user.get('total_messages_saved',0)}\n"
        f"├ Удалений: {user.get('total_deletions_tracked',0)}\n"
        f"└ Медиа: {user.get('total_media_saved',0)}\n\n"
        f"<b>Подключений:</b> {len(conns)}\n"
        f"Регистрация: {str(user.get('registered_at',''))[:10]}"
    )
    try:
        await bot.send_message(
            chat_id, text, reply_markup=kb_user_management(user["user_id"])
        )
    except Exception as e:
        logger.error(f"show_user_card: {e}")


@router.callback_query(F.data == "admin_search")
@require_admin
async def admin_search(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("🔍 Введите запрос (ID, username, имя, телефон):")
    await state.set_state(AdminStates.search_user)


@router.message(AdminStates.search_user)
async def admin_search_result(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    await state.clear()
    query = message.text.strip()
    if query.startswith("@"):
        query = query[1:]
    results = db.search_users(query)
    if not results:
        await message.answer("❌ Ничего не найдено.")
        return
    text = f"🔍 <b>Результаты ({len(results)})</b>\n\n"
    for i, u in enumerate(results[:10], 1):
        text += format_user_line(u, i) + "\n"
    b = InlineKeyboardBuilder()
    b.button(text="◀️ Назад", callback_data="admin_panel")
    await message.answer(text, reply_markup=b.as_markup())


@router.callback_query(F.data.startswith("adm_view_"))
@require_admin
async def adm_view_user(callback: CallbackQuery):
    target_id = int(callback.data.split("_")[-1])
    user = db.get_user(target_id)
    if not user:
        await callback.answer("Пользователь не найден", show_alert=True)
        return
    await show_user_card(callback.bot, callback.message.chat.id, user)


@router.callback_query(F.data.startswith("adm_msg_"))
@require_admin
async def adm_msg_start(callback: CallbackQuery, state: FSMContext):
    target_id = int(callback.data.split("_")[-1])
    await state.update_data(target=target_id)
    await callback.message.edit_text(
        f"💬 Введите сообщение пользователю {target_id}:\n"
        f"(Отправьте /cancel для отмены)"
    )
    await state.set_state(AdminStates.send_message)


@router.message(AdminStates.send_message)
async def adm_msg_send(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    if message.text and message.text.strip() == "/cancel":
        await state.clear()
        await message.answer("Отменено.")
        return
    data = await state.get_data()
    target = data.get("target")
    try:
        await message.bot.send_message(
            target,
            f"📨 <b>Сообщение от администратора:</b>\n\n{message.text or '(медиа)'}",
        )
        db.log_admin_action(ADMIN_ID, target, "message", (message.text or "")[:100])
        await message.answer(f"✅ Сообщение отправлено пользователю {target}")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")
    await state.clear()


@router.callback_query(F.data.startswith("adm_sub_"))
@require_admin
async def adm_sub(callback: CallbackQuery):
    target_id = int(callback.data.split("_")[-1])
    await callback.message.edit_text(
        f"🎁 Выберите подписку для пользователя {target_id}:",
        reply_markup=kb_gift_sub(target_id),
    )


@router.callback_query(F.data.startswith("adm_giveplan_"))
@require_admin
async def adm_give_plan(callback: CallbackQuery):
    parts = callback.data.split("_")
    target_id = int(parts[-2])
    plan_key = parts[-1]
    if plan_key not in SUBSCRIPTION_PRICES:
        await callback.answer("Неверный план", show_alert=True)
        return
    plan = SUBSCRIPTION_PRICES[plan_key]
    db.activate_subscription(target_id, plan_key, plan["days"])
    db.log_admin_action(
        ADMIN_ID, target_id, "gift_subscription",
        f"Plan={plan_key}"
    )
    # Уведомляем пользователя
    try:
        await callback.bot.send_message(
            target_id,
            f"🎁 <b>Вам подарена подписка!</b>\n\n"
            f"{plan['emoji']} <b>{plan['name']}</b>\n"
            f"Доступ к мониторингу бизнес-сообщений активирован.",
            reply_markup=kb_back_main(),
        )
    except Exception:
        pass
    await callback.answer(f"✅ Подписка «{plan['name']}» выдана", show_alert=True)
    await callback.message.edit_text(
        f"✅ Выдана подписка «{plan['name']}» пользователю {target_id}",
        reply_markup=kb_admin_main(),
    )


@router.callback_query(F.data.startswith("adm_stars_"))
@require_admin
async def adm_stars_start(callback: CallbackQuery, state: FSMContext):
    target_id = int(callback.data.split("_")[-1])
    await state.update_data(target=target_id)
    await callback.message.edit_text(
        f"⭐ Введите количество Stars для пользователя {target_id}:"
    )
    await state.set_state(AdminStates.send_stars)


@router.message(AdminStates.send_stars)
async def adm_stars_send(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        amount = int(message.text.strip())
        data = await state.get_data()
        target = data.get("target")
        db.add_stars(target, amount, "От администратора", related_user=ADMIN_ID)
        db.log_admin_action(ADMIN_ID, target, "give_stars", f"Amount={amount}")
        try:
            await message.bot.send_message(
                target,
                f"⭐ <b>Вам начислено {amount} Stars</b> от администратора!\n\n"
                f"💰 Баланс: {db.get_stars_balance(target)} Stars",
                reply_markup=kb_back_main(),
            )
        except Exception:
            pass
        await message.answer(f"✅ Отправлено {amount} Stars пользователю {target}")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число.")


@router.callback_query(F.data.startswith("adm_chats_"))
@require_admin
async def adm_view_chats(callback: CallbackQuery):
    target_id = int(callback.data.split("_")[-1])
    chats = db.get_user_chats(target_id)
    if not chats:
        await callback.answer("Нет чатов у этого пользователя", show_alert=True)
        return
    text = f"💬 <b>Чаты пользователя {target_id}</b>\n\n"
    for c in chats[:20]:
        title = c.get("chat_title") or f"Чат {c['chat_id']}"
        text += (
            f"📁 <b>{html_module.escape(str(title))}</b>\n"
            f"  ID: <code>{c['chat_id']}</code>\n"
            f"  💬 {c['total_messages']} / 🗑 {c['deleted_count']} / 📸 {c['media_count']}\n"
            f"  🕐 {str(c.get('last_message_at',''))[:16]}\n\n"
        )
    b = InlineKeyboardBuilder()
    b.button(text="◀️ Назад", callback_data=f"adm_view_{target_id}")
    await callback.message.edit_text(text, reply_markup=b.as_markup())


@router.callback_query(F.data.startswith("adm_hist_"))
@require_admin
async def adm_history(callback: CallbackQuery):
    target_id = int(callback.data.split("_")[-1])
    history = db.get_admin_history(target_id)
    text = f"📜 <b>История действий ({target_id})</b>\n\n"
    if not history:
        text += "Действий не было"
    for h in history:
        text += (
            f"• <b>{h['action_type']}</b>\n"
            f"  {html_module.escape(str(h.get('action_details',''))[:80])}\n"
            f"  🕐 {str(h.get('created_at',''))[:16]}\n\n"
        )
    b = InlineKeyboardBuilder()
    b.button(text="◀️ Назад", callback_data=f"adm_view_{target_id}")
    await callback.message.edit_text(text, reply_markup=b.as_markup())


@router.callback_query(F.data.startswith("adm_block_"))
@require_admin
async def adm_block(callback: CallbackQuery):
    target_id = int(callback.data.split("_")[-1])
    db.block_user(target_id, "Заблокирован администратором")
    try:
        await callback.bot.send_message(
            target_id,
            f"🚫 Ваш аккаунт заблокирован.\n"
            f"Свяжитесь с @{ADMIN_USERNAME} для уточнений.",
        )
    except Exception:
        pass
    await callback.answer("✅ Заблокирован", show_alert=True)
    await callback.message.edit_reply_markup(
        reply_markup=kb_user_management(target_id)
    )


@router.callback_query(F.data.startswith("adm_unblock_"))
@require_admin
async def adm_unblock(callback: CallbackQuery):
    target_id = int(callback.data.split("_")[-1])
    db.unblock_user(target_id)
    try:
        await callback.bot.send_message(
            target_id,
            "✅ <b>Ваш аккаунт разблокирован!</b>\n"
            "Добро пожаловать обратно!",
            reply_markup=kb_back_main(),
        )
    except Exception:
        pass
    await callback.answer("✅ Разблокирован", show_alert=True)
    await callback.message.edit_reply_markup(
        reply_markup=kb_user_management(target_id)
    )


@router.callback_query(F.data == "admin_broadcast")
@require_admin
async def admin_broadcast_start(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "📢 <b>Рассылка</b>\n\n"
        "Введите текст для отправки всем пользователям:\n"
        "(отмена: /cancel)"
    )
    await state.set_state(AdminStates.broadcast_message)


@router.message(AdminStates.broadcast_message)
async def admin_broadcast_send(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    if message.text and message.text.strip() == "/cancel":
        await state.clear()
        await message.answer("Отменено.", reply_markup=kb_admin_main())
        return
    text = message.text or ""
    await state.clear()
    users = db.get_all_users()
    ok = 0
    fail = 0
    for u in users:
        if u["is_blocked"]:
            continue
        try:
            await message.bot.send_message(u["user_id"], f"📢 <b>Сообщение от администратора:</b>\n\n{text}")
            ok += 1
        except Exception:
            fail += 1
        await asyncio.sleep(0.05)
    await message.answer(
        f"📢 <b>Рассылка завершена</b>\n\n✅ Отправлено: {ok}\n❌ Не доставлено: {fail}",
        reply_markup=kb_admin_main(),
    )


@router.callback_query(F.data == "admin_withdrawals")
@require_admin
async def admin_withdrawals(callback: CallbackQuery):
    pending = db.get_pending_withdrawals()
    if not pending:
        await callback.answer("Нет ожидающих запросов", show_alert=True)
        return
    text = f"💸 <b>Запросы на вывод ({len(pending)})</b>\n\n"
    for req in pending:
        uname = f"@{req['username']}" if req.get("username") else req.get("first_name", "?")
        text += (
            f"#{req['id']} — {uname} ({req['user_id']})\n"
            f"⭐ {req['amount_stars']} Stars (~{stars_to_rub(req['amount_stars'])}₽)\n"
            f"🕐 {str(req.get('created_at',''))[:16]}\n\n"
        )
    b = InlineKeyboardBuilder()
    for req in pending[:5]:
        b.button(
            text=f"#{req['id']} — {req['amount_stars']}⭐",
            callback_data=f"adm_with_detail_{req['id']}",
        )
    b.button(text="◀️ Назад", callback_data="admin_panel")
    b.adjust(1)
    await callback.message.edit_text(text, reply_markup=b.as_markup())


@router.callback_query(F.data.startswith("adm_with_detail_"))
@require_admin
async def adm_with_detail(callback: CallbackQuery):
    req_id = int(callback.data.split("_")[-1])
    pending = db.get_pending_withdrawals()
    req = next((r for r in pending if r["id"] == req_id), None)
    if not req:
        await callback.answer("Запрос не найден или уже обработан", show_alert=True)
        return
    uname = f"@{req['username']}" if req.get("username") else str(req["user_id"])
    await callback.message.edit_text(
        f"💸 <b>Запрос на вывод #{req_id}</b>\n\n"
        f"👤 {uname} (ID: {req['user_id']})\n"
        f"⭐ {req['amount_stars']} Stars\n"
        f"💵 ~{stars_to_rub(req['amount_stars'])}₽\n"
        f"🕐 {str(req.get('created_at',''))[:16]}\n\n"
        f"Одобрите после отправки Stars пользователю.",
        reply_markup=kb_withdrawal_admin(req_id),
    )


@router.callback_query(F.data.startswith("adm_with_approve_"))
@require_admin
async def adm_with_approve(callback: CallbackQuery):
    req_id = int(callback.data.split("_")[-1])
    result = db.approve_withdrawal(req_id)
    if not result:
        await callback.answer("Ошибка обработки", show_alert=True)
        return
    try:
        await callback.bot.send_message(
            result["user_id"],
            f"✅ <b>Запрос на вывод #{req_id} одобрен!</b>\n\n"
            f"⭐ {result['amount_stars']} Stars переведены.\n"
            f"Если Stars ещё не пришли — обратитесь к @{ADMIN_USERNAME}",
            reply_markup=kb_back_main(),
        )
    except Exception:
        pass
    db.log_admin_action(ADMIN_ID, result["user_id"], "withdrawal_approved", f"req={req_id}, amount={result['amount_stars']}")
    await callback.answer("✅ Запрос одобрен", show_alert=True)
    await callback.message.edit_text(
        f"✅ Запрос #{req_id} одобрен.",
        reply_markup=kb_admin_main(),
    )


@router.callback_query(F.data.startswith("adm_with_reject_"))
@require_admin
async def adm_with_reject(callback: CallbackQuery):
    req_id = int(callback.data.split("_")[-1])
    result = db.reject_withdrawal(req_id)
    if not result:
        await callback.answer("Ошибка обработки", show_alert=True)
        return
    try:
        await callback.bot.send_message(
            result["user_id"],
            f"❌ <b>Запрос на вывод #{req_id} отклонён.</b>\n\n"
            f"⭐ {result['amount_stars']} Stars возвращены на баланс.\n"
            f"Свяжитесь с @{ADMIN_USERNAME} для уточнений.",
            reply_markup=kb_back_main(),
        )
    except Exception:
        pass
    db.log_admin_action(ADMIN_ID, result["user_id"], "withdrawal_rejected", f"req={req_id}")
    await callback.answer("✅ Запрос отклонён", show_alert=True)
    await callback.message.edit_text(
        f"❌ Запрос #{req_id} отклонён. Stars возвращены.",
        reply_markup=kb_admin_main(),
    )


@router.callback_query(F.data == "admin_bulk_stars")
@require_admin
async def admin_bulk_stars_start(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "⭐ <b>Массовое начисление Stars</b>\n\n"
        "Введите количество Stars для начисления ВСЕМ пользователям:\n"
        "(введите /cancel для отмены)"
    )
    await state.set_state(AdminStates.bulk_stars)


@router.message(AdminStates.bulk_stars)
async def admin_bulk_stars_send(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    if message.text and message.text.strip() == "/cancel":
        await state.clear()
        await message.answer("Отменено.", reply_markup=kb_admin_main())
        return
    try:
        amount = int(message.text.strip())
        await state.clear()
        users = db.get_all_users()
        count = 0
        for u in users:
            if not u["is_blocked"]:
                db.add_stars(u["user_id"], amount, "Массовое начисление от администратора")
                count += 1
        await message.answer(
            f"✅ Начислено {amount} Stars для {count} пользователей.",
            reply_markup=kb_admin_main(),
        )
    except ValueError:
        await message.answer("❌ Введите число.")


# ═══════════════════════════════════════════════════════════════
# ОБРАБОТЧИКИ БИЗНЕС API
# ═══════════════════════════════════════════════════════════════


@router.business_connection()
async def on_business_connection(connection: BusinessConnection, bot: Bot):
    """Обработчик подключения/отключения бизнес-аккаунта."""
    user_id = connection.user.id
    connection_id = connection.id

    if connection.is_enabled:
        # Подключение
        db.add_business_connection(
            connection_id,
            user_id,
            connection.user.id,
            connection.can_reply,
        )
        try:
            await bot.send_message(
                user_id,
                "🎉 <b>Бизнес-аккаунт подключён!</b>\n\n"
                "✅ Мониторинг активирован.\n"
                "Все сообщения из бизнес-чатов будут сохраняться.\n"
                "При удалении — вы получите уведомление с содержимым.",
                reply_markup=kb_back_main(),
            )
        except Exception:
            pass
    else:
        # Отключение — генерируем HTML экспорт
        db.disable_business_connection(connection_id)
        await export_connection_on_disconnect(bot, user_id, connection_id)
        try:
            await bot.send_message(
                user_id,
                "📤 <b>Бизнес-аккаунт отключён.</b>\n\n"
                "Выше — HTML экспорт всей истории чатов.\n"
                f"⚠️ Файлы будут автоматически удалены через {MEDIA_CLEANUP_HOURS} часов.",
                reply_markup=kb_back_main(),
            )
        except Exception:
            pass


async def export_connection_on_disconnect(bot: Bot, user_id: int, connection_id: str):
    """Генерирует и отправляет HTML экспорт при отключении бизнес-аккаунта."""
    try:
        messages = db.get_connection_messages(user_id, connection_id)
        if not messages:
            return

        user = db.get_user(user_id)

        # Группируем по чатам
        chats: Dict[int, List[Dict]] = {}
        for msg in messages:
            cid = msg["chat_id"]
            if cid not in chats:
                chats[cid] = []
            chats[cid].append(msg)

        for chat_id, chat_msgs in chats.items():
            chat_title = chat_msgs[0].get("chat_title") or f"Чат {chat_id}"
            html_content = await generate_chat_html(
                user or {}, chat_msgs, chat_title, connection_id
            )

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            export_filename = f"chat_{chat_id}_{ts}.html"
            user_exports_dir = EXPORTS_DIR / str(user_id)
            user_exports_dir.mkdir(parents=True, exist_ok=True)
            export_path = user_exports_dir / export_filename

            async with aiofiles.open(export_path, "w", encoding="utf-8") as f:
                await f.write(html_content)

            media_count = sum(1 for m in chat_msgs if m.get("media_type"))
            db.save_export_record(
                user_id, connection_id, chat_id, chat_title,
                str(export_path), len(chat_msgs), media_count,
            )

            # Отправляем файл пользователю
            try:
                input_file = FSInputFile(export_path, filename=export_filename)
                await bot.send_document(
                    user_id,
                    input_file,
                    caption=(
                        f"📁 <b>{html_module.escape(str(chat_title))}</b>\n\n"
                        f"💬 Сообщений: {len(chat_msgs)}\n"
                        f"📸 Медиа: {media_count}\n"
                        f"⏰ Файл будет удалён через {MEDIA_CLEANUP_HOURS}ч"
                    ),
                )
            except Exception as e:
                logger.error(f"send export error: {e}")

    except Exception as e:
        logger.error(f"export_connection_on_disconnect error: {e}")


@router.business_message()
async def on_business_message(message: Message, bot: Bot):
    """Обработчик входящих бизнес-сообщений — сохраняет всё включая таймер-медиа."""
    try:
        if not message.business_connection_id:
            return
        conn = db.get_business_connection(message.business_connection_id)
        if not conn:
            logger.warning(f"Unknown connection: {message.business_connection_id}")
            return
        user_id = conn["user_id"]

        # Проверяем подписку
        if not db.check_subscription(user_id):
            return

        db.update_user_activity(user_id)

        # Определяем тип медиа и параметры таймера
        media_type = None
        media_file_id = None
        has_timer = False
        timer_seconds = None
        is_view_once = False
        has_spoiler = False
        media_width = media_height = media_duration = media_file_size = None
        chat_title = None

        # Название чата (если есть)
        if message.chat:
            if hasattr(message.chat, "title") and message.chat.title:
                chat_title = message.chat.title
            elif hasattr(message.chat, "first_name") and message.chat.first_name:
                chat_title = message.chat.first_name

        # ── Фото ──────────────────────────────────────────────
        if message.photo:
            media_type = "photo"
            photo = message.photo[-1]
            media_file_id = photo.file_id
            media_width = photo.width
            media_height = photo.height
            media_file_size = photo.file_size
            if hasattr(message, "has_media_spoiler") and message.has_media_spoiler:
                has_spoiler = True

        # ── Видео ─────────────────────────────────────────────
        elif message.video:
            media_type = "video"
            media_file_id = message.video.file_id
            media_width = message.video.width
            media_height = message.video.height
            media_duration = message.video.duration
            media_file_size = message.video.file_size
            if hasattr(message, "has_media_spoiler") and message.has_media_spoiler:
                has_spoiler = True

        # ── Видеокружок ───────────────────────────────────────
        elif message.video_note:
            media_type = "video_note"
            media_file_id = message.video_note.file_id
            media_duration = message.video_note.duration
            media_file_size = message.video_note.file_size
            # Кружки с таймером: если duration < 60 секунд — считаем таймером
            # На деле отслеживаем по has_timer флагу Telegram
            if hasattr(message.video_note, "self_destruct"):
                has_timer = True

        # ── Голосовое сообщение ───────────────────────────────
        elif message.voice:
            media_type = "voice"
            media_file_id = message.voice.file_id
            media_duration = message.voice.duration
            media_file_size = message.voice.file_size

        # ── Аудио ─────────────────────────────────────────────
        elif message.audio:
            media_type = "audio"
            media_file_id = message.audio.file_id
            media_duration = message.audio.duration
            media_file_size = message.audio.file_size

        # ── Документ ──────────────────────────────────────────
        elif message.document:
            media_type = "document"
            media_file_id = message.document.file_id
            media_file_size = message.document.file_size

        # ── Стикер ────────────────────────────────────────────
        elif message.sticker:
            media_type = "sticker"
            media_file_id = message.sticker.file_id

        # Скачиваем медиафайл
        media_file_path = None
        if media_file_id:
            prefix = "timer_" if (has_timer or has_spoiler or is_view_once) else ""
            media_file_path = await download_media(
                bot, media_file_id, media_type, user_id, prefix=prefix
            )

        # Сохраняем в БД
        sender_name = None
        if message.from_user:
            parts = [message.from_user.first_name or "", message.from_user.last_name or ""]
            sender_name = " ".join(p for p in parts if p).strip() or None

        db.save_message(
            user_id=user_id,
            connection_id=message.business_connection_id,
            chat_id=message.chat.id,
            message_id=message.message_id,
            sender_id=message.from_user.id if message.from_user else 0,
            sender_username=message.from_user.username if message.from_user else None,
            sender_name=sender_name,
            message_text=message.text,
            media_type=media_type,
            media_file_id=media_file_id,
            media_file_path=media_file_path,
            caption=message.caption,
            has_timer=has_timer,
            timer_seconds=timer_seconds,
            is_view_once=is_view_once,
            has_spoiler=has_spoiler,
            media_width=media_width,
            media_height=media_height,
            media_duration=media_duration,
            media_file_size=media_file_size,
            chat_title=chat_title,
        )

        # Если это таймер/спойлер/одноразовое — сохраняем отдельно для быстрого доступа
        if (has_timer or has_spoiler or is_view_once) and media_file_path:
            db.save_timer_media(
                user_id=user_id,
                connection_id=message.business_connection_id,
                chat_id=message.chat.id,
                message_id=message.message_id,
                media_type=media_type,
                media_file_path=media_file_path,
                sender_username=message.from_user.username if message.from_user else None,
            )

        logger.info(
            f"Saved: user={user_id}, chat={message.chat.id}, "
            f"msg={message.message_id}, media={media_type}"
        )

    except Exception as e:
        logger.error(f"on_business_message error: {e}", exc_info=True)


@router.edited_business_message()
async def on_edited_business_message(message: Message, bot: Bot):
    """Обработчик изменения бизнес-сообщения."""
    try:
        if not message.business_connection_id:
            return
        conn = db.get_business_connection(message.business_connection_id)
        if not conn:
            return
        user_id = conn["user_id"]
        if not db.check_subscription(user_id):
            return

        # Получаем оригинал
        original = db.get_message(user_id, message.chat.id, message.message_id)
        if not original:
            return

        original_text = original.get("message_text") or original.get("caption") or ""
        new_text = message.text or message.caption or ""

        db.mark_message_edited(user_id, message.chat.id, message.message_id, original_text)

        sender = f"@{message.from_user.username}" if (message.from_user and message.from_user.username) else "неизвестен"

        notification = (
            f"✏️ <b>Сообщение изменено</b>\n\n"
            f"От: {html_module.escape(sender)}\n\n"
            f"<b>Было:</b>\n"
            f"<code>{html_module.escape(original_text[:500])}</code>\n\n"
            f"<b>Стало:</b>\n"
            f"<code>{html_module.escape(new_text[:500])}</code>"
        )
        await bot.send_message(user_id, notification)

    except Exception as e:
        logger.error(f"on_edited_business_message error: {e}")


@router.deleted_business_messages()
async def on_deleted_messages(deleted: BusinessMessagesDeleted, bot: Bot):
    """
    Обработчик удалённых бизнес-сообщений.
    Формирует уведомления: текст как цитата/код, медиа — сами файлы.
    Таймер-медиа — отправляется немедленно.
    """
    try:
        if not deleted or not deleted.business_connection_id:
            return
        conn = db.get_business_connection(deleted.business_connection_id)
        if not conn:
            logger.warning(f"No connection for deletion: {deleted.business_connection_id}")
            return
        user_id = conn["user_id"]

        for msg_id in deleted.message_ids:
            try:
                saved = db.get_message(user_id, deleted.chat.id, msg_id)

                # Сначала проверяем таймер-медиа — приоритет!
                timer_med = db.get_timer_media(user_id, deleted.chat.id, msg_id)
                if timer_med and not timer_med.get("sent_to_user"):
                    await send_timer_media_notification(bot, user_id, timer_med, deleted)
                    db.mark_timer_media_sent(timer_med["id"])

                if saved:
                    db.mark_message_deleted(user_id, deleted.chat.id, msg_id)
                    await send_deletion_notification(bot, user_id, saved, deleted)
                else:
                    # Сообщение не было сохранено — просто уведомим
                    await bot.send_message(
                        user_id,
                        f"🗑 <b>Сообщение удалено</b>\n\n"
                        f"ID: {msg_id}\n"
                        f"⚠️ Содержимое не было сохранено\n"
                        f"(сообщение пришло до подключения бота)",
                    )

            except Exception as e:
                logger.error(f"on_deleted_messages item error msg={msg_id}: {e}")

    except Exception as e:
        logger.error(f"on_deleted_messages error: {e}", exc_info=True)


async def send_deletion_notification(
    bot: Bot,
    user_id: int,
    saved: Dict,
    deleted: BusinessMessagesDeleted,
):
    """Форматирует и отправляет уведомление об удалении."""
    sender = f"@{saved.get('sender_username')}" if saved.get("sender_username") else (
        saved.get("sender_name") or str(saved.get("sender_id", "?"))
    )
    chat_title = saved.get("chat_title") or f"Чат {deleted.chat.id}"
    ts = str(saved.get("created_at", ""))[:16]

    # Теги
    tags = []
    if saved.get("has_timer"):
        tags.append("⏱ ТАЙМЕР")
    if saved.get("is_view_once"):
        tags.append("👁 ОДНОРАЗОВО")
    if saved.get("has_spoiler"):
        tags.append("🔮 СПОЙЛЕР")
    tags_str = " | ".join(tags) + "\n" if tags else ""

    # Заголовок
    header = (
        f"🗑 <b>Сообщение удалено</b>\n\n"
        f"👤 От: {html_module.escape(sender)}\n"
        f"💬 Чат: {html_module.escape(str(chat_title))}\n"
        f"🕐 Отправлено: {ts}\n"
        f"{tags_str}"
    )

    # Текст как цитата/код
    text_content = saved.get("message_text") or saved.get("caption") or ""
    if text_content:
        # Если текст похож на код — оформляем как код, иначе как цитата
        if any(text_content.startswith(p) for p in ["/", "#", "def ", "class ", "import ", "{"]):
            text_block = f"\n\n<b>Текст (код):</b>\n<code>{html_module.escape(text_content[:2000])}</code>"
        else:
            text_block = f"\n\n<b>Содержимое:</b>\n<blockquote>{html_module.escape(text_content[:2000])}</blockquote>"
    else:
        text_block = ""

    # Медиа описание
    media_desc = ""
    mt = saved.get("media_type")
    if mt:
        icons = {
            "photo": "📸 Фото",
            "video": "🎥 Видео",
            "video_note": "⭕ Видеокружок",
            "voice": "🎤 Голосовое",
            "audio": "🎵 Аудио",
            "document": "📄 Документ",
            "sticker": "🎭 Стикер",
        }
        media_desc = f"\n\n<b>Медиа:</b> {icons.get(mt, mt.upper())}"
        if saved.get("media_duration"):
            media_desc += f" ({saved['media_duration']}сек)"
        if saved.get("media_file_size"):
            size_mb = round(saved["media_file_size"] / 1024 / 1024, 2)
            media_desc += f", {size_mb}MB"

    full_text = header + text_block + media_desc

    try:
        await bot.send_message(user_id, full_text)
    except Exception as e:
        logger.error(f"send notification error: {e}")
        try:
            await bot.send_message(user_id, f"🗑 Сообщение #{saved.get('message_id')} удалено.")
        except Exception:
            pass

    # Отправляем сохранённый медиафайл
    media_path = saved.get("media_file_path")
    if media_path and Path(media_path).exists():
        try:
            file = FSInputFile(media_path)
            mt = saved.get("media_type")
            caption_text = f"📎 Сохранённый {'таймер-' if saved.get('has_timer') else ''}файл"
            if mt == "photo":
                await bot.send_photo(user_id, file, caption=caption_text)
            elif mt in ("video", "video_note"):
                if mt == "video_note":
                    await bot.send_video_note(user_id, file)
                else:
                    await bot.send_video(user_id, file, caption=caption_text)
            elif mt == "voice":
                await bot.send_voice(user_id, file, caption=caption_text)
            elif mt == "audio":
                await bot.send_audio(user_id, file, caption=caption_text)
            elif mt == "document":
                await bot.send_document(user_id, file, caption=caption_text)
        except Exception as e:
            logger.error(f"send media on delete error: {e}")
            await bot.send_message(
                user_id,
                f"⚠️ Медиафайл был сохранён, но не удалось отправить.\n"
                f"Тип: {saved.get('media_type')}",
            )


async def send_timer_media_notification(
    bot: Bot,
    user_id: int,
    timer_med: Dict,
    deleted: BusinessMessagesDeleted,
):
    """Немедленно отправляет таймер-медиа при его исчезновении."""
    sender = f"@{timer_med.get('sender_username')}" if timer_med.get("sender_username") else "?"
    chat_title = getattr(deleted.chat, "title", None) or getattr(deleted.chat, "first_name", None) or f"Чат {deleted.chat.id}"

    try:
        await bot.send_message(
            user_id,
            f"⏱ <b>ПЕРЕХВАЧЕНО ТАЙМЕР-МЕДИА</b>\n\n"
            f"👤 Отправитель: {html_module.escape(sender)}\n"
            f"💬 Чат: {html_module.escape(str(chat_title))}\n"
            f"📁 Тип: {timer_med.get('media_type','?').upper()}\n\n"
            f"Файл сохранён ДО исчезновения 👇",
        )
    except Exception:
        pass

    media_path = timer_med.get("media_file_path")
    if media_path and Path(media_path).exists():
        try:
            file = FSInputFile(media_path)
            mt = timer_med.get("media_type")
            if mt == "photo":
                await bot.send_photo(user_id, file, caption="⏱ Таймер-фото")
            elif mt == "video":
                await bot.send_video(user_id, file, caption="⏱ Таймер-видео")
            elif mt == "video_note":
                await bot.send_video_note(user_id, file)
            elif mt == "voice":
                await bot.send_voice(user_id, file, caption="⏱ Голосовое с таймером")
            elif mt == "audio":
                await bot.send_audio(user_id, file, caption="⏱ Аудио с таймером")
            else:
                await bot.send_document(user_id, file, caption=f"⏱ Таймер: {mt}")
        except Exception as e:
            logger.error(f"send_timer_media error: {e}")


# ═══════════════════════════════════════════════════════════════
# КОМАНДЫ АДМИНИСТРАТОРА
# ═══════════════════════════════════════════════════════════════


@router.message(Command("admin"))
async def cmd_admin(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    total = db.get_user_count()
    active = db.get_active_subscriptions_count()
    pending = db.get_pending_withdrawals()
    await message.answer(
        f"🛡 <b>Быстрая статистика</b>\n\n"
        f"👥 Пользователей: {total}\n"
        f"💎 Активных: {active}\n"
        f"💸 Запросов вывода: {len(pending)}",
        reply_markup=kb_admin_main(),
    )


@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("✅ Действие отменено.", reply_markup=kb_back_main())


# ═══════════════════════════════════════════════════════════════
# ФОНОВЫЕ ЗАДАЧИ
# ═══════════════════════════════════════════════════════════════


async def task_cleanup_media(bot: Bot):
    """Очищает медиафайлы и экспорты через N часов после создания."""
    while True:
        try:
            exports = db.get_pending_cleanups()
            for export in exports:
                # Удаляем HTML экспорт
                fp = export.get("file_path")
                if fp and Path(fp).exists():
                    try:
                        Path(fp).unlink()
                        logger.info(f"Cleaned up export: {fp}")
                    except Exception as e:
                        logger.error(f"Cleanup export error: {e}")

                # Удаляем медиафайлы этого подключения
                conn_msgs = db.get_connection_messages(
                    export["user_id"], export["connection_id"]
                )
                for msg in conn_msgs:
                    for path_field in ["media_file_path", "media_thumbnail_path"]:
                        p = msg.get(path_field)
                        if p and Path(p).exists():
                            try:
                                Path(p).unlink()
                            except Exception:
                                pass

                db.mark_cleanup_done(export["id"])
                logger.info(f"Cleanup done for export #{export['id']}")

        except Exception as e:
            logger.error(f"task_cleanup_media error: {e}")

        await asyncio.sleep(1800)  # Каждые 30 минут


async def task_check_expiring_subscriptions(bot: Bot):
    """Уведомляет пользователей об истечении подписки за 3 дня и 1 день."""
    while True:
        try:
            # За 3 дня
            for u in db.get_expiring_subscriptions(3):
                user_id = u["user_id"]
                try:
                    exp = datetime.fromisoformat(u["subscription_expires"])
                    days_left = (exp - datetime.now()).days
                    if days_left == 3:
                        await bot.send_message(
                            user_id,
                            f"⚠️ <b>Подписка истекает через 3 дня!</b>\n\n"
                            f"Продлите сейчас чтобы не терять доступ к мониторингу.",
                            reply_markup=kb_back_main(),
                        )
                    elif days_left == 1:
                        await bot.send_message(
                            user_id,
                            f"🚨 <b>Подписка истекает ЗАВТРА!</b>\n\n"
                            f"После истечения — {GRACE_PERIOD_HOURS}ч льготный период,\n"
                            f"затем мониторинг остановится.",
                            reply_markup=kb_back_main(),
                        )
                    elif days_left <= 0:
                        db.deactivate_subscription(user_id)
                        await bot.send_message(
                            user_id,
                            f"⏰ <b>Подписка истекла.</b>\n\n"
                            f"У вас есть {GRACE_PERIOD_HOURS} часов льготного периода.\n"
                            f"Продлите подписку чтобы продолжить мониторинг.",
                            reply_markup=kb_back_main(),
                        )
                except Exception as e:
                    logger.error(f"Expiry notify user {user_id}: {e}")

        except Exception as e:
            logger.error(f"task_check_expiring_subscriptions error: {e}")

        await asyncio.sleep(3600)  # Каждый час


async def task_check_expired_grace_periods(bot: Bot):
    """Блокирует доступ после истечения льготного периода."""
    while True:
        try:
            conn = db.get_connection()
            cur = conn.cursor()
            cur.execute("""
                SELECT * FROM users
                WHERE grace_period_ends IS NOT NULL
                  AND grace_period_ends < CURRENT_TIMESTAMP
                  AND subscription_type != 'free'
            """)
            users = [dict(r) for r in cur.fetchall()]
            conn.close()

            for u in users:
                db.deactivate_subscription(u["user_id"])
                try:
                    await bot.send_message(
                        u["user_id"],
                        "🚫 <b>Льготный период завершён.</b>\n\n"
                        "Мониторинг приостановлен.\n"
                        "Оформите подписку в разделе 💎 Подписка.",
                        reply_markup=kb_back_main(),
                    )
                except Exception:
                    pass

        except Exception as e:
            logger.error(f"task_check_expired_grace_periods: {e}")

        await asyncio.sleep(1800)  # Каждые 30 минут


# ═══════════════════════════════════════════════════════════════
# CALLBACK ЗАГЛУШКА
# ═══════════════════════════════════════════════════════════════


@router.callback_query(F.data == "noop")
async def noop_callback(callback: CallbackQuery):
    await callback.answer()


@router.callback_query(F.data == "need_more_gifts")
async def need_more_gifts(callback: CallbackQuery):
    await callback.answer("Недостаточно Stars на балансе", show_alert=True)


# ═══════════════════════════════════════════════════════════════
# ЗАПУСК БОТА
# ═══════════════════════════════════════════════════════════════


async def main():
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    # Стартовое уведомление администратору
    try:
        info = await bot.get_me()
        logger.info(f"Бот запущен: @{info.username} — Business Monitor v7.0.0")
        await bot.send_message(
            ADMIN_ID,
            f"🚀 <b>Бот запущен!</b>\n\n"
            f"@{info.username} — Business Monitor v7.0.0\n"
            f"👥 Пользователей: {db.get_user_count()}\n"
            f"💎 Активных подписок: {db.get_active_subscriptions_count()}",
        )
    except Exception as e:
        logger.error(f"Startup notify error: {e}")

    # Запуск фоновых задач
    asyncio.create_task(task_cleanup_media(bot))
    asyncio.create_task(task_check_expiring_subscriptions(bot))
    asyncio.create_task(task_check_expired_grace_periods(bot))

    # Разрешённые типы обновлений
    allowed_updates = [
        "message", "callback_query", "business_connection",
        "business_message", "edited_business_message",
        "deleted_business_messages", "pre_checkout_query",
    ]

    try:
        await dp.start_polling(bot, allowed_updates=allowed_updates)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем.")
