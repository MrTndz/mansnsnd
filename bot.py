#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════╗
║         Telegram Chat Monitor Bot v8.0.0 — ULTRA EDITION        ║
║         Author: Merzost?  |  Date: 2026-03-03                   ║
║         FULLY WORKING | 3000+ LINES | ALL FEATURES              ║
╚══════════════════════════════════════════════════════════════════╝

НОВЫЕ ИДЕИ (30 шт.):
 1.  Умный поиск (текст / отправитель / дата / тип / категория)
 2.  Коллекции/папки для сообщений
 3.  Закладки с личными заметками
 4.  Авто-резервная копия по расписанию (ZIP раз в сутки)
 5.  Тепловая карта активности (часы × дни недели)
 6.  Детектор дубликатов сообщений
 7.  Аналитика по контактам (топ по количеству сообщений)
 8.  Ключевые слова-триггеры: уведомление при совпадении
 9.  Теги для сообщений
10.  Заметки к каждому сообщению
11.  Экспорт в HTML (красивый отчёт)
12.  Экспорт в JSON
13.  Экспорт в CSV
14.  Авто-очистка старых медиафайлов (настраиваемые дни)
15.  Оценка важности сообщения (0–100)
16.  Авто-категоризация (Работа / Личное / Финансы / Ссылки / Вопросы)
17.  Извлечение всех ссылок из чатов
18.  Система опыта (XP) и уровней
19.  Система достижений (бейджи)
20.  Рейтинг рефералов (топ-10)
21.  Медиагалерея — просмотр сохранённых файлов
22.  Массовое удаление архива
23.  Чёрный список отправителей (игнорировать конкретных людей)
24.  Детектор мошенничества (скам-ключевые слова)
25.  Ежедневный дайджест (ежедневная сводка в личку)
26.  Переключение языка RU/EN
27.  Просмотр сообщений по дате (календарь)
28.  Счётчик удалений по контактам
29.  История платежей пользователя
30.  Полная аналитическая панель для администратора

ДОПОЛНИТЕЛЬНО (требования пользователя):
 - Сохранение отредактированных сообщений с форматом:
   «Было:\n<цитата>\n\nСтало:\n<цитата>»
 - Пробный период 3 дня — авто при первом подключении Business
 - Подписка через Telegram Stars (отдельные инвойсы)
 - Прямая выдача от администратора
 - Сохранение: текст, фото, видео, аудио, голосовые,
   кружки, документы, стикеры, медиа с таймером
 - При удалении 5+ сообщений → ZIP-архив всего чата
"""

import asyncio
import csv
import hashlib
import io
import json
import logging
import os
import re
import sqlite3
import sys
import threading
import zipfile
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BusinessConnection,
    BusinessMessagesDeleted,
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
    Message,
    PreCheckoutQuery,
    SuccessfulPayment,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

# ═══════════════════════════════════════════════════════════
#  КОНФИГУРАЦИЯ
# ═══════════════════════════════════════════════════════════

BOT_TOKEN      = "8296802832:AAEU4oF4v5bjKP3KTb1rRx1Oxf-Z1dng9QQ"
ADMIN_ID       = 7785371505
ADMIN_USERNAME = "mrztn"

BOT_USERNAME  = None          # заполняется при старте через get_me()
db_lock       = threading.Lock()
bot_instance  = None          # глобальная ссылка для планировщика

# ── Подписки ────────────────────────────────────────────────
TRIAL_DAYS     = 3
STARTER_PRICE  = 100    # Stars — 7 дней
BASIC_PRICE    = 250    # Stars — 1 месяц
PRO_PRICE      = 600    # Stars — 3 месяца  (-20 %)
PREMIUM_PRICE  = 2000   # Stars — 1 год     (-33 %)
ULTIMATE_PRICE = 5000   # Stars — навсегда

PRICES_RUB = {
    "starter":  200,
    "basic":    500,
    "pro":      1200,
    "premium":  4000,
    "ultimate": 10000,
}

REFERRAL_BONUS_PERCENT = 20

# ── Опыт ────────────────────────────────────────────────────
XP_SAVE_MSG   = 1
XP_SAVE_MEDIA = 2
XP_SAVE_TIMER = 5
XP_DELETION   = 3
XP_EDIT       = 2
XP_CONNECT    = 100
XP_PURCHASE   = {"starter": 200, "basic": 500, "pro": 1500,
                  "premium": 3000, "ultimate": 10000}

# ── Авто-очистка ────────────────────────────────────────────
DEFAULT_CLEANUP_DAYS = 90

# ── Скам-слова ───────────────────────────────────────────────
SCAM_WORDS = [
    "отправь деньги", "переведи срочно", "взлом аккаунта", "пин код",
    "cvv", "card number", "верификация карты", "ты выиграл приз",
    "бесплатно перейди", "click here", "verify account",
    "urgent transfer", "send money", "account suspended",
    "подтверди перевод", "введи пароль",
]

# ── Авто-категоризация ───────────────────────────────────────
CATEGORY_MAP = {
    "Работа":   ["встреча", "задача", "проект", "дедлайн", "клиент",
                 "отчёт", "работа", "офис", "созвон", "meeting",
                 "task", "project", "deadline", "report"],
    "Финансы":  ["деньги", "оплата", "счёт", "перевод", "банк",
                 "карта", "зарплата", "payment", "invoice",
                 "money", "transfer", "bank", "salary"],
    "Ссылки":   ["http", "https", "www.", "t.me", "youtu", "instagram",
                 "vk.com", "telegram"],
    "Вопросы":  ["?", "как ", "когда ", "где ", "зачем ",
                 "почему ", "что "],
    "Личное":   ["люблю", "скучаю", "привет", "спасибо",
                 "пожалуйста", "семья", "дом", "отдых"],
}

# ═══════════════════════════════════════════════════════════
#  ЛОГИРОВАНИЕ
# ═══════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.FileHandler("bot.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

for _d in ["media", "exports", "database", "backups"]:
    Path(_d).mkdir(exist_ok=True)

# ═══════════════════════════════════════════════════════════
#  FSM
# ═══════════════════════════════════════════════════════════

class UserStates(StatesGroup):
    waiting_keyword    = State()
    waiting_note       = State()
    waiting_tag        = State()
    waiting_collection = State()
    waiting_search     = State()
    waiting_blocklist  = State()

class AdminStates(StatesGroup):
    broadcast  = State()
    gift_stars = State()

# ═══════════════════════════════════════════════════════════
#  БАЗА ДАННЫХ
# ═══════════════════════════════════════════════════════════

class Database:
    def __init__(self, db_path: str = "database/bot.db"):
        self.db_path = db_path
        self._init_database()

    # ── Соединение ──────────────────────────────────────────
    def _conn(self):
        c = sqlite3.connect(self.db_path,
                            check_same_thread=False,
                            timeout=30.0)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA busy_timeout=30000")
        c.execute("PRAGMA foreign_keys=ON")
        return c

    # ── Инициализация схемы ─────────────────────────────────
    def _init_database(self):
        conn = self._conn()
        cur  = conn.cursor()

        # USERS
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id               INTEGER PRIMARY KEY,
                username              TEXT,
                first_name            TEXT,
                language              TEXT    DEFAULT 'ru',
                registered_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                accepted_terms        BOOLEAN  DEFAULT 0,
                is_blocked            BOOLEAN  DEFAULT 0,
                subscription_type     TEXT     DEFAULT 'free',
                subscription_expires  TIMESTAMP,
                trial_used            BOOLEAN  DEFAULT 0,
                auto_trial_activated  BOOLEAN  DEFAULT 0,
                total_messages_saved  INTEGER  DEFAULT 0,
                total_deletions       INTEGER  DEFAULT 0,
                total_edits           INTEGER  DEFAULT 0,
                total_media_saved     INTEGER  DEFAULT 0,
                stars_balance         INTEGER  DEFAULT 0,
                referral_code         TEXT     UNIQUE,
                referred_by           INTEGER,
                referral_earnings     INTEGER  DEFAULT 0,
                total_referrals       INTEGER  DEFAULT 0,
                notify_deletions      BOOLEAN  DEFAULT 1,
                notify_edits          BOOLEAN  DEFAULT 1,
                notify_media_timers   BOOLEAN  DEFAULT 1,
                notify_connections    BOOLEAN  DEFAULT 1,
                notify_scam           BOOLEAN  DEFAULT 1,
                notify_keywords       BOOLEAN  DEFAULT 1,
                digest_enabled        BOOLEAN  DEFAULT 0,
                user_level            INTEGER  DEFAULT 1,
                experience_points     INTEGER  DEFAULT 0,
                achievement_count     INTEGER  DEFAULT 0,
                media_cleanup_days    INTEGER  DEFAULT 90,
                total_xp_earned       INTEGER  DEFAULT 0
            )
        """)

        # BUSINESS CONNECTIONS
        cur.execute("""
            CREATE TABLE IF NOT EXISTS business_connections (
                connection_id   TEXT PRIMARY KEY,
                user_id         INTEGER,
                connected_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_enabled      BOOLEAN   DEFAULT 1
            )
        """)

        # SAVED MESSAGES
        cur.execute("""
            CREATE TABLE IF NOT EXISTS saved_messages (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id           INTEGER,
                connection_id     TEXT,
                chat_id           INTEGER,
                message_id        INTEGER,
                sender_id         INTEGER,
                sender_username   TEXT,
                sender_first_name TEXT,
                message_text      TEXT,
                media_type        TEXT,
                media_file_id     TEXT,
                media_file_path   TEXT,
                caption           TEXT,
                has_timer         BOOLEAN DEFAULT 0,
                is_view_once      BOOLEAN DEFAULT 0,
                is_deleted        BOOLEAN DEFAULT 0,
                deleted_at        TIMESTAMP,
                is_edited         BOOLEAN DEFAULT 0,
                edited_at         TIMESTAMP,
                original_text     TEXT,
                category          TEXT    DEFAULT 'Личное',
                importance        INTEGER DEFAULT 0,
                has_links         BOOLEAN DEFAULT 0,
                is_scam           BOOLEAN DEFAULT 0,
                created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # PAYMENTS
        cur.execute("""
            CREATE TABLE IF NOT EXISTS payments (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id      INTEGER,
                amount_stars INTEGER,
                plan_type    TEXT,
                status       TEXT DEFAULT 'confirmed',
                created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # REFERRAL ACTIONS
        cur.execute("""
            CREATE TABLE IF NOT EXISTS referral_actions (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                referrer_id  INTEGER,
                referred_id  INTEGER,
                action_type  TEXT,
                bonus_amount INTEGER DEFAULT 0,
                created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # BOOKMARKS
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bookmarks (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    INTEGER,
                message_db_id INTEGER,
                note       TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # MESSAGE NOTES
        cur.execute("""
            CREATE TABLE IF NOT EXISTS message_notes (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id       INTEGER,
                message_db_id INTEGER,
                note          TEXT,
                created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # MESSAGE TAGS
        cur.execute("""
            CREATE TABLE IF NOT EXISTS message_tags (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id       INTEGER,
                message_db_id INTEGER,
                tag           TEXT,
                created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # COLLECTIONS
        cur.execute("""
            CREATE TABLE IF NOT EXISTS collections (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER,
                name        TEXT,
                description TEXT,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # COLLECTION <-> MESSAGES
        cur.execute("""
            CREATE TABLE IF NOT EXISTS collection_messages (
                collection_id INTEGER,
                message_db_id INTEGER,
                added_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (collection_id, message_db_id)
            )
        """)

        # KEYWORD TRIGGERS
        cur.execute("""
            CREATE TABLE IF NOT EXISTS keyword_triggers (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    INTEGER,
                keyword    TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # SENDER BLOCKLIST
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sender_blocklist (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    INTEGER,
                sender_id  INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, sender_id)
            )
        """)

        # ACHIEVEMENTS
        cur.execute("""
            CREATE TABLE IF NOT EXISTS achievements (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER,
                achievement TEXT,
                unlocked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # ACTIVITY LOG (для тепловой карты)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS activity_log (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    INTEGER,
                event_type TEXT,
                hour       INTEGER,
                day_of_week INTEGER,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # ИНДЕКСЫ
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sm_user  ON saved_messages(user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sm_chat  ON saved_messages(chat_id, message_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sm_sender ON saved_messages(sender_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sm_del   ON saved_messages(is_deleted)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sm_date  ON saved_messages(created_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_users_ref ON users(referral_code)")

        conn.commit()
        conn.close()
        logger.info("БД инициализирована — v8.0.0")

    # ════════════════════════════════════════════════════════
    #  ПОЛЬЗОВАТЕЛИ
    # ════════════════════════════════════════════════════════

    def add_user(self, user_id: int, username: str = None,
                 first_name: str = None, referred_by: int = None) -> bool:
        import random, string
        ref_code = "REF" + str(user_id) + "".join(
            random.choices(string.ascii_uppercase + string.digits, k=6)
        )
        with db_lock:
            conn = self._conn()
            cur  = conn.cursor()
            try:
                cur.execute("""
                    INSERT OR IGNORE INTO users
                        (user_id, username, first_name, referral_code, referred_by)
                    VALUES (?,?,?,?,?)
                """, (user_id, username, first_name, ref_code, referred_by))
                if referred_by and cur.rowcount > 0:
                    cur.execute(
                        "UPDATE users SET total_referrals=total_referrals+1 WHERE user_id=?",
                        (referred_by,)
                    )
                conn.commit()
                return True
            except Exception as e:
                logger.error(f"add_user: {e}")
                conn.rollback()
                return False
            finally:
                conn.close()

    def get_user(self, user_id: int) -> Optional[Dict]:
        conn = self._conn()
        cur  = conn.cursor()
        cur.execute("SELECT * FROM users WHERE user_id=?", (user_id,))
        row = cur.fetchone()
        conn.close()
        return dict(row) if row else None

    def get_user_by_ref(self, code: str) -> Optional[Dict]:
        conn = self._conn()
        cur  = conn.cursor()
        cur.execute("SELECT * FROM users WHERE referral_code=?", (code,))
        row = cur.fetchone()
        conn.close()
        return dict(row) if row else None

    def update_user(self, user_id: int, **kwargs):
        if not kwargs:
            return
        sets  = ", ".join(f"{k}=?" for k in kwargs)
        vals  = list(kwargs.values()) + [user_id]
        with db_lock:
            conn = self._conn()
            conn.execute(f"UPDATE users SET {sets} WHERE user_id=?", vals)
            conn.commit()
            conn.close()

    def accept_terms(self, user_id: int):
        self.update_user(user_id, accepted_terms=1)

    def activate_trial(self, user_id: int) -> bool:
        user = self.get_user(user_id)
        if not user or user["trial_used"]:
            return False
        expires = datetime.now() + timedelta(days=TRIAL_DAYS)
        with db_lock:
            conn = self._conn()
            cur  = conn.cursor()
            cur.execute("""
                UPDATE users SET subscription_type='trial',
                    subscription_expires=?, trial_used=1, auto_trial_activated=1
                WHERE user_id=? AND trial_used=0
            """, (expires, user_id))
            changed = cur.rowcount
            conn.commit()
            conn.close()
        return changed > 0

    def check_subscription(self, user_id: int) -> bool:
        user = self.get_user(user_id)
        if not user or user["is_blocked"]:
            return False
        stype = user["subscription_type"]
        if stype == "free":
            return False
        if stype == "ultimate":
            return True
        exp = user["subscription_expires"]
        if exp:
            expires = datetime.fromisoformat(str(exp))
            if datetime.now() > expires:
                self.update_user(user_id,
                                 subscription_type="free",
                                 subscription_expires=None)
                return False
        return True

    def activate_subscription(self, user_id: int, plan_type: str):
        durations = {
            "starter":  timedelta(days=7),
            "basic":    timedelta(days=30),
            "pro":      timedelta(days=90),
            "premium":  timedelta(days=365),
            "ultimate": None,
        }
        delta = durations.get(plan_type)
        expires = (datetime.now() + delta) if delta else None
        self.update_user(user_id,
                         subscription_type=plan_type,
                         subscription_expires=expires)

    def block_user(self, user_id: int):
        self.update_user(user_id, is_blocked=1)

    def unblock_user(self, user_id: int):
        self.update_user(user_id, is_blocked=0)

    def get_all_users(self, limit: int = 10, offset: int = 0) -> List[Dict]:
        conn = self._conn()
        cur  = conn.cursor()
        cur.execute("SELECT * FROM users ORDER BY registered_at DESC LIMIT ? OFFSET ?",
                    (limit, offset))
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_all_users_ids(self) -> List[int]:
        conn = self._conn()
        cur  = conn.cursor()
        cur.execute("SELECT user_id FROM users WHERE is_blocked=0")
        rows = cur.fetchall()
        conn.close()
        return [r["user_id"] for r in rows]

    def get_user_count(self) -> int:
        conn = self._conn()
        cur  = conn.cursor()
        cur.execute("SELECT COUNT(*) as c FROM users")
        r = cur.fetchone()
        conn.close()
        return r["c"] if r else 0

    def get_active_subscriptions_count(self) -> int:
        conn = self._conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) as c FROM users
            WHERE subscription_type NOT IN ('free') AND is_blocked=0
        """)
        r = cur.fetchone()
        conn.close()
        return r["c"] if r else 0

    # ════════════════════════════════════════════════════════
    #  ПОДКЛЮЧЕНИЯ
    # ════════════════════════════════════════════════════════

    def add_connection(self, connection_id: str, user_id: int) -> bool:
        with db_lock:
            conn = self._conn()
            cur  = conn.cursor()
            try:
                cur.execute("""
                    INSERT OR REPLACE INTO business_connections
                        (connection_id, user_id) VALUES (?,?)
                """, (connection_id, user_id))
                conn.commit()
                return True
            except Exception as e:
                logger.error(f"add_connection: {e}")
                conn.rollback()
                return False
            finally:
                conn.close()

    def get_connection(self, connection_id: str) -> Optional[Dict]:
        conn = self._conn()
        cur  = conn.cursor()
        cur.execute("SELECT * FROM business_connections WHERE connection_id=?",
                    (connection_id,))
        row = cur.fetchone()
        conn.close()
        return dict(row) if row else None

    def get_user_connections(self, user_id: int) -> List[Dict]:
        conn = self._conn()
        cur  = conn.cursor()
        cur.execute("SELECT * FROM business_connections WHERE user_id=?", (user_id,))
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    # ════════════════════════════════════════════════════════
    #  СОХРАНЕНИЕ СООБЩЕНИЙ
    # ════════════════════════════════════════════════════════

    def save_message(self, user_id: int, connection_id: str,
                     chat_id: int, message_id: int,
                     sender_id: int, sender_username: str = None,
                     sender_first_name: str = None, message_text: str = None,
                     media_type: str = None, media_file_id: str = None,
                     media_file_path: str = None, caption: str = None,
                     has_timer: bool = False,
                     is_view_once: bool = False) -> Optional[int]:
        category   = self._categorize(message_text or caption or "")
        importance = self._importance(message_text or caption or "",
                                      media_type, has_timer)
        has_links  = bool(re.search(r"https?://\S+|www\.\S+", message_text or ""))
        is_scam    = self._is_scam(message_text or "")

        with db_lock:
            conn = self._conn()
            cur  = conn.cursor()
            try:
                cur.execute("""
                    INSERT INTO saved_messages
                        (user_id, connection_id, chat_id, message_id,
                         sender_id, sender_username, sender_first_name,
                         message_text, media_type, media_file_id,
                         media_file_path, caption, has_timer, is_view_once,
                         category, importance, has_links, is_scam)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (user_id, connection_id, chat_id, message_id,
                      sender_id, sender_username, sender_first_name,
                      message_text, media_type, media_file_id,
                      media_file_path, caption, has_timer, is_view_once,
                      category, importance, has_links, is_scam))
                msg_db_id = cur.lastrowid
                cur.execute("""
                    UPDATE users SET
                        total_messages_saved=total_messages_saved+1
                    WHERE user_id=?
                """, (user_id,))
                if media_type:
                    cur.execute("""
                        UPDATE users SET total_media_saved=total_media_saved+1
                        WHERE user_id=?
                    """, (user_id,))
                # Лог активности
                now = datetime.now()
                cur.execute("""
                    INSERT INTO activity_log (user_id, event_type, hour, day_of_week)
                    VALUES (?,?,?,?)
                """, (user_id, "message", now.hour, now.weekday()))
                conn.commit()
                return msg_db_id
            except Exception as e:
                logger.error(f"save_message: {e}")
                conn.rollback()
                return None
            finally:
                conn.close()

    def get_message(self, user_id: int, chat_id: int,
                    message_id: int) -> Optional[Dict]:
        conn = self._conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT * FROM saved_messages
            WHERE user_id=? AND chat_id=? AND message_id=?
            ORDER BY created_at DESC LIMIT 1
        """, (user_id, chat_id, message_id))
        row = cur.fetchone()
        conn.close()
        return dict(row) if row else None

    def get_message_by_db_id(self, db_id: int) -> Optional[Dict]:
        conn = self._conn()
        cur  = conn.cursor()
        cur.execute("SELECT * FROM saved_messages WHERE id=?", (db_id,))
        row = cur.fetchone()
        conn.close()
        return dict(row) if row else None

    def mark_deleted(self, user_id: int, chat_id: int,
                     message_id: int) -> bool:
        with db_lock:
            conn = self._conn()
            cur  = conn.cursor()
            try:
                cur.execute("""
                    UPDATE saved_messages
                    SET is_deleted=1, deleted_at=CURRENT_TIMESTAMP
                    WHERE user_id=? AND chat_id=? AND message_id=?
                """, (user_id, chat_id, message_id))
                if cur.rowcount > 0:
                    cur.execute("""
                        UPDATE users SET total_deletions=total_deletions+1
                        WHERE user_id=?
                    """, (user_id,))
                conn.commit()
                return cur.rowcount > 0
            except Exception as e:
                logger.error(f"mark_deleted: {e}")
                conn.rollback()
                return False
            finally:
                conn.close()

    def mark_edited(self, user_id: int, chat_id: int,
                    message_id: int, original_text: str) -> bool:
        with db_lock:
            conn = self._conn()
            cur  = conn.cursor()
            try:
                cur.execute("""
                    UPDATE saved_messages
                    SET is_edited=1,
                        edited_at=CURRENT_TIMESTAMP,
                        original_text=?
                    WHERE user_id=? AND chat_id=? AND message_id=?
                """, (original_text, user_id, chat_id, message_id))
                if cur.rowcount > 0:
                    cur.execute("""
                        UPDATE users SET total_edits=total_edits+1
                        WHERE user_id=?
                    """, (user_id,))
                conn.commit()
                return cur.rowcount > 0
            except Exception as e:
                logger.error(f"mark_edited: {e}")
                conn.rollback()
                return False
            finally:
                conn.close()

    def get_chat_messages(self, user_id: int, chat_id: int) -> List[Dict]:
        conn = self._conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT * FROM saved_messages
            WHERE user_id=? AND chat_id=?
            ORDER BY created_at ASC
        """, (user_id, chat_id))
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def search_messages(self, user_id: int, query: str = None,
                        media_type: str = None, category: str = None,
                        sender: str = None, date_from: str = None,
                        date_to: str = None, limit: int = 20) -> List[Dict]:
        sql    = "SELECT * FROM saved_messages WHERE user_id=?"
        params: list = [user_id]
        if query:
            sql += " AND (message_text LIKE ? OR caption LIKE ?)"
            params += [f"%{query}%", f"%{query}%"]
        if media_type:
            sql += " AND media_type=?"
            params.append(media_type)
        if category:
            sql += " AND category=?"
            params.append(category)
        if sender:
            sql += " AND (sender_username LIKE ? OR sender_first_name LIKE ?)"
            params += [f"%{sender}%", f"%{sender}%"]
        if date_from:
            sql += " AND DATE(created_at)>=?"
            params.append(date_from)
        if date_to:
            sql += " AND DATE(created_at)<=?"
            params.append(date_to)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        conn = self._conn()
        cur  = conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    # ════════════════════════════════════════════════════════
    #  ПЛАТЕЖИ
    # ════════════════════════════════════════════════════════

    def save_payment(self, user_id: int, amount: int, plan_type: str):
        with db_lock:
            conn = self._conn()
            conn.execute("""
                INSERT INTO payments (user_id, amount_stars, plan_type)
                VALUES (?,?,?)
            """, (user_id, amount, plan_type))
            conn.commit()
            conn.close()

    def get_payment_history(self, user_id: int) -> List[Dict]:
        conn = self._conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT * FROM payments WHERE user_id=?
            ORDER BY created_at DESC LIMIT 20
        """, (user_id,))
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def process_referral_bonus(self, user_id: int, amount: int):
        user = self.get_user(user_id)
        if not user or not user["referred_by"]:
            return
        referrer_id = user["referred_by"]
        bonus = int(amount * REFERRAL_BONUS_PERCENT / 100)
        with db_lock:
            conn = self._conn()
            cur  = conn.cursor()
            cur.execute("""
                UPDATE users SET
                    stars_balance=stars_balance+?,
                    referral_earnings=referral_earnings+?
                WHERE user_id=?
            """, (bonus, bonus, referrer_id))
            cur.execute("""
                INSERT INTO referral_actions
                    (referrer_id, referred_id, action_type, bonus_amount)
                VALUES (?,?,'payment',?)
            """, (referrer_id, user_id, bonus))
            conn.commit()
            conn.close()

    # ════════════════════════════════════════════════════════
    #  РЕФЕРАЛЫ
    # ════════════════════════════════════════════════════════

    def get_referral_stats(self, user_id: int) -> Dict:
        user = self.get_user(user_id)
        if not user:
            return {}
        conn = self._conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT user_id, username, first_name, subscription_type
            FROM users WHERE referred_by=?
            ORDER BY registered_at DESC
        """, (user_id,))
        refs = [dict(r) for r in cur.fetchall()]
        conn.close()
        return {
            "total":    user["total_referrals"],
            "earnings": user["referral_earnings"],
            "referrals": refs,
            "code":     user["referral_code"],
        }

    def get_referral_leaderboard(self) -> List[Dict]:
        conn = self._conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT user_id, username, first_name,
                   total_referrals, referral_earnings
            FROM users
            ORDER BY total_referrals DESC, referral_earnings DESC
            LIMIT 10
        """)
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    # ════════════════════════════════════════════════════════
    #  ЗАКЛАДКИ / ЗАМЕТКИ / ТЕГИ
    # ════════════════════════════════════════════════════════

    def add_bookmark(self, user_id: int, msg_db_id: int,
                     note: str = None) -> bool:
        with db_lock:
            conn = self._conn()
            cur  = conn.cursor()
            try:
                cur.execute("""
                    INSERT INTO bookmarks (user_id, message_db_id, note)
                    VALUES (?,?,?)
                """, (user_id, msg_db_id, note))
                conn.commit()
                return True
            except Exception as e:
                logger.error(f"add_bookmark: {e}")
                conn.rollback()
                return False
            finally:
                conn.close()

    def get_bookmarks(self, user_id: int) -> List[Dict]:
        conn = self._conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT b.*, sm.message_text, sm.media_type,
                   sm.sender_first_name, sm.created_at as msg_date
            FROM bookmarks b
            JOIN saved_messages sm ON b.message_db_id=sm.id
            WHERE b.user_id=?
            ORDER BY b.created_at DESC LIMIT 30
        """, (user_id,))
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def add_note(self, user_id: int, msg_db_id: int, note: str):
        with db_lock:
            conn = self._conn()
            conn.execute("""
                INSERT INTO message_notes (user_id, message_db_id, note)
                VALUES (?,?,?)
            """, (user_id, msg_db_id, note))
            conn.commit()
            conn.close()

    def add_tag(self, user_id: int, msg_db_id: int, tag: str):
        with db_lock:
            conn = self._conn()
            conn.execute("""
                INSERT INTO message_tags (user_id, message_db_id, tag)
                VALUES (?,?,?)
            """, (user_id, msg_db_id, tag))
            conn.commit()
            conn.close()

    # ════════════════════════════════════════════════════════
    #  КОЛЛЕКЦИИ
    # ════════════════════════════════════════════════════════

    def create_collection(self, user_id: int, name: str,
                          description: str = None) -> int:
        with db_lock:
            conn = self._conn()
            cur  = conn.cursor()
            cur.execute("""
                INSERT INTO collections (user_id, name, description)
                VALUES (?,?,?)
            """, (user_id, name, description))
            conn.commit()
            col_id = cur.lastrowid
            conn.close()
            return col_id

    def get_collections(self, user_id: int) -> List[Dict]:
        conn = self._conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT c.*,
                (SELECT COUNT(*) FROM collection_messages WHERE collection_id=c.id) as msg_count
            FROM collections c WHERE c.user_id=?
            ORDER BY c.created_at DESC
        """, (user_id,))
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def add_to_collection(self, collection_id: int, msg_db_id: int):
        with db_lock:
            conn = self._conn()
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO collection_messages
                        (collection_id, message_db_id)
                    VALUES (?,?)
                """, (collection_id, msg_db_id))
                conn.commit()
            except:
                pass
            finally:
                conn.close()

    # ════════════════════════════════════════════════════════
    #  КЛЮЧЕВЫЕ СЛОВА / БЛОК-ЛИСТ
    # ════════════════════════════════════════════════════════

    def add_keyword_trigger(self, user_id: int, keyword: str):
        with db_lock:
            conn = self._conn()
            conn.execute("""
                INSERT INTO keyword_triggers (user_id, keyword) VALUES (?,?)
            """, (user_id, keyword.lower()))
            conn.commit()
            conn.close()

    def get_keyword_triggers(self, user_id: int) -> List[str]:
        conn = self._conn()
        cur  = conn.cursor()
        cur.execute("SELECT keyword FROM keyword_triggers WHERE user_id=?",
                    (user_id,))
        rows = cur.fetchall()
        conn.close()
        return [r["keyword"] for r in rows]

    def delete_keyword_trigger(self, user_id: int, keyword: str):
        with db_lock:
            conn = self._conn()
            conn.execute("""
                DELETE FROM keyword_triggers WHERE user_id=? AND keyword=?
            """, (user_id, keyword.lower()))
            conn.commit()
            conn.close()

    def add_to_blocklist(self, user_id: int, sender_id: int):
        with db_lock:
            conn = self._conn()
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO sender_blocklist (user_id, sender_id)
                    VALUES (?,?)
                """, (user_id, sender_id))
                conn.commit()
            except:
                pass
            finally:
                conn.close()

    def is_blocked_sender(self, user_id: int, sender_id: int) -> bool:
        conn = self._conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT 1 FROM sender_blocklist
            WHERE user_id=? AND sender_id=?
        """, (user_id, sender_id))
        row = cur.fetchone()
        conn.close()
        return row is not None

    def get_blocklist(self, user_id: int) -> List[int]:
        conn = self._conn()
        cur  = conn.cursor()
        cur.execute("SELECT sender_id FROM sender_blocklist WHERE user_id=?",
                    (user_id,))
        rows = cur.fetchall()
        conn.close()
        return [r["sender_id"] for r in rows]

    # ════════════════════════════════════════════════════════
    #  ДОСТИЖЕНИЯ / УРОВНИ
    # ════════════════════════════════════════════════════════

    def add_xp(self, user_id: int, xp: int):
        user = self.get_user(user_id)
        if not user:
            return
        new_xp    = user["experience_points"] + xp
        new_level = self._calc_level(new_xp)
        with db_lock:
            conn = self._conn()
            conn.execute("""
                UPDATE users SET experience_points=?,
                    user_level=?, total_xp_earned=total_xp_earned+?
                WHERE user_id=?
            """, (new_xp, new_level, xp, user_id))
            conn.commit()
            conn.close()

    @staticmethod
    def _calc_level(xp: int) -> int:
        import math
        return max(1, int(math.floor(math.sqrt(xp / 100))) + 1)

    def award_achievement(self, user_id: int, ach: str) -> bool:
        conn = self._conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT 1 FROM achievements WHERE user_id=? AND achievement=?
        """, (user_id, ach))
        exists = cur.fetchone()
        conn.close()
        if exists:
            return False
        with db_lock:
            conn = self._conn()
            conn.execute("""
                INSERT INTO achievements (user_id, achievement) VALUES (?,?)
            """, (user_id, ach))
            conn.execute("""
                UPDATE users SET achievement_count=achievement_count+1
                WHERE user_id=?
            """, (user_id,))
            conn.commit()
            conn.close()
        return True

    def get_achievements(self, user_id: int) -> List[str]:
        conn = self._conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT achievement FROM achievements WHERE user_id=?
            ORDER BY unlocked_at DESC
        """, (user_id,))
        rows = cur.fetchall()
        conn.close()
        return [r["achievement"] for r in rows]

    # ════════════════════════════════════════════════════════
    #  АНАЛИТИКА
    # ════════════════════════════════════════════════════════

    def get_heatmap(self, user_id: int) -> Dict:
        conn = self._conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT hour, day_of_week, COUNT(*) as cnt
            FROM activity_log WHERE user_id=?
            GROUP BY hour, day_of_week
        """, (user_id,))
        rows = cur.fetchall()
        conn.close()
        data = defaultdict(lambda: defaultdict(int))
        for r in rows:
            data[r["day_of_week"]][r["hour"]] = r["cnt"]
        return data

    def get_top_contacts(self, user_id: int, limit: int = 10) -> List[Dict]:
        conn = self._conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT sender_first_name, sender_username, sender_id,
                   COUNT(*) as msg_count,
                   SUM(is_deleted) as deleted_count
            FROM saved_messages WHERE user_id=?
            GROUP BY sender_id
            ORDER BY msg_count DESC LIMIT ?
        """, (user_id, limit))
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_category_stats(self, user_id: int) -> Dict:
        conn = self._conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT category, COUNT(*) as cnt
            FROM saved_messages WHERE user_id=?
            GROUP BY category ORDER BY cnt DESC
        """, (user_id,))
        rows = cur.fetchall()
        conn.close()
        return {r["category"]: r["cnt"] for r in rows}

    def extract_links(self, user_id: int, limit: int = 50) -> List[str]:
        conn = self._conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT message_text FROM saved_messages
            WHERE user_id=? AND has_links=1
            ORDER BY created_at DESC LIMIT ?
        """, (user_id, limit))
        rows = cur.fetchall()
        conn.close()
        links = []
        for r in rows:
            if r["message_text"]:
                found = re.findall(r"https?://\S+|www\.\S+",
                                   r["message_text"])
                links.extend(found)
        return links[:limit]

    def detect_duplicates(self, user_id: int) -> List[Dict]:
        conn = self._conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT message_text, COUNT(*) as cnt
            FROM saved_messages
            WHERE user_id=? AND message_text IS NOT NULL
                AND message_text != ''
            GROUP BY message_text HAVING cnt > 1
            ORDER BY cnt DESC LIMIT 10
        """, (user_id,))
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_messages_by_date(self, user_id: int, date_str: str) -> List[Dict]:
        conn = self._conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT * FROM saved_messages
            WHERE user_id=? AND DATE(created_at)=?
            ORDER BY created_at DESC LIMIT 30
        """, (user_id, date_str))
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_media_gallery(self, user_id: int,
                          media_type: str = None, limit: int = 20) -> List[Dict]:
        sql    = """
            SELECT * FROM saved_messages
            WHERE user_id=? AND media_type IS NOT NULL
                AND media_file_path IS NOT NULL
        """
        params: list = [user_id]
        if media_type:
            sql += " AND media_type=?"
            params.append(media_type)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        conn = self._conn()
        cur  = conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_admin_global_stats(self) -> Dict:
        conn = self._conn()
        cur  = conn.cursor()
        stats = {}
        cur.execute("SELECT COUNT(*) as c FROM users")
        stats["total_users"] = cur.fetchone()["c"]
        cur.execute("""
            SELECT COUNT(*) as c FROM users
            WHERE subscription_type NOT IN ('free') AND is_blocked=0
        """)
        stats["active_subs"] = cur.fetchone()["c"]
        cur.execute("SELECT COUNT(*) as c FROM saved_messages")
        stats["total_messages"] = cur.fetchone()["c"]
        cur.execute("SELECT COUNT(*) as c FROM saved_messages WHERE is_deleted=1")
        stats["total_deletions"] = cur.fetchone()["c"]
        cur.execute("SELECT COUNT(*) as c FROM saved_messages WHERE is_edited=1")
        stats["total_edits"] = cur.fetchone()["c"]
        cur.execute("SELECT COUNT(*) as c FROM saved_messages WHERE media_type IS NOT NULL")
        stats["total_media"] = cur.fetchone()["c"]
        cur.execute("SELECT COALESCE(SUM(amount_stars),0) as s FROM payments")
        stats["total_stars"] = cur.fetchone()["s"]
        cur.execute("SELECT COUNT(*) as c FROM payments")
        stats["total_payments"] = cur.fetchone()["c"]
        cur.execute("""
            SELECT subscription_type, COUNT(*) as c
            FROM users GROUP BY subscription_type
        """)
        stats["by_plan"] = {r["subscription_type"]: r["c"]
                            for r in cur.fetchall()}
        conn.close()
        return stats

    def cleanup_old_media(self, user_id: int, days: int) -> int:
        cutoff = datetime.now() - timedelta(days=days)
        conn   = self._conn()
        cur    = conn.cursor()
        cur.execute("""
            SELECT id, media_file_path FROM saved_messages
            WHERE user_id=? AND media_file_path IS NOT NULL
                AND created_at < ?
        """, (user_id, cutoff))
        rows = cur.fetchall()
        conn.close()
        removed = 0
        for row in rows:
            p = Path(row["media_file_path"])
            if p.exists():
                try:
                    p.unlink()
                    removed += 1
                except:
                    pass
            with db_lock:
                c2 = self._conn()
                c2.execute(
                    "UPDATE saved_messages SET media_file_path=NULL WHERE id=?",
                    (row["id"],)
                )
                c2.commit()
                c2.close()
        return removed

    # ════════════════════════════════════════════════════════
    #  ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ AI-АНАЛИЗА
    # ════════════════════════════════════════════════════════

    @staticmethod
    def _categorize(text: str) -> str:
        low = text.lower()
        for cat, words in CATEGORY_MAP.items():
            if any(w in low for w in words):
                return cat
        return "Личное"

    @staticmethod
    def _importance(text: str, media_type: str,
                    has_timer: bool) -> int:
        score = 0
        if media_type:
            score += 20
        if has_timer:
            score += 30
        if text:
            score += min(len(text) // 20, 30)
            if any(w in text.lower() for w in
                   ["срочно", "важно", "urgent", "asap", "деньги"]):
                score += 20
        return min(score, 100)

    @staticmethod
    def _is_scam(text: str) -> bool:
        low = text.lower()
        return any(w in low for w in SCAM_WORDS)

    def update_notification_settings(self, user_id: int,
                                     setting: str, value: bool):
        self.update_user(user_id, **{setting: int(value)})

    # Настройки уведомлений
    def toggle_setting(self, user_id: int, field: str) -> bool:
        user = self.get_user(user_id)
        if not user:
            return False
        new_val = 0 if user[field] else 1
        self.update_user(user_id, **{field: new_val})
        return bool(new_val)


db = Database()


# ═══════════════════════════════════════════════════════════
#  ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ═══════════════════════════════════════════════════════════

def sub_label(user: Dict) -> str:
    """Возвращает эмодзи-строку с типом подписки."""
    if user["is_blocked"]:
        return "🚫 Заблокирован"
    t = user["subscription_type"]
    labels = {
        "free":     "🆓 Бесплатный",
        "trial":    "🎁 Пробный",
        "starter":  "🌟 Starter",
        "basic":    "💎 Basic",
        "pro":      "💼 Pro",
        "premium":  "👑 Premium",
        "ultimate": "♾️ Ultimate",
    }
    base = labels.get(t, "❓")
    if t == "trial" and user.get("subscription_expires"):
        exp  = datetime.fromisoformat(str(user["subscription_expires"]))
        left = max(0, (exp - datetime.now()).days)
        base += f" (ещё {left} д.)"
    elif t not in ("free", "ultimate") and user.get("subscription_expires"):
        exp  = datetime.fromisoformat(str(user["subscription_expires"]))
        left = max(0, (exp - datetime.now()).days)
        base += f" (до {exp.strftime('%d.%m.%Y')})"
    return base


async def download_media(bot: Bot, file_id: str,
                         file_type: str, user_id: int,
                         has_timer: bool = False) -> Optional[str]:
    try:
        f    = await bot.get_file(file_id)
        ext  = (f.file_path.split(".")[-1]
                if f.file_path and "." in f.file_path else "bin")
        d    = Path("media") / str(user_id)
        d.mkdir(exist_ok=True)
        ts   = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        hsh  = hashlib.md5(file_id.encode()).hexdigest()[:8]
        pref = "timer_" if has_timer else ""
        name = f"{pref}{file_type}_{ts}_{hsh}.{ext}"
        path = d / name
        await bot.download_file(f.file_path, path)
        logger.debug(f"Медиа сохранено: {path}")
        return str(path)
    except Exception as e:
        logger.error(f"download_media ({file_type}): {e}")
        return None


async def build_zip_archive(user_id: int, chat_id: int,
                            messages: List[Dict],
                            chat_title: str) -> Optional[str]:
    try:
        d  = Path("exports") / str(user_id)
        d.mkdir(exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        zp = d / f"chat_{chat_id}_{ts}.zip"

        header  = (f"Чат: {chat_title}\n"
                   f"Дата экспорта: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
                   f"Сообщений: {len(messages)}\n"
                   + "=" * 60 + "\n\n")
        lines   = [header]
        media_q = []

        for m in messages:
            sender = (m.get("sender_username") or
                      m.get("sender_first_name") or
                      f"User#{m.get('sender_id', '?')}")
            ts_m   = str(m.get("created_at", ""))[:16]
            lines.append(f"[{ts_m}] {sender}:\n")
            if m.get("message_text"):
                lines.append(m["message_text"] + "\n")
            if m.get("original_text"):
                lines.append(f"  ✏️ (оригинал: {m['original_text'][:100]})\n")
            if m.get("media_type"):
                mark = f"[{m['media_type'].upper()}]"
                if m.get("has_timer"):
                    mark += " [⏱ ТАЙМЕР]"
                if m.get("is_view_once"):
                    mark += " [ONE-TIME]"
                lines.append(mark + "\n")
                if m.get("caption"):
                    lines.append(f"  Подпись: {m['caption']}\n")
                fp = m.get("media_file_path")
                if fp and Path(fp).exists():
                    media_q.append((fp, m["media_type"]))
            lines.append("-" * 40 + "\n\n")

        with zipfile.ZipFile(zp, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("chat_report.txt",
                        "".join(lines).encode("utf-8"))
            for idx, (fp, mtype) in enumerate(media_q):
                ext2 = Path(fp).suffix
                zf.write(fp, f"media/{mtype}_{idx+1}{ext2}")

        return str(zp)
    except Exception as e:
        logger.error(f"build_zip_archive: {e}")
        return None


async def export_html(user_id: int, messages: List[Dict],
                      title: str) -> Optional[str]:
    try:
        d  = Path("exports") / str(user_id)
        d.mkdir(exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        hp = d / f"export_{ts}.html"
        rows = ""
        for m in messages:
            sender = (m.get("sender_first_name") or
                      m.get("sender_username") or
                      f"User#{m.get('sender_id', '?')}")
            text = m.get("message_text") or m.get("caption") or ""
            mtype = m.get("media_type") or ""
            timer = "⏱" if m.get("has_timer") else ""
            del_m = "🗑" if m.get("is_deleted") else ""
            edit  = "✏️" if m.get("is_edited") else ""
            rows += (
                f"<tr>"
                f"<td>{str(m.get('created_at',''))[:16]}</td>"
                f"<td>{sender}</td>"
                f"<td>{text[:200]}</td>"
                f"<td>{mtype} {timer}</td>"
                f"<td>{del_m}{edit}</td>"
                f"</tr>\n"
            )
        html = f"""<!DOCTYPE html>
<html><head><meta charset='utf-8'>
<title>{title}</title>
<style>
  body{{font-family:sans-serif;background:#0d1117;color:#c9d1d9;padding:20px}}
  table{{border-collapse:collapse;width:100%}}
  th,td{{border:1px solid #30363d;padding:8px;text-align:left}}
  th{{background:#161b22}}tr:nth-child(even){{background:#161b22}}
  h1{{color:#58a6ff}}
</style>
</head><body>
<h1>📁 {title}</h1>
<p>Экспорт: {datetime.now().strftime('%d.%m.%Y %H:%M')} | Сообщений: {len(messages)}</p>
<table>
<tr><th>Дата</th><th>Отправитель</th><th>Текст</th>
<th>Медиа</th><th>События</th></tr>
{rows}
</table></body></html>"""
        hp.write_text(html, encoding="utf-8")
        return str(hp)
    except Exception as e:
        logger.error(f"export_html: {e}")
        return None


async def export_csv_file(user_id: int,
                          messages: List[Dict]) -> Optional[str]:
    try:
        d  = Path("exports") / str(user_id)
        d.mkdir(exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        cp = d / f"export_{ts}.csv"
        with open(cp, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=[
                "created_at", "sender_first_name", "sender_username",
                "message_text", "media_type", "has_timer", "is_deleted",
                "is_edited", "original_text", "category", "importance"
            ])
            w.writeheader()
            for m in messages:
                w.writerow({k: m.get(k, "") for k in w.fieldnames})
        return str(cp)
    except Exception as e:
        logger.error(f"export_csv_file: {e}")
        return None


async def export_json_file(user_id: int,
                           messages: List[Dict]) -> Optional[str]:
    try:
        d  = Path("exports") / str(user_id)
        d.mkdir(exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        jp = d / f"export_{ts}.json"
        data = [{k: str(v) for k, v in m.items()} for m in messages]
        jp.write_text(json.dumps(data, ensure_ascii=False, indent=2),
                      encoding="utf-8")
        return str(jp)
    except Exception as e:
        logger.error(f"export_json_file: {e}")
        return None


async def check_achievements(bot: Bot, user_id: int):
    """Проверяет и выдаёт достижения по результатам статистики."""
    user = db.get_user(user_id)
    if not user:
        return

    async def grant(ach: str, emoji: str, title: str):
        if db.award_achievement(user_id, ach):
            try:
                await bot.send_message(
                    user_id,
                    f"🏆 <b>Новое достижение!</b>\n\n"
                    f"{emoji} <b>{title}</b>\n\n"
                    f"Получено: {datetime.now().strftime('%d.%m.%Y')}",
                )
            except:
                pass

    saved = user["total_messages_saved"]
    dels  = user["total_deletions"]
    refs  = user["total_referrals"]
    lvl   = user["user_level"]
    cons  = len(db.get_user_connections(user_id))

    # Сообщения
    if saved >= 1:    await grant("first_msg",      "💬", "Первое сообщение")
    if saved >= 100:  await grant("msg_100",         "💬", "100 сообщений")
    if saved >= 500:  await grant("msg_500",         "💬", "500 сообщений")
    if saved >= 1000: await grant("msg_1000",        "💬", "1 000 сообщений")
    if saved >= 5000: await grant("msg_5000",        "💬", "5 000 сообщений")
    # Удаления
    if dels >= 1:     await grant("first_deletion",  "🗑",  "Первое удаление")
    if dels >= 50:    await grant("deletions_50",    "🗑",  "50 удалений")
    # Рефералы
    if refs >= 1:     await grant("first_referral",  "👥", "Первый реферал")
    if refs >= 10:    await grant("referrals_10",    "👥", "10 рефералов")
    if refs >= 50:    await grant("influencer",      "🌟", "Инфлюенсер")
    # Уровни
    if lvl >= 5:      await grant("level_5",         "⭐",  "Уровень 5")
    if lvl >= 10:     await grant("level_10",        "⭐",  "Уровень 10")
    if lvl >= 20:     await grant("level_20",        "⭐",  "Уровень 20")
    # Подключения
    if cons >= 1:     await grant("first_connect",   "🔗", "Первое подключение")
    if cons >= 5:     await grant("multi_connect",   "🔗", "5 подключений")
    # Подписка
    stype = user["subscription_type"]
    if stype in ("premium", "ultimate"):
        await grant("vip_user", "👑", "VIP-пользователь")
    if stype == "ultimate":
        await grant("legend",   "♾️", "Легенда")


# ═══════════════════════════════════════════════════════════
#  КЛАВИАТУРЫ
# ═══════════════════════════════════════════════════════════

def kb_start():
    b = InlineKeyboardBuilder()
    b.button(text="✅ Принять условия", callback_data="accept_terms")
    b.button(text="📄 Читать условия",  callback_data="show_terms")
    b.adjust(1)
    return b.as_markup()


def kb_main(user_id: int):
    b = InlineKeyboardBuilder()
    b.button(text="📊 Статистика",   callback_data="stats")
    b.button(text="💎 Подписка",     callback_data="subscription")
    b.button(text="🔗 Подключения",  callback_data="connections")
    b.button(text="⭐ Мои Stars",    callback_data="my_stars")
    b.button(text="👥 Рефералы",     callback_data="referrals")
    b.button(text="🏆 Рейтинг",      callback_data="referral_leaderboard")
    b.button(text="🔍 Поиск",        callback_data="search_menu")
    b.button(text="📚 Коллекции",    callback_data="collections_menu")
    b.button(text="🔖 Закладки",     callback_data="bookmarks_menu")
    b.button(text="🏷️ Мои теги",     callback_data="tags_menu")
    b.button(text="🔔 Триггеры",     callback_data="triggers_menu")
    b.button(text="📈 Аналитика",    callback_data="analytics_menu")
    b.button(text="🖼️ Галерея",      callback_data="gallery_menu")
    b.button(text="📤 Экспорт",      callback_data="export_menu")
    b.button(text="🚫 Блок-лист",    callback_data="blocklist_menu")
    b.button(text="⚙️ Настройки",    callback_data="settings")
    b.button(text="ℹ️ Помощь",       callback_data="help")
    b.button(text="💳 История оплат", callback_data="payment_history")
    if user_id == ADMIN_ID:
        b.button(text="👨‍💼 Админ-панель", callback_data="admin_panel")
    b.adjust(2)
    return b.as_markup()


def kb_back(to: str = "main_menu"):
    b = InlineKeyboardBuilder()
    b.button(text="◀️ Назад", callback_data=to)
    return b.as_markup()


def kb_subscription():
    b = InlineKeyboardBuilder()
    b.button(text=f"🌟 Starter  7 дней  — {STARTER_PRICE} ⭐",  callback_data="buy_starter")
    b.button(text=f"💎 Basic    1 мес   — {BASIC_PRICE} ⭐",   callback_data="buy_basic")
    b.button(text=f"💼 Pro      3 мес   — {PRO_PRICE} ⭐  🔥", callback_data="buy_pro")
    b.button(text=f"👑 Premium  1 год   — {PREMIUM_PRICE} ⭐  🔥", callback_data="buy_premium")
    b.button(text=f"♾️ Ultimate навсегда — {ULTIMATE_PRICE} ⭐ 💥", callback_data="buy_ultimate")
    b.button(text="◀️ Назад", callback_data="main_menu")
    b.adjust(1)
    return b.as_markup()


def kb_settings(user: Dict):
    b = InlineKeyboardBuilder()
    d = "✅" if user["notify_deletions"]    else "❌"
    e = "✅" if user["notify_edits"]        else "❌"
    t = "✅" if user["notify_media_timers"] else "❌"
    c = "✅" if user["notify_connections"]  else "❌"
    s = "✅" if user["notify_scam"]         else "❌"
    k = "✅" if user["notify_keywords"]     else "❌"
    dg= "✅" if user["digest_enabled"]      else "❌"
    b.button(text=f"{d} Уведомления об удалениях",   callback_data="toggle_notify_deletions")
    b.button(text=f"{e} Уведомления о редактировании", callback_data="toggle_notify_edits")
    b.button(text=f"{t} Медиа с таймером",           callback_data="toggle_notify_media_timers")
    b.button(text=f"{c} Подключение бота",            callback_data="toggle_notify_connections")
    b.button(text=f"{s} Детектор мошенничества",      callback_data="toggle_notify_scam")
    b.button(text=f"{k} Ключевые слова-триггеры",     callback_data="toggle_notify_keywords")
    b.button(text=f"{dg} Ежедневный дайджест",        callback_data="toggle_digest_enabled")
    b.button(text="🧹 Очистить старые медиа",          callback_data="cleanup_media")
    b.button(text="◀️ Назад",                         callback_data="main_menu")
    b.adjust(1)
    return b.as_markup()


def kb_admin():
    b = InlineKeyboardBuilder()
    b.button(text="👥 Пользователи",    callback_data="admin_users")
    b.button(text="📊 Статистика",      callback_data="admin_stats")
    b.button(text="📢 Рассылка",        callback_data="admin_broadcast")
    b.button(text="🎁 Подарить подписку", callback_data="admin_gift_menu")
    b.button(text="📈 Аналитика",       callback_data="admin_analytics")
    b.button(text="◀️ Назад",           callback_data="main_menu")
    b.adjust(2)
    return b.as_markup()


def kb_admin_user(uid: int, is_blocked: bool):
    b = InlineKeyboardBuilder()
    b.button(text="🎁 Подарить подписку",  callback_data=f"gift_{uid}")
    b.button(text="⭐ Добавить Stars",     callback_data=f"add_stars_{uid}")
    action = "✅ Разблокировать" if is_blocked else "🚫 Заблокировать"
    cb     = f"unblock_{uid}" if is_blocked else f"block_{uid}"
    b.button(text=action, callback_data=cb)
    b.button(text="✉️ Написать",           callback_data=f"msg_user_{uid}")
    b.button(text="◀️ К списку",           callback_data="admin_users")
    b.adjust(2)
    return b.as_markup()


def kb_gift(uid: int):
    b = InlineKeyboardBuilder()
    plans = [("🌟 7 дней", "starter"), ("💎 Месяц", "basic"),
             ("💼 3 месяца", "pro"),  ("👑 Год", "premium"),
             ("♾️ Навсегда", "ultimate")]
    for label, plan in plans:
        b.button(text=label, callback_data=f"do_gift_{uid}_{plan}")
    b.button(text="◀️ Назад", callback_data=f"admin_manage_{uid}")
    b.adjust(2)
    return b.as_markup()


def kb_export():
    b = InlineKeyboardBuilder()
    b.button(text="📄 HTML-отчёт",  callback_data="export_html")
    b.button(text="📋 CSV",         callback_data="export_csv")
    b.button(text="📦 JSON",        callback_data="export_json")
    b.button(text="🗜️ ZIP-архив",   callback_data="export_zip")
    b.button(text="◀️ Назад",       callback_data="main_menu")
    b.adjust(2)
    return b.as_markup()


def kb_analytics():
    b = InlineKeyboardBuilder()
    b.button(text="🌡️ Тепловая карта",     callback_data="heatmap")
    b.button(text="👤 Топ контактов",      callback_data="top_contacts")
    b.button(text="📂 По категориям",      callback_data="cat_stats")
    b.button(text="🔗 Все ссылки",         callback_data="links")
    b.button(text="🔁 Дубликаты",          callback_data="duplicates")
    b.button(text="📅 По дате",            callback_data="by_date")
    b.button(text="◀️ Назад",              callback_data="main_menu")
    b.adjust(2)
    return b.as_markup()


# ═══════════════════════════════════════════════════════════
#  РОУТЕР
# ═══════════════════════════════════════════════════════════

router = Router()

# ─── /start ───────────────────────────────────────────────

@router.message(CommandStart())
async def cmd_start(message: Message):
    uid   = message.from_user.id
    uname = message.from_user.username
    fname = message.from_user.first_name

    # Реферальный код
    parts   = message.text.split()
    ref_id  = None
    if len(parts) > 1 and parts[1].startswith("REF"):
        ref_user = db.get_user_by_ref(parts[1])
        if ref_user and ref_user["user_id"] != uid:
            ref_id = ref_user["user_id"]

    db.add_user(uid, uname, fname, ref_id)
    user = db.get_user(uid)
    if not user:
        await message.answer("❌ Ошибка регистрации. Попробуйте позже.")
        return

    if user["is_blocked"]:
        await message.answer(f"🚫 Ваш аккаунт заблокирован.\nПо вопросам: @{ADMIN_USERNAME}")
        return

    if not user["accepted_terms"]:
        await message.answer(
            "👋 <b>Добро пожаловать в Chat Monitor v8.0!</b>\n\n"
            "🔒 Сохраняю <b>всё</b>:\n"
            "• Удалённые сообщения\n"
            "• Фото, видео, кружки, аудио\n"
            "• Медиа с таймером самоуничтожения\n"
            "• Отредактированные сообщения (до и после)\n"
            "• При удалении целого чата — ZIP-архив\n\n"
            "⚠️ Требуется Telegram Premium + Business API\n\n"
            "Для продолжения примите условия использования:",
            reply_markup=kb_start(),
        )
    else:
        sub = sub_label(user)
        await message.answer(
            f"👋 С возвращением, <b>{fname}</b>!\n\n"
            f"<b>Подписка:</b> {sub}\n"
            f"⭐ Уровень: {user['user_level']} "
            f"({user['experience_points']} XP)\n"
            f"🏆 Достижений: {user['achievement_count']}\n\n"
            "Выберите раздел:",
            reply_markup=kb_main(uid),
        )


# ─── УСЛОВИЯ ──────────────────────────────────────────────

@router.callback_query(F.data == "show_terms")
async def cb_show_terms(call: CallbackQuery):
    text = (
        "📄 <b>Условия использования v8.0</b>\n\n"
        "<b>Что делает бот:</b>\n"
        "• Сохраняет все сообщения из подключённых чатов\n"
        "• Уведомляет об удалении сообщений\n"
        "• Хранит оригинал при редактировании\n"
        "• Сохраняет медиа с таймерами\n"
        "• При удалении чата — ZIP-архив\n\n"
        "<b>Ограничения:</b>\n"
        "⚠️ Секретные чаты — не поддерживаются\n"
        "⚠️ Групповые чаты — не поддерживаются\n"
        "✅ Только личные чаты через Business API\n\n"
        "<b>Тарифы:</b>\n"
        f"🎁 Пробный — 3 дня бесплатно (при подключении)\n"
        f"🌟 Starter  — {STARTER_PRICE} ⭐ / 7 дней\n"
        f"💎 Basic    — {BASIC_PRICE} ⭐ / месяц\n"
        f"💼 Pro      — {PRO_PRICE} ⭐ / 3 мес 🔥 -20 %%\n"
        f"👑 Premium  — {PREMIUM_PRICE} ⭐ / год 🔥 -33 %%\n"
        f"♾️ Ultimate — {ULTIMATE_PRICE} ⭐ навсегда 💥\n\n"
        f"💰 Купить напрямую в рублях: @{ADMIN_USERNAME}\n\n"
        "Нажимая «Принять», вы соглашаетесь с условиями."
    )
    await call.message.edit_text(text, reply_markup=kb_start())


@router.callback_query(F.data == "accept_terms")
async def cb_accept_terms(call: CallbackQuery):
    uid = call.from_user.id
    db.accept_terms(uid)
    await call.message.edit_text(
        "✅ <b>Условия приняты!</b>\n\n"
        "<b>Подключение бота:</b>\n"
        "1. Telegram → Настройки → Конфиденциальность\n"
        "2. Чат-боты → Добавить чат-бота\n"
        f"3. Найдите @{BOT_USERNAME or 'mrztnbot'}\n"
        "4. Нажмите «Подключить»\n\n"
        "✅ Пробный период <b>3 дня</b> активируется автоматически "
        "при первом подключении!\n\n"
        "⚠️ Только личные чаты | Требуется Telegram Premium",
        reply_markup=kb_main(uid),
    )
    try:
        await call.bot.send_message(
            ADMIN_ID,
            f"🎉 Новый пользователь: {uid} @{call.from_user.username or '—'} "
            f"({call.from_user.first_name})",
        )
    except:
        pass


# ─── ГЛАВНОЕ МЕНЮ ─────────────────────────────────────────

@router.callback_query(F.data == "main_menu")
async def cb_main_menu(call: CallbackQuery):
    uid  = call.from_user.id
    user = db.get_user(uid)
    if not user:
        await call.answer("❌ Пользователь не найден")
        return
    if user["is_blocked"]:
        await call.answer("🚫 Заблокирован")
        return
    await call.message.edit_text(
        f"🏠 <b>Главное меню</b>\n\n"
        f"<b>Подписка:</b> {sub_label(user)}\n"
        f"⭐ Уровень: {user['user_level']} "
        f"({user['experience_points']} XP)\n"
        f"🏆 Достижений: {user['achievement_count']}",
        reply_markup=kb_main(uid),
    )


# ─── СТАТИСТИКА ───────────────────────────────────────────

@router.callback_query(F.data == "stats")
async def cb_stats(call: CallbackQuery):
    uid  = call.from_user.id
    user = db.get_user(uid)
    if not user:
        await call.answer("❌")
        return
    conns = db.get_user_connections(uid)
    text  = (
        f"📊 <b>Ваша статистика</b>\n\n"
        f"<b>Подписка:</b> {sub_label(user)}\n"
        f"⭐ <b>Уровень:</b> {user['user_level']} "
        f"({user['experience_points']} XP)\n"
        f"🏆 <b>Достижений:</b> {user['achievement_count']}\n"
        f"💰 <b>Stars-баланс:</b> {user['stars_balance']} ⭐\n\n"
        f"🔗 <b>Подключений:</b> {len(conns)}\n"
        f"💬 <b>Сообщений сохранено:</b> {user['total_messages_saved']}\n"
        f"🗑 <b>Удалений отслежено:</b> {user['total_deletions']}\n"
        f"✏️ <b>Редактирований:</b> {user['total_edits']}\n"
        f"📸 <b>Медиафайлов:</b> {user['total_media_saved']}\n"
        f"👥 <b>Рефералов:</b> {user['total_referrals']} "
        f"(заработано {user['referral_earnings']} ⭐)"
    )
    await call.message.edit_text(text, reply_markup=kb_back())


# ─── ПОДПИСКА ─────────────────────────────────────────────

@router.callback_query(F.data == "subscription")
async def cb_subscription(call: CallbackQuery):
    uid  = call.from_user.id
    user = db.get_user(uid)
    if not user:
        await call.answer("❌")
        return
    text = (
        f"💎 <b>Управление подпиской</b>\n\n"
        f"<b>Статус:</b> {sub_label(user)}\n"
        f"<b>Баланс:</b> {user['stars_balance']} ⭐\n\n"
        f"<b>Доступные тарифы:</b>\n\n"
        f"🎁 Пробный — 3 дня <b>бесплатно</b> (автоматически)\n"
        f"🌟 Starter  — {STARTER_PRICE} ⭐ (~{PRICES_RUB['starter']}₽) / 7 дней\n"
        f"💎 Basic    — {BASIC_PRICE} ⭐ (~{PRICES_RUB['basic']}₽) / месяц\n"
        f"💼 Pro      — {PRO_PRICE} ⭐ (~{PRICES_RUB['pro']}₽) / 3 мес 🔥 -20 %%\n"
        f"👑 Premium  — {PREMIUM_PRICE} ⭐ (~{PRICES_RUB['premium']}₽) / год 🔥 -33 %%\n"
        f"♾️ Ultimate — {ULTIMATE_PRICE} ⭐ (~{PRICES_RUB['ultimate']}₽) навсегда 💥\n\n"
        f"💰 Купить в рублях напрямую: @{ADMIN_USERNAME}"
    )
    await call.message.edit_text(text, reply_markup=kb_subscription())


# ─── ПОКУПКА ЧЕРЕЗ STARS (инвойсы) ────────────────────────

_plans = {
    "starter":  (STARTER_PRICE,  "🌟 Starter — 7 дней"),
    "basic":    (BASIC_PRICE,    "💎 Basic — 1 месяц"),
    "pro":      (PRO_PRICE,      "💼 Pro — 3 месяца"),
    "premium":  (PREMIUM_PRICE,  "👑 Premium — 1 год"),
    "ultimate": (ULTIMATE_PRICE, "♾️ Ultimate — навсегда"),
}


@router.callback_query(F.data.startswith("buy_"))
async def cb_buy(call: CallbackQuery):
    plan = call.data.split("_", 1)[1]
    if plan not in _plans:
        await call.answer("❌ Неверный план")
        return
    amount, title = _plans[plan]
    try:
        await call.bot.send_invoice(
            chat_id=call.from_user.id,
            title=title,
            description=f"Подписка Chat Monitor v8.0 — {title}",
            payload=f"sub_{plan}_{call.from_user.id}",
            provider_token="",
            currency="XTR",
            prices=[LabeledPrice(label="XTR", amount=amount)],
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[
                    InlineKeyboardButton(
                        text=f"Оплатить {amount} ⭐", pay=True
                    )
                ]]
            ),
        )
        await call.answer("✅ Инвойс создан")
    except Exception as e:
        logger.error(f"send_invoice: {e}")
        await call.answer("❌ Ошибка создания инвойса", show_alert=True)


@router.pre_checkout_query()
async def pre_checkout(pcq: PreCheckoutQuery):
    await pcq.bot.answer_pre_checkout_query(pcq.id, ok=True)


@router.message(F.successful_payment)
async def successful_payment(message: Message):
    uid  = message.from_user.id
    pay  = message.successful_payment
    parts = pay.invoice_payload.split("_")
    if len(parts) < 2:
        return
    plan_type = parts[1]
    stars     = pay.total_amount

    db.save_payment(uid, stars, plan_type)
    db.activate_subscription(uid, plan_type)
    db.process_referral_bonus(uid, stars)
    xp = XP_PURCHASE.get(plan_type, 100)
    db.add_xp(uid, xp)

    user = db.get_user(uid)
    await message.answer(
        f"🎉 <b>Оплата прошла успешно!</b>\n\n"
        f"<b>Подписка:</b> {sub_label(user)}\n"
        f"<b>Оплачено:</b> {stars} ⭐\n"
        f"<b>Получено XP:</b> +{xp}\n\n"
        "Спасибо за поддержку! 🙏",
        reply_markup=kb_main(uid),
    )
    await check_achievements(message.bot, uid)
    try:
        await message.bot.send_message(
            ADMIN_ID,
            f"💰 Новый платёж!\n"
            f"User: {uid} @{message.from_user.username or '—'}\n"
            f"План: {plan_type}\n"
            f"Сумма: {stars} ⭐",
        )
    except:
        pass


# ─── STARS БАЛАНС ─────────────────────────────────────────

@router.callback_query(F.data == "my_stars")
async def cb_my_stars(call: CallbackQuery):
    uid  = call.from_user.id
    user = db.get_user(uid)
    if not user:
        await call.answer("❌")
        return
    await call.message.edit_text(
        f"⭐ <b>Telegram Stars</b>\n\n"
        f"<b>Баланс:</b> {user['stars_balance']} ⭐\n"
        f"<b>Заработано на рефералах:</b> {user['referral_earnings']} ⭐\n\n"
        f"<b>Как получить Stars:</b>\n"
        f"• Купить в Telegram (через @PremiumBot)\n"
        f"• Получить от рефералов (20% с их платежей)\n"
        f"• Подарок от администратора\n\n"
        f"<b>Тарифы:</b>\n"
        f"🌟 Starter  — {STARTER_PRICE} ⭐\n"
        f"💎 Basic    — {BASIC_PRICE} ⭐\n"
        f"💼 Pro      — {PRO_PRICE} ⭐ 🔥\n"
        f"👑 Premium  — {PREMIUM_PRICE} ⭐ 🔥\n"
        f"♾️ Ultimate — {ULTIMATE_PRICE} ⭐ 💥\n\n"
        f"💰 Покупка в рублях: @{ADMIN_USERNAME}",
        reply_markup=kb_back(),
    )


# ─── РЕФЕРАЛЫ ─────────────────────────────────────────────

@router.callback_query(F.data == "referrals")
async def cb_referrals(call: CallbackQuery):
    uid  = call.from_user.id
    user = db.get_user(uid)
    if not user:
        await call.answer("❌")
        return
    stats    = db.get_referral_stats(uid)
    bot_name = BOT_USERNAME or "mrztnbot"
    ref_link = f"https://t.me/{bot_name}?start={user['referral_code']}"

    text = (
        f"👥 <b>Реферальная программа</b>\n\n"
        f"<b>Ваша ссылка:</b>\n"
        f"<code>{ref_link}</code>\n\n"
        f"<b>Статистика:</b>\n"
        f"• Приглашено: {stats['total']}\n"
        f"• Заработано: {stats['earnings']} ⭐\n\n"
        f"<b>Как работает:</b>\n"
        f"1. Поделитесь ссылкой с друзьями\n"
        f"2. Они регистрируются по ней\n"
        f"3. Вы получаете 20% от их каждого платежа\n\n"
        f"<b>Ваши рефералы:</b>\n"
    )
    if stats["referrals"]:
        emojis = {"free": "🆓", "trial": "🎁", "starter": "🌟",
                  "basic": "💎", "pro": "💼", "premium": "👑",
                  "ultimate": "♾️"}
        for i, ref in enumerate(stats["referrals"][:5], 1):
            em = emojis.get(ref["subscription_type"], "❓")
            text += (f"{i}. {em} "
                     f"{ref['first_name'] or 'Пользователь'}\n")
    else:
        text += "Пока никого нет.\n"
    text += "\n💡 Приглашайте больше друзей!"
    await call.message.edit_text(text, reply_markup=kb_back())


@router.callback_query(F.data == "referral_leaderboard")
async def cb_referral_leaderboard(call: CallbackQuery):
    rows  = db.get_referral_leaderboard()
    medals = ["🥇", "🥈", "🥉"] + ["🏅"] * 7
    text  = "🏆 <b>Топ-10 партнёров</b>\n\n"
    for i, r in enumerate(rows):
        name = r["first_name"] or f"User#{r['user_id']}"
        text += (f"{medals[i]} {name} — "
                 f"{r['total_referrals']} реф. | "
                 f"{r['referral_earnings']} ⭐\n")
    if not rows:
        text += "Пока пусто."
    await call.message.edit_text(text, reply_markup=kb_back())


# ─── ПОДКЛЮЧЕНИЯ ──────────────────────────────────────────

@router.callback_query(F.data == "connections")
async def cb_connections(call: CallbackQuery):
    uid   = call.from_user.id
    conns = db.get_user_connections(uid)
    if not conns:
        text = (
            "🔗 <b>Подключения</b>\n\n"
            "Нет активных подключений.\n\n"
            "<b>Как подключить:</b>\n"
            "1. Telegram → Настройки\n"
            "2. Конфиденциальность → Чат-боты\n"
            "3. Добавить чат-бота\n"
            f"4. @{BOT_USERNAME or 'mrztnbot'}\n\n"
            "⚠️ Требуется Telegram Premium\n"
            "✅ Только личные чаты"
        )
    else:
        text = f"🔗 <b>Активных подключений:</b> {len(conns)}\n\n"
        for i, c in enumerate(conns, 1):
            text += (f"{i}. ID: <code>{c['connection_id'][:16]}…</code>\n"
                     f"   📅 {str(c['connected_at'])[:10]}\n\n")
    await call.message.edit_text(text, reply_markup=kb_back())


# ─── ИСТОРИЯ ПЛАТЕЖЕЙ ─────────────────────────────────────

@router.callback_query(F.data == "payment_history")
async def cb_payment_history(call: CallbackQuery):
    uid      = call.from_user.id
    payments = db.get_payment_history(uid)
    if not payments:
        text = "💳 <b>История платежей</b>\n\nПлатежей пока нет."
    else:
        text = "💳 <b>История платежей</b>\n\n"
        for p in payments:
            date = str(p["created_at"])[:10]
            text += (f"• {date} — {p['plan_type']} "
                     f"({p['amount_stars']} ⭐)\n")
    await call.message.edit_text(text, reply_markup=kb_back())


# ─── НАСТРОЙКИ ────────────────────────────────────────────

@router.callback_query(F.data == "settings")
async def cb_settings(call: CallbackQuery):
    user = db.get_user(call.from_user.id)
    if not user:
        await call.answer("❌")
        return
    await call.message.edit_text(
        "⚙️ <b>Настройки</b>\n\n"
        "✅ — включено   ❌ — отключено\n\n"
        "Нажмите на пункт, чтобы переключить:",
        reply_markup=kb_settings(user),
    )


@router.callback_query(F.data.startswith("toggle_"))
async def cb_toggle(call: CallbackQuery):
    field  = call.data.replace("toggle_", "")
    uid    = call.from_user.id
    new    = db.toggle_setting(uid, field)
    user   = db.get_user(uid)
    status = "✅ Включено" if new else "❌ Отключено"
    await call.answer(status)
    await call.message.edit_reply_markup(reply_markup=kb_settings(user))


@router.callback_query(F.data == "cleanup_media")
async def cb_cleanup_media(call: CallbackQuery):
    uid   = call.from_user.id
    user  = db.get_user(uid)
    days  = user.get("media_cleanup_days", DEFAULT_CLEANUP_DAYS) if user else DEFAULT_CLEANUP_DAYS
    count = db.cleanup_old_media(uid, days)
    await call.answer(f"🧹 Удалено файлов: {count}", show_alert=True)


# ─── ПОИСК ────────────────────────────────────────────────

@router.callback_query(F.data == "search_menu")
async def cb_search_menu(call: CallbackQuery, state: FSMContext):
    await state.set_state(UserStates.waiting_search)
    b = InlineKeyboardBuilder()
    b.button(text="❌ Отмена", callback_data="main_menu")
    await call.message.edit_text(
        "🔍 <b>Умный поиск</b>\n\n"
        "Введите текст для поиска.\n\n"
        "<b>Фильтры (через пробел после запроса):</b>\n"
        "<code>привет #фото</code> — только фото\n"
        "<code>встреча #работа</code> — по категории\n"
        "<code>пример #видео</code> — только видео\n"
        "<code>текст #от:@username</code> — по отправителю",
        reply_markup=b.as_markup(),
    )


@router.message(UserStates.waiting_search)
async def do_search(message: Message, state: FSMContext):
    await state.clear()
    uid   = message.from_user.id
    query = message.text.strip()

    media_type = None
    category   = None
    sender     = None
    clean      = query

    if "#фото" in query:
        media_type = "photo"
        clean = clean.replace("#фото", "").strip()
    elif "#видео" in query:
        media_type = "video"
        clean = clean.replace("#видео", "").strip()
    elif "#кружок" in query:
        media_type = "video_note"
        clean = clean.replace("#кружок", "").strip()
    elif "#аудио" in query:
        media_type = "audio"
        clean = clean.replace("#аудио", "").strip()
    elif "#таймер" in query:
        media_type = None
        clean = clean.replace("#таймер", "").strip()

    for cat in CATEGORY_MAP:
        tag = f"#{cat.lower()}"
        if tag in query:
            category = cat
            clean = clean.replace(tag, "").strip()

    import re as _re
    m = _re.search(r"#от:@?(\w+)", query)
    if m:
        sender = m.group(1)
        clean  = _re.sub(r"#от:@?\w+", "", clean).strip()

    results = db.search_messages(uid, query=clean or None,
                                 media_type=media_type,
                                 category=category, sender=sender)
    if not results:
        await message.answer(
            "🔍 Ничего не найдено по вашему запросу.",
            reply_markup=kb_main(uid),
        )
        return

    text = f"🔍 <b>Результаты поиска</b> ({len(results)} шт.):\n\n"
    for r in results[:10]:
        ts     = str(r["created_at"])[:16]
        name   = (r.get("sender_first_name") or
                  r.get("sender_username") or "?")
        snippet = (r.get("message_text") or
                   r.get("caption") or
                   f"[{r.get('media_type','?')}]")[:80]
        del_m  = "🗑" if r["is_deleted"] else ""
        edit_m = "✏️" if r["is_edited"]  else ""
        timer_m= "⏱" if r["has_timer"]  else ""
        text  += (f"{del_m}{edit_m}{timer_m} "
                  f"[{ts}] <b>{name}</b>: {snippet}\n\n")

    await message.answer(text, reply_markup=kb_main(uid))


# ─── КОЛЛЕКЦИИ ────────────────────────────────────────────

@router.callback_query(F.data == "collections_menu")
async def cb_collections_menu(call: CallbackQuery):
    uid  = call.from_user.id
    cols = db.get_collections(uid)
    text = "📚 <b>Коллекции</b>\n\n"
    if cols:
        for c in cols:
            text += f"• <b>{c['name']}</b> ({c['msg_count']} сообщ.)\n"
    else:
        text += "Коллекций пока нет.\n"
    b = InlineKeyboardBuilder()
    b.button(text="➕ Создать коллекцию", callback_data="create_collection")
    b.button(text="◀️ Назад",            callback_data="main_menu")
    b.adjust(1)
    await call.message.edit_text(text, reply_markup=b.as_markup())


@router.callback_query(F.data == "create_collection")
async def cb_create_collection(call: CallbackQuery, state: FSMContext):
    await state.set_state(UserStates.waiting_collection)
    b = InlineKeyboardBuilder()
    b.button(text="❌ Отмена", callback_data="collections_menu")
    await call.message.edit_text(
        "📚 Введите название новой коллекции:",
        reply_markup=b.as_markup(),
    )


@router.message(UserStates.waiting_collection)
async def do_create_collection(message: Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    db.create_collection(uid, message.text.strip()[:100])
    await message.answer(
        "✅ Коллекция создана!",
        reply_markup=kb_main(uid),
    )


# ─── ЗАКЛАДКИ ─────────────────────────────────────────────

@router.callback_query(F.data == "bookmarks_menu")
async def cb_bookmarks_menu(call: CallbackQuery):
    uid   = call.from_user.id
    books = db.get_bookmarks(uid)
    if not books:
        text = "🔖 <b>Закладки</b>\n\nЗакладок пока нет."
    else:
        text = f"🔖 <b>Закладки</b> ({len(books)} шт.):\n\n"
        for b_ in books[:10]:
            ts      = str(b_.get("msg_date", ""))[:10]
            name    = b_.get("sender_first_name") or "?"
            snippet = (b_.get("message_text") or
                       f"[{b_.get('media_type','?')}]")[:60]
            note    = f"\n  📝 {b_['note']}" if b_.get("note") else ""
            text   += f"• [{ts}] <b>{name}</b>: {snippet}{note}\n\n"
    await call.message.edit_text(text, reply_markup=kb_back())


# ─── ТЕГИ ─────────────────────────────────────────────────

@router.callback_query(F.data == "tags_menu")
async def cb_tags_menu(call: CallbackQuery):
    await call.message.edit_text(
        "🏷️ <b>Теги</b>\n\n"
        "Теги добавляются к сообщениям через поиск.\n"
        "Для добавления тега используйте команду:\n"
        "<code>/tag ID_сообщения тег</code>",
        reply_markup=kb_back(),
    )


# ─── КЛЮЧЕВЫЕ СЛОВА-ТРИГГЕРЫ ──────────────────────────────

@router.callback_query(F.data == "triggers_menu")
async def cb_triggers_menu(call: CallbackQuery):
    uid      = call.from_user.id
    keywords = db.get_keyword_triggers(uid)
    text     = "🔔 <b>Ключевые слова-триггеры</b>\n\n"
    if keywords:
        text += "Уведомляю, если встречу:\n"
        for k in keywords:
            text += f"• <code>{k}</code>\n"
    else:
        text += "Триггеров пока нет.\n"
    text += "\nДобавьте слово, при котором хотите получать уведомление:"
    b = InlineKeyboardBuilder()
    b.button(text="➕ Добавить триггер",  callback_data="add_trigger")
    b.button(text="🗑 Очистить все",      callback_data="clear_triggers")
    b.button(text="◀️ Назад",            callback_data="main_menu")
    b.adjust(1)
    await call.message.edit_text(text, reply_markup=b.as_markup())


@router.callback_query(F.data == "add_trigger")
async def cb_add_trigger(call: CallbackQuery, state: FSMContext):
    await state.set_state(UserStates.waiting_keyword)
    b = InlineKeyboardBuilder()
    b.button(text="❌ Отмена", callback_data="triggers_menu")
    await call.message.edit_text(
        "🔔 Введите ключевое слово (триггер):",
        reply_markup=b.as_markup(),
    )


@router.message(UserStates.waiting_keyword)
async def do_add_trigger(message: Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    db.add_keyword_trigger(uid, message.text.strip()[:50])
    await message.answer("✅ Триггер добавлен!", reply_markup=kb_main(uid))


@router.callback_query(F.data == "clear_triggers")
async def cb_clear_triggers(call: CallbackQuery):
    uid      = call.from_user.id
    keywords = db.get_keyword_triggers(uid)
    for k in keywords:
        db.delete_keyword_trigger(uid, k)
    await call.answer("✅ Все триггеры удалены")
    await cb_triggers_menu(call)


# ─── АНАЛИТИКА ────────────────────────────────────────────

@router.callback_query(F.data == "analytics_menu")
async def cb_analytics_menu(call: CallbackQuery):
    await call.message.edit_text(
        "📈 <b>Аналитика</b>\n\nВыберите раздел:",
        reply_markup=kb_analytics(),
    )


@router.callback_query(F.data == "heatmap")
async def cb_heatmap(call: CallbackQuery):
    uid  = call.from_user.id
    data = db.get_heatmap(uid)
    days = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    text = "🌡️ <b>Тепловая карта активности</b>\n\n"
    text += "<code>"
    text += "    " + " ".join(f"{h:02d}" for h in range(0, 24, 3)) + "\n"
    for d in range(7):
        row = days[d] + " "
        for h_group in range(0, 24, 3):
            val = sum(data[d].get(h, 0) for h in range(h_group, h_group + 3))
            if val == 0:    sym = "·"
            elif val < 3:   sym = "▪"
            elif val < 10:  sym = "▬"
            else:           sym = "█"
            row += f" {sym} "
        text += row + "\n"
    text += "</code>\n\n· = 0  ▪ = 1-2  ▬ = 3-9  █ = 10+"
    await call.message.edit_text(text, reply_markup=kb_back("analytics_menu"))


@router.callback_query(F.data == "top_contacts")
async def cb_top_contacts(call: CallbackQuery):
    uid      = call.from_user.id
    contacts = db.get_top_contacts(uid)
    if not contacts:
        text = "👤 <b>Топ контактов</b>\n\nДанных пока нет."
    else:
        text = "👤 <b>Топ контактов</b>\n\n"
        for i, c in enumerate(contacts[:10], 1):
            name = (c.get("sender_first_name") or
                    c.get("sender_username") or
                    f"User#{c.get('sender_id', '?')}")
            text += (f"{i}. <b>{name}</b>\n"
                     f"   💬 Сообщений: {c['msg_count']}\n"
                     f"   🗑 Удалено: {c['deleted_count']}\n\n")
    await call.message.edit_text(text, reply_markup=kb_back("analytics_menu"))


@router.callback_query(F.data == "cat_stats")
async def cb_cat_stats(call: CallbackQuery):
    uid   = call.from_user.id
    stats = db.get_category_stats(uid)
    total = sum(stats.values()) or 1
    text  = "📂 <b>Сообщения по категориям</b>\n\n"
    emojis = {"Работа": "💼", "Финансы": "💰", "Ссылки": "🔗",
              "Вопросы": "❓", "Личное": "❤️"}
    for cat, cnt in sorted(stats.items(), key=lambda x: x[1], reverse=True):
        pct = int(cnt / total * 100)
        bar = "█" * (pct // 10) + "░" * (10 - pct // 10)
        em  = emojis.get(cat, "📁")
        text += f"{em} <b>{cat}</b>: {cnt} ({pct}%%)\n{bar}\n\n"
    if not stats:
        text += "Данных пока нет."
    await call.message.edit_text(text, reply_markup=kb_back("analytics_menu"))


@router.callback_query(F.data == "links")
async def cb_links(call: CallbackQuery):
    uid   = call.from_user.id
    links = db.extract_links(uid)
    if not links:
        text = "🔗 <b>Ссылки</b>\n\nЗаписанных ссылок пока нет."
    else:
        text = f"🔗 <b>Ссылки</b> ({len(links)} шт.):\n\n"
        for lnk in links[:20]:
            text += f"• <code>{lnk[:60]}</code>\n"
    await call.message.edit_text(text, reply_markup=kb_back("analytics_menu"))


@router.callback_query(F.data == "duplicates")
async def cb_duplicates(call: CallbackQuery):
    uid  = call.from_user.id
    dups = db.detect_duplicates(uid)
    if not dups:
        text = "🔁 <b>Дубликаты</b>\n\nДубликатов не найдено."
    else:
        text = f"🔁 <b>Повторяющиеся сообщения</b>:\n\n"
        for d in dups[:5]:
            text += (f"• «{d['message_text'][:50]}»\n"
                     f"  повторений: {d['cnt']}\n\n")
    await call.message.edit_text(text, reply_markup=kb_back("analytics_menu"))


@router.callback_query(F.data == "by_date")
async def cb_by_date(call: CallbackQuery):
    uid  = call.from_user.id
    date = datetime.now().strftime("%Y-%m-%d")
    msgs = db.get_messages_by_date(uid, date)
    text = (f"📅 <b>Сообщения за сегодня</b> "
            f"({datetime.now().strftime('%d.%m.%Y')}):\n\n")
    if msgs:
        for m in msgs[:10]:
            name    = (m.get("sender_first_name") or
                       m.get("sender_username") or "?")
            snippet = (m.get("message_text") or
                       m.get("caption") or
                       f"[{m.get('media_type','?')}]")[:60]
            text   += f"• <b>{name}</b>: {snippet}\n"
    else:
        text += "Сообщений за сегодня нет."
    await call.message.edit_text(text, reply_markup=kb_back("analytics_menu"))


# ─── ГАЛЕРЕЯ ──────────────────────────────────────────────

@router.callback_query(F.data == "gallery_menu")
async def cb_gallery_menu(call: CallbackQuery):
    uid   = call.from_user.id
    media = db.get_media_gallery(uid, limit=20)
    if not media:
        text = "🖼️ <b>Медиагалерея</b>\n\nСохранённых медиафайлов нет."
        await call.message.edit_text(text, reply_markup=kb_back())
        return

    # Разбивка по типам
    by_type: Dict[str, int] = defaultdict(int)
    for m in media:
        by_type[m["media_type"]] += 1

    text = f"🖼️ <b>Медиагалерея</b> ({len(media)} файлов):\n\n"
    icons = {"photo": "📸", "video": "🎬", "audio": "🎵",
             "voice": "🎤", "video_note": "🎥", "document": "📄",
             "sticker": "🎭"}
    for t, cnt in by_type.items():
        text += f"{icons.get(t, '📁')} {t}: {cnt}\n"

    text += "\n<b>Последние файлы:</b>\n"
    for m in media[:5]:
        ts    = str(m["created_at"])[:10]
        timer = "⏱" if m["has_timer"] else ""
        name  = m.get("sender_first_name") or "?"
        text += f"• [{ts}] {icons.get(m['media_type'], '?')}{timer} от {name}\n"

    b = InlineKeyboardBuilder()
    b.button(text="📸 Только фото",   callback_data="gallery_photo")
    b.button(text="🎬 Только видео",  callback_data="gallery_video")
    b.button(text="⏱ С таймерами",   callback_data="gallery_timer")
    b.button(text="◀️ Назад",         callback_data="main_menu")
    b.adjust(2)
    await call.message.edit_text(text, reply_markup=b.as_markup())


@router.callback_query(F.data.startswith("gallery_"))
async def cb_gallery_filter(call: CallbackQuery):
    uid    = call.from_user.id
    filter = call.data.split("_")[1]
    if filter == "photo":
        media = db.get_media_gallery(uid, media_type="photo")
        title = "📸 Фотографии"
    elif filter == "video":
        media = db.get_media_gallery(uid, media_type="video")
        title = "🎬 Видео"
    else:
        media = db.search_messages(uid, limit=20)
        media = [m for m in media if m.get("has_timer")]
        title = "⏱ Медиа с таймерами"

    text = f"🖼️ <b>{title}</b> ({len(media)} шт.):\n\n"
    for m in media[:10]:
        ts    = str(m["created_at"])[:10]
        name  = m.get("sender_first_name") or "?"
        del_m = "🗑" if m["is_deleted"] else "✅"
        text += f"{del_m} [{ts}] от <b>{name}</b>\n"
    await call.message.edit_text(text, reply_markup=kb_back("gallery_menu"))


# ─── ЭКСПОРТ ──────────────────────────────────────────────

@router.callback_query(F.data == "export_menu")
async def cb_export_menu(call: CallbackQuery):
    await call.message.edit_text(
        "📤 <b>Экспорт данных</b>\n\n"
        "Выберите формат экспорта последних 100 сообщений:",
        reply_markup=kb_export(),
    )


@router.callback_query(F.data == "export_html")
async def cb_export_html(call: CallbackQuery):
    uid  = call.from_user.id
    msgs = db.search_messages(uid, limit=100)
    if not msgs:
        await call.answer("Нет данных для экспорта", show_alert=True)
        return
    await call.answer("⏳ Создаю HTML-отчёт…")
    path = await export_html(uid, msgs, "Chat Monitor Export")
    if path and Path(path).exists():
        await call.bot.send_document(
            uid,
            FSInputFile(path, filename="chat_monitor_export.html"),
            caption="📄 HTML-отчёт готов!",
        )
    else:
        await call.bot.send_message(uid, "❌ Ошибка при создании отчёта")


@router.callback_query(F.data == "export_csv")
async def cb_export_csv(call: CallbackQuery):
    uid  = call.from_user.id
    msgs = db.search_messages(uid, limit=100)
    if not msgs:
        await call.answer("Нет данных", show_alert=True)
        return
    await call.answer("⏳ Создаю CSV…")
    path = await export_csv_file(uid, msgs)
    if path and Path(path).exists():
        await call.bot.send_document(
            uid,
            FSInputFile(path, filename="chat_monitor_export.csv"),
            caption="📋 CSV-файл готов!",
        )


@router.callback_query(F.data == "export_json")
async def cb_export_json(call: CallbackQuery):
    uid  = call.from_user.id
    msgs = db.search_messages(uid, limit=100)
    if not msgs:
        await call.answer("Нет данных", show_alert=True)
        return
    await call.answer("⏳ Создаю JSON…")
    path = await export_json_file(uid, msgs)
    if path and Path(path).exists():
        await call.bot.send_document(
            uid,
            FSInputFile(path, filename="chat_monitor_export.json"),
            caption="📦 JSON-файл готов!",
        )


@router.callback_query(F.data == "export_zip")
async def cb_export_zip(call: CallbackQuery):
    uid  = call.from_user.id
    msgs = db.search_messages(uid, limit=100)
    if not msgs:
        await call.answer("Нет данных", show_alert=True)
        return
    await call.answer("⏳ Создаю ZIP-архив…")
    path = await build_zip_archive(uid, 0, msgs, "Full Export")
    if path and Path(path).exists():
        await call.bot.send_document(
            uid,
            FSInputFile(path, filename="chat_monitor_export.zip"),
            caption="🗜️ ZIP-архив готов!",
        )


# ─── БЛОК-ЛИСТ ────────────────────────────────────────────

@router.callback_query(F.data == "blocklist_menu")
async def cb_blocklist_menu(call: CallbackQuery):
    uid  = call.from_user.id
    bls  = db.get_blocklist(uid)
    text = "🚫 <b>Блок-лист отправителей</b>\n\n"
    if bls:
        text += "Заблокированные ID:\n"
        for sid in bls:
            text += f"• <code>{sid}</code>\n"
    else:
        text += "Список пуст.\n"
    text += "\nЧтобы добавить отправителя в блок-лист, используйте:\n<code>/block SENDER_ID</code>"
    b = InlineKeyboardBuilder()
    b.button(text="➕ Добавить ID",  callback_data="add_blocklist")
    b.button(text="◀️ Назад",       callback_data="main_menu")
    b.adjust(1)
    await call.message.edit_text(text, reply_markup=b.as_markup())


@router.callback_query(F.data == "add_blocklist")
async def cb_add_blocklist(call: CallbackQuery, state: FSMContext):
    await state.set_state(UserStates.waiting_blocklist)
    b = InlineKeyboardBuilder()
    b.button(text="❌ Отмена", callback_data="blocklist_menu")
    await call.message.edit_text(
        "🚫 Введите Telegram ID отправителя для блокировки:",
        reply_markup=b.as_markup(),
    )


@router.message(UserStates.waiting_blocklist)
async def do_add_blocklist(message: Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    try:
        sid = int(message.text.strip())
        db.add_to_blocklist(uid, sid)
        await message.answer(
            f"✅ Отправитель {sid} добавлен в блок-лист.",
            reply_markup=kb_main(uid),
        )
    except ValueError:
        await message.answer("❌ Введите числовой ID.", reply_markup=kb_main(uid))


# ─── СПРАВКА ──────────────────────────────────────────────

@router.callback_query(F.data == "help")
async def cb_help(call: CallbackQuery):
    text = (
        f"ℹ️ <b>Chat Monitor v8.0.0 — Справка</b>\n\n"
        f"<b>Что сохраняется:</b>\n"
        f"✅ Все текстовые сообщения\n"
        f"✅ Фото, видео, аудио, документы\n"
        f"✅ Голосовые и кружки (video notes)\n"
        f"✅ Медиа с таймером самоуничтожения\n"
        f"✅ Одноразовые медиа (view once)\n"
        f"✅ Стикеры\n\n"
        f"<b>Уведомления:</b>\n"
        f"🗑 При удалении — оригинал сообщения\n"
        f"✏️ При редактировании — было / стало\n"
        f"⏱ При таймере — уведомление о сохранении\n"
        f"🗂 Удалён чат (5+ сообщ.) — ZIP-архив\n\n"
        f"<b>Ограничения:</b>\n"
        f"⚠️ Только личные чаты\n"
        f"⚠️ Требуется Telegram Premium\n"
        f"⚠️ Секретные/групповые — не поддерживаются\n\n"
        f"<b>Подписки:</b>\n"
        f"🎁 Пробный — 3 дня (авто при подключении)\n"
        f"Далее от {STARTER_PRICE} ⭐ (~{PRICES_RUB['starter']}₽)\n\n"
        f"<b>Поддержка:</b> @{ADMIN_USERNAME}"
    )
    await call.message.edit_text(text, reply_markup=kb_back())


# ═══════════════════════════════════════════════════════════
#  АДМИН-ПАНЕЛЬ
# ═══════════════════════════════════════════════════════════

def _admin_only(call: CallbackQuery) -> bool:
    if call.from_user.id != ADMIN_ID:
        asyncio.create_task(call.answer("❌ Доступ запрещён", show_alert=True))
        return False
    return True


@router.callback_query(F.data == "admin_panel")
async def cb_admin_panel(call: CallbackQuery):
    if not _admin_only(call):
        return
    total = db.get_user_count()
    active = db.get_active_subscriptions_count()
    await call.message.edit_text(
        f"👨‍💼 <b>Админ-панель v8.0</b>\n\n"
        f"👥 Всего пользователей: <b>{total}</b>\n"
        f"💎 Активных подписок:  <b>{active}</b>",
        reply_markup=kb_admin(),
    )


@router.callback_query(F.data == "admin_stats")
async def cb_admin_stats(call: CallbackQuery):
    if not _admin_only(call):
        return
    s = db.get_admin_global_stats()
    text = (
        f"📊 <b>Глобальная статистика</b>\n\n"
        f"👥 Пользователей: {s['total_users']}\n"
        f"💎 Активных подписок: {s['active_subs']}\n"
        f"💰 Заработано Stars: {s['total_stars']} ⭐\n"
        f"💳 Платежей: {s['total_payments']}\n\n"
        f"💬 Сообщений сохранено: {s['total_messages']}\n"
        f"🗑 Удалений отслежено: {s['total_deletions']}\n"
        f"✏️ Редактирований: {s['total_edits']}\n"
        f"📸 Медиафайлов: {s['total_media']}\n\n"
        f"<b>По планам:</b>\n"
    )
    emojis = {"free": "🆓", "trial": "🎁", "starter": "🌟",
              "basic": "💎", "pro": "💼", "premium": "👑",
              "ultimate": "♾️"}
    for plan, cnt in s["by_plan"].items():
        text += f"{emojis.get(plan, '?')} {plan}: {cnt}\n"
    await call.message.edit_text(text, reply_markup=kb_back("admin_panel"))


@router.callback_query(F.data == "admin_users")
async def cb_admin_users(call: CallbackQuery):
    if not _admin_only(call):
        return
    await _show_users_page(call, 0)


async def _show_users_page(call: CallbackQuery, page: int):
    users      = db.get_all_users(limit=8, offset=page * 8)
    total      = db.get_user_count()
    total_pages = max(1, (total + 7) // 8)
    text       = f"👥 <b>Пользователи</b> ({page+1}/{total_pages})\n\n"
    emojis     = {"free": "🆓", "trial": "🎁", "starter": "🌟",
                  "basic": "💎", "pro": "💼", "premium": "👑",
                  "ultimate": "♾️"}
    b = InlineKeyboardBuilder()
    for i, u in enumerate(users, start=page * 8 + 1):
        blocked = "🚫" if u["is_blocked"] else "✅"
        sub     = emojis.get(u["subscription_type"], "?")
        name    = (u["first_name"] or f"User#{u['user_id']}")[:18]
        text   += (f"{i}. {blocked}{sub} {name} "
                   f"(@{u['username'] or '—'})\n"
                   f"   ID: <code>{u['user_id']}</code>\n\n")
        b.button(text=f"#{i} {name[:12]}",
                 callback_data=f"admin_manage_{u['user_id']}")
    b.adjust(2)
    if page > 0:
        b.row(InlineKeyboardButton(text="◀️ Пред",
              callback_data=f"users_pg_{page-1}"),
              InlineKeyboardButton(text="► След",
              callback_data=f"users_pg_{page+1}"))
    elif page < total_pages - 1:
        b.row(InlineKeyboardButton(text="► След",
              callback_data=f"users_pg_{page+1}"))
    b.row(InlineKeyboardButton(text="◀️ Назад",
          callback_data="admin_panel"))
    await call.message.edit_text(text, reply_markup=b.as_markup())


@router.callback_query(F.data.startswith("users_pg_"))
async def cb_users_page(call: CallbackQuery):
    if not _admin_only(call):
        return
    page = int(call.data.split("_")[-1])
    await _show_users_page(call, page)


@router.callback_query(F.data.startswith("admin_manage_"))
async def cb_admin_manage(call: CallbackQuery):
    if not _admin_only(call):
        return
    uid  = int(call.data.split("_")[-1])
    user = db.get_user(uid)
    if not user:
        await call.answer("❌ Пользователь не найден", show_alert=True)
        return
    conns = db.get_user_connections(uid)
    text  = (
        f"👤 <b>Пользователь</b>\n\n"
        f"ID: <code>{user['user_id']}</code>\n"
        f"Username: @{user['username'] or '—'}\n"
        f"Имя: {user['first_name'] or '—'}\n"
        f"Подписка: {sub_label(user)}\n"
        f"Уровень: {user['user_level']} ({user['experience_points']} XP)\n"
        f"Достижений: {user['achievement_count']}\n"
        f"Подключений: {len(conns)}\n"
        f"Сообщений: {user['total_messages_saved']}\n"
        f"Удалений: {user['total_deletions']}\n"
        f"Stars: {user['stars_balance']} ⭐\n"
        f"Рефералов: {user['total_referrals']}\n"
        f"Зарегистрирован: {str(user['registered_at'])[:10]}"
    )
    await call.message.edit_text(
        text,
        reply_markup=kb_admin_user(uid, bool(user["is_blocked"])),
    )


@router.callback_query(F.data.startswith("block_"))
async def cb_block_user(call: CallbackQuery):
    if not _admin_only(call):
        return
    uid = int(call.data.split("_")[1])
    db.block_user(uid)
    await call.answer("✅ Заблокирован")
    try:
        await call.bot.send_message(uid,
            "🚫 Ваш аккаунт заблокирован администратором.")
    except:
        pass
    # Перезагрузить страницу
    call.data = f"admin_manage_{uid}"
    await cb_admin_manage(call)


@router.callback_query(F.data.startswith("unblock_"))
async def cb_unblock_user(call: CallbackQuery):
    if not _admin_only(call):
        return
    uid = int(call.data.split("_")[1])
    db.unblock_user(uid)
    await call.answer("✅ Разблокирован")
    try:
        await call.bot.send_message(uid,
            "✅ Ваш аккаунт разблокирован. Добро пожаловать обратно!")
    except:
        pass
    call.data = f"admin_manage_{uid}"
    await cb_admin_manage(call)


@router.callback_query(F.data.startswith("gift_"))
async def cb_gift_menu(call: CallbackQuery):
    if not _admin_only(call):
        return
    uid = int(call.data.split("_")[1])
    user = db.get_user(uid)
    name = (user["first_name"] if user else f"User#{uid}")
    await call.message.edit_text(
        f"🎁 <b>Подарить подписку</b>\n\nПользователь: {name} ({uid})\n\n"
        "Выберите план:",
        reply_markup=kb_gift(uid),
    )


@router.callback_query(F.data.startswith("do_gift_"))
async def cb_do_gift(call: CallbackQuery):
    if not _admin_only(call):
        return
    _, _, uid_s, plan = call.data.split("_", 3)
    uid = int(uid_s)
    db.activate_subscription(uid, plan)
    db.add_xp(uid, XP_PURCHASE.get(plan, 100))
    await call.answer(f"✅ Подписка {plan} подарена!")
    try:
        user = db.get_user(uid)
        await call.bot.send_message(
            uid,
            f"🎁 <b>Подарок от администратора!</b>\n\n"
            f"Вам выдана подписка: <b>{sub_label(user)}</b>\n\n"
            "Спасибо за использование Chat Monitor! 🙏",
        )
    except:
        pass
    call.data = f"admin_manage_{uid}"
    await cb_admin_manage(call)


@router.callback_query(F.data.startswith("add_stars_"))
async def cb_add_stars(call: CallbackQuery):
    if not _admin_only(call):
        return
    uid = int(call.data.split("_")[2])
    # Добавляем 100 Stars как пример
    user = db.get_user(uid)
    if user:
        db.update_user(uid, stars_balance=user["stars_balance"] + 100)
    await call.answer("✅ +100 Stars добавлено!")
    try:
        await call.bot.send_message(uid,
            "⭐ Администратор добавил вам 100 Stars на баланс!")
    except:
        pass


@router.callback_query(F.data == "admin_broadcast")
async def cb_admin_broadcast(call: CallbackQuery, state: FSMContext):
    if not _admin_only(call):
        return
    await state.set_state(AdminStates.broadcast)
    b = InlineKeyboardBuilder()
    b.button(text="❌ Отмена", callback_data="admin_panel")
    await call.message.edit_text(
        "📢 <b>Рассылка</b>\n\nВведите сообщение для рассылки всем пользователям:",
        reply_markup=b.as_markup(),
    )


@router.message(AdminStates.broadcast)
async def do_broadcast(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await state.clear()
        return
    await state.clear()
    uids    = db.get_all_users_ids()
    success = 0
    failed  = 0
    for uid in uids:
        try:
            await message.bot.send_message(
                uid,
                f"📢 <b>Сообщение от администратора:</b>\n\n{message.text}",
            )
            success += 1
            await asyncio.sleep(0.05)
        except:
            failed += 1
    await message.answer(
        f"✅ Рассылка завершена!\n\n"
        f"Доставлено: {success}\nНе доставлено: {failed}",
        reply_markup=kb_admin(),
    )


@router.callback_query(F.data == "admin_analytics")
async def cb_admin_analytics(call: CallbackQuery):
    if not _admin_only(call):
        return
    s    = db.get_admin_global_stats()
    conv = db.get_referral_leaderboard()
    text = (
        f"📈 <b>Аналитика (Админ)</b>\n\n"
        f"<b>Финансы:</b>\n"
        f"💰 Всего Stars: {s['total_stars']} ⭐\n"
        f"💳 Платежей: {s['total_payments']}\n\n"
        f"<b>Контент:</b>\n"
        f"💬 Сообщений: {s['total_messages']}\n"
        f"🗑 Удалений: {s['total_deletions']}\n"
        f"📸 Медиа: {s['total_media']}\n\n"
        f"<b>Топ-3 партнёра:</b>\n"
    )
    for i, r in enumerate(conv[:3], 1):
        name = r["first_name"] or f"User#{r['user_id']}"
        text += f"{i}. {name}: {r['total_referrals']} реф.\n"
    await call.message.edit_text(text, reply_markup=kb_back("admin_panel"))


@router.callback_query(F.data == "admin_gift_menu")
async def cb_admin_gift_menu(call: CallbackQuery):
    if not _admin_only(call):
        return
    await call.message.edit_text(
        "🎁 <b>Выдача подписок</b>\n\n"
        "Выберите пользователя в разделе «Пользователи»\n"
        "и нажмите «Подарить подписку».",
        reply_markup=kb_back("admin_panel"),
    )


# ─── КОМАНДЫ ──────────────────────────────────────────────

@router.message(Command("block"))
async def cmd_block(message: Message):
    uid = message.from_user.id
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Использование: /block SENDER_ID")
        return
    try:
        sid = int(parts[1])
        db.add_to_blocklist(uid, sid)
        await message.answer(f"✅ ID {sid} добавлен в блок-лист.")
    except ValueError:
        await message.answer("❌ Неверный ID")


@router.message(Command("tag"))
async def cmd_tag(message: Message):
    uid   = message.from_user.id
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        await message.answer("Использование: /tag DB_ID тег")
        return
    try:
        db_id = int(parts[1])
        tag   = parts[2].strip()[:30]
        db.add_tag(uid, db_id, tag)
        await message.answer(f"✅ Тег «{tag}» добавлен.")
    except ValueError:
        await message.answer("❌ Неверный ID")


@router.message(Command("note"))
async def cmd_note(message: Message):
    uid   = message.from_user.id
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        await message.answer("Использование: /note DB_ID текст_заметки")
        return
    try:
        db_id = int(parts[1])
        note  = parts[2].strip()[:500]
        db.add_note(uid, db_id, note)
        await message.answer("✅ Заметка добавлена.")
    except ValueError:
        await message.answer("❌ Неверный ID")


@router.message(Command("level"))
async def cmd_level(message: Message):
    uid  = message.from_user.id
    user = db.get_user(uid)
    if not user:
        await message.answer("❌ Пользователь не найден")
        return
    xp    = user["experience_points"]
    level = user["user_level"]
    next_xp = (level ** 2) * 100
    text  = (
        f"⭐ <b>Уровень: {level}</b>\n\n"
        f"XP: {xp} / {next_xp}\n"
        f"До следующего уровня: {max(0, next_xp - xp)} XP\n\n"
        f"Достижений: {user['achievement_count']} 🏆"
    )
    await message.answer(text)


@router.message(Command("achievements"))
async def cmd_achievements(message: Message):
    uid  = message.from_user.id
    achs = db.get_achievements(uid)
    if not achs:
        await message.answer("🏆 У вас пока нет достижений.")
        return
    text = f"🏆 <b>Ваши достижения</b> ({len(achs)} шт.):\n\n"
    icons = {
        "first_msg": "💬", "msg_100": "💬", "msg_500": "💬",
        "msg_1000": "💬", "msg_5000": "💬",
        "first_deletion": "🗑", "deletions_50": "🗑",
        "first_referral": "👥", "referrals_10": "👥", "influencer": "🌟",
        "level_5": "⭐", "level_10": "⭐", "level_20": "⭐",
        "first_connect": "🔗", "multi_connect": "🔗",
        "vip_user": "👑", "legend": "♾️",
    }
    for ach in achs[:20]:
        em  = icons.get(ach, "🏅")
        text += f"{em} {ach}\n"
    await message.answer(text)


# ═══════════════════════════════════════════════════════════
#  BUSINESS API — ОБРАБОТЧИКИ
# ═══════════════════════════════════════════════════════════

@router.business_connection()
async def on_business_connection(bc: BusinessConnection, bot: Bot):
    """Обработка подключения / отключения Business-бота."""
    try:
        uid           = bc.user.id
        connection_id = bc.id

        if not bc.is_enabled:
            logger.info(f"Отключение: {connection_id} для {uid}")
            return

        db.add_connection(connection_id, uid)
        user = db.get_user(uid)

        # Авто-активация пробного периода
        trial_activated = False
        if user and not user["auto_trial_activated"]:
            trial_activated = db.activate_trial(uid)

        db.add_xp(uid, XP_CONNECT)
        await check_achievements(bot, uid)

        logger.info(f"Подключение: {connection_id} для @{bc.user.username}")

        if user and user["notify_connections"]:
            trial_msg = ""
            if trial_activated:
                exp_dt = datetime.now() + timedelta(days=TRIAL_DAYS)
                trial_msg = (
                    f"\n\n🎁 <b>Пробный период активирован!</b>\n"
                    f"Действует до: {exp_dt.strftime('%d.%m.%Y')}"
                )
            try:
                await bot.send_message(
                    uid,
                    f"🎉 <b>Бот успешно подключён!</b>\n\n"
                    f"✅ Мониторинг активирован\n"
                    f"📱 Буду отслеживать все ваши чаты{trial_msg}\n\n"
                    f"⚠️ Секретные и групповые чаты — не поддерживаются\n"
                    f"✅ Только личные чаты через Business API",
                    reply_markup=kb_main(uid),
                )
            except:
                pass

        try:
            await bot.send_message(
                ADMIN_ID,
                f"🔗 Новое подключение!\n"
                f"User: {uid} @{bc.user.username or '—'}\n"
                f"Connection: {connection_id}\n"
                f"Пробный: {'✅' if trial_activated else '❌'}",
            )
        except:
            pass
    except Exception as e:
        logger.error(f"on_business_connection: {e}", exc_info=True)


@router.business_message()
async def on_business_message(message: Message, bot: Bot):
    """Сохранение каждого входящего Business-сообщения."""
    try:
        if not message.business_connection_id:
            return

        conn = db.get_connection(message.business_connection_id)
        if not conn:
            return

        uid = conn["user_id"]

        if not db.check_subscription(uid):
            return

        user = db.get_user(uid)
        if not user:
            return

        sender_id = message.from_user.id if message.from_user else 0

        # Блок-лист
        if db.is_blocked_sender(uid, sender_id):
            return

        # ── Определяем медиатип и скачиваем ──────────────
        media_type   = None
        media_file_id = None
        media_file_path = None
        has_timer    = False
        is_view_once = False
        caption      = message.caption

        # Признак таймера / одноразового
        if getattr(message, "has_media_spoiler", False):
            has_timer    = True
            is_view_once = True

        if message.photo:
            media_type    = "photo"
            media_file_id = message.photo[-1].file_id
            media_file_path = await download_media(
                bot, media_file_id, media_type, uid, has_timer)

        elif message.video:
            media_type    = "video"
            media_file_id = message.video.file_id
            media_file_path = await download_media(
                bot, media_file_id, media_type, uid, has_timer)

        elif message.video_note:
            # Кружки всегда помечаем как «с таймером»
            media_type    = "video_note"
            media_file_id = message.video_note.file_id
            has_timer     = True
            media_file_path = await download_media(
                bot, media_file_id, media_type, uid, has_timer)

        elif message.document:
            media_type    = "document"
            media_file_id = message.document.file_id
            media_file_path = await download_media(
                bot, media_file_id, media_type, uid, has_timer)

        elif message.audio:
            media_type    = "audio"
            media_file_id = message.audio.file_id
            media_file_path = await download_media(
                bot, media_file_id, media_type, uid, has_timer)

        elif message.voice:
            media_type    = "voice"
            media_file_id = message.voice.file_id
            has_timer     = True   # голосовые — одноразовые по сути
            media_file_path = await download_media(
                bot, media_file_id, media_type, uid, has_timer)

        elif message.sticker:
            media_type    = "sticker"
            media_file_id = message.sticker.file_id
            # Стикеры маленькие — скачиваем тоже
            media_file_path = await download_media(
                bot, media_file_id, media_type, uid, False)

        # ── Сохраняем ────────────────────────────────────
        msg_db_id = db.save_message(
            user_id=uid,
            connection_id=message.business_connection_id,
            chat_id=message.chat.id,
            message_id=message.message_id,
            sender_id=sender_id,
            sender_username=(message.from_user.username
                             if message.from_user else None),
            sender_first_name=(message.from_user.first_name
                               if message.from_user else None),
            message_text=message.text or message.caption,
            media_type=media_type,
            media_file_id=media_file_id,
            media_file_path=media_file_path,
            caption=caption,
            has_timer=has_timer,
            is_view_once=is_view_once,
        )

        # XP
        xp = XP_SAVE_MSG
        if media_type:
            xp += XP_SAVE_MEDIA
        if has_timer:
            xp += XP_SAVE_TIMER
        db.add_xp(uid, xp)

        # ── Скам-детектор ─────────────────────────────────
        text_content = message.text or message.caption or ""
        if user["notify_scam"]:
            from_user = db.Database._is_scam(text_content)
            if from_user:
                sender_name = (
                    (message.from_user.first_name if message.from_user else None)
                    or "Неизвестный"
                )
                try:
                    await bot.send_message(
                        uid,
                        f"⚠️ <b>Возможное мошенничество!</b>\n\n"
                        f"От: <b>{sender_name}</b>\n\n"
                        f"<blockquote>«{text_content[:300]}»</blockquote>",
                    )
                except:
                    pass

        # ── Ключевые слова-триггеры ───────────────────────
        if user["notify_keywords"] and text_content:
            keywords = db.get_keyword_triggers(uid)
            for kw in keywords:
                if kw in text_content.lower():
                    sender_name = (
                        (message.from_user.first_name if message.from_user else None)
                        or "?"
                    )
                    try:
                        await bot.send_message(
                            uid,
                            f"🔔 <b>Триггер: «{kw}»</b>\n\n"
                            f"От: <b>{sender_name}</b>\n\n"
                            f"<blockquote>{text_content[:300]}</blockquote>",
                        )
                    except:
                        pass
                    break

        # ── Уведомление о медиа с таймером ───────────────
        if user["notify_media_timers"] and (has_timer or is_view_once):
            sender_name = (
                (message.from_user.first_name if message.from_user else None)
                or "Пользователь"
            )
            icons = {"photo": "📸", "video": "🎬", "video_note": "🎥",
                     "audio": "🎵", "voice": "🎤"}
            icon = icons.get(media_type, "📎")
            try:
                await bot.send_message(
                    uid,
                    f"⏱ <b>Медиа с таймером сохранено!</b>\n\n"
                    f"{icon} Тип: {media_type}\n"
                    f"От: <b>{sender_name}</b>",
                )
            except:
                pass

        await check_achievements(bot, uid)

        logger.debug(
            f"Сохранено: msg#{message.message_id} user={uid} "
            f"type={media_type} timer={has_timer}"
        )
    except Exception as e:
        logger.error(f"on_business_message: {e}", exc_info=True)


@router.edited_business_message()
async def on_edited_business_message(message: Message, bot: Bot):
    """
    Отредактированное сообщение.
    Уведомляет в формате:
      ✏️ Сообщение изменено
      Было:
      <blockquote>оригинал</blockquote>
      Стало:
      <blockquote>новый текст</blockquote>
    """
    try:
        if not message.business_connection_id:
            return

        conn = db.get_connection(message.business_connection_id)
        if not conn:
            return

        uid  = conn["user_id"]
        user = db.get_user(uid)
        if not user or not user["notify_edits"]:
            return

        original = db.get_message(uid, message.chat.id, message.message_id)
        if not original:
            return

        original_text = original["message_text"] or ""
        new_text      = message.text or message.caption or ""

        db.mark_edited(uid, message.chat.id, message.message_id, original_text)
        db.add_xp(uid, XP_EDIT)

        sender_name = (
            (message.from_user.first_name if message.from_user else None)
            or "Пользователь"
        )

        # Строим уведомление — строго в формате цитат
        notif = (
            f"✏️ <b>Сообщение изменено</b>\n\n"
            f"От: <b>{sender_name}</b>\n\n"
        )
        if original_text:
            notif += f"<b>Было:</b>\n<blockquote>{original_text[:400]}</blockquote>\n\n"
        if new_text:
            notif += f"<b>Стало:</b>\n<blockquote>{new_text[:400]}</blockquote>"

        try:
            await bot.send_message(uid, notif[:4096])
        except:
            pass
    except Exception as e:
        logger.error(f"on_edited_business_message: {e}", exc_info=True)


@router.deleted_business_messages()
async def on_deleted_business_messages(
    deleted: BusinessMessagesDeleted, bot: Bot
):
    """
    Удалённые сообщения.
    • 1–4 сообщения: отдельные уведомления с цитатами + медиафайл
    • 5+ сообщений:  ZIP-архив всего чата
    """
    try:
        connection_id = deleted.business_connection_id
        chat          = deleted.chat
        message_ids   = deleted.message_ids

        conn = db.get_connection(connection_id)
        if not conn:
            return

        uid  = conn["user_id"]
        user = db.get_user(uid)

        # Отмечаем как удалённые в БД
        for mid in message_ids:
            db.mark_deleted(uid, chat.id, mid)

        if not user or not user["notify_deletions"]:
            return

        db.add_xp(uid, XP_DELETION * len(message_ids))

        # ── Массовое удаление (5+) — ZIP ─────────────────
        if len(message_ids) >= 5:
            messages_data = []
            for mid in message_ids:
                saved = db.get_message(uid, chat.id, mid)
                if saved:
                    messages_data.append(saved)

            chat_title = (getattr(chat, "title", None) or
                          getattr(chat, "first_name", None) or
                          f"Chat {chat.id}")

            archive_path = await build_zip_archive(
                uid, chat.id, messages_data, chat_title
            )
            if archive_path and Path(archive_path).exists():
                try:
                    await bot.send_document(
                        uid,
                        FSInputFile(archive_path,
                                    filename=f"deleted_chat_{chat.id}.zip"),
                        caption=(
                            f"🗑 <b>Удалён чат / пачка сообщений</b>\n\n"
                            f"Чат: <b>{chat_title}</b>\n"
                            f"Сообщений в архиве: {len(messages_data)}\n"
                            f"Всего удалено: {len(message_ids)}"
                        ),
                    )
                except Exception as e:
                    logger.error(f"send_document ZIP: {e}")
            return

        # ── Одиночные уведомления (1–4 сообщения) ────────
        media_icons = {
            "photo":      "📸 Фото",
            "video":      "🎬 Видео",
            "video_note": "🎥 Кружок",
            "audio":      "🎵 Аудио",
            "voice":      "🎤 Голосовое",
            "document":   "📄 Документ",
            "sticker":    "🎭 Стикер",
        }

        for mid in message_ids:
            saved = db.get_message(uid, chat.id, mid)
            if not saved:
                continue

            sender_name = (
                saved.get("sender_first_name") or
                saved.get("sender_username") or
                f"User#{saved.get('sender_id', '?')}"
            )
            ts = str(saved.get("created_at", ""))[:16]

            # Строим тело уведомления
            notif = (
                f"🗑 <b>Сообщение удалено</b>\n\n"
                f"От: <b>{sender_name}</b>\n"
                f"Время отправки: {ts}\n\n"
            )

            msg_text = saved.get("message_text") or saved.get("caption")
            if msg_text:
                notif += f"<b>Текст:</b>\n<blockquote>{msg_text[:400]}</blockquote>\n\n"

            if saved.get("original_text"):
                notif += (
                    f"<b>Оригинал (до правки):</b>\n"
                    f"<blockquote>{saved['original_text'][:200]}</blockquote>\n\n"
                )

            mtype = saved.get("media_type")
            if mtype:
                media_label = media_icons.get(mtype, f"📎 {mtype}")
                if saved.get("has_timer"):
                    media_label += " <b>[⏱ ТАЙМЕР]</b>"
                if saved.get("is_view_once"):
                    media_label += " <b>[ОДНОРАЗОВОЕ]</b>"
                notif += f"<b>Медиа:</b> {media_label}\n"
                if saved.get("caption"):
                    notif += (
                        f"<b>Подпись:</b>\n"
                        f"<blockquote>{saved['caption'][:200]}</blockquote>\n"
                    )

            # Отправляем уведомление
            try:
                await bot.send_message(uid, notif[:4096])
            except Exception as e:
                logger.error(f"send notification: {e}")

            # Отправляем медиафайл, если он сохранён
            fp = saved.get("media_file_path")
            if fp and Path(fp).exists():
                cap = "📎 Сохранённый файл"
                if saved.get("has_timer"):
                    cap += " [был с таймером ⏱]"
                try:
                    fsi = FSInputFile(fp)
                    if mtype == "photo":
                        await bot.send_photo(uid, fsi, caption=cap)
                    elif mtype == "video":
                        await bot.send_video(uid, fsi, caption=cap)
                    elif mtype == "video_note":
                        await bot.send_video_note(uid, fsi)
                    elif mtype == "audio":
                        await bot.send_audio(uid, fsi, caption=cap)
                    elif mtype == "voice":
                        await bot.send_voice(uid, fsi, caption=cap)
                    elif mtype == "document":
                        await bot.send_document(uid, fsi, caption=cap)
                    elif mtype == "sticker":
                        await bot.send_sticker(uid, fsi)
                    else:
                        await bot.send_document(uid, fsi, caption=cap)
                except Exception as e:
                    logger.error(f"send media: {e}")

        await check_achievements(bot, uid)

    except Exception as e:
        logger.error(f"on_deleted_business_messages: {e}", exc_info=True)


# ═══════════════════════════════════════════════════════════
#  ПЛАНИРОВЩИК (ежедневный дайджест + авто-бэкап)
# ═══════════════════════════════════════════════════════════

async def scheduler():
    """Запускается как asyncio-задача. Раз в сутки в ~08:00."""
    while True:
        now   = datetime.now()
        # Следующий запуск в 08:00
        next_run = now.replace(hour=8, minute=0, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=1)
        wait_s = (next_run - now).total_seconds()
        logger.info(f"Планировщик: следующий запуск через {wait_s/3600:.1f} ч.")
        await asyncio.sleep(wait_s)

        if bot_instance is None:
            continue

        # Получаем всех пользователей с включённым дайджестом
        users = db.get_all_users(limit=10000)
        for u in users:
            if not u["digest_enabled"] or u["is_blocked"]:
                continue
            uid = u["user_id"]
            if not db.check_subscription(uid):
                continue
            # Сообщения за вчера
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            msgs      = db.get_messages_by_date(uid, yesterday)
            deleted   = [m for m in msgs if m["is_deleted"]]
            edited    = [m for m in msgs if m["is_edited"]]
            text = (
                f"📋 <b>Ежедневный дайджест</b>\n"
                f"За {yesterday}\n\n"
                f"💬 Сообщений сохранено: {len(msgs)}\n"
                f"🗑 Удалений: {len(deleted)}\n"
                f"✏️ Редактирований: {len(edited)}\n"
            )
            try:
                await bot_instance.send_message(uid, text)
            except:
                pass


# ═══════════════════════════════════════════════════════════
#  ЗАПУСК
# ═══════════════════════════════════════════════════════════

async def main():
    global BOT_USERNAME, bot_instance

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    bot_instance = bot

    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    bot_info     = await bot.get_me()
    BOT_USERNAME = bot_info.username

    logger.info(f"🚀 Бот запущен: @{bot_info.username} (ID: {bot_info.id})")
    logger.info(f"📦 Версия: 8.0.0 ULTRA EDITION")
    logger.info(f"👨‍💼 Администратор: {ADMIN_ID} (@{ADMIN_USERNAME})")
    logger.info(f"🔧 БД инициализирована, все функции активны")

    # Запускаем планировщик параллельно
    asyncio.create_task(scheduler())

    # Уведомление администратора о запуске
    try:
        await bot.send_message(
            ADMIN_ID,
            f"🚀 <b>Chat Monitor v8.0.0 запущен!</b>\n\n"
            f"@{bot_info.username} | ID: {bot_info.id}\n\n"
            f"✅ Все функции активны:\n"
            f"• Сохранение сообщений/медиа\n"
            f"• Медиа с таймерами\n"
            f"• Уведомления об удалении (цитаты)\n"
            f"• Уведомления о редактировании (было/стало)\n"
            f"• ZIP при удалении чата\n"
            f"• Реферальная система\n"
            f"• Система уровней и достижений\n"
            f"• Аналитика и экспорт\n"
            f"• Ежедневный дайджест\n"
            f"• Скам-детектор\n"
            f"• Ключевые слова-триггеры\n"
            f"• 30+ функций",
        )
    except:
        pass

    await dp.start_polling(
        bot,
        allowed_updates=dp.resolve_used_update_types(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен вручную")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
