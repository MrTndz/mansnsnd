#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram Chat Monitor Bot
Version: 6.0.0
Author: Merzost?
Date: 2026-03-02

Профессиональный мониторинг чатов с интеграцией Telegram Stars,
реферальной системой и расширенными настройками
"""

import asyncio
import logging
import os
import sys
import json
import sqlite3
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple
from pathlib import Path
import aiofiles
import hashlib
import base64
import zipfile
import io

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode, ChatType
from aiogram.filters import Command, CommandStart, StateFilter
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
    Update,
    FSInputFile,
    BufferedInputFile,
    PhotoSize,
    Video,
    VideoNote,
    LabeledPrice,
    PreCheckoutQuery,
    SuccessfulPayment
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

# ========================================
# КОНФИГУРАЦИЯ
# ========================================

BOT_TOKEN = "8296802832:AAEU4oF4v5bjKP3KTb1rRx1Oxf-Z1dng9QQ"
ADMIN_ID = 7785371505
ADMIN_USERNAME = "mrztn"

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Создание необходимых директорий
MEDIA_DIR = Path("media")
EXPORTS_DIR = Path("exports")
DB_DIR = Path("database")

for directory in [MEDIA_DIR, EXPORTS_DIR, DB_DIR]:
    directory.mkdir(exist_ok=True)

# Константы
USERS_PER_PAGE = 10
MAX_MEDIA_SIZE = 50 * 1024 * 1024  # 50MB
MEDIA_CLEANUP_DAYS = 90

# Цены подписок в Stars
TRIAL_DAYS = 3
WEEKLY_PRICE = 20  # Stars
MONTHLY_PRICE = 50  # Stars
QUARTERLY_PRICE = 120  # Stars (скидка 20%)
YEARLY_PRICE = 400  # Stars (скидка 33%)

# Реферальная система
REFERRAL_BONUS_PERCENT = 20  # 20% от оплаты реферала

# ========================================
# FSM СОСТОЯНИЯ
# ========================================

class AdminStates(StatesGroup):
    """Состояния для админ-панели"""
    main_menu = State()
    user_management = State()
    viewing_user = State()
    user_number_input = State()
    send_message = State()
    gift_subscription = State()
    send_stars = State()
    manage_subscription = State()
    broadcast_message = State()
    statistics = State()
    search_user = State()

class UserStates(StatesGroup):
    """Состояния для пользователей"""
    settings_notifications = State()
    referral_stats = State()

# ========================================
# DATABASE
# ========================================

class Database:
    """Класс для работы с базой данных SQLite"""
    
    def __init__(self, db_path: str = "database/bot.db"):
        self.db_path = db_path
        self.init_database()
    
    def get_connection(self):
        """Получение подключения к БД"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def init_database(self):
        """Инициализация структуры базы данных"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Таблица пользователей
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                accepted_terms BOOLEAN DEFAULT 0,
                is_active BOOLEAN DEFAULT 1,
                is_blocked BOOLEAN DEFAULT 0,
                subscription_type TEXT DEFAULT 'free',
                subscription_expires TIMESTAMP,
                trial_used BOOLEAN DEFAULT 0,
                auto_trial_activated BOOLEAN DEFAULT 0,
                total_messages_saved INTEGER DEFAULT 0,
                total_deletions_tracked INTEGER DEFAULT 0,
                total_edits_tracked INTEGER DEFAULT 0,
                total_media_saved INTEGER DEFAULT 0,
                total_photo INTEGER DEFAULT 0,
                total_video INTEGER DEFAULT 0,
                total_document INTEGER DEFAULT 0,
                total_audio INTEGER DEFAULT 0,
                total_voice INTEGER DEFAULT 0,
                total_video_note INTEGER DEFAULT 0,
                total_sticker INTEGER DEFAULT 0,
                stars_balance INTEGER DEFAULT 0,
                referral_code TEXT UNIQUE,
                referred_by INTEGER,
                referral_earnings INTEGER DEFAULT 0,
                total_referrals INTEGER DEFAULT 0,
                notify_deletions BOOLEAN DEFAULT 1,
                notify_edits BOOLEAN DEFAULT 1,
                notify_media_timers BOOLEAN DEFAULT 1,
                notify_connections BOOLEAN DEFAULT 1,
                FOREIGN KEY (referred_by) REFERENCES users(user_id)
            )
        ''')
        
        # Таблица подключений
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS business_connections (
                connection_id TEXT PRIMARY KEY,
                user_id INTEGER,
                connected_user_id INTEGER,
                is_enabled BOOLEAN DEFAULT 1,
                can_reply BOOLEAN DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        ''')
        
        # Таблица сохраненных сообщений
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS saved_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                connection_id TEXT,
                chat_id INTEGER,
                message_id INTEGER,
                sender_id INTEGER,
                sender_username TEXT,
                sender_first_name TEXT,
                message_text TEXT,
                media_type TEXT,
                media_file_id TEXT,
                media_file_path TEXT,
                media_thumbnail_path TEXT,
                caption TEXT,
                has_timer BOOLEAN DEFAULT 0,
                timer_seconds INTEGER,
                timer_expires TIMESTAMP,
                is_view_once BOOLEAN DEFAULT 0,
                media_width INTEGER,
                media_height INTEGER,
                media_duration INTEGER,
                media_file_size INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_deleted BOOLEAN DEFAULT 0,
                deleted_at TIMESTAMP,
                is_edited BOOLEAN DEFAULT 0,
                edited_at TIMESTAMP,
                original_text TEXT,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        ''')
        
        # Таблица экспортированных чатов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS exported_chats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                chat_id INTEGER,
                chat_title TEXT,
                export_format TEXT,
                file_path TEXT,
                messages_count INTEGER,
                exported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        ''')
        
        # Таблица уведомлений
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                notification_type TEXT,
                title TEXT,
                message TEXT,
                is_read BOOLEAN DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        ''')
        
        # Таблица платежей
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                amount_stars INTEGER,
                plan_type TEXT,
                payment_charge_id TEXT,
                telegram_payment_charge_id TEXT,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                confirmed_at TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        ''')
        
        # Таблица статистики
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS statistics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                stat_type TEXT,
                stat_value INTEGER,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        ''')
        
        # Таблица действий администратора
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS admin_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id INTEGER,
                target_user_id INTEGER,
                action_type TEXT,
                action_details TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблица транзакций Stars
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stars_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                amount INTEGER,
                transaction_type TEXT,
                description TEXT,
                related_user_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        ''')
        
        # Таблица рефералов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS referral_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                referrer_id INTEGER,
                referred_id INTEGER,
                action_type TEXT,
                bonus_amount INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (referrer_id) REFERENCES users(user_id),
                FOREIGN KEY (referred_id) REFERENCES users(user_id)
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info("База данных инициализирована (версия 6.0.0)")
    
    # ========================================
    # МЕТОДЫ ДЛЯ РАБОТЫ С ПОЛЬЗОВАТЕЛЯМИ
    # ========================================
    
    def add_user(self, user_id: int, username: str = None, first_name: str = None, 
                 last_name: str = None, referred_by: int = None):
        """Добавление нового пользователя"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            # Генерируем уникальный реферальный код
            referral_code = self._generate_referral_code(user_id)
            
            cursor.execute('''
                INSERT OR IGNORE INTO users 
                (user_id, username, first_name, last_name, referral_code, referred_by)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (user_id, username, first_name, last_name, referral_code, referred_by))
            
            # Если был приглашен по реферальной ссылке
            if referred_by and cursor.rowcount > 0:
                cursor.execute('''
                    UPDATE users 
                    SET total_referrals = total_referrals + 1 
                    WHERE user_id = ?
                ''', (referred_by,))
                
                cursor.execute('''
                    INSERT INTO referral_actions 
                    (referrer_id, referred_id, action_type, bonus_amount)
                    VALUES (?, ?, 'registration', 0)
                ''', (referred_by, user_id))
            
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления пользователя: {e}")
            return False
        finally:
            conn.close()
    
    def _generate_referral_code(self, user_id: int) -> str:
        """Генерация уникального реферального кода"""
        import random
        import string
        base = f"{user_id}"
        random_part = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
        return f"REF{base}{random_part}"
    
    def get_user(self, user_id: int) -> Optional[Dict]:
        """Получение данных пользователя"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None
    
    def get_user_by_referral_code(self, code: str) -> Optional[Dict]:
        """Получение пользователя по реферальному коду"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM users WHERE referral_code = ?', (code,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None
    
    def update_user_activity(self, user_id: int):
        """Обновление времени последней активности"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE users SET last_activity = CURRENT_TIMESTAMP WHERE user_id = ?
        ''', (user_id,))
        conn.commit()
        conn.close()
    
    def accept_terms(self, user_id: int):
        """Принятие условий использования"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE users SET accepted_terms = 1 WHERE user_id = ?
        ''', (user_id,))
        conn.commit()
        conn.close()
    
    def activate_auto_trial(self, user_id: int):
        """Автоматическая активация пробного периода при первом подключении"""
        conn = self.get_connection()
        cursor = conn.cursor()
        expires = datetime.now() + timedelta(days=TRIAL_DAYS)
        cursor.execute('''
            UPDATE users 
            SET subscription_type = 'trial', 
                subscription_expires = ?,
                trial_used = 1,
                auto_trial_activated = 1
            WHERE user_id = ? AND trial_used = 0
        ''', (expires, user_id))
        affected = cursor.rowcount
        conn.commit()
        conn.close()
        return affected > 0
    
    def activate_subscription(self, user_id: int, plan_type: str, days: int = None):
        """Активация подписки"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        if days:
            expires = datetime.now() + timedelta(days=days)
        elif plan_type == 'weekly':
            expires = datetime.now() + timedelta(days=7)
        elif plan_type == 'monthly':
            expires = datetime.now() + timedelta(days=30)
        elif plan_type == 'quarterly':
            expires = datetime.now() + timedelta(days=90)
        elif plan_type == 'yearly':
            expires = datetime.now() + timedelta(days=365)
        elif plan_type == 'lifetime':
            expires = None
        else:
            expires = None
        
        cursor.execute('''
            UPDATE users 
            SET subscription_type = ?,
                subscription_expires = ?
            WHERE user_id = ?
        ''', (plan_type, expires, user_id))
        conn.commit()
        conn.close()
        
        self.log_admin_action(ADMIN_ID, user_id, 'subscription_activated', 
                             f'Plan: {plan_type}, Expires: {expires}')
    
    def check_subscription(self, user_id: int) -> bool:
        """Проверка активности подписки"""
        user = self.get_user(user_id)
        if not user:
            return False
        
        if user['is_blocked']:
            return False
        
        if user['subscription_type'] == 'free':
            return False
        
        if user['subscription_type'] == 'lifetime':
            return True
        
        if user['subscription_expires']:
            expires = datetime.fromisoformat(user['subscription_expires'])
            if datetime.now() > expires:
                self.deactivate_subscription(user_id)
                return False
        
        return True
    
    def deactivate_subscription(self, user_id: int):
        """Деактивация подписки"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE users 
            SET subscription_type = 'free',
                subscription_expires = NULL
            WHERE user_id = ?
        ''', (user_id,))
        conn.commit()
        conn.close()
    
    def block_user(self, user_id: int):
        """Блокировка пользователя"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE users SET is_blocked = 1 WHERE user_id = ?
        ''', (user_id,))
        conn.commit()
        conn.close()
        self.log_admin_action(ADMIN_ID, user_id, 'user_blocked', 'User blocked by admin')
    
    def unblock_user(self, user_id: int):
        """Разблокировка пользователя"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE users SET is_blocked = 0 WHERE user_id = ?
        ''', (user_id,))
        conn.commit()
        conn.close()
        self.log_admin_action(ADMIN_ID, user_id, 'user_unblocked', 'User unblocked by admin')
    
    def update_notification_settings(self, user_id: int, setting: str, value: bool):
        """Обновление настроек уведомлений"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(f'''
            UPDATE users SET {setting} = ? WHERE user_id = ?
        ''', (value, user_id))
        conn.commit()
        conn.close()
    
    # ========================================
    # МЕТОДЫ ДЛЯ РАБОТЫ С STARS
    # ========================================
    
    def add_stars(self, user_id: int, amount: int, description: str = "", related_user_id: int = None):
        """Добавление звезд пользователю"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE users SET stars_balance = stars_balance + ? WHERE user_id = ?
        ''', (amount, user_id))
        
        cursor.execute('''
            INSERT INTO stars_transactions 
            (user_id, amount, transaction_type, description, related_user_id)
            VALUES (?, ?, 'add', ?, ?)
        ''', (user_id, amount, description, related_user_id))
        
        conn.commit()
        conn.close()
        
        if description.startswith("Admin"):
            self.log_admin_action(ADMIN_ID, user_id, 'stars_added', 
                                 f'Amount: {amount}, Reason: {description}')
    
    def spend_stars(self, user_id: int, amount: int, description: str = ""):
        """Списание звезд"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE users SET stars_balance = stars_balance - ? WHERE user_id = ?
        ''', (amount, user_id))
        
        cursor.execute('''
            INSERT INTO stars_transactions 
            (user_id, amount, transaction_type, description)
            VALUES (?, ?, 'spend', ?)
        ''', (user_id, -amount, description))
        
        conn.commit()
        conn.close()
    
    def get_stars_balance(self, user_id: int) -> int:
        """Получение баланса звезд"""
        user = self.get_user(user_id)
        return user['stars_balance'] if user else 0
    
    # ========================================
    # МЕТОДЫ ДЛЯ РАБОТЫ С ПОДКЛЮЧЕНИЯМИ
    # ========================================
    
    def add_business_connection(self, connection_id: str, user_id: int, 
                               connected_user_id: int, can_reply: bool = False):
        """Добавление подключения"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO business_connections 
                (connection_id, user_id, connected_user_id, can_reply)
                VALUES (?, ?, ?, ?)
            ''', (connection_id, user_id, connected_user_id, can_reply))
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления подключения: {e}")
            return False
        finally:
            conn.close()
    
    def get_business_connection(self, connection_id: str) -> Optional[Dict]:
        """Получение данных подключения"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM business_connections WHERE connection_id = ?', (connection_id,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None
    
    def get_user_connections(self, user_id: int) -> List[Dict]:
        """Получение всех подключений пользователя"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM business_connections WHERE user_id = ?', (user_id,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    # ========================================
    # МЕТОДЫ ДЛЯ РАБОТЫ С СООБЩЕНИЯМИ
    # ========================================
    
    def save_message(self, user_id: int, connection_id: str, chat_id: int, message_id: int,
                    sender_id: int, sender_username: str = None, sender_first_name: str = None,
                    message_text: str = None, media_type: str = None, media_file_id: str = None, 
                    media_file_path: str = None, media_thumbnail_path: str = None, 
                    caption: str = None, has_timer: bool = False, timer_seconds: int = None, 
                    is_view_once: bool = False, media_width: int = None, media_height: int = None, 
                    media_duration: int = None, media_file_size: int = None):
        """Сохранение сообщения с поддержкой таймеров"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            timer_expires = None
            if has_timer and timer_seconds:
                timer_expires = datetime.now() + timedelta(seconds=timer_seconds)
            
            cursor.execute('''
                INSERT INTO saved_messages 
                (user_id, connection_id, chat_id, message_id, sender_id, sender_username,
                 sender_first_name, message_text, media_type, media_file_id, media_file_path, 
                 media_thumbnail_path, caption, has_timer, timer_seconds, timer_expires, 
                 is_view_once, media_width, media_height, media_duration, media_file_size)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (user_id, connection_id, chat_id, message_id, sender_id, sender_username,
                  sender_first_name, message_text, media_type, media_file_id, media_file_path, 
                  media_thumbnail_path, caption, has_timer, timer_seconds, timer_expires, 
                  is_view_once, media_width, media_height, media_duration, media_file_size))
            conn.commit()
            
            # Обновляем статистику
            cursor.execute('''
                UPDATE users SET total_messages_saved = total_messages_saved + 1
                WHERE user_id = ?
            ''', (user_id,))
            
            if media_type:
                cursor.execute('''
                    UPDATE users SET total_media_saved = total_media_saved + 1
                    WHERE user_id = ?
                ''', (user_id,))
                
                media_column = f'total_{media_type}'
                cursor.execute(f'''
                    UPDATE users SET {media_column} = {media_column} + 1
                    WHERE user_id = ?
                ''', (user_id,))
            
            conn.commit()
            return cursor.lastrowid
        except Exception as e:
            logger.error(f"Ошибка сохранения сообщения: {e}")
            return None
        finally:
            conn.close()
    
    def get_message(self, user_id: int, chat_id: int, message_id: int) -> Optional[Dict]:
        """Получение сохраненного сообщения"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM saved_messages 
            WHERE user_id = ? AND chat_id = ? AND message_id = ?
            ORDER BY created_at DESC LIMIT 1
        ''', (user_id, chat_id, message_id))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None
    
    def mark_message_deleted(self, user_id: int, chat_id: int, message_id: int):
        """Отметка сообщения как удаленного"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE saved_messages 
            SET is_deleted = 1, deleted_at = CURRENT_TIMESTAMP
            WHERE user_id = ? AND chat_id = ? AND message_id = ?
        ''', (user_id, chat_id, message_id))
        affected = cursor.rowcount
        
        if affected > 0:
            cursor.execute('''
                UPDATE users SET total_deletions_tracked = total_deletions_tracked + 1
                WHERE user_id = ?
            ''', (user_id,))
        
        conn.commit()
        conn.close()
        return affected > 0
    
    def mark_message_edited(self, user_id: int, chat_id: int, message_id: int, original_text: str):
        """Отметка сообщения как измененного"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE saved_messages 
            SET is_edited = 1, edited_at = CURRENT_TIMESTAMP, original_text = ?
            WHERE user_id = ? AND chat_id = ? AND message_id = ?
        ''', (original_text, user_id, chat_id, message_id))
        affected = cursor.rowcount
        
        if affected > 0:
            cursor.execute('''
                UPDATE users SET total_edits_tracked = total_edits_tracked + 1
                WHERE user_id = ?
            ''', (user_id,))
        
        conn.commit()
        conn.close()
    
    def get_chat_messages(self, user_id: int, chat_id: int) -> List[Dict]:
        """Получение всех сообщений из чата"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM saved_messages 
            WHERE user_id = ? AND chat_id = ?
            ORDER BY created_at ASC
        ''', (user_id, chat_id))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    # ========================================
    # МЕТОДЫ ДЛЯ РЕФЕРАЛОВ
    # ========================================
    
    def process_referral_payment(self, user_id: int, amount_stars: int):
        """Обработка реферального бонуса при оплате"""
        user = self.get_user(user_id)
        if not user or not user['referred_by']:
            return
        
        referrer_id = user['referred_by']
        bonus = int(amount_stars * REFERRAL_BONUS_PERCENT / 100)
        
        self.add_stars(referrer_id, bonus, 
                      f"Реферальный бонус от пользователя {user_id}", user_id)
        
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE users 
            SET referral_earnings = referral_earnings + ?
            WHERE user_id = ?
        ''', (bonus, referrer_id))
        
        cursor.execute('''
            INSERT INTO referral_actions 
            (referrer_id, referred_id, action_type, bonus_amount)
            VALUES (?, ?, 'payment', ?)
        ''', (referrer_id, user_id, bonus))
        
        conn.commit()
        conn.close()
    
    def get_referral_stats(self, user_id: int) -> Dict:
        """Получение статистики по рефералам"""
        user = self.get_user(user_id)
        if not user:
            return {}
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Получаем список рефералов
        cursor.execute('''
            SELECT user_id, username, first_name, subscription_type, registered_at
            FROM users
            WHERE referred_by = ?
            ORDER BY registered_at DESC
        ''', (user_id,))
        referrals = [dict(row) for row in cursor.fetchall()]
        
        # Получаем историю бонусов
        cursor.execute('''
            SELECT * FROM referral_actions
            WHERE referrer_id = ?
            ORDER BY created_at DESC
            LIMIT 10
        ''', (user_id,))
        actions = [dict(row) for row in cursor.fetchall()]
        
        conn.close()
        
        return {
            'total': user['total_referrals'],
            'earnings': user['referral_earnings'],
            'referrals': referrals,
            'actions': actions,
            'code': user['referral_code']
        }
    
    # ========================================
    # МЕТОДЫ ДЛЯ ПЛАТЕЖЕЙ
    # ========================================
    
    def save_payment(self, user_id: int, amount_stars: int, plan_type: str, 
                     payment_charge_id: str, telegram_payment_charge_id: str):
        """Сохранение платежа"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO payments 
            (user_id, amount_stars, plan_type, payment_charge_id, 
             telegram_payment_charge_id, status, confirmed_at)
            VALUES (?, ?, ?, ?, ?, 'confirmed', CURRENT_TIMESTAMP)
        ''', (user_id, amount_stars, plan_type, payment_charge_id, telegram_payment_charge_id))
        conn.commit()
        conn.close()
    
    # ========================================
    # МЕТОДЫ ДЛЯ АДМИНИСТРАТОРА
    # ========================================
    
    def get_all_users(self, limit: int = None, offset: int = 0) -> List[Dict]:
        """Получение всех пользователей с пагинацией"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        if limit:
            cursor.execute('''
                SELECT * FROM users 
                ORDER BY registered_at DESC 
                LIMIT ? OFFSET ?
            ''', (limit, offset))
        else:
            cursor.execute('SELECT * FROM users ORDER BY registered_at DESC')
        
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def search_users(self, query: str) -> List[Dict]:
        """Поиск пользователей"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        search_pattern = f'%{query}%'
        cursor.execute('''
            SELECT * FROM users 
            WHERE CAST(user_id AS TEXT) LIKE ? 
               OR username LIKE ? 
               OR first_name LIKE ? 
               OR last_name LIKE ?
            ORDER BY registered_at DESC
        ''', (search_pattern, search_pattern, search_pattern, search_pattern))
        
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def get_user_count(self) -> int:
        """Получение количества пользователей"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) as count FROM users')
        result = cursor.fetchone()
        conn.close()
        return result['count'] if result else 0
    
    def get_active_subscriptions_count(self) -> int:
        """Получение количества активных подписок"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT COUNT(*) as count FROM users 
            WHERE subscription_type != 'free' 
            AND (subscription_expires IS NULL OR subscription_expires > CURRENT_TIMESTAMP)
            AND is_blocked = 0
        ''')
        result = cursor.fetchone()
        conn.close()
        return result['count'] if result else 0
    
    def get_total_messages_saved(self) -> int:
        """Получение общего количества сохраненных сообщений"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) as count FROM saved_messages')
        result = cursor.fetchone()
        conn.close()
        return result['count'] if result else 0
    
    def get_total_deletions_tracked(self) -> int:
        """Получение общего количества отслеженных удалений"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) as count FROM saved_messages WHERE is_deleted = 1')
        result = cursor.fetchone()
        conn.close()
        return result['count'] if result else 0
    
    def get_total_media_by_type(self) -> Dict[str, int]:
        """Получение статистики по типам медиа"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT media_type, COUNT(*) as count 
            FROM saved_messages 
            WHERE media_type IS NOT NULL
            GROUP BY media_type
        ''')
        rows = cursor.fetchall()
        conn.close()
        return {row['media_type']: row['count'] for row in rows}
    
    def log_admin_action(self, admin_id: int, target_user_id: int, action_type: str, details: str):
        """Логирование действий администратора"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO admin_actions (admin_id, target_user_id, action_type, action_details)
            VALUES (?, ?, ?, ?)
        ''', (admin_id, target_user_id, action_type, details))
        conn.commit()
        conn.close()
    
    def get_user_admin_history(self, user_id: int) -> List[Dict]:
        """Получение истории действий администратора с пользователем"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM admin_actions 
            WHERE target_user_id = ?
            ORDER BY created_at DESC
            LIMIT 20
        ''', (user_id,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def add_notification(self, user_id: int, notification_type: str, title: str, message: str):
        """Добавление уведомления"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO notifications (user_id, notification_type, title, message)
            VALUES (?, ?, ?, ?)
        ''', (user_id, notification_type, title, message))
        conn.commit()
        conn.close()

# Создание экземпляра базы данных
db = Database()

# ========================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ========================================

async def download_media(bot: Bot, file_id: str, file_type: str, user_id: int, 
                        has_timer: bool = False) -> Optional[str]:
    """Скачивание медиафайла с поддержкой таймеров"""
    try:
        file = await bot.get_file(file_id)
        file_extension = file.file_path.split('.')[-1] if file.file_path else 'bin'
        
        user_media_dir = MEDIA_DIR / str(user_id)
        user_media_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_hash = hashlib.md5(file_id.encode()).hexdigest()[:8]
        timer_prefix = "timer_" if has_timer else ""
        filename = f"{timer_prefix}{file_type}_{timestamp}_{file_hash}.{file_extension}"
        file_path = user_media_dir / filename
        
        await bot.download_file(file.file_path, file_path)
        
        logger.info(f"Медиафайл сохранен: {file_path} (таймер: {has_timer})")
        return str(file_path)
    except Exception as e:
        logger.error(f"Ошибка скачивания медиа: {e}")
        return None

async def download_thumbnail(bot: Bot, photo: PhotoSize, user_id: int) -> Optional[str]:
    """Скачивание миниатюры"""
    try:
        file = await bot.get_file(photo.file_id)
        
        user_media_dir = MEDIA_DIR / str(user_id) / "thumbnails"
        user_media_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_hash = hashlib.md5(photo.file_id.encode()).hexdigest()[:8]
        filename = f"thumb_{timestamp}_{file_hash}.jpg"
        file_path = user_media_dir / filename
        
        await bot.download_file(file.file_path, file_path)
        
        return str(file_path)
    except Exception as e:
        logger.error(f"Ошибка скачивания миниатюры: {e}")
        return None

async def export_deleted_chat_to_archive(user_id: int, chat_id: int, 
                                        messages: List[Dict], chat_title: str) -> Optional[str]:
    """Экспорт удаленного чата в ZIP архив с медиа"""
    try:
        user_export_dir = EXPORTS_DIR / str(user_id)
        user_export_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        zip_filename = f"deleted_chat_{chat_id}_{timestamp}.zip"
        zip_path = user_export_dir / zip_filename
        
        # Создаем текстовый отчет
        report = f"Удаленный чат: {chat_title}\n"
        report += f"Дата удаления: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        report += f"Всего сообщений: {len(messages)}\n"
        report += "=" * 80 + "\n\n"
        
        media_files = []
        
        for msg in messages:
            timestamp_str = msg['created_at']
            sender = msg['sender_username'] or msg['sender_first_name'] or f"User {msg['sender_id']}"
            
            report += f"[{timestamp_str}] {sender}:\n"
            
            if msg['message_text']:
                report += f"{msg['message_text']}\n"
            
            if msg['media_type']:
                media_filename = f"media_{msg['message_id']}"
                report += f"[{msg['media_type'].upper()}]"
                if msg['has_timer']:
                    report += f" [⏱ ТАЙМЕР: {msg['timer_seconds']}с]"
                if msg['is_view_once']:
                    report += " [👁 ОДНОРАЗОВЫЙ]"
                report += f" → {media_filename}\n"
                
                if msg['caption']:
                    report += f"Подпись: {msg['caption']}\n"
                
                # Добавляем медиафайл в список для архива
                if msg['media_file_path'] and Path(msg['media_file_path']).exists():
                    media_files.append((msg['media_file_path'], media_filename))
            
            report += "-" * 80 + "\n\n"
        
        # Создаем ZIP архив
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Добавляем текстовый отчет
            zipf.writestr('chat_report.txt', report.encode('utf-8'))
            
            # Добавляем медиафайлы
            for media_path, media_name in media_files:
                try:
                    ext = Path(media_path).suffix
                    zipf.write(media_path, f"media/{media_name}{ext}")
                except Exception as e:
                    logger.error(f"Ошибка добавления медиа в архив: {e}")
        
        logger.info(f"Чат экспортирован в архив: {zip_path}")
        return str(zip_path)
    except Exception as e:
        logger.error(f"Ошибка экспорта чата в архив: {e}")
        return None

def format_subscription_info(user: Dict) -> str:
    """Форматирование информации о подписке"""
    sub_type = user['subscription_type']
    
    if user['is_blocked']:
        return "🚫 Заблокирован"
    
    if sub_type == 'free':
        return "🆓 Бесплатный"
    elif sub_type == 'trial':
        if user['subscription_expires']:
            expires = datetime.fromisoformat(user['subscription_expires'])
            days_left = (expires - datetime.now()).days
            return f"🎁 Пробный ({days_left}д)"
        return "🎁 Пробный"
    elif sub_type == 'weekly':
        if user['subscription_expires']:
            expires = datetime.fromisoformat(user['subscription_expires'])
            return f"📅 Неделя (до {expires.strftime('%d.%m.%Y')})"
        return "📅 Неделя"
    elif sub_type == 'monthly':
        if user['subscription_expires']:
            expires = datetime.fromisoformat(user['subscription_expires'])
            return f"💎 Месяц (до {expires.strftime('%d.%m.%Y')})"
        return "💎 Месяц"
    elif sub_type == 'quarterly':
        if user['subscription_expires']:
            expires = datetime.fromisoformat(user['subscription_expires'])
            return f"💼 Квартал (до {expires.strftime('%d.%m.%Y')})"
        return "💼 Квартал"
    elif sub_type == 'yearly':
        if user['subscription_expires']:
            expires = datetime.fromisoformat(user['subscription_expires'])
            return f"👑 Год (до {expires.strftime('%d.%m.%Y')})"
        return "👑 Год"
    elif sub_type == 'lifetime':
        return "♾️ Навсегда"
    else:
        return "❓ Неизвестно"

def format_user_short(user: Dict, index: int) -> str:
    """Краткое форматирование информации о пользователе для списка"""
    status_emoji = "🚫" if user['is_blocked'] else "✅"
    sub_emoji = {
        'free': '🆓',
        'trial': '🎁',
        'weekly': '📅',
        'monthly': '💎',
        'quarterly': '💼',
        'yearly': '👑',
        'lifetime': '♾️'
    }.get(user['subscription_type'], '❓')
    
    username = f"@{user['username']}" if user['username'] else "без username"
    name = user['first_name'] or "Без имени"
    
    return f"{index}. {status_emoji} {sub_emoji} {name} ({username})\n   ID: {user['user_id']}"

# ========================================
# КЛАВИАТУРЫ
# ========================================

def get_start_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура при старте"""
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Принять условия", callback_data="accept_terms")
    builder.button(text="📄 Прочитать условия", callback_data="show_terms")
    builder.adjust(1)
    return builder.as_markup()

def get_main_menu_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """Главное меню"""
    builder = InlineKeyboardBuilder()
    builder.button(text="📊 Статистика", callback_data="stats")
    builder.button(text="💎 Подписка", callback_data="subscription")
    builder.button(text="🔗 Подключения", callback_data="connections")
    builder.button(text="⭐ Мои Stars", callback_data="my_stars")
    builder.button(text="👥 Рефералы", callback_data="referrals")
    builder.button(text="⚙️ Настройки", callback_data="settings")
    builder.button(text="ℹ️ Помощь", callback_data="help")
    
    if user_id == ADMIN_ID:
        builder.button(text="👨‍💼 Админ", callback_data="admin_panel")
    
    builder.adjust(2)
    return builder.as_markup()

def get_subscription_keyboard(trial_used: bool, has_stars: bool = False) -> InlineKeyboardMarkup:
    """Клавиатура выбора подписки"""
    builder = InlineKeyboardBuilder()
    
    builder.button(text=f"📅 Неделя - {WEEKLY_PRICE} ⭐", callback_data="sub_weekly")
    builder.button(text=f"💎 Месяц - {MONTHLY_PRICE} ⭐", callback_data="sub_monthly")
    builder.button(text=f"💼 Квартал - {QUARTERLY_PRICE} ⭐ (-20%)", callback_data="sub_quarterly")
    builder.button(text=f"👑 Год - {YEARLY_PRICE} ⭐ (-33%)", callback_data="sub_yearly")
    builder.button(text="◀️ Назад", callback_data="main_menu")
    builder.adjust(1)
    return builder.as_markup()

def get_settings_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура настроек"""
    builder = InlineKeyboardBuilder()
    builder.button(text="🔔 Уведомления", callback_data="settings_notifications")
    builder.button(text="📥 Экспорт данных", callback_data="settings_export")
    builder.button(text="🗑 Очистка", callback_data="settings_cleanup")
    builder.button(text="◀️ Назад", callback_data="main_menu")
    builder.adjust(1)
    return builder.as_markup()

def get_notifications_settings_keyboard(user: Dict) -> InlineKeyboardMarkup:
    """Клавиатура настроек уведомлений"""
    builder = InlineKeyboardBuilder()
    
    deletions_status = "✅" if user['notify_deletions'] else "❌"
    edits_status = "✅" if user['notify_edits'] else "❌"
    timers_status = "✅" if user['notify_media_timers'] else "❌"
    connections_status = "✅" if user['notify_connections'] else "❌"
    
    builder.button(text=f"{deletions_status} Удаления", 
                  callback_data="toggle_notify_deletions")
    builder.button(text=f"{edits_status} Редактирования", 
                  callback_data="toggle_notify_edits")
    builder.button(text=f"{timers_status} Медиа с таймерами", 
                  callback_data="toggle_notify_media_timers")
    builder.button(text=f"{connections_status} Подключения", 
                  callback_data="toggle_notify_connections")
    builder.button(text="◀️ Назад", callback_data="settings")
    
    builder.adjust(1)
    return builder.as_markup()

def get_admin_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура админ-панели"""
    builder = InlineKeyboardBuilder()
    builder.button(text="👥 Пользователи", callback_data="admin_users")
    builder.button(text="📊 Статистика", callback_data="admin_stats")
    builder.button(text="📢 Рассылка", callback_data="admin_broadcast")
    builder.button(text="🔍 Поиск", callback_data="admin_search")
    builder.button(text="◀️ Назад", callback_data="main_menu")
    builder.adjust(2)
    return builder.as_markup()

def get_users_list_keyboard(page: int = 0, total_pages: int = 1) -> InlineKeyboardMarkup:
    """Клавиатура списка пользователей с пагинацией"""
    builder = InlineKeyboardBuilder()
    
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="◀️ Пред", 
                                               callback_data=f"users_page_{page-1}"))
    nav_buttons.append(InlineKeyboardButton(text=f"📄 {page+1}/{total_pages}", 
                                           callback_data="users_page_info"))
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton(text="След ▶️", 
                                               callback_data=f"users_page_{page+1}"))
    
    for btn in nav_buttons:
        builder.add(btn)
    
    builder.row(InlineKeyboardButton(text="🔢 Выбрать по номеру", 
                                    callback_data="select_user_by_number"))
    builder.row(InlineKeyboardButton(text="◀️ Назад", callback_data="admin_panel"))
    
    return builder.as_markup()

def get_user_management_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """Клавиатура управления пользователем"""
    builder = InlineKeyboardBuilder()
    
    user = db.get_user(user_id)
    
    builder.button(text="💬 Отправить сообщение", callback_data=f"admin_msg_{user_id}")
    builder.button(text="🎁 Подарить подписку", callback_data=f"admin_gift_{user_id}")
    builder.button(text="⭐ Отправить Stars", callback_data=f"admin_stars_{user_id}")
    builder.button(text="💎 Управление подпиской", callback_data=f"admin_sub_{user_id}")
    
    if user and user['is_blocked']:
        builder.button(text="✅ Разблокировать", callback_data=f"admin_unblock_{user_id}")
    else:
        builder.button(text="🚫 Заблокировать", callback_data=f"admin_block_{user_id}")
    
    builder.button(text="📜 История действий", callback_data=f"admin_history_{user_id}")
    builder.button(text="◀️ К списку", callback_data="admin_users")
    
    builder.adjust(2)
    return builder.as_markup()

def get_gift_subscription_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """Клавиатура выбора подарочной подписки"""
    builder = InlineKeyboardBuilder()
    
    builder.button(text="📅 7 дней", callback_data=f"gift_sub_{user_id}_trial_7")
    builder.button(text="💎 1 месяц", callback_data=f"gift_sub_{user_id}_monthly_30")
    builder.button(text="💼 3 месяца", callback_data=f"gift_sub_{user_id}_quarterly_90")
    builder.button(text="👑 1 год", callback_data=f"gift_sub_{user_id}_yearly_365")
    builder.button(text="♾️ Навсегда", callback_data=f"gift_sub_{user_id}_lifetime_0")
    builder.button(text="◀️ Назад", callback_data=f"manage_user_{user_id}")
    
    builder.adjust(2)
    return builder.as_markup()

def get_back_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура с кнопкой назад"""
    builder = InlineKeyboardBuilder()
    builder.button(text="◀️ Назад", callback_data="main_menu")
    return builder.as_markup()

# ========================================
# ОБРАБОТЧИКИ КОМАНД
# ========================================

router = Router()

@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    """Обработка команды /start"""
    user_id = message.from_user.id
    username = message.from_user.username
    first_name = message.from_user.first_name
    last_name = message.from_user.last_name
    
    # Проверяем на реферальный код
    args = message.text.split()[1:] if len(message.text.split()) > 1 else []
    referrer_id = None
    
    if args and args[0].startswith('ref'):
        ref_code = args[0]
        referrer = db.get_user_by_referral_code(ref_code)
        if referrer and referrer['user_id'] != user_id:
            referrer_id = referrer['user_id']
    
    db.add_user(user_id, username, first_name, last_name, referrer_id)
    user = db.get_user(user_id)
    
    if user['is_blocked']:
        await message.answer(
            "🚫 <b>Ваш аккаунт заблокирован</b>\n\n"
            "Для разблокировки свяжитесь с администратором: @" + ADMIN_USERNAME
        )
        return
    
    if not user['accepted_terms']:
        await message.answer(
            "👋 <b>Добро пожаловать в Chat Monitor!</b>\n\n"
            "🔐 Мониторинг удаленных и измененных сообщений\n"
            "📸 Сохранение медиа с таймерами самоуничтожения\n"
            "⚡ Мгновенные уведомления\n"
            "👥 Реферальная система с бонусами\n\n"
            "Перед использованием необходимо принять условия.",
            reply_markup=get_start_keyboard()
        )
    else:
        stars_emoji = "⭐" * min(db.get_stars_balance(user_id) // 10, 5)
        await message.answer(
            f"👋 С возвращением, <b>{first_name}</b>!\n\n"
            f"{format_subscription_info(user)}\n"
            f"{stars_emoji} Stars: {db.get_stars_balance(user_id)}\n\n"
            "Выберите действие:",
            reply_markup=get_main_menu_keyboard(user_id)
        )

@router.callback_query(F.data == "show_terms")
async def show_terms(callback: CallbackQuery):
    """Показ условий использования"""
    terms_text = """
📄 <b>УСЛОВИЯ ИСПОЛЬЗОВАНИЯ</b>

<b>1. ОБЩИЕ ПОЛОЖЕНИЯ</b>
Бот мониторит ваши чаты и сохраняет сообщения, включая удаленные и с таймерами самоуничтожения.

<b>2. ФУНКЦИОНАЛ</b>
• Сохранение всех сообщений из подключенных чатов
• Уведомления об удаленных сообщениях
• Отслеживание изменений
• Сохранение медиафайлов с таймерами
• Реферальная система

<b>3. ПОДКЛЮЧЕНИЕ</b>
• Настройки → Чат-боты → Добавить
• Бот работает только с явно подключенными чатами
• Нет доступа к другим вашим чатам

<b>4. КОНФИДЕНЦИАЛЬНОСТЬ И ОТВЕТСТВЕННОСТЬ</b>
• Администрация НЕ несет ответственности за действия пользователей
• Администрация НЕ несет ответственности за непредвиденные ситуации
• Администрация ГАРАНТИРУЕТ: личная информация пользователей не передается третьим лицам (кроме госорганов по официальным запросам)
• Это ЕДИНСТВЕННАЯ гарантия администрации
• Все остальные риски пользователь берет на себя

<b>5. ПОДПИСКИ</b>
• Пробный: 3 дня автоматически
• Неделя: 20 ⭐
• Месяц: 50 ⭐
• Квартал: 120 ⭐ (скидка 20%)
• Год: 400 ⭐ (скидка 33%)

<b>6. ОПЛАТА</b>
Оплата производится в Telegram Stars напрямую в боте. Возврат возможен в течение 24 часов.

Нажимая "Принять", вы соглашаетесь с условиями.
    """
    await callback.message.edit_text(terms_text, reply_markup=get_start_keyboard())

@router.callback_query(F.data == "accept_terms")
async def accept_terms(callback: CallbackQuery):
    """Принятие условий использования"""
    user_id = callback.from_user.id
    db.accept_terms(user_id)
    
    await callback.message.edit_text(
        "✅ <b>Условия приняты!</b>\n\n"
        "<b>Подключите бота:</b>\n"
        "1. Настройки → Чат-боты\n"
        "2. Добавить чат-бота\n"
        "3. Введите: @mrztnbot\n"
        "4. Настройте параметры\n\n"
        "После подключения бот автоматически активирует пробный период!",
        reply_markup=get_main_menu_keyboard(user_id)
    )
    
    try:
        await callback.bot.send_message(
            ADMIN_ID,
            f"🎉 Новый пользователь:\n"
            f"ID: {user_id}\n"
            f"Username: @{callback.from_user.username or 'нет'}\n"
            f"Имя: {callback.from_user.first_name}"
        )
    except:
        pass

@router.callback_query(F.data == "main_menu")
async def main_menu(callback: CallbackQuery):
    """Главное меню"""
    user_id = callback.from_user.id
    user = db.get_user(user_id)
    
    if user['is_blocked']:
        await callback.answer("🚫 Ваш аккаунт заблокирован", show_alert=True)
        return
    
    stars_emoji = "⭐" * min(db.get_stars_balance(user_id) // 10, 5)
    await callback.message.edit_text(
        f"🏠 <b>Главное меню</b>\n\n"
        f"{format_subscription_info(user)}\n"
        f"{stars_emoji} Stars: {db.get_stars_balance(user_id)}\n\n"
        "Выберите действие:",
        reply_markup=get_main_menu_keyboard(user_id)
    )

@router.callback_query(F.data == "stats")
async def show_stats(callback: CallbackQuery):
    """Показ статистики пользователя"""
    user_id = callback.from_user.id
    user = db.get_user(user_id)
    connections = db.get_user_connections(user_id)
    
    stats_text = f"""
📊 <b>Ваша статистика</b>

<b>Статус:</b> {format_subscription_info(user)}
<b>⭐ Stars:</b> {user['stars_balance']}
<b>👥 Рефералов:</b> {user['total_referrals']} (заработано {user['referral_earnings']} ⭐)

<b>📱 Подключения:</b> {len(connections)}
<b>💬 Сообщений:</b> {user['total_messages_saved']}
<b>🗑 Удалений:</b> {user['total_deletions_tracked']}
<b>✏️ Изменений:</b> {user['total_edits_tracked']}

<b>📸 Медиафайлов:</b> {user['total_media_saved']}
├ Фото: {user['total_photo']}
├ Видео: {user['total_video']}
├ Кружки: {user['total_video_note']}
├ Документы: {user['total_document']}
├ Аудио: {user['total_audio']}
├ Голосовые: {user['total_voice']}
└ Стикеры: {user['total_sticker']}

<b>📅 Зарегистрирован:</b> {user['registered_at'][:10]}
<b>🕐 Последняя активность:</b> {user['last_activity'][:16]}
    """
    
    await callback.message.edit_text(stats_text, reply_markup=get_back_keyboard())

@router.callback_query(F.data == "my_stars")
async def show_stars(callback: CallbackQuery):
    """Показ информации о Stars"""
    user_id = callback.from_user.id
    balance = db.get_stars_balance(user_id)
    
    stars_emoji = "⭐" * min(balance // 10, 5)
    
    text = f"""
⭐ <b>Telegram Stars</b>

{stars_emoji}

<b>Ваш баланс:</b> {balance} ⭐

<b>Что такое Stars?</b>
Stars - виртуальная валюта Telegram для оплаты подписок и услуг.

<b>Как получить?</b>
• Купить в Telegram (@PremiumBot)
• Пригласить друзей (20% от их оплат)
• Подарок от админа

<b>Как использовать?</b>
• Оплатить подписку бота
• Получить реферальные бонусы

<b>Цены подписок:</b>
📅 Неделя - {WEEKLY_PRICE} ⭐
💎 Месяц - {MONTHLY_PRICE} ⭐
💼 Квартал - {QUARTERLY_PRICE} ⭐ (скидка 20%)
👑 Год - {YEARLY_PRICE} ⭐ (скидка 33%)
    """
    
    await callback.message.edit_text(text, reply_markup=get_back_keyboard())

@router.callback_query(F.data == "referrals")
async def show_referrals(callback: CallbackQuery):
    """Показ реферальной системы"""
    user_id = callback.from_user.id
    user = db.get_user(user_id)
    ref_stats = db.get_referral_stats(user_id)
    
    ref_link = f"https://t.me/{callback.bot.username}?start={user['referral_code']}"
    
    text = f"""
👥 <b>Реферальная программа</b>

<b>Ваша реферальная ссылка:</b>
<code>{ref_link}</code>

<b>Статистика:</b>
• Приглашено: {ref_stats['total']} чел.
• Заработано: {ref_stats['earnings']} ⭐

<b>Как это работает?</b>
1. Делитесь ссылкой с друзьями
2. Они регистрируются по вашей ссылке
3. При их оплате вы получаете {REFERRAL_BONUS_PERCENT}% в Stars

<b>Пример:</b>
Друг купил подписку за 50 ⭐
Вы получили: {int(50 * REFERRAL_BONUS_PERCENT / 100)} ⭐

<b>Ваши рефералы:</b>
"""
    
    if ref_stats['referrals']:
        for i, ref in enumerate(ref_stats['referrals'][:5], 1):
            sub_emoji = {
                'free': '🆓',
                'trial': '🎁',
                'weekly': '📅',
                'monthly': '💎',
                'quarterly': '💼',
                'yearly': '👑',
                'lifetime': '♾️'
            }.get(ref['subscription_type'], '❓')
            
            name = ref['first_name'] or "Пользователь"
            text += f"{i}. {sub_emoji} {name}\n"
    else:
        text += "Пока никого\n"
    
    text += "\n💡 Пригласите друзей и зарабатывайте Stars!"
    
    await callback.message.edit_text(text, reply_markup=get_back_keyboard())

@router.callback_query(F.data == "subscription")
async def show_subscription(callback: CallbackQuery):
    """Управление подпиской"""
    user_id = callback.from_user.id
    user = db.get_user(user_id)
    balance = db.get_stars_balance(user_id)
    
    text = f"""
💎 <b>Управление подпиской</b>

<b>Текущий статус:</b>
{format_subscription_info(user)}

<b>Ваш баланс:</b> {balance} ⭐

<b>Доступные планы:</b>

📅 <b>Неделя</b> - {WEEKLY_PRICE} ⭐
Попробуйте на 7 дней

💎 <b>Месяц</b> - {MONTHLY_PRICE} ⭐
Оптимальный выбор

💼 <b>Квартал</b> - {QUARTERLY_PRICE} ⭐
Экономия {int((MONTHLY_PRICE * 3 - QUARTERLY_PRICE) / (MONTHLY_PRICE * 3) * 100)}%

👑 <b>Год</b> - {YEARLY_PRICE} ⭐
Максимальная экономия {int((MONTHLY_PRICE * 12 - YEARLY_PRICE) / (MONTHLY_PRICE * 12) * 100)}%

<b>Что входит:</b>
✅ Все функции бота
✅ Неограниченное хранилище
✅ Приоритетная поддержка
✅ Реферальные бонусы

Выберите подходящий план:
    """
    
    await callback.message.edit_text(
        text,
        reply_markup=get_subscription_keyboard(user['trial_used'], balance > 0)
    )

@router.callback_query(F.data.startswith("sub_"))
async def process_subscription_payment(callback: CallbackQuery):
    """Обработка выбора подписки и создание инвойса"""
    user_id = callback.from_user.id
    plan = callback.data.split("_")[1]
    
    prices_map = {
        "weekly": (WEEKLY_PRICE, "Недельная подписка"),
        "monthly": (MONTHLY_PRICE, "Месячная подписка"),
        "quarterly": (QUARTERLY_PRICE, "Квартальная подписка"),
        "yearly": (YEARLY_PRICE, "Годовая подписка")
    }
    
    if plan not in prices_map:
        await callback.answer("❌ Неверный план")
        return
    
    amount, title = prices_map[plan]
    
    # Создаем инвойс для оплаты в Stars
    try:
        await callback.bot.send_invoice(
            chat_id=user_id,
            title=title,
            description=f"Оплата подписки на {plan.replace('_', ' ')}",
            payload=f"subscription_{plan}_{user_id}",
            provider_token="",  # Для XTR оставляем пустым
            currency="XTR",
            prices=[LabeledPrice(label="XTR", amount=amount)],
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Оплатить ⭐", pay=True)]
            ])
        )
        await callback.answer("✅ Инвойс создан!")
    except Exception as e:
        logger.error(f"Ошибка создания инвойса: {e}")
        await callback.answer("❌ Ошибка создания платежа", show_alert=True)

@router.pre_checkout_query()
async def process_pre_checkout_query(pre_checkout_query: PreCheckoutQuery):
    """Обработка pre-checkout запроса"""
    await pre_checkout_query.bot.answer_pre_checkout_query(
        pre_checkout_query.id, 
        ok=True
    )

@router.message(F.successful_payment)
async def process_successful_payment(message: Message):
    """Обработка успешного платежа"""
    user_id = message.from_user.id
    payment = message.successful_payment
    
    # Парсим payload
    payload_parts = payment.invoice_payload.split("_")
    if len(payload_parts) < 2:
        logger.error(f"Неверный payload: {payment.invoice_payload}")
        return
    
    plan_type = payload_parts[1]
    amount_stars = payment.total_amount
    
    # Сохраняем платеж
    db.save_payment(
        user_id=user_id,
        amount_stars=amount_stars,
        plan_type=plan_type,
        payment_charge_id=payment.provider_payment_charge_id,
        telegram_payment_charge_id=payment.telegram_payment_charge_id
    )
    
    # Активируем подписку
    db.activate_subscription(user_id, plan_type)
    
    # Обрабатываем реферальный бонус
    db.process_referral_payment(user_id, amount_stars)
    
    user = db.get_user(user_id)
    
    await message.answer(
        f"🎉 <b>Оплата успешна!</b>\n\n"
        f"Подписка активирована: {format_subscription_info(user)}\n\n"
        f"Спасибо за использование бота!"
    )
    
    # Уведомляем админа
    try:
        await message.bot.send_message(
            ADMIN_ID,
            f"💰 Новый платеж!\n"
            f"User: {user_id}\n"
            f"Plan: {plan_type}\n"
            f"Amount: {amount_stars} ⭐"
        )
    except:
        pass

@router.callback_query(F.data == "connections")
async def show_connections(callback: CallbackQuery):
    """Показ подключений"""
    user_id = callback.from_user.id
    connections = db.get_user_connections(user_id)
    
    if not connections:
        text = """
🔗 <b>Мои подключения</b>

У вас нет активных подключений.

<b>Как подключить бота:</b>
1. Настройки → Чат-боты
2. Добавить чат-бота
3. @mrztnbot
4. Настройте параметры

После подключения бот начнет мониторинг!
        """
    else:
        text = f"🔗 <b>Мои подключения</b>\n\nАктивных: {len(connections)}\n\n"
        for i, conn in enumerate(connections, 1):
            status = "✅" if conn['is_enabled'] else "❌"
            text += f"{i}. {status} ID: {conn['connection_id'][:12]}...\n"
            text += f"   📅 {conn['created_at'][:10]}\n"
            text += f"   💬 Ответы: {'Да' if conn['can_reply'] else 'Нет'}\n\n"
    
    await callback.message.edit_text(text, reply_markup=get_back_keyboard())

@router.callback_query(F.data == "settings")
async def show_settings(callback: CallbackQuery):
    """Показ настроек"""
    await callback.message.edit_text(
        "⚙️ <b>Настройки</b>\n\n"
        "Выберите раздел:",
        reply_markup=get_settings_keyboard()
    )

@router.callback_query(F.data == "settings_notifications")
async def settings_notifications(callback: CallbackQuery):
    """Настройки уведомлений"""
    user_id = callback.from_user.id
    user = db.get_user(user_id)
    
    text = """
🔔 <b>Настройки уведомлений</b>

Настройте, какие события будут присылать вам уведомления:

✅ - уведомления включены
❌ - уведомления отключены
    """
    
    await callback.message.edit_text(
        text,
        reply_markup=get_notifications_settings_keyboard(user)
    )

@router.callback_query(F.data.startswith("toggle_notify_"))
async def toggle_notification(callback: CallbackQuery):
    """Переключение настройки уведомления"""
    user_id = callback.from_user.id
    setting = callback.data.replace("toggle_", "")
    
    user = db.get_user(user_id)
    current_value = user[setting]
    new_value = not current_value
    
    db.update_notification_settings(user_id, setting, new_value)
    
    user = db.get_user(user_id)
    await callback.message.edit_reply_markup(
        reply_markup=get_notifications_settings_keyboard(user)
    )
    await callback.answer(f"{'✅ Включено' if new_value else '❌ Отключено'}")

@router.callback_query(F.data == "settings_export")
async def settings_export(callback: CallbackQuery):
    """Экспорт данных"""
    await callback.answer("Функция в разработке", show_alert=True)

@router.callback_query(F.data == "settings_cleanup")
async def settings_cleanup(callback: CallbackQuery):
    """Очистка данных"""
    await callback.answer("Функция в разработке", show_alert=True)

@router.callback_query(F.data == "help")
async def show_help(callback: CallbackQuery):
    """Помощь"""
    help_text = """
ℹ️ <b>Справка</b>

<b>Как работает:</b>
Бот мониторит ваши чаты и сохраняет все сообщения, включая удаленные и с таймерами.

<b>Основные функции:</b>
• 📝 Сохранение всех сообщений
• 🗑 Уведомления об удалении
• ✏️ Отслеживание изменений
• 📸 Сохранение медиа с таймерами
• ⏱ Кружки с таймерами
• 📦 Экспорт чатов
• 👥 Реферальная система

<b>Подписка:</b>
Первые 3 дня бесплатно автоматически!
Далее от 20 ⭐ в неделю

<b>Рефералы:</b>
Приглашайте друзей и получайте 20% от их оплат

<b>Команды:</b>
/start - Главное меню
/help - Справка

<b>Поддержка:</b>
@""" + ADMIN_USERNAME + """
    """
    
    await callback.message.edit_text(help_text, reply_markup=get_back_keyboard())

# ========================================
# ADMIN ПАНЕЛЬ
# ========================================

@router.callback_query(F.data == "admin_panel")
async def admin_panel(callback: CallbackQuery):
    """Админ-панель"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Доступ запрещен", show_alert=True)
        return
    
    total_users = db.get_user_count()
    active_subs = db.get_active_subscriptions_count()
    
    await callback.message.edit_text(
        f"👨‍💼 <b>Админ-панель</b>\n\n"
        f"👥 Пользователей: {total_users}\n"
        f"💎 Активных подписок: {active_subs}\n\n"
        "Выберите действие:",
        reply_markup=get_admin_keyboard()
    )

@router.callback_query(F.data == "admin_users")
@router.callback_query(F.data.startswith("users_page_"))
async def admin_users(callback: CallbackQuery):
    """Список пользователей с пагинацией"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Доступ запрещен", show_alert=True)
        return
    
    if callback.data.startswith("users_page_"):
        try:
            page = int(callback.data.split("_")[-1])
        except:
            page = 0
    else:
        page = 0
    
    total_users = db.get_user_count()
    total_pages = (total_users + USERS_PER_PAGE - 1) // USERS_PER_PAGE
    
    offset = page * USERS_PER_PAGE
    users = db.get_all_users(limit=USERS_PER_PAGE, offset=offset)
    
    text = f"👥 <b>Пользователи</b> (стр. {page+1}/{total_pages})\n\n"
    
    start_index = offset + 1
    for i, user in enumerate(users, start=start_index):
        text += format_user_short(user, i) + "\n\n"
    
    text += f"\n<i>Всего пользователей: {total_users}</i>"
    
    await callback.message.edit_text(
        text,
        reply_markup=get_users_list_keyboard(page, total_pages)
    )

@router.callback_query(F.data == "users_page_info")
async def users_page_info(callback: CallbackQuery):
    """Информация о текущей странице"""
    await callback.answer("Используйте стрелки для навигации", show_alert=False)

@router.callback_query(F.data == "select_user_by_number")
async def select_user_by_number(callback: CallbackQuery, state: FSMContext):
    """Выбор пользователя по номеру"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Доступ запрещен", show_alert=True)
        return
    
    await callback.message.edit_text(
        "🔢 <b>Выбор пользователя</b>\n\n"
        "Отправьте номер пользователя из списка:\n"
        "(число от 1 до общего количества пользователей)\n\n"
        "Отправьте /cancel для отмены"
    )
    await state.set_state(AdminStates.user_number_input)

@router.message(AdminStates.user_number_input)
async def process_user_number(message: Message, state: FSMContext):
    """Обработка номера пользователя"""
    if message.from_user.id != ADMIN_ID:
        return
    
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Отменено", reply_markup=get_admin_keyboard())
        return
    
    try:
        number = int(message.text)
        total_users = db.get_user_count()
        
        if number < 1 or number > total_users:
            await message.answer(
                f"❌ Неверный номер!\n"
                f"Введите число от 1 до {total_users}"
            )
            return
        
        users = db.get_all_users(limit=1, offset=number-1)
        if not users:
            await message.answer("❌ Пользователь не найден")
            return
        
        user = users[0]
        await state.clear()
        await show_user_details(message, user['user_id'])
        
    except ValueError:
        await message.answer("❌ Введите корректное число!")

async def show_user_details(message: Message, user_id: int):
    """Показ детальной информации о пользователе"""
    user = db.get_user(user_id)
    if not user:
        await message.answer("❌ Пользователь не найден")
        return
    
    connections = db.get_user_connections(user_id)
    
    text = f"""
👤 <b>Пользователь #{user_id}</b>

<b>Имя:</b> {user['first_name'] or 'Не указано'}
<b>Username:</b> @{user['username'] or 'нет'}
<b>Статус:</b> {format_subscription_info(user)}
<b>⭐ Stars:</b> {user['stars_balance']}
<b>👥 Рефералов:</b> {user['total_referrals']} (заработано {user['referral_earnings']} ⭐)

<b>Статистика:</b>
📱 Подключений: {len(connections)}
💬 Сообщений: {user['total_messages_saved']}
🗑 Удалений: {user['total_deletions_tracked']}
✏️ Изменений: {user['total_edits_tracked']}
📸 Медиа: {user['total_media_saved']}

<b>Регистрация:</b> {user['registered_at'][:10]}
<b>Активность:</b> {user['last_activity'][:16]}

Выберите действие:
    """
    
    await message.answer(text, reply_markup=get_user_management_keyboard(user_id))

@router.callback_query(F.data.startswith("manage_user_"))
async def manage_user_callback(callback: CallbackQuery):
    """Обработка управления пользователем через callback"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Доступ запрещен", show_alert=True)
        return
    
    user_id = int(callback.data.split("_")[-1])
    user = db.get_user(user_id)
    
    if not user:
        await callback.answer("❌ Пользователь не найден", show_alert=True)
        return
    
    connections = db.get_user_connections(user_id)
    
    text = f"""
👤 <b>Пользователь #{user_id}</b>

<b>Имя:</b> {user['first_name'] or 'Не указано'}
<b>Username:</b> @{user['username'] or 'нет'}
<b>Статус:</b> {format_subscription_info(user)}
<b>⭐ Stars:</b> {user['stars_balance']}
<b>👥 Рефералов:</b> {user['total_referrals']} (заработано {user['referral_earnings']} ⭐)

<b>Статистика:</b>
📱 Подключений: {len(connections)}
💬 Сообщений: {user['total_messages_saved']}
🗑 Удалений: {user['total_deletions_tracked']}
✏️ Изменений: {user['total_edits_tracked']}
📸 Медиа: {user['total_media_saved']}

<b>Регистрация:</b> {user['registered_at'][:10]}
<b>Активность:</b> {user['last_activity'][:16]}

Выберите действие:
    """
    
    await callback.message.edit_text(text, reply_markup=get_user_management_keyboard(user_id))

@router.callback_query(F.data.startswith("admin_msg_"))
async def admin_send_message(callback: CallbackQuery, state: FSMContext):
    """Отправка сообщения пользователю"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Доступ запрещен", show_alert=True)
        return
    
    user_id = int(callback.data.split("_")[-1])
    await state.update_data(target_user_id=user_id)
    
    await callback.message.edit_text(
        f"💬 <b>Отправка сообщения пользователю {user_id}</b>\n\n"
        "Отправьте текст сообщения или /cancel для отмены:"
    )
    await state.set_state(AdminStates.send_message)

@router.message(AdminStates.send_message)
async def process_admin_message(message: Message, state: FSMContext):
    """Обработка отправки сообщения от админа"""
    if message.from_user.id != ADMIN_ID:
        return
    
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Отменено")
        return
    
    data = await state.get_data()
    target_user_id = data.get('target_user_id')
    
    try:
        await message.bot.send_message(
            target_user_id,
            f"📨 <b>Сообщение от администратора:</b>\n\n{message.text}"
        )
        await message.answer("✅ Сообщение отправлено!")
        db.log_admin_action(ADMIN_ID, target_user_id, 'message_sent', message.text[:100])
    except Exception as e:
        await message.answer(f"❌ Ошибка отправки: {e}")
    
    await state.clear()

@router.callback_query(F.data.startswith("admin_gift_"))
async def admin_gift_subscription(callback: CallbackQuery):
    """Подарок подписки"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Доступ запрещен", show_alert=True)
        return
    
    user_id = int(callback.data.split("_")[-1])
    
    await callback.message.edit_text(
        f"🎁 <b>Подарок подписки для {user_id}</b>\n\n"
        "Выберите тип подписки:",
        reply_markup=get_gift_subscription_keyboard(user_id)
    )

@router.callback_query(F.data.startswith("gift_sub_"))
async def process_gift_subscription(callback: CallbackQuery):
    """Обработка подарка подписки"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Доступ запрещен", show_alert=True)
        return
    
    parts = callback.data.split("_")
    user_id = int(parts[2])
    plan_type = parts[3]
    days = int(parts[4]) if parts[4] != '0' else None
    
    db.activate_subscription(user_id, plan_type, days)
    
    bonus_stars = {
        'trial': 5,
        'monthly': 10,
        'quarterly': 25,
        'yearly': 50,
        'lifetime': 100
    }.get(plan_type, 0)
    
    if bonus_stars > 0:
        db.add_stars(user_id, bonus_stars, f"Бонус за подписку {plan_type}")
    
    plan_names = {
        'trial': 'Пробный период (7 дней)',
        'monthly': 'Месячная подписка',
        'quarterly': 'Квартальная подписка',
        'yearly': 'Годовая подписка',
        'lifetime': 'Вечная подписка'
    }
    
    await callback.message.edit_text(
        f"✅ <b>Подписка подарена!</b>\n\n"
        f"Пользователь: {user_id}\n"
        f"План: {plan_names.get(plan_type)}\n"
        f"🎁 Бонус: {bonus_stars} ⭐",
        reply_markup=get_user_management_keyboard(user_id)
    )
    
    try:
        await callback.bot.send_message(
            user_id,
            f"🎉 <b>Вам подарена подписка!</b>\n\n"
            f"План: {plan_names.get(plan_type)}\n"
            f"🎁 Бонус: {bonus_stars} ⭐\n\n"
            f"Спасибо за использование бота!"
        )
    except:
        pass

@router.callback_query(F.data.startswith("admin_stars_"))
async def admin_send_stars(callback: CallbackQuery, state: FSMContext):
    """Отправка Stars пользователю"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Доступ запрещен", show_alert=True)
        return
    
    user_id = int(callback.data.split("_")[-1])
    await state.update_data(target_user_id=user_id)
    
    await callback.message.edit_text(
        f"⭐ <b>Отправка Stars пользователю {user_id}</b>\n\n"
        "Отправьте количество Stars (число) или /cancel:"
    )
    await state.set_state(AdminStates.send_stars)

@router.message(AdminStates.send_stars)
async def process_admin_stars(message: Message, state: FSMContext):
    """Обработка отправки Stars"""
    if message.from_user.id != ADMIN_ID:
        return
    
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Отменено")
        return
    
    try:
        amount = int(message.text)
        if amount <= 0:
            await message.answer("❌ Количество должно быть положительным!")
            return
        
        data = await state.get_data()
        target_user_id = data.get('target_user_id')
        
        db.add_stars(target_user_id, amount, "Подарок от администратора")
        
        await message.answer(f"✅ Отправлено {amount} ⭐ пользователю {target_user_id}")
        
        try:
            await message.bot.send_message(
                target_user_id,
                f"🎁 <b>Вы получили Stars!</b>\n\n"
                f"Количество: {amount} ⭐\n"
                f"От: Администратор\n\n"
                f"Спасибо за использование бота!"
            )
        except:
            pass
        
        await state.clear()
        
    except ValueError:
        await message.answer("❌ Введите корректное число!")

@router.callback_query(F.data.startswith("admin_block_"))
async def admin_block_user(callback: CallbackQuery):
    """Блокировка пользователя"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Доступ запрещен", show_alert=True)
        return
    
    user_id = int(callback.data.split("_")[-1])
    db.block_user(user_id)
    
    await callback.answer("✅ Пользователь заблокирован", show_alert=True)
    await callback.message.edit_reply_markup(reply_markup=get_user_management_keyboard(user_id))
    
    try:
        await callback.bot.send_message(
            user_id,
            "🚫 <b>Ваш аккаунт заблокирован</b>\n\n"
            f"Для разблокировки свяжитесь с @{ADMIN_USERNAME}"
        )
    except:
        pass

@router.callback_query(F.data.startswith("admin_unblock_"))
async def admin_unblock_user(callback: CallbackQuery):
    """Разблокировка пользователя"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Доступ запрещен", show_alert=True)
        return
    
    user_id = int(callback.data.split("_")[-1])
    db.unblock_user(user_id)
    
    await callback.answer("✅ Пользователь разблокирован", show_alert=True)
    await callback.message.edit_reply_markup(reply_markup=get_user_management_keyboard(user_id))
    
    try:
        await callback.bot.send_message(
            user_id,
            "✅ <b>Ваш аккаунт разблокирован!</b>\n\n"
            "Вы снова можете пользоваться ботом."
        )
    except:
        pass

@router.callback_query(F.data.startswith("admin_history_"))
async def admin_user_history(callback: CallbackQuery):
    """История действий с пользователем"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Доступ запрещен", show_alert=True)
        return
    
    user_id = int(callback.data.split("_")[-1])
    history = db.get_user_admin_history(user_id)
    
    text = f"📜 <b>История действий</b>\nПользователь: {user_id}\n\n"
    
    if not history:
        text += "История пуста"
    else:
        for action in history[:10]:
            text += f"• {action['action_type']}\n"
            text += f"  📅 {action['created_at'][:16]}\n"
            if action['action_details']:
                text += f"  ℹ️ {action['action_details'][:50]}\n"
            text += "\n"
    
    builder = InlineKeyboardBuilder()
    builder.button(text="◀️ Назад", callback_data=f"manage_user_{user_id}")
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())

@router.callback_query(F.data == "admin_stats")
async def admin_stats(callback: CallbackQuery):
    """Статистика для админа"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Доступ запрещен", show_alert=True)
        return
    
    total_users = db.get_user_count()
    active_subs = db.get_active_subscriptions_count()
    total_messages = db.get_total_messages_saved()
    total_deletions = db.get_total_deletions_tracked()
    media_stats = db.get_total_media_by_type()
    
    text = f"""
📊 <b>Общая статистика</b>

<b>👥 Пользователи:</b>
Всего: {total_users}
Активных подписок: {active_subs}

<b>💬 Сообщения:</b>
Сохранено: {total_messages}
Удалений: {total_deletions}

<b>📸 Медиа по типам:</b>
"""
    
    for media_type, count in media_stats.items():
        emoji = {
            'photo': '📸',
            'video': '🎥',
            'video_note': '⭕',
            'document': '📄',
            'audio': '🎵',
            'voice': '🎤',
            'sticker': '🎨'
        }.get(media_type, '📎')
        text += f"{emoji} {media_type}: {count}\n"
    
    text += f"\n<b>🤖 Система:</b>\nВерсия: 6.0.0\nСтатус: Работает ✅"
    
    await callback.message.edit_text(text, reply_markup=get_back_keyboard())

@router.callback_query(F.data == "admin_broadcast")
async def admin_broadcast(callback: CallbackQuery, state: FSMContext):
    """Рассылка"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Доступ запрещен", show_alert=True)
        return
    
    await callback.message.edit_text(
        "📢 <b>Рассылка</b>\n\n"
        "Отправьте сообщение для рассылки всем пользователям\n"
        "или /cancel для отмены:"
    )
    await state.set_state(AdminStates.broadcast_message)

@router.message(AdminStates.broadcast_message)
async def process_broadcast(message: Message, state: FSMContext):
    """Обработка рассылки"""
    if message.from_user.id != ADMIN_ID:
        return
    
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Отменено")
        return
    
    users = db.get_all_users()
    success = 0
    failed = 0
    
    status_msg = await message.answer("📤 Начинаю рассылку...")
    
    for user in users:
        if user['is_blocked']:
            continue
        try:
            await message.bot.send_message(
                user['user_id'],
                f"📢 <b>Объявление:</b>\n\n{message.text}"
            )
            success += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            failed += 1
            logger.error(f"Ошибка рассылки {user['user_id']}: {e}")
    
    await status_msg.edit_text(
        f"✅ Рассылка завершена!\n\n"
        f"Успешно: {success}\n"
        f"Не удалось: {failed}"
    )
    await state.clear()

@router.callback_query(F.data == "admin_search")
async def admin_search(callback: CallbackQuery, state: FSMContext):
    """Поиск пользователя"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Доступ запрещен", show_alert=True)
        return
    
    await callback.message.edit_text(
        "🔍 <b>Поиск пользователя</b>\n\n"
        "Отправьте ID, username или имя для поиска\n"
        "или /cancel для отмены:"
    )
    await state.set_state(AdminStates.search_user)

@router.message(AdminStates.search_user)
async def process_search(message: Message, state: FSMContext):
    """Обработка поиска"""
    if message.from_user.id != ADMIN_ID:
        return
    
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Отменено")
        return
    
    results = db.search_users(message.text)
    
    if not results:
        await message.answer("❌ Ничего не найдено")
        return
    
    text = f"🔍 <b>Результаты поиска</b> ({len(results)})\n\n"
    
    for i, user in enumerate(results[:10], 1):
        text += format_user_short(user, i) + "\n\n"
    
    if len(results) > 10:
        text += f"\n<i>Показано первых 10 из {len(results)}</i>"
    
    builder = InlineKeyboardBuilder()
    builder.button(text="🔢 Выбрать по номеру", callback_data="select_user_by_number")
    builder.button(text="◀️ Назад", callback_data="admin_panel")
    builder.adjust(1)
    
    await message.answer(text, reply_markup=builder.as_markup())
    await state.clear()

# ========================================
# ОБРАБОТКА BUSINESS API
# ========================================

@router.business_connection()
async def on_business_connection(business_connection: BusinessConnection, bot: Bot):
    """Обработка подключения"""
    try:
        user_id = business_connection.user.id
        connection_id = business_connection.id
        can_reply = business_connection.can_reply
        
        db.add_business_connection(
            connection_id=connection_id,
            user_id=user_id,
            connected_user_id=business_connection.user.id,
            can_reply=can_reply
        )
        
        # Автоматически активируем пробный период при первом подключении
        user = db.get_user(user_id)
        if user and not user['auto_trial_activated']:
            trial_activated = db.activate_auto_trial(user_id)
            if trial_activated:
                logger.info(f"Автоматический пробный период активирован для {user_id}")
        
        logger.info(f"Подключение: {connection_id} для {user_id}")
        
        user = db.get_user(user_id)
        
        # Проверяем настройки уведомлений
        if user and user['notify_connections']:
            try:
                trial_msg = ""
                if user['auto_trial_activated'] and user['subscription_type'] == 'trial':
                    trial_msg = f"\n\n🎁 <b>Пробный период активирован!</b>\nДоступ ко всем функциям на {TRIAL_DAYS} дня"
                
                await bot.send_message(
                    user_id,
                    f"🎉 <b>Бот подключен!</b>\n\n"
                    f"Теперь я отслеживаю ваши чаты.\n\n"
                    f"✅ Сохранение сообщений\n"
                    f"✅ Отслеживание удалений\n"
                    f"✅ Сохранение медиа с таймерами\n"
                    f"✅ Мгновенные уведомления{trial_msg}\n\n"
                    f"ID: <code>{connection_id[:16]}...</code>"
                )
            except Exception as e:
                logger.error(f"Ошибка уведомления о подключении: {e}")
        
        try:
            await bot.send_message(
                ADMIN_ID,
                f"🔗 Новое подключение!\n"
                f"User: {user_id}\n"
                f"Connection: {connection_id}"
            )
        except:
            pass
            
    except Exception as e:
        logger.error(f"Ошибка обработки подключения: {e}")

@router.business_message()
async def on_business_message(message: Message, bot: Bot):
    """Обработка входящих сообщений с поддержкой таймеров"""
    try:
        if not message.business_connection_id:
            return
        
        connection = db.get_business_connection(message.business_connection_id)
        if not connection:
            logger.warning(f"Неизвестное подключение: {message.business_connection_id}")
            return
        
        user_id = connection['user_id']
        
        if not db.check_subscription(user_id):
            logger.info(f"Нет активной подписки: {user_id}")
            return
        
        media_type = None
        media_file_id = None
        media_file_path = None
        media_thumbnail_path = None
        has_timer = False
        timer_seconds = None
        is_view_once = False
        media_width = None
        media_height = None
        media_duration = None
        media_file_size = None
        caption = message.caption
        
        # Проверяем медиа с таймерами
        if hasattr(message, 'has_media_spoiler') and message.has_media_spoiler:
            has_timer = True
            is_view_once = True
        
        # Фото
        if message.photo:
            media_type = "photo"
            photo = message.photo[-1]
            media_file_id = photo.file_id
            media_width = photo.width
            media_height = photo.height
            media_file_size = photo.file_size
            media_file_path = await download_media(bot, media_file_id, media_type, user_id, has_timer)
        
        # Видео
        elif message.video:
            media_type = "video"
            video = message.video
            media_file_id = video.file_id
            media_width = video.width
            media_height = video.height
            media_duration = video.duration
            media_file_size = video.file_size
            media_file_path = await download_media(bot, media_file_id, media_type, user_id, has_timer)
            
            if video.thumbnail:
                media_thumbnail_path = await download_thumbnail(bot, video.thumbnail, user_id)
        
        # Кружки
        elif message.video_note:
            media_type = "video_note"
            video_note = message.video_note
            media_file_id = video_note.file_id
            media_duration = video_note.duration
            media_file_size = video_note.file_size
            has_timer = True
            timer_seconds = video_note.duration if video_note.duration else 60
            media_file_path = await download_media(bot, media_file_id, media_type, user_id, has_timer)
            
            if video_note.thumbnail:
                media_thumbnail_path = await download_thumbnail(bot, video_note.thumbnail, user_id)
        
        # Документы
        elif message.document:
            media_type = "document"
            media_file_id = message.document.file_id
            media_file_size = message.document.file_size
            media_file_path = await download_media(bot, media_file_id, media_type, user_id, has_timer)
        
        # Аудио
        elif message.audio:
            media_type = "audio"
            media_file_id = message.audio.file_id
            media_duration = message.audio.duration
            media_file_size = message.audio.file_size
            media_file_path = await download_media(bot, media_file_id, media_type, user_id, has_timer)
        
        # Голосовые
        elif message.voice:
            media_type = "voice"
            media_file_id = message.voice.file_id
            media_duration = message.voice.duration
            media_file_size = message.voice.file_size
            has_timer = True
            timer_seconds = message.voice.duration
            media_file_path = await download_media(bot, media_file_id, media_type, user_id, has_timer)
        
        # Стикеры
        elif message.sticker:
            media_type = "sticker"
            media_file_id = message.sticker.file_id
        
        # Сохраняем в БД
        db.save_message(
            user_id=user_id,
            connection_id=message.business_connection_id,
            chat_id=message.chat.id,
            message_id=message.message_id,
            sender_id=message.from_user.id,
            sender_username=message.from_user.username,
            sender_first_name=message.from_user.first_name,
            message_text=message.text or message.caption,
            media_type=media_type,
            media_file_id=media_file_id,
            media_file_path=media_file_path,
            media_thumbnail_path=media_thumbnail_path,
            caption=caption,
            has_timer=has_timer or is_view_once,
            timer_seconds=timer_seconds,
            is_view_once=is_view_once,
            media_width=media_width,
            media_height=media_height,
            media_duration=media_duration,
            media_file_size=media_file_size
        )
        
        logger.info(f"Сохранено сообщение {message.message_id} (тип: {media_type}, таймер: {has_timer})")
        
        # Уведомляем о медиа с таймером
        user = db.get_user(user_id)
        if user and user['notify_media_timers'] and (has_timer or is_view_once):
            try:
                await bot.send_message(
                    user_id,
                    f"⏱ <b>Сохранено медиа с таймером!</b>\n\n"
                    f"Тип: {media_type}\n"
                    f"{'Одноразовый просмотр' if is_view_once else f'Таймер: {timer_seconds}с'}\n"
                    f"От: {message.from_user.first_name or 'Пользователь'}"
                )
            except:
                pass
        
    except Exception as e:
        logger.error(f"Ошибка обработки сообщения: {e}", exc_info=True)

@router.edited_business_message()
async def on_edited_business_message(message: Message, bot: Bot):
    """Обработка отредактированных сообщений"""
    try:
        if not message.business_connection_id:
            return
        
        connection = db.get_business_connection(message.business_connection_id)
        if not connection:
            return
        
        user_id = connection['user_id']
        user = db.get_user(user_id)
        
        if not user or not user['notify_edits']:
            return
        
        original = db.get_message(user_id, message.chat.id, message.message_id)
        if not original:
            logger.warning(f"Оригинал не найден: {message.message_id}")
            return
        
        original_text = original['message_text'] or ""
        new_text = message.text or message.caption or ""
        
        db.mark_message_edited(user_id, message.chat.id, message.message_id, original_text)
        
        sender_name = message.from_user.first_name or f"User {message.from_user.id}"
        
        # Форматируем в цитату
        notification = f"✏️ <b>Сообщение изменено</b>\n\n"
        notification += f"От: {sender_name}\n"
        notification += f"Чат: {message.chat.title or message.chat.first_name or 'ЛС'}\n"
        notification += f"Время: {datetime.now().strftime('%H:%M:%S')}\n\n"
        
        if original_text:
            notification += f"<blockquote>Было:\n{original_text[:200]}</blockquote>\n\n"
        if new_text:
            notification += f"<blockquote>Стало:\n{new_text[:200]}</blockquote>"
        
        try:
            await bot.send_message(user_id, notification[:4000], parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.error(f"Ошибка уведомления об изменении: {e}")
        
        logger.info(f"Изменение {message.message_id} пользователя {user_id}")
        
    except Exception as e:
        logger.error(f"Ошибка обработки изменения: {e}", exc_info=True)

@router.deleted_business_messages()
async def on_deleted_business_messages(deleted: BusinessMessagesDeleted, bot: Bot):
    """Обработка удаленных сообщений"""
    try:
        connection_id = deleted.business_connection_id
        chat = deleted.chat
        message_ids = deleted.message_ids
        
        logger.info(f"Удаление: connection={connection_id}, chat={chat.id}, messages={message_ids}")
        
        connection = db.get_business_connection(connection_id)
        if not connection:
            logger.warning(f"Неизвестное подключение при удалении: {connection_id}")
            return
        
        user_id = connection['user_id']
        user = db.get_user(user_id)
        
        if not user or not user['notify_deletions']:
            # Все равно отмечаем как удаленные
            for message_id in message_ids:
                db.mark_message_deleted(user_id, chat.id, message_id)
            return
        
        # Если удалено больше 5 сообщений, экспортируем в архив
        if len(message_ids) > 5:
            messages = []
            for message_id in message_ids:
                saved_msg = db.get_message(user_id, chat.id, message_id)
                if saved_msg:
                    db.mark_message_deleted(user_id, chat.id, message_id)
                    messages.append(saved_msg)
            
            if messages:
                chat_title = chat.title or chat.first_name or f"Chat {chat.id}"
                archive_path = await export_deleted_chat_to_archive(
                    user_id, chat.id, messages, chat_title
                )
                
                if archive_path:
                    try:
                        await bot.send_document(
                            user_id,
                            FSInputFile(archive_path),
                            caption=f"🗑 <b>Удален чат</b>\n\n"
                                   f"Чат: {chat_title}\n"
                                   f"Сообщений: {len(messages)}\n\n"
                                   f"Все сообщения и медиафайлы сохранены в архиве"
                        )
                    except Exception as e:
                        logger.error(f"Ошибка отправки архива: {e}")
            return
        
        # Если удалено мало сообщений, отправляем по отдельности
        for message_id in message_ids:
            saved_msg = db.get_message(user_id, chat.id, message_id)
            
            if saved_msg:
                db.mark_message_deleted(user_id, chat.id, message_id)
                
                sender_name = saved_msg['sender_first_name'] or f"User {saved_msg['sender_id']}"
                
                # Форматируем в цитату
                notification = f"🗑 <b>Сообщение удалено</b>\n\n"
                notification += f"От: {sender_name}\n"
                notification += f"Чат: {chat.title or chat.first_name or 'ЛС'}\n"
                notification += f"Время: {datetime.now().strftime('%H:%M:%S')}\n\n"
                
                if saved_msg['message_text']:
                    notification += f"<blockquote>{saved_msg['message_text'][:300]}</blockquote>\n\n"
                
                if saved_msg['media_type']:
                    notification += f"<b>Медиа:</b> {saved_msg['media_type'].upper()}"
                    if saved_msg['has_timer']:
                        notification += f" [⏱ ТАЙМЕР]"
                    if saved_msg['is_view_once']:
                        notification += f" [👁 ОДНОРАЗОВЫЙ]"
                    notification += "\n"
                    
                    if saved_msg['caption']:
                        notification += f"<blockquote>Подпись: {saved_msg['caption'][:100]}</blockquote>\n"
                
                try:
                    await bot.send_message(user_id, notification[:4000], parse_mode=ParseMode.HTML)
                    
                    # Отправляем сохраненное медиа
                    if saved_msg['media_file_path'] and Path(saved_msg['media_file_path']).exists():
                        file = FSInputFile(saved_msg['media_file_path'])
                        
                        caption_text = "📎 Сохраненное медиа"
                        if saved_msg['has_timer']:
                            caption_text += " [⏱ было с таймером]"
                        
                        if saved_msg['media_type'] == 'photo':
                            await bot.send_photo(user_id, file, caption=caption_text)
                        elif saved_msg['media_type'] == 'video':
                            await bot.send_video(user_id, file, caption=caption_text)
                        elif saved_msg['media_type'] == 'video_note':
                            await bot.send_video_note(user_id, file)
                        elif saved_msg['media_type'] == 'document':
                            await bot.send_document(user_id, file, caption=caption_text)
                        elif saved_msg['media_type'] == 'audio':
                            await bot.send_audio(user_id, file, caption=caption_text)
                        elif saved_msg['media_type'] == 'voice':
                            await bot.send_voice(user_id, file, caption=caption_text)
                
                except Exception as e:
                    logger.error(f"Ошибка уведомления об удалении: {e}")
                
                logger.info(f"Удаление {message_id} пользователя {user_id}")
            else:
                logger.warning(f"Сообщение {message_id} не найдено в БД")
        
    except Exception as e:
        logger.error(f"Ошибка обработки удаленных сообщений: {e}", exc_info=True)

# ========================================
# ГЛАВНАЯ ФУНКЦИЯ
# ========================================

async def main():
    """Главная функция запуска бота"""
    try:
        bot = Bot(
            token=BOT_TOKEN,
            default=DefaultBotProperties(
                parse_mode=ParseMode.HTML
            )
        )
        
        dp = Dispatcher(storage=MemoryStorage())
        dp.include_router(router)
        
        bot_info = await bot.get_me()
        logger.info(f"🚀 Бот запущен: @{bot_info.username} (ID: {bot_info.id})")
        logger.info(f"📦 Версия: 6.0.0")
        logger.info(f"👨‍💼 Админ: {ADMIN_ID} (@{ADMIN_USERNAME})")
        logger.info(f"💎 Автор: Merzost?")
        
        try:
            await bot.send_message(
                ADMIN_ID,
                "🚀 <b>Бот запущен!</b>\n\n"
                f"Username: @{bot_info.username}\n"
                f"ID: {bot_info.id}\n"
                f"Версия: 6.0.0\n"
                f"Автор: Merzost?\n\n"
                f"✨ Новое в этой версии:\n"
                f"• Оплата через Telegram Stars\n"
                f"• Реферальная система (20% бонус)\n"
                f"• Настройки уведомлений\n"
                f"• Автопробный период {TRIAL_DAYS} дня\n"
                f"• Улучшенное форматирование (цитаты)\n"
                f"• Экспорт удаленных чатов в архив\n"
                f"• Полная поддержка таймеров\n"
                f"• Обновленная политика\n"
                f"• 30+ улучшений"
            )
        except Exception as e:
            logger.error(f"Не удалось отправить уведомление о запуске: {e}")
        
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
    finally:
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {e}", exc_info=True)
