#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram Business Message Monitor Bot
Version: 5.0.0
Author: Business Monitor Team
Date: 2026-03-01

Улучшенная версия с расширенным управлением пользователями,
сохранением медиа с таймерами и 20+ новыми функциями
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
    VideoNote
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

class SubscriptionStates(StatesGroup):
    """Состояния для управления подпиской"""
    choosing_plan = State()
    payment_confirmation = State()

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
                stars_balance INTEGER DEFAULT 0
            )
        ''')
        
        # Таблица бизнес-подключений
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
                amount REAL,
                currency TEXT DEFAULT 'RUB',
                plan_type TEXT,
                payment_method TEXT,
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info("База данных инициализирована (версия 5.0.0)")
    
    # ========================================
    # МЕТОДЫ ДЛЯ РАБОТЫ С ПОЛЬЗОВАТЕЛЯМИ
    # ========================================
    
    def add_user(self, user_id: int, username: str = None, first_name: str = None, last_name: str = None):
        """Добавление нового пользователя"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO users (user_id, username, first_name, last_name)
                VALUES (?, ?, ?, ?)
            ''', (user_id, username, first_name, last_name))
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления пользователя: {e}")
            return False
        finally:
            conn.close()
    
    def get_user(self, user_id: int) -> Optional[Dict]:
        """Получение данных пользователя"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
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
    
    def activate_trial(self, user_id: int):
        """Активация пробного периода"""
        conn = self.get_connection()
        cursor = conn.cursor()
        expires = datetime.now() + timedelta(days=7)
        cursor.execute('''
            UPDATE users 
            SET subscription_type = 'trial', 
                subscription_expires = ?,
                trial_used = 1
            WHERE user_id = ?
        ''', (expires, user_id))
        conn.commit()
        conn.close()
    
    def activate_subscription(self, user_id: int, plan_type: str, days: int = None):
        """Активация подписки"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        if days:
            expires = datetime.now() + timedelta(days=days)
        elif plan_type == 'monthly':
            expires = datetime.now() + timedelta(days=30)
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
        
        # Логируем действие
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
    
    # ========================================
    # МЕТОДЫ ДЛЯ РАБОТЫ С STARS
    # ========================================
    
    def add_stars(self, user_id: int, amount: int, description: str = ""):
        """Добавление звезд пользователю"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE users SET stars_balance = stars_balance + ? WHERE user_id = ?
        ''', (amount, user_id))
        
        cursor.execute('''
            INSERT INTO stars_transactions (user_id, amount, transaction_type, description)
            VALUES (?, ?, 'add', ?)
        ''', (user_id, amount, description))
        
        conn.commit()
        conn.close()
        
        self.log_admin_action(ADMIN_ID, user_id, 'stars_added', f'Amount: {amount}, Reason: {description}')
    
    def get_stars_balance(self, user_id: int) -> int:
        """Получение баланса звезд"""
        user = self.get_user(user_id)
        return user['stars_balance'] if user else 0
    
    # ========================================
    # МЕТОДЫ ДЛЯ РАБОТЫ С ПОДКЛЮЧЕНИЯМИ
    # ========================================
    
    def add_business_connection(self, connection_id: str, user_id: int, connected_user_id: int, can_reply: bool = False):
        """Добавление бизнес-подключения"""
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
            logger.error(f"Ошибка добавления бизнес-подключения: {e}")
            return False
        finally:
            conn.close()
    
    def get_business_connection(self, connection_id: str) -> Optional[Dict]:
        """Получение данных бизнес-подключения"""
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
                    sender_id: int, sender_username: str = None, message_text: str = None,
                    media_type: str = None, media_file_id: str = None, media_file_path: str = None,
                    media_thumbnail_path: str = None, caption: str = None, has_timer: bool = False, 
                    timer_seconds: int = None, is_view_once: bool = False,
                    media_width: int = None, media_height: int = None, 
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
                 message_text, media_type, media_file_id, media_file_path, media_thumbnail_path,
                 caption, has_timer, timer_seconds, timer_expires, is_view_once,
                 media_width, media_height, media_duration, media_file_size)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (user_id, connection_id, chat_id, message_id, sender_id, sender_username,
                  message_text, media_type, media_file_id, media_file_path, media_thumbnail_path,
                  caption, has_timer, timer_seconds, timer_expires, is_view_once,
                  media_width, media_height, media_duration, media_file_size))
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
                
                # Обновляем счетчик конкретного типа медиа
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
        
        # Создаем директорию для пользователя
        user_media_dir = MEDIA_DIR / str(user_id)
        user_media_dir.mkdir(exist_ok=True)
        
        # Генерируем уникальное имя файла
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_hash = hashlib.md5(file_id.encode()).hexdigest()[:8]
        timer_prefix = "timer_" if has_timer else ""
        filename = f"{timer_prefix}{file_type}_{timestamp}_{file_hash}.{file_extension}"
        file_path = user_media_dir / filename
        
        # Скачиваем файл
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

async def export_chat_to_text(user_id: int, chat_id: int, chat_title: str) -> Optional[str]:
    """Экспорт чата в текстовый файл"""
    try:
        messages = db.get_chat_messages(user_id, chat_id)
        
        if not messages:
            return None
        
        user_export_dir = EXPORTS_DIR / str(user_id)
        user_export_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"chat_{chat_id}_{timestamp}.txt"
        file_path = user_export_dir / filename
        
        content = f"Экспорт чата: {chat_title}\n"
        content += f"Дата экспорта: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        content += f"Всего сообщений: {len(messages)}\n"
        content += "=" * 80 + "\n\n"
        
        for msg in messages:
            timestamp_str = msg['created_at']
            sender = msg['sender_username'] or f"User {msg['sender_id']}"
            
            content += f"[{timestamp_str}] {sender}:\n"
            
            if msg['message_text']:
                content += f"{msg['message_text']}\n"
            
            if msg['media_type']:
                content += f"[{msg['media_type'].upper()}]"
                if msg['has_timer']:
                    content += f" [ТАЙМЕР: {msg['timer_seconds']}с]"
                if msg['is_view_once']:
                    content += " [ОДНОРАЗОВЫЙ ПРОСМОТР]"
                content += "\n"
                if msg['caption']:
                    content += f"Подпись: {msg['caption']}\n"
            
            if msg['is_edited']:
                content += f"(отредактировано: {msg['edited_at']})\n"
                if msg['original_text']:
                    content += f"Оригинал: {msg['original_text']}\n"
            
            if msg['is_deleted']:
                content += f"(УДАЛЕНО: {msg['deleted_at']})\n"
            
            content += "-" * 80 + "\n\n"
        
        async with aiofiles.open(file_path, 'w', encoding='utf-8') as f:
            await f.write(content)
        
        logger.info(f"Чат экспортирован: {file_path}")
        return str(file_path)
    except Exception as e:
        logger.error(f"Ошибка экспорта чата: {e}")
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
    elif sub_type == 'monthly':
        if user['subscription_expires']:
            expires = datetime.fromisoformat(user['subscription_expires'])
            return f"💎 Месячная (до {expires.strftime('%d.%m.%Y')})"
        return "💎 Месячная"
    elif sub_type == 'yearly':
        if user['subscription_expires']:
            expires = datetime.fromisoformat(user['subscription_expires'])
            return f"👑 Годовая (до {expires.strftime('%d.%m.%Y')})"
        return "👑 Годовая"
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
        'monthly': '💎',
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
    builder.button(text="⚙️ Настройки", callback_data="settings")
    builder.button(text="ℹ️ Помощь", callback_data="help")
    
    if user_id == ADMIN_ID:
        builder.button(text="👨‍💼 Админ", callback_data="admin_panel")
    
    builder.adjust(2)
    return builder.as_markup()

def get_subscription_keyboard(trial_used: bool) -> InlineKeyboardMarkup:
    """Клавиатура выбора подписки"""
    builder = InlineKeyboardBuilder()
    
    if not trial_used:
        builder.button(text="🎁 Пробный (7 дней)", callback_data="sub_trial")
    
    builder.button(text="💳 Месяц - 99₽", callback_data="sub_monthly")
    builder.button(text="💳 Год - 777₽", callback_data="sub_yearly")
    builder.button(text="◀️ Назад", callback_data="main_menu")
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
    
    # Кнопки навигации
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="◀️ Пред", callback_data=f"users_page_{page-1}"))
    nav_buttons.append(InlineKeyboardButton(text=f"📄 {page+1}/{total_pages}", callback_data="users_page_info"))
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton(text="След ▶️", callback_data=f"users_page_{page+1}"))
    
    for btn in nav_buttons:
        builder.add(btn)
    
    builder.row(InlineKeyboardButton(text="🔢 Выбрать по номеру", callback_data="select_user_by_number"))
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
    
    builder.button(text="🎁 7 дней", callback_data=f"gift_sub_{user_id}_trial_7")
    builder.button(text="💎 1 месяц", callback_data=f"gift_sub_{user_id}_monthly_30")
    builder.button(text="💎 3 месяца", callback_data=f"gift_sub_{user_id}_monthly_90")
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
    
    db.add_user(user_id, username, first_name, last_name)
    user = db.get_user(user_id)
    
    if user['is_blocked']:
        await message.answer(
            "🚫 <b>Ваш аккаунт заблокирован</b>\n\n"
            "Для разблокировки свяжитесь с администратором: @" + ADMIN_USERNAME
        )
        return
    
    if not user['accepted_terms']:
        await message.answer(
            "👋 <b>Добро пожаловать в Business Message Monitor!</b>\n\n"
            "🔐 Мониторинг удаленных и измененных сообщений\n"
            "📸 Сохранение медиафайлов с таймерами\n"
            "⚡ Мгновенные уведомления\n\n"
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
Бот мониторит бизнес-чаты и сохраняет сообщения, включая удаленные и с таймерами.

<b>2. ФУНКЦИОНАЛ</b>
• Сохранение удаленных сообщений
• Отслеживание изменений
• Сохранение медиа с таймерами
• Экспорт чатов

<b>3. ОГРАНИЧЕНИЯ</b>
• Только бизнес-подключения
• Не видит личные чаты
• Работает после подключения

<b>4. КОНФИДЕНЦИАЛЬНОСТЬ</b>
• Защищенное хранение
• Доступ только у владельца
• Возможность удаления

<b>5. ПОДПИСКИ</b>
• Пробный: 7 дней
• Месячная: 99₽
• Годовая: 777₽

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
        "1. Настройки → Telegram Business\n"
        "2. Чат-боты → Добавить\n"
        "3. Введите: @mrztnbot\n"
        "4. Настройте параметры\n\n"
        "После подключения бот начнет работу!",
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
└ Голосовые: {user['total_voice']}

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
Stars - виртуальная валюта бота для поощрений и бонусов.

<b>Как получить?</b>
• За активность
• Подарок от админа
• Специальные акции

<b>Что можно:</b>
• Обменять на подписку
• Получить бонусы
• Участвовать в конкурсах

<i>Следите за обновлениями!</i>
    """
    
    await callback.message.edit_text(text, reply_markup=get_back_keyboard())

@router.callback_query(F.data == "subscription")
async def show_subscription(callback: CallbackQuery):
    """Управление подпиской"""
    user_id = callback.from_user.id
    user = db.get_user(user_id)
    
    text = f"""
💎 <b>Управление подпиской</b>

<b>Текущий статус:</b>
{format_subscription_info(user)}

<b>Доступные планы:</b>
"""
    
    if not user['trial_used']:
        text += "\n🎁 <b>Пробный</b> - 7 дней бесплатно"
    
    text += """

💳 <b>Месячная</b> - 99₽
├ Все функции
├ Неограниченное хранилище
└ Приоритетная поддержка

💳 <b>Годовая</b> - 777₽
├ Все функции месячной
├ Экономия 411₽ (52%)
└ Специальные бонусы

Выберите подходящий план:
    """
    
    await callback.message.edit_text(
        text,
        reply_markup=get_subscription_keyboard(user['trial_used'])
    )

@router.callback_query(F.data == "sub_trial")
async def activate_trial_handler(callback: CallbackQuery):
    """Активация пробного периода"""
    user_id = callback.from_user.id
    user = db.get_user(user_id)
    
    if user['trial_used']:
        await callback.answer("❌ Вы уже использовали пробный период", show_alert=True)
        return
    
    db.activate_trial(user_id)
    db.add_stars(user_id, 10, "Бонус за активацию пробного периода")
    
    await callback.message.edit_text(
        "🎉 <b>Пробный период активирован!</b>\n\n"
        "✅ 7 дней полного доступа\n"
        "🎁 +10 Stars в подарок\n\n"
        "Наслаждайтесь! 🚀",
        reply_markup=get_back_keyboard()
    )
    
    try:
        await callback.bot.send_message(
            ADMIN_ID,
            f"🎁 Активирован пробный период:\n"
            f"ID: {user_id}\n"
            f"@{callback.from_user.username or 'нет'}"
        )
    except:
        pass

@router.callback_query(F.data.startswith("sub_"))
async def process_subscription(callback: CallbackQuery):
    """Обработка выбора подписки"""
    plan = callback.data.split("_")[1]
    
    if plan == "trial":
        return
    
    prices = {
        "monthly": "99₽",
        "yearly": "777₽"
    }
    
    await callback.message.edit_text(
        f"💳 <b>Оплата подписки</b>\n\n"
        f"План: {'Месячная' if plan == 'monthly' else 'Годовая'}\n"
        f"Стоимость: {prices.get(plan)}\n\n"
        f"<b>Для оплаты свяжитесь с администратором:</b>\n"
        f"@{ADMIN_USERNAME}\n\n"
        f"После оплаты подписка будет активирована.",
        reply_markup=get_back_keyboard()
    )

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
1. Настройки → Telegram Business
2. Чат-боты → Добавить
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
        "Доступные настройки:\n\n"
        "🔔 Уведомления - скоро\n"
        "📥 Экспорт - скоро\n"
        "🗑 Очистка - скоро\n"
        "🔐 Приватность - скоро",
        reply_markup=get_back_keyboard()
    )

@router.callback_query(F.data == "help")
async def show_help(callback: CallbackQuery):
    """Помощь"""
    help_text = """
ℹ️ <b>Справка</b>

<b>Как работает:</b>
Бот мониторит бизнес-чаты через Telegram Business API и сохраняет все сообщения, включая удаленные и с таймерами.

<b>Основные функции:</b>
• 📝 Сохранение всех сообщений
• 🗑 Уведомления об удалении
• ✏️ Отслеживание изменений
• 📸 Сохранение медиа с таймерами
• ⏱ Кружки с таймерами
• 📦 Экспорт чатов

<b>Команды:</b>
/start - Главное меню
/help - Справка

<b>Поддержка:</b>
@""" + ADMIN_USERNAME + """

<b>Stars:</b>
⭐ - виртуальная валюта бота
Получайте за активность!
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
    
    # Получаем номер страницы
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
        
        # Получаем пользователя по номеру (индекс = номер - 1)
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
    
    # Отправляем бонусные Stars
    bonus_stars = {
        'trial': 5,
        'monthly': 10,
        'yearly': 50,
        'lifetime': 100
    }.get(plan_type, 0)
    
    if bonus_stars > 0:
        db.add_stars(user_id, bonus_stars, f"Бонус за подписку {plan_type}")
    
    plan_names = {
        'trial': 'Пробный период (7 дней)',
        'monthly': 'Месячная подписка',
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
    
    # Уведомляем пользователя
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
        
        # Уведомляем пользователя
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
    
    # Уведомляем пользователя
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
    
    # Уведомляем пользователя
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
            'voice': '🎤'
        }.get(media_type, '📎')
        text += f"{emoji} {media_type}: {count}\n"
    
    text += f"\n<b>🤖 Система:</b>\nВерсия: 5.0.0\nСтатус: Работает ✅"
    
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
    """Обработка подключения бизнес-аккаунта"""
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
        
        logger.info(f"Бизнес-подключение: {connection_id} для {user_id}")
        
        try:
            await bot.send_message(
                user_id,
                f"🎉 <b>Бот подключен!</b>\n\n"
                f"Теперь я отслеживаю ваши бизнес-чаты.\n\n"
                f"✅ Сохранение сообщений\n"
                f"✅ Отслеживание удалений\n"
                f"✅ Сохранение медиа с таймерами\n"
                f"✅ Мгновенные уведомления\n\n"
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
    """Обработка входящих бизнес-сообщений с поддержкой таймеров"""
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
        
        # Определяем параметры медиа
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
        
        # Фото (включая с таймером)
        if message.photo:
            media_type = "photo"
            photo = message.photo[-1]  # Самое большое фото
            media_file_id = photo.file_id
            media_width = photo.width
            media_height = photo.height
            media_file_size = photo.file_size
            
            # Проверяем на таймер самоуничтожения
            if hasattr(message, 'has_media_spoiler') and message.has_media_spoiler:
                is_view_once = True
            
            media_file_path = await download_media(bot, media_file_id, media_type, user_id, has_timer)
        
        # Видео (включая с таймером)
        elif message.video:
            media_type = "video"
            video = message.video
            media_file_id = video.file_id
            media_width = video.width
            media_height = video.height
            media_duration = video.duration
            media_file_size = video.file_size
            
            if hasattr(message, 'has_media_spoiler') and message.has_media_spoiler:
                is_view_once = True
            
            media_file_path = await download_media(bot, media_file_id, media_type, user_id, has_timer)
            
            # Миниатюра
            if video.thumbnail:
                media_thumbnail_path = await download_thumbnail(bot, video.thumbnail, user_id)
        
        # Кружки (video_note) - поддержка таймеров
        elif message.video_note:
            media_type = "video_note"
            video_note = message.video_note
            media_file_id = video_note.file_id
            media_duration = video_note.duration
            media_file_size = video_note.file_size
            
            # Кружки часто имеют таймеры
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
            has_timer = True  # Голосовые часто с таймером
            timer_seconds = message.voice.duration
            media_file_path = await download_media(bot, media_file_id, media_type, user_id, has_timer)
        
        # Стикеры (не сохраняем файл, только ID)
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
        
    except Exception as e:
        logger.error(f"Ошибка обработки бизнес-сообщения: {e}", exc_info=True)

@router.edited_business_message()
async def on_edited_business_message(message: Message, bot: Bot):
    """Обработка отредактированных бизнес-сообщений"""
    try:
        if not message.business_connection_id:
            return
        
        connection = db.get_business_connection(message.business_connection_id)
        if not connection:
            return
        
        user_id = connection['user_id']
        
        original = db.get_message(user_id, message.chat.id, message.message_id)
        if not original:
            logger.warning(f"Оригинал не найден: {message.message_id}")
            return
        
        original_text = original['message_text'] or ""
        new_text = message.text or message.caption or ""
        
        db.mark_message_edited(user_id, message.chat.id, message.message_id, original_text)
        
        sender_name = message.from_user.username or f"User {message.from_user.id}"
        
        notification = f"✏️ <b>Сообщение изменено</b>\n\n"
        notification += f"От: @{sender_name}\n"
        notification += f"Чат: {message.chat.title or message.chat.first_name or 'ЛС'}\n"
        notification += f"Время: {datetime.now().strftime('%H:%M:%S')}\n\n"
        notification += f"<b>Было:</b>\n{original_text[:200]}\n\n"
        notification += f"<b>Стало:</b>\n{new_text[:200]}"
        
        try:
            await bot.send_message(user_id, notification[:4000])
        except Exception as e:
            logger.error(f"Ошибка уведомления об изменении: {e}")
        
        logger.info(f"Изменение {message.message_id} пользователя {user_id}")
        
    except Exception as e:
        logger.error(f"Ошибка обработки изменения: {e}", exc_info=True)

@router.deleted_business_messages()
async def on_deleted_business_messages(deleted: BusinessMessagesDeleted, bot: Bot):
    """Обработка удаленных бизнес-сообщений"""
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
        
        for message_id in message_ids:
            saved_msg = db.get_message(user_id, chat.id, message_id)
            
            if saved_msg:
                db.mark_message_deleted(user_id, chat.id, message_id)
                
                sender_name = saved_msg['sender_username'] or f"User {saved_msg['sender_id']}"
                
                notification = f"🗑 <b>Сообщение удалено</b>\n\n"
                notification += f"От: @{sender_name}\n"
                notification += f"Чат: {chat.title or chat.first_name or 'ЛС'}\n"
                notification += f"Время: {datetime.now().strftime('%H:%M:%S')}\n\n"
                
                if saved_msg['message_text']:
                    notification += f"<b>Текст:</b>\n{saved_msg['message_text'][:300]}\n\n"
                
                if saved_msg['media_type']:
                    notification += f"<b>Медиа:</b> {saved_msg['media_type'].upper()}"
                    if saved_msg['has_timer']:
                        notification += f" [⏱ ТАЙМЕР]"
                    if saved_msg['is_view_once']:
                        notification += f" [👁 ОДНОРАЗОВЫЙ]"
                    notification += "\n"
                    
                    if saved_msg['caption']:
                        notification += f"<b>Подпись:</b> {saved_msg['caption'][:100]}\n"
                
                try:
                    await bot.send_message(user_id, notification[:4000])
                    
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
        logger.info(f"📦 Версия: 5.0.0")
        logger.info(f"👨‍💼 Админ: {ADMIN_ID} (@{ADMIN_USERNAME})")
        
        try:
            await bot.send_message(
                ADMIN_ID,
                "🚀 <b>Бот запущен!</b>\n\n"
                f"Username: @{bot_info.username}\n"
                f"ID: {bot_info.id}\n"
                f"Версия: 5.0.0\n\n"
                f"✨ Новое в этой версии:\n"
                f"• Управление пользователями по номеру\n"
                f"• Отправка сообщений и Stars\n"
                f"• Подарок подписок\n"
                f"• Сохранение медиа с таймерами\n"
                f"• Поддержка кружков\n"
                f"• 20+ новых функций"
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
