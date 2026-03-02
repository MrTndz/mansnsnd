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
MAX_MEDIA_SIZE = 50 * 1024 * 1024
MEDIA_CLEANUP_DAYS = 90

# ПРАЙС-ЛИСТ (в Telegram Stars)
SUBSCRIPTION_PRICES = {
    'week': {'stars': 100, 'rub': 179, 'days': 7, 'name': 'Неделя'},
    'month_3': {'stars': 1500, 'rub': 500, 'days': 90, 'name': '3 месяца'},
    'month_6': {'stars': 2000, 'rub': 800, 'days': 180, 'name': '6 месяцев'},
    'year': {'stars': 5000, 'rub': 1500, 'days': 365, 'name': 'Год'},
    'lifetime': {'stars': 25000, 'rub': 5000, 'days': None, 'name': 'Навсегда'}
}

# Конвертация Stars в рубли для отображения
STAR_TO_RUB_RATE = 1.79  # 100 Stars ≈ 179₽

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
    send_gifts = State()
    manage_subscription = State()
    broadcast_message = State()
    statistics = State()
    search_user = State()

class SubscriptionStates(StatesGroup):
    """Состояния для управления подпиской"""
    choosing_plan = State()
    choosing_payment_method = State()
    payment_confirmation = State()

class GiftStates(StatesGroup):
    """Состояния для управления подарками"""
    exchange_gifts = State()

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
                stars_balance INTEGER DEFAULT 0,
                gifts_balance INTEGER DEFAULT 0
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
                currency TEXT DEFAULT 'XTR',
                plan_type TEXT,
                payment_method TEXT,
                telegram_payment_charge_id TEXT,
                provider_payment_charge_id TEXT,
                invoice_payload TEXT,
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
        
        # Таблица транзакций Gifts (подарков)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS gifts_transactions (
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
        logger.info("База данных инициализирована (версия 6.0.0)")
    
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
    
    def activate_subscription(self, user_id: int, plan_type: str, days: int = None):
        """Активация подписки"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        if days:
            expires = datetime.now() + timedelta(days=days)
        elif plan_type == 'lifetime':
            expires = None
        else:
            expires = datetime.now() + timedelta(days=30)
        
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
    
    # ========================================
    # МЕТОДЫ ДЛЯ РАБОТЫ СО STARS И GIFTS
    # ========================================
    
    def add_stars(self, user_id: int, amount: int, description: str = ""):
        """Добавление Stars пользователю"""
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
    
    def remove_stars(self, user_id: int, amount: int, description: str = ""):
        """Списание Stars у пользователя"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE users SET stars_balance = stars_balance - ? WHERE user_id = ?
        ''', (amount, user_id))
        
        cursor.execute('''
            INSERT INTO stars_transactions (user_id, amount, transaction_type, description)
            VALUES (?, ?, 'remove', ?)
        ''', (user_id, -amount, description))
        
        conn.commit()
        conn.close()
    
    def get_stars_balance(self, user_id: int) -> int:
        """Получение баланса Stars"""
        user = self.get_user(user_id)
        return user['stars_balance'] if user else 0
    
    def add_gifts(self, user_id: int, amount: int, description: str = ""):
        """Добавление подарков (gifts) пользователю"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE users SET gifts_balance = gifts_balance + ? WHERE user_id = ?
        ''', (amount, user_id))
        
        cursor.execute('''
            INSERT INTO gifts_transactions (user_id, amount, transaction_type, description)
            VALUES (?, ?, 'add', ?)
        ''', (user_id, amount, description))
        
        conn.commit()
        conn.close()
        
        self.log_admin_action(ADMIN_ID, user_id, 'gifts_added', f'Amount: {amount}, Reason: {description}')
    
    def remove_gifts(self, user_id: int, amount: int, description: str = ""):
        """Списание подарков у пользователя"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE users SET gifts_balance = gifts_balance - ? WHERE user_id = ?
        ''', (amount, user_id))
        
        cursor.execute('''
            INSERT INTO gifts_transactions (user_id, amount, transaction_type, description)
            VALUES (?, ?, 'remove', ?)
        ''', (user_id, -amount, description))
        
        conn.commit()
        conn.close()
    
    def get_gifts_balance(self, user_id: int) -> int:
        """Получение баланса подарков"""
        user = self.get_user(user_id)
        return user['gifts_balance'] if user else 0
    
    def can_afford_subscription(self, user_id: int, plan_key: str) -> Tuple[bool, str]:
        """Проверка возможности купить подписку за подарки"""
        gifts_balance = self.get_gifts_balance(user_id)
        required_gifts = SUBSCRIPTION_PRICES[plan_key]['stars']
        
        if gifts_balance >= required_gifts:
            return True, f"Достаточно подарков ({gifts_balance} >= {required_gifts})"
        else:
            return False, f"Недостаточно подарков ({gifts_balance} < {required_gifts})"
    
    def exchange_gifts_for_subscription(self, user_id: int, plan_key: str) -> bool:
        """Обмен подарков на подписку"""
        can_afford, message = self.can_afford_subscription(user_id, plan_key)
        
        if not can_afford:
            return False
        
        plan = SUBSCRIPTION_PRICES[plan_key]
        self.remove_gifts(user_id, plan['stars'], f"Обмен на подписку: {plan['name']}")
        self.activate_subscription(user_id, plan_key, plan['days'])
        
        return True
    
    # ========================================
    # МЕТОДЫ ДЛЯ РАБОТЫ С ПЛАТЕЖАМИ
    # ========================================
    
    def save_payment(self, user_id: int, amount: int, currency: str, plan_type: str,
                    telegram_payment_charge_id: str, provider_payment_charge_id: str,
                    invoice_payload: str):
        """Сохранение информации о платеже"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO payments 
            (user_id, amount, currency, plan_type, payment_method,
             telegram_payment_charge_id, provider_payment_charge_id, invoice_payload,
             status, confirmed_at)
            VALUES (?, ?, ?, ?, 'stars', ?, ?, ?, 'completed', CURRENT_TIMESTAMP)
        ''', (user_id, amount, currency, plan_type, 
              telegram_payment_charge_id, provider_payment_charge_id, invoice_payload))
        
        conn.commit()
        conn.close()
        
        logger.info(f"Платеж сохранен: user={user_id}, amount={amount} {currency}, plan={plan_type}")
    
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

async def download_media(bot: Bot, file_id: str, file_type: str, user_id: int, has_timer: bool = False) -> Optional[str]:
    """Скачивание медиафайла"""
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
        logger.info(f"Медиафайл сохранен: {file_path}")
        return str(file_path)
    except Exception as e:
        logger.error(f"Ошибка скачивания медиа: {e}")
        return None

def format_subscription_info(user: Dict) -> str:
    """Форматирование информации о подписке"""
    if user['is_blocked']:
        return "🚫 Заблокирован"
    sub_type = user['subscription_type']
    if sub_type == 'free':
        return "🆓 Бесплатный"
    elif sub_type == 'trial':
        if user['subscription_expires']:
            expires = datetime.fromisoformat(user['subscription_expires'])
            days_left = (expires - datetime.now()).days
            return f"🎁 Пробный ({days_left}д)"
        return "🎁 Пробный"
    elif sub_type in ['week', 'month_3', 'month_6', 'year']:
        if user['subscription_expires']:
            expires = datetime.fromisoformat(user['subscription_expires'])
            days_left = (expires - datetime.now()).days
            plan_name = SUBSCRIPTION_PRICES[sub_type]['name']
            return f"💎 {plan_name} ({days_left}д)"
        return "💎 Активна"
    elif sub_type == 'lifetime':
        return "♾️ Навсегда"
    return "❓ Неизвестно"

def format_user_short(user: Dict, index: int) -> str:
    """Краткое форматирование пользователя"""
    status_emoji = "🚫" if user['is_blocked'] else "✅"
    username = f"@{user['username']}" if user['username'] else "без username"
    name = user['first_name'] or "Без имени"
    return f"{index}. {status_emoji} {name} ({username})\n   ID: {user['user_id']}"

# ========================================
# КЛАВИАТУРЫ
# ========================================

def get_start_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Принять условия", callback_data="accept_terms")
    builder.button(text="📄 Прочитать условия", callback_data="show_terms")
    builder.adjust(1)
    return builder.as_markup()

def get_main_menu_keyboard(user_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="📊 Статистика", callback_data="stats")
    builder.button(text="💎 Подписка", callback_data="subscription")
    builder.button(text="🎁 Мои подарки", callback_data="my_gifts")
    builder.button(text="⚙️ Настройки", callback_data="settings")
    if user_id == ADMIN_ID:
        builder.button(text="👨‍💼 Админ", callback_data="admin_panel")
    builder.adjust(2)
    return builder.as_markup()

def get_subscription_keyboard_v6(user: Dict) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for key, plan in SUBSCRIPTION_PRICES.items():
        builder.button(text=f"{plan['name']}: {plan['stars']}⭐", callback_data=f"show_sub_{key}")
    builder.button(text="◀️ Назад", callback_data="main_menu")
    builder.adjust(1)
    return builder.as_markup()

def get_payment_method_keyboard(plan_key: str, user_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="⭐ Оплатить Stars", callback_data=f"buy_stars_{plan_key}")
    gifts_balance = db.get_gifts_balance(user_id)
    required = SUBSCRIPTION_PRICES[plan_key]['stars']
    if gifts_balance >= required:
        builder.button(text=f"🎁 Обменять подарки ({gifts_balance})", callback_data=f"exchange_{plan_key}")
    else:
        builder.button(text=f"🎁 Подарков мало ({gifts_balance}/{required})", callback_data="need_more_gifts")
    builder.button(text="👤 Связаться с админом", url=f"https://t.me/{ADMIN_USERNAME}")
    builder.button(text="◀️ Назад", callback_data="subscription")
    builder.adjust(1)
    return builder.as_markup()

def get_admin_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="👥 Пользователи", callback_data="admin_users")
    builder.button(text="📊 Статистика", callback_data="admin_stats")
    builder.button(text="📢 Рассылка", callback_data="admin_broadcast")
    builder.button(text="◀️ Назад", callback_data="main_menu")
    builder.adjust(2)
    return builder.as_markup()

def get_users_list_keyboard(page: int, total_pages: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="◀️", callback_data=f"users_page_{page-1}"))
    nav_buttons.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="users_page_info"))
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton(text="▶️", callback_data=f"users_page_{page+1}"))
    for btn in nav_buttons:
        builder.add(btn)
    builder.row(InlineKeyboardButton(text="🔢 Выбрать по номеру", callback_data="select_user_by_number"))
    builder.row(InlineKeyboardButton(text="◀️ Назад", callback_data="admin_panel"))
    return builder.as_markup()

def get_user_management_keyboard(user_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="💬 Сообщение", callback_data=f"admin_msg_{user_id}")
    builder.button(text="🎁 Подарить", callback_data=f"admin_gifts_{user_id}")
    builder.button(text="💎 Подписка", callback_data=f="admin_sub_{user_id}")
    user = db.get_user(user_id)
    if user and user['is_blocked']:
        builder.button(text="✅ Разблокировать", callback_data=f"admin_unblock_{user_id}")
    else:
        builder.button(text="🚫 Заблокировать", callback_data=f"admin_block_{user_id}")
    builder.button(text="◀️ Список", callback_data="admin_users")
    builder.adjust(2)
    return builder.as_markup()

def get_back_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="◀️ Назад", callback_data="main_menu")
    return builder.as_markup()

# ========================================
# ОБРАБОТЧИКИ
# ========================================

router = Router()

@router.message(CommandStart())
async def cmd_start(message: Message):
    user_id = message.from_user.id
    db.add_user(user_id, message.from_user.username, message.from_user.first_name, message.from_user.last_name)
    user = db.get_user(user_id)
    if user['is_blocked']:
        await message.answer("🚫 Ваш аккаунт заблокирован. Свяжитесь с @" + ADMIN_USERNAME)
        return
    if not user['accepted_terms']:
        await message.answer(
            "👋 <b>Business Message Monitor v6.0</b>\n\n"
            "Мониторинг удаленных сообщений и медиа с таймерами.\n\n"
            "<b>ВАЖНО:</b> Перед использованием примите условия.",
            reply_markup=get_start_keyboard()
        )
    else:
        gifts = db.get_gifts_balance(user_id)
        await message.answer(
            f"Привет, <b>{message.from_user.first_name}</b>!\n\n"
            f"{format_subscription_info(user)}\n"
            f"🎁 Подарков: {gifts}\n\n"
            "Выберите действие:",
            reply_markup=get_main_menu_keyboard(user_id)
        )

@router.callback_query(F.data == "show_terms")
async def show_terms(callback: CallbackQuery):
    terms = (
        "📄 <b>УСЛОВИЯ ИСПОЛЬЗОВАНИЯ v6.0</b>\n\n"
        "<b>АДМИНИСТРАЦИЯ НЕ НЕСЕТ ОТВЕТСТВЕННОСТИ:</b>\n"
        "• За любые действия пользователей\n"
        "• За потерю данных\n"
        "• За технические сбои\n"
        "• За последствия использования бота\n\n"
        "<b>ВСЕ ДЕЙСТВИЯ ВЫПОЛНЯЮТСЯ ДОБРОВОЛЬНО И НА СОБСТВЕННЫЙ РИСК.</b>\n\n"
        "Бот сохраняет сообщения из бизнес-чатов, отслеживает удаления и изменения, сохраняет медиа с таймерами.\n\n"
        "Нажимая 'Принять', вы соглашаетесь с условиями."
    )
    await callback.message.edit_text(terms, reply_markup=get_start_keyboard())

@router.callback_query(F.data == "accept_terms")
async def accept_terms(callback: CallbackQuery):
    db.accept_terms(callback.from_user.id)
    db.add_gifts(callback.from_user.id, 10, "Бонус за регистрацию")
    await callback.message.edit_text(
        "✅ Условия приняты!\n\n"
        "🎁 Бонус: 10 подарков\n\n"
        "Подключите бота через Telegram Business:\n"
        "Настройки → Business → Чат-боты → @mrztnbot",
        reply_markup=get_main_menu_keyboard(callback.from_user.id)
    )
    try:
        await callback.bot.send_message(ADMIN_ID, f"Новый пользователь: {callback.from_user.id}")
    except:
        pass

@router.callback_query(F.data == "main_menu")
async def main_menu(callback: CallbackQuery):
    user = db.get_user(callback.from_user.id)
    gifts = db.get_gifts_balance(callback.from_user.id)
    await callback.message.edit_text(
        f"🏠 Главное меню\n\n{format_subscription_info(user)}\n🎁 Подарков: {gifts}",
        reply_markup=get_main_menu_keyboard(callback.from_user.id)
    )

@router.callback_query(F.data == "stats")
async def show_stats(callback: CallbackQuery):
    user = db.get_user(callback.from_user.id)
    gifts = db.get_gifts_balance(callback.from_user.id)
    text = (
        f"📊 Статистика\n\n"
        f"{format_subscription_info(user)}\n"
        f"🎁 Подарков: {gifts}\n\n"
        f"💬 Сообщений: {user['total_messages_saved']}\n"
        f"🗑 Удалений: {user['total_deletions_tracked']}\n"
        f"📸 Медиа: {user['total_media_saved']}"
    )
    await callback.message.edit_text(text, reply_markup=get_back_keyboard())

@router.callback_query(F.data == "subscription")
async def show_subscription(callback: CallbackQuery):
    user = db.get_user(callback.from_user.id)
    gifts = db.get_gifts_balance(callback.from_user.id)
    text = (
        f"💎 Подписка\n\n{format_subscription_info(user)}\n🎁 Подарков: {gifts}\n\n"
        "<b>Прайс-лист:</b>\n"
    )
    for key, plan in SUBSCRIPTION_PRICES.items():
        text += f"{plan['name']}: {plan['stars']}⭐ ({plan['rub']}₽)\n"
    await callback.message.edit_text(text, reply_markup=get_subscription_keyboard_v6(user))

@router.callback_query(F.data.startswith("show_sub_"))
async def show_sub_details(callback: CallbackQuery):
    plan_key = callback.data.split("_")[-1]
    plan = SUBSCRIPTION_PRICES[plan_key]
    user_id = callback.from_user.id
    gifts = db.get_gifts_balance(user_id)
    text = (
        f"💎 {plan['name']}\n\n"
        f"Стоимость: {plan['stars']}⭐ ({plan['rub']}₽)\n"
        f"Ваши подарки: {gifts}🎁\n\n"
        "Выберите способ оплаты:"
    )
    await callback.message.edit_text(text, reply_markup=get_payment_method_keyboard(plan_key, user_id))

@router.callback_query(F.data.startswith("buy_stars_"))
async def buy_with_stars(callback: CallbackQuery):
    plan_key = callback.data.split("_")[-1]
    plan = SUBSCRIPTION_PRICES[plan_key]
    prices = [LabeledPrice(label=plan['name'], amount=plan['stars'])]
    try:
        await callback.bot.send_invoice(
            chat_id=callback.from_user.id,
            title=f"Подписка: {plan['name']}",
            description=f"Подписка на {plan['name']}",
            payload=f"sub_{plan_key}",
            currency="XTR",
            prices=prices
        )
        await callback.answer("Инвойс отправлен")
    except Exception as e:
        await callback.answer(f"Ошибка: {e}", show_alert=True)

@router.pre_checkout_query()
async def pre_checkout(query: PreCheckoutQuery):
    await query.answer(ok=True)

@router.message(F.successful_payment)
async def successful_payment(message: Message):
    payment = message.successful_payment
    plan_key = payment.invoice_payload.split("_")[1]
    plan = SUBSCRIPTION_PRICES[plan_key]
    db.activate_subscription(message.from_user.id, plan_key, plan['days'])
    db.save_payment(message.from_user.id, payment.total_amount, payment.currency, plan_key,
                    payment.telegram_payment_charge_id, payment.provider_payment_charge_id, payment.invoice_payload)
    db.add_gifts(message.from_user.id, plan['stars'] // 10, "Бонус за покупку")
    await message.answer(
        f"✅ Подписка активирована!\n\n"
        f"План: {plan['name']}\n"
        f"🎁 Бонус: {plan['stars']//10} подарков"
    )

@router.callback_query(F.data.startswith("exchange_"))
async def exchange_gifts(callback: CallbackQuery):
    plan_key = callback.data.split("_")[-1]
    user_id = callback.from_user.id
    if db.exchange_gifts_for_subscription(user_id, plan_key):
        plan = SUBSCRIPTION_PRICES[plan_key]
        await callback.message.edit_text(
            f"✅ Обмен успешен!\n\nСписано: {plan['stars']}🎁\nПолучено: {plan['name']}"
        )
    else:
        await callback.answer("Недостаточно подарков", show_alert=True)

@router.callback_query(F.data == "my_gifts")
async def show_gifts(callback: CallbackQuery):
    gifts = db.get_gifts_balance(callback.from_user.id)
    text = f"🎁 Мои подарки\n\nБаланс: {gifts}🎁\n\nИспользуйте для покупки подписки в разделе 💎 Подписка"
    await callback.message.edit_text(text, reply_markup=get_back_keyboard())

@router.callback_query(F.data == "admin_panel")
async def admin_panel(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    total = db.get_user_count()
    active = db.get_active_subscriptions_count()
    await callback.message.edit_text(
        f"👨‍💼 Админ\n\nПользователей: {total}\nПодписок: {active}",
        reply_markup=get_admin_keyboard()
    )

@router.callback_query(F.data == "admin_users")
@router.callback_query(F.data.startswith("users_page_"))
async def admin_users(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    page = 0
    if callback.data.startswith("users_page_"):
        try:
            page = int(callback.data.split("_")[-1])
        except:
            page = 0
    total = db.get_user_count()
    total_pages = max(1, (total + USERS_PER_PAGE - 1) // USERS_PER_PAGE)
    page = max(0, min(page, total_pages - 1))
    users = db.get_all_users(USERS_PER_PAGE, page * USERS_PER_PAGE)
    text = f"👥 Пользователи ({page+1}/{total_pages})\n\n"
    for i, user in enumerate(users, page * USERS_PER_PAGE + 1):
        text += format_user_short(user, i) + "\n"
    try:
        await callback.message.edit_text(text, reply_markup=get_users_list_keyboard(page, total_pages))
    except:
        await callback.answer()

@router.callback_query(F.data == "select_user_by_number")
async def select_user(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.message.edit_text("Отправьте номер пользователя:")
    await state.set_state(AdminStates.user_number_input)

@router.message(AdminStates.user_number_input)
async def process_number(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        num = int(message.text)
        users = db.get_all_users(1, num - 1)
        if users:
            user = users[0]
            gifts = db.get_gifts_balance(user['user_id'])
            text = f"👤 {user['first_name']}\nID: {user['user_id']}\n{format_subscription_info(user)}\n🎁: {gifts}"
            await message.answer(text, reply_markup=get_user_management_keyboard(user['user_id']))
        else:
            await message.answer("Пользователь не найден")
        await state.clear()
    except:
        await message.answer("Введите число")

@router.callback_query(F.data.startswith("admin_gifts_"))
async def admin_gifts(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    user_id = int(callback.data.split("_")[-1])
    await state.update_data(target=user_id)
    await callback.message.edit_text("Введите количество подарков:")
    await state.set_state(AdminStates.send_gifts)

@router.message(AdminStates.send_gifts)
async def process_gifts(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        amount = int(message.text)
        data = await state.get_data()
        target = data['target']
        db.add_gifts(target, amount, "От администратора")
        await message.answer(f"✅ Отправлено {amount}🎁 пользователю {target}")
        try:
            await message.bot.send_message(target, f"🎁 Вы получили {amount} подарков от администратора!")
        except:
            pass
        await state.clear()
    except:
        await message.answer("Введите число")

@router.callback_query(F.data.startswith("admin_block_"))
async def block(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    user_id = int(callback.data.split("_")[-1])
    db.block_user(user_id)
    await callback.answer("Заблокирован")
    await callback.message.edit_reply_markup(reply_markup=get_user_management_keyboard(user_id))

@router.callback_query(F.data.startswith("admin_unblock_"))
async def unblock(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    user_id = int(callback.data.split("_")[-1])
    db.unblock_user(user_id)
    await callback.answer("Разблокирован")
    await callback.message.edit_reply_markup(reply_markup=get_user_management_keyboard(user_id))

@router.callback_query(F.data == "admin_stats")
async def admin_stats(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    total = db.get_user_count()
    active = db.get_active_subscriptions_count()
    messages = db.get_total_messages_saved()
    deletions = db.get_total_deletions_tracked()
    text = f"📊 Статистика\n\nПользователей: {total}\nПодписок: {active}\nСообщений: {messages}\nУдалений: {deletions}"
    await callback.message.edit_text(text, reply_markup=get_back_keyboard())

@router.business_connection()
async def on_connection(connection: BusinessConnection, bot: Bot):
    db.add_business_connection(connection.id, connection.user.id, connection.user.id, connection.can_reply)
    try:
        await bot.send_message(connection.user.id, "🎉 Бот подключен к вашему бизнес-аккаунту!")
    except:
        pass

@router.business_message()
async def on_business_message(message: Message, bot: Bot):
    try:
        if not message.business_connection_id:
            return
        conn = db.get_business_connection(message.business_connection_id)
        if not conn:
            return
        user_id = conn['user_id']
        if not db.check_subscription(user_id):
            return
        media_type = media_file_id = media_file_path = caption = None
        has_timer = timer_seconds = False, None
        if message.photo:
            media_type, media_file_id = "photo", message.photo[-1].file_id
            media_file_path = await download_media(bot, media_file_id, media_type, user_id, has_timer)
        elif message.video:
            media_type, media_file_id = "video", message.video.file_id
            media_file_path = await download_media(bot, media_file_id, media_type, user_id, has_timer)
        elif message.video_note:
            media_type, media_file_id = "video_note", message.video_note.file_id
            has_timer, timer_seconds = True, message.video_note.duration
            media_file_path = await download_media(bot, media_file_id, media_type, user_id, has_timer)
        caption = message.caption
        db.save_message(user_id, message.business_connection_id, message.chat.id, message.message_id,
                       message.from_user.id, message.from_user.username, message.text or caption,
                       media_type, media_file_id, media_file_path, None, caption, has_timer, timer_seconds)
    except Exception as e:
        logger.error(f"Ошибка: {e}")

@router.deleted_business_messages()
async def on_deleted(deleted: BusinessMessagesDeleted, bot: Bot):
    try:
        conn = db.get_business_connection(deleted.business_connection_id)
        if not conn:
            return
        user_id = conn['user_id']
        for msg_id in deleted.message_ids:
            saved = db.get_message(user_id, deleted.chat.id, msg_id)
            if saved:
                db.mark_message_deleted(user_id, deleted.chat.id, msg_id)
                text = f"🗑 Удалено\n\nОт: @{saved['sender_username'] or saved['sender_id']}\n"
                if saved['message_text']:
                    text += f"Текст: {saved['message_text'][:200]}"
                await bot.send_message(user_id, text)
                if saved['media_file_path'] and Path(saved['media_file_path']).exists():
                    try:
                        file = FSInputFile(saved['media_file_path'])
                        if saved['media_type'] == 'photo':
                            await bot.send_photo(user_id, file)
                        elif saved['media_type'] == 'video':
                            await bot.send_video(user_id, file)
                    except:
                        pass
    except Exception as e:
        logger.error(f"Ошибка: {e}")

async def main():
    try:
        bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        dp = Dispatcher(storage=MemoryStorage())
        dp.include_router(router)
        info = await bot.get_me()
        logger.info(f"Бот запущен: @{info.username} v6.0.0")
        try:
            await bot.send_message(ADMIN_ID, "🚀 Бот запущен v6.0.0")
        except:
            pass
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    except Exception as e:
        logger.error(f"Ошибка: {e}")
    finally:
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Остановлен")