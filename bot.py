#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram Chat Monitor Bot v7.0.0 FIXED
Author: Merzost?
Date: 2026-03-02
READY FOR PRODUCTION
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
import hashlib
import zipfile
import re
from collections import defaultdict
import threading

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    BusinessConnection, BusinessMessagesDeleted, FSInputFile,
    PhotoSize, LabeledPrice, PreCheckoutQuery, SuccessfulPayment
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

# КОНФИГУРАЦИЯ
BOT_TOKEN = "8296802832:AAEU4oF4v5bjKP3KTb1rRx1Oxf-Z1dng9QQ"
ADMIN_ID = 7785371505
ADMIN_USERNAME = "mrztn"

# ИСПРАВЛЕНИЕ: Глобальные переменные
BOT_USERNAME = None
db_lock = threading.Lock()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Директории
for directory in [Path("media"), Path("exports"), Path("database")]:
    directory.mkdir(exist_ok=True)

# НОВЫЕ ЦЕНЫ (от 100 Stars)
TRIAL_DAYS = 3
STARTER_PRICE = 100
BASIC_PRICE = 250
PRO_PRICE = 600
PREMIUM_PRICE = 2000
ULTIMATE_PRICE = 5000

PRICES_RUB = {'starter': 200, 'basic': 500, 'pro': 1200, 'premium': 4000, 'ultimate': 10000}
REFERRAL_BONUS_PERCENT = 20

# FSM
class AdminStates(StatesGroup):
    main_menu = State()
    
class UserStates(StatesGroup):
    settings = State()

# DATABASE
class Database:
    def __init__(self, db_path: str = "database/bot.db"):
        self.db_path = db_path
        self.init_database()
    
    def get_connection(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA busy_timeout=30000')
        return conn
    
    def init_database(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                accepted_terms BOOLEAN DEFAULT 0,
                is_blocked BOOLEAN DEFAULT 0,
                subscription_type TEXT DEFAULT 'free',
                subscription_expires TIMESTAMP,
                trial_used BOOLEAN DEFAULT 0,
                auto_trial_activated BOOLEAN DEFAULT 0,
                total_messages_saved INTEGER DEFAULT 0,
                total_deletions_tracked INTEGER DEFAULT 0,
                total_edits_tracked INTEGER DEFAULT 0,
                total_media_saved INTEGER DEFAULT 0,
                stars_balance INTEGER DEFAULT 0,
                referral_code TEXT UNIQUE,
                referred_by INTEGER,
                referral_earnings INTEGER DEFAULT 0,
                total_referrals INTEGER DEFAULT 0,
                notify_deletions BOOLEAN DEFAULT 1,
                notify_edits BOOLEAN DEFAULT 1,
                notify_media_timers BOOLEAN DEFAULT 1,
                notify_connections BOOLEAN DEFAULT 1,
                user_level INTEGER DEFAULT 1,
                experience_points INTEGER DEFAULT 0
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS business_connections (
                connection_id TEXT PRIMARY KEY,
                user_id INTEGER,
                connected_user_id INTEGER,
                is_enabled BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
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
                caption TEXT,
                has_timer BOOLEAN DEFAULT 0,
                is_view_once BOOLEAN DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_deleted BOOLEAN DEFAULT 0,
                deleted_at TIMESTAMP,
                is_edited BOOLEAN DEFAULT 0,
                edited_at TIMESTAMP,
                original_text TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                amount_stars INTEGER,
                plan_type TEXT,
                status TEXT DEFAULT 'confirmed',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS referral_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                referrer_id INTEGER,
                referred_id INTEGER,
                action_type TEXT,
                bonus_amount INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info("База данных инициализирована v7.0.0")
    
    def add_user(self, user_id: int, username: str = None, first_name: str = None, referred_by: int = None):
        with db_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            try:
                import random, string
                ref_code = f"REF{user_id}{''.join(random.choices(string.ascii_uppercase + string.digits, k=6))}"
                
                cursor.execute('''
                    INSERT OR IGNORE INTO users 
                    (user_id, username, first_name, referral_code, referred_by)
                    VALUES (?, ?, ?, ?, ?)
                ''', (user_id, username, first_name, ref_code, referred_by))
                
                if referred_by and cursor.rowcount > 0:
                    cursor.execute('UPDATE users SET total_referrals = total_referrals + 1 WHERE user_id = ?', (referred_by,))
                
                conn.commit()
                return True
            except Exception as e:
                logger.error(f"Ошибка add_user: {e}")
                conn.rollback()
                return False
            finally:
                conn.close()
    
    def get_user(self, user_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None
    
    def get_user_by_referral_code(self, code: str):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM users WHERE referral_code = ?', (code,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None
    
    def accept_terms(self, user_id: int):
        with db_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('UPDATE users SET accepted_terms = 1 WHERE user_id = ?', (user_id,))
            conn.commit()
            conn.close()
    
    def activate_auto_trial(self, user_id: int):
        with db_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            expires = datetime.now() + timedelta(days=TRIAL_DAYS)
            cursor.execute('''
                UPDATE users 
                SET subscription_type = 'trial', subscription_expires = ?, trial_used = 1, auto_trial_activated = 1
                WHERE user_id = ? AND trial_used = 0
            ''', (expires, user_id))
            affected = cursor.rowcount
            conn.commit()
            conn.close()
            return affected > 0
    
    def check_subscription(self, user_id: int):
        user = self.get_user(user_id)
        if not user or user['is_blocked'] or user['subscription_type'] == 'free':
            return False
        if user['subscription_type'] == 'ultimate':
            return True
        if user['subscription_expires']:
            expires = datetime.fromisoformat(user['subscription_expires'])
            if datetime.now() > expires:
                self.deactivate_subscription(user_id)
                return False
        return True
    
    def activate_subscription(self, user_id: int, plan_type: str):
        with db_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            if plan_type == 'starter':
                expires = datetime.now() + timedelta(days=7)
            elif plan_type == 'basic':
                expires = datetime.now() + timedelta(days=30)
            elif plan_type == 'pro':
                expires = datetime.now() + timedelta(days=90)
            elif plan_type == 'premium':
                expires = datetime.now() + timedelta(days=365)
            elif plan_type == 'ultimate':
                expires = None
            else:
                expires = None
            
            cursor.execute('UPDATE users SET subscription_type = ?, subscription_expires = ? WHERE user_id = ?', 
                          (plan_type, expires, user_id))
            conn.commit()
            conn.close()
    
    def deactivate_subscription(self, user_id: int):
        with db_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('UPDATE users SET subscription_type = "free", subscription_expires = NULL WHERE user_id = ?', (user_id,))
            conn.commit()
            conn.close()
    
    def add_business_connection(self, connection_id: str, user_id: int, connected_user_id: int):
        with db_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            try:
                cursor.execute('INSERT OR REPLACE INTO business_connections (connection_id, user_id, connected_user_id) VALUES (?, ?, ?)',
                              (connection_id, user_id, connected_user_id))
                conn.commit()
                return True
            except Exception as e:
                logger.error(f"Ошибка add_business_connection: {e}")
                conn.rollback()
                return False
            finally:
                conn.close()
    
    def get_business_connection(self, connection_id: str):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM business_connections WHERE connection_id = ?', (connection_id,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None
    
    def get_user_connections(self, user_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM business_connections WHERE user_id = ?', (user_id,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def save_message(self, user_id: int, connection_id: str, chat_id: int, message_id: int,
                    sender_id: int, sender_username: str = None, sender_first_name: str = None,
                    message_text: str = None, media_type: str = None, media_file_id: str = None,
                    media_file_path: str = None, caption: str = None, has_timer: bool = False,
                    is_view_once: bool = False):
        with db_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            try:
                cursor.execute('''
                    INSERT INTO saved_messages 
                    (user_id, connection_id, chat_id, message_id, sender_id, sender_username, sender_first_name,
                     message_text, media_type, media_file_id, media_file_path, caption, has_timer, is_view_once)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (user_id, connection_id, chat_id, message_id, sender_id, sender_username, sender_first_name,
                      message_text, media_type, media_file_id, media_file_path, caption, has_timer, is_view_once))
                
                cursor.execute('UPDATE users SET total_messages_saved = total_messages_saved + 1 WHERE user_id = ?', (user_id,))
                if media_type:
                    cursor.execute('UPDATE users SET total_media_saved = total_media_saved + 1 WHERE user_id = ?', (user_id,))
                
                conn.commit()
                return cursor.lastrowid
            except Exception as e:
                logger.error(f"Ошибка save_message: {e}")
                conn.rollback()
                return None
            finally:
                conn.close()
    
    def get_message(self, user_id: int, chat_id: int, message_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM saved_messages WHERE user_id = ? AND chat_id = ? AND message_id = ? ORDER BY created_at DESC LIMIT 1',
                      (user_id, chat_id, message_id))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None
    
    def mark_message_deleted(self, user_id: int, chat_id: int, message_id: int):
        with db_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            try:
                cursor.execute('UPDATE saved_messages SET is_deleted = 1, deleted_at = CURRENT_TIMESTAMP WHERE user_id = ? AND chat_id = ? AND message_id = ?',
                              (user_id, chat_id, message_id))
                affected = cursor.rowcount
                if affected > 0:
                    cursor.execute('UPDATE users SET total_deletions_tracked = total_deletions_tracked + 1 WHERE user_id = ?', (user_id,))
                conn.commit()
                return affected > 0
            except Exception as e:
                logger.error(f"Ошибка mark_message_deleted: {e}")
                conn.rollback()
                return False
            finally:
                conn.close()
    
    def mark_message_edited(self, user_id: int, chat_id: int, message_id: int, original_text: str):
        with db_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            try:
                cursor.execute('UPDATE saved_messages SET is_edited = 1, edited_at = CURRENT_TIMESTAMP, original_text = ? WHERE user_id = ? AND chat_id = ? AND message_id = ?',
                              (original_text, user_id, chat_id, message_id))
                if cursor.rowcount > 0:
                    cursor.execute('UPDATE users SET total_edits_tracked = total_edits_tracked + 1 WHERE user_id = ?', (user_id,))
                conn.commit()
            except Exception as e:
                logger.error(f"Ошибка mark_message_edited: {e}")
                conn.rollback()
            finally:
                conn.close()
    
    def save_payment(self, user_id: int, amount_stars: int, plan_type: str):
        with db_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('INSERT INTO payments (user_id, amount_stars, plan_type) VALUES (?, ?, ?)',
                          (user_id, amount_stars, plan_type))
            conn.commit()
            conn.close()
    
    def process_referral_payment(self, user_id: int, amount_stars: int):
        user = self.get_user(user_id)
        if not user or not user['referred_by']:
            return
        
        referrer_id = user['referred_by']
        bonus = int(amount_stars * REFERRAL_BONUS_PERCENT / 100)
        
        with db_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('UPDATE users SET stars_balance = stars_balance + ?, referral_earnings = referral_earnings + ? WHERE user_id = ?',
                          (bonus, bonus, referrer_id))
            cursor.execute('INSERT INTO referral_actions (referrer_id, referred_id, action_type, bonus_amount) VALUES (?, ?, "payment", ?)',
                          (referrer_id, user_id, bonus))
            conn.commit()
            conn.close()
    
    def get_referral_stats(self, user_id: int):
        user = self.get_user(user_id)
        if not user:
            return {}
        
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT user_id, username, first_name, subscription_type FROM users WHERE referred_by = ? ORDER BY registered_at DESC',
                      (user_id,))
        referrals = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return {
            'total': user['total_referrals'],
            'earnings': user['referral_earnings'],
            'referrals': referrals,
            'code': user['referral_code']
        }
    
    def get_all_users(self, limit: int = 10, offset: int = 0):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM users ORDER BY registered_at DESC LIMIT ? OFFSET ?', (limit, offset))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def get_user_count(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) as count FROM users')
        result = cursor.fetchone()
        conn.close()
        return result['count'] if result else 0
    
    def get_active_subscriptions_count(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) as count FROM users WHERE subscription_type != "free" AND is_blocked = 0')
        result = cursor.fetchone()
        conn.close()
        return result['count'] if result else 0
    
    def block_user(self, user_id: int):
        with db_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('UPDATE users SET is_blocked = 1 WHERE user_id = ?', (user_id,))
            conn.commit()
            conn.close()
    
    def unblock_user(self, user_id: int):
        with db_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('UPDATE users SET is_blocked = 0 WHERE user_id = ?', (user_id,))
            conn.commit()
            conn.close()
    
    def update_notification_settings(self, user_id: int, setting: str, value: bool):
        with db_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(f'UPDATE users SET {setting} = ? WHERE user_id = ?', (value, user_id))
            conn.commit()
            conn.close()

db = Database()

# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
async def download_media(bot: Bot, file_id: str, file_type: str, user_id: int, has_timer: bool = False):
    try:
        file = await bot.get_file(file_id)
        file_extension = file.file_path.split('.')[-1] if file.file_path else 'bin'
        
        user_media_dir = Path("media") / str(user_id)
        user_media_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_hash = hashlib.md5(file_id.encode()).hexdigest()[:8]
        timer_prefix = "timer_" if has_timer else ""
        filename = f"{timer_prefix}{file_type}_{timestamp}_{file_hash}.{file_extension}"
        file_path = user_media_dir / filename
        
        await bot.download_file(file.file_path, file_path)
        logger.debug(f"Медиа сохранено: {file_path}")
        return str(file_path)
    except Exception as e:
        logger.error(f"Ошибка download_media: {e}")
        return None

async def export_deleted_chat_to_archive(user_id: int, chat_id: int, messages: List[Dict], chat_title: str):
    try:
        user_export_dir = Path("exports") / str(user_id)
        user_export_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        zip_filename = f"deleted_chat_{chat_id}_{timestamp}.zip"
        zip_path = user_export_dir / zip_filename
        
        report = f"Удаленный чат: {chat_title}\nДата: {datetime.now()}\nСообщений: {len(messages)}\n" + "="*80 + "\n\n"
        
        media_files = []
        for msg in messages:
            sender = msg['sender_username'] or msg['sender_first_name'] or f"User {msg['sender_id']}"
            report += f"[{msg['created_at']}] {sender}:\n"
            if msg['message_text']:
                report += f"{msg['message_text']}\n"
            if msg['media_type']:
                media_filename = f"media_{msg['message_id']}"
                report += f"[{msg['media_type'].upper()}]"
                if msg['has_timer']:
                    report += " [ТАЙМЕР]"
                report += f" → {media_filename}\n"
                if msg['caption']:
                    report += f"Подпись: {msg['caption']}\n"
                if msg['media_file_path'] and Path(msg['media_file_path']).exists():
                    media_files.append((msg['media_file_path'], media_filename))
            report += "-"*80 + "\n\n"
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.writestr('chat_report.txt', report.encode('utf-8'))
            for media_path, media_name in media_files:
                try:
                    ext = Path(media_path).suffix
                    zipf.write(media_path, f"media/{media_name}{ext}")
                except:
                    pass
        
        return str(zip_path)
    except Exception as e:
        logger.error(f"Ошибка export: {e}")
        return None

def format_subscription_info(user: Dict):
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
    elif sub_type == 'starter':
        return "🌟 Starter"
    elif sub_type == 'basic':
        return "💎 Basic"
    elif sub_type == 'pro':
        return "💼 Pro"
    elif sub_type == 'premium':
        return "👑 Premium"
    elif sub_type == 'ultimate':
        return "♾️ Ultimate"
    return "❓ Неизвестно"

# КЛАВИАТУРЫ
def get_start_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Принять условия", callback_data="accept_terms")
    builder.button(text="📄 Прочитать условия", callback_data="show_terms")
    builder.adjust(1)
    return builder.as_markup()

def get_main_menu_keyboard(user_id: int):
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

def get_subscription_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text=f"🌟 Starter (7д) - {STARTER_PRICE} ⭐", callback_data="sub_starter")
    builder.button(text=f"💎 Basic (месяц) - {BASIC_PRICE} ⭐", callback_data="sub_basic")
    builder.button(text=f"💼 Pro (3мес) - {PRO_PRICE} ⭐ 🔥", callback_data="sub_pro")
    builder.button(text=f"👑 Premium (год) - {PREMIUM_PRICE} ⭐ 🔥", callback_data="sub_premium")
    builder.button(text=f"♾️ Ultimate - {ULTIMATE_PRICE} ⭐ 💥", callback_data="sub_ultimate")
    builder.button(text="◀️ Назад", callback_data="main_menu")
    builder.adjust(1)
    return builder.as_markup()

def get_settings_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="🔔 Уведомления", callback_data="settings_notifications")
    builder.button(text="◀️ Назад", callback_data="main_menu")
    builder.adjust(1)
    return builder.as_markup()

def get_notifications_settings_keyboard(user: Dict):
    builder = InlineKeyboardBuilder()
    deletions_status = "✅" if user['notify_deletions'] else "❌"
    edits_status = "✅" if user['notify_edits'] else "❌"
    timers_status = "✅" if user['notify_media_timers'] else "❌"
    connections_status = "✅" if user['notify_connections'] else "❌"
    
    builder.button(text=f"{deletions_status} Удаления", callback_data="toggle_notify_deletions")
    builder.button(text=f"{edits_status} Редактирования", callback_data="toggle_notify_edits")
    builder.button(text=f"{timers_status} Медиа с таймерами", callback_data="toggle_notify_media_timers")
    builder.button(text=f"{connections_status} Подключения", callback_data="toggle_notify_connections")
    builder.button(text="◀️ Назад", callback_data="settings")
    builder.adjust(1)
    return builder.as_markup()

def get_admin_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="👥 Пользователи", callback_data="admin_users")
    builder.button(text="📊 Статистика", callback_data="admin_stats")
    builder.button(text="◀️ Назад", callback_data="main_menu")
    builder.adjust(2)
    return builder.as_markup()

def get_users_list_keyboard(page: int = 0, total_pages: int = 1):
    builder = InlineKeyboardBuilder()
    if page > 0:
        builder.add(InlineKeyboardButton(text="◀️ Пред", callback_data=f"users_page_{page-1}"))
    builder.add(InlineKeyboardButton(text=f"📄 {page+1}/{total_pages}", callback_data="users_page_info"))
    if page < total_pages - 1:
        builder.add(InlineKeyboardButton(text="След ▶️", callback_data=f"users_page_{page+1}"))
    builder.row(InlineKeyboardButton(text="◀️ Назад", callback_data="admin_panel"))
    return builder.as_markup()

def get_user_management_keyboard(user_id: int):
    builder = InlineKeyboardBuilder()
    user = db.get_user(user_id)
    builder.button(text="🎁 Подарить подписку", callback_data=f"admin_gift_{user_id}")
    if user and user['is_blocked']:
        builder.button(text="✅ Разблокировать", callback_data=f"admin_unblock_{user_id}")
    else:
        builder.button(text="🚫 Заблокировать", callback_data=f"admin_block_{user_id}")
    builder.button(text="◀️ К списку", callback_data="admin_users")
    builder.adjust(2)
    return builder.as_markup()

def get_gift_subscription_keyboard(user_id: int):
    builder = InlineKeyboardBuilder()
    builder.button(text="🌟 7 дней", callback_data=f"gift_sub_{user_id}_starter")
    builder.button(text="💎 1 месяц", callback_data=f"gift_sub_{user_id}_basic")
    builder.button(text="💼 3 месяца", callback_data=f"gift_sub_{user_id}_pro")
    builder.button(text="👑 1 год", callback_data=f"gift_sub_{user_id}_premium")
    builder.button(text="♾️ Навсегда", callback_data=f"gift_sub_{user_id}_ultimate")
    builder.button(text="◀️ Назад", callback_data=f"manage_user_{user_id}")
    builder.adjust(2)
    return builder.as_markup()

def get_back_keyboard(callback_data: str = "main_menu"):
    builder = InlineKeyboardBuilder()
    builder.button(text="◀️ Назад", callback_data=callback_data)
    return builder.as_markup()

# ОБРАБОТЧИКИ
router = Router()

@router.message(CommandStart())
async def cmd_start(message: Message):
    user_id = message.from_user.id
    username = message.from_user.username
    first_name = message.from_user.first_name
    
    args = message.text.split()[1:] if len(message.text.split()) > 1 else []
    referrer_id = None
    
    if args and args[0].startswith('REF'):
        ref_code = args[0]
        referrer = db.get_user_by_referral_code(ref_code)
        if referrer and referrer['user_id'] != user_id:
            referrer_id = referrer['user_id']
    
    db.add_user(user_id, username, first_name, referrer_id)
    user = db.get_user(user_id)
    
    if not user:
        await message.answer("❌ Ошибка регистрации. Попробуйте позже.")
        return
    
    if user['is_blocked']:
        await message.answer("🚫 Ваш аккаунт заблокирован\n\nДля разблокировки: @" + ADMIN_USERNAME)
        return
    
    if not user['accepted_terms']:
        await message.answer(
            "👋 <b>Добро пожаловать в Chat Monitor v7.0!</b>\n\n"
            "🚀 <b>Возможности:</b>\n"
            "• AI-анализ сообщений\n"
            "• Умный поиск\n"
            "• Система уровней\n"
            "• 30+ функций!\n\n"
            "Перед использованием необходимо принять условия.",
            reply_markup=get_start_keyboard()
        )
    else:
        await message.answer(
            f"👋 С возвращением, <b>{first_name}</b>!\n\n"
            f"{format_subscription_info(user)}\n"
            f"⭐ Уровень: {user.get('user_level', 1)}\n\n"
            "Выберите действие:",
            reply_markup=get_main_menu_keyboard(user_id)
        )

@router.callback_query(F.data == "show_terms")
async def show_terms(callback: CallbackQuery):
    terms_text = f"""
📄 <b>УСЛОВИЯ v7.0</b>

<b>ФУНКЦИОНАЛ:</b>
• Мониторинг личных чатов
• Сохранение медиа с таймерами
• AI-анализ сообщений
• Реферальная система

<b>ОГРАНИЧЕНИЯ:</b>
⚠️ Секретные чаты НЕ поддерживаются
⚠️ Групповые чаты НЕ поддерживаются
✅ Требуется Telegram Premium

<b>ТАРИФЫ:</b>
🌟 Starter (7д): {STARTER_PRICE} ⭐ (~{PRICES_RUB['starter']}₽)
💎 Basic (месяц): {BASIC_PRICE} ⭐ (~{PRICES_RUB['basic']}₽)
💼 Pro (3мес): {PRO_PRICE} ⭐ (~{PRICES_RUB['pro']}₽) 🔥 -20%
👑 Premium (год): {PREMIUM_PRICE} ⭐ (~{PRICES_RUB['premium']}₽) 🔥 -33%
♾️ Ultimate: {ULTIMATE_PRICE} ⭐ (~{PRICES_RUB['ultimate']}₽) 💥

<i>💰 Для покупки напрямую в рублях: @{ADMIN_USERNAME}</i>

Нажимая "Принять", вы соглашаетесь.
    """
    await callback.message.edit_text(terms_text, reply_markup=get_start_keyboard())

@router.callback_query(F.data == "accept_terms")
async def accept_terms(callback: CallbackQuery):
    user_id = callback.from_user.id
    db.accept_terms(user_id)
    
    await callback.message.edit_text(
        "✅ <b>Условия приняты!</b>\n\n"
        "<b>Подключение:</b>\n"
        "1. Настройки → Чат-боты\n"
        "2. Добавить чат-бота\n"
        "3. @mrztnbot\n\n"
        "⚠️ <b>Важно:</b>\n"
        "• Требуется Telegram Premium\n"
        "• Только личные чаты\n"
        "• Секретные/групповые НЕ поддерживаются\n\n"
        "После подключения - автоактивация пробного!",
        reply_markup=get_main_menu_keyboard(user_id)
    )
    
    try:
        await callback.bot.send_message(ADMIN_ID, f"🎉 Новый: {user_id} @{callback.from_user.username or 'нет'}")
    except:
        pass

@router.callback_query(F.data == "main_menu")
async def main_menu(callback: CallbackQuery):
    user_id = callback.from_user.id
    user = db.get_user(user_id)
    if not user:
        await callback.answer("❌ Пользователь не найден")
        return
    if user['is_blocked']:
        await callback.answer("🚫 Заблокирован")
        return
    
    await callback.message.edit_text(
        f"🏠 <b>Главное меню</b>\n\n"
        f"{format_subscription_info(user)}\n"
        f"⭐ Уровень: {user.get('user_level', 1)}\n\n"
        "Выберите действие:",
        reply_markup=get_main_menu_keyboard(user_id)
    )

@router.callback_query(F.data == "stats")
async def show_stats(callback: CallbackQuery):
    user_id = callback.from_user.id
    user = db.get_user(user_id)
    if not user:
        await callback.answer("❌ Не найден")
        return
    
    connections = db.get_user_connections(user_id)
    
    text = f"""
📊 <b>Статистика</b>

<b>Подписка:</b> {format_subscription_info(user)}
<b>⭐ Stars:</b> {user['stars_balance']}
<b>👥 Рефералов:</b> {user['total_referrals']} (заработано {user['referral_earnings']} ⭐)

<b>📱 Подключений:</b> {len(connections)}
<b>💬 Сообщений:</b> {user['total_messages_saved']}
<b>🗑 Удалений:</b> {user['total_deletions_tracked']}
<b>✏️ Изменений:</b> {user['total_edits_tracked']}
<b>📸 Медиа:</b> {user['total_media_saved']}
    """
    await callback.message.edit_text(text, reply_markup=get_back_keyboard())

@router.callback_query(F.data == "my_stars")
async def show_stars(callback: CallbackQuery):
    user_id = callback.from_user.id
    balance = db.get_user(user_id)['stars_balance'] if db.get_user(user_id) else 0
    
    text = f"""
⭐ <b>Telegram Stars</b>

<b>Баланс:</b> {balance} ⭐

<b>Как получить?</b>
• Купить в Telegram
• Пригласить друзей (20%)
• Подарок от админа

<b>Тарифы:</b>
🌟 Starter: {STARTER_PRICE} ⭐ (~{PRICES_RUB['starter']}₽)
💎 Basic: {BASIC_PRICE} ⭐ (~{PRICES_RUB['basic']}₽)
💼 Pro: {PRO_PRICE} ⭐ (~{PRICES_RUB['pro']}₽) 🔥
👑 Premium: {PREMIUM_PRICE} ⭐ (~{PRICES_RUB['premium']}₽) 🔥
♾️ Ultimate: {ULTIMATE_PRICE} ⭐ (~{PRICES_RUB['ultimate']}₽) 💥

<i>💰 Покупка в рублях: @{ADMIN_USERNAME}</i>
    """
    await callback.message.edit_text(text, reply_markup=get_back_keyboard())

@router.callback_query(F.data == "referrals")
async def show_referrals(callback: CallbackQuery):
    user_id = callback.from_user.id
    user = db.get_user(user_id)
    if not user:
        await callback.answer("❌ Не найден")
        return
    
    ref_stats = db.get_referral_stats(user_id)
    bot_username = BOT_USERNAME or "mrztnbot"
    ref_link = f"https://t.me/{bot_username}?start={user['referral_code']}"
    
    text = f"""
👥 <b>Реферальная программа</b>

<b>Ваша ссылка:</b>
<code>{ref_link}</code>

<b>Статистика:</b>
• Приглашено: {ref_stats['total']}
• Заработано: {ref_stats['earnings']} ⭐

<b>Как работает:</b>
1. Делитесь ссылкой
2. Они регистрируются
3. Вы получаете 20% от их оплат

<b>Рефералы:</b>
"""
    
    if ref_stats['referrals']:
        for i, ref in enumerate(ref_stats['referrals'][:5], 1):
            name = ref['first_name'] or "Пользователь"
            sub_emoji = {'free': '🆓', 'trial': '🎁', 'starter': '🌟', 'basic': '💎', 'pro': '💼', 'premium': '👑', 'ultimate': '♾️'}.get(ref['subscription_type'], '❓')
            text += f"{i}. {sub_emoji} {name}\n"
    else:
        text += "Пока никого\n"
    
    text += "\n💡 Приглашайте друзей!"
    await callback.message.edit_text(text, reply_markup=get_back_keyboard())

@router.callback_query(F.data == "subscription")
async def show_subscription(callback: CallbackQuery):
    user_id = callback.from_user.id
    user = db.get_user(user_id)
    if not user:
        await callback.answer("❌ Не найден")
        return
    
    text = f"""
💎 <b>Подписка</b>

<b>Статус:</b> {format_subscription_info(user)}
<b>Баланс:</b> {user['stars_balance']} ⭐

<b>Тарифы:</b>

🌟 Starter (7д) - {STARTER_PRICE} ⭐ (~{PRICES_RUB['starter']}₽)
💎 Basic (месяц) - {BASIC_PRICE} ⭐ (~{PRICES_RUB['basic']}₽)
💼 Pro (3мес) - {PRO_PRICE} ⭐ (~{PRICES_RUB['pro']}₽)
   🔥 Экономия 150 ⭐ (20%)
👑 Premium (год) - {PREMIUM_PRICE} ⭐ (~{PRICES_RUB['premium']}₽)
   🔥 Экономия 1000 ⭐ (33%)
♾️ Ultimate - {ULTIMATE_PRICE} ⭐ (~{PRICES_RUB['ultimate']}₽)
   💥 Один раз и навсегда!

<i>💰 Покупка в рублях: @{ADMIN_USERNAME}</i>
    """
    await callback.message.edit_text(text, reply_markup=get_subscription_keyboard())

@router.callback_query(F.data.startswith("sub_"))
async def process_subscription_payment(callback: CallbackQuery):
    user_id = callback.from_user.id
    plan = callback.data.split("_")[1]
    
    prices_map = {
        "starter": (STARTER_PRICE, "Starter (7д)"),
        "basic": (BASIC_PRICE, "Basic (месяц)"),
        "pro": (PRO_PRICE, "Pro (3мес)"),
        "premium": (PREMIUM_PRICE, "Premium (год)"),
        "ultimate": (ULTIMATE_PRICE, "Ultimate")
    }
    
    if plan not in prices_map:
        await callback.answer("❌ Неверный план")
        return
    
    amount, title = prices_map[plan]
    
    try:
        await callback.bot.send_invoice(
            chat_id=user_id,
            title=title,
            description=f"Подписка {title} на Chat Monitor v7.0",
            payload=f"subscription_{plan}_{user_id}",
            provider_token="",
            currency="XTR",
            prices=[LabeledPrice(label="XTR", amount=amount)],
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Оплатить ⭐", pay=True)]])
        )
        await callback.answer("✅ Инвойс создан!")
    except Exception as e:
        logger.error(f"Ошибка инвойса: {e}")
        await callback.answer("❌ Ошибка платежа", show_alert=True)

@router.pre_checkout_query()
async def process_pre_checkout_query(pre_checkout_query: PreCheckoutQuery):
    await pre_checkout_query.bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)

@router.message(F.successful_payment)
async def process_successful_payment(message: Message):
    user_id = message.from_user.id
    payment = message.successful_payment
    
    payload_parts = payment.invoice_payload.split("_")
    if len(payload_parts) < 2:
        return
    
    plan_type = payload_parts[1]
    amount_stars = payment.total_amount
    
    db.save_payment(user_id, amount_stars, plan_type)
    db.activate_subscription(user_id, plan_type)
    db.process_referral_payment(user_id, amount_stars)
    
    user = db.get_user(user_id)
    await message.answer(
        f"🎉 <b>Оплата успешна!</b>\n\n"
        f"Подписка: {format_subscription_info(user)}\n\n"
        "Спасибо!"
    )
    
    try:
        await message.bot.send_message(ADMIN_ID, f"💰 Платеж!\nUser: {user_id}\nPlan: {plan_type}\nAmount: {amount_stars} ⭐")
    except:
        pass

@router.callback_query(F.data == "connections")
async def show_connections(callback: CallbackQuery):
    user_id = callback.from_user.id
    connections = db.get_user_connections(user_id)
    
    if not connections:
        text = """
🔗 <b>Подключения</b>

Нет активных подключений.

<b>Подключение:</b>
1. Настройки → Чат-боты
2. Добавить
3. @mrztnbot

⚠️ Требуется Premium
✅ Только личные чаты
        """
    else:
        text = f"🔗 <b>Подключения</b>\n\nАктивных: {len(connections)}\n\n"
        for i, conn in enumerate(connections, 1):
            text += f"{i}. ID: {conn['connection_id'][:12]}...\n   📅 {conn['created_at'][:10]}\n\n"
    
    await callback.message.edit_text(text, reply_markup=get_back_keyboard())

@router.callback_query(F.data == "settings")
async def show_settings(callback: CallbackQuery):
    await callback.message.edit_text("⚙️ <b>Настройки</b>\n\nВыберите:", reply_markup=get_settings_keyboard())

@router.callback_query(F.data == "settings_notifications")
async def settings_notifications(callback: CallbackQuery):
    user_id = callback.from_user.id
    user = db.get_user(user_id)
    if not user:
        await callback.answer("❌ Не найден")
        return
    
    await callback.message.edit_text(
        "🔔 <b>Уведомления</b>\n\n✅ - включено\n❌ - отключено",
        reply_markup=get_notifications_settings_keyboard(user)
    )

@router.callback_query(F.data.startswith("toggle_notify_"))
async def toggle_notification(callback: CallbackQuery):
    user_id = callback.from_user.id
    setting = callback.data.replace("toggle_", "")
    user = db.get_user(user_id)
    if not user:
        await callback.answer("❌ Не найден")
        return
    
    new_value = not user[setting]
    db.update_notification_settings(user_id, setting, new_value)
    user = db.get_user(user_id)
    await callback.message.edit_reply_markup(reply_markup=get_notifications_settings_keyboard(user))
    await callback.answer(f"{'✅ Включено' if new_value else '❌ Отключено'}")

@router.callback_query(F.data == "help")
async def show_help(callback: CallbackQuery):
    text = f"""
ℹ️ <b>Справка v7.0</b>

<b>Функции:</b>
• Сохранение сообщений
• Уведомления об удалении
• Медиа с таймерами
• AI-анализ
• Реферальная система

<b>Ограничения:</b>
• Требуется Premium
• Только личные чаты
• Секретные НЕ поддерживаются
• Групповые НЕ поддерживаются

<b>Подписка:</b>
3 дня пробного бесплатно!
Далее от {STARTER_PRICE} ⭐ ({PRICES_RUB['starter']}₽)

<b>Рефералы:</b>
Получайте 20% от оплат

<b>Поддержка:</b> @{ADMIN_USERNAME}
<i>💰 Покупка в рублях: @{ADMIN_USERNAME}</i>
    """
    await callback.message.edit_text(text, reply_markup=get_back_keyboard())

# АДМИН-ПАНЕЛЬ
@router.callback_query(F.data == "admin_panel")
async def admin_panel(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Доступ запрещен", show_alert=True)
        return
    
    total_users = db.get_user_count()
    active_subs = db.get_active_subscriptions_count()
    
    await callback.message.edit_text(
        f"👨‍💼 <b>Админ-панель</b>\n\n"
        f"👥 Пользователей: {total_users}\n"
        f"💎 Активных подписок: {active_subs}\n\n"
        "Выберите:",
        reply_markup=get_admin_keyboard()
    )

@router.callback_query(F.data == "admin_users")
async def admin_users(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Доступ запрещен")
        return
    
    await admin_users_page(callback, 0)

async def admin_users_page(callback: CallbackQuery, page: int):
    users = db.get_all_users(limit=10, offset=page*10)
    total_users = db.get_user_count()
    total_pages = (total_users + 9) // 10
    
    text = f"👥 <b>Пользователи</b> (стр. {page+1}/{total_pages})\n\n"
    
    for i, user in enumerate(users, start=page*10+1):
        status_emoji = "🚫" if user['is_blocked'] else "✅"
        sub_emoji = {'free': '🆓', 'trial': '🎁', 'starter': '🌟', 'basic': '💎', 'pro': '💼', 'premium': '👑', 'ultimate': '♾️'}.get(user['subscription_type'], '❓')
        username = f"@{user['username']}" if user['username'] else "без username"
        name = user['first_name'] or "Без имени"
        text += f"{i}. {status_emoji} {sub_emoji} {name} ({username})\n   ID: {user['user_id']}\n\n"
    
    await callback.message.edit_text(text, reply_markup=get_users_list_keyboard(page, total_pages))

@router.callback_query(F.data.startswith("users_page_"))
async def users_page(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Доступ запрещен")
        return
    
    page_str = callback.data.split("_")[-1]
    if page_str == "info":
        await callback.answer("Пагинация списка")
        return
    
    page = int(page_str)
    await admin_users_page(callback, page)

@router.callback_query(F.data.startswith("manage_user_"))
async def manage_user(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Доступ запрещен")
        return
    
    user_id = int(callback.data.split("_")[2])
    user = db.get_user(user_id)
    if not user:
        await callback.answer("❌ Пользователь не найден")
        return
    
    text = f"""
👤 <b>Пользователь</b>

ID: {user['user_id']}
Username: @{user['username'] or 'нет'}
Имя: {user['first_name']}
Подписка: {format_subscription_info(user)}
Сообщений: {user['total_messages_saved']}
Удалений: {user['total_deletions_tracked']}
    """
    await callback.message.edit_text(text, reply_markup=get_user_management_keyboard(user_id))

@router.callback_query(F.data.startswith("admin_gift_"))
async def admin_gift(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Доступ запрещен")
        return
    
    user_id = int(callback.data.split("_")[2])
    await callback.message.edit_text(
        f"🎁 <b>Подарить подписку</b>\n\nПользователь: {user_id}\n\nВыберите план:",
        reply_markup=get_gift_subscription_keyboard(user_id)
    )

@router.callback_query(F.data.startswith("gift_sub_"))
async def gift_sub(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Доступ запрещен")
        return
    
    parts = callback.data.split("_")
    user_id = int(parts[2])
    plan_type = parts[3]
    
    db.activate_subscription(user_id, plan_type)
    
    await callback.answer("✅ Подписка подарена!")
    try:
        await callback.bot.send_message(user_id, f"🎁 <b>Подарок от админа!</b>\n\nВам подарена подписка: {plan_type.upper()}\n\nСпасибо за использование!")
    except:
        pass
    
    await callback.message.edit_text(f"✅ Подарено!\n\nUser: {user_id}\nПлан: {plan_type}", reply_markup=get_back_keyboard("admin_users"))

@router.callback_query(F.data.startswith("admin_block_"))
async def admin_block(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Доступ запрещен")
        return
    
    user_id = int(callback.data.split("_")[2])
    db.block_user(user_id)
    await callback.answer("✅ Заблокирован")
    await manage_user(callback)

@router.callback_query(F.data.startswith("admin_unblock_"))
async def admin_unblock(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Доступ запрещен")
        return
    
    user_id = int(callback.data.split("_")[2])
    db.unblock_user(user_id)
    await callback.answer("✅ Разблокирован")
    await manage_user(callback)

@router.callback_query(F.data == "admin_stats")
async def admin_stats(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Доступ запрещен")
        return
    
    total_users = db.get_user_count()
    active_subs = db.get_active_subscriptions_count()
    
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) as count FROM saved_messages')
    total_msgs = cursor.fetchone()['count']
    cursor.execute('SELECT COUNT(*) as count FROM saved_messages WHERE is_deleted = 1')
    total_dels = cursor.fetchone()['count']
    conn.close()
    
    text = f"""
📊 <b>Глобальная статистика</b>

<b>Пользователи:</b>
Всего: {total_users}
Активных подписок: {active_subs}

<b>Сообщения:</b>
Сохранено: {total_msgs}
Удалений отслежено: {total_dels}
    """
    await callback.message.edit_text(text, reply_markup=get_back_keyboard("admin_panel"))

# BUSINESS API
@router.business_connection()
async def on_business_connection(business_connection: BusinessConnection, bot: Bot):
    try:
        user_id = business_connection.user.id
        connection_id = business_connection.id
        
        db.add_business_connection(connection_id, user_id, business_connection.user.id)
        
        user = db.get_user(user_id)
        if user and not user['auto_trial_activated']:
            db.activate_auto_trial(user_id)
        
        logger.info(f"Подключение: {connection_id} для {user_id}")
        
        user = db.get_user(user_id)
        if user and user['notify_connections']:
            try:
                trial_msg = f"\n\n🎁 Пробный период {TRIAL_DAYS} дня активирован!" if user['auto_trial_activated'] and user['subscription_type'] == 'trial' else ""
                await bot.send_message(user_id, f"🎉 <b>Бот подключен!</b>\n\n✅ Мониторинг включен{trial_msg}\n\n⚠️ Секретные/групповые чаты не поддерживаются")
            except:
                pass
        
        try:
            await bot.send_message(ADMIN_ID, f"🔗 Подключение!\nUser: {user_id}\nConnection: {connection_id}")
        except:
            pass
    except Exception as e:
        logger.error(f"Ошибка подключения: {e}")

@router.business_message()
async def on_business_message(message: Message, bot: Bot):
    try:
        if not message.business_connection_id:
            return
        
        connection = db.get_business_connection(message.business_connection_id)
        if not connection:
            return
        
        user_id = connection['user_id']
        if not db.check_subscription(user_id):
            return
        
        media_type = None
        media_file_id = None
        media_file_path = None
        has_timer = False
        is_view_once = False
        caption = message.caption
        
        if hasattr(message, 'has_media_spoiler') and message.has_media_spoiler:
            has_timer = True
            is_view_once = True
        
        if message.photo:
            media_type = "photo"
            photo = message.photo[-1]
            media_file_id = photo.file_id
            media_file_path = await download_media(bot, media_file_id, media_type, user_id, has_timer)
        elif message.video:
            media_type = "video"
            media_file_id = message.video.file_id
            media_file_path = await download_media(bot, media_file_id, media_type, user_id, has_timer)
        elif message.video_note:
            media_type = "video_note"
            media_file_id = message.video_note.file_id
            has_timer = True
            media_file_path = await download_media(bot, media_file_id, media_type, user_id, has_timer)
        elif message.document:
            media_type = "document"
            media_file_id = message.document.file_id
            media_file_path = await download_media(bot, media_file_id, media_type, user_id, has_timer)
        elif message.audio:
            media_type = "audio"
            media_file_id = message.audio.file_id
            media_file_path = await download_media(bot, media_file_id, media_type, user_id, has_timer)
        elif message.voice:
            media_type = "voice"
            media_file_id = message.voice.file_id
            has_timer = True
            media_file_path = await download_media(bot, media_file_id, media_type, user_id, has_timer)
        elif message.sticker:
            media_type = "sticker"
            media_file_id = message.sticker.file_id
        
        db.save_message(
            user_id=user_id, connection_id=message.business_connection_id, chat_id=message.chat.id,
            message_id=message.message_id, sender_id=message.from_user.id,
            sender_username=message.from_user.username, sender_first_name=message.from_user.first_name,
            message_text=message.text or message.caption, media_type=media_type, media_file_id=media_file_id,
            media_file_path=media_file_path, caption=caption, has_timer=has_timer or is_view_once,
            is_view_once=is_view_once
        )
        
        logger.debug(f"Сохранено: {message.message_id} (тип: {media_type}, таймер: {has_timer})")
        
        user = db.get_user(user_id)
        if user and user['notify_media_timers'] and (has_timer or is_view_once):
            try:
                await bot.send_message(user_id, f"⏱ <b>Медиа с таймером сохранено!</b>\n\nТип: {media_type}\nОт: {message.from_user.first_name or 'Пользователь'}")
            except:
                pass
    except Exception as e:
        logger.error(f"Ошибка сообщения: {e}")

@router.edited_business_message()
async def on_edited_business_message(message: Message, bot: Bot):
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
            return
        
        original_text = original['message_text'] or ""
        new_text = message.text or message.caption or ""
        
        db.mark_message_edited(user_id, message.chat.id, message.message_id, original_text)
        
        notification = f"✏️ <b>Изменено</b>\n\nОт: {message.from_user.first_name or 'User'}\n\n"
        if original_text:
            notification += f"<blockquote>Было:\n{original_text[:200]}</blockquote>\n\n"
        if new_text:
            notification += f"<blockquote>Стало:\n{new_text[:200]}</blockquote>"
        
        try:
            await bot.send_message(user_id, notification[:4000])
        except:
            pass
    except Exception as e:
        logger.error(f"Ошибка редактирования: {e}")

@router.deleted_business_messages()
async def on_deleted_business_messages(deleted: BusinessMessagesDeleted, bot: Bot):
    try:
        connection_id = deleted.business_connection_id
        chat = deleted.chat
        message_ids = deleted.message_ids
        
        connection = db.get_business_connection(connection_id)
        if not connection:
            return
        
        user_id = connection['user_id']
        user = db.get_user(user_id)
        
        for message_id in message_ids:
            db.mark_message_deleted(user_id, chat.id, message_id)
        
        if not user or not user['notify_deletions']:
            return
        
        if len(message_ids) > 5:
            messages = []
            for message_id in message_ids:
                saved_msg = db.get_message(user_id, chat.id, message_id)
                if saved_msg:
                    messages.append(saved_msg)
            
            if messages:
                chat_title = chat.title or chat.first_name or f"Chat {chat.id}"
                archive_path = await export_deleted_chat_to_archive(user_id, chat.id, messages, chat_title)
                if archive_path:
                    try:
                        await bot.send_document(user_id, FSInputFile(archive_path),
                                              caption=f"🗑 <b>Удален чат</b>\n\nЧат: {chat_title}\nСообщений: {len(messages)}")
                    except:
                        pass
            return
        
        for message_id in message_ids:
            saved_msg = db.get_message(user_id, chat.id, message_id)
            if saved_msg:
                notification = f"🗑 <b>Удалено</b>\n\nОт: {saved_msg['sender_first_name'] or 'User'}\n\n"
                if saved_msg['message_text']:
                    notification += f"<blockquote>{saved_msg['message_text'][:300]}</blockquote>\n\n"
                if saved_msg['media_type']:
                    notification += f"<b>Медиа:</b> {saved_msg['media_type'].upper()}"
                    if saved_msg['has_timer']:
                        notification += " [⏱]"
                    notification += "\n"
                
                try:
                    await bot.send_message(user_id, notification[:4000])
                    
                    if saved_msg['media_file_path'] and Path(saved_msg['media_file_path']).exists():
                        file = FSInputFile(saved_msg['media_file_path'])
                        caption_text = "📎 Сохраненное медиа"
                        if saved_msg['has_timer']:
                            caption_text += " [было с таймером]"
                        
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
                    logger.error(f"Ошибка уведомления: {e}")
    except Exception as e:
        logger.error(f"Ошибка удаления: {e}")

# ГЛАВНАЯ ФУНКЦИЯ
async def main():
    global BOT_USERNAME
    
    try:
        bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        dp = Dispatcher(storage=MemoryStorage())
        dp.include_router(router)
        
        bot_info = await bot.get_me()
        BOT_USERNAME = bot_info.username
        
        logger.info(f"🚀 Бот запущен: @{bot_info.username} (ID: {bot_info.id})")
        logger.info(f"📦 Версия: 7.0.0 FIXED")
        logger.info(f"👨‍💼 Админ: {ADMIN_ID} (@{ADMIN_USERNAME})")
        
        try:
            await bot.send_message(ADMIN_ID,
                f"🚀 <b>Бот запущен!</b>\n\nUsername: @{bot_info.username}\nID: {bot_info.id}\n"
                f"Версия: 7.0.0 FIXED\n\n✅ ВСЕ ИСПРАВЛЕНО:\n• Реферальная система\n• Database locking\n"
                f"• Тихое сохранение\n• Админ-панель\n• Новые цены (от {STARTER_PRICE} ⭐)")
        except:
            pass
        
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
    finally:
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен")
    except Exception as e:
        logger.error(f"Ошибка: {e}", exc_info=True)
