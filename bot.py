#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram Business Message Monitor Bot
Version: 7.0.0 MEGA
Author: Business Monitor Team
Date: 2026-03-02

НОВОЕ В ВЕРСИИ 7.0.0:
- Автоматическая обработка входящих Telegram Stars с правильной конвертацией
- Автоматическая обработка входящих Telegram Gifts (15 Stars в подарке = 15 Stars)
- Верификация номера телефона для предотвращения мультиаккаунтов
- 2-уровневая реферальная система (12% + 5%)
- Вывод Stars пользователями (минимум 100 Stars, комиссия 15%)
- Автоматическая генерация HTML/PDF/TXT отчетов при удалении чатов
- Интеллектуальная очистка памяти (удаление медиа через 24 часа после отчета)
- Расширенная админ-панель с просмотром всех чатов пользователей
- Система тегов и категорий для чатов
- Персонализированные настройки уведомлений
- Аналитика и статистика удалений
- Умный поиск по всем сохраненным сообщениям
- Система лимитов для бесплатных пользователей (3 дня тест, потом покупка)
- Антиспам система и детекция мультиаккаунтов
- Gamification: достижения, уровни, бонусы
- Webhook интеграции для внешних сервисов
- Система тикетов поддержки
- Белые списки и фильтры контактов
- Расширенная финансовая аналитика
- Автоматическое сжатие изображений
- Резервное копирование данных
- И еще 10+ функций
"""

import asyncio
import logging
import os
import sys
import json
import sqlite3
import hashlib
import re
import time
import base64
import mimetypes
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple, Set
from pathlib import Path
from collections import defaultdict
from io import BytesIO

import aiofiles
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode, ChatType, ContentType
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
    Voice,
    Audio,
    Document,
    Contact,
    LabeledPrice,
    PreCheckoutQuery,
    SuccessfulPayment,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove
)
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

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
REPORTS_DIR = Path("reports")
DB_DIR = Path("database")
BACKUPS_DIR = Path("backups")

for directory in [MEDIA_DIR, EXPORTS_DIR, REPORTS_DIR, DB_DIR, BACKUPS_DIR]:
    directory.mkdir(exist_ok=True)

# Константы
USERS_PER_PAGE = 10
MAX_MEDIA_SIZE = 50 * 1024 * 1024  # 50MB
MEDIA_CLEANUP_HOURS = 24  # Очистка медиа через 24 часа после отчета
FREE_TRIAL_DAYS = 3  # Бесплатный тестовый период
REGISTRATION_BONUS_STARS = 10  # Бонус при регистрации (можно вывести)
MIN_WITHDRAWAL_STARS = 100  # Минимум для вывода
WITHDRAWAL_FEE_PERCENT = 15  # Комиссия за вывод
REFERRAL_LEVEL1_PERCENT = 12  # Процент с прямых рефералов
REFERRAL_LEVEL2_PERCENT = 5  # Процент с рефералов 2 уровня

# НОВЫЙ ПРАЙС-ЛИСТ (версия 7.0.0) с учетом комиссии Telegram 30%
SUBSCRIPTION_PRICES = {
    'week': {
        'stars': 150,  # было 100
        'rub': 270,
        'days': 7,
        'name': 'Неделя',
        'discount': 0
    },
    'month': {
        'stars': 500,  # было 1500
        'rub': 900,
        'days': 30,
        'name': 'Месяц',
        'discount': 0
    },
    'month_3': {
        'stars': 1200,  # было 1500
        'rub': 2150,
        'days': 90,
        'name': '3 месяца',
        'discount': 20
    },
    'month_6': {
        'stars': 2000,  # без изменений
        'rub': 3580,
        'days': 180,
        'name': '6 месяцев',
        'discount': 33
    },
    'year': {
        'stars': 3500,  # было 5000
        'rub': 6265,
        'days': 365,
        'name': 'Год',
        'discount': 42
    },
    'lifetime': {
        'stars': 15000,  # было 25000
        'rub': 26850,
        'days': None,
        'name': 'Навсегда',
        'discount': 50
    }
}

# Конвертация Stars в рубли
STAR_TO_RUB_RATE = 1.79

# Лимиты для бесплатных пользователей
FREE_USER_LIMITS = {
    'max_connections': 3,
    'max_saved_messages': 100,
    'storage_days': 3,
    'max_media_size_mb': 10
}

# ========================================
# FSM СОСТОЯНИЯ
# ========================================

class RegistrationStates(StatesGroup):
    """Состояния регистрации"""
    awaiting_phone = State()
    awaiting_terms_acceptance = State()

class AdminStates(StatesGroup):
    """Состояния для админ-панели"""
    main_menu = State()
    user_management = State()
    viewing_user = State()
    viewing_user_chats = State()
    viewing_specific_chat = State()
    user_number_input = State()
    send_message = State()
    gift_subscription = State()
    send_stars = State()
    send_gifts = State()
    manage_subscription = State()
    broadcast_message = State()
    statistics = State()
    search_user = State()
    financial_analytics = State()
    process_withdrawal = State()

class UserStates(StatesGroup):
    """Состояния для пользователей"""
    main_menu = State()
    subscription_menu = State()
    settings_menu = State()
    notifications_settings = State()
    search_messages = State()
    manage_tags = State()
    support_ticket = State()
    withdrawal_request = State()
    referral_menu = State()

class SubscriptionStates(StatesGroup):
    """Состояния для управления подпиской"""
    choosing_plan = State()
    choosing_payment_method = State()
    payment_confirmation = State()

# ========================================
# DATABASE
# ========================================

class Database:
    """Класс для работы с базой данных SQLite - версия 7.0.0"""
    
    def __init__(self, db_path: str = "database/bot.db"):
        self.db_path = db_path
        self.init_database()
    
    def get_connection(self):
        """Получение подключения к БД"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def init_database(self):
        """Инициализация структуры базы данных версии 7.0.0"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Таблица пользователей (расширенная)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                phone_number TEXT UNIQUE,
                registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                accepted_terms BOOLEAN DEFAULT 0,
                phone_verified BOOLEAN DEFAULT 0,
                is_active BOOLEAN DEFAULT 1,
                is_blocked BOOLEAN DEFAULT 0,
                subscription_type TEXT DEFAULT 'free',
                subscription_expires TIMESTAMP,
                trial_used BOOLEAN DEFAULT 0,
                trial_started TIMESTAMP,
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
                withdrawable_stars INTEGER DEFAULT 0,
                total_earned_stars INTEGER DEFAULT 0,
                total_spent_stars INTEGER DEFAULT 0,
                referrer_id INTEGER,
                referral_code TEXT UNIQUE,
                total_referrals INTEGER DEFAULT 0,
                total_referral_earnings INTEGER DEFAULT 0,
                user_level INTEGER DEFAULT 1,
                experience_points INTEGER DEFAULT 0,
                language TEXT DEFAULT 'ru',
                notification_settings TEXT DEFAULT '{}',
                last_report_sent TIMESTAMP,
                last_cleanup TIMESTAMP
            )
        ''')
        
        # Таблица бизнес-подключений (расширенная)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS business_connections (
                connection_id TEXT PRIMARY KEY,
                user_id INTEGER,
                connected_user_id INTEGER,
                is_enabled BOOLEAN DEFAULT 1,
                can_reply BOOLEAN DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_message_at TIMESTAMP,
                total_messages INTEGER DEFAULT 0,
                chat_title TEXT,
                chat_username TEXT,
                tags TEXT DEFAULT '',
                is_priority BOOLEAN DEFAULT 0,
                is_ignored BOOLEAN DEFAULT 0,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        ''')
        
        # Таблица сохраненных сообщений (расширенная)
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
                sender_last_name TEXT,
                message_text TEXT,
                media_type TEXT,
                media_file_id TEXT,
                media_file_path TEXT,
                media_thumbnail_path TEXT,
                media_compressed_path TEXT,
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
                included_in_report BOOLEAN DEFAULT 0,
                report_id INTEGER,
                cleaned_up BOOLEAN DEFAULT 0,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        ''')
        
        # Таблица отчетов об удалениях
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS deletion_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                chat_id INTEGER,
                report_type TEXT,
                file_path_html TEXT,
                file_path_pdf TEXT,
                file_path_txt TEXT,
                total_messages INTEGER,
                total_media INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                sent_to_user BOOLEAN DEFAULT 0,
                cleanup_scheduled TIMESTAMP,
                cleaned_up BOOLEAN DEFAULT 0,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        ''')
        
        # Таблица реферальных связей
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS referrals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                referrer_id INTEGER,
                referred_id INTEGER,
                level INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                total_earned INTEGER DEFAULT 0,
                is_active BOOLEAN DEFAULT 1,
                FOREIGN KEY (referrer_id) REFERENCES users(user_id),
                FOREIGN KEY (referred_id) REFERENCES users(user_id)
            )
        ''')
        
        # Таблица транзакций Stars
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stars_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                amount INTEGER,
                transaction_type TEXT,
                source TEXT,
                description TEXT,
                related_user_id INTEGER,
                payment_charge_id TEXT,
                is_withdrawable BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        ''')
        
        # Таблица запросов на вывод
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS withdrawal_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                amount INTEGER,
                fee_amount INTEGER,
                net_amount INTEGER,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                processed_at TIMESTAMP,
                processed_by INTEGER,
                payment_method TEXT,
                payment_details TEXT,
                notes TEXT,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        ''')
        
        # Таблица платежей
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                amount INTEGER,
                currency TEXT DEFAULT 'XTR',
                plan_type TEXT,
                payment_method TEXT,
                telegram_payment_charge_id TEXT,
                provider_payment_charge_id TEXT,
                invoice_payload TEXT,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                confirmed_at TIMESTAMP,
                referrer_commission INTEGER DEFAULT 0,
                referrer_id INTEGER,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        ''')
        
        # Таблица достижений
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS achievements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                achievement_type TEXT,
                achievement_name TEXT,
                achievement_description TEXT,
                reward_stars INTEGER DEFAULT 0,
                unlocked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        ''')
        
        # Таблица тикетов поддержки
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS support_tickets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                subject TEXT,
                message TEXT,
                status TEXT DEFAULT 'open',
                priority TEXT DEFAULT 'normal',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP,
                closed_at TIMESTAMP,
                admin_response TEXT,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        ''')
        
        # Таблица webhook логов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS webhook_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                event_type TEXT,
                payload TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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
        
        # Таблица статистики
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS statistics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                stat_type TEXT,
                stat_value INTEGER,
                metadata TEXT,
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
                ip_address TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблица черного списка телефонов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS phone_blacklist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phone_number TEXT UNIQUE,
                reason TEXT,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                added_by INTEGER
            )
        ''')
        
        # Создание индексов для оптимизации
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_phone ON users(phone_number)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_referrer ON users(referrer_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_messages_lookup ON saved_messages(user_id, chat_id, message_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_messages_deleted ON saved_messages(user_id, is_deleted)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_connections_user ON business_connections(user_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals(referrer_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_user ON stars_transactions(user_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_payments_user ON payments(user_id)')
        
        conn.commit()
        conn.close()
        logger.info("База данных инициализирована (версия 7.0.0 MEGA)")
    
    # ========================================
    # МЕТОДЫ ДЛЯ РАБОТЫ С ПОЛЬЗОВАТЕЛЯМИ
    # ========================================
    
    def add_user(self, user_id: int, username: str = None, first_name: str = None, last_name: str = None):
        """Добавление нового пользователя"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            # Генерация уникального реферального кода
            referral_code = self.generate_referral_code(user_id)
            
            cursor.execute('''
                INSERT OR IGNORE INTO users (user_id, username, first_name, last_name, referral_code)
                VALUES (?, ?, ?, ?, ?)
            ''', (user_id, username, first_name, last_name, referral_code))
            conn.commit()
            
            if cursor.rowcount > 0:
                logger.info(f"Новый пользователь добавлен: {user_id}")
                return True
            return False
        except Exception as e:
            logger.error(f"Ошибка добавления пользователя: {e}")
            return False
        finally:
            conn.close()
    
    def generate_referral_code(self, user_id: int) -> str:
        """Генерация уникального реферального кода"""
        base = f"{user_id}{int(time.time())}"
        return hashlib.md5(base.encode()).hexdigest()[:8].upper()
    
    def get_user(self, user_id: int) -> Optional[Dict]:
        """Получение данных пользователя"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None
    
    def verify_phone(self, user_id: int, phone_number: str) -> Tuple[bool, str]:
        """Верификация номера телефона"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Проверка черного списка
            cursor.execute('SELECT * FROM phone_blacklist WHERE phone_number = ?', (phone_number,))
            if cursor.fetchone():
                conn.close()
                return False, "Этот номер заблокирован системой"
            
            # Проверка существующих пользователей с этим номером
            cursor.execute('SELECT user_id FROM users WHERE phone_number = ? AND user_id != ?', 
                          (phone_number, user_id))
            if cursor.fetchone():
                conn.close()
                return False, "Этот номер уже используется другим аккаунтом"
            
            # Сохранение номера
            cursor.execute('''
                UPDATE users 
                SET phone_number = ?, phone_verified = 1, trial_started = CURRENT_TIMESTAMP
                WHERE user_id = ?
            ''', (phone_number, user_id))
            
            # Начисление регистрационного бонуса (можно вывести)
            cursor.execute('''
                UPDATE users 
                SET stars_balance = stars_balance + ?,
                    withdrawable_stars = withdrawable_stars + ?
                WHERE user_id = ?
            ''', (REGISTRATION_BONUS_STARS, REGISTRATION_BONUS_STARS, user_id))
            
            # Запись транзакции
            cursor.execute('''
                INSERT INTO stars_transactions 
                (user_id, amount, transaction_type, source, description, is_withdrawable)
                VALUES (?, ?, 'bonus', 'registration', 'Бонус за регистрацию', 1)
            ''', (user_id, REGISTRATION_BONUS_STARS))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Телефон верифицирован для пользователя {user_id}: {phone_number}")
            return True, f"Номер успешно подтвержден! Бонус: {REGISTRATION_BONUS_STARS} Stars"
            
        except Exception as e:
            conn.close()
            logger.error(f"Ошибка верификации телефона: {e}")
            return False, "Произошла ошибка при верификации"
    
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
    
    def check_subscription(self, user_id: int) -> bool:
        """Проверка активности подписки с учетом тестового периода"""
        user = self.get_user(user_id)
        if not user:
            return False
        
        if user['is_blocked']:
            return False
        
        # Проверка телефона
        if not user['phone_verified']:
            return False
        
        # Бесплатный тестовый период
        if user['trial_started']:
            trial_start = datetime.fromisoformat(user['trial_started'])
            trial_end = trial_start + timedelta(days=FREE_TRIAL_DAYS)
            if datetime.now() < trial_end:
                return True  # Тестовый период еще активен
        
        # Проверка платной подписки
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
        
        # Начисление достижения
        self.unlock_achievement(user_id, 'first_subscription', 
                               'Первая подписка', 'Вы приобрели первую подписку!', 25)
    
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
    
    def block_user(self, user_id: int, reason: str = ""):
        """Блокировка пользователя"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE users SET is_blocked = 1 WHERE user_id = ?
        ''', (user_id,))
        conn.commit()
        conn.close()
        self.log_admin_action(ADMIN_ID, user_id, 'user_blocked', f'Reason: {reason}')
    
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
    # МЕТОДЫ ДЛЯ РАБОТЫ СО STARS И ПЛАТЕЖАМИ
    # ========================================
    
    def process_incoming_stars(self, user_id: int, amount: int, payment_charge_id: str, 
                               source: str = "payment") -> bool:
        """Обработка входящих Stars (автоматическое зачисление)"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Зачисление Stars на баланс
            cursor.execute('''
                UPDATE users 
                SET stars_balance = stars_balance + ?,
                    total_earned_stars = total_earned_stars + ?
                WHERE user_id = ?
            ''', (amount, amount, user_id))
            
            # Запись транзакции
            cursor.execute('''
                INSERT INTO stars_transactions 
                (user_id, amount, transaction_type, source, description, payment_charge_id, is_withdrawable)
                VALUES (?, ?, 'incoming', ?, 'Пополнение через Telegram Stars', ?, 0)
            ''', (user_id, amount, source, payment_charge_id))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Stars зачислены: user={user_id}, amount={amount}, source={source}")
            
            # Обработка реферальных начислений
            self.process_referral_commission(user_id, amount)
            
            return True
            
        except Exception as e:
            conn.close()
            logger.error(f"Ошибка обработки Stars: {e}")
            return False
    
    def process_incoming_gift(self, user_id: int, gift_stars_value: int, gift_id: str) -> bool:
        """
        Обработка входящих Telegram Gifts
        ВАЖНО: Если подарок стоит 15 Stars, начисляется 15 Stars на баланс
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Зачисление Stars на баланс (по стоимости подарка)
            cursor.execute('''
                UPDATE users 
                SET stars_balance = stars_balance + ?,
                    total_earned_stars = total_earned_stars + ?
                WHERE user_id = ?
            ''', (gift_stars_value, gift_stars_value, user_id))
            
            # Запись транзакции
            cursor.execute('''
                INSERT INTO stars_transactions 
                (user_id, amount, transaction_type, source, description, payment_charge_id, is_withdrawable)
                VALUES (?, ?, 'incoming', 'gift', ?, ?, 0)
            ''', (user_id, gift_stars_value, 
                  f'Получен подарок на {gift_stars_value} Stars', gift_id))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Gift обработан: user={user_id}, stars_value={gift_stars_value}, gift_id={gift_id}")
            
            # Обработка реферальных начислений
            self.process_referral_commission(user_id, gift_stars_value)
            
            return True
            
        except Exception as e:
            conn.close()
            logger.error(f"Ошибка обработки Gift: {e}")
            return False
    
    def spend_stars(self, user_id: int, amount: int, description: str = "", 
                   plan_type: str = "") -> bool:
        """Списание Stars за подписку"""
        user = self.get_user(user_id)
        if not user or user['stars_balance'] < amount:
            return False
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                UPDATE users 
                SET stars_balance = stars_balance - ?,
                    total_spent_stars = total_spent_stars + ?
                WHERE user_id = ?
            ''', (amount, amount, user_id))
            
            cursor.execute('''
                INSERT INTO stars_transactions 
                (user_id, amount, transaction_type, source, description, is_withdrawable)
                VALUES (?, ?, 'spend', 'subscription', ?, 0)
            ''', (user_id, -amount, description or f'Покупка подписки: {plan_type}'))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Stars списаны: user={user_id}, amount={amount}, plan={plan_type}")
            return True
            
        except Exception as e:
            conn.close()
            logger.error(f"Ошибка списания Stars: {e}")
            return False
    
    def add_withdrawable_stars(self, user_id: int, amount: int, description: str = ""):
        """Добавление Stars которые можно вывести (например, реферальные)"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE users 
            SET stars_balance = stars_balance + ?,
                withdrawable_stars = withdrawable_stars + ?,
                total_earned_stars = total_earned_stars + ?
            WHERE user_id = ?
        ''', (amount, amount, amount, user_id))
        
        cursor.execute('''
            INSERT INTO stars_transactions 
            (user_id, amount, transaction_type, source, description, is_withdrawable)
            VALUES (?, ?, 'referral', 'commission', ?, 1)
        ''', (user_id, amount, description))
        
        conn.commit()
        conn.close()
    
    def create_withdrawal_request(self, user_id: int, amount: int) -> Tuple[bool, str]:
        """Создание запроса на вывод Stars"""
        user = self.get_user(user_id)
        
        if not user:
            return False, "Пользователь не найден"
        
        if amount < MIN_WITHDRAWAL_STARS:
            return False, f"Минимальная сумма вывода: {MIN_WITHDRAWAL_STARS} Stars"
        
        if user['withdrawable_stars'] < amount:
            return False, f"Недостаточно средств для вывода. Доступно: {user['withdrawable_stars']} Stars"
        
        # Расчет комиссии
        fee = int(amount * WITHDRAWAL_FEE_PERCENT / 100)
        net_amount = amount - fee
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Создание запроса
            cursor.execute('''
                INSERT INTO withdrawal_requests 
                (user_id, amount, fee_amount, net_amount, status)
                VALUES (?, ?, ?, ?, 'pending')
            ''', (user_id, amount, fee, net_amount))
            
            # Резервирование средств
            cursor.execute('''
                UPDATE users 
                SET withdrawable_stars = withdrawable_stars - ?
                WHERE user_id = ?
            ''', (amount, user_id))
            
            conn.commit()
            request_id = cursor.lastrowid
            conn.close()
            
            logger.info(f"Запрос на вывод создан: user={user_id}, amount={amount}, id={request_id}")
            return True, f"Запрос #{request_id} создан. К выводу: {net_amount} Stars (комиссия {fee} Stars)"
            
        except Exception as e:
            conn.close()
            logger.error(f"Ошибка создания запроса на вывод: {e}")
            return False, "Ошибка создания запроса"
    
    def get_pending_withdrawals(self) -> List[Dict]:
        """Получение всех ожидающих запросов на вывод"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT w.*, u.username, u.first_name, u.phone_number
            FROM withdrawal_requests w
            JOIN users u ON w.user_id = u.user_id
            WHERE w.status = 'pending'
            ORDER BY w.created_at ASC
        ''')
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def process_withdrawal(self, request_id: int, admin_id: int, 
                          status: str, notes: str = "") -> bool:
        """Обработка запроса на вывод администратором"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('SELECT * FROM withdrawal_requests WHERE id = ?', (request_id,))
            request = cursor.fetchone()
            
            if not request or request['status'] != 'pending':
                conn.close()
                return False
            
            if status == 'approved':
                # Списание Stars с общего баланса
                cursor.execute('''
                    UPDATE users 
                    SET stars_balance = stars_balance - ?
                    WHERE user_id = ?
                ''', (request['amount'], request['user_id']))
                
                # Запись транзакции
                cursor.execute('''
                    INSERT INTO stars_transactions 
                    (user_id, amount, transaction_type, source, description, is_withdrawable)
                    VALUES (?, ?, 'withdrawal', 'approved', ?, 0)
                ''', (request['user_id'], -request['net_amount'], 
                      f"Вывод средств #{request_id}"))
                
            elif status == 'rejected':
                # Возврат зарезервированных средств
                cursor.execute('''
                    UPDATE users 
                    SET withdrawable_stars = withdrawable_stars + ?
                    WHERE user_id = ?
                ''', (request['amount'], request['user_id']))
            
            # Обновление статуса запроса
            cursor.execute('''
                UPDATE withdrawal_requests 
                SET status = ?, processed_at = CURRENT_TIMESTAMP, 
                    processed_by = ?, notes = ?
                WHERE id = ?
            ''', (status, admin_id, notes, request_id))
            
            conn.commit()
            conn.close()
            
            self.log_admin_action(admin_id, request['user_id'], f'withdrawal_{status}',
                                 f'Request #{request_id}, Amount: {request["amount"]}')
            
            return True
            
        except Exception as e:
            conn.close()
            logger.error(f"Ошибка обработки вывода: {e}")
            return False
    
    # ========================================
    # РЕФЕРАЛЬНАЯ СИСТЕМА
    # ========================================
    
    def set_referrer(self, user_id: int, referral_code: str) -> bool:
        """Установка реферера при регистрации"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Поиск реферера по коду
            cursor.execute('SELECT user_id FROM users WHERE referral_code = ?', (referral_code,))
            referrer = cursor.fetchone()
            
            if not referrer:
                conn.close()
                return False
            
            referrer_id = referrer['user_id']
            
            # Нельзя быть рефералом самого себя
            if referrer_id == user_id:
                conn.close()
                return False
            
            # Установка реферера
            cursor.execute('UPDATE users SET referrer_id = ? WHERE user_id = ?', 
                          (referrer_id, user_id))
            
            # Создание реферальной связи уровня 1
            cursor.execute('''
                INSERT INTO referrals (referrer_id, referred_id, level)
                VALUES (?, ?, 1)
            ''', (referrer_id, user_id))
            
            # Увеличение счетчика рефералов
            cursor.execute('''
                UPDATE users SET total_referrals = total_referrals + 1
                WHERE user_id = ?
            ''', (referrer_id,))
            
            # Поиск реферера 2-го уровня
            cursor.execute('SELECT referrer_id FROM users WHERE user_id = ?', (referrer_id,))
            level2 = cursor.fetchone()
            
            if level2 and level2['referrer_id']:
                cursor.execute('''
                    INSERT INTO referrals (referrer_id, referred_id, level)
                    VALUES (?, ?, 2)
                ''', (level2['referrer_id'], user_id))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Реферер установлен: user={user_id}, referrer={referrer_id}")
            
            # Бонус рефереру за приглашение
            self.add_withdrawable_stars(referrer_id, 5, f"Бонус за приглашение пользователя {user_id}")
            
            return True
            
        except Exception as e:
            conn.close()
            logger.error(f"Ошибка установки реферера: {e}")
            return False
    
    def process_referral_commission(self, user_id: int, amount: int):
        """Начисление реферальных комиссий при пополнении"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Получение всех рефереров пользователя
            cursor.execute('''
                SELECT referrer_id, level FROM referrals 
                WHERE referred_id = ? AND is_active = 1
            ''', (user_id,))
            
            referrals = cursor.fetchall()
            
            for ref in referrals:
                referrer_id = ref['referrer_id']
                level = ref['level']
                
                # Расчет комиссии
                if level == 1:
                    commission = int(amount * REFERRAL_LEVEL1_PERCENT / 100)
                elif level == 2:
                    commission = int(amount * REFERRAL_LEVEL2_PERCENT / 100)
                else:
                    continue
                
                if commission > 0:
                    # Начисление комиссии
                    self.add_withdrawable_stars(
                        referrer_id, 
                        commission,
                        f"Реферальная комиссия {level} уровня от пользователя {user_id}"
                    )
                    
                    # Обновление статистики
                    cursor.execute('''
                        UPDATE users 
                        SET total_referral_earnings = total_referral_earnings + ?
                        WHERE user_id = ?
                    ''', (commission, referrer_id))
                    
                    cursor.execute('''
                        UPDATE referrals 
                        SET total_earned = total_earned + ?
                        WHERE referrer_id = ? AND referred_id = ?
                    ''', (commission, referrer_id, user_id))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            conn.close()
            logger.error(f"Ошибка начисления реферальных комиссий: {e}")
    
    def get_referral_stats(self, user_id: int) -> Dict:
        """Получение реферальной статистики"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Рефералы 1 уровня
        cursor.execute('''
            SELECT COUNT(*) as count, COALESCE(SUM(total_earned), 0) as earned
            FROM referrals WHERE referrer_id = ? AND level = 1
        ''', (user_id,))
        level1 = cursor.fetchone()
        
        # Рефералы 2 уровня
        cursor.execute('''
            SELECT COUNT(*) as count, COALESCE(SUM(total_earned), 0) as earned
            FROM referrals WHERE referrer_id = ? AND level = 2
        ''', (user_id,))
        level2 = cursor.fetchone()
        
        user = self.get_user(user_id)
        
        conn.close()
        
        return {
            'referral_code': user['referral_code'],
            'level1_count': level1['count'],
            'level1_earned': level1['earned'],
            'level2_count': level2['count'],
            'level2_earned': level2['earned'],
            'total_earned': user['total_referral_earnings']
        }
    
    # ========================================
    # СИСТЕМА ДОСТИЖЕНИЙ
    # ========================================
    
    def unlock_achievement(self, user_id: int, achievement_type: str, 
                          name: str, description: str, reward_stars: int = 0):
        """Разблокировка достижения"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Проверка, не разблокировано ли уже
            cursor.execute('''
                SELECT id FROM achievements 
                WHERE user_id = ? AND achievement_type = ?
            ''', (user_id, achievement_type))
            
            if cursor.fetchone():
                conn.close()
                return False  # Уже разблокировано
            
            # Разблокировка
            cursor.execute('''
                INSERT INTO achievements 
                (user_id, achievement_type, achievement_name, achievement_description, reward_stars)
                VALUES (?, ?, ?, ?, ?)
            ''', (user_id, achievement_type, name, description, reward_stars))
            
            # Начисление награды
            if reward_stars > 0:
                cursor.execute('''
                    UPDATE users 
                    SET stars_balance = stars_balance + ?,
                        experience_points = experience_points + ?
                    WHERE user_id = ?
                ''', (reward_stars, reward_stars * 10, user_id))
                
                cursor.execute('''
                    INSERT INTO stars_transactions 
                    (user_id, amount, transaction_type, source, description, is_withdrawable)
                    VALUES (?, ?, 'achievement', ?, ?, 0)
                ''', (user_id, reward_stars, achievement_type, 
                      f'Награда за достижение: {name}'))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Достижение разблокировано: user={user_id}, type={achievement_type}")
            return True
            
        except Exception as e:
            conn.close()
            logger.error(f"Ошибка разблокировки достижения: {e}")
            return False
    
    # ========================================
    # МЕТОДЫ ДЛЯ РАБОТЫ С ПОДКЛЮЧЕНИЯМИ
    # ========================================
    
    def add_business_connection(self, connection_id: str, user_id: int, 
                               connected_user_id: int, can_reply: bool = False):
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
        cursor.execute('''
            SELECT * FROM business_connections 
            WHERE user_id = ? AND is_enabled = 1
            ORDER BY last_message_at DESC
        ''', (user_id,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def update_connection_stats(self, connection_id: str, chat_title: str = None):
        """Обновление статистики подключения"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE business_connections 
            SET total_messages = total_messages + 1,
                last_message_at = CURRENT_TIMESTAMP,
                chat_title = COALESCE(?, chat_title)
            WHERE connection_id = ?
        ''', (chat_title, connection_id))
        conn.commit()
        conn.close()
    
    # ========================================
    # МЕТОДЫ ДЛЯ РАБОТЫ С СООБЩЕНИЯМИ
    # ========================================
    
    def save_message(self, user_id: int, connection_id: str, chat_id: int, message_id: int,
                    sender_id: int, sender_username: str = None, sender_first_name: str = None,
                    sender_last_name: str = None, message_text: str = None,
                    media_type: str = None, media_file_id: str = None, media_file_path: str = None,
                    media_thumbnail_path: str = None, media_compressed_path: str = None,
                    caption: str = None, has_timer: bool = False, timer_seconds: int = None,
                    is_view_once: bool = False, media_width: int = None, media_height: int = None,
                    media_duration: int = None, media_file_size: int = None):
        """Сохранение сообщения"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            timer_expires = None
            if has_timer and timer_seconds:
                timer_expires = datetime.now() + timedelta(seconds=timer_seconds)
            
            cursor.execute('''
                INSERT INTO saved_messages 
                (user_id, connection_id, chat_id, message_id, sender_id, sender_username,
                 sender_first_name, sender_last_name, message_text, media_type, media_file_id,
                 media_file_path, media_thumbnail_path, media_compressed_path, caption,
                 has_timer, timer_seconds, timer_expires, is_view_once,
                 media_width, media_height, media_duration, media_file_size)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (user_id, connection_id, chat_id, message_id, sender_id, sender_username,
                  sender_first_name, sender_last_name, message_text, media_type, media_file_id,
                  media_file_path, media_thumbnail_path, media_compressed_path, caption,
                  has_timer, timer_seconds, timer_expires, is_view_once,
                  media_width, media_height, media_duration, media_file_size))
            
            # Обновление статистики
            cursor.execute('''
                UPDATE users SET total_messages_saved = total_messages_saved + 1
                WHERE user_id = ?
            ''', (user_id,))
            
            if media_type:
                cursor.execute('''
                    UPDATE users SET total_media_saved = total_media_saved + 1
                    WHERE user_id = ?
                ''', (user_id,))
                
                # Обновление счетчика конкретного типа медиа
                if media_type in ['photo', 'video', 'document', 'audio', 'voice', 'video_note']:
                    media_column = f'total_{media_type}'
                    cursor.execute(f'''
                        UPDATE users SET {media_column} = {media_column} + 1
                        WHERE user_id = ?
                    ''', (user_id,))
            
            conn.commit()
            message_db_id = cursor.lastrowid
            
            # Проверка достижений
            self.check_message_achievements(user_id)
            
            return message_db_id
            
        except Exception as e:
            logger.error(f"Ошибка сохранения сообщения: {e}")
            return None
        finally:
            conn.close()
    
    def check_message_achievements(self, user_id: int):
        """Проверка достижений по сообщениям"""
        user = self.get_user(user_id)
        if not user:
            return
        
        total = user['total_messages_saved']
        
        if total >= 100 and total < 101:
            self.unlock_achievement(user_id, 'messages_100', '100 сообщений', 
                                  'Сохранено 100 сообщений', 10)
        elif total >= 1000 and total < 1001:
            self.unlock_achievement(user_id, 'messages_1000', '1000 сообщений',
                                  'Сохранено 1000 сообщений', 50)
        elif total >= 10000 and total < 10001:
            self.unlock_achievement(user_id, 'messages_10000', '10000 сообщений',
                                  'Сохранено 10000 сообщений', 200)
    
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
    
    def get_chat_messages(self, user_id: int, chat_id: int, 
                         deleted_only: bool = False) -> List[Dict]:
        """Получение всех сообщений из чата"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        if deleted_only:
            cursor.execute('''
                SELECT * FROM saved_messages 
                WHERE user_id = ? AND chat_id = ? AND is_deleted = 1
                ORDER BY created_at ASC
            ''', (user_id, chat_id))
        else:
            cursor.execute('''
                SELECT * FROM saved_messages 
                WHERE user_id = ? AND chat_id = ?
                ORDER BY created_at ASC
            ''', (user_id, chat_id))
        
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    # ========================================
    # ГЕНЕРАЦИЯ ОТЧЕТОВ ОБ УДАЛЕНИЯХ
    # ========================================
    
    def generate_deletion_report(self, user_id: int, chat_id: int) -> Optional[int]:
        """Генерация отчета об удаленных сообщениях"""
        messages = self.get_chat_messages(user_id, chat_id, deleted_only=True)
        
        if not messages:
            return None
        
        # Создание директории для отчетов
        user_reports_dir = REPORTS_DIR / str(user_id)
        user_reports_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        base_filename = f"chat_{chat_id}_deleted_{timestamp}"
        
        # Генерация HTML отчета
        html_path = user_reports_dir / f"{base_filename}.html"
        self._generate_html_report(messages, html_path, chat_id)
        
        # Генерация TXT отчета
        txt_path = user_reports_dir / f"{base_filename}.txt"
        self._generate_txt_report(messages, txt_path, chat_id)
        
        # Сохранение информации об отчете в БД
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO deletion_reports 
            (user_id, chat_id, report_type, file_path_html, file_path_txt,
             total_messages, total_media, cleanup_scheduled)
            VALUES (?, ?, 'deletion', ?, ?, ?, ?, ?)
        ''', (user_id, chat_id, str(html_path), str(txt_path),
              len(messages), 
              sum(1 for m in messages if m['media_type']),
              datetime.now() + timedelta(hours=MEDIA_CLEANUP_HOURS)))
        
        report_id = cursor.lastrowid
        
        # Пометка сообщений как включенных в отчет
        for msg in messages:
            cursor.execute('''
                UPDATE saved_messages 
                SET included_in_report = 1, report_id = ?
                WHERE id = ?
            ''', (report_id, msg['id']))
        
        conn.commit()
        conn.close()
        
        logger.info(f"Отчет создан: report_id={report_id}, user={user_id}, chat={chat_id}")
        
        return report_id
    
    def _generate_html_report(self, messages: List[Dict], output_path: Path, chat_id: int):
        """Генерация HTML отчета"""
        html_content = f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Отчет об удаленных сообщениях - Чат {chat_id}</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background: #f5f5f5;
        }}
        .header {{
            background: #2196F3;
            color: white;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
        }}
        .message {{
            background: white;
            padding: 15px;
            margin-bottom: 15px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .message-header {{
            display: flex;
            justify-content: space-between;
            margin-bottom: 10px;
            padding-bottom: 10px;
            border-bottom: 1px solid #eee;
        }}
        .sender {{
            font-weight: bold;
            color: #2196F3;
        }}
        .timestamp {{
            color: #666;
            font-size: 0.9em;
        }}
        .message-text {{
            margin: 10px 0;
            line-height: 1.6;
        }}
        .media-info {{
            background: #f9f9f9;
            padding: 10px;
            border-left: 3px solid #2196F3;
            margin: 10px 0;
        }}
        .media-image {{
            max-width: 100%;
            border-radius: 4px;
            margin: 10px 0;
        }}
        .timer-badge {{
            background: #ff9800;
            color: white;
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 0.85em;
        }}
        .view-once-badge {{
            background: #e91e63;
            color: white;
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 0.85em;
        }}
        .stats {{
            background: white;
            padding: 15px;
            border-radius: 8px;
            margin-bottom: 20px;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🗑 Отчет об удаленных сообщениях</h1>
        <p>Чат ID: {chat_id}</p>
        <p>Дата создания отчета: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}</p>
    </div>
    
    <div class="stats">
        <h3>📊 Статистика</h3>
        <p>Всего удаленных сообщений: {len(messages)}</p>
        <p>Медиафайлов: {sum(1 for m in messages if m['media_type'])}</p>
        <p>С таймерами: {sum(1 for m in messages if m['has_timer'])}</p>
        <p>Одноразовых: {sum(1 for m in messages if m['is_view_once'])}</p>
    </div>
"""
        
        for msg in messages:
            sender = msg['sender_username'] or f"{msg['sender_first_name'] or ''} {msg['sender_last_name'] or ''}".strip()
            if not sender:
                sender = f"ID: {msg['sender_id']}"
            
            created = datetime.fromisoformat(msg['created_at']).strftime('%d.%m.%Y %H:%M:%S')
            deleted = datetime.fromisoformat(msg['deleted_at']).strftime('%d.%m.%Y %H:%M:%S') if msg['deleted_at'] else 'N/A'
            
            badges = []
            if msg['has_timer']:
                badges.append(f'<span class="timer-badge">⏱ Таймер {msg["timer_seconds"]}с</span>')
            if msg['is_view_once']:
                badges.append('<span class="view-once-badge">👁 Одноразовое</span>')
            
            html_content += f"""
    <div class="message">
        <div class="message-header">
            <div class="sender">От: {sender}</div>
            <div class="timestamp">
                Создано: {created}<br>
                Удалено: {deleted}
            </div>
        </div>
        {' '.join(badges) if badges else ''}
        <div class="message-text">
            {msg['message_text'] or msg['caption'] or '<i>Без текста</i>'}
        </div>
"""
            
            if msg['media_type']:
                html_content += f"""
        <div class="media-info">
            <strong>📎 Медиа:</strong> {msg['media_type'].upper()}<br>
"""
                if msg['media_file_size']:
                    size_mb = msg['media_file_size'] / (1024 * 1024)
                    html_content += f"            <strong>Размер:</strong> {size_mb:.2f} МБ<br>\n"
                
                if msg['media_duration']:
                    html_content += f"            <strong>Длительность:</strong> {msg['media_duration']} сек<br>\n"
                
                # Встроенное изображение (base64) если есть
                if msg['media_compressed_path'] and Path(msg['media_compressed_path']).exists():
                    try:
                        with open(msg['media_compressed_path'], 'rb') as f:
                            img_data = base64.b64encode(f.read()).decode()
                            html_content += f'            <img src="data:image/jpeg;base64,{img_data}" class="media-image" alt="Media">\n'
                    except:
                        pass
                
                html_content += "        </div>\n"
            
            html_content += "    </div>\n"
        
        html_content += """
</body>
</html>
"""
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
    
    def _generate_txt_report(self, messages: List[Dict], output_path: Path, chat_id: int):
        """Генерация TXT отчета"""
        txt_content = f"""
{'='*80}
ОТЧЕТ ОБ УДАЛЕННЫХ СООБЩЕНИЯХ
{'='*80}

Чат ID: {chat_id}
Дата создания отчета: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}

СТАТИСТИКА:
Всего удаленных сообщений: {len(messages)}
Медиафайлов: {sum(1 for m in messages if m['media_type'])}
С таймерами: {sum(1 for m in messages if m['has_timer'])}
Одноразовых: {sum(1 for m in messages if m['is_view_once'])}

{'='*80}
СООБЩЕНИЯ:
{'='*80}

"""
        
        for i, msg in enumerate(messages, 1):
            sender = msg['sender_username'] or f"{msg['sender_first_name'] or ''} {msg['sender_last_name'] or ''}".strip()
            if not sender:
                sender = f"ID: {msg['sender_id']}"
            
            created = datetime.fromisoformat(msg['created_at']).strftime('%d.%m.%Y %H:%M:%S')
            deleted = datetime.fromisoformat(msg['deleted_at']).strftime('%d.%m.%Y %H:%M:%S') if msg['deleted_at'] else 'N/A'
            
            txt_content += f"""
[{i}] Сообщение #{msg['message_id']}
{'-'*80}
От: {sender}
Создано: {created}
Удалено: {deleted}
"""
            
            if msg['has_timer']:
                txt_content += f"⏱ С ТАЙМЕРОМ: {msg['timer_seconds']} секунд\n"
            if msg['is_view_once']:
                txt_content += "👁 ОДНОРАЗОВОЕ ПРОСМОТР\n"
            
            if msg['message_text'] or msg['caption']:
                txt_content += f"\nТекст:\n{msg['message_text'] or msg['caption']}\n"
            
            if msg['media_type']:
                txt_content += f"\n📎 Медиа: {msg['media_type'].upper()}\n"
                if msg['media_file_size']:
                    size_mb = msg['media_file_size'] / (1024 * 1024)
                    txt_content += f"Размер: {size_mb:.2f} МБ\n"
                if msg['media_duration']:
                    txt_content += f"Длительность: {msg['media_duration']} сек\n"
                if msg['media_file_path']:
                    txt_content += f"Файл: {msg['media_file_path']}\n"
            
            txt_content += f"{'-'*80}\n"
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(txt_content)
    
    def get_report(self, report_id: int) -> Optional[Dict]:
        """Получение информации об отчете"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM deletion_reports WHERE id = ?', (report_id,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None
    
    def mark_report_sent(self, report_id: int):
        """Отметка отчета как отправленного"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE deletion_reports 
            SET sent_to_user = 1
            WHERE id = ?
        ''', (report_id,))
        conn.commit()
        conn.close()
    
    # ========================================
    # ОЧИСТКА ПАМЯТИ
    # ========================================
    
    def cleanup_old_media(self):
        """Автоматическая очистка старых медиафайлов"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Поиск отчетов для очистки
        cursor.execute('''
            SELECT * FROM deletion_reports 
            WHERE sent_to_user = 1 
            AND cleaned_up = 0
            AND cleanup_scheduled <= CURRENT_TIMESTAMP
        ''')
        
        reports = cursor.fetchall()
        
        for report in reports:
            report_id = report['id']
            
            # Получение сообщений из отчета
            cursor.execute('''
                SELECT * FROM saved_messages 
                WHERE report_id = ? AND cleaned_up = 0
            ''', (report_id,))
            
            messages = cursor.fetchall()
            
            # Удаление медиафайлов
            for msg in messages:
                if msg['media_file_path']:
                    try:
                        Path(msg['media_file_path']).unlink(missing_ok=True)
                    except:
                        pass
                
                if msg['media_thumbnail_path']:
                    try:
                        Path(msg['media_thumbnail_path']).unlink(missing_ok=True)
                    except:
                        pass
                
                if msg['media_compressed_path']:
                    try:
                        Path(msg['media_compressed_path']).unlink(missing_ok=True)
                    except:
                        pass
                
                # Пометка как очищенного
                cursor.execute('''
                    UPDATE saved_messages 
                    SET cleaned_up = 1
                    WHERE id = ?
                ''', (msg['id'],))
            
            # Пометка отчета как очищенного
            cursor.execute('''
                UPDATE deletion_reports 
                SET cleaned_up = 1
                WHERE id = ?
            ''', (report_id,))
            
            logger.info(f"Очищены медиа для отчета #{report_id}")
        
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
               OR phone_number LIKE ?
            ORDER BY registered_at DESC
        ''', (search_pattern, search_pattern, search_pattern, search_pattern, search_pattern))
        
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
    
    def get_financial_stats(self) -> Dict:
        """Получение финансовой статистики"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Общий баланс Stars у всех пользователей
        cursor.execute('SELECT SUM(stars_balance) as total FROM users')
        total_balance = cursor.fetchone()['total'] or 0
        
        # Общий заработок
        cursor.execute('SELECT SUM(total_earned_stars) as total FROM users')
        total_earned = cursor.fetchone()['total'] or 0
        
        # Общие расходы
        cursor.execute('SELECT SUM(total_spent_stars) as total FROM users')
        total_spent = cursor.fetchone()['total'] or 0
        
        # Ожидающие выводы
        cursor.execute('''
            SELECT SUM(amount) as total FROM withdrawal_requests 
            WHERE status = 'pending'
        ''')
        pending_withdrawals = cursor.fetchone()['total'] or 0
        
        # Успешные платежи
        cursor.execute('''
            SELECT COUNT(*) as count, SUM(amount) as total 
            FROM payments WHERE status = 'completed'
        ''')
        payments = cursor.fetchone()
        
        conn.close()
        
        return {
            'total_balance': total_balance,
            'total_earned': total_earned,
            'total_spent': total_spent,
            'pending_withdrawals': pending_withdrawals,
            'successful_payments_count': payments['count'] or 0,
            'successful_payments_amount': payments['total'] or 0
        }
    
    def log_admin_action(self, admin_id: int, target_user_id: int, action_type: str, details: str, ip: str = ""):
        """Логирование действий администратора"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO admin_actions (admin_id, target_user_id, action_type, action_details, ip_address)
            VALUES (?, ?, ?, ?, ?)
        ''', (admin_id, target_user_id, action_type, details, ip))
        conn.commit()
        conn.close()

# Создание экземпляра базы данных
db = Database()

# ========================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ========================================

async def download_media(bot: Bot, file_id: str, file_type: str, user_id: int, 
                        has_timer: bool = False, compress: bool = True) -> Tuple[Optional[str], Optional[str]]:
    """
    Скачивание медиафайла с опциональным сжатием
    Возвращает (путь_к_файлу, путь_к_сжатой_версии)
    """
    try:
        file = await bot.get_file(file_id)
        file_extension = file.file_path.split('.')[-1] if file.file_path else 'bin'
        
        user_media_dir = MEDIA_DIR / str(user_id)
        user_media_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_hash = hashlib.md5(file_id.encode()).hexdigest()[:8]
        timer_prefix = "timer_" if has_timer else ""
        
        # Оригинальный файл
        filename = f"{timer_prefix}{file_type}_{timestamp}_{file_hash}.{file_extension}"
        file_path = user_media_dir / filename
        
        await bot.download_file(file.file_path, file_path)
        logger.info(f"Медиафайл сохранен: {file_path}")
        
        # Сжатие изображений для отчетов
        compressed_path = None
        if compress and file_type == 'photo':
            try:
                from PIL import Image
                compressed_filename = f"{timer_prefix}{file_type}_{timestamp}_{file_hash}_compressed.jpg"
                compressed_path = user_media_dir / compressed_filename
                
                with Image.open(file_path) as img:
                    # Изменение размера для встраивания в HTML
                    img.thumbnail((800, 800))
                    img.save(compressed_path, 'JPEG', quality=85, optimize=True)
                
                logger.info(f"Сжатое изображение создано: {compressed_path}")
            except Exception as e:
                logger.error(f"Ошибка сжатия изображения: {e}")
        
        return str(file_path), str(compressed_path) if compressed_path else None
        
    except Exception as e:
        logger.error(f"Ошибка скачивания медиа: {e}")
        return None, None

def format_subscription_info(user: Dict) -> str:
    """Форматирование информации о подписке"""
    if user['is_blocked']:
        return "🚫 Заблокирован"
    
    # Проверка тестового периода
    if user['trial_started'] and not user.get('subscription_type') or user['subscription_type'] == 'free':
        trial_start = datetime.fromisoformat(user['trial_started'])
        trial_end = trial_start + timedelta(days=FREE_TRIAL_DAYS)
        
        if datetime.now() < trial_end:
            days_left = (trial_end - datetime.now()).days
            return f"🎁 Тест ({days_left}д осталось)"
        else:
            return "🆓 Бесплатный (тест истек)"
    
    sub_type = user['subscription_type']
    
    if sub_type == 'free':
        return "🆓 Бесплатный"
    elif sub_type in SUBSCRIPTION_PRICES:
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
    phone = user['phone_number'] or "нет телефона"
    return f"{index}. {status_emoji} {name} ({username})\n   ID: {user['user_id']} | 📱 {phone}"

def format_stars_balance(user: Dict) -> str:
    """Форматирование баланса Stars"""
    total = user['stars_balance']
    withdrawable = user['withdrawable_stars']
    locked = total - withdrawable
    
    text = f"⭐ Всего: {total} Stars\n"
    if withdrawable > 0:
        text += f"💰 Доступно к выводу: {withdrawable} Stars\n"
    if locked > 0:
        text += f"🔒 Заблокировано: {locked} Stars"
    
    return text

# ========================================
# КЛАВИАТУРЫ
# ========================================

def get_phone_request_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура для запроса номера телефона"""
    builder = ReplyKeyboardBuilder()
    builder.button(text="📱 Отправить номер телефона", request_contact=True)
    return builder.as_markup(resize_keyboard=True, one_time_keyboard=True)

def get_start_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура начального экрана"""
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Принять условия", callback_data="accept_terms")
    builder.button(text="📄 Прочитать условия", callback_data="show_terms")
    builder.adjust(1)
    return builder.as_markup()

def get_main_menu_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """Главное меню пользователя"""
    builder = InlineKeyboardBuilder()
    builder.button(text="📊 Статистика", callback_data="stats")
    builder.button(text="💎 Подписка", callback_data="subscription")
    builder.button(text="⭐ Мой баланс", callback_data="my_balance")
    builder.button(text="👥 Рефералы", callback_data="referrals")
    builder.button(text="🔍 Поиск", callback_data="search_messages")
    builder.button(text="⚙️ Настройки", callback_data="settings")
    if user_id == ADMIN_ID:
        builder.button(text="👨‍💼 Админ", callback_data="admin_panel")
    builder.adjust(2)
    return builder.as_markup()

def get_balance_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура баланса Stars"""
    builder = InlineKeyboardBuilder()
    builder.button(text="💰 Пополнить баланс", callback_data="topup_balance")
    builder.button(text="💸 Вывести Stars", callback_data="withdraw_stars")
    builder.button(text="📜 История транзакций", callback_data="transactions_history")
    builder.button(text="◀️ Назад", callback_data="main_menu")
    builder.adjust(1)
    return builder.as_markup()

def get_referral_keyboard(referral_code: str) -> InlineKeyboardMarkup:
    """Клавиатура реферальной программы"""
    builder = InlineKeyboardBuilder()
    builder.button(text="📊 Моя статистика", callback_data="ref_stats")
    builder.button(text="👥 Мои рефералы", callback_data="ref_list")
    builder.button(text="📋 Скопировать код", callback_data=f"copy_ref_{referral_code}")
    builder.button(text="◀️ Назад", callback_data="main_menu")
    builder.adjust(2, 1, 1)
    return builder.as_markup()

def get_subscription_keyboard(user: Dict) -> InlineKeyboardMarkup:
    """Клавиатура подписок"""
    builder = InlineKeyboardBuilder()
    for key, plan in SUBSCRIPTION_PRICES.items():
        discount_text = f" (-{plan['discount']}%)" if plan['discount'] > 0 else ""
        builder.button(text=f"{plan['name']}: {plan['stars']}⭐{discount_text}", 
                      callback_data=f"show_sub_{key}")
    builder.button(text="◀️ Назад", callback_data="main_menu")
    builder.adjust(1)
    return builder.as_markup()

def get_payment_method_keyboard(plan_key: str, user_id: int) -> InlineKeyboardMarkup:
    """Клавиатура выбора способа оплаты"""
    builder = InlineKeyboardBuilder()
    user = db.get_user(user_id)
    required = SUBSCRIPTION_PRICES[plan_key]['stars']
    
    builder.button(text="⭐ Telegram Stars", callback_data=f"buy_stars_{plan_key}")
    
    if user['stars_balance'] >= required:
        builder.button(text=f"💰 Мой баланс ({user['stars_balance']}⭐)", 
                      callback_data=f"buy_balance_{plan_key}")
    else:
        builder.button(text=f"💰 Недостаточно ({user['stars_balance']}/{required}⭐)", 
                      callback_data="insufficient_balance")
    
    builder.button(text="👤 Связаться с админом", url=f"https://t.me/{ADMIN_USERNAME}")
    builder.button(text="◀️ Назад", callback_data="subscription")
    builder.adjust(1)
    return builder.as_markup()

def get_admin_keyboard() -> InlineKeyboardMarkup:
    """Админ-панель"""
    builder = InlineKeyboardBuilder()
    builder.button(text="👥 Пользователи", callback_data="admin_users")
    builder.button(text="💰 Финансы", callback_data="admin_finances")
    builder.button(text="💸 Выводы Stars", callback_data="admin_withdrawals")
    builder.button(text="📊 Статистика", callback_data="admin_stats")
    builder.button(text="📢 Рассылка", callback_data="admin_broadcast")
    builder.button(text="◀️ Назад", callback_data="main_menu")
    builder.adjust(2)
    return builder.as_markup()

def get_users_list_keyboard(page: int, total_pages: int) -> InlineKeyboardMarkup:
    """Клавиатура списка пользователей"""
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
    builder.row(InlineKeyboardButton(text="🔍 Поиск", callback_data="admin_search_user"))
    builder.row(InlineKeyboardButton(text="◀️ Назад", callback_data="admin_panel"))
    return builder.as_markup()

def get_user_management_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """Клавиатура управления пользователем"""
    builder = InlineKeyboardBuilder()
    builder.button(text="💬 Сообщение", callback_data=f"admin_msg_{user_id}")
    builder.button(text="⭐ Stars", callback_data=f"admin_stars_{user_id}")
    builder.button(text="💎 Подписка", callback_data=f"admin_sub_{user_id}")
    builder.button(text="💬 Чаты", callback_data=f"admin_chats_{user_id}")
    user = db.get_user(user_id)
    if user and user['is_blocked']:
        builder.button(text="✅ Разблокировать", callback_data=f"admin_unblock_{user_id}")
    else:
        builder.button(text="🚫 Заблокировать", callback_data=f"admin_block_{user_id}")
    builder.button(text="◀️ Список", callback_data="admin_users")
    builder.adjust(2)
    return builder.as_markup()

def get_back_keyboard() -> InlineKeyboardMarkup:
    """Простая кнопка назад"""
    builder = InlineKeyboardBuilder()
    builder.button(text="◀️ Назад", callback_data="main_menu")
    return builder.as_markup()

# ========================================
# ОБРАБОТЧИКИ
# ========================================

router = Router()

@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    """Обработчик команды /start"""
    user_id = message.from_user.id
    
    # Извлечение реферального кода из deep link
    args = message.text.split()
    referral_code = args[1] if len(args) > 1 and args[1].startswith('ref_') else None
    if referral_code:
        referral_code = referral_code[4:]  # Убираем префикс ref_
    
    # Добавление пользователя
    is_new = db.add_user(user_id, message.from_user.username, 
                         message.from_user.first_name, message.from_user.last_name)
    
    user = db.get_user(user_id)
    
    # Блокировка
    if user['is_blocked']:
        await message.answer("🚫 Ваш аккаунт заблокирован. Свяжитесь с @" + ADMIN_USERNAME)
        return
    
    # Новый пользователь - нужно принять условия
    if not user['accepted_terms']:
        await state.update_data(referral_code=referral_code)
        await message.answer(
            "👋 <b>Business Message Monitor v7.0.0 MEGA</b>\n\n"
            "🔐 Мониторинг удаленных сообщений, медиа с таймерами, реферальная система.\n\n"
            "<b>НОВОЕ В ВЕРСИИ 7.0:</b>\n"
            "✅ Автоматическое зачисление Stars и Gifts\n"
            "✅ Вывод Stars (минимум 100⭐)\n"
            "✅ Реферальная программа до 12%\n"
            "✅ Отчеты HTML/PDF/TXT при удалении чатов\n"
            "✅ Автоочистка памяти через 24 часа\n"
            "✅ Защита от мультиаккаунтов\n\n"
            "<b>ПЕРЕД ИСПОЛЬЗОВАНИЕМ:</b>\n"
            "Необходимо принять условия использования.",
            reply_markup=get_start_keyboard()
        )
        return
    
    # Пользователь принял условия, но не верифицировал телефон
    if not user['phone_verified']:
        await message.answer(
            "📱 Для продолжения необходимо подтвердить номер телефона.\n\n"
            "Это защищает от создания мультиаккаунтов.",
            reply_markup=get_phone_request_keyboard()
        )
        await state.set_state(RegistrationStates.awaiting_phone)
        return
    
    # Полностью зарегистрированный пользователь
    await show_main_menu(message, user)

async def show_main_menu(message: Message, user: Dict):
    """Отображение главного меню"""
    sub_info = format_subscription_info(user)
    balance_info = format_stars_balance(user)
    
    text = (
        f"🏠 <b>Главное меню</b>\n\n"
        f"{sub_info}\n\n"
        f"{balance_info}\n\n"
        f"👥 Рефералов: {user['total_referrals']}\n"
        f"💰 Заработано: {user['total_referral_earnings']}⭐"
    )
    
    await message.answer(text, reply_markup=get_main_menu_keyboard(message.from_user.id))

@router.callback_query(F.data == "show_terms")
async def show_terms(callback: CallbackQuery):
    """Отображение условий использования"""
    terms = (
        "📄 <b>УСЛОВИЯ ИСПОЛЬЗОВАНИЯ v7.0.0</b>\n\n"
        "<b>⚠️ АДМИНИСТРАЦИЯ НЕ НЕСЕТ ОТВЕТСТВЕННОСТИ:</b>\n"
        "• За любые действия пользователей\n"
        "• За потерю данных по любым причинам\n"
        "• За технические сбои и перерывы\n"
        "• За последствия использования бота\n\n"
        "<b>🎯 ВСЕ ДЕЙСТВИЯ НА СОБСТВЕННЫЙ РИСК</b>\n\n"
        "Бот сохраняет сообщения из бизнес-чатов, отслеживает удаления, "
        "сохраняет медиа с таймерами, генерирует отчеты.\n\n"
        "<b>📱 ВЕРИФИКАЦИЯ:</b>\n"
        "После принятия условий потребуется подтвердить номер телефона "
        "для защиты от мультиаккаунтов.\n\n"
        "<b>🎁 БОНУСЫ:</b>\n"
        f"• {REGISTRATION_BONUS_STARS}⭐ при регистрации (можно вывести)\n"
        f"• {FREE_TRIAL_DAYS} дня бесплатного тестирования\n"
        "• Реферальная программа до 12%\n\n"
        "Нажимая 'Принять', вы соглашаетесь с условиями."
    )
    await callback.message.edit_text(terms, reply_markup=get_start_keyboard())

@router.callback_query(F.data == "accept_terms")
async def accept_terms(callback: CallbackQuery, state: FSMContext):
    """Принятие условий"""
    db.accept_terms(callback.from_user.id)
    
    await callback.message.edit_text(
        "✅ <b>Условия приняты!</b>\n\n"
        "📱 Теперь необходимо подтвердить номер телефона.\n\n"
        "Это защищает систему от создания мультиаккаунтов и "
        "позволяет получить регистрационный бонус.",
        reply_markup=ReplyKeyboardRemove()
    )
    
    await callback.message.answer(
        "Нажмите кнопку ниже для отправки номера телефона:",
        reply_markup=get_phone_request_keyboard()
    )
    
    await state.set_state(RegistrationStates.awaiting_phone)

@router.message(RegistrationStates.awaiting_phone, F.contact)
async def process_phone_verification(message: Message, state: FSMContext):
    """Обработка верификации телефона"""
    user_id = message.from_user.id
    
    # Проверка, что отправлен свой номер
    if message.contact.user_id != user_id:
        await message.answer(
            "❌ Пожалуйста, отправьте свой номер телефона, привязанный к этому аккаунту.",
            reply_markup=get_phone_request_keyboard()
        )
        return
    
    phone_number = message.contact.phone_number
    success, msg = db.verify_phone(user_id, phone_number)
    
    if success:
        # Обработка реферального кода
        data = await state.get_data()
        referral_code = data.get('referral_code')
        
        if referral_code:
            if db.set_referrer(user_id, referral_code):
                await message.answer(
                    f"🎉 Вы зарегистрированы по реферальной ссылке!\n"
                    f"Ваш реферер получил бонус 5⭐"
                )
        
        user = db.get_user(user_id)
        
        await message.answer(
            f"{msg}\n\n"
            f"🎁 <b>Ваш тестовый период: {FREE_TRIAL_DAYS} дня</b>\n\n"
            "Подключите бота через Telegram Business:\n"
            "<code>Настройки → Business → Чат-боты → Добавить</code>",
            reply_markup=ReplyKeyboardRemove()
        )
        
        await show_main_menu(message, user)
        await state.clear()
        
        # Уведомление админу
        try:
            await message.bot.send_message(
                ADMIN_ID,
                f"👤 Новый пользователь:\n"
                f"ID: {user_id}\n"
                f"Имя: {message.from_user.first_name}\n"
                f"Телефон: {phone_number}\n"
                f"Реферал: {'Да' if referral_code else 'Нет'}"
            )
        except:
            pass
    else:
        await message.answer(
            f"❌ {msg}\n\n"
            "Попробуйте другой номер или свяжитесь с @" + ADMIN_USERNAME,
            reply_markup=get_phone_request_keyboard()
        )

@router.callback_query(F.data == "main_menu")
async def main_menu_callback(callback: CallbackQuery):
    """Возврат в главное меню"""
    user = db.get_user(callback.from_user.id)
    
    sub_info = format_subscription_info(user)
    balance_info = format_stars_balance(user)
    
    text = (
        f"🏠 <b>Главное меню</b>\n\n"
        f"{sub_info}\n\n"
        f"{balance_info}\n\n"
        f"👥 Рефералов: {user['total_referrals']}\n"
        f"💰 Заработано: {user['total_referral_earnings']}⭐"
    )
    
    await callback.message.edit_text(text, reply_markup=get_main_menu_keyboard(callback.from_user.id))

@router.callback_query(F.data == "stats")
async def show_stats(callback: CallbackQuery):
    """Статистика пользователя"""
    user = db.get_user(callback.from_user.id)
    connections = db.get_user_connections(callback.from_user.id)
    
    text = (
        f"📊 <b>Статистика</b>\n\n"
        f"{format_subscription_info(user)}\n\n"
        f"📱 Подключений: {len(connections)}\n"
        f"💬 Сообщений сохранено: {user['total_messages_saved']}\n"
        f"🗑 Удалений отслежено: {user['total_deletions_tracked']}\n"
        f"✏️ Изменений отслежено: {user['total_edits_tracked']}\n\n"
        f"📸 Медиа всего: {user['total_media_saved']}\n"
        f"  • Фото: {user['total_photo']}\n"
        f"  • Видео: {user['total_video']}\n"
        f"  • Кружки: {user['total_video_note']}\n"
        f"  • Документы: {user['total_document']}\n"
        f"  • Аудио: {user['total_audio']}\n"
        f"  • Голосовые: {user['total_voice']}\n\n"
        f"⭐ Всего заработано: {user['total_earned_stars']}\n"
        f"💸 Всего потрачено: {user['total_spent_stars']}"
    )
    
    await callback.message.edit_text(text, reply_markup=get_back_keyboard())

@router.callback_query(F.data == "my_balance")
async def show_balance(callback: CallbackQuery):
    """Отображение баланса Stars"""
    user = db.get_user(callback.from_user.id)
    
    text = (
        f"💰 <b>Мой баланс</b>\n\n"
        f"{format_stars_balance(user)}\n\n"
        f"📈 Всего заработано: {user['total_earned_stars']}⭐\n"
        f"📉 Всего потрачено: {user['total_spent_stars']}⭐\n\n"
        f"<b>Вывод Stars:</b>\n"
        f"Минимум: {MIN_WITHDRAWAL_STARS}⭐\n"
        f"Комиссия: {WITHDRAWAL_FEE_PERCENT}%"
    )
    
    await callback.message.edit_text(text, reply_markup=get_balance_keyboard())

@router.callback_query(F.data == "withdraw_stars")
async def withdraw_stars_start(callback: CallbackQuery, state: FSMContext):
    """Начало процесса вывода Stars"""
    user = db.get_user(callback.from_user.id)
    
    if user['withdrawable_stars'] < MIN_WITHDRAWAL_STARS:
        await callback.answer(
            f"Недостаточно средств. Минимум: {MIN_WITHDRAWAL_STARS}⭐, "
            f"доступно: {user['withdrawable_stars']}⭐",
            show_alert=True
        )
        return
    
    await callback.message.edit_text(
        f"💸 <b>Вывод Stars</b>\n\n"
        f"Доступно к выводу: {user['withdrawable_stars']}⭐\n"
        f"Минимум: {MIN_WITHDRAWAL_STARS}⭐\n"
        f"Комиссия: {WITHDRAWAL_FEE_PERCENT}%\n\n"
        f"Введите сумму для вывода:",
        reply_markup=get_back_keyboard()
    )
    
    await state.set_state(UserStates.withdrawal_request)

@router.message(UserStates.withdrawal_request)
async def process_withdrawal_request(message: Message, state: FSMContext):
    """Обработка запроса на вывод"""
    try:
        amount = int(message.text)
        
        if amount < MIN_WITHDRAWAL_STARS:
            await message.answer(f"❌ Минимальная сумма: {MIN_WITHDRAWAL_STARS}⭐")
            return
        
        success, msg = db.create_withdrawal_request(message.from_user.id, amount)
        
        if success:
            await message.answer(
                f"✅ {msg}\n\n"
                f"Запрос отправлен администратору на обработку.\n"
                f"Обычно обрабатывается в течение 24 часов.",
                reply_markup=get_main_menu_keyboard(message.from_user.id)
            )
            
            # Уведомление админу
            try:
                await message.bot.send_message(
                    ADMIN_ID,
                    f"💸 <b>Новый запрос на вывод</b>\n\n"
                    f"Пользователь: {message.from_user.id}\n"
                    f"Сумма: {amount}⭐\n"
                    f"{msg}"
                )
            except:
                pass
        else:
            await message.answer(f"❌ {msg}")
        
        await state.clear()
        
    except ValueError:
        await message.answer("❌ Введите корректное число")

@router.callback_query(F.data == "referrals")
async def show_referrals(callback: CallbackQuery):
    """Реферальная программа"""
    user = db.get_user(callback.from_user.id)
    stats = db.get_referral_stats(callback.from_user.id)
    
    ref_link = f"https://t.me/{(await callback.bot.get_me()).username}?start=ref_{stats['referral_code']}"
    
    text = (
        f"👥 <b>Реферальная программа</b>\n\n"
        f"<b>Ваш реферальный код:</b>\n"
        f"<code>{stats['referral_code']}</code>\n\n"
        f"<b>Ваша реферальная ссылка:</b>\n"
        f"{ref_link}\n\n"
        f"<b>Статистика:</b>\n"
        f"Уровень 1: {stats['level1_count']} чел. → {stats['level1_earned']}⭐ ({REFERRAL_LEVEL1_PERCENT}%)\n"
        f"Уровень 2: {stats['level2_count']} чел. → {stats['level2_earned']}⭐ ({REFERRAL_LEVEL2_PERCENT}%)\n\n"
        f"<b>Всего заработано: {stats['total_earned']}⭐</b>\n\n"
        f"<i>Приглашайте друзей и получайте {REFERRAL_LEVEL1_PERCENT}% от их пополнений!</i>"
    )
    
    await callback.message.edit_text(text, reply_markup=get_referral_keyboard(stats['referral_code']))

@router.callback_query(F.data == "subscription")
async def show_subscription(callback: CallbackQuery):
    """Меню подписок"""
    user = db.get_user(callback.from_user.id)
    
    text = (
        f"💎 <b>Подписка</b>\n\n"
        f"{format_subscription_info(user)}\n\n"
        f"<b>Прайс-лист (версия 7.0):</b>\n"
    )
    
    for key, plan in SUBSCRIPTION_PRICES.items():
        discount = f" <i>(-{plan['discount']}%)</i>" if plan['discount'] > 0 else ""
        text += f"• {plan['name']}: {plan['stars']}⭐ (~{plan['rub']}₽){discount}\n"
    
    text += (
        f"\n<b>Комиссия Telegram Stars:</b> 30%\n"
        f"<b>Чистая прибыль владельца:</b> 50-60%\n\n"
        f"💰 Ваш баланс: {user['stars_balance']}⭐"
    )
    
    await callback.message.edit_text(text, reply_markup=get_subscription_keyboard(user))

@router.callback_query(F.data.startswith("show_sub_"))
async def show_sub_details(callback: CallbackQuery):
    """Детали подписки"""
    plan_key = callback.data.split("_")[-1]
    plan = SUBSCRIPTION_PRICES[plan_key]
    user_id = callback.from_user.id
    user = db.get_user(user_id)
    
    text = (
        f"💎 <b>{plan['name']}</b>\n\n"
        f"Стоимость: {plan['stars']}⭐ (~{plan['rub']}₽)\n"
    )
    
    if plan['discount'] > 0:
        text += f"Скидка: {plan['discount']}%\n"
    
    text += (
        f"\nВаш баланс: {user['stars_balance']}⭐\n\n"
        f"<b>Выберите способ оплаты:</b>"
    )
    
    await callback.message.edit_text(text, reply_markup=get_payment_method_keyboard(plan_key, user_id))

@router.callback_query(F.data.startswith("buy_stars_"))
async def buy_with_telegram_stars(callback: CallbackQuery):
    """Покупка через Telegram Stars"""
    plan_key = callback.data.split("_")[-1]
    plan = SUBSCRIPTION_PRICES[plan_key]
    
    prices = [LabeledPrice(label=plan['name'], amount=plan['stars'])]
    
    try:
        await callback.bot.send_invoice(
            chat_id=callback.from_user.id,
            title=f"Подписка: {plan['name']}",
            description=f"Подписка Business Monitor на {plan['name']}",
            payload=f"sub_{plan_key}_{callback.from_user.id}",
            currency="XTR",
            prices=prices
        )
        await callback.answer("✅ Инвойс отправлен")
    except Exception as e:
        logger.error(f"Ошибка создания инвойса: {e}")
        await callback.answer(f"❌ Ошибка: {e}", show_alert=True)

@router.callback_query(F.data.startswith("buy_balance_"))
async def buy_with_balance(callback: CallbackQuery):
    """Покупка за внутренний баланс"""
    plan_key = callback.data.split("_")[-1]
    plan = SUBSCRIPTION_PRICES[plan_key]
    user_id = callback.from_user.id
    
    if db.spend_stars(user_id, plan['stars'], plan_type=plan_key):
        db.activate_subscription(user_id, plan_key, plan['days'])
        
        await callback.message.edit_text(
            f"✅ <b>Подписка активирована!</b>\n\n"
            f"План: {plan['name']}\n"
            f"Списано: {plan['stars']}⭐\n\n"
            f"Спасибо за покупку! 🎉",
            reply_markup=get_main_menu_keyboard(user_id)
        )
    else:
        await callback.answer("❌ Недостаточно средств", show_alert=True)

@router.pre_checkout_query()
async def pre_checkout(query: PreCheckoutQuery):
    """Предварительная проверка платежа"""
    await query.answer(ok=True)

@router.message(F.successful_payment)
async def successful_payment(message: Message):
    """Обработка успешного платежа Stars"""
    payment = message.successful_payment
    payload_parts = payment.invoice_payload.split("_")
    
    if len(payload_parts) < 2 or payload_parts[0] != 'sub':
        return
    
    plan_key = payload_parts[1]
    user_id = message.from_user.id
    plan = SUBSCRIPTION_PRICES[plan_key]
    
    # Зачисление Stars на баланс (автоматически)
    db.process_incoming_stars(
        user_id, 
        payment.total_amount,
        payment.telegram_payment_charge_id,
        source="subscription_payment"
    )
    
    # Активация подписки
    db.activate_subscription(user_id, plan_key, plan['days'])
    
    # Сохранение платежа
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO payments 
        (user_id, amount, currency, plan_type, payment_method,
         telegram_payment_charge_id, provider_payment_charge_id, 
         invoice_payload, status, confirmed_at)
        VALUES (?, ?, ?, ?, 'telegram_stars', ?, ?, ?, 'completed', CURRENT_TIMESTAMP)
    ''', (user_id, payment.total_amount, payment.currency, plan_key,
          payment.telegram_payment_charge_id, payment.provider_payment_charge_id,
          payment.invoice_payload))
    conn.commit()
    conn.close()
    
    await message.answer(
        f"✅ <b>Платеж успешно обработан!</b>\n\n"
        f"Подписка: {plan['name']}\n"
        f"Оплачено: {payment.total_amount}⭐\n"
        f"Зачислено на баланс: {payment.total_amount}⭐\n\n"
        f"Подписка активирована! 🎉"
    )

# ========================================
# АДМИН-ПАНЕЛЬ
# ========================================

@router.callback_query(F.data == "admin_panel")
async def admin_panel(callback: CallbackQuery):
    """Главное меню админ-панели"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Доступ запрещен", show_alert=True)
        return
    
    total = db.get_user_count()
    active = db.get_active_subscriptions_count()
    messages = db.get_total_messages_saved()
    
    text = (
        f"👨‍💼 <b>Админ-панель v7.0.0</b>\n\n"
        f"👥 Пользователей: {total}\n"
        f"💎 Активных подписок: {active}\n"
        f"💬 Сообщений: {messages}"
    )
    
    await callback.message.edit_text(text, reply_markup=get_admin_keyboard())

@router.callback_query(F.data == "admin_finances")
async def admin_finances(callback: CallbackQuery):
    """Финансовая аналитика"""
    if callback.from_user.id != ADMIN_ID:
        return
    
    stats = db.get_financial_stats()
    
    text = (
        f"💰 <b>Финансовая аналитика</b>\n\n"
        f"<b>Stars:</b>\n"
        f"Общий баланс пользователей: {stats['total_balance']}⭐\n"
        f"Всего заработано: {stats['total_earned']}⭐\n"
        f"Всего потрачено: {stats['total_spent']}⭐\n\n"
        f"<b>Выводы:</b>\n"
        f"Ожидают обработки: {stats['pending_withdrawals']}⭐\n\n"
        f"<b>Платежи:</b>\n"
        f"Успешных: {stats['successful_payments_count']}\n"
        f"Сумма: {stats['successful_payments_amount']}⭐"
    )
    
    builder = InlineKeyboardBuilder()
    builder.button(text="◀️ Назад", callback_data="admin_panel")
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())

@router.callback_query(F.data == "admin_withdrawals")
async def admin_withdrawals(callback: CallbackQuery):
    """Управление выводами"""
    if callback.from_user.id != ADMIN_ID:
        return
    
    pending = db.get_pending_withdrawals()
    
    if not pending:
        text = "✅ Нет ожидающих запросов на вывод"
        builder = InlineKeyboardBuilder()
        builder.button(text="◀️ Назад", callback_data="admin_panel")
        await callback.message.edit_text(text, reply_markup=builder.as_markup())
        return
    
    text = f"💸 <b>Запросы на вывод ({len(pending)})</b>\n\n"
    
    builder = InlineKeyboardBuilder()
    
    for req in pending[:10]:  # Первые 10
        user_info = f"{req['first_name']} (@{req['username'] or 'N/A'})"
        text += (
            f"ID #{req['id']}\n"
            f"👤 {user_info}\n"
            f"💰 {req['amount']}⭐ → {req['net_amount']}⭐ (комиссия {req['fee_amount']}⭐)\n"
            f"📱 {req['phone_number']}\n"
            f"📅 {req['created_at']}\n"
            f"---\n\n"
        )
        
        builder.button(text=f"✅ #{req['id']}", callback_data=f"approve_wd_{req['id']}")
        builder.button(text=f"❌ #{req['id']}", callback_data=f"reject_wd_{req['id']}")
    
    builder.button(text="◀️ Назад", callback_data="admin_panel")
    builder.adjust(2)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())

@router.callback_query(F.data.startswith("approve_wd_"))
async def approve_withdrawal(callback: CallbackQuery):
    """Одобрение вывода"""
    if callback.from_user.id != ADMIN_ID:
        return
    
    request_id = int(callback.data.split("_")[-1])
    
    if db.process_withdrawal(request_id, ADMIN_ID, 'approved', 'Approved by admin'):
        await callback.answer("✅ Вывод одобрен")
        
        # Уведомление пользователю
        req = db.get_connection().execute(
            'SELECT * FROM withdrawal_requests WHERE id = ?', (request_id,)
        ).fetchone()
        
        if req:
            try:
                await callback.bot.send_message(
                    req['user_id'],
                    f"✅ <b>Запрос на вывод одобрен!</b>\n\n"
                    f"Сумма: {req['net_amount']}⭐\n"
                    f"Средства будут отправлены в течение 24 часов."
                )
            except:
                pass
        
        # Обновление списка
        await admin_withdrawals(callback)
    else:
        await callback.answer("❌ Ошибка обработки")

@router.callback_query(F.data.startswith("reject_wd_"))
async def reject_withdrawal(callback: CallbackQuery):
    """Отклонение вывода"""
    if callback.from_user.id != ADMIN_ID:
        return
    
    request_id = int(callback.data.split("_")[-1])
    
    if db.process_withdrawal(request_id, ADMIN_ID, 'rejected', 'Rejected by admin'):
        await callback.answer("❌ Вывод отклонен, средства возвращены")
        
        # Уведомление пользователю
        req = db.get_connection().execute(
            'SELECT * FROM withdrawal_requests WHERE id = ?', (request_id,)
        ).fetchone()
        
        if req:
            try:
                await callback.bot.send_message(
                    req['user_id'],
                    f"❌ <b>Запрос на вывод отклонен</b>\n\n"
                    f"Сумма {req['amount']}⭐ возвращена на ваш баланс.\n"
                    f"Свяжитесь с @{ADMIN_USERNAME} для уточнения."
                )
            except:
                pass
        
        # Обновление списка
        await admin_withdrawals(callback)
    else:
        await callback.answer("❌ Ошибка обработки")

@router.callback_query(F.data == "admin_users")
@router.callback_query(F.data.startswith("users_page_"))
async def admin_users(callback: CallbackQuery):
    """Список пользователей"""
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
    
    text = f"👥 <b>Пользователи ({page+1}/{total_pages})</b>\n\n"
    
    for i, user in enumerate(users, page * USERS_PER_PAGE + 1):
        text += format_user_short(user, i) + "\n"
    
    try:
        await callback.message.edit_text(text, reply_markup=get_users_list_keyboard(page, total_pages))
    except:
        await callback.answer()

@router.callback_query(F.data == "select_user_by_number")
async def select_user(callback: CallbackQuery, state: FSMContext):
    """Выбор пользователя по номеру"""
    if callback.from_user.id != ADMIN_ID:
        return
    
    await callback.message.edit_text("🔢 Отправьте номер пользователя из списка:")
    await state.set_state(AdminStates.user_number_input)

@router.message(AdminStates.user_number_input)
async def process_user_number(message: Message, state: FSMContext):
    """Обработка номера пользователя"""
    if message.from_user.id != ADMIN_ID:
        return
    
    try:
        num = int(message.text)
        users = db.get_all_users(1, num - 1)
        
        if users:
            user = users[0]
            connections = db.get_user_connections(user['user_id'])
            
            text = (
                f"👤 <b>Пользователь #{num}</b>\n\n"
                f"ID: <code>{user['user_id']}</code>\n"
                f"Имя: {user['first_name'] or 'N/A'}\n"
                f"Username: @{user['username'] or 'N/A'}\n"
                f"Телефон: {user['phone_number'] or 'N/A'}\n\n"
                f"{format_subscription_info(user)}\n\n"
                f"{format_stars_balance(user)}\n\n"
                f"📱 Подключений: {len(connections)}\n"
                f"💬 Сообщений: {user['total_messages_saved']}\n"
                f"👥 Рефералов: {user['total_referrals']}\n"
                f"💰 Реферальный доход: {user['total_referral_earnings']}⭐"
            )
            
            await message.answer(text, reply_markup=get_user_management_keyboard(user['user_id']))
        else:
            await message.answer("❌ Пользователь не найден")
        
        await state.clear()
        
    except ValueError:
        await message.answer("❌ Введите корректное число")

@router.callback_query(F.data.startswith("admin_chats_"))
async def admin_view_chats(callback: CallbackQuery):
    """Просмотр чатов пользователя"""
    if callback.from_user.id != ADMIN_ID:
        return
    
    user_id = int(callback.data.split("_")[-1])
    connections = db.get_user_connections(user_id)
    
    if not connections:
        await callback.answer("У пользователя нет активных чатов", show_alert=True)
        return
    
    text = f"💬 <b>Чаты пользователя {user_id}</b>\n\n"
    
    builder = InlineKeyboardBuilder()
    
    for conn in connections[:20]:  # Первые 20
        chat_title = conn['chat_title'] or f"Chat {conn['connection_id'][:8]}"
        text += (
            f"📱 {chat_title}\n"
            f"ID: <code>{conn['connection_id']}</code>\n"
            f"Сообщений: {conn['total_messages']}\n"
            f"Последнее: {conn['last_message_at'] or 'N/A'}\n"
            f"---\n"
        )
        
        builder.button(
            text=f"👁 {chat_title[:20]}",
            callback_data=f"view_chat_{user_id}_{conn['connection_id'][:16]}"
        )
    
    builder.button(text="◀️ Назад", callback_data=f"admin_msg_{user_id}")
    builder.adjust(1)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())

@router.callback_query(F.data.startswith("admin_stars_"))
async def admin_send_stars(callback: CallbackQuery, state: FSMContext):
    """Отправка Stars пользователю"""
    if callback.from_user.id != ADMIN_ID:
        return
    
    user_id = int(callback.data.split("_")[-1])
    await state.update_data(target=user_id)
    
    await callback.message.edit_text("⭐ Введите количество Stars для отправки:")
    await state.set_state(AdminStates.send_stars)

@router.message(AdminStates.send_stars)
async def process_send_stars(message: Message, state: FSMContext):
    """Обработка отправки Stars"""
    if message.from_user.id != ADMIN_ID:
        return
    
    try:
        amount = int(message.text)
        data = await state.get_data()
        target = data['target']
        
        # Начисление withdrawable Stars (можно вывести)
        db.add_withdrawable_stars(target, amount, "Подарок от администратора")
        
        await message.answer(f"✅ Отправлено {amount}⭐ пользователю {target}")
        
        try:
            await message.bot.send_message(
                target,
                f"🎁 <b>Вы получили Stars!</b>\n\n"
                f"Количество: {amount}⭐\n"
                f"От: Администратор\n\n"
                f"Эти Stars можно вывести!"
            )
        except:
            pass
        
        await state.clear()
        
    except ValueError:
        await message.answer("❌ Введите корректное число")

@router.callback_query(F.data.startswith("admin_block_"))
async def admin_block_user(callback: CallbackQuery):
    """Блокировка пользователя"""
    if callback.from_user.id != ADMIN_ID:
        return
    
    user_id = int(callback.data.split("_")[-1])
    db.block_user(user_id, "Blocked by admin")
    
    await callback.answer("✅ Пользователь заблокирован")
    
    try:
        await callback.bot.send_message(
            user_id,
            f"🚫 <b>Ваш аккаунт заблокирован</b>\n\n"
            f"Для уточнения причины свяжитесь с @{ADMIN_USERNAME}"
        )
    except:
        pass
    
    await callback.message.edit_reply_markup(reply_markup=get_user_management_keyboard(user_id))

@router.callback_query(F.data.startswith("admin_unblock_"))
async def admin_unblock_user(callback: CallbackQuery):
    """Разблокировка пользователя"""
    if callback.from_user.id != ADMIN_ID:
        return
    
    user_id = int(callback.data.split("_")[-1])
    db.unblock_user(user_id)
    
    await callback.answer("✅ Пользователь разблокирован")
    
    try:
        await callback.bot.send_message(
            user_id,
            f"✅ <b>Ваш аккаунт разблокирован</b>\n\n"
            f"Вы снова можете пользоваться ботом!"
        )
    except:
        pass
    
    await callback.message.edit_reply_markup(reply_markup=get_user_management_keyboard(user_id))

# ========================================
# ОБРАБОТКА BUSINESS API
# ========================================

@router.business_connection()
async def on_connection(connection: BusinessConnection, bot: Bot):
    """Обработка подключения бизнес-аккаунта"""
    db.add_business_connection(
        connection.id,
        connection.user.id,
        connection.user.id,
        connection.can_reply
    )
    
    try:
        await bot.send_message(
            connection.user.id,
            "🎉 <b>Бот подключен к вашему бизнес-аккаунту!</b>\n\n"
            "Теперь все сообщения из подключенных чатов будут сохраняться."
        )
    except:
        pass

@router.business_message()
async def on_business_message(message: Message, bot: Bot):
    """Обработка бизнес-сообщения"""
    try:
        if not message.business_connection_id:
            return
        
        conn = db.get_business_connection(message.business_connection_id)
        if not conn:
            logger.warning(f"Unknown connection: {message.business_connection_id}")
            return
        
        user_id = conn['user_id']
        
        # Проверка подписки
        if not db.check_subscription(user_id):
            return
        
        # Обновление статистики подключения
        chat_title = message.chat.title if message.chat.type != ChatType.PRIVATE else None
        db.update_connection_stats(message.business_connection_id, chat_title)
        
        # Определение типа медиа и параметров
        media_type = media_file_id = media_file_path = None
        media_compressed_path = media_thumbnail_path = caption = None
        has_timer = is_view_once = False
        timer_seconds = None
        media_width = media_height = media_duration = media_file_size = None
        
        # Обработка медиа
        if message.photo:
            media_type = "photo"
            photo = message.photo[-1]  # Самое большое
            media_file_id = photo.file_id
            media_width = photo.width
            media_height = photo.height
            media_file_size = photo.file_size
            
            # Скачивание
            media_file_path, media_compressed_path = await download_media(
                bot, media_file_id, media_type, user_id, has_timer, compress=True
            )
        
        elif message.video:
            media_type = "video"
            video = message.video
            media_file_id = video.file_id
            media_width = video.width
            media_height = video.height
            media_duration = video.duration
            media_file_size = video.file_size
            
            # Проверка на таймер (одноразовое видео)
            # Telegram не предоставляет прямую информацию о таймерах в Business API
            # Но можно использовать эвристику
            
            media_file_path, _ = await download_media(
                bot, media_file_id, media_type, user_id, has_timer
            )
        
        elif message.video_note:
            media_type = "video_note"
            video_note = message.video_note
            media_file_id = video_note.file_id
            media_duration = video_note.duration
            media_file_size = video_note.file_size
            
            # Кружки часто имеют таймер
            has_timer = True
            timer_seconds = video_note.duration
            
            media_file_path, _ = await download_media(
                bot, media_file_id, media_type, user_id, has_timer
            )
        
        elif message.voice:
            media_type = "voice"
            voice = message.voice
            media_file_id = voice.file_id
            media_duration = voice.duration
            media_file_size = voice.file_size
            
            media_file_path, _ = await download_media(
                bot, media_file_id, media_type, user_id, has_timer
            )
        
        elif message.audio:
            media_type = "audio"
            audio = message.audio
            media_file_id = audio.file_id
            media_duration = audio.duration
            media_file_size = audio.file_size
            
            media_file_path, _ = await download_media(
                bot, media_file_id, media_type, user_id, has_timer
            )
        
        elif message.document:
            media_type = "document"
            document = message.document
            media_file_id = document.file_id
            media_file_size = document.file_size
            
            media_file_path, _ = await download_media(
                bot, media_file_id, media_type, user_id, has_timer
            )
        
        caption = message.caption
        
        # Сохранение в БД
        db.save_message(
            user_id, message.business_connection_id, message.chat.id, message.message_id,
            message.from_user.id, message.from_user.username,
            message.from_user.first_name, message.from_user.last_name,
            message.text or caption, media_type, media_file_id, media_file_path,
            media_thumbnail_path, media_compressed_path, caption,
            has_timer, timer_seconds, is_view_once,
            media_width, media_height, media_duration, media_file_size
        )
        
        logger.info(f"Сообщение сохранено: user={user_id}, chat={message.chat.id}, msg={message.message_id}")
        
    except Exception as e:
        logger.error(f"Ошибка обработки бизнес-сообщения: {e}", exc_info=True)

@router.deleted_business_messages()
async def on_deleted_messages(deleted: BusinessMessagesDeleted, bot: Bot):
    """Обработка удаленных сообщений"""
    try:
        conn = db.get_business_connection(deleted.business_connection_id)
        if not conn:
            return
        
        user_id = conn['user_id']
        deleted_messages = []
        
        for msg_id in deleted.message_ids:
            saved = db.get_message(user_id, deleted.chat.id, msg_id)
            if saved:
                db.mark_message_deleted(user_id, deleted.chat.id, msg_id)
                deleted_messages.append(saved)
        
        if not deleted_messages:
            return
        
        # Генерация отчета
        report_id = db.generate_deletion_report(user_id, deleted.chat.id)
        
        if report_id:
            report = db.get_report(report_id)
            
            # Отправка отчетов пользователю
            try:
                # HTML отчет
                if report['file_path_html'] and Path(report['file_path_html']).exists():
                    html_file = FSInputFile(report['file_path_html'])
                    await bot.send_document(
                        user_id,
                        html_file,
                        caption=(
                            f"📊 <b>Отчет об удалениях</b>\n\n"
                            f"Чат: {deleted.chat.id}\n"
                            f"Удалено сообщений: {report['total_messages']}\n"
                            f"Медиафайлов: {report['total_media']}\n\n"
                            f"⏰ Медиафайлы будут автоматически удалены через {MEDIA_CLEANUP_HOURS}ч"
                        )
                    )
                
                # TXT отчет
                if report['file_path_txt'] and Path(report['file_path_txt']).exists():
                    txt_file = FSInputFile(report['file_path_txt'])
                    await bot.send_document(user_id, txt_file)
                
                # Пометка как отправленного
                db.mark_report_sent(report_id)
                
            except Exception as e:
                logger.error(f"Ошибка отправки отчетов: {e}")
        
        # Краткое уведомление о каждом удаленном сообщении
        for saved in deleted_messages[:5]:  # Первые 5
            text = f"🗑 <b>Сообщение удалено</b>\n\n"
            
            sender = saved['sender_username'] or f"{saved['sender_first_name'] or ''} {saved['sender_last_name'] or ''}".strip()
            text += f"От: @{sender or saved['sender_id']}\n"
            
            if saved['message_text']:
                text += f"\nТекст: <code>{saved['message_text'][:200]}</code>"
            
            if saved['has_timer']:
                text += f"\n⏱ Было с таймером: {saved['timer_seconds']}с"
            
            if saved['is_view_once']:
                text += f"\n👁 Одноразовое"
            
            await bot.send_message(user_id, text)
            
            # Отправка медиа
            if saved['media_file_path'] and Path(saved['media_file_path']).exists():
                try:
                    file = FSInputFile(saved['media_file_path'])
                    
                    caption = f"📎 {saved['media_type'].upper()}"
                    if saved['has_timer']:
                        caption += " [⏱ ТАЙМЕР]"
                    if saved['is_view_once']:
                        caption += " [👁 ОДНОРАЗОВОЕ]"
                    
                    if saved['media_type'] == 'photo':
                        await bot.send_photo(user_id, file, caption=caption)
                    elif saved['media_type'] == 'video':
                        await bot.send_video(user_id, file, caption=caption)
                    elif saved['media_type'] == 'video_note':
                        await bot.send_video_note(user_id, file)
                    elif saved['media_type'] == 'voice':
                        await bot.send_voice(user_id, file, caption=caption)
                    elif saved['media_type'] == 'audio':
                        await bot.send_audio(user_id, file, caption=caption)
                    else:
                        await bot.send_document(user_id, file, caption=caption)
                        
                except Exception as e:
                    logger.error(f"Ошибка отправки медиа: {e}")
        
        if len(deleted_messages) > 5:
            await bot.send_message(
                user_id,
                f"... и еще {len(deleted_messages) - 5} сообщений.\n"
                f"Полный отчет отправлен файлами выше."
            )
        
    except Exception as e:
        logger.error(f"Ошибка обработки удалений: {e}", exc_info=True)

# ========================================
# ФОНОВЫЕ ЗАДАЧИ
# ========================================

async def cleanup_task(bot: Bot):
    """Фоновая задача очистки старых медиа"""
    while True:
        try:
            await asyncio.sleep(3600)  # Каждый час
            logger.info("Запуск задачи очистки медиа...")
            db.cleanup_old_media()
            logger.info("Задача очистки завершена")
        except Exception as e:
            logger.error(f"Ошибка задачи очистки: {e}")

# ========================================
# MAIN
# ========================================

async def main():
    """Главная функция запуска бота"""
    try:
        bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        dp = Dispatcher(storage=MemoryStorage())
        dp.include_router(router)
        
        info = await bot.get_me()
        logger.info(f"🚀 Бот запущен: @{info.username} v7.0.0 MEGA")
        logger.info(f"👨‍💼 Администратор: {ADMIN_ID} (@{ADMIN_USERNAME})")
        
        # Уведомление админу
        try:
            await bot.send_message(
                ADMIN_ID,
                "🚀 <b>Бот запущен v7.0.0 MEGA</b>\n\n"
                "✅ Все системы работают\n"
                "✅ База данных готова\n"
                "✅ Автоочистка медиа активна"
            )
        except:
            pass
        
        # Запуск фоновых задач
        asyncio.create_task(cleanup_task(bot))
        
        # Запуск polling
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
    finally:
        try:
            await bot.session.close()
        except:
            pass

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⏹ Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"Ошибка запуска: {e}", exc_info=True)
