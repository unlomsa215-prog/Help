import logging
import random
import sqlite3
import asyncio
import os
import hashlib
import secrets
import html
import re
from datetime import datetime, timedelta, date
from typing import Optional, Tuple, List, Dict, Any, Set
from contextlib import closing

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    CallbackQueryHandler,
    ConversationHandler,
)
from telegram.constants import ParseMode
from telegram.error import Forbidden, TelegramError

# Настройка логирования
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Токен бота
TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise ValueError("Токен не задан! Укажите BOT_TOKEN в переменных окружения.")

# Пароль для админ-панели
ADMIN_PANEL_PASSWORD = "ownerok14882288admingosuslugioffixialpocox3noviymagamed"

# ID суперадмина
SUPER_ADMIN_ID = 7993533453

# Особые пользователи
DEVELOPER_ID = 7993533453
OFFICIAL_IDS = {8156441061, 8300020320}
TESTER_ID = 5976418163

# База данных
DB_NAME = "game_bot.db"
DB_TIMEOUT = 10

# Параметры игр
PYRAMID_FLOORS = 10
PYRAMID_CELLS_PER_FLOOR = 4
PYRAMID_MULTIPLIERS = [1.2, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0, 7.0, 10.0, 15.0]

TOWER_FLOORS = 10
TOWER_CELLS_PER_FLOOR = 5

MINES_FIELD_SIZE = 5
MINES_TOTAL_CELLS = 25
MINES_COUNT = 3
MINES_WIN_MULTIPLIER = 1.6

# Множители для рулетки
ROULETTE_PAYOUTS = {
    "число": 36,
    "цвет": 2,
    "чет": 2,
    "нечет": 2,
    "1-12": 3,
    "13-24": 3,
    "25-36": 3,
    "1-18": 2,
    "19-36": 2,
    "колонка1": 3,
    "колонка2": 3,
    "колонка3": 3,
}

# Множители для дартса
DARTS_MULTIPLIER_RED = 2.0
DARTS_MULTIPLIER_WHITE = 2.1

# Множители для игры Золото
GOLD_MULTIPLIERS = [2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096]

# Команды без суммы
NON_GAME_COMMANDS = {
    "работа", "бонус", "ежедневныйбонус", "профиль", "б", "баланс", "дать",
    "помощь", "промо", "такси", "автосалон", "моя", "моимашины", "моямашина", "топ",
    "реф", "реферальнаясистема", "го", "ставки", "отмена", "история",
    "конкурс", "сотрудничество", "крестики", "задания"
}

# Игровые команды (с суммой)
GAME_COMMANDS = {
    "футбол", "баскетбол", "бс", "рулетка", "кубик", "21", "слоты", "башня",
    "фишки", "пирамида", "кубы", "мины", "дартс", "бдж", "сундуки", "золото"
}

ALL_COMMANDS = NON_GAME_COMMANDS | GAME_COMMANDS

# Глобальные переменные для управления ботом
BOT_ENABLED = True
ADMIN_IDS: Set[int] = set()  # будет заполнено при старте

# Параметры мультиплеерного блэкджека
BJ_MAX_PLAYERS = 6
BJ_MIN_PLAYERS = 3
BJ_LOBBY_TIMEOUT = 60  # секунд на сбор игроков

# ---------- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ----------
def escape_html(text: str) -> str:
    """Экранирует специальные символы для HTML."""
    return html.escape(text)

def parse_amount(amount_str: str, balance: int = None) -> int:
    """
    Преобразует строку вида '100', '100к', '5кк' или 'все' в целое число.
    Если balance передан и amount_str == 'все', возвращает balance.
    Для админских команд 'все' не допускается (balance не передаётся), поэтому будет ошибка.
    """
    amount_str = amount_str.lower().strip()
    if amount_str in ("все", "вб"):
        if balance is None:
            raise ValueError("Нельзя использовать 'все' без баланса")
        return balance
    if amount_str.endswith('кк'):
        try:
            val = float(amount_str[:-2])
            return int(val * 1_000_000)
        except:
            raise ValueError("Неверный формат суммы")
    elif amount_str.endswith('к'):
        try:
            val = float(amount_str[:-1])
            return int(val * 1_000)
        except:
            raise ValueError("Неверный формат суммы")
    else:
        try:
            return int(amount_str)
        except:
            raise ValueError("Неверный формат суммы")

def extract_target_id(text: str) -> Optional[int]:
    """Извлекает user_id из строки (число или @username) с учётом регистра."""
    text = text.strip()
    if text.isdigit():
        return int(text)
    if text.startswith('@'):
        username = text[1:].lower()
        with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
            c = conn.cursor()
            c.execute("SELECT user_id FROM users WHERE LOWER(username) = ?", (username,))
            row = c.fetchone()
            if row:
                return row[0]
    return None

def is_bot_enabled(context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Проверяет, включён ли бот."""
    return context.bot_data.get('bot_enabled', True)

def check_admin_limit(admin_id: int, amount: int) -> bool:
    """Проверяет, может ли админ выдать сумму amount (0 - безлимит)."""
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute("SELECT max_give_amount FROM admin_limits WHERE admin_id = ?", (admin_id,))
        row = c.fetchone()
        limit = row[0] if row else 0
    return limit == 0 or amount <= limit

# ---------- БАЗА ДАННЫХ ----------
def init_db():
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        # Таблица пользователей
        c.execute('''CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            balance INTEGER DEFAULT 0,
            total_won INTEGER DEFAULT 0,
            total_lost INTEGER DEFAULT 0,
            total_games INTEGER DEFAULT 0,
            last_hourly_bonus TIMESTAMP,
            last_daily_bonus TIMESTAMP,
            last_work TIMESTAMP,
            last_taxi TIMESTAMP,
            is_banned INTEGER DEFAULT 0,
            is_admin INTEGER DEFAULT 0,
            referrer_id INTEGER,
            active_car_id INTEGER DEFAULT 0,
            has_limited_car INTEGER DEFAULT 0,
            last_lux_bonus TIMESTAMP,
            registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status TEXT DEFAULT 'Player',
            description TEXT DEFAULT 'игрок kredigs bot!',
            account_number INTEGER,
            protection TEXT DEFAULT 'standart',
            FOREIGN KEY(referrer_id) REFERENCES users(user_id)
        )''')
        # Добавляем новые столбцы, если их нет (миграция)
        c.execute("PRAGMA table_info(users)")
        existing_columns = [col[1] for col in c.fetchall()]
        new_columns = [
            ('status', 'TEXT DEFAULT "Player"'),
            ('description', 'TEXT DEFAULT "игрок kredigs bot!"'),
            ('account_number', 'INTEGER'),
            ('protection', 'TEXT DEFAULT "standart"')
        ]
        for col_name, col_def in new_columns:
            if col_name not in existing_columns:
                c.execute(f"ALTER TABLE users ADD COLUMN {col_name} {col_def}")

        # Таблица рефералов
        c.execute('''CREATE TABLE IF NOT EXISTS referrals (
            referrer_id INTEGER,
            referral_id INTEGER,
            bonus_claimed INTEGER DEFAULT 0,
            PRIMARY KEY (referrer_id, referral_id)
        )''')
        # Таблица промокодов
        c.execute('''CREATE TABLE IF NOT EXISTS promocodes (
            code TEXT PRIMARY KEY,
            amount INTEGER,
            max_uses INTEGER,
            used INTEGER DEFAULT 0,
            message TEXT
        )''')
        # Таблица активаций промокодов
        c.execute('''CREATE TABLE IF NOT EXISTS promo_activations (
            user_id INTEGER,
            code TEXT,
            activated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, code)
        )''')
        # Таблица машин (каталог)
        c.execute('''CREATE TABLE IF NOT EXISTS cars (
            car_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            price INTEGER,
            rent_price INTEGER,
            taxi_min_earn INTEGER,
            taxi_max_earn INTEGER,
            is_available INTEGER DEFAULT 1,
            is_limited INTEGER DEFAULT 0,
            stock INTEGER DEFAULT 0,
            tip_min INTEGER DEFAULT 0,
            tip_max INTEGER DEFAULT 0
        )''')
        # Таблица машин пользователей
        c.execute('''CREATE TABLE IF NOT EXISTS user_cars (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            car_id INTEGER,
            purchase_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_rented INTEGER DEFAULT 0,
            expires TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users(user_id),
            FOREIGN KEY(car_id) REFERENCES cars(car_id)
        )''')
        # Таблица логов администраторов
        c.execute('''CREATE TABLE IF NOT EXISTS admin_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_id INTEGER,
            action TEXT,
            target_id INTEGER,
            amount INTEGER,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        # Таблица лимитов для админов
        c.execute('''CREATE TABLE IF NOT EXISTS admin_limits (
            admin_id INTEGER PRIMARY KEY,
            max_give_amount INTEGER DEFAULT 0
        )''')
        # Таблица заданий (эвент)
        c.execute('''CREATE TABLE IF NOT EXISTS quests (
            quest_id INTEGER PRIMARY KEY,
            description TEXT,
            target INTEGER,
            reward INTEGER
        )''')
        # Заполняем задания, если их нет
        c.execute("SELECT COUNT(*) FROM quests")
        if c.fetchone()[0] == 0:
            quests_data = [
                (1, 'Сыграть в любые игры 5 раз', 5, 5000),
                (2, 'Выиграть в играх 3 раза', 3, 10000),
                (3, 'Заработать в такси 20000 кредиксов', 20000, 15000),
                (4, 'Пригласить 1 реферала', 1, 20000),
                (5, 'Сыграть в рулетку 3 раза', 3, 8000),
                (6, 'Выиграть в башне 2 раза', 2, 12000),
                (7, 'Потратить 50000 кредиксов в автосалоне', 50000, 20000),
                (8, 'Привести 2 рефералов', 2, 25000),
                (9, 'Заработать в такси 50000 кредиксов', 50000, 18000),
            ]
            c.executemany('INSERT INTO quests (quest_id, description, target, reward) VALUES (?,?,?,?)', quests_data)

        c.execute('''CREATE TABLE IF NOT EXISTS user_quests (
            user_id INTEGER,
            quest_id INTEGER,
            progress INTEGER DEFAULT 0,
            completed INTEGER DEFAULT 0,
            reward_claimed INTEGER DEFAULT 0,
            last_reset DATE DEFAULT CURRENT_DATE,
            PRIMARY KEY (user_id, quest_id)
        )''')

        # Добавим обычные машины, если их нет
        c.execute("SELECT COUNT(*) FROM cars WHERE is_limited=0")
        if c.fetchone()[0] == 0:
            cars_data = [
                ("Жигули", 1_000_000, 10_000, 10_000, 30_000),
                ("Лада", 2_000_000, 20_000, 20_000, 50_000),
                ("Хендай", 4_000_000, 50_000, 30_000, 80_000),
                ("Тойота", 8_000_000, 100_000, 40_000, 120_000),
                ("Мерседес", 12_000_000, 200_000, 50_000, 150_000),
                ("Бентли", 15_250_000, 300_000, 60_000, 200_000),
            ]
            for car in cars_data:
                c.execute('''INSERT INTO cars (name, price, rent_price, taxi_min_earn, taxi_max_earn, is_limited, stock, tip_min, tip_max)
                            VALUES (?,?,?,?,?,0,0,0,0)''', car)
        # Добавим лимитированные машины, если их нет
        limited_cars = [
            ("Lamborghini Aventador off-road", 65_000_000, 0, 1_000_000, 3_000_000, 1, 5, 150_000, 300_000),
            ("Mercedes AMG Gold", 150_000_000, 0, 1_500_000, 2_500_000, 1, 5, 200_000, 400_000),
            ("BMW M5 F90 KS Gold", 250_000_000, 0, 2_000_000, 4_000_000, 1, 5, 250_000, 500_000)
        ]
        for car in limited_cars:
            c.execute("SELECT car_id FROM cars WHERE name = ?", (car[0],))
            if not c.fetchone():
                c.execute('''INSERT INTO cars (name, price, rent_price, taxi_min_earn, taxi_max_earn, is_limited, stock, tip_min, tip_max)
                            VALUES (?,?,?,?,?,?,?,?,?)''', car)

        # Миграция старых данных
        c.execute("PRAGMA table_info(users)")
        columns = [col[1] for col in c.fetchall()]
        if 'car_id' in columns:
            c.execute("SELECT user_id, car_id, has_limited_car FROM users WHERE car_id IS NOT NULL AND car_id != 0")
            rows = c.fetchall()
            for user_id, car_id, has_limited in rows:
                c.execute("SELECT 1 FROM user_cars WHERE user_id = ? AND car_id = ?", (user_id, car_id))
                if not c.fetchone():
                    c.execute('''INSERT INTO user_cars (user_id, car_id, is_rented, expires)
                                 VALUES (?,?,0,NULL)''', (user_id, car_id))
                c.execute("SELECT active_car_id FROM users WHERE user_id = ?", (user_id,))
                active = c.fetchone()[0]
                if not active or active == 0:
                    c.execute("UPDATE users SET active_car_id = ? WHERE user_id = ?", (car_id, user_id))
        conn.commit()

def generate_unique_account_number():
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        while True:
            num = random.randint(1, 100000)
            c.execute("SELECT 1 FROM users WHERE account_number = ?", (num,))
            if not c.fetchone():
                return num

# ---------- ФУНКЦИИ ДЛЯ РАБОТЫ С ПОЛЬЗОВАТЕЛЯМИ ----------
def get_user(user_id: int) -> Optional[Tuple]:
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        return c.fetchone()

def create_user(user_id: int, username: str = "", first_name: str = "", referrer_id: int = None):
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        account_number = generate_unique_account_number()
        c.execute('''INSERT OR IGNORE INTO users 
                    (user_id, username, first_name, balance, last_hourly_bonus, last_daily_bonus, last_work, last_taxi, registered_at, account_number)
                    VALUES (?,?,?,1500,?,?,?,?, CURRENT_TIMESTAMP, ?)''',
                  (user_id, username, first_name, None, None, None, None, account_number))
        if referrer_id and referrer_id != user_id:
            c.execute("SELECT user_id FROM users WHERE user_id = ?", (referrer_id,))
            if c.fetchone():
                # Только реферер получает бонус, новому пользователю не начисляем дополнительно
                c.execute('''UPDATE users SET referrer_id = ? WHERE user_id = ?''', (referrer_id, user_id))
                c.execute('''INSERT INTO referrals (referrer_id, referral_id) VALUES (?,?)''',
                          (referrer_id, user_id))
                c.execute('''UPDATE users SET balance = balance + 10000 WHERE user_id = ?''',
                          (referrer_id,))
                update_quest_progress(referrer_id, 4, 1)  # старый квест
                update_quest_progress(referrer_id, 8, 1)  # новый квест на рефералов
        if user_id == SUPER_ADMIN_ID:
            c.execute("UPDATE users SET is_admin = 1 WHERE user_id = ?", (user_id,))
        conn.commit()

def ensure_user_exists(user_id: int, username: str = "", first_name: str = "", referrer_id: int = None):
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
        if not c.fetchone():
            account_number = generate_unique_account_number()
            c.execute('''INSERT INTO users 
                        (user_id, username, first_name, balance, last_hourly_bonus, last_daily_bonus, last_work, last_taxi, registered_at, account_number)
                        VALUES (?,?,?,1500,?,?,?,?, CURRENT_TIMESTAMP, ?)''',
                      (user_id, username, first_name, None, None, None, None, account_number))
            if referrer_id and referrer_id != user_id:
                c.execute("SELECT user_id FROM users WHERE user_id = ?", (referrer_id,))
                if c.fetchone():
                    c.execute('''UPDATE users SET referrer_id = ? WHERE user_id = ?''', (referrer_id, user_id))
                    c.execute('''INSERT INTO referrals (referrer_id, referral_id) VALUES (?,?)''',
                              (referrer_id, user_id))
                    c.execute('''UPDATE users SET balance = balance + 10000 WHERE user_id = ?''',
                              (referrer_id,))
                    update_quest_progress(referrer_id, 4, 1)
                    update_quest_progress(referrer_id, 8, 1)
            if user_id == SUPER_ADMIN_ID:
                c.execute("UPDATE users SET is_admin = 1 WHERE user_id = ?", (user_id,))
            conn.commit()

def update_balance(user_id: int, delta: int):
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (delta, user_id))
        conn.commit()

def get_balance(user_id: int) -> int:
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
        res = c.fetchone()
        return res[0] if res else 0

def get_user_description(user_id: int) -> str:
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute("SELECT description FROM users WHERE user_id = ?", (user_id,))
        res = c.fetchone()
        return res[0] if res else ""

def add_game_stat(user_id: int, won: int, lost: int):
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute('''UPDATE users SET total_won = total_won + ?, total_lost = total_lost + ?, 
                    total_games = total_games + 1 WHERE user_id = ?''', (won, lost, user_id))
        conn.commit()
    if won > 0:
        update_quest_progress(user_id, 2, 1)
    update_quest_progress(user_id, 1, 1)

def is_banned(user_id: int) -> bool:
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute("SELECT is_banned FROM users WHERE user_id = ?", (user_id,))
        res = c.fetchone()
        return res and res[0] == 1

def is_admin(user_id: int) -> bool:
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute("SELECT is_admin FROM users WHERE user_id = ?", (user_id,))
        res = c.fetchone()
        return res and res[0] == 1

def log_admin_action(admin_id: int, action: str, target_id: int = None, amount: int = None):
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute('''INSERT INTO admin_logs (admin_id, action, target_id, amount)
                     VALUES (?,?,?,?)''', (admin_id, action, target_id, amount))
        conn.commit()

def get_bot_stats():
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM users")
        total_users = c.fetchone()[0]
        c.execute("SELECT SUM(balance) FROM users")
        total_balance = c.fetchone()[0] or 0
        c.execute("SELECT SUM(total_games) FROM users")
        total_games = c.fetchone()[0] or 0
        c.execute("SELECT SUM(total_won) FROM users")
        total_won = c.fetchone()[0] or 0
        c.execute("SELECT SUM(total_lost) FROM users")
        total_lost = c.fetchone()[0] or 0
        c.execute("SELECT COUNT(*) FROM users WHERE is_banned=1")
        banned_users = c.fetchone()[0]
        return {
            "total_users": total_users,
            "total_balance": total_balance,
            "total_games": total_games,
            "total_won": total_won,
            "total_lost": total_lost,
            "banned_users": banned_users
        }

def generate_fair_hash() -> Tuple[str, str]:
    seed = secrets.token_hex(16)
    h = hashlib.sha256(seed.encode()).hexdigest()
    return seed, h

# ---------- КВЕСТЫ ----------
def reset_quests_if_needed(user_id: int):
    today = date.today().isoformat()
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute("SELECT quest_id FROM quests")
        quest_ids = [row[0] for row in c.fetchall()]
        for qid in quest_ids:
            c.execute('''SELECT last_reset FROM user_quests WHERE user_id = ? AND quest_id = ?''', (user_id, qid))
            row = c.fetchone()
            if not row:
                c.execute('''INSERT INTO user_quests (user_id, quest_id, last_reset) VALUES (?,?,?)''',
                          (user_id, qid, today))
            else:
                last_reset = row[0]
                if last_reset < today:
                    c.execute('''UPDATE user_quests SET progress = 0, completed = 0, reward_claimed = 0, last_reset = ?
                                 WHERE user_id = ? AND quest_id = ?''', (today, user_id, qid))
        conn.commit()

def update_quest_progress(user_id: int, quest_id: int, amount: int = 1):
    reset_quests_if_needed(user_id)
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute('''SELECT progress, completed, reward_claimed FROM user_quests WHERE user_id = ? AND quest_id = ?''',
                  (user_id, quest_id))
        row = c.fetchone()
        if not row:
            return
        progress, completed, claimed = row
        if completed or claimed:
            return
        c.execute("SELECT target FROM quests WHERE quest_id = ?", (quest_id,))
        target = c.fetchone()[0]
        new_progress = progress + amount
        completed_now = 1 if new_progress >= target else 0
        c.execute('''UPDATE user_quests SET progress = ?, completed = ? WHERE user_id = ? AND quest_id = ?''',
                  (new_progress, completed_now, user_id, quest_id))
        conn.commit()

async def quests_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ensure_user_exists(user_id)
    reset_quests_if_needed(user_id)

    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute('''SELECT q.quest_id, q.description, q.target, q.reward,
                            uq.progress, uq.completed, uq.reward_claimed
                     FROM quests q
                     LEFT JOIN user_quests uq ON q.quest_id = uq.quest_id AND uq.user_id = ?
                     ORDER BY q.quest_id''', (user_id,))
        quests = c.fetchall()

    text = "<b>🎯 Ежедневные задания</b>\n\n"
    keyboard = []
    for q in quests:
        qid, desc, target, reward, progress, completed, claimed = q
        progress = progress or 0
        completed = completed or 0
        claimed = claimed or 0
        status = "✅ Выполнено" if completed else f"Прогресс: {progress}/{target}"
        if claimed:
            status = "🎁 Награда получена"
        text += f"<b>{escape_html(desc)}</b>\n{status} | Награда: {reward:,}\n\n"
        if completed and not claimed:
            keyboard.append([InlineKeyboardButton(f"Забрать награду за {desc}", callback_data=f"claim_quest_{qid}")])

    reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)

async def claim_quest_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data
    if not data.startswith("claim_quest_"):
        return
    quest_id = int(data.split("_")[2])

    reset_quests_if_needed(user_id)
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute('''SELECT completed, reward_claimed, reward FROM user_quests uq
                     JOIN quests q ON uq.quest_id = q.quest_id
                     WHERE uq.user_id = ? AND uq.quest_id = ?''', (user_id, quest_id))
        row = c.fetchone()
        if not row or not row[0] or row[1]:
            await query.edit_message_text("❌ Это задание уже выполнено или награда получена.")
            return
        completed, claimed, reward = row
        update_balance(user_id, reward)
        c.execute('''UPDATE user_quests SET reward_claimed = 1 WHERE user_id = ? AND quest_id = ?''',
                  (user_id, quest_id))
        conn.commit()
    await query.edit_message_text(f"✅ Вы получили награду {reward:,} кредиксов за выполнение задания!")

# ---------- ПОЛНЫЙ ПРОФИЛЬ ----------
async def profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if is_banned(user_id):
        await update.message.reply_text("❌ Вы забанены.")
        return
    user = update.effective_user
    ensure_user_exists(user_id, user.username or "", user.first_name or "")

    # Проверяем LUX-бонус для владельцев лимитированных машин
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute("SELECT has_limited_car, last_lux_bonus FROM users WHERE user_id = ?", (user_id,))
        row = c.fetchone()
        if row:
            has_limited = row[0]
            last_bonus = row[1]
            if has_limited:
                now = datetime.now()
                if last_bonus:
                    try:
                        last = datetime.fromisoformat(last_bonus)
                    except:
                        last = None
                    if last and (now - last).total_seconds() >= 10 * 3600:
                        update_balance(user_id, 250000)
                        c.execute("UPDATE users SET last_lux_bonus = ? WHERE user_id = ?", (now.isoformat(), user_id))
                        conn.commit()
                else:
                    c.execute("UPDATE users SET last_lux_bonus = ? WHERE user_id = ?", (now.isoformat(), user_id))
                    conn.commit()

    # Получаем все данные для профиля
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute('''SELECT username, first_name, balance, total_won, total_lost, total_games, 
                    registered_at, referrer_id, active_car_id, has_limited_car, status, description, account_number, protection
                    FROM users WHERE user_id = ?''', (user_id,))
        row = c.fetchone()
        if not row:
            await update.message.reply_text("Сначала введите /start")
            return
        (username, first_name, balance, won, lost, games, reg_date, referrer,
         active_car_id, has_limited, status, description, account_number, protection) = row

        # Название активной машины
        car_name = "Нет"
        if active_car_id and active_car_id != 0:
            c.execute("SELECT name FROM cars WHERE car_id = ?", (active_car_id,))
            car_row = c.fetchone()
            if car_row:
                car_name = car_row[0]

    try:
        reg_date_str = datetime.fromisoformat(reg_date).strftime("%d.%m.%Y %H:%M") if reg_date else "неизвестно"
    except:
        reg_date_str = "неизвестно"
    safe_username = escape_html(username) if username else "не указан"
    safe_first_name = escape_html(first_name)
    safe_status = escape_html(status)
    safe_description = escape_html(description)

    profile_text = (
        f"⚡<b>привет {safe_username} это твой игровой профиль</b>⚡\n"
        f"~~~~~~~~~~~~~~~~~~~~~~\n"
        f"🆔<b>Твое айди:</b> <code>{user_id}</code>\n"
        f"👾<b>сыграно игр:</b> {games}\n"
        f"💟<b>Проиграно:</b> {lost:,}\n"
        f"💌<b>Выиграно:</b> {won:,}\n"
        f"❄️<b>Дата регистрации в боте:</b> {reg_date_str}\n"
        f"🏎️<b>Ваш автомобиль:</b> {car_name}\n"
        f"💰<b>Ваш баланс:</b> {balance:,}\n"
        f"👑<b>Статус:</b> {safe_status}\n\n"
        f"💢<b>Описание:</b> {safe_description}\n"
        f"🕸️<b>Номер аккаунта:</b> {account_number}\n"
        f"🏆<b>защита:</b> {protection}\n"
    )
    await update.message.reply_text(profile_text, parse_mode=ParseMode.HTML)

# ---------- УПРОЩЁННЫЙ БАЛАНС С КНОПКАМИ ----------
async def show_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает краткую информацию о балансе и три кнопки: Бонус, Ежедневный бонус, Конкурс."""
    user_id = update.effective_user.id
    if is_banned(user_id):
        await update.message.reply_text("❌ Вы забанены.")
        return
    user = update.effective_user
    ensure_user_exists(user_id, user.username or "", user.first_name or "")

    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute('''SELECT first_name, balance, total_won, total_lost, total_games
                     FROM users WHERE user_id = ?''', (user_id,))
        row = c.fetchone()
        if not row:
            await update.message.reply_text("Сначала введите /start")
            return
        first_name, balance, won, lost, games = row

    safe_name = escape_html(first_name)

    text = (
        f"<b>Привет, {safe_name}!</b>\n"
        f"💌 <b>Твой баланс:</b> {balance:,} кредиксов\n"
        f"----------------------------------------------\n"
        f"👾 <b>Выиграно:</b> {won:,}\n"
        f"😕 <b>Проиграно:</b> {lost:,}\n"
        f"👌 <b>Сыграно игр:</b> {games}\n"
    )

    keyboard = [
        [
            InlineKeyboardButton("🎁 Бонус", callback_data="bonus_btn"),
            InlineKeyboardButton("📅 Ежедневный бонус", callback_data="daily_bonus_btn"),
        ],
        [InlineKeyboardButton("🎉 Конкурс", callback_data="contest_btn")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)

# Обработчики кнопок
async def bonus_button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await hourly_bonus(update, context)  # hourly_bonus поддерживает callback

async def daily_bonus_button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await daily_bonus(update, context)  # daily_bonus поддерживает callback

async def contest_button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    text = "<b>🎉 Конкурс на автомобиль Bentley уже идёт!</b>\n\nПрисоединяйтесь к конкурсу в телеграмм канале: https://t.me/werdoxz_wiinere"
    await context.bot.send_message(chat_id=query.from_user.id, text=text, parse_mode=ParseMode.HTML)

# ---------- АДМИН-ПАНЕЛЬ ----------
# Состояния для ConversationHandler админ-панели
ADMIN_AWAIT_PASSWORD, ADMIN_MENU, ADMIN_AWAIT_TARGET, ADMIN_AWAIT_AMOUNT, ADMIN_AWAIT_MESSAGE, ADMIN_AWAIT_PROMO, ADMIN_AWAIT_DESC, ADMIN_AWAIT_STATUS, ADMIN_AWAIT_PROTECTION = range(9)

async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id) and user_id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ У вас нет прав доступа.")
        return ConversationHandler.END

    # Если пароль передан как аргумент команды
    if context.args:
        if context.args[0] == ADMIN_PANEL_PASSWORD:
            context.user_data['admin_authenticated'] = True
            await show_admin_keyboard(update, context)
            return ConversationHandler.END
        else:
            await update.message.reply_text("❌ Неверный пароль.")
            return ConversationHandler.END
    else:
        await update.message.reply_text("🔐 Введите пароль для входа в админ-панель:")
        return ADMIN_AWAIT_PASSWORD

async def admin_check_password(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id) and user_id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ Доступ запрещён.")
        return ConversationHandler.END
    password = update.message.text.strip()
    if password != ADMIN_PANEL_PASSWORD:
        await update.message.reply_text("❌ Неверный пароль.")
        return ConversationHandler.END
    context.user_data['admin_authenticated'] = True
    await show_admin_keyboard(update, context)
    return ADMIN_MENU

async def show_admin_keyboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats"),
         InlineKeyboardButton("💰 Выдать", callback_data="admin_give")],
        [InlineKeyboardButton("💸 Снять", callback_data="admin_take"),
         InlineKeyboardButton("🔨 Бан", callback_data="admin_ban")],
        [InlineKeyboardButton("✅ Разбан", callback_data="admin_unban"),
         InlineKeyboardButton("📢 Рассылка", callback_data="admin_notify")],
        [InlineKeyboardButton("🎁 Промокод", callback_data="admin_create_promo"),
         InlineKeyboardButton("📝 Изменить описание", callback_data="admin_setdesc")],
        [InlineKeyboardButton("👑 Изменить статус", callback_data="admin_setstatus"),
         InlineKeyboardButton("🛡️ Изменить защиту", callback_data="admin_setprotection")],
        [InlineKeyboardButton("⚙️ Управление", callback_data="admin_manage"),
         InlineKeyboardButton("❌ Выход", callback_data="admin_exit")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    if update.message:
        await update.message.reply_text("🛠 Админ-панель:", reply_markup=reply_markup)
    else:
        await update.callback_query.message.reply_text("🛠 Админ-панель:", reply_markup=reply_markup)
        try:
            await update.callback_query.message.delete()
        except:
            pass

async def admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    if not context.user_data.get('admin_authenticated') and user_id != SUPER_ADMIN_ID:
        await query.edit_message_text("❌ Сессия истекла. Введите /Admin заново.")
        return ConversationHandler.END

    data = query.data
    context.user_data['admin_last_action'] = data

    if data == "admin_stats":
        stats = get_bot_stats()
        text = (f"<b>📊 Статистика бота</b>\n"
                f"Пользователей: {stats['total_users']}\n"
                f"Общий баланс: {stats['total_balance']:,}\n"
                f"Всего игр: {stats['total_games']}\n"
                f"Выиграно всего: {stats['total_won']:,}\n"
                f"Проиграно всего: {stats['total_lost']:,}\n"
                f"Забанено: {stats['banned_users']}")
        await query.edit_message_text(text, parse_mode=ParseMode.HTML)
        await show_admin_keyboard(update, context)
        return ADMIN_MENU

    elif data in ("admin_give", "admin_take", "admin_setdesc", "admin_setstatus", "admin_setprotection"):
        await query.edit_message_text("Введите ID или @username пользователя:")
        return ADMIN_AWAIT_TARGET

    elif data in ("admin_ban", "admin_unban"):
        context.user_data['admin_action'] = data  # запомним действие без суммы
        await query.edit_message_text("Введите ID или @username пользователя:")
        return ADMIN_AWAIT_TARGET

    elif data == "admin_notify":
        await query.edit_message_text("Введите текст для рассылки всем пользователям:")
        return ADMIN_AWAIT_MESSAGE

    elif data == "admin_create_promo":
        await query.edit_message_text("Введите параметры промокода в формате:\nсумма кол-во [код] текст\nНапример: 10000 5 WIN500 Поздравляем!")
        return ADMIN_AWAIT_PROMO

    elif data == "admin_manage":
        await query.edit_message_text("Функция в разработке.")
        await show_admin_keyboard(update, context)
        return ADMIN_MENU

    elif data == "admin_exit":
        context.user_data.clear()
        await query.edit_message_text("👋 Выход из админ-панели.")
        return ConversationHandler.END

    else:
        await query.edit_message_text("Неизвестная команда.")
        return ConversationHandler.END

async def admin_handle_target(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target_text = update.message.text.strip()
    target_id = extract_target_id(target_text)
    if not target_id:
        await update.message.reply_text("❌ Пользователь не найден. Попробуйте ещё раз.")
        return ADMIN_AWAIT_TARGET

    context.user_data['admin_target_id'] = target_id
    action = context.user_data.get('admin_last_action')
    admin_action = context.user_data.get('admin_action')  # для бана/разбана

    if action in ("admin_give", "admin_take"):
        await update.message.reply_text("Введите сумму:")
        return ADMIN_AWAIT_AMOUNT
    elif action in ("admin_ban", "admin_unban"):
        # выполняем сразу
        await perform_ban_unban(update, context, target_id, action)
        await show_admin_keyboard(update, context)
        return ADMIN_MENU
    elif action == "admin_setdesc":
        await update.message.reply_text("Введите новый текст описания:")
        return ADMIN_AWAIT_DESC
    elif action == "admin_setstatus":
        await update.message.reply_text("Введите новый статус:")
        return ADMIN_AWAIT_STATUS
    elif action == "admin_setprotection":
        await update.message.reply_text("Введите новый тип защиты:")
        return ADMIN_AWAIT_PROTECTION
    else:
        await update.message.reply_text("Ошибка. Начните заново.")
        return ConversationHandler.END

async def perform_ban_unban(update: Update, context: ContextTypes.DEFAULT_TYPE, target_id: int, action: str):
    admin_id = update.effective_user.id
    if action == "admin_ban":
        with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
            c = conn.cursor()
            c.execute("UPDATE users SET is_banned = 1 WHERE user_id = ?", (target_id,))
            conn.commit()
        log_admin_action(admin_id, "ban", target_id)
        await update.message.reply_text(f"✅ Пользователь {target_id} забанен.")
    elif action == "admin_unban":
        with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
            c = conn.cursor()
            c.execute("UPDATE users SET is_banned = 0 WHERE user_id = ?", (target_id,))
            conn.commit()
        log_admin_action(admin_id, "unban", target_id)
        await update.message.reply_text(f"✅ Пользователь {target_id} разбанен.")

async def admin_handle_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    amount_text = update.message.text.strip()
    if amount_text.lower() in ("все", "вб"):
        await update.message.reply_text("❌ Нельзя использовать 'все' для административных операций. Введите конкретное число.")
        return ADMIN_AWAIT_AMOUNT
    try:
        amount = int(amount_text) if amount_text.isdigit() else 0
    except:
        await update.message.reply_text("❌ Неверная сумма. Введите число.")
        return ADMIN_AWAIT_AMOUNT

    target_id = context.user_data.get('admin_target_id')
    action = context.user_data.get('admin_last_action')
    admin_id = update.effective_user.id

    if action == "admin_give":
        if not check_admin_limit(admin_id, amount):
            await update.message.reply_text("❌ Превышен лимит выдачи.")
            return ADMIN_MENU
        update_balance(target_id, amount)
        log_admin_action(admin_id, "give", target_id, amount)
        await update.message.reply_text(f"✅ Выдано {amount:,} кредиксов пользователю {target_id}.")

    elif action == "admin_take":
        balance = get_balance(target_id)
        if balance < amount:
            await update.message.reply_text("❌ У пользователя недостаточно средств.")
            return ADMIN_MENU
        update_balance(target_id, -amount)
        log_admin_action(admin_id, "take", target_id, amount)
        await update.message.reply_text(f"✅ Списано {amount:,} кредиксов у пользователя {target_id}.")

    await show_admin_keyboard(update, context)
    return ADMIN_MENU

async def admin_handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message_text = update.message.text.strip()
    admin_id = update.effective_user.id
    await update.message.reply_text("⏳ Начинаю рассылку...")
    asyncio.create_task(broadcast_notification(context.bot, message_text, admin_id))
    await show_admin_keyboard(update, context)
    return ADMIN_MENU

async def admin_handle_promo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    parts = text.split()
    if len(parts) < 3:
        await update.message.reply_text("❌ Неверный формат. Попробуйте ещё раз.")
        return ADMIN_AWAIT_PROMO
    try:
        amount = parse_amount(parts[0])
        max_uses = int(parts[1])
    except:
        await update.message.reply_text("❌ Сумма и количество должны быть числами (не используйте 'все').")
        return ADMIN_AWAIT_PROMO

    if len(parts) >= 4:
        code = parts[2].upper()
        promo_text = ' '.join(parts[3:])
    else:
        code = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=8))
        promo_text = ' '.join(parts[2:])

    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute("SELECT code FROM promocodes WHERE code = ?", (code,))
        if c.fetchone():
            await update.message.reply_text(f"❌ Промокод <b>{code}</b> уже существует. Придумайте другой.", parse_mode=ParseMode.HTML)
            return ADMIN_AWAIT_PROMO
        c.execute('''INSERT INTO promocodes (code, amount, max_uses, message) VALUES (?,?,?,?)''',
                  (code, amount, max_uses, promo_text))
        conn.commit()
    await update.message.reply_text(
        f"✅ Промокод создан: <b>{code}</b>\nСумма: {amount:,}, активаций: {max_uses}\nСообщение: {promo_text}",
        parse_mode=ParseMode.HTML
    )
    await show_admin_keyboard(update, context)
    return ADMIN_MENU

async def admin_handle_desc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_desc = update.message.text.strip()
    target_id = context.user_data.get('admin_target_id')
    admin_id = update.effective_user.id
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute("UPDATE users SET description = ? WHERE user_id = ?", (new_desc, target_id))
        conn.commit()
    log_admin_action(admin_id, "setdesc", target_id)
    await update.message.reply_text(f"✅ Описание пользователя {target_id} изменено.")
    await show_admin_keyboard(update, context)
    return ADMIN_MENU

async def admin_handle_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_status = update.message.text.strip()
    target_id = context.user_data.get('admin_target_id')
    admin_id = update.effective_user.id
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute("UPDATE users SET status = ? WHERE user_id = ?", (new_status, target_id))
        conn.commit()
    log_admin_action(admin_id, "setstatus", target_id)
    await update.message.reply_text(f"✅ Статус пользователя {target_id} изменён на '{new_status}'.")
    await show_admin_keyboard(update, context)
    return ADMIN_MENU

async def admin_handle_protection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_protection = update.message.text.strip()
    target_id = context.user_data.get('admin_target_id')
    admin_id = update.effective_user.id
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute("UPDATE users SET protection = ? WHERE user_id = ?", (new_protection, target_id))
        conn.commit()
    log_admin_action(admin_id, "setprotection", target_id)
    await update.message.reply_text(f"✅ Защита пользователя {target_id} изменена на '{new_protection}'.")
    await show_admin_keyboard(update, context)
    return ADMIN_MENU

# ---------- ТОП ----------
async def top(update: Update, context: ContextTypes.DEFAULT_TYPE):
    exclude_ids = set(OFFICIAL_IDS) | {SUPER_ADMIN_ID}
    placeholders = ','.join(['?'] * len(exclude_ids))
    query = f'''SELECT first_name, username, balance FROM users 
                WHERE is_banned = 0 AND is_admin = 0 AND user_id NOT IN ({placeholders})
                ORDER BY balance DESC LIMIT 10'''
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute(query, list(exclude_ids))
        top_users = c.fetchall()
    if not top_users:
        await update.message.reply_text("Топ пока пуст.")
        return
    text = "<b>🏆 Топ-10 богачей</b>\n"
    for i, (name, username, bal) in enumerate(top_users, 1):
        safe_name = escape_html(name)
        safe_username = escape_html(username) if username else "—"
        text += f"{i}. <b>{safe_name}</b> @{safe_username} — {bal:,} кредиксов\n"
    final_text = f"<blockquote>{text}</blockquote>"
    await update.message.reply_text(final_text, parse_mode=ParseMode.HTML)

# ---------- ПОМОЩЬ ----------
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = """
<b>🎮 Доступные игры (без /):</b>
• <b>Футбол</b> (сумма) (гол/мимо)
• <b>Баскетбол/Бс</b> (сумма) (гол/мимо)
• <b>Рулетка</b> (сумма) (ставка) — число, красное/черное, чет/нечет, 1-12,13-24,25-36, 1-18,19-36, колонка1/2/3, или диапазон вида 10-30
• <b>Мины</b> (сумма) — открывайте ячейки, избегая мин
• <b>Кубик</b> (сумма) (меньше 3/больше 3/чёт/нечёт/равно 3)
• <b>21</b> (сумма) — игра против дилера
• <b>Слоты</b> (сумма) — игровой автомат
• <b>Башня</b> (сумма) — выбери безопасную ячейку на каждом этаже
• <b>Фишки</b> (сумма) (черное/белое)
• <b>Пирамида</b> (сумма) — выбирай ячейки, множитель растёт
• <b>Кубы</b> (сумма) — бросай кубик против бота
• <b>Дартс</b> (сумма) (красное/белое) — бросок дротика
• <b>Крестики</b> (@username) — игра в крестики-нолики с другим игроком
• <b>Блэкджек / бдж</b> (сумма) — многопользовательская игра в чате (от 3 до 6 игроков)
• <b>Сундуки</b> (сумма) — выбери один из трёх сундуков, угадай с ключом
• <b>Золото</b> (сумма) — выбирай левую или правую ячейку, избегая мины. Множители растут до 4096x

<b>💰 Экономика:</b>
• <b>работа</b> — 20 кредиксов (без кулдауна)
• <b>бонус</b> — 2000 раз в час
• <b>ежедневный бонус</b> — 10000 раз в день
• <b>профиль</b> — полная статистика
• <b>б</b> или <b>баланс</b> — краткий баланс и кнопки бонусов
• <b>дать</b> (сумма) (@юзернейм или ответом) — перевод
• <b>такси</b> — работа на машине (раз в час)
• <b>автосалон</b> — покупка/аренда машин (кнопки)
• <b>мои машины</b> — список ваших машин
• <b>моя машина</b> — информация об активной машине

<b>📦 Рефералы:</b> приглашайте друзей, получайте 10000 за каждого
• <b>реф</b> — информация о рефералах

<b>🎁 Промокоды:</b> промо (код)

<b>🎉 Конкурс:</b> информация о текущем конкурсе

<b>🤝 Сотрудничество:</b> информация для партнёров

<b>🎯 Ежедневные задания:</b> задания — список и прогресс

📢 <b>Новости:</b> https://t.me/werdoxz_wiinere
💬 <b>Чат:</b> https://t.me/+B7u5OmPsako4MTAy
🆘 <b>Поддержка:</b> @what_lova

<i>Суммы можно указывать с суффиксами: 100к = 100 000, 5кк = 5 000 000. Также можно писать "все" вместо суммы — будет поставлена ставка на весь баланс.</i>
"""
    final_text = f"<blockquote>{help_text}</blockquote>"
    await update.message.reply_text(final_text, parse_mode=ParseMode.HTML)

# ---------- РЕФЕРАЛЫ ----------
async def referrals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ensure_user_exists(user_id)
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id = ?", (user_id,))
        count = c.fetchone()[0]
        bonus = count * 10000
    bot_username = context.bot.username
    ref_link = f"https://t.me/{bot_username}?start=ref{user_id}"
    text = (
        f"<b>📊 Ваша реферальная статистика</b>\n\n"
        f"Приглашено друзей: <b>{count}</b>\n"
        f"Заработано бонусов: <b>{bonus:,}</b> кредиксов\n\n"
        f"<b>Ваша ссылка для приглашения:</b>\n<code>{ref_link}</code>\n\n"
        f"За каждого друга, который перейдёт по ссылке и запустит бота, вы получите <b>10000</b> кредиксов!\n\n"
        f"🎁 <b>Розыгрыш:</b> https://t.me/gidtskredigs/4259"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)

# ---------- ИГРЫ ----------
# Футбол
async def football(update: Update, context: ContextTypes.DEFAULT_TYPE, bet: int, choice: str):
    user_id = update.effective_user.id
    ensure_user_exists(user_id)
    balance = get_balance(user_id)
    if bet <= 0 or bet > balance:
        await update.message.reply_text("❌ Недостаточно средств или неверная ставка.")
        return

    seed, fair_hash = generate_fair_hash()
    context.user_data['fair_seed'] = seed
    context.user_data['fair_hash'] = fair_hash

    msg = await update.message.reply_dice(emoji="⚽")
    await asyncio.sleep(1)
    dice_value = msg.dice.value
    result = "гол" if dice_value >= 3 else "мимо"

    if result == choice:
        win = bet * 2
        update_balance(user_id, win - bet)
        add_game_stat(user_id, win, 0)
        text = f"⚽ <b>Мяч попал в ворота!</b> Вы выиграли <b>{win:,}</b> кредиксов.\n"
    else:
        update_balance(user_id, -bet)
        add_game_stat(user_id, 0, bet)
        text = f"⚽ <b>Проигрыш!</b> Мяч пролетел мимо. Вы проиграли <b>{bet:,}</b> кредиксов.\n"

    text += f"<code>Хэш раунда: {fair_hash}</code>"
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)
    context.user_data.pop('fair_seed', None)
    context.user_data.pop('fair_hash', None)

# Баскетбол
async def basketball(update: Update, context: ContextTypes.DEFAULT_TYPE, bet: int, choice: str):
    user_id = update.effective_user.id
    ensure_user_exists(user_id)
    balance = get_balance(user_id)
    if bet <= 0 or bet > balance:
        await update.message.reply_text("❌ Недостаточно средств или неверная ставка.")
        return

    seed, fair_hash = generate_fair_hash()
    context.user_data['fair_seed'] = seed
    context.user_data['fair_hash'] = fair_hash

    msg = await update.message.reply_dice(emoji="🏀")
    await asyncio.sleep(1)
    dice_value = msg.dice.value

    if dice_value == 3:
        if choice == "мимо":
            win = int(bet * 1.5)
            update_balance(user_id, win - bet)
            add_game_stat(user_id, win, 0)
            text = f"🏀 <b>Мяч попадает в дужку!</b> Вы выиграли <b>{win:,}</b> кредиксов (x1.5).\n"
        else:
            update_balance(user_id, -bet)
            add_game_stat(user_id, 0, bet)
            text = f"🏀 <b>Мяч попадает в дужку, но не гол!</b> Вы проиграли <b>{bet:,}</b> кредиксов.\n"
    else:
        result = "гол" if dice_value >= 4 else "мимо"
        if result == choice:
            win = bet * 2
            update_balance(user_id, win - bet)
            add_game_stat(user_id, win, 0)
            text = f"🏀 <b>Попадание!</b> Вы выиграли <b>{win:,}</b> кредиксов.\n"
        else:
            update_balance(user_id, -bet)
            add_game_stat(user_id, 0, bet)
            text = f"🏀 <b>Проигрыш!</b> Вы проиграли <b>{bet:,}</b> кредиксов.\n"

    text += f"<code>Хэш раунда: {fair_hash}</code>"
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)
    context.user_data.pop('fair_seed', None)
    context.user_data.pop('fair_hash', None)

# Кубик
async def dice_game(update: Update, context: ContextTypes.DEFAULT_TYPE, bet: int, bet_type: str):
    user_id = update.effective_user.id
    ensure_user_exists(user_id)
    balance = get_balance(user_id)
    if bet <= 0 or bet > balance:
        await update.message.reply_text("❌ Недостаточно средств или неверная ставка.")
        return

    seed, fair_hash = generate_fair_hash()
    context.user_data['fair_seed'] = seed
    context.user_data['fair_hash'] = fair_hash

    msg = await update.message.reply_dice(emoji="🎲")
    await asyncio.sleep(1)
    dice_value = msg.dice.value
    win = False
    multiplier = 2
    if bet_type == "равно 3":
        if dice_value == 3:
            win = True
            multiplier = 5
    elif bet_type == "меньше 3":
        win = dice_value < 3
    elif bet_type == "больше 3":
        win = dice_value > 3
    elif bet_type == "чёт":
        win = dice_value % 2 == 0
    elif bet_type == "нечёт":
        win = dice_value % 2 == 1

    if win:
        win_amount = bet * multiplier
        update_balance(user_id, win_amount - bet)
        add_game_stat(user_id, win_amount, 0)
        text = f"🎲 Выпало <b>{dice_value}</b>. <b>Вы выиграли {win_amount:,} кредиксов!</b>\n"
    else:
        update_balance(user_id, -bet)
        add_game_stat(user_id, 0, bet)
        text = f"🎲 Выпало <b>{dice_value}</b>. <b>Вы проиграли {bet:,} кредиксов.</b>\n"

    text += f"<code>Хэш раунда: {fair_hash}</code>"
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)
    context.user_data.pop('fair_seed', None)
    context.user_data.pop('fair_hash', None)

# Слоты
async def slots(update: Update, context: ContextTypes.DEFAULT_TYPE, bet: int):
    user_id = update.effective_user.id
    ensure_user_exists(user_id)
    balance = get_balance(user_id)
    if bet <= 0 or bet > balance:
        await update.message.reply_text("❌ Недостаточно средств или неверная ставка.")
        return

    seed, fair_hash = generate_fair_hash()
    context.user_data['fair_seed'] = seed
    context.user_data['fair_hash'] = fair_hash

    msg = await update.message.reply_dice(emoji="🎰")
    await asyncio.sleep(1)
    slot_value = msg.dice.value

    triple_values = {1, 22, 43, 64}
    double_values = set(range(2, 7)) | set(range(23, 28)) | set(range(44, 49))

    if slot_value in triple_values:
        win_mult = 3
        win = int(bet * win_mult)
        update_balance(user_id, win - bet)
        add_game_stat(user_id, win, 0)
        text = f"🎰 <b>Джекпот!</b> Вы выиграли <b>{win:,}</b> кредиксов (x3)!\n"
    elif slot_value in double_values:
        win_mult = 1.3
        win = int(bet * win_mult)
        update_balance(user_id, win - bet)
        add_game_stat(user_id, win, 0)
        text = f"🎰 <b>Повезло!</b> Вы выиграли <b>{win:,}</b> кредиксов (x1.3)!\n"
    else:
        update_balance(user_id, -bet)
        add_game_stat(user_id, 0, bet)
        text = f"🎰 <b>Проигрыш.</b> Вы проиграли <b>{bet:,}</b> кредиксов.\n"

    text += f"<code>Хэш раунда: {fair_hash}</code>"
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)
    context.user_data.pop('fair_seed', None)
    context.user_data.pop('fair_hash', None)

# Кубы
async def cubes(update: Update, context: ContextTypes.DEFAULT_TYPE, bet: int):
    user_id = update.effective_user.id
    ensure_user_exists(user_id)
    balance = get_balance(user_id)
    if bet <= 0 or bet > balance:
        await update.message.reply_text("❌ Недостаточно средств или неверная ставка.")
        return

    seed, fair_hash = generate_fair_hash()
    context.user_data['fair_seed'] = seed
    context.user_data['fair_hash'] = fair_hash

    update_balance(user_id, -bet)

    player_msg = await update.message.reply_dice(emoji="🎲")
    await asyncio.sleep(1)
    player_value = player_msg.dice.value

    bot_msg = await update.message.reply_dice(emoji="🎲")
    await asyncio.sleep(1)
    bot_value = bot_msg.dice.value

    if player_value > bot_value:
        win = bet * 2
        update_balance(user_id, win)
        add_game_stat(user_id, win, 0)
        text = f"🎲 Ваш кубик: <b>{player_value}</b>\n🎲 Кубик бота: <b>{bot_value}</b>\n✅ <b>Вы выиграли {win:,} кредиксов!</b>\n"
    elif player_value < bot_value:
        add_game_stat(user_id, 0, bet)
        text = f"🎲 Ваш кубик: <b>{player_value}</b>\n🎲 Кубик бота: <b>{bot_value}</b>\n❌ <b>Вы проиграли {bet:,} кредиксов.</b>\n"
    else:
        update_balance(user_id, bet)
        add_game_stat(user_id, 0, 0)
        text = f"🎲 Ваш кубик: <b>{player_value}</b>\n🎲 Кубик бота: <b>{bot_value}</b>\n🤝 <b>Ничья, ставка возвращена.</b>\n"

    text += f"<code>Хэш раунда: {fair_hash}</code>"
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)
    context.user_data.pop('fair_seed', None)
    context.user_data.pop('fair_hash', None)

# Фишки
async def chips(update: Update, context: ContextTypes.DEFAULT_TYPE, bet: int, color: str):
    user_id = update.effective_user.id
    ensure_user_exists(user_id)
    balance = get_balance(user_id)
    if bet <= 0 or bet > balance:
        await update.message.reply_text("❌ Недостаточно средств или неверная ставка.")
        return

    seed, fair_hash = generate_fair_hash()
    context.user_data['fair_seed'] = seed
    context.user_data['fair_hash'] = fair_hash

    result = random.choice(["черное", "белое"])
    if result == color:
        win = bet * 2
        update_balance(user_id, win - bet)
        add_game_stat(user_id, win, 0)
        text = f"🎴 Выпало <b>{result}</b>. ✅ <b>Вы выиграли {win:,} кредиксов!</b>\n"
    else:
        update_balance(user_id, -bet)
        add_game_stat(user_id, 0, bet)
        text = f"🎴 Выпало <b>{result}</b>. ❌ <b>Вы проиграли {bet:,} кредиксов.</b>\n"

    text += f"<code>Хэш раунда: {fair_hash}</code>"
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)
    context.user_data.pop('fair_seed', None)
    context.user_data.pop('fair_hash', None)

# Дартс
async def darts(update: Update, context: ContextTypes.DEFAULT_TYPE, bet: int, color: str):
    user_id = update.effective_user.id
    ensure_user_exists(user_id)
    balance = get_balance(user_id)
    if bet <= 0 or bet > balance:
        await update.message.reply_text("❌ Недостаточно средств или неверная ставка.")
        return

    seed, fair_hash = generate_fair_hash()
    context.user_data['fair_seed'] = seed
    context.user_data['fair_hash'] = fair_hash

    msg = await update.message.reply_dice(emoji="🎯")
    await asyncio.sleep(1)
    dice_value = msg.dice.value

    if dice_value in (1, 2):
        result = "красное"
    elif dice_value in (3, 4):
        result = "белое"
    else:
        result = "мимо"

    if result == "мимо":
        update_balance(user_id, -bet)
        add_game_stat(user_id, 0, bet)
        text = f"🎯 Дротик улетел <b>мимо</b>! Вы проиграли <b>{bet:,}</b> кредиксов.\n"
    elif result == color:
        multiplier = DARTS_MULTIPLIER_RED if color == "красное" else DARTS_MULTIPLIER_WHITE
        win = int(bet * multiplier)
        update_balance(user_id, win - bet)
        add_game_stat(user_id, win, 0)
        text = f"🎯 <b>Попадание в {result}!</b> Вы выиграли <b>{win:,}</b> кредиксов (x{multiplier})!\n"
    else:
        update_balance(user_id, -bet)
        add_game_stat(user_id, 0, bet)
        text = f"🎯 Выпало <b>{result}</b>, а вы ставили на <b>{color}</b>. ❌ Проигрыш <b>{bet:,}</b> кредиксов.\n"

    text += f"<code>Хэш раунда: {fair_hash}</code>"
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)
    context.user_data.pop('fair_seed', None)
    context.user_data.pop('fair_hash', None)

# ---------- РУЛЕТКА ----------
roulette_lock = asyncio.Lock()

async def roulette_bet(update: Update, context: ContextTypes.DEFAULT_TYPE, bet: int, bet_type: str):
    user_id = update.effective_user.id
    ensure_user_exists(user_id)
    balance = get_balance(user_id)
    if bet <= 0 or bet > balance:
        await update.message.reply_text("❌ Недостаточно средств или неверная ставка.")
        return

    valid_bet_types = (
        [str(i) for i in range(37)] +
        ["красное", "черное", "чет", "нечет", "1-12", "13-24", "25-36",
         "1-18", "19-36", "колонка1", "колонка2", "колонка3"]
    )
    is_range = False
    if '-' in bet_type and not bet_type.startswith('колонка'):
        parts = bet_type.split('-')
        if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
            low = int(parts[0])
            high = int(parts[1])
            if 0 <= low <= 36 and 0 <= high <= 36 and low <= high:
                is_range = True
    if not is_range and bet_type not in valid_bet_types:
        await update.message.reply_text("❌ Неверный тип ставки.")
        return

    async with roulette_lock:
        update_balance(user_id, -bet)
        if 'roulette' not in context.bot_data:
            context.bot_data['roulette'] = {'bets': [], 'last_spin': None, 'last_bet_time': None}
        context.bot_data['roulette']['last_bet_time'] = datetime.now()
        context.bot_data['roulette']['bets'].append({
            'user_id': user_id,
            'bet': bet,
            'bet_type': bet_type
        })

    update_quest_progress(user_id, 5, 1)
    await update.message.reply_text(f"✅ Ставка принята: {bet:,} на {bet_type}. Всего ставок: {len(context.bot_data['roulette']['bets'])}")

async def roulette_spin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with roulette_lock:
        roulette_data = context.bot_data.get('roulette')
        if not roulette_data or not roulette_data['bets']:
            await update.message.reply_text("❌ Нет активных ставок.")
            return

        last_bet = roulette_data.get('last_bet_time')
        now = datetime.now()
        if last_bet:
            elapsed = (now - last_bet).total_seconds()
            if elapsed < 12:
                remaining = 12 - elapsed
                await update.message.reply_text(f"⏳ Рулетку можно запускать через 12 секунд после последней ставки. Подождите ещё {remaining:.1f} сек.")
                return

        seed, fair_hash = generate_fair_hash()
        number = random.randint(0, 36)
        red_numbers = [1,3,5,7,9,12,14,16,18,19,21,23,25,27,30,32,34,36]
        color = "зеленое" if number == 0 else ("красное" if number in red_numbers else "черное")

        column = None
        if number != 0:
            if number % 3 == 1:
                column = "колонка1"
            elif number % 3 == 2:
                column = "колонка2"
            else:
                column = "колонка3"
        dozen = None
        if 1 <= number <= 12:
            dozen = "1-12"
        elif 13 <= number <= 24:
            dozen = "13-24"
        elif 25 <= number <= 36:
            dozen = "25-36"

        results = []
        total_bet = 0
        total_win = 0
        for bet_info in roulette_data['bets']:
            uid = bet_info['user_id']
            bet = bet_info['bet']
            bet_type = bet_info['bet_type']
            total_bet += bet

            win_multiplier = 0
            if bet_type.isdigit() and 0 <= int(bet_type) <= 36:
                if int(bet_type) == number:
                    win_multiplier = ROULETTE_PAYOUTS["число"]
            elif bet_type in ("красное", "черное"):
                if bet_type == color:
                    win_multiplier = ROULETTE_PAYOUTS["цвет"]
            elif bet_type in ("чет", "нечет"):
                if number != 0:
                    if (bet_type == "чет" and number % 2 == 0) or (bet_type == "нечет" and number % 2 == 1):
                        win_multiplier = ROULETTE_PAYOUTS["чет"]
            elif bet_type in ("1-12", "13-24", "25-36"):
                if bet_type == dozen:
                    win_multiplier = ROULETTE_PAYOUTS[bet_type]
            elif bet_type == "1-18":
                if 1 <= number <= 18:
                    win_multiplier = ROULETTE_PAYOUTS["1-18"]
            elif bet_type == "19-36":
                if 19 <= number <= 36:
                    win_multiplier = ROULETTE_PAYOUTS["19-36"]
            elif bet_type in ("колонка1", "колонка2", "колонка3"):
                if bet_type == column:
                    win_multiplier = ROULETTE_PAYOUTS[bet_type]
            elif '-' in bet_type and not bet_type.startswith('колонка'):
                parts = bet_type.split('-')
                if len(parts) == 2:
                    low, high = int(parts[0]), int(parts[1])
                    if low <= number <= high:
                        count = high - low + 1
                        win_multiplier = int(36 / count)

            if win_multiplier > 0:
                win = int(bet * win_multiplier)
                update_balance(uid, win)
                add_game_stat(uid, win, 0)
                results.append(f"🟢 Игрок {uid} выиграл {win:,} (ставка {bet:,} на {bet_type})")
                total_win += win
            else:
                add_game_stat(uid, 0, bet)
                results.append(f"🔴 Игрок {uid} проиграл {bet:,} (ставка {bet_type})")

        report = f"<b>🎲 Рулетка: выпало {number} ({color})</b>\n\n"
        report += "\n".join(results)
        report += f"\n\n<b>Итог:</b> Всего ставок: {total_bet:,}, выплачено: {total_win:,}\n<code>Хэш раунда: {fair_hash}</code>"
        wrapped_report = f"<blockquote>{report}</blockquote>"

        roulette_data['bets'] = []
        roulette_data['last_spin'] = now
        roulette_data['last_hash'] = fair_hash
        roulette_data['last_seed'] = seed

    await update.message.reply_text(wrapped_report, parse_mode=ParseMode.HTML)

async def roulette_bets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    roulette_data = context.bot_data.get('roulette')
    if not roulette_data or not roulette_data['bets']:
        await update.message.reply_text("Сейчас нет активных ставок.")
        return
    text = "<b>Текущие ставки в рулетке:</b>\n"
    for b in roulette_data['bets']:
        text += f"• Игрок {b['user_id']}: {b['bet']:,} на {b['bet_type']}\n"
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)

# ---------- МИНЫ ----------
async def mines_start(update: Update, context: ContextTypes.DEFAULT_TYPE, bet: int):
    user_id = update.effective_user.id
    ensure_user_exists(user_id)
    balance = get_balance(user_id)
    if bet <= 0 or bet > balance:
        await update.message.reply_text("❌ Недостаточно средств или неверная ставка.")
        return

    update_balance(user_id, -bet)

    cells = [0] * MINES_TOTAL_CELLS
    mines_positions = random.sample(range(MINES_TOTAL_CELLS), MINES_COUNT)
    for pos in mines_positions:
        cells[pos] = 1

    seed, fair_hash = generate_fair_hash()
    context.user_data['fair_seed'] = seed
    context.user_data['fair_hash'] = fair_hash

    context.user_data['mines'] = {
        'cells': cells,
        'revealed': [False] * MINES_TOTAL_CELLS,
        'bet': bet,
        'step': 0,
        'max_step': 10,
        'hash': fair_hash,
        'seed': seed,
        'user_id': user_id
    }
    context.user_data['current_game'] = 'mines'
    context.user_data['game_msg_count'] = 0  # счётчик сообщений во время игры

    await update.message.reply_text(
        f"<b>💣 Игра Мины начата!</b>\nОткройте 10 безопасных ячеек для выигрыша x{MINES_WIN_MULTIPLIER}.\n"
        f"<code>Хэш раунда: {fair_hash}</code>",
        parse_mode=ParseMode.HTML
    )
    await show_mines_field(update, context)

async def show_mines_field(update: Update, context: ContextTypes.DEFAULT_TYPE):
    mines = context.user_data.get('mines')
    if not mines:
        return
    revealed = mines['revealed']
    cells = mines['cells']
    keyboard = []
    for i in range(0, MINES_TOTAL_CELLS, 5):
        row = []
        for j in range(5):
            idx = i + j
            if revealed[idx]:
                if cells[idx] == 1:
                    text = "💥"
                else:
                    text = "💰"
            else:
                text = "❓"
            row.append(InlineKeyboardButton(text, callback_data=f"mine_{idx}"))
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("💰 Забрать выигрыш", callback_data="mine_take")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    if update.callback_query:
        await update.callback_query.edit_message_text("Выберите ячейку:", reply_markup=reply_markup)
    else:
        await update.message.reply_text("Выберите ячейку:", reply_markup=reply_markup)

async def mines_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data

    mines = context.user_data.get('mines')
    if not mines:
        await query.edit_message_text("Игра не найдена.")
        return
    if mines.get('user_id') != user_id:
        await query.answer("Это не ваша игра!", show_alert=True)
        return

    # Сбрасываем счётчик сообщений при действии
    context.user_data['game_msg_count'] = 0

    if data == "mine_take":
        if mines['step'] == 0:
            await query.edit_message_text("Вы ещё не открыли ни одной ячейки.")
            return
        win = int(mines['bet'] * (1 + 0.06 * mines['step']))
        update_balance(user_id, win)
        add_game_stat(user_id, win, 0)
        text = f"✅ <b>Вы забрали выигрыш: {win:,} кредиксов.</b>\n"
        text += f"<code>Seed раунда: {mines['seed']}</code>"
        await query.edit_message_text(text, parse_mode=ParseMode.HTML)
        context.user_data.pop('mines', None)
        context.user_data.pop('current_game', None)
        return

    if not data.startswith("mine_"):
        return
    cell = int(data.split("_")[1])
    if mines['revealed'][cell]:
        await query.answer("Эта ячейка уже открыта.", show_alert=True)
        await show_mines_field(update, context)
        return
    mines['revealed'][cell] = True
    mines['step'] += 1
    if mines['cells'][cell] == 1:
        await query.edit_message_text("💥 <b>БАХ! Вы наступили на мину!</b>", parse_mode=ParseMode.HTML)
        add_game_stat(user_id, 0, mines['bet'])
        context.user_data.pop('mines', None)
        context.user_data.pop('current_game', None)
        return
    if mines['step'] >= mines['max_step']:
        win = int(mines['bet'] * MINES_WIN_MULTIPLIER)
        update_balance(user_id, win)
        add_game_stat(user_id, win, 0)
        text = f"🎉 <b>Вы открыли 10 безопасных ячеек! Выигрыш: {win:,} кредиксов (x{MINES_WIN_MULTIPLIER}).</b>\n"
        text += f"<code>Seed раунда: {mines['seed']}</code>"
        await query.edit_message_text(text, parse_mode=ParseMode.HTML)
        context.user_data.pop('mines', None)
        context.user_data.pop('current_game', None)
        return
    await show_mines_field(update, context)

# ---------- БАШНЯ ----------
async def tower_start(update: Update, context: ContextTypes.DEFAULT_TYPE, bet: int):
    user_id = update.effective_user.id
    ensure_user_exists(user_id)
    balance = get_balance(user_id)
    if bet <= 0 or bet > balance:
        await update.message.reply_text("❌ Недостаточно средств или неверная ставка.")
        return

    update_balance(user_id, -bet)
    tower = []
    for floor in range(TOWER_FLOORS):
        mine_pos = random.randint(0, TOWER_CELLS_PER_FLOOR - 1)
        tower.append(mine_pos)

    seed, fair_hash = generate_fair_hash()
    context.user_data['fair_seed'] = seed
    context.user_data['fair_hash'] = fair_hash

    context.user_data['tower'] = {
        'tower': tower,
        'floor': 0,
        'bet': bet,
        'hash': fair_hash,
        'seed': seed,
        'user_id': user_id
    }
    context.user_data['current_game'] = 'tower'
    context.user_data['game_msg_count'] = 0

    await update.message.reply_text(
        f"<b>🏛 Игра Башня!</b> Пройдите все 10 этажей, избегая мин. Выигрыш x3.\n"
        f"<code>Хэш раунда: {fair_hash}</code>",
        parse_mode=ParseMode.HTML
    )
    await show_tower_floor(update, context)

async def show_tower_floor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tower_data = context.user_data.get('tower')
    if not tower_data:
        return
    floor = tower_data['floor'] + 1
    keyboard = []
    row = []
    for i in range(1, TOWER_CELLS_PER_FLOOR + 1):
        row.append(InlineKeyboardButton(str(i), callback_data=f"tower_{i-1}"))
    keyboard.append(row)
    reply_markup = InlineKeyboardMarkup(keyboard)
    if update.callback_query:
        await update.callback_query.edit_message_text(f"Этаж {floor}. Выберите ячейку:", reply_markup=reply_markup)
    else:
        await update.message.reply_text(f"Этаж {floor}. Выберите ячейку:", reply_markup=reply_markup)

async def tower_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data
    if not data.startswith("tower_"):
        return
    cell = int(data.split("_")[1])
    tower_data = context.user_data.get('tower')
    if not tower_data:
        await query.edit_message_text("Игра не найдена.")
        return
    if tower_data.get('user_id') != user_id:
        await query.answer("Это не ваша игра!", show_alert=True)
        return
    context.user_data['game_msg_count'] = 0  # сброс счётчика
    floor = tower_data['floor']
    mine_pos = tower_data['tower'][floor]
    if cell == mine_pos:
        await query.edit_message_text("💥 <b>БАХ! Вы подорвались на мине!</b>", parse_mode=ParseMode.HTML)
        add_game_stat(user_id, 0, tower_data['bet'])
        context.user_data.pop('tower', None)
        context.user_data.pop('current_game', None)
        return
    tower_data['floor'] += 1
    if tower_data['floor'] >= TOWER_FLOORS:
        win = int(tower_data['bet'] * 3)
        update_balance(user_id, win)
        add_game_stat(user_id, win, 0)
        update_quest_progress(user_id, 6, 1)
        text = f"🎉 <b>Вы прошли все 10 этажей! Выигрыш: {win:,} кредиксов (x3)!</b>\n"
        text += f"<code>Seed раунда: {tower_data['seed']}</code>"
        await query.edit_message_text(text, parse_mode=ParseMode.HTML)
        context.user_data.pop('tower', None)
        context.user_data.pop('current_game', None)
        return
    await show_tower_floor(update, context)

# ---------- ПИРАМИДА ----------
async def pyramid_start(update: Update, context: ContextTypes.DEFAULT_TYPE, bet: int):
    user_id = update.effective_user.id
    ensure_user_exists(user_id)
    balance = get_balance(user_id)
    if bet <= 0 or bet > balance:
        await update.message.reply_text("❌ Недостаточно средств или неверная ставка.")
        return

    update_balance(user_id, -bet)
    pyramid = []
    for floor in range(PYRAMID_FLOORS):
        mine_pos = random.randint(0, PYRAMID_CELLS_PER_FLOOR - 1)
        pyramid.append(mine_pos)

    seed, fair_hash = generate_fair_hash()
    context.user_data['fair_seed'] = seed
    context.user_data['fair_hash'] = fair_hash

    context.user_data['pyramid'] = {
        'pyramid': pyramid,
        'floor': 0,
        'bet': bet,
        'multiplier': 1.0,
        'hash': fair_hash,
        'seed': seed,
        'user_id': user_id
    }
    context.user_data['current_game'] = 'pyramid'
    context.user_data['game_msg_count'] = 0

    await update.message.reply_text(
        f"<b>🏛 Игра Пирамида!</b>\nНа каждом этаже {PYRAMID_CELLS_PER_FLOOR} ячейки, одна мина.\n"
        f"Множители: {', '.join([f'{m}x' for m in PYRAMID_MULTIPLIERS])}\n"
        f"<code>Хэш раунда: {fair_hash}</code>",
        parse_mode=ParseMode.HTML
    )
    await show_pyramid_floor(update, context)

async def show_pyramid_floor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pyramid_data = context.user_data.get('pyramid')
    if not pyramid_data:
        return
    floor = pyramid_data['floor'] + 1
    current_mult = PYRAMID_MULTIPLIERS[pyramid_data['floor']] if pyramid_data['floor'] < PYRAMID_FLOORS else 1.0
    keyboard = []
    row = []
    for i in range(1, PYRAMID_CELLS_PER_FLOOR + 1):
        row.append(InlineKeyboardButton(str(i), callback_data=f"pyramid_{i-1}"))
    keyboard.append(row)
    keyboard.append([InlineKeyboardButton("💰 Забрать", callback_data="pyramid_take")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    if update.callback_query:
        await update.callback_query.edit_message_text(
            f"Этаж {floor}. Текущий множитель: {current_mult}x. Выберите ячейку:",
            reply_markup=reply_markup
        )
    else:
        await update.message.reply_text(
            f"Этаж {floor}. Текущий множитель: {current_mult}x. Выберите ячейку:",
            reply_markup=reply_markup
        )

async def pyramid_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data
    pyramid_data = context.user_data.get('pyramid')
    if not pyramid_data:
        await query.edit_message_text("Игра не найдена.")
        return
    if pyramid_data.get('user_id') != user_id:
        await query.answer("Это не ваша игра!", show_alert=True)
        return
    context.user_data['game_msg_count'] = 0  # сброс счётчика

    if data == "pyramid_take":
        floor = pyramid_data['floor']
        if floor == 0:
            await query.edit_message_text("Вы ещё не открыли ни одного этажа.")
            return
        win = int(pyramid_data['bet'] * pyramid_data['multiplier'])
        update_balance(user_id, win)
        add_game_stat(user_id, win, 0)
        text = f"✅ <b>Вы забрали выигрыш: {win:,} кредиксов (x{pyramid_data['multiplier']}).</b>\n"
        text += f"<code>Seed раунда: {pyramid_data['seed']}</code>"
        await query.edit_message_text(text, parse_mode=ParseMode.HTML)
        context.user_data.pop('pyramid', None)
        context.user_data.pop('current_game', None)
        return

    if data.startswith("pyramid_"):
        cell = int(data.split("_")[1])
        floor = pyramid_data['floor']
        if floor >= PYRAMID_FLOORS:
            return
        mine_pos = pyramid_data['pyramid'][floor]
        if cell == mine_pos:
            await query.edit_message_text("💥 <b>БАХ! Вы подорвались на мине!</b>", parse_mode=ParseMode.HTML)
            add_game_stat(user_id, 0, pyramid_data['bet'])
            context.user_data.pop('pyramid', None)
            context.user_data.pop('current_game', None)
            return
        pyramid_data['floor'] += 1
        pyramid_data['multiplier'] = PYRAMID_MULTIPLIERS[pyramid_data['floor'] - 1]
        if pyramid_data['floor'] >= PYRAMID_FLOORS:
            win = int(pyramid_data['bet'] * pyramid_data['multiplier'])
            update_balance(user_id, win)
            add_game_stat(user_id, win, 0)
            text = f"🎉 <b>Поздравляем! Вы прошли все {PYRAMID_FLOORS} этажей!</b>\n"
            text += f"<b>Выигрыш: {win:,} кредиксов (x{pyramid_data['multiplier']}).</b>\n"
            text += f"<code>Seed раунда: {pyramid_data['seed']}</code>"
            await query.edit_message_text(text, parse_mode=ParseMode.HTML)
            context.user_data.pop('pyramid', None)
            context.user_data.pop('current_game', None)
            return
        await show_pyramid_floor(update, context)

# ---------- 21 ОЧКО (одиночное) ----------
async def blackjack_start(update: Update, context: ContextTypes.DEFAULT_TYPE, bet: int):
    user_id = update.effective_user.id
    ensure_user_exists(user_id)
    balance = get_balance(user_id)
    if bet <= 0 or bet > balance:
        await update.message.reply_text("❌ Недостаточно средств или неверная ставка.")
        return

    seed, fair_hash = generate_fair_hash()
    context.user_data['fair_seed'] = seed
    context.user_data['fair_hash'] = fair_hash

    update_balance(user_id, -bet)
    deck = list(range(2, 12)) * 4
    random.shuffle(deck)
    player_cards = [deck.pop(), deck.pop()]
    dealer_cards = [deck.pop(), deck.pop()]
    player_sum = sum(player_cards)
    dealer_sum = sum(dealer_cards)
    context.user_data['blackjack'] = {
        'bet': bet,
        'deck': deck,
        'player': player_cards,
        'dealer': dealer_cards,
        'player_sum': player_sum,
        'dealer_sum': dealer_sum,
        'hash': fair_hash,
        'seed': seed,
        'user_id': user_id
    }
    context.user_data['current_game'] = 'blackjack'
    context.user_data['game_msg_count'] = 0

    keyboard = [
        [InlineKeyboardButton("➕ Ещё", callback_data="bj_hit"),
         InlineKeyboardButton("⏹ Хватит", callback_data="bj_stand")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        f"<b>🃏 21 очко</b>\n\n"
        f"<b>Ваши карты:</b> {' '.join(map(str, player_cards))} (сумма {player_sum})\n"
        f"<b>Карта дилера:</b> {dealer_cards[0]} и ?\n\n"
        f"<code>Хэш раунда: {fair_hash}</code>",
        parse_mode=ParseMode.HTML,
        reply_markup=reply_markup
    )

async def blackjack_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data
    bj = context.user_data.get('blackjack')
    if not bj:
        await query.edit_message_text("Игра не найдена.")
        return
    if bj.get('user_id') != user_id:
        await query.answer("Это не ваша игра!", show_alert=True)
        return
    context.user_data['game_msg_count'] = 0

    if data == "bj_hit":
        deck = bj['deck']
        if not deck:
            deck = list(range(2, 12)) * 4
            random.shuffle(deck)
        card = deck.pop()
        bj['player'].append(card)
        bj['player_sum'] += card
        bj['deck'] = deck
        if bj['player_sum'] > 21:
            text = f"<b>🃏 21 очко</b>\n\nВы взяли карту {card}. Сумма {bj['player_sum']} — <b>перебор!</b>\n"
            text += f"<code>Seed раунда: {bj['seed']}</code>"
            await query.edit_message_text(text, parse_mode=ParseMode.HTML)
            add_game_stat(user_id, 0, bj['bet'])
            context.user_data.pop('blackjack', None)
            context.user_data.pop('current_game', None)
            return
        keyboard = [
            [InlineKeyboardButton("➕ Ещё", callback_data="bj_hit"),
             InlineKeyboardButton("⏹ Хватит", callback_data="bj_stand")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            f"<b>🃏 21 очко</b>\n\n"
            f"<b>Ваши карты:</b> {' '.join(map(str, bj['player']))} (сумма {bj['player_sum']})\n"
            f"<b>Карта дилера:</b> {bj['dealer'][0]} и ?\n\n",
            parse_mode=ParseMode.HTML,
            reply_markup=reply_markup
        )

    elif data == "bj_stand":
        dealer_sum = bj['dealer_sum']
        deck = bj['deck']
        while dealer_sum < 17:
            if not deck:
                deck = list(range(2, 12)) * 4
                random.shuffle(deck)
            card = deck.pop()
            dealer_sum += card
            bj['dealer'].append(card)
            bj['deck'] = deck
        bj['dealer_sum'] = dealer_sum
        result = ""
        if dealer_sum > 21 or bj['player_sum'] > dealer_sum:
            win = bj['bet'] * 2
            update_balance(user_id, win)
            add_game_stat(user_id, win, 0)
            result = f"<b>✅ Вы выиграли {win:,} кредиксов!</b>"
        elif dealer_sum == bj['player_sum']:
            update_balance(user_id, bj['bet'])
            add_game_stat(user_id, 0, 0)
            result = "<b>🤝 Ничья, ставка возвращена.</b>"
        else:
            add_game_stat(user_id, 0, bj['bet'])
            result = f"<b>❌ Вы проиграли {bj['bet']:,} кредиксов.</b>"
        text = f"<b>🃏 21 очко</b>\n\n<b>Карты дилера:</b> {' '.join(map(str, bj['dealer']))} (сумма {dealer_sum})\n" \
               f"<b>Ваши карты:</b> {' '.join(map(str, bj['player']))} (сумма {bj['player_sum']})\n\n{result}\n" \
               f"<code>Seed раунда: {bj['seed']}</code>"
        await query.edit_message_text(text, parse_mode=ParseMode.HTML)
        context.user_data.pop('blackjack', None)
        context.user_data.pop('current_game', None)

# ---------- МУЛЬТИПЛЕЕРНЫЙ БЛЭКДЖЕК (БДЖ) ----------
async def blackjack_multi_start(update: Update, context: ContextTypes.DEFAULT_TYPE, bet: int):
    user_id = update.effective_user.id
    ensure_user_exists(user_id)
    chat_id = update.effective_chat.id
    if chat_id == user_id:
        await update.message.reply_text("❌ Игра доступна только в групповых чатах.")
        return

    if context.bot_data.get(f"bj_lobby_{chat_id}"):
        await update.message.reply_text("❌ В этом чате уже собирают игроков для блэкджека.")
        return

    balance = get_balance(user_id)
    if bet <= 0 or bet > balance:
        await update.message.reply_text("❌ Недостаточно средств или неверная ставка.")
        return

    update_balance(user_id, -bet)

    lobby = {
        'creator': user_id,
        'bet': bet,
        'players': [user_id],
        'chat_id': chat_id,
        'message_id': None,
        'timer_task': None,
        'paid': [user_id]
    }
    context.bot_data[f"bj_lobby_{chat_id}"] = lobby

    keyboard = [[InlineKeyboardButton("➕ Присоединиться", callback_data=f"bj_join_{chat_id}")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    msg = await update.message.reply_text(
        f"<b>🃏 Блэкджек (мультиплеер)</b>\n"
        f"Создатель: {update.effective_user.first_name}\n"
        f"Ставка: {bet:,} кредиксов\n"
        f"Игроков: 1/{BJ_MAX_PLAYERS} (минимум {BJ_MIN_PLAYERS})\n"
        f"⏳ Ожидание игроков {BJ_LOBBY_TIMEOUT} секунд...",
        parse_mode=ParseMode.HTML,
        reply_markup=reply_markup
    )
    lobby['message_id'] = msg.message_id

    async def lobby_timeout():
        await asyncio.sleep(BJ_LOBBY_TIMEOUT)
        await bj_start_game_if_ready(context, chat_id)

    task = asyncio.create_task(lobby_timeout())
    lobby['timer_task'] = task

async def bj_join_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    ensure_user_exists(user_id)
    data = query.data
    if not data.startswith("bj_join_"):
        return
    chat_id = int(data.split("_")[2])
    lobby = context.bot_data.get(f"bj_lobby_{chat_id}")
    if not lobby:
        await query.edit_message_text("❌ Лобби больше не существует.")
        return

    if user_id in lobby['players']:
        await query.answer("Вы уже в игре!", show_alert=True)
        return
    if len(lobby['players']) >= BJ_MAX_PLAYERS:
        await query.answer("Достигнуто максимальное количество игроков.", show_alert=True)
        return

    balance = get_balance(user_id)
    if balance < lobby['bet']:
        await query.answer("❌ Недостаточно средств для ставки.", show_alert=True)
        return

    lobby['players'].append(user_id)
    update_balance(user_id, -lobby['bet'])
    lobby.setdefault('paid', []).append(user_id)

    keyboard = [[InlineKeyboardButton("➕ Присоединиться", callback_data=f"bj_join_{chat_id}")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(
        f"<b>🃏 Блэкджек (мультиплеер)</b>\n"
        f"Создатель: {lobby['creator']}\n"
        f"Ставка: {lobby['bet']:,} кредиксов\n"
        f"Игроков: {len(lobby['players'])}/{BJ_MAX_PLAYERS} (минимум {BJ_MIN_PLAYERS})\n"
        f"⏳ Ожидание игроков...",
        parse_mode=ParseMode.HTML,
        reply_markup=reply_markup
    )

    if len(lobby['players']) >= BJ_MIN_PLAYERS:
        if lobby['timer_task']:
            lobby['timer_task'].cancel()
        await bj_start_game_if_ready(context, chat_id)

async def bj_start_game_if_ready(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    lobby = context.bot_data.get(f"bj_lobby_{chat_id}")
    if not lobby:
        return
    if len(lobby['players']) < BJ_MIN_PLAYERS:
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"❌ Не удалось набрать минимум {BJ_MIN_PLAYERS} игроков. Игра отменена, ставки возвращены."
        )
        for pid in lobby.get('paid', []):
            update_balance(pid, lobby['bet'])
        del context.bot_data[f"bj_lobby_{chat_id}"]
        return

    players = lobby['players']
    bet = lobby['bet']
    chat_id = lobby['chat_id']

    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=lobby['message_id'])
    except:
        pass

    game_id = f"bj_game_{chat_id}_{random.randint(1000,9999)}"
    game = {
        'players': players,
        'bet': bet,
        'chat_id': chat_id,
        'dealer_cards': [],
        'player_hands': {pid: {'cards': [], 'sum': 0, 'status': 'waiting'} for pid in players},
        'deck': list(range(2, 12)) * 4,
        'step': 'waiting_for_players',
        'current_player_index': 0,
        'message_ids': {},
        'finished': False  # флаг для предотвращения двойного завершения
    }
    random.shuffle(game['deck'])

    for pid in players:
        cards = [game['deck'].pop(), game['deck'].pop()]
        game['player_hands'][pid]['cards'] = cards
        game['player_hands'][pid]['sum'] = sum(cards)
        game['player_hands'][pid]['status'] = 'active' if sum(cards) < 21 else 'stand'

    game['dealer_cards'] = [game['deck'].pop(), game['deck'].pop()]
    game['dealer_sum'] = sum(game['dealer_cards'])

    context.bot_data[game_id] = game
    del context.bot_data[f"bj_lobby_{chat_id}"]

    # Отправляем личные сообщения и удаляем игроков, которым не удалось доставить
    failed_players = []
    for pid in players:
        hand = game['player_hands'][pid]
        try:
            if hand['status'] == 'active':
                keyboard = [
                    [InlineKeyboardButton("➕ Ещё", callback_data=f"bj_multi_hit_{game_id}_{pid}"),
                     InlineKeyboardButton("⏹ Хватит", callback_data=f"bj_multi_stand_{game_id}_{pid}")]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                msg = await context.bot.send_message(
                    chat_id=pid,
                    text=f"<b>🃏 Блэкджек (мультиплеер)</b>\n\n"
                         f"Ваши карты: {' '.join(map(str, hand['cards']))} (сумма {hand['sum']})\n"
                         f"Карта дилера: {game['dealer_cards'][0]} и ?\n\n"
                         f"Ожидайте хода...",
                    parse_mode=ParseMode.HTML,
                    reply_markup=reply_markup
                )
                game['message_ids'][pid] = msg.message_id
            else:
                await context.bot.send_message(
                    chat_id=pid,
                    text=f"<b>🃏 Блэкджек (мультиплеер)</b>\n\n"
                         f"Ваши карты: {' '.join(map(str, hand['cards']))} (сумма {hand['sum']}) — у вас 21! Ожидайте результатов.",
                    parse_mode=ParseMode.HTML
                )
        except Exception as e:
            logger.error(f"Не удалось отправить сообщение игроку {pid}: {e}")
            failed_players.append(pid)

    # Исключаем failed_players из игры, возвращаем ставки
    if failed_players:
        for pid in failed_players:
            update_balance(pid, bet)
            # удаляем из списка игроков и рук
            game['players'].remove(pid)
            del game['player_hands'][pid]
        if len(game['players']) < BJ_MIN_PLAYERS:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"❌ Недостаточно игроков после исключения тех, кому не удалось доставить сообщение. Игра отменена, ставки возвращены всем."
            )
            for pid in game['players']:
                update_balance(pid, bet)
            del context.bot_data[game_id]
            return

    await context.bot.send_message(
        chat_id=chat_id,
        text=f"<b>🃏 Игра началась!</b>\nУчаствуют: {len(game['players'])} игроков.\nКаждому отправлены личные сообщения для хода.",
        parse_mode=ParseMode.HTML
    )

    async def game_timeout():
        await asyncio.sleep(120)
        await bj_multi_finish_round(context, game_id)

    game['timer_task'] = asyncio.create_task(game_timeout())

async def bj_multi_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    ensure_user_exists(user_id)
    data = query.data

    if data.startswith("bj_multi_hit_"):
        parts = data.split('_')
        game_id = parts[3]
        pid = int(parts[4])
        if user_id != pid:
            await query.answer("Это не ваша игра!", show_alert=True)
            return
        game = context.bot_data.get(game_id)
        if not game:
            await query.edit_message_text("Игра устарела.")
            return
        if game.get('finished'):
            await query.edit_message_text("Игра уже завершена.")
            return
        hand = game['player_hands'][pid]
        if hand['status'] != 'active':
            await query.answer("Вы уже закончили ход.", show_alert=True)
            return
        if not game['deck']:
            game['deck'] = list(range(2, 12)) * 4
            random.shuffle(game['deck'])
        card = game['deck'].pop()
        hand['cards'].append(card)
        hand['sum'] += card
        if hand['sum'] > 21:
            hand['status'] = 'bust'
            text = f"<b>🃏 Перебор!</b> Ваши карты: {' '.join(map(str, hand['cards']))} (сумма {hand['sum']})"
            await query.edit_message_text(text, parse_mode=ParseMode.HTML)
        elif hand['sum'] == 21:
            hand['status'] = 'stand'
            text = f"<b>🃏 21!</b> Ваши карты: {' '.join(map(str, hand['cards']))} (сумма 21)"
            await query.edit_message_text(text, parse_mode=ParseMode.HTML)
        else:
            keyboard = [
                [InlineKeyboardButton("➕ Ещё", callback_data=f"bj_multi_hit_{game_id}_{pid}"),
                 InlineKeyboardButton("⏹ Хватит", callback_data=f"bj_multi_stand_{game_id}_{pid}")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(
                f"<b>🃏 Блэкджек (мультиплеер)</b>\n\n"
                f"Ваши карты: {' '.join(map(str, hand['cards']))} (сумма {hand['sum']})\n"
                f"Карта дилера: {game['dealer_cards'][0]} и ?\n\n"
                f"Выберите действие:",
                parse_mode=ParseMode.HTML,
                reply_markup=reply_markup
            )
        all_done = all(v['status'] in ('stand', 'bust') for v in game['player_hands'].values())
        if all_done:
            if game.get('timer_task'):
                game['timer_task'].cancel()
            await bj_multi_finish_round(context, game_id)

    elif data.startswith("bj_multi_stand_"):
        parts = data.split('_')
        game_id = parts[3]
        pid = int(parts[4])
        if user_id != pid:
            await query.answer("Это не ваша игра!", show_alert=True)
            return
        game = context.bot_data.get(game_id)
        if not game:
            await query.edit_message_text("Игра устарела.")
            return
        if game.get('finished'):
            await query.edit_message_text("Игра уже завершена.")
            return
        hand = game['player_hands'][pid]
        if hand['status'] != 'active':
            await query.answer("Вы уже закончили ход.", show_alert=True)
            return
        hand['status'] = 'stand'
        await query.edit_message_text(
            f"<b>🃏 Вы остановились.</b> Ваши карты: {' '.join(map(str, hand['cards']))} (сумма {hand['sum']})",
            parse_mode=ParseMode.HTML
        )
        all_done = all(v['status'] in ('stand', 'bust') for v in game['player_hands'].values())
        if all_done:
            if game.get('timer_task'):
                game['timer_task'].cancel()
            await bj_multi_finish_round(context, game_id)

async def bj_multi_finish_round(context: ContextTypes.DEFAULT_TYPE, game_id: str):
    game = context.bot_data.get(game_id)
    if not game or game.get('finished'):
        return
    game['finished'] = True

    dealer_sum = game['dealer_sum']
    while dealer_sum < 17:
        if not game['deck']:
            game['deck'] = list(range(2, 12)) * 4
            random.shuffle(game['deck'])
        card = game['deck'].pop()
        dealer_sum += card
        game['dealer_cards'].append(card)
    game['dealer_sum'] = dealer_sum

    results = []
    winners = []
    push = []
    losers = []
    for pid in game['players']:
        hand = game['player_hands'][pid]
        if hand['status'] == 'bust':
            losers.append(pid)
            results.append(f"🔴 Игрок {pid} — перебор, проигрыш")
        else:
            if dealer_sum > 21 or hand['sum'] > dealer_sum:
                winners.append(pid)
                results.append(f"🟢 Игрок {pid} — {hand['sum']} (выигрыш)")
            elif hand['sum'] == dealer_sum:
                push.append(pid)
                results.append(f"⚪ Игрок {pid} — {hand['sum']} (ничья, возврат)")
            else:
                losers.append(pid)
                results.append(f"🔴 Игрок {pid} — {hand['sum']} (проигрыш)")

    total_bank = len(game['players']) * game['bet']
    for pid in push:
        update_balance(pid, game['bet'])
        add_game_stat(pid, 0, 0)
        total_bank -= game['bet']
    if winners:
        win_per_winner = total_bank // len(winners)
        for pid in winners:
            update_balance(pid, win_per_winner)
            add_game_stat(pid, win_per_winner, 0)

    result_text = f"<b>🃏 Итоги игры</b>\n\nКарты дилера: {' '.join(map(str, game['dealer_cards']))} (сумма {dealer_sum})\n\n"
    result_text += "\n".join(results)
    await context.bot.send_message(chat_id=game['chat_id'], text=result_text, parse_mode=ParseMode.HTML)

    del context.bot_data[game_id]

# ---------- СУНДУКИ ----------
async def chests_start(update: Update, context: ContextTypes.DEFAULT_TYPE, bet: int):
    user_id = update.effective_user.id
    ensure_user_exists(user_id)
    balance = get_balance(user_id)
    if bet <= 0 or bet > balance:
        await update.message.reply_text("❌ Недостаточно средств или неверная ставка.")
        return

    seed, fair_hash = generate_fair_hash()
    context.user_data['fair_seed'] = seed
    context.user_data['fair_hash'] = fair_hash

    update_balance(user_id, -bet)

    winning_chest = random.randint(0, 2)  # 0,1,2 - случайный сундук
    context.user_data['chests'] = {
        'winning': winning_chest,
        'bet': bet,
        'hash': fair_hash,
        'seed': seed,
        'user_id': user_id
    }
    context.user_data['current_game'] = 'chests'
    context.user_data['game_msg_count'] = 0

    keyboard = [
        [
            InlineKeyboardButton("📦 1", callback_data="chest_0"),
            InlineKeyboardButton("📦 2", callback_data="chest_1"),
            InlineKeyboardButton("📦 3", callback_data="chest_2")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        f"<b>📦 Игра Сундуки!</b>\nВыберите один из трёх сундуков. В одном из них ключ. Угадаете — получите x3!\n"
        f"<code>Хэш раунда: {fair_hash}</code>",
        parse_mode=ParseMode.HTML,
        reply_markup=reply_markup
    )

async def chests_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data
    if not data.startswith("chest_"):
        return
    chest_data = context.user_data.get('chests')
    if not chest_data:
        await query.edit_message_text("Игра не найдена.")
        return
    if chest_data.get('user_id') != user_id:
        await query.answer("Это не ваша игра!", show_alert=True)
        return
    context.user_data['game_msg_count'] = 0

    chosen = int(data.split("_")[1])
    winning = chest_data['winning']
    bet = chest_data['bet']

    if chosen == winning:
        win = bet * 3
        update_balance(user_id, win)
        add_game_stat(user_id, win, 0)
        text = f"🎉 <b>Вы угадали! В сундуке {chosen+1} был ключ!</b>\n💰 Выигрыш: {win:,} кредиксов (x3)!\n"
    else:
        add_game_stat(user_id, 0, bet)
        text = f"💔 <b>Вы не угадали. Ключ был в сундуке {winning+1}.</b>\n❌ Проигрыш: {bet:,} кредиксов.\n"

    text += f"<code>Seed раунда: {chest_data['seed']}</code>"
    await query.edit_message_text(text, parse_mode=ParseMode.HTML)
    context.user_data.pop('chests', None)
    context.user_data.pop('current_game', None)

# ---------- ЗОЛОТО ----------
async def gold_start(update: Update, context: ContextTypes.DEFAULT_TYPE, bet: int):
    user_id = update.effective_user.id
    ensure_user_exists(user_id)
    balance = get_balance(user_id)
    if bet <= 0 or bet > balance:
        await update.message.reply_text("❌ Недостаточно средств или неверная ставка.")
        return

    seed, fair_hash = generate_fair_hash()
    context.user_data['fair_seed'] = seed
    context.user_data['fair_hash'] = fair_hash

    update_balance(user_id, -bet)

    mines = [random.randint(0, 1) for _ in range(len(GOLD_MULTIPLIERS))]

    context.user_data['gold'] = {
        'mines': mines,
        'level': 0,
        'bet': bet,
        'hash': fair_hash,
        'seed': seed,
        'user_id': user_id
    }
    context.user_data['current_game'] = 'gold'
    context.user_data['game_msg_count'] = 0

    await show_gold_field(update, context)

async def show_gold_field(update: Update, context: ContextTypes.DEFAULT_TYPE):
    gold = context.user_data.get('gold')
    if not gold:
        # Игра уже удалена, просто выходим
        return
    level = gold['level']
    total_levels = len(GOLD_MULTIPLIERS)
    multiplier = GOLD_MULTIPLIERS[level] if level < total_levels else GOLD_MULTIPLIERS[-1]

    text = f"<b>💌 Золото 💌</b>\n"
    text += "×××××××××××××××××××××\n"
    text += f"⚡ Ставка: {gold['bet']:,} кредиксов\n\n"

    for i in range(level):
        mult = GOLD_MULTIPLIERS[i]
        text += f"| ✅ | ✅ | Сумма ({mult}x)\n"
    if level < total_levels:
        text += f"| ❓ | ❓ | Сумма ({multiplier}x)\n"
    for i in range(level + 1, total_levels):
        mult = GOLD_MULTIPLIERS[i]
        text += f"| ❓ | ❓ | Сумма ({mult}x)\n"

    keyboard = [
        [
            InlineKeyboardButton("⬅️ Левая", callback_data="gold_left"),
            InlineKeyboardButton("➡️ Правая", callback_data="gold_right")
        ],
        [InlineKeyboardButton("💰 Забрать выигрыш", callback_data="gold_take")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    try:
        if update.callback_query:
            await update.callback_query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
        else:
            await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
    except Exception as e:
        logger.error(f"Ошибка при показе поля золота: {e}")
        # В случае ошибки попробуем отправить новое сообщение
        if update.callback_query:
            await context.bot.send_message(chat_id=update.callback_query.from_user.id, text=text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
        else:
            await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)

async def gold_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data

    gold = context.user_data.get('gold')
    if not gold:
        await query.edit_message_text("Игра не найдена.")
        return
    if gold.get('user_id') != user_id:
        await query.answer("Это не ваша игра!", show_alert=True)
        return
    context.user_data['game_msg_count'] = 0

    if data == "gold_take":
        level = gold['level']
        if level == 0:
            await query.edit_message_text("Вы ещё не прошли ни одного уровня.")
            return
        multiplier = GOLD_MULTIPLIERS[level - 1]
        win = int(gold['bet'] * multiplier)
        try:
            update_balance(user_id, win)
            add_game_stat(user_id, win, 0)
        except Exception as e:
            logger.error(f"Ошибка при обновлении баланса в золоте: {e}")
            await query.edit_message_text("Произошла ошибка при обработке выигрыша. Обратитесь к администратору.")
            context.user_data.pop('gold', None)
            context.user_data.pop('current_game', None)
            return
        text = f"✅ <b>Вы забрали выигрыш: {win:,} кредиксов (x{multiplier}).</b>\n"
        text += f"<code>Seed раунда: {gold['seed']}</code>"
        await query.edit_message_text(text, parse_mode=ParseMode.HTML)
        context.user_data.pop('gold', None)
        context.user_data.pop('current_game', None)
        return

    if data in ("gold_left", "gold_right"):
        level = gold['level']
        if level >= len(GOLD_MULTIPLIERS):
            await query.edit_message_text("Игра завершена.")
            return
        mine_pos = gold['mines'][level]
        choice = 0 if data == "gold_left" else 1
        if choice == mine_pos:
            await query.edit_message_text(
                f"💥 <b>БАХ! Вы наткнулись на мину на уровне {level+1}!</b>\n"
                f"❌ Проигрыш {gold['bet']:,} кредиксов.",
                parse_mode=ParseMode.HTML
            )
            try:
                add_game_stat(user_id, 0, gold['bet'])
            except Exception as e:
                logger.error(f"Ошибка при обновлении статистики в золоте: {e}")
            context.user_data.pop('gold', None)
            context.user_data.pop('current_game', None)
            return
        else:
            gold['level'] += 1
            if gold['level'] >= len(GOLD_MULTIPLIERS):
                multiplier = GOLD_MULTIPLIERS[-1]
                win = int(gold['bet'] * multiplier)
                try:
                    update_balance(user_id, win)
                    add_game_stat(user_id, win, 0)
                except Exception as e:
                    logger.error(f"Ошибка при обновлении баланса в золоте: {e}")
                    await query.edit_message_text("Произошла ошибка при обработке выигрыша. Обратитесь к администратору.")
                    context.user_data.pop('gold', None)
                    context.user_data.pop('current_game', None)
                    return
                text = f"🎉 <b>Поздравляем! Вы прошли все уровни!</b>\n"
                text += f"💰 Выигрыш: {win:,} кредиксов (x{multiplier})!\n"
                text += f"<code>Seed раунда: {gold['seed']}</code>"
                await query.edit_message_text(text, parse_mode=ParseMode.HTML)
                context.user_data.pop('gold', None)
                context.user_data.pop('current_game', None)
                return
            else:
                await show_gold_field(update, context)

# ---------- КРЕСТИКИ-НОЛИКИ ----------
async def xo_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if is_banned(user_id):
        await update.message.reply_text("❌ Вы забанены.")
        return
    ensure_user_exists(user_id)

    args = update.message.text.split()
    if len(args) != 2:
        await update.message.reply_text("❌ Использование: крестики @username")
        return

    target_username = args[1].lstrip('@')
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute("SELECT user_id FROM users WHERE LOWER(username) = ?", (target_username.lower(),))
        row = c.fetchone()
        if not row:
            await update.message.reply_text("❌ Пользователь не найден в базе.")
            return
        opponent_id = row[0]

    if opponent_id == user_id:
        await update.message.reply_text("❌ Нельзя играть с самим собой.")
        return

    # Проверяем, не занят ли оппонент в другой игре
    xo_active = context.bot_data.setdefault('xo_active_players', set())
    if opponent_id in xo_active or user_id in xo_active:
        await update.message.reply_text("❌ Один из игроков уже участвует в другой игре.")
        return

    game_id = f"xo_{user_id}_{opponent_id}_{random.randint(1000,9999)}"
    context.bot_data[game_id] = {
        'players': [user_id, opponent_id],
        'board': [' ']*9,
        'turn': user_id,
        'message_id': None,
        'chat_id': update.effective_chat.id
    }
    xo_active.add(user_id)
    xo_active.add(opponent_id)

    keyboard = [
        [InlineKeyboardButton("✅ Принять", callback_data=f"xo_accept_{game_id}"),
         InlineKeyboardButton("❌ Отклонить", callback_data=f"xo_decline_{game_id}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        f"@{target_username}, вас приглашают сыграть в крестики-нолики!",
        reply_markup=reply_markup
    )

async def xo_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = query.from_user.id
    xo_active = context.bot_data.get('xo_active_players', set())

    if data.startswith('xo_accept_'):
        game_id = data[10:]
        game = context.bot_data.get(game_id)
        if not game:
            await query.edit_message_text("Игра устарела или уже завершена.")
            return
        if user_id not in game['players']:
            await query.answer("Это не ваша игра!", show_alert=True)
            return

        game['started'] = True
        game['turn'] = game['players'][0]
        game['board'] = [' ']*9

        await show_xo_board(query, context, game_id, game)

    elif data.startswith('xo_decline_'):
        game_id = data[11:]
        game = context.bot_data.get(game_id)
        if game:
            # Удаляем игроков из активных
            for pid in game['players']:
                xo_active.discard(pid)
            await query.edit_message_text("❌ Игрок отклонил приглашение.")
            context.bot_data.pop(game_id, None)
        else:
            await query.edit_message_text("Игра уже неактуальна.")

    elif data.startswith('xo_move_'):
        parts = data.split('_')
        if len(parts) != 4:
            return
        game_id = parts[2]
        cell = int(parts[3])
        game = context.bot_data.get(game_id)
        if not game or not game.get('started'):
            await query.edit_message_text("Игра не найдена или не начата.")
            return
        if user_id not in game['players']:
            await query.answer("Это не ваша игра!", show_alert=True)
            return
        if game['turn'] != user_id:
            await query.answer("Сейчас не ваш ход!", show_alert=True)
            return

        symbol = 'X' if user_id == game['players'][0] else 'O'
        if game['board'][cell] != ' ':
            await query.answer("Эта клетка уже занята!", show_alert=True)
            return

        game['board'][cell] = symbol
        win_combinations = [
            [0,1,2], [3,4,5], [6,7,8],
            [0,3,6], [1,4,7], [2,5,8],
            [0,4,8], [2,4,6]
        ]
        winner = None
        for combo in win_combinations:
            if game['board'][combo[0]] == game['board'][combo[1]] == game['board'][combo[2]] != ' ':
                winner = game['board'][combo[0]]
                break
        if winner:
            winner_id = game['players'][0] if winner == 'X' else game['players'][1]
            # Удаляем игроков из активных
            for pid in game['players']:
                xo_active.discard(pid)
            await query.edit_message_text(f"🏆 Игрок {winner_id} победил!")
            context.bot_data.pop(game_id, None)
            return

        if ' ' not in game['board']:
            for pid in game['players']:
                xo_active.discard(pid)
            await query.edit_message_text("🤝 Ничья!")
            context.bot_data.pop(game_id, None)
            return

        game['turn'] = game['players'][1] if user_id == game['players'][0] else game['players'][0]
        await show_xo_board(query, context, game_id, game)

async def show_xo_board(query, context, game_id, game):
    board = game['board']
    keyboard = []
    for i in range(0, 9, 3):
        row = []
        for j in range(3):
            cell = i+j
            text = board[cell] if board[cell] != ' ' else str(cell+1)
            row.append(InlineKeyboardButton(text, callback_data=f"xo_move_{game_id}_{cell}"))
        keyboard.append(row)
    reply_markup = InlineKeyboardMarkup(keyboard)
    current_player_id = game['turn']
    try:
        current_player_name = (await context.bot.get_chat(current_player_id)).first_name
    except:
        current_player_name = str(current_player_id)
    await query.edit_message_text(
        f"Крестики-нолики\nХод игрока: {current_player_name} ({'X' if current_player_id == game['players'][0] else 'O'})",
        reply_markup=reply_markup
    )

# ---------- ИСТОРИЯ И ОТМЕНА ----------
async def history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    current_game = context.user_data.get('current_game')
    if not current_game:
        await update.message.reply_text("У вас нет активных игр.")
        return

    game_data = None
    game_name = ""
    if current_game == 'mines' and 'mines' in context.user_data:
        game_data = context.user_data['mines']
        game_name = "Мины"
    elif current_game == 'tower' and 'tower' in context.user_data:
        game_data = context.user_data['tower']
        game_name = "Башня"
    elif current_game == 'pyramid' and 'pyramid' in context.user_data:
        game_data = context.user_data['pyramid']
        game_name = "Пирамида"
    elif current_game == 'blackjack' and 'blackjack' in context.user_data:
        game_data = context.user_data['blackjack']
        game_name = "21 очко"
    elif current_game == 'chests' and 'chests' in context.user_data:
        game_data = context.user_data['chests']
        game_name = "Сундуки"
    elif current_game == 'gold' and 'gold' in context.user_data:
        game_data = context.user_data['gold']
        game_name = "Золото"

    if not game_data:
        await update.message.reply_text("Активная игра не найдена.")
        return

    text = f"<b>🎮 У вас есть активная игра: {game_name}</b>\n"
    if current_game == 'mines':
        text += f"Открыто ячеек: {game_data['step']} из {game_data['max_step']}\n"
    elif current_game == 'tower':
        text += f"Пройдено этажей: {game_data['floor']} из {TOWER_FLOORS}\n"
    elif current_game == 'pyramid':
        text += f"Пройдено этажей: {game_data['floor']} из {PYRAMID_FLOORS}, текущий множитель: {game_data['multiplier']}x\n"
    elif current_game == 'blackjack':
        text += f"Ваши карты: {' '.join(map(str, game_data['player']))} (сумма {game_data['player_sum']})\n"
    elif current_game == 'chests':
        text += f"Ставка: {game_data['bet']}\n"
    elif current_game == 'gold':
        text += f"Пройдено уровней: {game_data['level']} из {len(GOLD_MULTIPLIERS)}\n"

    keyboard = [[InlineKeyboardButton("▶️ Перейти к игре", callback_data="continue_game")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)

async def continue_game_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    if query.data != "continue_game":
        return
    current_game = context.user_data.get('current_game')
    if not current_game:
        await query.edit_message_text("Активная игра не найдена.")
        return

    context.user_data['game_msg_count'] = 0  # сброс при возврате

    if current_game == 'mines':
        await show_mines_field(update, context)
    elif current_game == 'tower':
        await show_tower_floor(update, context)
    elif current_game == 'pyramid':
        await show_pyramid_floor(update, context)
    elif current_game == 'blackjack':
        bj = context.user_data['blackjack']
        keyboard = [
            [InlineKeyboardButton("➕ Ещё", callback_data="bj_hit"),
             InlineKeyboardButton("⏹ Хватит", callback_data="bj_stand")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            f"<b>🃏 21 очко</b>\n\n"
            f"<b>Ваши карты:</b> {' '.join(map(str, bj['player']))} (сумма {bj['player_sum']})\n"
            f"<b>Карта дилера:</b> {bj['dealer'][0]} и ?\n\n",
            parse_mode=ParseMode.HTML,
            reply_markup=reply_markup
        )
    elif current_game == 'chests':
        chests = context.user_data['chests']
        keyboard = [
            [
                InlineKeyboardButton("📦 1", callback_data="chest_0"),
                InlineKeyboardButton("📦 2", callback_data="chest_1"),
                InlineKeyboardButton("📦 3", callback_data="chest_2")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            f"<b>📦 Игра Сундуки!</b>\nВыберите один из трёх сундуков.\n"
            f"<code>Хэш раунда: {chests['hash']}</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=reply_markup
        )
    elif current_game == 'gold':
        await show_gold_field(update, context)

async def cancel_game(update: Update, context: ContextTypes.DEFAULT_TYPE, reason: str = "Отмена", force_return: bool = True):
    user_id = update.effective_user.id
    game = context.user_data.get('current_game')
    if not game:
        return False
    bet = None
    returned = False
    if game == 'mines' and 'mines' in context.user_data:
        bet = context.user_data['mines'].get('bet')
        if force_return and context.user_data['mines']['step'] == 0:
            returned = True
    elif game == 'tower' and 'tower' in context.user_data:
        bet = context.user_data['tower'].get('bet')
        if force_return and context.user_data['tower']['floor'] == 0:
            returned = True
    elif game == 'pyramid' and 'pyramid' in context.user_data:
        bet = context.user_data['pyramid'].get('bet')
        if force_return and context.user_data['pyramid']['floor'] == 0:
            returned = True
    elif game == 'blackjack' and 'blackjack' in context.user_data:
        bet = context.user_data['blackjack'].get('bet')
    elif game == 'chests' and 'chests' in context.user_data:
        bet = context.user_data['chests'].get('bet')
        if force_return:
            returned = True
    elif game == 'gold' and 'gold' in context.user_data:
        bet = context.user_data['gold'].get('bet')
        if force_return and context.user_data['gold']['level'] == 0:
            returned = True

    if bet and returned:
        update_balance(user_id, bet)
        await update.message.reply_text(f"⚡ {reason}. Ставка {bet:,} возвращена.")
    else:
        await update.message.reply_text(f"⚡ {reason}.")
    context.user_data.clear()
    return True

# ---------- ЭКОНОМИКА ----------
async def work(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ensure_user_exists(user_id)
    update_balance(user_id, 20)
    await update.message.reply_text("💼 Вы получили <b>20 кредиксов</b> за работу.", parse_mode=ParseMode.HTML)

async def hourly_bonus(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ensure_user_exists(user_id)
    now = datetime.now()
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute("SELECT last_hourly_bonus FROM users WHERE user_id = ?", (user_id,))
        row = c.fetchone()
        last = row[0] if row else None
        if last:
            try:
                last_time = datetime.fromisoformat(last)
            except:
                last_time = None
            if last_time and (now - last_time).total_seconds() < 3600:
                remaining = 3600 - (now - last_time).total_seconds()
                minutes = int(remaining // 60)
                seconds = int(remaining % 60)
                await update.message.reply_text(f"❌ Бонус можно получить раз в час. Осталось {minutes} мин {seconds} сек.")
                return
        update_balance(user_id, 2000)
        c.execute("UPDATE users SET last_hourly_bonus = ? WHERE user_id = ?", (now.isoformat(), user_id))
        conn.commit()
    if update.callback_query:
        await update.callback_query.edit_message_text("🎁 Вы получили <b>2000 кредиксов</b> (часовой бонус).", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text("🎁 Вы получили <b>2000 кредиксов</b> (часовой бонус).", parse_mode=ParseMode.HTML)

async def daily_bonus(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ensure_user_exists(user_id)
    now = datetime.now()
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute("SELECT last_daily_bonus FROM users WHERE user_id = ?", (user_id,))
        row = c.fetchone()
        last = row[0] if row else None
        if last:
            try:
                last_time = datetime.fromisoformat(last)
            except:
                last_time = None
            if last_time and (now - last_time).total_seconds() < 86400:
                remaining = 86400 - (now - last_time).total_seconds()
                hours = int(remaining // 3600)
                minutes = int((remaining % 3600) // 60)
                await update.message.reply_text(f"❌ Ежедневный бонус можно получить раз в день. Осталось {hours} ч {minutes} мин.")
                return
        # Убрана проверка описания
        update_balance(user_id, 10000)
        c.execute("UPDATE users SET last_daily_bonus = ? WHERE user_id = ?", (now.isoformat(), user_id))
        conn.commit()
    if update.callback_query:
        await update.callback_query.edit_message_text("🎁 Вы получили <b>10000 кредиксов</b> (ежедневный бонус).", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text("🎁 Вы получили <b>10000 кредиксов</b> (ежедневный бонус).", parse_mode=ParseMode.HTML)

async def give(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ensure_user_exists(user_id)
    text = update.message.text
    if update.message.reply_to_message:
        to_user = update.message.reply_to_message.from_user.id
        if to_user == user_id:
            await update.message.reply_text("❌ Нельзя перевести средства самому себе.")
            return
        parts = text.split()
        if len(parts) != 2:
            await update.message.reply_text("❌ Формат: Дать (сумма) в ответ на сообщение")
            return
        try:
            amount = parse_amount(parts[1], get_balance(user_id))
        except ValueError as e:
            await update.message.reply_text(f"❌ {e}")
            return
        ensure_user_exists(to_user)
    else:
        parts = text.split(maxsplit=2)
        if len(parts) != 3:
            await update.message.reply_text("❌ Формат: Дать (сумма) (юзернейм) или ответом на сообщение")
            return
        try:
            amount = parse_amount(parts[1], get_balance(user_id))
            username = parts[2].lstrip('@')
        except ValueError as e:
            await update.message.reply_text(f"❌ {e}")
            return
        with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
            c = conn.cursor()
            c.execute("SELECT user_id FROM users WHERE LOWER(username) = ?", (username.lower(),))
            row = c.fetchone()
            if not row:
                await update.message.reply_text("❌ Пользователь не найден в базе.")
                return
            to_user = row[0]
            if to_user == user_id:
                await update.message.reply_text("❌ Нельзя перевести средства самому себе.")
                return
    if amount <= 0:
        await update.message.reply_text("❌ Сумма должна быть положительной.")
        return
    balance = get_balance(user_id)
    if balance < amount:
        await update.message.reply_text("❌ Недостаточно средств.")
        return
    update_balance(user_id, -amount)
    update_balance(to_user, amount)
    await update.message.reply_text(f"✅ Вы перевели <b>{amount:,}</b> кредиксов пользователю.", parse_mode=ParseMode.HTML)

# ---------- АВТОСАЛОН И МАШИНЫ ----------
async def car_shop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute("SELECT car_id, name, price, rent_price, taxi_min_earn, taxi_max_earn, is_limited, stock, tip_min, tip_max FROM cars WHERE is_available=1")
        cars = c.fetchall()
    text = "<b>🚗 Автосалон:</b>\n\n"
    keyboard = []
    for car in cars:
        car_id, name, price, rent_price, min_earn, max_earn, is_limited, stock, tip_min, tip_max = car
        if is_limited:
            stock_info = f" (в наличии: {stock})" if stock > 0 else " (нет в наличии)"
            price_info = f"💰 Цена: {price:,} | 🚖 Доход такси: {min_earn:,}-{max_earn:,} + чаевые {tip_min:,}-{tip_max:,}"
        else:
            stock_info = ""
            price_info = f"💰 Цена: {price:,} | 🕒 Аренда: {rent_price:,}/24ч | 🚖 Доход такси: {min_earn:,}-{max_earn:,}"
        text += f"<b>{car_id}. {escape_html(name)}</b>{stock_info}\n{price_info}\n\n"
        if (is_limited and stock > 0) or not is_limited:
            keyboard.append([
                InlineKeyboardButton(f"Купить {name}", callback_data=f"buy_car_{car_id}"),
                InlineKeyboardButton(f"Арендовать {name}", callback_data=f"rent_car_{car_id}") if not is_limited else InlineKeyboardButton("❌ Аренда недоступна", callback_data="ignore")
            ])
    reply_markup = InlineKeyboardMarkup(keyboard)
    final_text = f"<blockquote>{text}</blockquote>"
    await update.message.reply_text(final_text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)

async def car_shop_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if data == "ignore":
        return
    if data.startswith("buy_car_"):
        car_id = int(data.split("_")[2])
        await buy_car(query, context, car_id)
    elif data.startswith("rent_car_"):
        car_id = int(data.split("_")[2])
        await rent_car(query, context, car_id)

async def buy_car(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE, car_id: int):
    user_id = query.from_user.id
    ensure_user_exists(user_id)
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        # Используем транзакцию с IMMEDIATE для избежания race condition
        conn.isolation_level = 'IMMEDIATE'
        c = conn.cursor()
        c.execute("SELECT price, is_limited, stock, name FROM cars WHERE car_id = ? AND is_available=1", (car_id,))
        row = c.fetchone()
        if not row:
            await query.edit_message_text("❌ Машина не найдена.")
            return
        price, is_limited, stock, name = row
        if is_limited and stock <= 0:
            await query.edit_message_text("❌ Эта лимитированная машина закончилась.")
            return
        balance = get_balance(user_id)
        if balance < price:
            await query.edit_message_text("❌ Недостаточно средств.")
            return
        update_balance(user_id, -price)
        update_quest_progress(user_id, 7, price)
        c.execute('''INSERT INTO user_cars (user_id, car_id, is_rented) VALUES (?,?,0)''', (user_id, car_id))
        if is_limited:
            c.execute("UPDATE cars SET stock = stock - 1 WHERE car_id = ?", (car_id,))
        c.execute("SELECT active_car_id FROM users WHERE user_id = ?", (user_id,))
        active = c.fetchone()[0]
        if not active or active == 0:
            c.execute("UPDATE users SET active_car_id = ? WHERE user_id = ?", (car_id, user_id))
        conn.commit()
    await query.edit_message_text(f"✅ Вы купили машину <b>{escape_html(name)}</b>! Она добавлена в гараж.", parse_mode=ParseMode.HTML)

async def rent_car(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE, car_id: int):
    user_id = query.from_user.id
    ensure_user_exists(user_id)
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute("SELECT rent_price, is_limited FROM cars WHERE car_id = ? AND is_available=1", (car_id,))
        row = c.fetchone()
        if not row:
            await query.edit_message_text("❌ Машина не найдена.")
            return
        rent_price, is_limited = row
        if is_limited:
            await query.edit_message_text("❌ Лимитированные машины нельзя арендовать.")
            return
        balance = get_balance(user_id)
        if balance < rent_price:
            await query.edit_message_text("❌ Недостаточно средств.")
            return
        now = datetime.now()
        expires = now + timedelta(hours=24)
        update_balance(user_id, -rent_price)
        update_quest_progress(user_id, 7, rent_price)
        c.execute('''INSERT INTO user_cars (user_id, car_id, is_rented, expires) VALUES (?,?,1,?)''',
                  (user_id, car_id, expires.isoformat()))
        c.execute("SELECT active_car_id FROM users WHERE user_id = ?", (user_id,))
        active = c.fetchone()[0]
        if not active or active == 0:
            c.execute("UPDATE users SET active_car_id = ? WHERE user_id = ?", (car_id, user_id))
        conn.commit()
    await query.edit_message_text(f"✅ Вы арендовали машину на 24 часа. ID {car_id}")

async def my_cars(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ensure_user_exists(user_id)
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute("SELECT active_car_id FROM users WHERE user_id = ?", (user_id,))
        active_id = c.fetchone()[0]
        c.execute('''SELECT uc.id, uc.car_id, c.name, uc.is_rented, uc.expires, c.price
                     FROM user_cars uc
                     JOIN cars c ON uc.car_id = c.car_id
                     WHERE uc.user_id = ?''', (user_id,))
        cars = c.fetchall()
    if not cars:
        await update.message.reply_text("У вас нет машин. Купите или арендуйте в автосалоне.")
        return

    text = "<b>🚗 Ваши машины:</b>\n\n"
    keyboard = []
    for record in cars:
        uc_id, car_id, name, is_rented, expires, price = record
        status = " (аренда)" if is_rented else " (в собственности)"
        if is_rented and expires:
            try:
                exp = datetime.fromisoformat(expires)
            except:
                exp = None
            if exp and exp < datetime.now():
                status += " [истекла]"
            elif exp:
                time_left = exp - datetime.now()
                hours = time_left.seconds // 3600
                minutes = (time_left.seconds % 3600) // 60
                status += f" осталось {hours}ч {minutes}м"
        active_mark = " ✅Активная" if car_id == active_id else ""
        text += f"<b>{escape_html(name)}</b>{status}{active_mark}\n"
        row = []
        if car_id == active_id:
            row.append(InlineKeyboardButton(f"✅ {name} (активна)", callback_data="car_noop"))
        else:
            row.append(InlineKeyboardButton(f"⭐ Сделать активной", callback_data=f"car_activate_{uc_id}"))
        if not is_rented:
            row.append(InlineKeyboardButton(f"💰 Продать за {price//2:,}", callback_data=f"car_sell_{uc_id}"))
        keyboard.append(row)
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)

async def my_car(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ensure_user_exists(user_id)
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute("SELECT active_car_id FROM users WHERE user_id = ?", (user_id,))
        active_id = c.fetchone()[0]
        if not active_id or active_id == 0:
            await update.message.reply_text("У вас нет активной машины. Выберите в 'мои машины'.")
            return
        c.execute('''SELECT uc.car_id, c.name, uc.is_rented, uc.expires, c.taxi_min_earn, c.taxi_max_earn, c.is_limited, c.tip_min, c.tip_max
                     FROM user_cars uc
                     JOIN cars c ON uc.car_id = c.car_id
                     WHERE uc.user_id = ? AND uc.car_id = ?''', (user_id, active_id))
        row = c.fetchone()
        if not row:
            c.execute("UPDATE users SET active_car_id = 0 WHERE user_id = ?", (user_id,))
            conn.commit()
            await update.message.reply_text("Активная машина не найдена, попробуйте выбрать другую.")
            return
        car_id, name, is_rented, expires, min_earn, max_earn, is_limited, tip_min, tip_max = row
        if is_rented and expires:
            try:
                exp = datetime.fromisoformat(expires)
            except:
                exp = None
            if exp and exp < datetime.now():
                c.execute("DELETE FROM user_cars WHERE user_id = ? AND car_id = ?", (user_id, active_id))
                c.execute("UPDATE users SET active_car_id = 0 WHERE user_id = ?", (user_id,))
                conn.commit()
                await update.message.reply_text("Срок аренды истек, машина изъята.")
                return
            elif exp:
                time_left = exp - datetime.now()
                hours = time_left.seconds // 3600
                minutes = (time_left.seconds % 3600) // 60
                rent_info = f" (аренда, осталось {hours} ч {minutes} мин)"
            else:
                rent_info = ""
        else:
            rent_info = ""
        tip_text = f" + чаевые {tip_min:,}-{tip_max:,}" if is_limited else ""
        await update.message.reply_text(f"Ваша активная машина: <b>{escape_html(name)}</b>{rent_info}\nДоход в такси: {min_earn:,}-{max_earn:,}{tip_text}", parse_mode=ParseMode.HTML)

async def car_activate_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data
    if data.startswith("car_activate_"):
        uc_id = int(data.split("_")[2])
        with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
            c = conn.cursor()
            c.execute("SELECT car_id FROM user_cars WHERE id = ? AND user_id = ?", (uc_id, user_id))
            row = c.fetchone()
            if not row:
                await query.edit_message_text("Ошибка: машина не найдена.")
                return
            car_id = row[0]
            c.execute("SELECT is_rented, expires FROM user_cars WHERE id = ?", (uc_id,))
            r = c.fetchone()
            if r and r[0] == 1 and r[1]:
                try:
                    exp = datetime.fromisoformat(r[1])
                except:
                    exp = None
                if exp and exp < datetime.now():
                    await query.edit_message_text("Нельзя активировать: срок аренды истёк.")
                    return
            c.execute("UPDATE users SET active_car_id = ? WHERE user_id = ?", (car_id, user_id))
            conn.commit()
        await query.edit_message_text("✅ Активная машина обновлена.")

async def car_sell_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data
    if data.startswith("car_sell_"):
        uc_id = int(data.split("_")[2])
        with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
            c = conn.cursor()
            c.execute('''SELECT uc.car_id, c.price, uc.is_rented, uc.car_id 
                         FROM user_cars uc
                         JOIN cars c ON uc.car_id = c.car_id
                         WHERE uc.id = ? AND uc.user_id = ?''', (uc_id, user_id))
            row = c.fetchone()
            if not row:
                await query.edit_message_text("Ошибка: машина не найдена.")
                return
            car_id, price, is_rented, _ = row
            if is_rented:
                await query.edit_message_text("Арендованные машины продавать нельзя.")
                return
            sell_price = price // 2
            c.execute("DELETE FROM user_cars WHERE id = ?", (uc_id,))
            c.execute("SELECT active_car_id FROM users WHERE user_id = ?", (user_id,))
            active = c.fetchone()[0]
            if active == car_id:
                c.execute("UPDATE users SET active_car_id = 0 WHERE user_id = ?", (user_id,))
            conn.commit()
        update_balance(user_id, sell_price)
        await query.edit_message_text(f"✅ Вы продали машину за {sell_price:,} кредиксов.")

async def car_noop_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("Это ваша текущая активная машина", show_alert=True)

async def taxi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ensure_user_exists(user_id)
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute("SELECT active_car_id FROM users WHERE user_id = ?", (user_id,))
        active_id = c.fetchone()[0]
        if not active_id or active_id == 0:
            await update.message.reply_text("У вас нет активной машины. Выберите в 'мои машины'.")
            return
        c.execute('''SELECT uc.is_rented, uc.expires, c.taxi_min_earn, c.taxi_max_earn, c.is_limited, c.tip_min, c.tip_max
                     FROM user_cars uc
                     JOIN cars c ON uc.car_id = c.car_id
                     WHERE uc.user_id = ? AND uc.car_id = ?''', (user_id, active_id))
        row = c.fetchone()
        if not row:
            c.execute("UPDATE users SET active_car_id = 0 WHERE user_id = ?", (user_id,))
            conn.commit()
            await update.message.reply_text("Активная машина не найдена, попробуйте выбрать другую.")
            return
        is_rented, expires, min_earn, max_earn, is_limited, tip_min, tip_max = row
        if is_rented and expires:
            try:
                exp = datetime.fromisoformat(expires)
            except:
                exp = None
            if exp and exp < datetime.now():
                c.execute("DELETE FROM user_cars WHERE user_id = ? AND car_id = ?", (user_id, active_id))
                c.execute("UPDATE users SET active_car_id = 0 WHERE user_id = ?", (user_id,))
                conn.commit()
                await update.message.reply_text("Срок аренды истек, машина изъята.")
                return
        c.execute("SELECT last_taxi FROM users WHERE user_id = ?", (user_id,))
        last_taxi = c.fetchone()[0]
        now = datetime.now()
        if last_taxi:
            try:
                last = datetime.fromisoformat(last_taxi)
            except:
                last = None
            if last and (now - last).total_seconds() < 3600:
                remaining = 3600 - (now - last).total_seconds()
                minutes = int(remaining // 60)
                await update.message.reply_text(f"❌ Такси можно вызывать раз в час. Осталось {minutes} мин.")
                return
        earn = random.randint(min_earn, max_earn)
        tip = 0
        if is_limited:
            tip = random.randint(tip_min, tip_max)
            earn += tip
            tip_text = f" (включая чаевые {tip})"
        else:
            tip_text = ""
        update_balance(user_id, earn)
        c.execute("UPDATE users SET last_taxi = ? WHERE user_id = ?", (now.isoformat(), user_id))
        conn.commit()
        update_quest_progress(user_id, 3, earn)
        update_quest_progress(user_id, 9, earn)
    await update.message.reply_text(f"🚕 Вы поработали в такси и заработали <b>{earn:,}</b> кредиксов{tip_text}.", parse_mode=ParseMode.HTML)

# ---------- КОНКУРС И СОТРУДНИЧЕСТВО ----------
async def contest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "<b>🎉 Конкурс на автомобиль Bentley уже идёт!</b>\n\n"
        "Присоединяйтесь к конкурсу в телеграмм канале: https://t.me/werdoxz_wiinere",
        parse_mode=ParseMode.HTML
    )

async def cooperation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "<b>🤝 Сотрудничество</b>\n\n"
        "Для сотрудничества напишите @what_lova\n"
        "Нам нужны люди, которые будут раздавать валюту в своём телеграмм канале!\n"
        "О зарплате разберёмся, пишите!",
        parse_mode=ParseMode.HTML
    )

# ---------- АДМИН-КОМАНДЫ (прямые, для удобства) ----------
async def ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id) and user_id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ Недостаточно прав.")
        return
    args = context.args
    if not args:
        await update.message.reply_text("❌ Введите ID или @username пользователя.")
        return
    target = args[0]
    target_id = extract_target_id(target)
    if not target_id:
        await update.message.reply_text("❌ Пользователь не найден.")
        return
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute("UPDATE users SET is_banned = 1 WHERE user_id = ?", (target_id,))
        conn.commit()
    await update.message.reply_text(f"✅ Пользователь {target_id} забанен.")
    log_admin_action(update.effective_user.id, "ban", target_id=target_id)

async def unban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id) and user_id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ Недостаточно прав.")
        return
    args = context.args
    if not args:
        await update.message.reply_text("❌ Введите ID или @username пользователя.")
        return
    target = args[0]
    target_id = extract_target_id(target)
    if not target_id:
        await update.message.reply_text("❌ Пользователь не найден.")
        return
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute("UPDATE users SET is_banned = 0 WHERE user_id = ?", (target_id,))
        conn.commit()
    await update.message.reply_text(f"✅ Пользователь {target_id} разбанен.")
    log_admin_action(update.effective_user.id, "unban", target_id=target_id)

async def give_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id) and user_id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ Недостаточно прав.")
        return
    args = context.args
    if len(args) != 2:
        await update.message.reply_text("❌ Формат: /give ID или @username сумма")
        return
    target = args[0]
    try:
        amount = parse_amount(args[1], None)
    except ValueError as e:
        await update.message.reply_text(f"❌ {e}")
        return
    target_id = extract_target_id(target)
    if not target_id:
        await update.message.reply_text("❌ Пользователь не найден.")
        return
    if not check_admin_limit(user_id, amount):
        await update.message.reply_text("❌ Вы не можете выдать сумму больше вашего лимита.")
        return
    update_balance(target_id, amount)
    await update.message.reply_text(f"✅ Выдано {amount:,} кредиксов пользователю {target_id}.")
    log_admin_action(user_id, "give", target_id=target_id, amount=amount)

async def take_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id) and user_id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ Недостаточно прав.")
        return
    args = context.args
    if len(args) != 2:
        await update.message.reply_text("❌ Формат: /take ID или @username сумма")
        return
    target = args[0]
    try:
        amount = parse_amount(args[1], None)
    except ValueError as e:
        await update.message.reply_text(f"❌ {e}")
        return
    target_id = extract_target_id(target)
    if not target_id:
        await update.message.reply_text("❌ Пользователь не найден.")
        return
    balance = get_balance(target_id)
    if balance < amount:
        await update.message.reply_text("❌ У пользователя недостаточно средств.")
        return
    update_balance(target_id, -amount)
    await update.message.reply_text(f"✅ Списано {amount:,} кредиксов у пользователя {target_id}.")
    log_admin_action(user_id, "take", target_id=target_id, amount=amount)

async def notify(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id) and user_id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ Недостаточно прав.")
        return
    args = context.args
    if len(args) < 1:
        await update.message.reply_text("❌ Введите текст уведомления.")
        return
    message = ' '.join(args)
    await update.message.reply_text("⏳ Начинаю рассылку уведомлений...")
    asyncio.create_task(broadcast_notification(context.bot, message, user_id))

async def create_promo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id) and user_id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ Недостаточно прав.")
        return
    args = context.args
    if len(args) < 3:
        await update.message.reply_text("❌ Формат: /create_promo сумма кол-во [код] текст")
        return
    try:
        amount = parse_amount(args[0], None)
        max_uses = int(args[1])
    except ValueError as e:
        await update.message.reply_text(f"❌ {e}")
        return

    if len(args) >= 4:
        code = args[2].upper()
        promo_text = ' '.join(args[3:])
    else:
        code = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=8))
        promo_text = ' '.join(args[2:])

    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute("SELECT code FROM promocodes WHERE code = ?", (code,))
        if c.fetchone():
            await update.message.reply_text(f"❌ Промокод <b>{code}</b> уже существует. Придумайте другой.", parse_mode=ParseMode.HTML)
            return
        c.execute('''INSERT INTO promocodes (code, amount, max_uses, message) VALUES (?,?,?,?)''',
                  (code, amount, max_uses, promo_text))
        conn.commit()
    await update.message.reply_text(
        f"✅ Промокод создан: <b>{code}</b>\nСумма: {amount:,}, активаций: {max_uses}\nСообщение: {promo_text}",
        parse_mode=ParseMode.HTML
    )

async def set_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id) and user_id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ Недостаточно прав.")
        return
    args = context.args
    if len(args) < 2:
        await update.message.reply_text("❌ Формат: /setstatus user_id статус")
        return
    target = args[0]
    status = ' '.join(args[1:])
    target_id = extract_target_id(target)
    if not target_id:
        await update.message.reply_text("❌ Пользователь не найден.")
        return
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute("UPDATE users SET status = ? WHERE user_id = ?", (status, target_id))
        conn.commit()
    await update.message.reply_text(f"✅ Статус пользователя {target_id} изменён на '{status}'.")
    log_admin_action(user_id, "setstatus", target_id=target_id)

async def set_description(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id) and user_id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ Недостаточно прав.")
        return
    args = context.args
    if len(args) < 2:
        await update.message.reply_text("❌ Формат: /setdesc user_id описание")
        return
    target = args[0]
    description = ' '.join(args[1:])
    target_id = extract_target_id(target)
    if not target_id:
        await update.message.reply_text("❌ Пользователь не найден.")
        return
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute("UPDATE users SET description = ? WHERE user_id = ?", (description, target_id))
        conn.commit()
    await update.message.reply_text(f"✅ Описание пользователя {target_id} изменено.")
    log_admin_action(user_id, "setdesc", target_id=target_id)

async def set_protection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id) and user_id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ Недостаточно прав.")
        return
    args = context.args
    if len(args) < 2:
        await update.message.reply_text("❌ Формат: /setprotection user_id тип_защиты")
        return
    target = args[0]
    protection = ' '.join(args[1:])
    target_id = extract_target_id(target)
    if not target_id:
        await update.message.reply_text("❌ Пользователь не найден.")
        return
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute("UPDATE users SET protection = ? WHERE user_id = ?", (protection, target_id))
        conn.commit()
    await update.message.reply_text(f"✅ Защита пользователя {target_id} изменена на '{protection}'.")
    log_admin_action(user_id, "setprotection", target_id=target_id)

async def activate_promo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ensure_user_exists(user_id)
    text = update.message.text
    parts = text.split()
    if len(parts) != 2:
        await update.message.reply_text("❌ Формат: промо (код)")
        return
    code = parts[1].upper()
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute("SELECT 1 FROM promo_activations WHERE user_id = ? AND code = ?", (user_id, code))
        if c.fetchone():
            await update.message.reply_text("❌ Вы уже активировали этот промокод.")
            return
        c.execute("SELECT amount, max_uses, used, message FROM promocodes WHERE code = ?", (code,))
        row = c.fetchone()
        if not row:
            await update.message.reply_text("❌ Промокод не найден.")
            return
        amount, max_uses, used, promo_message = row
        if used >= max_uses:
            await update.message.reply_text("❌ Промокод уже использован максимальное количество раз.")
            return
        update_balance(user_id, amount)
        c.execute("INSERT INTO promo_activations (user_id, code) VALUES (?,?)", (user_id, code))
        c.execute("UPDATE promocodes SET used = used + 1 WHERE code = ?", (code,))
        conn.commit()
    await update.message.reply_text(f"✅ Промокод активирован! Вы получили <b>{amount:,}</b> кредиксов.\n{promo_message}", parse_mode=ParseMode.HTML)

# ---------- ГЛАВНЫЙ ОБРАБОТЧИК ТЕКСТОВЫХ СООБЩЕНИЙ ----------
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not is_bot_enabled(context) and user_id not in ADMIN_IDS:
        await update.message.reply_text("🔌 Бот временно отключён администратором.")
        return

    if 'admin_action' in context.user_data:
        pass

    if is_banned(user_id):
        await update.message.reply_text("❌ Вы забанены.")
        return

    text = update.message.text.strip()
    if not text:
        return
    lower_text = text.lower()
    words = lower_text.split()
    if not words:
        return

    cmd = ''
    args_start = 0
    for i in range(1, len(words)+1):
        candidate = ' '.join(words[:i])
        if candidate in ALL_COMMANDS:
            cmd = candidate
            args_start = i
            break
    if not cmd:
        return

    if cmd == "отмена":
        await cancel_game(update, context, "Игра отменена")
        return

    if context.user_data.get('current_game'):
        msg_count = context.user_data.get('game_msg_count', 0) + 1
        context.user_data['game_msg_count'] = msg_count
        if msg_count > 3:
            game = context.user_data.get('current_game')
            force_return = False
            if game == 'mines':
                if context.user_data['mines']['step'] == 0:
                    force_return = True
            elif game == 'tower':
                if context.user_data['tower']['floor'] == 0:
                    force_return = True
            elif game == 'pyramid':
                if context.user_data['pyramid']['floor'] == 0:
                    force_return = True
            elif game == 'chests':
                force_return = True
            elif game == 'gold':
                if context.user_data['gold']['level'] == 0:
                    force_return = True
            if force_return:
                await cancel_game(update, context, "Превышено количество сообщений во время игры", force_return=True)
            else:
                # Не прерываем игру, если уже есть прогресс
                pass
        return

    if cmd in ("вб", "все"):
        rest = ' '.join(words[args_start:])
        if not rest:
            await update.message.reply_text("❌ Укажите игру и параметры. Например: вб футбол гол")
            return
        balance = get_balance(user_id)
        if balance <= 0:
            await update.message.reply_text("❌ У вас нет средств для ставки.")
            return
        rest_parts = rest.split()
        if len(rest_parts) < 2:
            await update.message.reply_text("❌ Недостаточно параметров. Например: вб футбол гол")
            return
        game_cmd = rest_parts[0]
        params = rest_parts[1:]
        if context.user_data.get('processing_vb'):
            return
        context.user_data['processing_vb'] = True
        try:
            await process_game(update, context, game_cmd, balance, params)
        finally:
            context.user_data.pop('processing_vb', None)
        return

    remaining_args = words[args_start:]
    if cmd in NON_GAME_COMMANDS:
        await process_non_game(update, context, cmd, remaining_args)
    elif cmd in GAME_COMMANDS:
        if not remaining_args:
            await update.message.reply_text("❌ Укажите сумму ставки.")
            return
        try:
            bet = parse_amount(remaining_args[0], get_balance(user_id))
        except ValueError as e:
            await update.message.reply_text(f"❌ {e}")
            return
        game_args = remaining_args[1:] if len(remaining_args) > 1 else []
        await process_game(update, context, cmd, bet, game_args)

async def process_game(update: Update, context: ContextTypes.DEFAULT_TYPE, cmd: str, bet: int, args: List[str]):
    if cmd == "футбол":
        if not args or args[0] not in ("гол", "мимо"):
            await update.message.reply_text("❌ Укажите: гол или мимо")
            return
        await football(update, context, bet, args[0])
    elif cmd in ("баскетбол", "бс"):
        if not args or args[0] not in ("гол", "мимо"):
            await update.message.reply_text("❌ Укажите: гол или мимо")
            return
        await basketball(update, context, bet, args[0])
    elif cmd == "рулетка":
        if not args:
            await update.message.reply_text("❌ Укажите тип ставки")
            return
        await roulette_bet(update, context, bet, ' '.join(args))
    elif cmd == "кубик":
        if not args or ' '.join(args) not in ("меньше 3", "больше 3", "чёт", "нечёт", "равно 3"):
            await update.message.reply_text("❌ Выбор: меньше 3, больше 3, чёт, нечёт, равно 3")
            return
        await dice_game(update, context, bet, ' '.join(args))
    elif cmd == "21":
        await blackjack_start(update, context, bet)
    elif cmd == "слоты":
        await slots(update, context, bet)
    elif cmd == "башня":
        await tower_start(update, context, bet)
    elif cmd == "фишки":
        if not args or args[0] not in ("черное", "белое"):
            await update.message.reply_text("❌ Цвет: черное или белое")
            return
        await chips(update, context, bet, args[0])
    elif cmd == "пирамида":
        await pyramid_start(update, context, bet)
    elif cmd == "кубы":
        await cubes(update, context, bet)
    elif cmd == "мины":
        await mines_start(update, context, bet)
    elif cmd == "дартс":
        if not args or args[0] not in ("красное", "белое"):
            await update.message.reply_text("❌ Цвет: красное или белое")
            return
        await darts(update, context, bet, args[0])
    elif cmd == "бдж":
        await blackjack_multi_start(update, context, bet)
    elif cmd == "сундуки":
        await chests_start(update, context, bet)
    elif cmd == "золото":
        if args:
            await update.message.reply_text("❌ Для игры Золото не требуется дополнительных параметров.")
            return
        await gold_start(update, context, bet)

async def process_non_game(update: Update, context: ContextTypes.DEFAULT_TYPE, cmd: str, args: List[str]):
    if cmd == "работа":
        await work(update, context)
    elif cmd == "бонус":
        await hourly_bonus(update, context)
    elif cmd == "ежедневныйбонус":
        await daily_bonus(update, context)
    elif cmd in ("профиль", "профиль"):  # полный профиль
        await profile(update, context)
    elif cmd in ("б", "баланс"):  # краткий баланс с кнопками
        await show_balance(update, context)
    elif cmd == "дать":
        await give(update, context)
    elif cmd == "помощь":
        await help_command(update, context)
    elif cmd == "промо":
        await activate_promo(update, context)
    elif cmd == "такси":
        await taxi(update, context)
    elif cmd == "автосалон":
        await car_shop(update, context)
    elif cmd == "моимашины":
        await my_cars(update, context)
    elif cmd == "моямашина":
        await my_car(update, context)
    elif cmd == "топ":
        await top(update, context)
    elif cmd in ("реф", "реферальнаясистема"):
        await referrals(update, context)
    elif cmd == "го":
        await roulette_spin(update, context)
    elif cmd == "ставки":
        await roulette_bets(update, context)
    elif cmd == "история":
        await history(update, context)
    elif cmd == "конкурс":
        await contest(update, context)
    elif cmd == "сотрудничество":
        await cooperation(update, context)
    elif cmd == "крестики":
        await xo_start(update, context)
    elif cmd == "задания":
        await quests_command(update, context)

# ---------- РАССЫЛКА ----------
async def broadcast_notification(bot, message: str, admin_id: int):
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute("SELECT user_id FROM users")
        users = c.fetchall()
    success = 0
    failed = 0
    for (uid,) in users:
        try:
            await bot.send_message(chat_id=uid, text=message, parse_mode=ParseMode.HTML)
            success += 1
        except Exception as e:
            logger.error(f"Ошибка при отправке пользователю {uid}: {e}")
            failed += 1
        await asyncio.sleep(0.05)
    await bot.send_message(chat_id=admin_id, text=f"✅ Рассылка завершена.\nУспешно: {success}\nНеудачно: {failed}")

# ---------- ЗАПУСК ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id
    username = user.username or ""
    first_name = user.first_name or ""
    referrer_id = None
    if context.args and context.args[0].startswith('ref'):
        try:
            referrer_id = int(context.args[0][3:])
        except:
            pass
    ensure_user_exists(user_id, username, first_name, referrer_id)
    welcome_text = (
        f"🎉 Добро пожаловать, {escape_html(first_name)}!\n\n"
        "<blockquote>"
        "🎮 Наш бот предлагает множество игр:\n"
        "Футбол, Баскетбол, Рулетка, Мины, Кубик, 21, Слоты, Башня, Фишки, Пирамида, Кубы, Дартс, Крестики-нолики, Русская рулетка.\n"
        "💰 Экономика: работа, бонусы, такси, магазин машин, рефералы.\n\n"
        "📢 Новости и обновления: https://t.me/werdoxz_wiinere\n"
        "💬 Официальный чат: https://t.me/+B7u5OmPsako4MTAy\n"
        "🆘 Поддержка 24/7: @what_lova\n\n"
        "🌟 этот бот создан строго в цели развлечения игроков и веселья 🌟"
        "</blockquote>"
    )
    await update.message.reply_text(welcome_text, parse_mode=ParseMode.HTML)

def main():
    init_db()
    global ADMIN_IDS
    with closing(sqlite3.connect(DB_NAME, timeout=DB_TIMEOUT)) as conn:
        c = conn.cursor()
        c.execute("SELECT user_id FROM users WHERE is_admin = 1")
        ADMIN_IDS = {row[0] for row in c.fetchall()}
        ADMIN_IDS.add(SUPER_ADMIN_ID)

    app = Application.builder().token(TOKEN).build()
    app.bot_data['bot_enabled'] = True

    # Команды
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("Admin", admin_panel))
    app.add_handler(CommandHandler("ban", ban))
    app.add_handler(CommandHandler("unban", unban))
    app.add_handler(CommandHandler("give", give_admin))
    app.add_handler(CommandHandler("take", take_admin))
    app.add_handler(CommandHandler("notify", notify))
    app.add_handler(CommandHandler("create_promo", create_promo))
    app.add_handler(CommandHandler("setstatus", set_status))
    app.add_handler(CommandHandler("setdesc", set_description))
    app.add_handler(CommandHandler("setprotection", set_protection))

    # ConversationHandler для админ-панели
    admin_conv = ConversationHandler(
        entry_points=[CommandHandler("Admin", admin_panel)],
        states={
            ADMIN_AWAIT_PASSWORD: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_check_password)],
            ADMIN_MENU: [CallbackQueryHandler(admin_callback, pattern="^admin_")],
            ADMIN_AWAIT_TARGET: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_handle_target)],
            ADMIN_AWAIT_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_handle_amount)],
            ADMIN_AWAIT_MESSAGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_handle_message)],
            ADMIN_AWAIT_PROMO: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_handle_promo)],
            ADMIN_AWAIT_DESC: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_handle_desc)],
            ADMIN_AWAIT_STATUS: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_handle_status)],
            ADMIN_AWAIT_PROTECTION: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_handle_protection)],
        },
        fallbacks=[CommandHandler("cancel", lambda u,c: ConversationHandler.END)],
    )
    app.add_handler(admin_conv)

    # Callback-обработчики
    app.add_handler(CallbackQueryHandler(mines_callback, pattern="^mine_"))
    app.add_handler(CallbackQueryHandler(tower_callback, pattern="^tower_"))
    app.add_handler(CallbackQueryHandler(pyramid_callback, pattern="^(pyramid_|pyramid_take)"))
    app.add_handler(CallbackQueryHandler(blackjack_callback, pattern="^bj_"))
    app.add_handler(CallbackQueryHandler(chests_callback, pattern="^chest_"))
    app.add_handler(CallbackQueryHandler(gold_callback, pattern="^gold_"))
    app.add_handler(CallbackQueryHandler(bj_join_callback, pattern="^bj_join_"))
    app.add_handler(CallbackQueryHandler(bj_multi_callback, pattern="^bj_multi_"))
    app.add_handler(CallbackQueryHandler(car_shop_callback, pattern="^(buy_car_|rent_car_|ignore)$"))
    app.add_handler(CallbackQueryHandler(continue_game_callback, pattern="^continue_game$"))
    app.add_handler(CallbackQueryHandler(xo_callback, pattern="^xo_"))
    app.add_handler(CallbackQueryHandler(car_activate_callback, pattern="^car_activate_"))
    app.add_handler(CallbackQueryHandler(car_sell_callback, pattern="^car_sell_"))
    app.add_handler(CallbackQueryHandler(car_noop_callback, pattern="^car_noop$"))
    app.add_handler(CallbackQueryHandler(claim_quest_callback, pattern="^claim_quest_"))
    # Обработчики кнопок баланса
    app.add_handler(CallbackQueryHandler(bonus_button_callback, pattern="^bonus_btn$"))
    app.add_handler(CallbackQueryHandler(daily_bonus_button_callback, pattern="^daily_bonus_btn$"))
    app.add_handler(CallbackQueryHandler(contest_button_callback, pattern="^contest_btn$"))

    # Основной обработчик текстовых сообщений
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    app.run_polling()

if __name__ == "__main__":
    main()