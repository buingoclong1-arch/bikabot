import asyncio
import logging
import os
import re
import sqlite3
import time
import threading
from datetime import datetime
from zoneinfo import ZoneInfo
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from html import escape

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from telegram import InputFile, Update
from telegram.constants import ChatMemberStatus, ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# =========================================================
# CONFIGURATION
# =========================================================

TOKEN = os.environ.get("BOT_TOKEN")
SETUP_PASSWORD = os.environ.get("SETUP_PASSWORD")
DB_PATH = os.environ.get("DB_PATH", "/data/pure_group_orders.db")

# Google Sheets
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID")
GOOGLE_SERVICE_ACCOUNT_FILE = os.environ.get(
    "GOOGLE_SERVICE_ACCOUNT_FILE",
    "/etc/secrets/google-credentials.json",
)

# Super Admin User IDs (Cấp quyền cho cả 2 ID admin sử dụng hệ thống)
ALLOWED_ADMIN_IDS = (7157300503, 995060043)

REPORT_TIMEZONE = os.environ.get("REPORT_TIMEZONE", "Asia/Ho_Chi_Minh")

# Automatically sync Google Sheets every 5 minutes. Can be changed on Render if needed.
GOOGLE_SHEETS_SYNC_INTERVAL_SECONDS = int(
    os.environ.get("GOOGLE_SHEETS_SYNC_INTERVAL_SECONDS", "300")
)
if GOOGLE_SHEETS_SYNC_INTERVAL_SECONDS < 60:
    GOOGLE_SHEETS_SYNC_INTERVAL_SECONDS = 60

# Prevent manual /gsheet and auto-sync from running at the same time.
GOOGLE_SHEETS_SYNC_LOCK = threading.Lock()
try:
    REPORT_TZ = ZoneInfo(REPORT_TIMEZONE)
except Exception as error:
    raise RuntimeError(f"Invalid REPORT_TIMEZONE: {REPORT_TIMEZONE}") from error

if not TOKEN:
    raise RuntimeError("Missing environment variable: BOT_TOKEN.")

if not SETUP_PASSWORD:
    raise RuntimeError("Missing environment variable: SETUP_PASSWORD.")

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

def local_now() -> datetime:
    return datetime.now(REPORT_TZ)

MONEY_PATTERN = re.compile(r"^\d+(?:[.,]\d{1,2})?$")
PLUS_PATTERN = re.compile(r"^(\d+(?:[.,]\d{1,2})?)[\s\xa0]*/$")
MINUS_PATTERN = re.compile(r"^(\d+(?:[.,]\d{1,2})?)[\s\xa0]*-\s*/$")
MULTIPLIER_PATTERN = re.compile(r"^\d+(?:[.,]\d+)?$")


# =========================================================
# DATABASE
# =========================================================

def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)

    with get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS active_groups (
                chat_id INTEGER PRIMARY KEY,
                group_name TEXT NOT NULL,
                setup_by INTEGER NOT NULL,
                setup_at TEXT NOT NULL,
                multiplier TEXT NOT NULL DEFAULT '1'
            )
            """
        )

        columns = {
            row["name"]
            for row in conn.execute(
                "PRAGMA table_info(active_groups)"
            ).fetchall()
        }
        if "multiplier" not in columns:
            conn.execute(
                """
                ALTER TABLE active_groups
                ADD COLUMN multiplier TEXT NOT NULL DEFAULT '1'
                """
            )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS member_orders (
                chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                display_name TEXT NOT NULL,
                username TEXT,
                current_total_cents INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (chat_id, user_id),
                FOREIGN KEY (chat_id)
                    REFERENCES active_groups(chat_id)
                    ON DELETE CASCADE
            )
            """
        )

        columns = {
            row["name"]
            for row in conn.execute(
                "PRAGMA table_info(member_orders)"
            ).fetchall()
        }
        if "updated_at" not in columns:
            conn.execute(
                """
                ALTER TABLE member_orders
                ADD COLUMN updated_at TEXT NOT NULL DEFAULT ''
                """
            )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_member_orders_chat
            ON member_orders (chat_id)
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS transaction_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                display_name TEXT NOT NULL,
                username TEXT,
                group_name TEXT NOT NULL,
                action TEXT NOT NULL,
                amount_cents INTEGER NOT NULL,
                balance_after_cents INTEGER NOT NULL,
                group_total_after_cents INTEGER NOT NULL,
                multiplier TEXT NOT NULL DEFAULT '1',
                created_at TEXT NOT NULL
            )
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_transaction_history_created_at
            ON transaction_history (created_at)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_transaction_history_chat
            ON transaction_history (chat_id)
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_member_summary (
                summary_date TEXT NOT NULL,
                chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                group_name TEXT NOT NULL,
                display_name TEXT NOT NULL,
                username TEXT,
                current_total_cents INTEGER NOT NULL DEFAULT 0,
                multiplier TEXT NOT NULL DEFAULT '1',
                updated_at TEXT NOT NULL,
                PRIMARY KEY (summary_date, chat_id, user_id),
                FOREIGN KEY (chat_id)
                    REFERENCES active_groups(chat_id)
                    ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_daily_member_summary_date
            ON daily_member_summary (summary_date)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_daily_member_summary_chat_date
            ON daily_member_summary (chat_id, summary_date)
            """
        )


# =========================================================
# HELPER FUNCTIONS
# =========================================================

def is_group_active(chat_id: int) -> bool:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT 1 FROM active_groups WHERE chat_id = ?",
            (chat_id,),
        ).fetchone()
    return row is not None


def parse_money(value: str) -> int:
    normalized = value.strip().replace(",", ".")

    if not MONEY_PATTERN.fullmatch(normalized):
        raise ValueError("Invalid amount format.")

    try:
        amount = Decimal(normalized)
    except InvalidOperation as error:
        raise ValueError("Invalid amount format.") from error

    if amount <= 0:
        raise ValueError("Amount must be greater than 0.")

    cents = int(
        (amount * 100).quantize(
            Decimal("1"),
            rounding=ROUND_HALF_UP,
        )
    )
    if cents <= 0:
        raise ValueError("Amount is too small.")

    return cents


def parse_multiplier(value: str) -> Decimal:
    normalized = value.strip()
    # Thêm hàm xử lý cụ thể bên dưới nếu cần thiết


# =========================================================
# HÀM MẪU DÙNG ĐỂ KIỂM TRA QUYỀN TRONG CODE CỦA BẠN
# =========================================================

async def reset_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Đoạn check quyền đã được cập nhật đúng chuẩn Python cho cả 2 ID
    if update.effective_user is None or update.effective_user.id not in ALLOWED_ADMIN_IDS:
        await update.message.reply_text("⛔ You do not have permission to use /reset.")
        return

    # Logic xử lý reset tiếp theo của bạn nằm ở đây...
    await update.message.reply_text("🔄 Hệ thống đang tiến hành reset...")


async def gsheet_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Bạn chỉ cần copy đoạn check quyền này dán vào hàm /gsheet của bạn
    if update.effective_user is None or update.effective_user.id not in ALLOWED_ADMIN_IDS:
        await update.message.reply_text("⛔ You do not have permission to use /gsheet.")
        return

    # Logic xuất file Google Sheet tiếp theo của bạn nằm ở đây...
