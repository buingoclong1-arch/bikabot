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

# Super Admin User ID (set multiplier and export all group sheets)
ALLOWED_MULTIPLIER_USER_ID = [7157300503, 995060043]

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

        # Save the end-of-day balance snapshot. Do not save individual additions/deductions to Google Sheets.
        # One row per day / group / member.
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
    normalized = value.strip().replace(",", ".")

    if not MULTIPLIER_PATTERN.fullmatch(normalized):
        raise ValueError("Invalid multiplier format.")

    try:
        multiplier = Decimal(normalized)
    except InvalidOperation as error:
        raise ValueError("Invalid multiplier format.") from error

    if not multiplier.is_finite() or multiplier <= 0:
        raise ValueError("Multiplier must be greater than 0.")

    return multiplier


def format_money(cents: int) -> str:
    sign = "-" if cents < 0 else ""
    absolute_cents = abs(cents)
    return f"{sign}${absolute_cents / 100:,.2f}"


def format_decimal(value: Decimal) -> str:
    text = format(value.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def get_display_name(update: Update) -> str:
    user = update.effective_user
    if not user:
        return "Unknown Member"
    return user.full_name or user.first_name or f"User {user.id}"


def get_member_order(chat_id: int, user_id: int) -> sqlite3.Row | None:
    with get_connection() as conn:
        return conn.execute(
            """
            SELECT *
            FROM member_orders
            WHERE chat_id = ? AND user_id = ?
            """,
            (chat_id, user_id),
        ).fetchone()


def refresh_daily_snapshot_for_group(
    conn: sqlite3.Connection,
    chat_id: int,
    summary_date: str,
) -> None:
    """Save the current final state of all group members for the date."""
    group = conn.execute(
        "SELECT group_name, multiplier FROM active_groups WHERE chat_id = ?",
        (chat_id,),
    ).fetchone()
    if not group:
        return

    conn.execute(
        "DELETE FROM daily_member_summary WHERE summary_date = ? AND chat_id = ?",
        (summary_date, chat_id),
    )

    members = conn.execute(
        """
        SELECT user_id, display_name, username, current_total_cents, updated_at
        FROM member_orders
        WHERE chat_id = ? AND current_total_cents <> 0
        ORDER BY current_total_cents DESC, display_name COLLATE NOCASE ASC
        """,
        (chat_id,),
    ).fetchall()

    conn.executemany(
        """
        INSERT INTO daily_member_summary
            (summary_date, chat_id, user_id, group_name, display_name,
             username, current_total_cents, multiplier, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                summary_date,
                chat_id,
                row["user_id"],
                group["group_name"],
                row["display_name"],
                row["username"],
                row["current_total_cents"],
                group["multiplier"] or "1",
                row["updated_at"],
            )
            for row in members
        ],
    )


def snapshot_all_groups_for_date(summary_date: str) -> None:
    """Create/update the end-of-day snapshot for all groups."""
    with get_connection() as conn:
        groups = conn.execute(
            "SELECT chat_id FROM active_groups ORDER BY chat_id"
        ).fetchall()
        for group in groups:
            refresh_daily_snapshot_for_group(
                conn,
                group["chat_id"],
                summary_date,
            )


def change_member_total(
    chat_id: int,
    user_id: int,
    display_name: str,
    username: str | None,
    change_cents: int,
) -> sqlite3.Row:
    now = local_now()
    now_text = now.isoformat(timespec="seconds")
    summary_date = now.date().isoformat()

    with get_connection() as conn:
        group = conn.execute(
            "SELECT group_name, multiplier FROM active_groups WHERE chat_id = ?",
            (chat_id,),
        ).fetchone()
        if not group:
            raise ValueError("This group is not active.")

        row = conn.execute(
            """
            SELECT current_total_cents
            FROM member_orders
            WHERE chat_id = ? AND user_id = ?
            """,
            (chat_id, user_id),
        ).fetchone()

        if row:
            new_total = row["current_total_cents"] + change_cents
            if new_total < 0:
                raise ValueError(
                    "Deduction amount exceeds member's current total."
                )

            conn.execute(
                """
                UPDATE member_orders
                SET display_name = ?,
                    username = ?,
                    current_total_cents = ?,
                    updated_at = ?
                WHERE chat_id = ? AND user_id = ?
                """,
                (
                    display_name,
                    username,
                    new_total,
                    now_text,
                    chat_id,
                    user_id,
                ),
            )
        else:
            if change_cents < 0:
                raise ValueError(
                    "You have no existing order to deduct from."
                )

            new_total = change_cents

            conn.execute(
                """
                INSERT INTO member_orders
                    (chat_id, user_id, display_name, username,
                     current_total_cents, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    chat_id,
                    user_id,
                    display_name,
                    username,
                    new_total,
                    now_text,
                ),
            )

        group_total_after_cents = int(
            conn.execute(
                """
                SELECT COALESCE(SUM(current_total_cents), 0) AS total
                FROM member_orders
                WHERE chat_id = ?
                """,
                (chat_id,),
            ).fetchone()["total"] or 0
        )

        # Save only the end-of-day state; do not use transaction_history
        # for Google Sheets exports anymore.
        refresh_daily_snapshot_for_group(
            conn,
            chat_id,
            summary_date,
        )

        return conn.execute(
            """
            SELECT *
            FROM member_orders
            WHERE chat_id = ? AND user_id = ?
            """,
            (chat_id, user_id),
        ).fetchone()


def get_group_summary(chat_id: int) -> tuple[int, Decimal, int]:
    with get_connection() as conn:
        group = conn.execute(
            """
            SELECT multiplier
            FROM active_groups
            WHERE chat_id = ?
            """,
            (chat_id,),
        ).fetchone()
        total_row = conn.execute(
            """
            SELECT COALESCE(SUM(current_total_cents), 0) AS total
            FROM member_orders
            WHERE chat_id = ?
            """,
            (chat_id,),
        ).fetchone()

    multiplier = Decimal(group["multiplier"] if group else "1")
    total_cents = int(total_row["total"] or 0)
    multiplied_cents = int(
        (Decimal(total_cents) * multiplier).quantize(
            Decimal("1"),
            rounding=ROUND_HALF_UP,
        )
    )
    return total_cents, multiplier, multiplied_cents


def generate_simple_receipt(
    display_name: str,
    amount_changed_cents: int,
    group_total_cents: int,
    action: str,
) -> str:
    if action == "plus":
        title = "📦 AMOUNT ADDED"
        change_label = "➕ Added"
    else:
        title = "➖ AMOUNT DEDUCTED"
        change_label = "➖ Deducted"

    return "\n".join(
        [
            "━━━━━━━━━━━━━━━━━━━━",
            f"<b>{title}</b>",
            "━━━━━━━━━━━━━━━━━━━━",
            f"👤 Member: <b>{escape(display_name)}</b>",
            f"{change_label}: <b>{format_money(amount_changed_cents)}</b>",
            f"🧾 Group Total: <b>{format_money(group_total_cents)}</b>",
        ]
    )


async def check_group_access(update: Update) -> bool:
    if not update.message:
        return False

    chat = update.effective_chat
    if not chat:
        return False

    if chat.type not in ("group", "supergroup"):
        await update.message.reply_text("⚠️ This bot only works in group chats.")
        return False

    if not is_group_active(chat.id):
        await update.message.reply_text(
            "🔒 This group is not activated yet.\n\n"
            "Please send:\n"
            "/setup YOUR_PASSWORD"
        )
        return False

    return True


async def is_group_admin(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> bool:
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user:
        return False

    member = await context.bot.get_chat_member(chat.id, user.id)
    return member.status in (
        ChatMemberStatus.ADMINISTRATOR,
        ChatMemberStatus.OWNER,
    )


def is_allowed_multiplier_user(update: Update) -> bool:
    user = update.effective_user
    return bool(user and user.id == ALLOWED_MULTIPLIER_USER_ID)


# =========================================================
# EXCEL EXPORT (ONE SHEET PER GROUP)
# =========================================================

def clean_sheet_name(name: str) -> str:
    """Remove Excel-invalid characters from sheet names (maximum 31 characters)."""
    cleaned = re.sub(r"[\[\]\:\*\?\/\\]", "", name)
    return cleaned[:30] or "Group"


async def export_excel_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    if not update.message:
        return

    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user:
        return

    # If sent in a private chat and the user is not the Super Admin
    if chat.type not in ("group", "supergroup") and not is_allowed_multiplier_user(update):
        await update.message.reply_text("⚠️ Please use this command in a group chat.")
        return

    file_path = f"BaoCao_HoaDon_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    with get_connection() as conn:
        # If Super Admin -> export all groups in the database
        if is_allowed_multiplier_user(update) and chat.type not in ("group", "supergroup"):
            groups = conn.execute("SELECT chat_id, group_name, multiplier FROM active_groups").fetchall()
        else:
            # Otherwise, export only the current group
            groups = conn.execute(
                "SELECT chat_id, group_name, multiplier FROM active_groups WHERE chat_id = ?",
                (chat.id,)
            ).fetchall()

    if not groups:
        await update.message.reply_text("🎉 There is no data to export to Excel.")
        return

    with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
        all_summary_rows = []

        for group in groups:
            g_id = group["chat_id"]
            g_name = group["group_name"]
            g_mult = Decimal(group["multiplier"] or "1")

            with get_connection() as conn:
                members = conn.execute(
                    """
                    SELECT 
                        display_name AS "Personal Name",
                        username AS "Username",
                        current_total_cents / 100.0 AS "TOTAL PERSONAL ($)",
                        updated_at AS "LASTED UPDATED"
                    FROM member_orders
                    WHERE chat_id = ? AND current_total_cents <> 0
                    ORDER BY current_total_cents DESC
                    """,
                    (g_id,)
                ).fetchall()

            total_cents, multiplier, multiplied_cents = get_group_summary(g_id)
            all_summary_rows.append({
                "Group ID": g_id,
                "Group Name": g_name,
                "Group Total ($)": total_cents / 100.0,
                "Multiplier": float(multiplier),
                "Total After Multiplier ($)": multiplied_cents / 100.0,
                "Members": len(members)
            })

            # Build member table data
            if members:
                df_members = pd.DataFrame([dict(m) for m in members])
            else:
                df_members = pd.DataFrame(columns=["Member Name", "Username", "TOTAL PERSONAL ($)", "LASTED UPDATED"])

            # Create the summary table at the top of the sheet
            df_group_summary = pd.DataFrame([
                {"GROUP NAME": g_name, "TOTAL ($)": total_cents / 100.0, "RATE": float(multiplier), "AFTER RATE ($)": multiplied_cents / 100.0}
            ])

            sheet_title = clean_sheet_name(g_name)
            
            # Write the group summary table at row 1
            df_group_summary.to_excel(writer, sheet_name=sheet_title, index=False, startrow=0)
            
            # Write member details starting at row 4 (with two blank rows)
            df_members.to_excel(writer, sheet_name=sheet_title, index=False, startrow=4)

        # Create an All Groups Summary sheet at the beginning of the file
        df_all_summary = pd.DataFrame(all_summary_rows)
        df_all_summary.to_excel(writer, sheet_name="TOTAL ALL GROUP", index=False)

    # Send the file to the user
    try:
        with open(file_path, "rb") as file_to_send:
            await update.message.reply_document(
                document=InputFile(file_to_send, filename=os.path.basename(file_path)),
                caption="📊 **REPORT EXCEL COMPLETED (EACH GROUP 1 SHEET)**",
                parse_mode=ParseMode.MARKDOWN
            )
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)


# =========================================================
# GOOGLE SHEETS EXPORT
# =========================================================

def get_google_sheet_client():
    """Create the Google Sheets client using a Service Account."""
    if not GOOGLE_SHEET_ID:
        raise RuntimeError("Missing environment variable: GOOGLE_SHEET_ID.")
    if not GOOGLE_SERVICE_ACCOUNT_FILE:
        raise RuntimeError(
            "Missing environment variable: GOOGLE_SERVICE_ACCOUNT_FILE."
        )

    if not os.path.isfile(GOOGLE_SERVICE_ACCOUNT_FILE):
        raise RuntimeError(
            f"Google credentials file not found: {GOOGLE_SERVICE_ACCOUNT_FILE}"
        )

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    credentials = Credentials.from_service_account_file(
        GOOGLE_SERVICE_ACCOUNT_FILE,
        scopes=scopes,
    )
    return gspread.authorize(credentials)


def clean_google_sheet_title(name: str) -> str:
    """Worksheet names can be up to 100 characters and must be unique."""
    cleaned = re.sub(r"[\[\]\:\*\?\/\\]", "", name).strip()
    return cleaned[:100] or "Group"


def get_unique_worksheet_title(spreadsheet, desired_title: str, used_titles: set[str]) -> str:
    base = clean_google_sheet_title(desired_title)
    title = base
    index = 2

    while title in used_titles:
        suffix = f" ({index})"
        title = f"{base[:100 - len(suffix)]}{suffix}"
        index += 1

    used_titles.add(title)
    return title


def excel_column_name(number: int) -> str:
    """Convert 1-based column number to A, B, ..., Z, AA, AB, ..."""
    if number < 1:
        raise ValueError("Column number must be >= 1.")

    result = ""
    while number:
        number, remainder = divmod(number - 1, 26)
        result = chr(65 + remainder) + result
    return result


def dataframe_to_values(df: pd.DataFrame) -> list[list]:
    """Convert a DataFrame into data suitable for writing to Google Sheets."""
    if df.empty:
        return [list(df.columns)]

    values = [list(df.columns)]
    for row in df.itertuples(index=False, name=None):
        cleaned_row = []
        for value in row:
            if pd.isna(value):
                cleaned_row.append("")
            elif isinstance(value, (int, float, str, bool)):
                cleaned_row.append(value)
            else:
                cleaned_row.append(str(value))
        values.append(cleaned_row)
    return values


def worksheet_safe_title(desired_title: str, used_titles: set[str]) -> str:
    base = re.sub(r"[:\\/?*\[\]]", "", desired_title).strip()[:100] or "Sheet"
    title = base
    index = 2
    while title in used_titles:
        suffix = f" ({index})"
        title = f"{base[:100-len(suffix)]}{suffix}"
        index += 1
    used_titles.add(title)
    return title


def write_values_to_worksheet(spreadsheet, title: str, values: list[list], used_titles: set[str]):
    safe_title = worksheet_safe_title(title, used_titles)
    row_count = max(20, len(values) + 5)
    col_count = max(8, max((len(r) for r in values), default=1))
    ws = spreadsheet.add_worksheet(title=safe_title, rows=row_count, cols=col_count)
    end_col = excel_column_name(col_count)
    ws.update(
        values=values,
        range_name=f"A1:{end_col}{max(1, len(values))}",
        raw=True,
    )
    ws.freeze(rows=1)
    if values:
        ws.format(
            f"A1:{excel_column_name(len(values[0]))}1",
            {"textFormat": {"bold": True}},
        )
    return ws


def cents_to_dollars(cents: int) -> float:
    return int(cents or 0) / 100.0


def parse_report_date(value: str | None) -> str:
    """Accept YYYY-MM-DD; if omitted, use the current date in REPORT_TIMEZONE."""
    if not value:
        return local_now().date().isoformat()

    try:
        return datetime.strptime(value, "%Y-%m-%d").date().isoformat()
    except ValueError as error:
        raise ValueError(
            "Invalid date. Use YYYY-MM-DD format, for example 2026-08-13."
        ) from error


def build_daily_report_values(summary_date: str) -> list[list]:
    """Build only the end-of-day group summary; do not export member details."""
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                chat_id,
                group_name,
                user_id,
                current_total_cents,
                multiplier
            FROM daily_member_summary
            WHERE summary_date = ?
              AND current_total_cents <> 0
            """,
            (summary_date,),
        ).fetchall()

    def multiplier_sort_key(value) -> tuple:
        try:
            multiplier = float(value or 0)
        except (TypeError, ValueError):
            multiplier = float("inf")
        return (0 if multiplier < 0.4 else 1, multiplier)

    group_totals: dict[int, dict] = {}
    for row in rows:
        g_id = int(row["chat_id"])
        item = group_totals.setdefault(
            g_id,
            {
                "group_name": row["group_name"],
                "total_cents": 0,
                "multiplier": Decimal(row["multiplier"] or "1"),
                "members": 0,
            },
        )
        item["total_cents"] += int(row["current_total_cents"])
        item["members"] += 1

    sorted_groups = sorted(
        group_totals.items(),
        key=lambda pair: (
            *multiplier_sort_key(pair[1]["multiplier"]),
            str(pair[1]["group_name"] or "").lower(),
        ),
    )

    values = [
        ["DAILY REPORT", summary_date, "", "", "", ""],
        ["Time Zone", REPORT_TIMEZONE, "", "", "", ""],
        [],
        ["GROUP SUMMARY", "", "", "", "", ""],
        [
            "Group ID",
            "Group Name",
            "Group Total ($)",
            "Multiplier",
            "Total After Multiplier ($)",
            "Members",
        ],
    ]

    for g_id, item in sorted_groups:
        total_cents = int(item["total_cents"])
        multiplier = item["multiplier"]
        multiplied_cents = int(
            (Decimal(total_cents) * multiplier).quantize(
                Decimal("1"),
                rounding=ROUND_HALF_UP,
            )
        )
        values.append([
            g_id,
            item["group_name"],
            cents_to_dollars(total_cents),
            float(multiplier),
            cents_to_dollars(multiplied_cents),
            item["members"],
        ])

    if not sorted_groups:
        values.append(["", "No data for this date", 0, 1, 0, 0])

    return values


def _remove_monthly_report_worksheet(spreadsheet) -> None:
    """Remove the old monthly report worksheet if it exists.

    The bot no longer creates or synchronizes monthly reports. This cleanup is
    intentionally best-effort so an already-missing sheet does not fail sync.
    """
    try:
        ws = spreadsheet.worksheet("BAO_CAO_THANG")
    except gspread.WorksheetNotFound:
        return

    try:
        spreadsheet.del_worksheet(ws)
        logger.info("Removed obsolete BAO_CAO_THANG worksheet.")
    except Exception:
        logger.exception("Could not remove obsolete BAO_CAO_THANG worksheet.")


DAILY_TABLE_WORKSHEET_TITLE = "BAO_CAO_NGAY"


def _get_daily_table_worksheet(spreadsheet):
    """One normal worksheet that contains native Google Sheets tables for daily reports."""
    try:
        return spreadsheet.worksheet(DAILY_TABLE_WORKSHEET_TITLE)
    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(
            title=DAILY_TABLE_WORKSHEET_TITLE,
            rows=2000,
            cols=10,
        )


DAILY_TABLE_NAME = "BaoCaoNgay"


def _daily_table_name(summary_date: str) -> str:
    """Single native Google Sheets table used for the current daily report."""
    return DAILY_TABLE_NAME


def build_daily_table_values(summary_date: str) -> list[list]:
    """Build the current daily report for one date."""
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                chat_id,
                group_name,
                current_total_cents,
                multiplier
            FROM daily_member_summary
            WHERE summary_date = ?
              AND current_total_cents <> 0
            """,
            (summary_date,),
        ).fetchall()

    group_totals: dict[int, dict] = {}
    for row in rows:
        g_id = int(row["chat_id"])
        item = group_totals.setdefault(
            g_id,
            {
                "group_name": row["group_name"],
                "total_cents": 0,
                "multiplier": Decimal(row["multiplier"] or "1"),
                "members": 0,
            },
        )
        item["total_cents"] += int(row["current_total_cents"])
        item["members"] += 1
        item["multiplier"] = Decimal(row["multiplier"] or "1")

    def multiplier_sort_key(value) -> tuple:
        try:
            multiplier = float(value or 0)
        except (TypeError, ValueError):
            multiplier = float("inf")
        return (0 if multiplier < 0.4 else 1, multiplier)

    groups = sorted(
        group_totals.items(),
        key=lambda pair: (
            *multiplier_sort_key(pair[1]["multiplier"]),
            str(pair[1]["group_name"] or "").lower(),
        ),
    )

    values = [[
        "Group ID",
        "Group Name",
        "Daily Total ($)",
        "Multiplier",
        "Total After Multiplier ($)",
        "Members",
        "Date",
    ]]

    for g_id, item in groups:
        total_cents = int(item["total_cents"])
        multiplier = item["multiplier"]
        multiplied_cents = int(
            (Decimal(total_cents) * multiplier).quantize(
                Decimal("1"),
                rounding=ROUND_HALF_UP,
            )
        )
        values.append([
            g_id,
            item["group_name"],
            cents_to_dollars(total_cents),
            float(multiplier),
            cents_to_dollars(multiplied_cents),
            item["members"],
            summary_date,
        ])

    if not groups:
        values.append([
            "",
            "No data for this date",
            0,
            1,
            0,
            0,
            summary_date,
        ])

    return values


def _get_existing_daily_table(spreadsheet, sheet_id: int):
    """Return the one existing daily table, if present."""
    metadata = spreadsheet.fetch_sheet_metadata()
    sheet_meta = next(
        (
            sheet
            for sheet in metadata.get("sheets", [])
            if int(sheet.get("properties", {}).get("sheetId", -1)) == sheet_id
        ),
        None,
    )
    tables = (sheet_meta or {}).get("tables", []) or []
    for table in tables:
        if str(table.get("name", "")) == DAILY_TABLE_NAME and table.get("tableId"):
            return table
    return None


def _write_daily_native_table(spreadsheet, summary_date: str) -> str:
    """Write the current daily report directly into the worksheet.

    This version intentionally does NOT create/update Google Sheets Tables and
    does NOT create Google Sheets Tables. There is only one current report worksheet.
    """
    ws = _get_daily_table_worksheet(spreadsheet)
    values = build_daily_table_values(summary_date)

    # Remove any old native tables from this report worksheet. We only need the
    # tableId to delete them; no range object is created or sent to the API.
    metadata = spreadsheet.fetch_sheet_metadata()
    sheet_meta = next(
        (
            sheet
            for sheet in metadata.get("sheets", [])
            if int(sheet.get("properties", {}).get("sheetId", -1)) == int(ws.id)
        ),
        None,
    )
    existing_table_ids = [
        table.get("tableId")
        for table in ((sheet_meta or {}).get("tables", []) or [])
        if table.get("tableId")
    ]
    if existing_table_ids:
        spreadsheet.batch_update({
            "requests": [
                {"deleteTable": {"tableId": table_id}}
                for table_id in existing_table_ids
            ]
        })

    # Keep exactly one report: clear the worksheet and write the latest values.
    ws.clear()
    if values:
        end_col_letter = excel_column_name(len(values[0]))
        spreadsheet.values_batch_update({
            "valueInputOption": "RAW",
            "data": [{
                "range": f"'{DAILY_TABLE_WORKSHEET_TITLE}'!A1:{end_col_letter}{len(values)}",
                "majorDimension": "ROWS",
                "values": values,
            }],
        })

    return DAILY_TABLE_WORKSHEET_TITLE

def sync_all_daily_reports() -> list[str]:
    """Update the single current daily report table."""
    if not GOOGLE_SHEET_ID:
        return []

    client = get_google_sheet_client()
    spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
    summary_date = local_now().date().isoformat()
    _remove_monthly_report_worksheet(spreadsheet)
    try:
        return [_write_daily_native_table(spreadsheet, summary_date)]
    except Exception:
        logger.exception("Daily native table sync failed.")
        raise


def _export_daily_summary_to_google_sheet(summary_date: str) -> tuple[str, int]:
    """Update the existing current daily report table; never create a new table per day."""
    if not GOOGLE_SHEET_ID:
        raise RuntimeError("Missing environment variable: GOOGLE_SHEET_ID.")

    if summary_date == local_now().date().isoformat():
        snapshot_all_groups_for_date(summary_date)

    client = get_google_sheet_client()
    spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)

    _remove_monthly_report_worksheet(spreadsheet)
    _write_daily_native_table(spreadsheet, summary_date)

    return DAILY_TABLE_WORKSHEET_TITLE, len(build_daily_table_values(summary_date))


def export_daily_summary_to_google_sheet(summary_date: str) -> tuple[str, int]:
    """Export with a lock to prevent manual and auto-sync writes from overlapping."""
    with GOOGLE_SHEETS_SYNC_LOCK:
        return _export_daily_summary_to_google_sheet(summary_date)


async def export_google_sheet_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    if not update.message or not update.effective_user:
        return

    if update.effective_user.id != ALLOWED_MULTIPLIER_USER_ID:
        await update.message.reply_text(
            f"⛔ Only User ID {ALLOWED_MULTIPLIER_USER_ID} can use /gsheet."
        )
        return

    try:
        date_arg = context.args[0] if context.args else None
        summary_date = parse_report_date(date_arg)

        sheet_title, row_count = await asyncio.to_thread(
            export_daily_summary_to_google_sheet,
            summary_date,
        )

        await update.message.reply_text(
            "✅ GOOGLE SHEETS EXPORT COMPLETED\n\n"
            f"📅 Date: {summary_date}\n"
            f"📊 Sheet: {sheet_title}\n"
            "📋 Only the current daily report is updated; no new table is created for each day.\n"
            "📊 Monthly reports have been removed.\n"
            f"🧾 Data rows: {row_count}"
        )
    except Exception as error:
        logger.exception("Google Sheets daily export failed.")
        await update.message.reply_text(
            f"❌ Google Sheets export failed: {error}"
        )


# =========================================================
# AUTOMATIC GOOGLE SHEETS SYNC
# =========================================================

async def google_sheets_auto_sync_loop(application: Application) -> None:
    """Automatically sync the current daily report to Google Sheets every 5 minutes."""
    # Wait 5 minutes after bot startup to avoid write requests immediately after deployment.
    while True:
        try:
            await asyncio.sleep(GOOGLE_SHEETS_SYNC_INTERVAL_SECONDS)

            if not GOOGLE_SHEET_ID or not os.path.isfile(GOOGLE_SERVICE_ACCOUNT_FILE):
                logger.warning(
                    "Auto Google Sheets sync skipped: GOOGLE_SHEET_ID or credentials file is missing."
                )
                continue

            summary_date = local_now().date().isoformat()
            sheet_title, row_count = await asyncio.to_thread(
                export_daily_summary_to_google_sheet,
                summary_date,
            )
            logger.info(
                "Google Sheets auto-sync completed: date=%s sheet=%s rows=%s",
                summary_date,
                sheet_title,
                row_count,
            )
        except asyncio.CancelledError:
            logger.info("Google Sheets auto-sync task stopped.")
            raise
        except Exception:
            logger.exception("Google Sheets auto-sync failed; will retry next cycle.")


async def post_init(application: Application) -> None:
    """Start the background auto-sync after the Telegram application is initialized."""
    application.create_task(
        google_sheets_auto_sync_loop(application),
        name="google-sheets-auto-sync",
    )
    logger.info(
        "Google Sheets auto-sync enabled: every %s seconds.",
        GOOGLE_SHEETS_SYNC_INTERVAL_SECONDS,
    )




# =========================================================
# GROUP SHEET RESET HISTORY
# =========================================================

def group_sheet_title(group_name: str) -> str:
    """Stable worksheet name for a Telegram group."""
    cleaned = re.sub(r"[\[\]\:\*\?\/\\]", "", str(group_name)).strip()
    return (f"BOT_GROUP_{cleaned}"[:100] or "BOT_GROUP")


def get_or_create_group_worksheet(spreadsheet, group_name: str):
    title = group_sheet_title(group_name)
    try:
        return spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(
            title=title,
            rows=1000,
            cols=12,
        )


def snapshot_current_group(chat_id: int):
    """Read the current group state before /reset."""
    with get_connection() as conn:
        group = conn.execute(
            """
            SELECT chat_id, group_name, multiplier
            FROM active_groups
            WHERE chat_id = ?
            """,
            (chat_id,),
        ).fetchone()

        if not group:
            return None

        members = conn.execute(
            """
            SELECT user_id, display_name, username,
                   current_total_cents, updated_at
            FROM member_orders
            WHERE chat_id = ?
            ORDER BY current_total_cents DESC, display_name COLLATE NOCASE
            """,
            (chat_id,),
        ).fetchall()

    total_cents, multiplier, multiplied_cents = get_group_summary(chat_id)

    return {
        "chat_id": int(group["chat_id"]),
        "group_name": group["group_name"],
        "total_cents": total_cents,
        "multiplier": multiplier,
        "multiplied_cents": multiplied_cents,
        "member_count": len(members),
        "members": [dict(row) for row in members],
    }


def append_reset_history_to_group_sheet(snapshot: dict) -> str:
    """
    Keep one worksheet per group.
    Each /reset appends ONE summary row under RESET HISTORY.
    No new worksheet is created per day.
    """
    client = get_google_sheet_client()
    spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
    ws = get_or_create_group_worksheet(spreadsheet, snapshot["group_name"])

    # Check whether this is a newly-created/empty worksheet.
    existing_values = ws.get_all_values()

    if not existing_values:
        initial_rows = [
            ["CURRENT SUMMARY"],
            ["Group ID", snapshot["chat_id"]],
            ["Group Name", snapshot["group_name"]],
            ["Current Total ($)", snapshot["total_cents"] / 100.0],
            ["Rate", float(snapshot["multiplier"])],
            ["After Rate ($)", snapshot["multiplied_cents"] / 100.0],
            ["Members", snapshot["member_count"]],
            [],
            ["RESET HISTORY"],
            [
                "Date",
                "Reset Time",
                "Group Total ($)",
                "Rate",
                "After Rate ($)",
                "Members",
            ],
        ]

        end_col = excel_column_name(6)
        last_row = len(initial_rows)

        for attempt in range(3):
            try:
                ws.update(
                    values=initial_rows,
                    range_name=f"A1:{end_col}{last_row}",
                    raw=True,
                )
                break
            except Exception as error:
                if "[429]" not in str(error) and "Quota exceeded" not in str(error):
                    raise
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))

    # Refresh current summary AFTER reset is not desired here; this snapshot is
    # specifically the state BEFORE reset. The history row is the authoritative
    # record of the reset.
    now = local_now()
    history_row = [[
        now.strftime("%Y-%m-%d"),
        now.strftime("%H:%M:%S"),
        snapshot["total_cents"] / 100.0,
        float(snapshot["multiplier"]),
        snapshot["multiplied_cents"] / 100.0,
        snapshot["member_count"],
    ]]

    # Find RESET HISTORY section. Header is followed by appended rows.
    values = ws.get_all_values()
    header_row = None
    for idx, row in enumerate(values, start=1):
        if row and row[0] == "RESET HISTORY":
            header_row = idx
            break

    if header_row is None:
        # Repair an older/empty sheet by appending the section.
        start_row = len(values) + 1
        repair = [
            ["RESET HISTORY"],
            [
                "Date",
                "Reset Time",
                "Group Total ($)",
                "Rate",
                "After Rate ($)",
                "Members",
            ],
        ]
        ws.update(
            values=repair,
            range_name=f"A{start_row}:F{start_row + 1}",
            raw=True,
        )
        header_row = start_row

    # Append exactly ONE row for this reset.
    ws.append_rows(history_row, value_input_option="RAW")

    return ws.title


def update_current_group_sheet(chat_id: int):
    """
    Auto-sync the latest current state to the stable group worksheet.
    Reset history remains below it and is never deleted.
    """
    with get_connection() as conn:
        group = conn.execute(
            """
            SELECT chat_id, group_name, multiplier
            FROM active_groups
            WHERE chat_id = ?
            """,
            (chat_id,),
        ).fetchone()

    if not group:
        return None

    total_cents, multiplier, multiplied_cents = get_group_summary(chat_id)
    client = get_google_sheet_client()
    spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
    ws = get_or_create_group_worksheet(spreadsheet, group["group_name"])

    current_rows = [
        ["CURRENT SUMMARY"],
        ["Group ID", group["chat_id"]],
        ["Group Name", group["group_name"]],
        ["Current Total ($)", total_cents / 100.0],
        ["Rate", float(multiplier)],
        ["After Rate ($)", multiplied_cents / 100.0],
    ]

    with get_connection() as conn:
        member_count = conn.execute(
            "SELECT COUNT(*) AS n FROM member_orders WHERE chat_id = ?",
            (chat_id,),
        ).fetchone()["n"]

    current_rows.append(["Members", member_count])

    # Keep reset history intact. We only rewrite the top summary block.
    ws.update(
        values=current_rows,
        range_name=f"A1:B{len(current_rows)}",
        raw=True,
    )

    # If this is a brand-new sheet, ensure the reset-history header exists.
    values = ws.get_all_values()
    if not any(row and row[0] == "RESET HISTORY" for row in values):
        start_row = len(values) + 2
        ws.update(
            values=[
                ["RESET HISTORY"],
                [
                    "Date",
                    "Reset Time",
                    "Group Total ($)",
                    "Rate",
                    "After Rate ($)",
                    "Members",
                ],
            ],
            range_name=f"A{start_row}:F{start_row + 1}",
            raw=True,
        )

    # Reorder group worksheets so multipliers < 0.4 stay together at the top.
    try:
        sort_group_worksheets_by_multiplier(spreadsheet)
    except Exception:
        # Sorting is cosmetic; never make the data sync fail because of it.
        logger.exception("Could not sort group worksheets by multiplier.")

    return ws.title




# =========================================================
# SORT GROUP SHEETS BY MULTIPLIER
# =========================================================

def sort_group_worksheets_by_multiplier(spreadsheet) -> None:
    """
    Put BOT_GROUP_* worksheets in multiplier order.

    Priority:
      1. multiplier < 0.4 first, adjacent to each other
      2. then multiplier >= 0.4 in ascending order

    Other worksheets are left after the group worksheets.
    """
    group_items = []

    for ws in spreadsheet.worksheets():
        if not ws.title.startswith("BOT_GROUP_"):
            continue

        # Read multiplier from CURRENT SUMMARY, row 5, column B.
        try:
            value = ws.acell("B5").value
            multiplier = float(value)
        except Exception:
            multiplier = float("inf")

        group_items.append((ws, multiplier))

    if len(group_items) <= 1:
        return

    group_items.sort(
        key=lambda item: (
            0 if item[1] < 0.4 else 1,
            item[1],
            item[0].title.lower(),
        )
    )

    # Google Sheets API batchUpdate: move each sheet to the desired index.
    # We do one API request for the entire reorder operation.
    requests = []
    for target_index, (ws, _multiplier) in enumerate(group_items):
        requests.append({
            "updateSheetProperties": {
                "properties": {
                    "sheetId": ws.id,
                    "index": target_index,
                },
                "fields": "index",
            }
        })

    spreadsheet.batch_update({"requests": requests})



# =========================================================
# COMMAND HANDLERS
# =========================================================

async def setup_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    chat, user = update.effective_chat, update.effective_user

    if chat.type not in ("group", "supergroup"):
        await update.message.reply_text("⚠️ The /setup command can only be used in a group.")
        return

    if is_group_active(chat.id):
        await update.message.reply_text("✅ ALREADY ACTIVATED.")
        return

    if not context.args or context.args[0] != SETUP_PASSWORD:
        await update.message.reply_text("❌ Incorrect password.")
        return

    group_name = chat.title or "Unknown Group"
    with get_connection() as conn:
        conn.execute(
            "INSERT INTO active_groups (chat_id, group_name, setup_by, setup_at, multiplier) VALUES (?, ?, ?, ?, '1')",
            (chat.id, group_name, user.id, local_now().isoformat(timespec="seconds")),
        )

    await update.message.reply_text(f"🎉 ACTIVATED!\n📌 Group: {group_name}")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message:
        await update.message.reply_text(
            "👋 Garu Calculation Bot.\n"
            "Use /excel to output Excel.\n"
            "Use /gsheet to export the daily report (User ID 7157300503 only).\n"
            "Google Sheets automatically syncs the latest data every 5 minutes."
        )


async def apply_member_change(update: Update, amount_cents: int, action: str) -> None:
    if not update.message or not await check_group_access(update):
        return
    chat, user = update.effective_chat, update.effective_user
    if not chat or not user:
        return

    signed_change = amount_cents if action == "plus" else -amount_cents
    try:
        row = change_member_total(chat.id, user.id, get_display_name(update), user.username, signed_change)
    except ValueError as error:
        await update.message.reply_text(f"⚠️ {error}")
        return

    group_total, _, _ = get_group_summary(chat.id)
    receipt = generate_simple_receipt(row["display_name"], amount_cents, group_total, action)
    await update.message.reply_text(receipt, parse_mode=ParseMode.HTML)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    text = (update.message.text or update.message.caption or "").strip()
    minus_match = MINUS_PATTERN.fullmatch(text)
    plus_match = PLUS_PATTERN.fullmatch(text)

    if not minus_match and not plus_match:
        return

    match = minus_match or plus_match
    try:
        amount_cents = parse_money(match.group(1))
    except ValueError as error:
        await update.message.reply_text(f"⚠️ {error}")
        return

    await apply_member_change(update, amount_cents, "minus" if minus_match else "plus")


async def minus_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    try:
        amount_cents = parse_money(context.args[0])
    except (IndexError, ValueError):
        await update.message.reply_text("⚠️ Invalid format. Example: /minus 5")
        return
    await apply_member_change(update, amount_cents, "minus")


async def view_my_receipt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not await check_group_access(update):
        return
    chat, user = update.effective_chat, update.effective_user
    if not chat or not user:
        return

    row = get_member_order(chat.id, user.id)
    if not row or row["current_total_cents"] == 0:
        await update.message.reply_text("🎉 You do not have any data.")
        return

    await update.message.reply_text(
        f"<b>📋 PERSONAL BILL</b>\n"
        f"👤 Member: <b>{escape(row['display_name'])}</b>\n"
        f"🧾 Personal Bill: <b>{format_money(row['current_total_cents'])}</b>\n"
        f"⏱ Updated: {escape(row['updated_at'])}",
        parse_mode=ParseMode.HTML,
    )


async def view_receipt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not await check_group_access(update):
        return
    chat = update.effective_chat
    if not chat:
        return

    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT display_name, username, current_total_cents
            FROM member_orders
            WHERE chat_id = ? AND current_total_cents <> 0
            ORDER BY current_total_cents DESC, display_name COLLATE NOCASE ASC
            """,
            (chat.id,),
        ).fetchall()

    if not rows:
        await update.message.reply_text("🎉 The group has no bill data.")
        return

    group_total, multiplier, multiplied_total = get_group_summary(chat.id)
    lines = ["<b>📋 DETAILED GROUP REPORT</b>"]

    for index, row in enumerate(rows, start=1):
        username = f" (@{escape(row['username'])})" if row["username"] else ""
        lines.append(f"<b>{index}. {escape(row['display_name'])}</b>{username}: <b>{format_money(row['current_total_cents'])}</b>")

    lines.extend([
        "━━━━━━━━━━━━━━━━━━━━",
        f"🧾 Group Total: <b>{format_money(group_total)}</b>",
        f"✖️ Group Multiplier: <b>{format_decimal(multiplier)}</b>",
        f"💰 Total After Multiplier: <b>{format_money(multiplied_total)}</b>",
    ])

    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def set_multiplier_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not await check_group_access(update):
        return

    if not is_allowed_multiplier_user(update):
        await update.message.reply_text(f"⛔ Only User ID {ALLOWED_MULTIPLIER_USER_ID} can update the multiplier.")
        return

    chat = update.effective_chat
    if not chat:
        return

    try:
        multiplier = parse_multiplier(context.args[0])
    except (IndexError, ValueError):
        await update.message.reply_text("⚠️ WRONG: /setmultiplier 1.5")
        return

    multiplier_text = format_decimal(multiplier)
    with get_connection() as conn:
        conn.execute("UPDATE active_groups SET multiplier = ? WHERE chat_id = ?", (multiplier_text, chat.id))

    group_total, multiplier, multiplied_total = get_group_summary(chat.id)
    await update.message.reply_text(
        f"<b>✅ UPDATING GROUP RATE</b>\n"
        f"✖️ RATE: <b>{format_decimal(multiplier)}</b>\n"
        f"🧾 Total: <b>{format_money(group_total)}</b>\n"
        f"💰 After rate: <b>{format_money(multiplied_total)}</b>",
        parse_mode=ParseMode.HTML,
    )


async def reset_me_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not await check_group_access(update):
        return
    chat, user = update.effective_chat, update.effective_user
    if not chat or not user:
        return

    with get_connection() as conn:
        cursor = conn.execute("DELETE FROM member_orders WHERE chat_id = ? AND user_id = ?", (chat.id, user.id))

    if cursor.rowcount == 0:
        await update.message.reply_text("🎉 You have no data to delete.")
    else:
        await update.message.reply_text("🗑 PERSONAL DATA DELETED.")


async def reset_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not await check_group_access(update):
        return

    # /reset is restricted to this Telegram user ID only.
    if update.effective_user is None or update.effective_user.id != [995060043,7157300503]:
        await update.message.reply_text("⛔ You do not have permission to use /reset.")
        return

    chat = update.effective_chat
    if not chat:
        return

    try:
        # 1. Capture the current state BEFORE reset.
        snapshot = await asyncio.to_thread(
            snapshot_current_group,
            chat.id,
        )

        if not snapshot:
            await update.message.reply_text(
                "❌ Group data was not found. Nothing was reset."
            )
            return

        # 2. Save exactly ONE reset-summary row in the group's existing sheet.
        sheet_name = await asyncio.to_thread(
            append_reset_history_to_group_sheet,
            snapshot,
        )

        # 3. Only after Google Sheets succeeds, reset the database.
        today = local_now().date().isoformat()

        with get_connection() as conn:
            conn.execute(
                "DELETE FROM member_orders WHERE chat_id = ?",
                (chat.id,),
            )
            conn.execute(
                """
                DELETE FROM daily_member_summary
                WHERE chat_id = ? AND summary_date = ?
                """,
                (chat.id, today),
            )
            conn.commit()

        # 4. Immediately refresh CURRENT SUMMARY to zero.
        try:
            await asyncio.to_thread(
                update_current_group_sheet,
                chat.id,
            )
        except Exception:
            # Database reset has already succeeded. The next 5-minute
            # auto-sync will repair CURRENT SUMMARY.
            logger.exception("Could not refresh current group sheet after reset.")

        await update.message.reply_text(
            "✅ RESET COMPLETED\n\n"
            f"📌 Group: {snapshot['group_name']}\n"
            f"💰 Total before reset: ${snapshot['total_cents'] / 100:.2f}\n"
            f"📊 Total after multiplier: ${snapshot['multiplied_cents'] / 100:.2f}\n"
            f"👥 Members: {snapshot['member_count']}\n\n"
            f"🗂 Added one row to: {sheet_name}\n"
            "♻️ Database has been reset."
        )

    except Exception as error:
        logger.exception("Reset failed.")

        # If the archive write failed, the destructive DB operation was never reached.
        await update.message.reply_text(
            "❌ RESET CANCELLED\n\n"
            "Could not save the data to Google Sheets.\n"
            "The database was NOT reset to prevent data loss.\n\n"
            f"Error: {error}"
        )


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("An error occurred while handling update:", exc_info=context.error)


# =========================================================
# MAIN FUNCTION
# =========================================================

def main() -> None:
    init_db()

    application = Application.builder().token(TOKEN).post_init(post_init).build()

    application.add_handler(CommandHandler("setup", setup_command))
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("minus", minus_command))
    application.add_handler(CommandHandler("me", view_my_receipt))
    application.add_handler(CommandHandler("view", view_receipt))
    application.add_handler(CommandHandler("setmultiplier", set_multiplier_command))
    application.add_handler(CommandHandler("excel", export_excel_command))
    application.add_handler(CommandHandler("gsheet", export_google_sheet_command))
    application.add_handler(CommandHandler("resetme", reset_me_command))
    application.add_handler(CommandHandler("reset", reset_command))

    application.add_handler(
        MessageHandler((filters.TEXT | filters.Caption()) & ~filters.COMMAND, handle_message)
    )
    application.add_error_handler(error_handler)

    logger.info("Bot is running...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
