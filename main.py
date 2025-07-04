import os
import calendar
import threading
import pandas as pd
import gspread
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
#from google.oauth2 import service_account   # Перенесено в utils.py
#from google_api import get_management_value # Перенесено в utils.py
from forecast import forecast                # Прогноз — импортируем из forecast.py
from utils import get_management_percent, get_management_value, format_ruble # все, что нужно
from apscheduler.schedulers.blocking import BlockingScheduler
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# === Настройки ===4
load_dotenv()
SHEET_ID = "1SHHKKcgXgbzs_AyBQJpyHx9zDauVz6iR9lz1V7Q3hyw"
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# Управляющая таблица с переменными расходами
MANAGEMENT_SHEET_ID = "1nqpQ97D9rS2hPVQrrlbPKO5QG5RXvc936xvw6TSHnXc"
MANAGEMENT_SHEET_NAME = "Лист1"

SERVICE_ACCOUNT_FILE = 'fifth-medley-461515-h0-089884c74c28.json'
#CREDS = service_account.Credentials.from_service_account_file(
#    SERVICE_ACCOUNT_FILE,
#    scopes=SCOPES
#)

# def format_ruble(val, decimals=0):
#     if pd.isna(val):
#         return "—"
#     formatted = f"{val:,.{decimals}f}₽".replace(",", " ")
#     if decimals == 0:
#         formatted = formatted.replace(".00", "")
#     return formatted

# def send_to_telegram(message: str):
#     url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
#     data = {"chat_id": CHAT_ID, "text": message}
#     requests.post(url, data=data)

# def read_data():
#     gc = gspread.authorize(CREDS)
#     sheet = gc.open_by_key(SHEET_ID).sheet1
#     df = pd.DataFrame(sheet.get_all_records())
#     if "Дата" not in df.columns:
#         return pd.DataFrame()
#     for col in df.columns:
#         if col not in ["Дата", "Фудкост общий, %", "Менеджер"]:
#             df[col] = (
#                 df[col].astype(str)
#                 .str.replace(",", ".")
#                 .str.replace(r"[^\d\.]", "", regex=True)
#             )
#             df[col] = pd.to_numeric(df[col], errors="coerce")
#     df["Дата"] = pd.to_datetime(df["Дата"], dayfirst=True, errors="coerce")
#     df = df.dropna(subset=["Дата"])
#     return df

# def get_management_foodcost():
#     gc = gspread.authorize(CREDS)
#     sheet = gc.open_by_key(MANAGEMENT_SHEET_ID).worksheet(MANAGEMENT_SHEET_NAME)
#     df = pd.DataFrame(sheet.get_all_records())
#     fc_row = df[df.iloc[:,0].astype(str).str.lower().str.strip() == "фудкост"]
#     if not fc_row.empty:
#         fc_percent = fc_row.iloc[0]["Процент"]
#         try:
#             return float(str(fc_percent).replace(",", "."))
#         except Exception:
#             return None
#     return None

# --- Импортируйте эти функции из utils.py ---
from utils import (
    read_data,             # Функция для чтения основной таблицы
    send_to_telegram,      # Функция для отправки сообщения в телегу
    format_ruble,          # Форматирование рубля
    get_management_foodcost
)

def analyze(df):
    last_date = df["Дата"].max()
    if pd.isna(last_date):
        return "📅 Дата: не определена\n\n⚠️ Нет доступных данных"

    today_df = df[df["Дата"] == last_date]
    bar = round(today_df["Выручка бар"].sum())
    kitchen = round(today_df["Выручка кухня"].sum())
    total = bar + kitchen
    avg_check = round(today_df["Ср. чек общий"].mean())
    depth = round(today_df["Ср. поз чек общий"].mean() / 10, 1)
    hall_income = round(today_df["Зал начислено"].sum())
    delivery = round(today_df["Выручка доставка "].sum())
    hall_share = (hall_income / total * 100) if total else 0
    delivery_share = (delivery / total * 100) if total else 0

    foodcost_raw = today_df["Фудкост общий, %"].astype(str)\
        .str.replace(",", ".")\
        .str.replace("%", "")\
        .str.strip()
    foodcost = round(pd.to_numeric(foodcost_raw, errors="coerce").mean() / 10, 1)

    discount_raw = today_df["Скидка общий, %"].astype(str)\
        .str.replace(",", ".")\
        .str.replace("%", "")\
        .str.strip()
    discount = round(pd.to_numeric(discount_raw, errors="coerce").mean() / 10, 1)

    avg_check_emoji = "🙂" if avg_check >= 1300 else "🙁"
    foodcost_emoji = "🙂" if foodcost <= 23 else "🙁"

    managers_today = today_df["Менеджер"].dropna().unique()
    manager_name = managers_today[0] if len(managers_today) > 0 else "—"

    return (
        f"📅 Дата: {last_date.strftime('%Y-%m-%d')}\n\n"
        f"👤 {manager_name}\n"
        f"📊 Выручка: {format_ruble(total)} (Бар: {format_ruble(bar)} + Кухня: {format_ruble(kitchen)})\n"
        f"🧾 Ср.чек: {format_ruble(avg_check)} {avg_check_emoji}\n"
        f"📏 Глубина: {depth:.1f}\n"
        f"🪑 ЗП зал: {format_ruble(hall_income)}\n"
        f"📦 Доставка: {format_ruble(delivery)} ({delivery_share:.1f}%)\n"
        f"📊 Доля ЗП зала: {hall_share:.1f}%\n"
        f"🍔 Фудкост: {foodcost}% {foodcost_emoji}\n"
        f"💸 Скидка: {discount}%"
    )

# === Обработка команды /forecast ===
async def forecast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_chat.id) != str(CHAT_ID):
        return
    try:
        df = read_data()
        result = forecast(df)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=result)
    except Exception as e:
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"❌ Ошибка: {str(e)}")

# --- Остальные команды без изменений ---
async def analyze_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_chat.id) != str(CHAT_ID):
        return
    try:
        df = read_data()
        report = analyze(df)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=report)
    except Exception as e:
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"❌ Ошибка: {str(e)}")

async def managers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_chat.id) != str(CHAT_ID):
        return
    try:
        df = read_data()

        if "Менеджер" not in df.columns:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="⚠️ Колонка 'Менеджер' не найдена в данных.")
            return

        now = datetime.now()
        filtered = df[
            df["Менеджер"].notna() &
            (df["Дата"].dt.year == now.year) &
            (df["Дата"].dt.month == now.month)
        ]

        if filtered.empty:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="⚠️ Нет строк с указанными менеджерами за текущий месяц.")
            return

        manager_stats = filtered.groupby("Менеджер").agg({
            "Выручка бар": "sum",
            "Выручка кухня": "sum",
            "Ср. чек общий": "mean",
            "Ср. поз чек общий": "mean",
            "Скидка общий, %": "mean"
        }).fillna(0)

        manager_stats["Общая выручка"] = manager_stats["Выручка бар"] + manager_stats["Выручка кухня"]
        manager_stats["Глубина"] = manager_stats["Ср. поз чек общий"] / 10

        max_values = {
            "Ср. чек общий": manager_stats["Ср. чек общий"].max(),
            "Общая выручка": manager_stats["Общая выручка"].max(),
            "Глубина": manager_stats["Глубина"].max()
        }

        manager_stats["Оценка"] = (
            (manager_stats["Ср. чек общий"] / max_values["Ср. чек общий"]) * 0.5 +
            (manager_stats["Общая выручка"] / max_values["Общая выручка"]) * 0.3 +
            (manager_stats["Глубина"] / max_values["Глубина"]) * 0.2
        )

        manager_stats = manager_stats.sort_values("Оценка", ascending=False)
        message = f"📅 Период: {now.strftime('%B %Y')}\n\n"
        
        for name, row in manager_stats.iterrows():
            discount_percent = round(row['Скидка общий, %'] / 10, 1)
            message += (
                f"👤 {name}\n"
                f"📊 Выручка: {format_ruble(row['Общая выручка'])}\n"
                f"🧾 Ср. чек: {format_ruble(row['Ср. чек общий'])}\n"
                f"📏 Глубина: {row['Глубина']:.1f}\n"
                f"💸 Скидка: {discount_percent}%\n\n"
            )

        message += f"🏆 Победитель: {manager_stats.index[0]}"
        await context.bot.send_message(chat_id=update.effective_chat.id, text=message)

    except Exception as e:
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"❌ Ошибка: {str(e)}")

# --- Планировщик для ежедневного отчёта ---
def job():
    try:
        df = read_data()
        report = analyze(df)
        send_to_telegram(report)
    except Exception as e:
        send_to_telegram(f"❌ Ошибка: {str(e)}")

if __name__ == "__main__":
    print("⏰ Тестовый запуск без Telegram\n")
    df = read_data()
    print("=== Анализ дня ===")
    print(analyze(df))
    print("=== Прогноз ===")
    print(forecast(df))         # <--- ДОБАВИТЬ ЭТУ СТРОКУ
    print("⏰ Бот запущен. Отчёт будет в 9:30 по Калининграду")
    send_to_telegram("⚡️ Проверка! Это тестовое сообщение из main.py")
    # остальной код

    #send_to_telegram("⚡ Проверка! Это тестовое сообщение из main.py")

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("analyze", analyze_command))
    app.add_handler(CommandHandler("forecast", forecast_command))
    app.add_handler(CommandHandler("managers", managers_command))

    scheduler = BlockingScheduler(timezone="Europe/Kaliningrad")
    scheduler.add_job(job, trigger="cron", hour=9, minute=30)
    threading.Thread(target=scheduler.start).start()

    app.run_polling()
