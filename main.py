import os
import pandas as pd
import matplotlib.pyplot as plt
import gspread
from google.oauth2.service_account import Credentials
from apscheduler.schedulers.blocking import BlockingScheduler
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from dotenv import load_dotenv
import requests
import threading
from datetime import datetime, timedelta

# === Настройки ===
load_dotenv()
SHEET_ID = "1SHHKKcgXgbzs_AyBQJpyHx9zDauVz6iR9lz1V7Q3hyw"
SERVICE_ACCOUNT_FILE = "credentials.json"
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
CREDS = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

def format_ruble(val):
    return f"{val:,.2f}₽".replace(",", " ").replace(".00", "")

def send_to_telegram(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID, "text": message}
    requests.post(url, data=data)

def read_data():
    print("Читаем таблицу...")
    gc = gspread.authorize(CREDS)
    sheet = gc.open_by_key(SHEET_ID).sheet1
    df = pd.DataFrame(sheet.get_all_records())
    print("Столбцы из Google Sheets:", list(df.columns))

    if "Дата" not in df.columns:
        return pd.DataFrame()

    for col in df.columns:
        if col != "Дата":
            df[col] = (
                df[col].astype(str)
                .str.replace(",", ".")
                .str.replace(r"[^\d\.]", "", regex=True)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["Дата"] = pd.to_datetime(df["Дата"], dayfirst=True, errors="coerce")
    df = df.dropna(subset=["Дата"])
    print("Уникальные даты после парсинга:", df["Дата"].unique())
    print(f"Успешно прочитали! {df.shape}")
    return df

def analyze(df):
    last_date = df["Дата"].max()
    if pd.isna(last_date):
        return "📅 Дата: не определена\n\n⚠️ Нет доступных данных"

    today_df = df[df["Дата"] == last_date]

    bar = today_df["Выручка бар"].sum()
    kitchen = today_df["Выручка кухня"].sum()
    total = bar + kitchen
    avg_check = today_df["Ср. чек общий"].mean()/100
    depth = today_df["Ср. поз чек общий"].mean()/10
    hall_income = today_df["Зал начислено"].sum()/100
    delivery = today_df["Выручка доставка "].sum()

    return (
        f"📅 Дата: {last_date.strftime('%Y-%m-%d')}\n\n"
        f"📊 Выручка: {format_ruble(total)} (Бар: {format_ruble(bar)} + Кухня: {format_ruble(kitchen)})\n"
        f"🧾 Средний чек: {format_ruble(avg_check)}\n"
        f"📏 Глубина чека: {depth:.2f}\n"
        f"🪑 Начислено по залу: {format_ruble(hall_income)}\n"
        f"📦 Доставка: {format_ruble(delivery)}"
    )

async def analyze_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_chat.id) != str(CHAT_ID):
        return
    try:
        df = read_data()
        report = analyze(df)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=report)
    except Exception as e:
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"❌ Ошибка: {str(e)}")

def job():
    try:
        df = read_data()
        report = analyze(df)
        send_to_telegram(report)
    except Exception as e:
        send_to_telegram(f"❌ Ошибка: {str(e)}")

if __name__ == "__main__":
    print("⏰ Бот запущен. Отчёт будет в 9:30 по Калининграду")
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("analyze", analyze_command))
    scheduler = BlockingScheduler(timezone="Europe/Kaliningrad")
    scheduler.add_job(job, trigger="cron", hour=9, minute=30)
    threading.Thread(target=scheduler.start).start()
    app.run_polling()
