import os
import json
import calendar
import threading
import pandas as pd
import matplotlib.pyplot as plt
import gspread
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from google.oauth2 import service_account
from apscheduler.schedulers.blocking import BlockingScheduler
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# === Настройки ===
load_dotenv()
SHEET_ID = "1SHHKKcgXgbzs_AyBQJpyHx9zDauVz6iR9lz1V7Q3hyw"
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

CREDS = service_account.Credentials.from_service_account_info(
    json.loads(os.environ['GOOGLE_CREDENTIALS']),
    scopes=SCOPES
)

def format_ruble(val, decimals=0):
    if pd.isna(val):
        return "—"
    formatted = f"{val:,.{decimals}f}₽".replace(",", " ")
    if decimals == 0:
        formatted = formatted.replace(".00", "")
    return formatted

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
        if col not in ["\u0414\u0430\u0442\u0430", "\u0424\u0443\u0434\u043a\u043e\u0441\u0442 \u043e\u0431\u0449\u0438\u0439, %"]:
            df[col] = (
                df[col].astype(str)
                .str.replace(",", ".")
                .str.replace(r"[^\d\.]", "", regex=True)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["\u0414\u0430\u0442\u0430"] = pd.to_datetime(df["\u0414\u0430\u0442\u0430"], dayfirst=True, errors="coerce")
    df = df.dropna(subset=["\u0414\u0430\u0442\u0430"])
    print("\u0423\u043d\u0438\u043a\u0430\u043b\u044c\u043d\u044b\u0435 \u0434\u0430\u0442\u044b после парсинга:", df["\u0414\u0430\u0442\u0430"].unique())
    print(f"Успешно прочитали! {df.shape}")
    return df

def analyze(df):
    last_date = df["\u0414\u0430\u0442\u0430"].max()
    if pd.isna(last_date):
        return "\ud83d\uddd3 \u0414\u0430\u0442\u0430: не определена\n\n\u26a0\ufe0f Нет доступных данных"

    today_df = df[df["\u0414\u0430\u0442\u0430"] == last_date]
    bar = round(today_df["\u0412\u044bручка бар"].sum())
    kitchen = round(today_df["\u0412\u044bручка кухня"].sum())
    total = bar + kitchen
    avg_check = round(today_df["\u0421р. чек общий"].mean())
    depth = round(today_df["\u0421р. поз чек общий"].mean() / 10, 1)
    hall_income = round(today_df["\u0417ал начислено"].sum())
    delivery = round(today_df["\u0412ыручка доставка "].sum())
    hall_share = (hall_income / total * 100) if total else 0
    delivery_share = (delivery / total * 100) if total else 0

    foodcost_raw = today_df["\u0424удкост общий, %"].astype(str)
    foodcost_raw = foodcost_raw.str.replace(",", ".").str.replace("%", "").str.strip()
    foodcost = round(pd.to_numeric(foodcost_raw, errors="coerce").mean() / 100, 1)

    avg_check_emoji = "\ud83d\ude42" if avg_check >= 1300 else "\ud83d\ude41"
    foodcost_emoji = "\ud83d\ude42" if foodcost <= 23 else "\ud83d\ude41"

    return (
        f"🔽 Дата: {last_date.strftime('%Y-%m-%d')}\n\n"
        f"📊 Выручка: {format_ruble(total)} (Бар: {format_ruble(bar)} + Кухня: {format_ruble(kitchen)})\n"
        f"🧾 Ср.чек: {format_ruble(avg_check)} {avg_check_emoji}\n"
        f"📏 Глубина: {depth:.1f}\n"
        f"🪑 ЗП зал: {format_ruble(hall_income)}\n"
        f"📦 Доставка: {format_ruble(delivery)} ({delivery_share:.1f}%)\n"
        f"📊 Доля ЗП зала: {hall_share:.1f}%\n"
        f"🍔 Фудкост: {foodcost}% {foodcost_emoji}"
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

async def forecast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_chat.id) != str(CHAT_ID):
        return
    try:
        df = read_data()
        now = datetime.now()
        current_month_df = df[(df["\u0414\u0430\u0442\u0430"].dt.year == now.year) & (df["\u0414\u0430\u0442\u0430"].dt.month == now.month)]

        if current_month_df.empty:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="\u26a0\ufe0f Нет данных за текущий месяц.")
            return

        total_revenue_series = current_month_df["\u0412ыручка бар"] + current_month_df["\u0412ыручка кухня"]
        salary_series = current_month_df["\u041dачислено"]

        avg_daily_revenue = total_revenue_series.mean()
        avg_daily_salary = salary_series.mean()

        days_in_month = calendar.monthrange(now.year, now.month)[1]

        forecast_revenue = avg_daily_revenue * days_in_month
        fixed_salaries = 600_000
        forecast_salary = avg_daily_salary * days_in_month + fixed_salaries
        labor_cost_share = (forecast_salary / forecast_revenue * 100) if forecast_revenue else 0

        message = (
            f"🔽 Прогноз на {now.strftime('%B %Y')}:\n"
            f"📊 Выручка: {format_ruble(forecast_revenue)}\n"
            f"🪑 ЗП: {format_ruble(forecast_salary)} (LC: {labor_cost_share:.1f}%)"
        )
        await context.bot.send_message(chat_id=update.effective_chat.id, text=message)

    except Exception as e:
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"❌ Ошибка: {str(e)}")

async def best_manager_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_chat.id) != str(CHAT_ID):
        return
    try:
        df = read_data()
        now = datetime.now()
        current_month_df = df[(df["\u0414\u0430\u0442\u0430"].dt.year == now.year) & (df["\u0414\u0430\u0442\u0430"].dt.month == now.month)]

        if current_month_df.empty or "\u041c\u0435\u043d\u0435\u0434\u0436\u0435\u0440" not in current_month_df.columns:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="\u26a0\ufe0f Нет данных о менеджерах.")
            return

        manager_stats = current_month_df.groupby("\u041cенеджер").agg({
            "\u0412\u044bручка бар": "sum",
            "\u0412\u044bручка кухня": "sum",
            "\u0421р. чек общий": "mean",
            "\u0421р. поз чек общий": "mean"
        })

        manager_stats["\u041eбщая выручка"] = manager_stats["\u0412\u044bручка бар"] + manager_stats["\u0412\u044bручка кухня"]
        top_manager = manager_stats.sort_values("\u041eбщая выручка", ascending=False).head(1)

        if top_manager.empty:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="\u26a0\ufe0f Не удалось определить лучшего менеджера.")
            return

        name = top_manager.index[0]
        total = top_manager["\u041eбщая выручка"].values[0]
        avg_check = top_manager["\u0421р. чек общий"].values[0]
        avg_depth = top_manager["\u0421р. поз чек общий"].values[0] / 10

       message = (
           f"🏆 Лучший менеджер за {now.strftime('%B %Y')}:\n\n"
           f"👤 {name}\n"
           f"📊 Выручка: {format_ruble(total)}\n"
           f"🧾 Ср. чек: {format_ruble(avg_check)}\n"
           f"📏 Глубина чека: {avg_depth:.1f}"
)

