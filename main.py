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
        if col not in ["Дата", "Фудкост общий, %"]:
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

    foodcost = round(pd.to_numeric(foodcost_raw, errors="coerce").mean() / 100, 1)

    avg_check_emoji = "🙂" if avg_check >= 1300 else "🙁"
    foodcost_emoji = "🙂" if foodcost <= 23 else "🙁"

    return (
        f"📅 Дата: {last_date.strftime('%Y-%m-%d')}\n\n"
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
        current_month_df = df[(df["Дата"].dt.year == now.year) & (df["Дата"].dt.month == now.month)]

        if current_month_df.empty:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="⚠️ Нет данных за текущий месяц.")
            return

        total_revenue_series = current_month_df["Выручка бар"] + current_month_df["Выручка кухня"]
        salary_series = current_month_df["Начислено"]

        avg_daily_revenue = total_revenue_series.mean()
        avg_daily_salary = salary_series.mean()

        days_in_month = calendar.monthrange(now.year, now.month)[1]

        forecast_revenue = avg_daily_revenue * days_in_month
        fixed_salaries = 600_000
        forecast_salary = avg_daily_salary * days_in_month + fixed_salaries
        labor_cost_share = (forecast_salary / forecast_revenue * 100) if forecast_revenue else 0
        forecast_profit = forecast_revenue * 0.55 - forecast_salary

        message = (
            f"📅 Прогноз на {now.strftime('%B %Y')}:\n"
            f"📊 Выручка: {format_ruble(forecast_revenue)}\n"
            f"🪑 ЗП: {format_ruble(forecast_salary)} (LC: {labor_cost_share:.1f}%)\n"
            f"💰 Прибыль: {format_ruble(forecast_profit)}"
        )
        await context.bot.send_message(chat_id=update.effective_chat.id, text=message)

    except Exception as e:
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"❌ Ошибка: {str(e)}")

async def managers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_chat.id) != str(CHAT_ID):
        return
    try:
        df = read_data()
        report = analyze_all_managers(df)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=report)
    except Exception as e:
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"❌ Ошибка: {str(e)}")

def analyze_all_managers(df):
    df["Дата"] = pd.to_datetime(df["Дата"], dayfirst=True, errors="coerce")
    df = df.dropna(subset=["Дата", "Менеджер"])

    now = datetime.now()
    df_month = df[(df["Дата"].dt.year == now.year) & (df["Дата"].dt.month == now.month)]
    df_month["Выручка"] = df_month["Выручка бар"] + df_month["Выручка кухня"]

    grouped = df_month.groupby("Менеджер").agg({
        "Выручка": "sum",
        "Ср. чек общий": "mean",
        "Ср. поз чек общий": "mean"
    }).reset_index()

    max_revenue = grouped["Выручка"].max()
    max_check = grouped["Ср. чек общий"].max()
    max_depth = grouped["Ср. поз чек общий"].max()

    grouped["Оценка"] = (
        (grouped["Выручка"] / max_revenue) * 0.5 +
        (grouped["Ср. чек общий"] / max_check) * 0.3 +
        (grouped["Ср. поз чек общий"] / max_depth) * 0.2
    ) * 100

    grouped = grouped.round({"Выручка": 0, "Ср. чек общий": 0, "Ср. поз чек общий": 2, "Оценка": 1})
    grouped = grouped.sort_values(by="Оценка", ascending=False).reset_index(drop=True)

    lines = ["🏆 Рейтинг менеджеров за месяц:\n"]
    for i, row in grouped.iterrows():
        lines.append(
            f"{i+1}. {row['Менеджер']} — 💸 {format_ruble(row['Выручка'])}, 🧾 {int(row['Ср. чек общий'])}₽, 📏 {row['Ср. поз чек общий']} → 🔥 {row['Оценка']}%"
        )

    best = grouped.iloc[0]
    lines.append(f"\n🥇 Лучший менеджер: {best['Менеджер']} — {best['Оценка']}%")
    return "\n".join(lines)

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
    app.add_handler(CommandHandler("forecast", forecast_command))
    app.add_handler(CommandHandler("managers", managers_command))

    scheduler = BlockingScheduler(timezone="Europe/Kaliningrad")
    scheduler.add_job(job, trigger="cron", hour=9, minute=30)
    threading.Thread(target=scheduler.start).start()

    app.run_polling()
